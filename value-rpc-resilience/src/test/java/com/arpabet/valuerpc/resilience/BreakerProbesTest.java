/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.resilience;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.ClientInterceptor;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.VrpcException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The tests of the Go implementation for a breaker with more than one probe
 * ({@code breaker_probes_test.go}), one for one.
 */
@Timeout(30)
class BreakerProbesTest {

    private static final CallOptions BG = CallOptions.DEFAULT;
    private static final int THRESHOLD = 5;

    private static final ClientInterceptor.Invoker OK = (method, request, options) -> Value.utf8("ok");
    private static final ClientInterceptor.Invoker DOWN = (method, request, options) -> {
        throw new VrpcException(Code.UNAVAILABLE, "down");
    };

    private static VrpcException failure(ClientInterceptor policy, ClientInterceptor.Invoker next) {
        return assertThrows(VrpcException.class, () -> policy.intercept("m", null, BG, next));
    }

    /** Fails as often as it takes to open the breaker, and checks that it is open. */
    private static void trip(ClientInterceptor breaker) {
        for (int i = 0; i < THRESHOLD; i++) {
            failure(breaker, DOWN);
        }
        final AtomicBoolean invoked = new AtomicBoolean();
        failure(breaker, (method, request, options) -> {
            invoked.set(true);
            return Value.ofNull();
        });
        assertFalse(invoked.get(), "the breaker did not open after as many failures as it takes");
    }

    private static ClientInterceptor breakerWithThreeProbes() {
        return Resilience.circuitBreaker().withFailureThreshold(THRESHOLD).withCooldownMillis(20).withHalfOpenProbes(3);
    }

    /**
     * With three probes the breaker stays half open until three succeeded: a
     * probe that fails after fewer opens it again. The threshold is high, so
     * that "still half open", where one failure opens it, is told apart from
     * "closed already after one success", where one failure would only be
     * counted.
     */
    // TestCircuitBreakerRequiresAllProbes
    @Test
    void circuitBreakerRequiresAllProbes() throws Exception {
        final ClientInterceptor breaker = breakerWithThreeProbes();
        trip(breaker);
        Thread.sleep(30); // the cooldown is over: half open

        // One probe succeeds: with three probes the breaker is still half open.
        assertEquals(Value.utf8("ok"), breaker.intercept("m", null, BG, OK), "the first probe is let through");
        // A probe that fails now opens the breaker again.
        assertEquals("down", failure(breaker, DOWN).detail(), "the failing probe is let through");
        // Open: the next call fails at once.
        final AtomicBoolean invoked = new AtomicBoolean();
        final VrpcException open = failure(breaker, (method, request, options) -> {
            invoked.set(true);
            return Value.ofNull();
        });
        assertEquals(Code.UNAVAILABLE, open.code(), "after a probe that failed the breaker is open");
        assertFalse(invoked.get(), "the breaker closed after one probe that succeeded: three were asked for");
    }

    /** The breaker does close once every probe succeeded, and then lets every call through. */
    // TestCircuitBreakerClosesAfterAllProbes
    @Test
    void circuitBreakerClosesAfterAllProbes() throws Exception {
        final ClientInterceptor breaker = breakerWithThreeProbes();
        trip(breaker);
        Thread.sleep(30);

        // Three probes that succeed close the breaker.
        for (int i = 0; i < 3; i++) {
            assertEquals(Value.utf8("ok"), breaker.intercept("m", null, BG, OK), "probe " + (i + 1) + " is let through");
        }
        // Closed: one failure does not open it, which takes five in a row, so
        // the next call is made.
        final AtomicBoolean invoked = new AtomicBoolean();
        failure(breaker, (method, request, options) -> {
            invoked.set(true);
            throw new VrpcException(Code.UNAVAILABLE, "x");
        });
        assertTrue(invoked.get(), "the breaker is closed after every probe succeeded");
    }
}
