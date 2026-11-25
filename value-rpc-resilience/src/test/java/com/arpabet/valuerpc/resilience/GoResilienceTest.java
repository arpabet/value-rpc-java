/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.resilience;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.arpabet.value.NumberValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.GoServer;
import com.arpabet.valuerpc.VrpcException;
import com.arpabet.valuerpc.client.VrpcClient;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The policies in a client of the Go server: what they decide by is the code
 * the Go implementation puts on the wire, and what they send is what it
 * reads. Skipped where there is no Go toolchain.
 */
@Timeout(60)
class GoResilienceTest {

    private static GoServer server;
    private static final AtomicInteger KEYS = new AtomicInteger();

    @BeforeAll
    static void start() throws Exception {
        assumeTrue(GoServer.available(), "no Go toolchain: the Go server cannot be built");
        server = GoServer.start();
    }

    @AfterAll
    static void stop() {
        if (server != null) {
            server.close();
        }
    }

    private static VrpcClient.Builder client() {
        return VrpcClient.builder().address(server.tcp()).timeoutMillis(5000);
    }

    /** The arguments of the function that fails with {@code Unavailable} the first times it is called with a key. */
    private static Value flaky(int failures) {
        return Value.list(Value.utf8("key-" + KEYS.incrementAndGet()), Value.ofLong(failures));
    }

    @Test
    void aRetryMakesUpForAServerThatIsNotUpYet() {
        try (VrpcClient client = client().interceptors(Resilience.retry().withMaxAttempts(5).withBackoffMillis(1, 5)).build()) {
            // Unavailable twice, and then the number of the call that got through.
            assertEquals(Value.ofLong(3), client.callFunction("flaky", flaky(2)));
        }
        // Without the policy the caller sees the failure, with the code the server gave it.
        try (VrpcClient plain = client().build()) {
            final VrpcException down = assertThrows(VrpcException.class, () -> plain.callFunction("flaky", flaky(2)));
            assertSame(Code.UNAVAILABLE, down.code());
            assertEquals("function flaky: warming up, call 1", down.detail());
        }
    }

    @Test
    void aRetryGivesUpWhenItsAttemptsAreSpent() {
        final Value always = flaky(1000);
        try (VrpcClient client = client().interceptors(Resilience.retry().withMaxAttempts(4).withBackoffMillis(1, 2)).build()) {
            final VrpcException last = assertThrows(VrpcException.class, () -> client.callFunction("flaky", always));
            assertSame(Code.UNAVAILABLE, last.code());
            assertEquals("function flaky: warming up, call 4", last.detail(), "the failure of the last of four attempts");
            // What is not worth another try is tried once: the server saw one call more.
            assertSame(Code.INVALID_ARGUMENT, assertThrows(VrpcException.class,
                    () -> client.callFunction("typed", Value.list(Value.ofLong(1)))).code());
            assertEquals("function flaky: warming up, call 8",
                    assertThrows(VrpcException.class, () -> client.callFunction("flaky", always)).detail());
        }
    }

    @Test
    void theCodesOfTheServerOpenTheBreakerOrDoNot() {
        try (VrpcClient client = client()
                .interceptors(Resilience.circuitBreaker().withFailureThreshold(3).withCooldownMillis(60_000)).build()) {
            // The faults of the caller, as the Go server names them, are not the server's failing.
            for (int i = 0; i < 6; i++) {
                assertSame(Code.INVALID_ARGUMENT, assertThrows(VrpcException.class,
                        () -> client.callFunction("typed", Value.list(Value.ofLong(1)))).code());
                assertSame(Code.NOT_FOUND, assertThrows(VrpcException.class, () -> client.callFunction("no.such", null)).code());
            }
            // A failure of the server is: three in a row, and the fourth call is not made.
            for (int i = 0; i < 3; i++) {
                assertSame(Code.RESOURCE_EXHAUSTED, assertThrows(VrpcException.class, () -> client.callFunction("fail", null)).code());
            }
            final VrpcException open = assertThrows(VrpcException.class, () -> client.callFunction("fail", null));
            assertSame(Code.UNAVAILABLE, open.code());
            assertEquals("circuit breaker open for \"fail\"", open.detail());
            // Each method has a breaker of its own.
            assertEquals(Value.utf8("Hello, go!"), client.callFunction("greet", Value.list(Value.utf8("go"))));
            assertSame(Code.INVALID_ARGUMENT, assertThrows(VrpcException.class,
                    () -> client.callFunction("typed", Value.list(Value.ofLong(1)))).code());
        }
    }

    @Test
    void theTimeoutOfThePolicyIsTheDeadlineTheHandlerSees() {
        try (VrpcClient client = client().interceptors(Resilience.timeoutMillis(750)).build()) {
            final long left = ((NumberValue) client.callFunction("deadline", null)).longValue();
            assertTrue(left > 0 && left <= 750, "the handler had " + left + " ms");
            // A call with less time of its own keeps it.
            final long less = ((NumberValue) client.callFunction("deadline", null, CallOptions.timeoutMillis(300))).longValue();
            assertTrue(less > 0 && less <= 300, "the handler had " + less + " ms");
            // And it bounds the call here: the handler would take five seconds.
            final long start = System.nanoTime();
            assertSame(Code.DEADLINE_EXCEEDED, assertThrows(VrpcException.class, () -> client.callFunction("slow", null)).code());
            final long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(took >= 700 && took < 3000, "it took " + took + " ms");
        }
    }

    @Test
    void theChainOfTheDocumentation() {
        // Rate limit, circuit breaker, retry, a timeout for each attempt, the call.
        try (VrpcClient client = client().interceptors(
                Resilience.rateLimit(1000, 100),
                Resilience.circuitBreaker(),
                Resilience.retry().withMaxAttempts(3).withBackoffMillis(1, 5),
                Resilience.timeoutMillis(2000)).build()) {
            assertEquals(Value.ofLong(3), client.callFunction("flaky", flaky(2)));
            assertEquals(Value.utf8("Hello, chain!"), client.callFunction("greet", Value.list(Value.utf8("chain"))));
            final long left = ((NumberValue) client.callFunction("deadline", null)).longValue();
            assertTrue(left > 0 && left <= 2000, "the handler had " + left + " ms");
        }
    }

    @Test
    void aFallbackAnswersInPlaceOfAServerThatFails() {
        final AtomicReference<VrpcException> seen = new AtomicReference<VrpcException>();
        try (VrpcClient client = client().interceptors(Resilience.fallback((method, request, options, failure) -> {
            seen.set(failure);
            return Value.utf8("cached");
        })).build()) {
            assertEquals(Value.utf8("cached"), client.callFunction("boom", null));
            assertSame(Code.INTERNAL, seen.get().code());
            assertEquals("function boom: boom", seen.get().detail());
            assertEquals(Value.utf8("Hello, go!"), client.callFunction("greet", Value.list(Value.utf8("go"))), "not asked when the call succeeds");
        }
    }

    @Test
    void aRateLimitShedsBeforeTheServerIsAsked() {
        final Value key = flaky(0);
        try (VrpcClient client = client().interceptors(Resilience.rateLimit(0, 2)).build()) {
            assertEquals(Value.ofLong(1), client.callFunction("flaky", key));
            assertEquals(Value.ofLong(2), client.callFunction("flaky", key));
            final VrpcException shed = assertThrows(VrpcException.class, () -> client.callFunction("flaky", key));
            assertSame(Code.RESOURCE_EXHAUSTED, shed.code());
            assertEquals("rate limit exceeded for \"flaky\"", shed.detail());
        }
        // The server counted two calls and no third.
        try (VrpcClient plain = client().build()) {
            assertEquals(Value.ofLong(3), plain.callFunction("flaky", key));
        }
    }
}
