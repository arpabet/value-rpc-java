/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.resilience;

import static com.arpabet.valuerpc.resilience.Calls.fail;
import static com.arpabet.valuerpc.resilience.Calls.ok;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.ClientInterceptor;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.VrpcException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The tests of the Go implementation ({@code resilience_test.go}), one for
 * one and with its timings, so that the two are seen to behave alike. Where
 * the Go test cancels a context, this one interrupts the thread.
 */
@Timeout(30)
class ResilienceTest {

    private static final CallOptions BG = CallOptions.DEFAULT;

    private static VrpcException failure(ClientInterceptor policy, ClientInterceptor.Invoker next) {
        return assertThrows(VrpcException.class, () -> policy.intercept("m", null, BG, next));
    }

    // TestRetrySucceedsAfterTransient
    @Test
    void retrySucceedsAfterTransient() {
        final Calls calls = Calls.of(fail(Code.UNAVAILABLE), fail(Code.UNAVAILABLE), ok("done"));
        final ClientInterceptor retry = Resilience.retry().withMaxAttempts(3).withBackoffMillis(1, 2);

        assertEquals(Value.utf8("done"), retry.intercept("m", null, BG, calls));
        assertEquals(3, calls.count());
    }

    // TestRetryGivesUpAndStopsOnNonRetryable
    @Test
    void retryGivesUpAndStopsOnNonRetryable() {
        final ClientInterceptor retry = Resilience.retry().withMaxAttempts(5).withBackoffMillis(1, 1);

        // A code that is not worth another try: one attempt only.
        Calls calls = Calls.of(fail(Code.INVALID_ARGUMENT));
        assertEquals(Code.INVALID_ARGUMENT, failure(retry, calls).code());
        assertEquals(1, calls.count());

        // Worth another try but failing always: as many attempts as there are.
        calls = Calls.of(fail(Code.UNAVAILABLE));
        assertEquals(Code.UNAVAILABLE, failure(retry, calls).code());
        assertEquals(5, calls.count());
    }

    // TestRetryHonoursContextCancel
    @Test
    void retryHonoursTheInterruptOfItsThread() throws Exception {
        final Calls calls = Calls.of(fail(Code.UNAVAILABLE));
        final ClientInterceptor retry = Resilience.retry().withMaxAttempts(10).withBackoffMillis(1000, 1000).withJitter(false);

        final Thread caller = Thread.currentThread();
        final Thread canceller = new Thread(() -> {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                return;
            }
            caller.interrupt();
        });
        canceller.start();
        final long start = System.nanoTime();
        try {
            assertEquals(Code.UNAVAILABLE, failure(retry, calls).code(), "the last failure is what it came to");
            assertTrue(System.nanoTime() - start < TimeUnit.MILLISECONDS.toNanos(500), "the retry stopped waiting");
            assertEquals(1, calls.count(), "interrupted during the first wait");
            assertTrue(Thread.currentThread().isInterrupted(), "the thread stays interrupted");
        } finally {
            Thread.interrupted();
            canceller.join();
        }
    }

    // TestCircuitBreakerTripsAndRecovers
    @Test
    void circuitBreakerTripsAndRecovers() throws Exception {
        final AtomicInteger calls = new AtomicInteger();
        final ClientInterceptor.Invoker failing = (method, request, options) -> {
            calls.incrementAndGet();
            throw new VrpcException(Code.UNAVAILABLE, "down");
        };
        final ClientInterceptor breaker = Resilience.circuitBreaker().withFailureThreshold(2).withCooldownMillis(30);

        // Two failures open the breaker.
        failure(breaker, failing);
        failure(breaker, failing);
        assertEquals(2, calls.get());
        // Open: the next call fails at once and is not made.
        final VrpcException open = failure(breaker, failing);
        assertEquals(Code.UNAVAILABLE, open.code());
        assertEquals("circuit breaker open for \"m\"", open.detail());
        assertEquals(2, calls.get(), "the breaker did not let the call through");
        // After the cooldown: half open lets a probe through, and its success closes the breaker.
        Thread.sleep(40);
        assertEquals(Value.utf8("ok"), breaker.intercept("m", null, BG, (method, request, options) -> {
            calls.incrementAndGet();
            return Value.utf8("ok");
        }));
        assertEquals(3, calls.get(), "the probe was let through");
    }

    // TestCircuitBreakerIgnoresClientErrors
    @Test
    void circuitBreakerIgnoresTheFaultsOfTheCaller() {
        final ClientInterceptor breaker = Resilience.circuitBreaker().withFailureThreshold(2);
        final Calls bad = Calls.of(fail(Code.INVALID_ARGUMENT));
        for (int i = 0; i < 5; i++) {
            failure(breaker, bad);
        }
        // The faults of the caller do not open the breaker: every call is made.
        assertEquals(5, bad.count());
    }

    // TestTimeoutSetsDeadline
    @Test
    void timeoutSetsTheDeadline() {
        final ClientInterceptor timeout = Resilience.timeoutMillis(50);
        final Calls calls = Calls.of(Value.ofNull());
        timeout.intercept("m", null, BG, calls);
        assertEquals(50L, calls.timeouts().get(0), "the call is made with a deadline");
    }

    // TestFallback
    @Test
    void fallback() {
        final ClientInterceptor fallback = Resilience.fallback((method, request, options, error) -> Value.utf8("fallback"));
        assertEquals(Value.utf8("fallback"), fallback.intercept("m", null, BG, Calls.of(new VrpcException(Code.INTERNAL, "boom"))));
    }

    // TestBulkheadRejectsWhenFull
    @Test
    void bulkheadRejectsWhenFull() throws Exception {
        final ClientInterceptor bulkhead = Resilience.bulkhead(1);
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread holder = new Thread(() -> bulkhead.intercept("m", null, BG, (method, request, options) -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return Value.utf8("ok");
        }));
        holder.start();
        assertTrue(entered.await(10, TimeUnit.SECONDS)); // the one place is taken

        // One that waits and has given up fails at once.
        final AtomicBoolean ran = new AtomicBoolean();
        Thread.currentThread().interrupt();
        try {
            final VrpcException full = failure(bulkhead, (method, request, options) -> {
                ran.set(true);
                return null;
            });
            assertEquals(Code.RESOURCE_EXHAUSTED, full.code());
            assertEquals("bulkhead full for \"m\": canceled", full.detail());
            assertFalse(ran.get(), "the call is not made while the bulkhead is full");
            assertTrue(Thread.currentThread().isInterrupted(), "the thread stays interrupted");
        } finally {
            Thread.interrupted();
            release.countDown();
            holder.join();
        }
    }

    // TestRateLimitShedsOverBurst
    @Test
    void rateLimitShedsOverTheBurst() {
        final ClientInterceptor limit = Resilience.rateLimit(1, 1); // one a second, a burst of one
        final Calls next = Calls.of(ok("ok"));
        assertEquals(Value.utf8("ok"), limit.intercept("m", null, BG, next), "the first call passes");
        final VrpcException shed = failure(limit, next);
        assertEquals(Code.RESOURCE_EXHAUSTED, shed.code());
        assertEquals("rate limit exceeded for \"m\"", shed.detail());
        assertEquals(1, next.count());
    }
}
