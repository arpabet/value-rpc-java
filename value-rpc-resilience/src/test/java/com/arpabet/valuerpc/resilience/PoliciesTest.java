/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.resilience;

import static com.arpabet.valuerpc.resilience.Calls.fail;
import static com.arpabet.valuerpc.resilience.Calls.ok;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.ClientInterceptor;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.VrpcException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * What the Go tests do not say, and what is particular to this
 * implementation: how the policies read the options of a call where the Go
 * ones read a context, the waits of a retry by a clock that is moved by hand,
 * and the edges of each policy.
 */
@Timeout(30)
class PoliciesTest {

    private static final CallOptions BG = CallOptions.DEFAULT;
    private static final long MS = 1_000_000L;

    private static VrpcException failure(ClientInterceptor policy, CallOptions options, ClientInterceptor.Invoker next) {
        return assertThrows(VrpcException.class, () -> policy.intercept("m", null, options, next));
    }

    // --- retry ---------------------------------------------------------------------

    @Test
    void theDefaultsOfARetryAreThoseOfTheGoImplementation() {
        final FakeTicker ticker = new FakeTicker();
        final Calls calls = Calls.of(fail(Code.UNAVAILABLE));
        final VrpcException last = failure(Resilience.retry().withJitter(false).withTicker(ticker), BG, calls);
        assertEquals(Code.UNAVAILABLE, last.code());
        assertEquals(3, calls.count(), "three attempts, the first included");
        assertEquals(Arrays.asList(50L, 100L), ticker.sleepsMillis(), "50 ms, then twice that");
        assertEquals(Arrays.asList(0L, 0L, 0L), calls.timeouts(), "without a timeout of the caller the options go on as they came");
    }

    @Test
    void theWaitOfARetryDoublesUpToItsMaximum() {
        final FakeTicker ticker = new FakeTicker();
        final Calls calls = Calls.of(fail(Code.RESOURCE_EXHAUSTED));
        failure(Resilience.retry().withMaxAttempts(8).withBackoffMillis(50, 1000).withJitter(false).withTicker(ticker), BG, calls);
        assertEquals(8, calls.count());
        assertEquals(Arrays.asList(50L, 100L, 200L, 400L, 800L, 1000L, 1000L), ticker.sleepsMillis());
    }

    @Test
    void aBaseAboveTheMaximumIsWaitedOnceAsInGo() {
        // The Go implementation caps a wait only after it doubled it.
        final FakeTicker ticker = new FakeTicker();
        failure(Resilience.retry().withMaxAttempts(4).withBackoffMillis(500, 200).withJitter(false).withTicker(ticker), BG,
                Calls.of(fail(Code.UNAVAILABLE)));
        assertEquals(Arrays.asList(500L, 200L, 200L), ticker.sleepsMillis());
    }

    @Test
    void jitterWaitsBetweenHalfAWaitAndAllOfIt() {
        final FakeTicker ticker = new FakeTicker();
        final Retry retry = Resilience.retry().withMaxAttempts(2).withBackoffMillis(100, 100).withTicker(ticker);
        long least = Long.MAX_VALUE;
        long most = 0;
        for (int i = 0; i < 2000; i++) {
            failure(retry, BG, Calls.of(fail(Code.UNAVAILABLE)));
        }
        for (long nanos : ticker.sleeps()) {
            assertTrue(nanos >= 50 * MS && nanos <= 100 * MS, "a wait of " + nanos + " ns");
            least = Math.min(least, nanos);
            most = Math.max(most, nanos);
        }
        assertEquals(2000, ticker.sleeps().size());
        assertTrue(least < 55 * MS && most > 95 * MS, "the waits are spread: " + least + " to " + most);
    }

    @Test
    void noWaitIsNoWait() {
        final FakeTicker ticker = new FakeTicker();
        final Calls calls = Calls.of(fail(Code.UNAVAILABLE), fail(Code.UNAVAILABLE), ok("done"));
        assertEquals(Value.utf8("done"), Resilience.retry().withBackoffMillis(0, 0).withTicker(ticker).intercept("m", null, BG, calls));
        assertEquals(Arrays.asList(0L, 0L), ticker.sleeps());
    }

    @Test
    void fewerThanOneAttemptIsOneAttempt() {
        for (int attempts : new int[] {1, 0, -3}) {
            final Calls calls = Calls.of(fail(Code.UNAVAILABLE));
            failure(Resilience.retry().withMaxAttempts(attempts), BG, calls);
            assertEquals(1, calls.count(), attempts + " attempts");
        }
    }

    @Test
    void aRetryPassesOnWhatACallReturnsAndWhatItWasGiven() {
        final AtomicReference<Object[]> seen = new AtomicReference<Object[]>();
        final CallOptions options = CallOptions.DEFAULT.withMetadata("trace", "t1");
        assertNull(Resilience.retry().intercept("user.get", Value.ofLong(7), options, (method, request, o) -> {
            seen.set(new Object[] {method, request, o});
            return null;
        }), "a call that returns nothing");
        assertEquals("user.get", seen.get()[0]);
        assertEquals(Value.ofLong(7), seen.get()[1]);
        assertSame(options, seen.get()[2]);
    }

    @Test
    void theRuleOfWhatIsWorthAnotherTryCanBeReplaced() {
        final List<String> asked = new ArrayList<String>();
        final Retry retry = Resilience.retry().withMaxAttempts(4).withBackoffMillis(0, 0).withRetryable((method, error) -> {
            asked.add(method + ":" + error.code());
            return error.code() == Code.DEADLINE_EXCEEDED;
        });
        final Calls calls = Calls.of(fail(Code.DEADLINE_EXCEEDED), fail(Code.DEADLINE_EXCEEDED), fail(Code.UNAVAILABLE), ok("never"));
        assertEquals(Code.UNAVAILABLE, failure(retry, BG, calls).code(), "what the default tries again is no longer tried again");
        assertEquals(3, calls.count());
        assertEquals(Arrays.asList("m:DeadlineExceeded", "m:DeadlineExceeded", "m:Unavailable"), asked);
        assertThrows(NullPointerException.class, () -> Resilience.retry().withRetryable(null));
    }

    @Test
    void whatIsNotAFailureOfTheCallIsNotTriedAgain() {
        // A bug, in an interceptor further in or in the application: it is
        // not the peer that failed, and it goes to the caller as it is.
        final IllegalStateException bug = new IllegalStateException("a bug");
        final Calls calls = Calls.of(bug);
        assertSame(bug, assertThrows(IllegalStateException.class, () -> Resilience.retry().intercept("m", null, BG, calls)));
        assertEquals(1, calls.count());
    }

    @Test
    void aTimeoutOfTheCallerIsForTheWholeCall() {
        // 250 ms for the call: the attempts and the waits between them share
        // it, as they share the deadline of a context in the Go implementation.
        final FakeTicker ticker = new FakeTicker();
        final List<Long> timeouts = new ArrayList<Long>();
        final Retry retry = Resilience.retry().withMaxAttempts(10).withBackoffMillis(50, 1000).withJitter(false).withTicker(ticker);
        final VrpcException last = failure(retry, CallOptions.timeoutMillis(250), (method, request, options) -> {
            timeouts.add(options.timeoutMillis());
            ticker.advanceMillis(20); // each attempt takes 20 ms
            throw new VrpcException(Code.UNAVAILABLE, "attempt " + timeouts.size());
        });
        // 0: attempt (250 left), 20: wait 50, 70: attempt (180 left), 90: wait
        // 100, 190: attempt (60 left), 210: a wait of 200 with 40 left.
        assertEquals(Arrays.asList(250L, 180L, 60L), timeouts);
        assertEquals(Arrays.asList(50L, 100L), ticker.sleepsMillis(), "it does not wait for an attempt it cannot make");
        assertEquals("attempt 3", last.detail(), "the last failure is what the call came to");
    }

    @Test
    void aTimeoutOfTheCallerThatIsUsedUpByAnAttemptEndsTheRetry() {
        final FakeTicker ticker = new FakeTicker();
        final Calls calls = Calls.of(fail(Code.UNAVAILABLE));
        final Retry retry = Resilience.retry().withMaxAttempts(5).withBackoffMillis(0, 0).withTicker(ticker);
        final AtomicInteger attempts = new AtomicInteger();
        failure(retry, CallOptions.timeoutMillis(100), (method, request, options) -> {
            attempts.incrementAndGet();
            ticker.advanceMillis(100); // the attempt took all there was
            return calls.invoke(method, request, options);
        });
        assertEquals(1, attempts.get());
        assertTrue(ticker.sleeps().isEmpty());
    }

    @Test
    void whatIsLeftOfTheCallersTimeIsRoundedUpToAMillisecond() {
        // A timeout of 0 would mean the timeout of the client: an attempt
        // with less than a millisecond left is made with one.
        final FakeTicker ticker = new FakeTicker();
        final List<Long> timeouts = new ArrayList<Long>();
        final Retry retry = Resilience.retry().withMaxAttempts(2).withBackoffMillis(0, 0).withTicker(ticker);
        failure(retry, CallOptions.timeoutMillis(10), (method, request, options) -> {
            timeouts.add(options.timeoutMillis());
            ticker.advanceNanos(10 * MS - 1000); // all but a microsecond
            throw new VrpcException(Code.UNAVAILABLE, "x");
        });
        assertEquals(Arrays.asList(10L, 1L), timeouts);
    }

    @Test
    void withoutATimeoutOfTheCallerEachAttemptHasTheTimeoutOfTheClient() {
        final FakeTicker ticker = new FakeTicker();
        for (CallOptions options : new CallOptions[] {CallOptions.DEFAULT, CallOptions.timeoutMillis(-1)}) {
            final Calls calls = Calls.of(fail(Code.UNAVAILABLE));
            failure(Resilience.retry().withTicker(ticker), options, (method, request, o) -> {
                ticker.advanceMillis(60_000); // however long an attempt takes
                return calls.invoke(method, request, o);
            });
            assertEquals(3, calls.count());
            assertEquals(Collections.nCopies(3, options.timeoutMillis()), calls.timeouts());
        }
    }

    @Test
    void aRetryIsImmutable() {
        final Retry base = Resilience.retry();
        final Retry other = base.withMaxAttempts(7);
        final Calls calls = Calls.of(fail(Code.UNAVAILABLE));
        failure(base.withBackoffMillis(0, 0), BG, calls);
        assertEquals(3, calls.count(), "the retry it was made of is as it was");
        assertTrue(other.toString().contains("attempts=7"), other.toString());
        assertThrows(IllegalArgumentException.class, () -> base.withBackoffMillis(-1, 10));
        assertThrows(IllegalArgumentException.class, () -> base.withBackoffMillis(1, -10));
    }

    // --- timeout -------------------------------------------------------------------

    @Test
    void aTimeoutBoundsACallThatHasNoneOrALongerOne() {
        final ClientInterceptor timeout = Resilience.timeoutMillis(200);
        final Calls calls = Calls.of(ok("ok"));
        timeout.intercept("m", null, CallOptions.DEFAULT, calls);        // the timeout of the client
        timeout.intercept("m", null, CallOptions.timeoutMillis(-1), calls);   // none
        timeout.intercept("m", null, CallOptions.timeoutMillis(5000), calls); // a longer one
        timeout.intercept("m", null, CallOptions.timeoutMillis(50), calls);   // a shorter one, which it keeps
        timeout.intercept("m", null, CallOptions.timeoutMillis(200), calls);
        assertEquals(Arrays.asList(200L, 200L, 200L, 50L, 200L), calls.timeouts());
    }

    @Test
    void aTimeoutKeepsWhatElseTheOptionsCarry() {
        final AtomicReference<CallOptions> seen = new AtomicReference<CallOptions>();
        Resilience.timeoutMillis(200).intercept("m", null, CallOptions.DEFAULT.withMetadata("trace", "t1"), (method, request, options) -> {
            seen.set(options);
            return null;
        });
        assertEquals(200, seen.get().timeoutMillis());
        assertEquals("t1", seen.get().metadata().get("trace"));
    }

    @Test
    void aTimeoutHasToBePositive() {
        assertThrows(IllegalArgumentException.class, () -> Resilience.timeoutMillis(0));
        assertThrows(IllegalArgumentException.class, () -> Resilience.timeoutMillis(-5));
    }

    @Test
    void aTimeoutInsideARetryBoundsEachAttempt() {
        final FakeTicker ticker = new FakeTicker();
        final Calls calls = Calls.of(fail(Code.UNAVAILABLE));
        final ClientInterceptor.Invoker chain = ClientInterceptor.chain(calls, Arrays.asList(
                Resilience.retry().withBackoffMillis(10, 10).withJitter(false).withTicker(ticker),
                Resilience.timeoutMillis(100)));
        // Without a timeout of the caller: 100 ms for each attempt.
        assertThrows(VrpcException.class, () -> chain.invoke("m", null, BG));
        assertEquals(Arrays.asList(100L, 100L, 100L), calls.timeouts());
        // With one of 150 ms: what is left of it once it is less than that.
        final List<Long> timeouts = new ArrayList<Long>();
        final ClientInterceptor.Invoker slow = ClientInterceptor.chain((method, request, options) -> {
            timeouts.add(options.timeoutMillis());
            ticker.advanceMillis(40);
            throw new VrpcException(Code.UNAVAILABLE, "x");
        }, Arrays.asList(Resilience.retry().withBackoffMillis(10, 10).withJitter(false).withTicker(ticker),
                Resilience.timeoutMillis(100)));
        assertThrows(VrpcException.class, () -> slow.invoke("m", null, CallOptions.timeoutMillis(150)));
        assertEquals(Arrays.asList(100L, 100L, 50L), timeouts);
    }

    // --- fallback ------------------------------------------------------------------

    @Test
    void aFallbackIsNotAskedWhenTheCallSucceeds() {
        final AtomicInteger asked = new AtomicInteger();
        final ClientInterceptor fallback = Resilience.fallback((method, request, options, error) -> {
            asked.incrementAndGet();
            return Value.utf8("fallback");
        });
        assertEquals(Value.utf8("ok"), fallback.intercept("m", null, BG, Calls.of(ok("ok"))));
        assertNull(fallback.intercept("m", null, BG, Calls.of((Object) null)));
        assertEquals(0, asked.get());
    }

    @Test
    void aFallbackSeesTheCallAndItsFailureAndMayFailInItsPlace() {
        final AtomicReference<Object[]> seen = new AtomicReference<Object[]>();
        final VrpcException boom = new VrpcException(Code.INTERNAL, "boom");
        final CallOptions options = CallOptions.timeoutMillis(75);
        final ClientInterceptor fallback = Resilience.fallback((method, request, o, error) -> {
            seen.set(new Object[] {method, request, o, error});
            throw new VrpcException(Code.UNAVAILABLE, "no answer and no cached one", error);
        });
        final VrpcException instead = assertThrows(VrpcException.class,
                () -> fallback.intercept("quote.get", Value.utf8("ACME"), options, Calls.of(boom)));
        assertEquals(Code.UNAVAILABLE, instead.code());
        assertSame(boom, instead.getCause());
        assertEquals("quote.get", seen.get()[0]);
        assertEquals(Value.utf8("ACME"), seen.get()[1]);
        assertSame(options, seen.get()[2]);
        assertSame(boom, seen.get()[3]);
        assertThrows(NullPointerException.class, () -> Resilience.fallback(null));
    }

    // --- bulkhead ------------------------------------------------------------------

    @Test
    void aBulkheadLetsAsManyThroughAtOnceAsItHasPlacesAndNoMore() throws Exception {
        final int places = 3;
        final ClientInterceptor bulkhead = Resilience.bulkhead(places);
        final CountDownLatch inside = new CountDownLatch(places);
        final CountDownLatch release = new CountDownLatch(1);
        final List<Thread> holders = new ArrayList<Thread>();
        for (int i = 0; i < places; i++) {
            final Thread t = new Thread(() -> bulkhead.intercept("m", null, BG, (method, request, options) -> {
                inside.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return null;
            }));
            holders.add(t);
            t.start();
        }
        try {
            assertTrue(inside.await(10, TimeUnit.SECONDS), "as many calls at once as there are places");
            final Calls oneMore = Calls.of(ok("never"));
            assertEquals(Code.RESOURCE_EXHAUSTED, failure(bulkhead, CallOptions.timeoutMillis(30), oneMore).code());
            assertEquals(0, oneMore.count(), "and not one more");
        } finally {
            release.countDown();
            for (Thread t : holders) {
                t.join();
            }
        }
    }

    @Test
    void manyCallersNeverExceedThePlaces() throws Exception {
        final int places = 3;
        final int callers = 24;
        final ClientInterceptor bulkhead = Resilience.bulkhead(places);
        final AtomicInteger inside = new AtomicInteger();
        final AtomicInteger most = new AtomicInteger();
        final AtomicInteger done = new AtomicInteger();
        final List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < callers; i++) {
            final Thread t = new Thread(() -> {
                for (int n = 0; n < 50; n++) {
                    bulkhead.intercept("m", null, BG, (method, request, options) -> {
                        most.accumulateAndGet(inside.incrementAndGet(), Math::max);
                        Thread.yield();
                        inside.decrementAndGet();
                        return null;
                    });
                    done.incrementAndGet();
                }
            });
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        assertEquals(callers * 50, done.get(), "every call was made in the end");
        assertTrue(most.get() <= places, most.get() + " calls at once through " + places + " places");
    }

    @Test
    void aPlaceIsGivenBackWhenACallFails() {
        final ClientInterceptor bulkhead = Resilience.bulkhead(1);
        for (int i = 0; i < 3; i++) {
            assertEquals(Code.INTERNAL, failure(bulkhead, BG, Calls.of(fail(Code.INTERNAL))).code());
            assertThrows(IllegalStateException.class, () -> bulkhead.intercept("m", null, BG, Calls.of(new IllegalStateException("a bug"))));
        }
        assertEquals(Value.utf8("ok"), bulkhead.intercept("m", null, BG, Calls.of(ok("ok"))));
    }

    @Test
    void aCallWaitsForAPlaceNoLongerThanItsTimeout() throws Exception {
        final ClientInterceptor bulkhead = Resilience.bulkhead(0); // taken as one place
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread holder = new Thread(() -> bulkhead.intercept("m", null, BG, (method, request, options) -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }));
        holder.start();
        try {
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            final long start = System.nanoTime();
            final Calls calls = Calls.of(ok("never"));
            final VrpcException full = failure(bulkhead, CallOptions.timeoutMillis(60), calls);
            final long waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertEquals(Code.RESOURCE_EXHAUSTED, full.code());
            assertEquals("bulkhead full for \"m\": deadline exceeded", full.detail());
            assertEquals(0, calls.count());
            assertTrue(waited >= 55 && waited < 5000, "waited " + waited + " ms");
            assertFalse(Thread.currentThread().isInterrupted());
        } finally {
            release.countDown();
            holder.join();
        }
    }

    @Test
    void whatACallWaitedForAPlaceIsTakenFromItsTime() throws Exception {
        final FakeTicker ticker = new FakeTicker();
        final ClientInterceptor bulkhead = Resilience.bulkhead(1, ticker);
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread holder = new Thread(() -> bulkhead.intercept("m", null, BG, (method, request, options) -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }));
        holder.start();
        assertTrue(entered.await(10, TimeUnit.SECONDS));

        final Calls calls = Calls.of(ok("ok"));
        final AtomicReference<Object> result = new AtomicReference<Object>();
        final Thread waiter = new Thread(() -> {
            try {
                result.set(bulkhead.intercept("m", null, CallOptions.timeoutMillis(10_000), calls));
            } catch (RuntimeException e) {
                result.set(e);
            }
        });
        waiter.start();
        // Once it waits for the place, three seconds pass, and the place is given back.
        final long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (waiter.getState() != Thread.State.TIMED_WAITING) {
            assertTrue(System.nanoTime() < until, "the second call never waited");
            Thread.sleep(1);
        }
        ticker.advanceMillis(3000);
        release.countDown();
        waiter.join();
        holder.join();
        assertEquals(Value.utf8("ok"), result.get());
        assertEquals(Collections.singletonList(7000L), calls.timeouts(), "ten seconds, less the three it waited");

        // One that did not wait is made with the options it came with.
        final CallOptions options = CallOptions.timeoutMillis(10_000);
        final AtomicReference<CallOptions> seen = new AtomicReference<CallOptions>();
        bulkhead.intercept("m", null, options, (method, request, o) -> {
            seen.set(o);
            return null;
        });
        assertSame(options, seen.get());
    }

    @Test
    void aCallWhoseTimeRanOutWhileItWaitedIsNotMade() throws Exception {
        // The place came, but too late: by the clock of the policy, which the
        // test moves, the time of the call is over.
        final FakeTicker ticker = new FakeTicker();
        final ClientInterceptor bulkhead = Resilience.bulkhead(1, ticker);
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread holder = new Thread(() -> bulkhead.intercept("m", null, BG, (method, request, options) -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }));
        holder.start();
        assertTrue(entered.await(10, TimeUnit.SECONDS));

        final Calls calls = Calls.of(ok("never"));
        final AtomicReference<Object> result = new AtomicReference<Object>();
        final Thread waiter = new Thread(() -> {
            try {
                result.set(bulkhead.intercept("m", null, CallOptions.timeoutMillis(10_000), calls));
            } catch (RuntimeException e) {
                result.set(e);
            }
        });
        waiter.start();
        final long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (waiter.getState() != Thread.State.TIMED_WAITING) {
            assertTrue(System.nanoTime() < until, "the second call never waited");
            Thread.sleep(1);
        }
        ticker.advanceMillis(10_000);
        release.countDown();
        waiter.join();
        holder.join();
        assertTrue(result.get() instanceof VrpcException, String.valueOf(result.get()));
        assertEquals("bulkhead full for \"m\": deadline exceeded", ((VrpcException) result.get()).detail());
        assertEquals(0, calls.count());
        // The place it was given and did not use is free again.
        assertEquals(Value.utf8("ok"), bulkhead.intercept("m", null, BG, Calls.of(ok("ok"))));
    }

    @Test
    void anInterruptedThreadThatFindsAPlaceMakesItsCall() {
        // The bulkhead is not full, so it is not what fails: the call is
        // made, and the client says canceled.
        final ClientInterceptor bulkhead = Resilience.bulkhead(1);
        final Calls calls = Calls.of(ok("ok"));
        Thread.currentThread().interrupt();
        try {
            assertEquals(Value.utf8("ok"), bulkhead.intercept("m", null, BG, calls));
            assertEquals(1, calls.count());
        } finally {
            assertTrue(Thread.interrupted());
        }
    }

    // --- rate limit ----------------------------------------------------------------

    @Test
    void aRateLimitFillsAgainWithTime() {
        final FakeTicker ticker = new FakeTicker();
        final ClientInterceptor limit = Resilience.rateLimit(10, 3, ticker); // ten a second, a burst of three
        final Calls next = Calls.of(ok("ok"));
        for (int i = 0; i < 3; i++) {
            limit.intercept("m", null, BG, next);
        }
        assertEquals(Code.RESOURCE_EXHAUSTED, failure(limit, BG, next).code(), "the burst is spent");
        ticker.advanceMillis(90);
        assertEquals(Code.RESOURCE_EXHAUSTED, failure(limit, BG, next).code(), "not yet a tenth of a second");
        ticker.advanceMillis(20);
        limit.intercept("m", null, BG, next);
        assertEquals(Code.RESOURCE_EXHAUSTED, failure(limit, BG, next).code());
        // A long quiet time fills it to its burst and no further.
        ticker.advanceMillis(60_000);
        for (int i = 0; i < 3; i++) {
            limit.intercept("m", null, BG, next);
        }
        assertEquals(Code.RESOURCE_EXHAUSTED, failure(limit, BG, next).code());
        assertEquals(7, next.count());
    }

    @Test
    void aRateLimitIsSharedByTheMethods() {
        final FakeTicker ticker = new FakeTicker();
        final ClientInterceptor limit = Resilience.rateLimit(1, 2, ticker);
        final Calls next = Calls.of(ok("ok"));
        limit.intercept("a", null, BG, next);
        limit.intercept("b", null, BG, next);
        final VrpcException shed = assertThrows(VrpcException.class, () -> limit.intercept("c", null, BG, next));
        assertEquals("rate limit exceeded for \"c\"", shed.detail());
    }

    @Test
    void aBurstOfLessThanOneIsOneAndARateOfNothingNeverFills() {
        final FakeTicker ticker = new FakeTicker();
        final ClientInterceptor limit = Resilience.rateLimit(0, 0, ticker);
        final Calls next = Calls.of(ok("ok"));
        limit.intercept("m", null, BG, next);
        ticker.advanceMillis(3_600_000);
        assertEquals(Code.RESOURCE_EXHAUSTED, failure(limit, BG, next).code());
        assertThrows(IllegalArgumentException.class, () -> Resilience.rateLimit(-1, 1));
        assertThrows(IllegalArgumentException.class, () -> Resilience.rateLimit(Double.NaN, 1));
        assertThrows(IllegalArgumentException.class, () -> Resilience.rateLimit(Double.POSITIVE_INFINITY, 1));
    }

    // --- circuit breaker -----------------------------------------------------------

    private static final ClientInterceptor.Invoker OK = (method, request, options) -> Value.utf8("ok");
    private static final ClientInterceptor.Invoker DOWN = (method, request, options) -> {
        throw new VrpcException(Code.UNAVAILABLE, "down");
    };

    private static boolean letsThrough(ClientInterceptor breaker, String method) {
        final AtomicInteger made = new AtomicInteger();
        try {
            breaker.intercept(method, null, BG, (m, request, options) -> {
                made.incrementAndGet();
                return null;
            });
        } catch (VrpcException e) {
            assertEquals(Code.UNAVAILABLE, e.code());
            assertEquals("circuit breaker open for \"" + method + "\"", e.detail());
        }
        return made.get() == 1;
    }

    @Test
    void theDefaultsOfABreakerAreThoseOfTheGoImplementation() {
        final FakeTicker ticker = new FakeTicker();
        final CircuitBreaker breaker = Resilience.circuitBreaker().withTicker(ticker);
        for (int i = 0; i < 4; i++) {
            failure(breaker, BG, DOWN);
        }
        assertTrue(letsThrough(breaker, "m"), "four failures in a row do not open it; and a success starts the count again");
        for (int i = 0; i < 4; i++) {
            failure(breaker, BG, DOWN);
        }
        assertTrue(letsThrough(breaker, "m"));
        for (int i = 0; i < 5; i++) {
            failure(breaker, BG, DOWN);
        }
        assertFalse(letsThrough(breaker, "m"), "five in a row open it");
        ticker.advanceMillis(9_999);
        assertFalse(letsThrough(breaker, "m"), "for ten seconds");
        ticker.advanceMillis(1);
        assertTrue(letsThrough(breaker, "m"), "then one probe is let through, and its success closes it");
        assertTrue(letsThrough(breaker, "m"));
    }

    @Test
    void eachMethodHasABreakerOfItsOwn() {
        final CircuitBreaker breaker = Resilience.circuitBreaker().withFailureThreshold(1).withTicker(new FakeTicker());
        assertThrows(VrpcException.class, () -> breaker.intercept("orders.get", null, BG, DOWN));
        assertFalse(letsThrough(breaker, "orders.get"));
        assertTrue(letsThrough(breaker, "users.get"), "another method is not held up");
    }

    @Test
    void aProbeThatFailsOpensTheBreakerForAnotherCooldown() {
        final FakeTicker ticker = new FakeTicker();
        final CircuitBreaker breaker = Resilience.circuitBreaker().withFailureThreshold(1).withCooldownMillis(100).withTicker(ticker);
        failure(breaker, BG, DOWN);
        ticker.advanceMillis(100);
        assertEquals("down", failure(breaker, BG, DOWN).detail(), "the probe is made, and fails");
        ticker.advanceMillis(99);
        assertFalse(letsThrough(breaker, "m"), "the cooldown began again with the probe");
        ticker.advanceMillis(1);
        assertTrue(letsThrough(breaker, "m"));
    }

    @Test
    void whileHalfOpenNoMoreCallsAreLetThroughThanThereAreProbes() throws Exception {
        final FakeTicker ticker = new FakeTicker();
        final CircuitBreaker breaker = Resilience.circuitBreaker().withFailureThreshold(1).withCooldownMillis(10)
                .withHalfOpenProbes(2).withTicker(ticker);
        failure(breaker, BG, DOWN);
        ticker.advanceMillis(10);

        // Two probes are under way at once; a third call is not let through.
        final CountDownLatch entered = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        final List<Thread> probes = new ArrayList<Thread>();
        for (int i = 0; i < 2; i++) {
            final Thread t = new Thread(() -> breaker.intercept("m", null, BG, (method, request, options) -> {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return null;
            }));
            probes.add(t);
            t.start();
        }
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        assertFalse(letsThrough(breaker, "m"), "a third call while two probes are under way");
        release.countDown();
        for (Thread t : probes) {
            t.join();
        }
        assertTrue(letsThrough(breaker, "m"), "both probes succeeded: closed");
        assertTrue(letsThrough(breaker, "m"));
    }

    @Test
    void whatIsThrownThatIsNotAFailureOfTheCallCountsAsOne() {
        // Also a probe that ends this way is recorded: a breaker that was not
        // told would stay half open, and let nothing through, for good.
        final FakeTicker ticker = new FakeTicker();
        final CircuitBreaker breaker = Resilience.circuitBreaker().withFailureThreshold(2).withCooldownMillis(10).withTicker(ticker);
        final ClientInterceptor.Invoker bug = (method, request, options) -> {
            throw new IllegalStateException("a bug");
        };
        assertThrows(IllegalStateException.class, () -> breaker.intercept("m", null, BG, bug));
        assertThrows(IllegalStateException.class, () -> breaker.intercept("m", null, BG, bug));
        assertFalse(letsThrough(breaker, "m"), "two of them open it");
        ticker.advanceMillis(10);
        assertThrows(IllegalStateException.class, () -> breaker.intercept("m", null, BG, bug), "the probe");
        assertFalse(letsThrough(breaker, "m"), "open again");
        ticker.advanceMillis(10);
        assertTrue(letsThrough(breaker, "m"), "and not stuck: the next probe is let through");
    }

    @Test
    void theRuleOfWhatCountsAsAFailureCanBeReplaced() {
        final CircuitBreaker breaker = Resilience.circuitBreaker().withFailureThreshold(2)
                .withIsFailure(error -> error.code() == Code.NOT_FOUND).withTicker(new FakeTicker());
        for (int i = 0; i < 6; i++) {
            failure(breaker, BG, DOWN);
        }
        assertTrue(letsThrough(breaker, "m"), "what the default counts is no longer counted");
        final Calls missing = Calls.of(fail(Code.NOT_FOUND));
        failure(breaker, BG, missing);
        failure(breaker, BG, missing);
        assertFalse(letsThrough(breaker, "m"));
        assertEquals(2, missing.count());
        assertThrows(NullPointerException.class, () -> Resilience.circuitBreaker().withIsFailure(null));
    }

    @Test
    void theFaultsOfTheCallerThatDoNotCount() {
        for (Code code : Code.values()) {
            if (code == Code.OK) {
                continue;
            }
            final CircuitBreaker breaker = Resilience.circuitBreaker().withFailureThreshold(1).withTicker(new FakeTicker());
            failure(breaker, BG, Calls.of(fail(code)));
            final boolean callersFault = code == Code.INVALID_ARGUMENT || code == Code.NOT_FOUND
                    || code == Code.UNAUTHENTICATED || code == Code.CANCELED;
            assertEquals(callersFault, letsThrough(breaker, "m"), code.toString());
        }
    }

    @Test
    void eachWithGivesABreakerThatKnowsNothingOfTheOther() {
        final FakeTicker ticker = new FakeTicker();
        final CircuitBreaker first = Resilience.circuitBreaker().withFailureThreshold(1).withTicker(ticker);
        failure(first, BG, DOWN);
        assertFalse(letsThrough(first, "m"));
        assertTrue(letsThrough(first.withCooldownMillis(5), "m"));
        assertTrue(letsThrough(Resilience.circuitBreaker(), "m"));
        assertThrows(IllegalArgumentException.class, () -> first.withCooldownMillis(-1));
        assertTrue(first.withHalfOpenProbes(0).toString().contains("probes=1"));
        assertTrue(first.withFailureThreshold(-2).toString().contains("threshold=1"));
    }

    @Test
    void aCooldownOfNothingProbesAtOnce() {
        final CircuitBreaker breaker = Resilience.circuitBreaker().withFailureThreshold(1).withCooldownMillis(0).withTicker(new FakeTicker());
        failure(breaker, BG, DOWN);
        assertEquals("down", failure(breaker, BG, DOWN).detail(), "the next call is the probe");
        assertTrue(letsThrough(breaker, "m"));
        assertEquals(Value.utf8("ok"), breaker.intercept("m", null, BG, OK));
    }

    // --- the chain the documentation recommends ---------------------------------------

    @Test
    void theBreakerSeesWholeOperationsAfterTheirRetries() {
        final FakeTicker ticker = new FakeTicker();
        final Calls calls = Calls.of(fail(Code.UNAVAILABLE));
        final ClientInterceptor.Invoker chain = ClientInterceptor.chain(calls, Arrays.asList(
                Resilience.circuitBreaker().withFailureThreshold(2).withTicker(ticker),
                Resilience.retry().withMaxAttempts(3).withJitter(false).withTicker(ticker)));
        assertThrows(VrpcException.class, () -> chain.invoke("m", null, BG));
        assertThrows(VrpcException.class, () -> chain.invoke("m", null, BG));
        assertEquals(6, calls.count(), "two operations of three attempts each");
        final VrpcException open = assertThrows(VrpcException.class, () -> chain.invoke("m", null, BG));
        assertEquals("circuit breaker open for \"m\"", open.detail());
        assertEquals(6, calls.count(), "the third is not made, and not tried again");
    }
}
