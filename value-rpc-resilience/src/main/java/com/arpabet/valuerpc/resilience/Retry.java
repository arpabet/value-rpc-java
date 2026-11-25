/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.resilience;

import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.ClientInterceptor;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.VrpcException;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

/**
 * Makes a failed call again, with a wait between the attempts that doubles
 * each time, while the failure is one worth another try and attempts are
 * left. It is the Go implementation's {@code resilience.Retry}.
 *
 * <p>By default: three attempts, the first included; a wait of 50 ms that
 * doubles up to 1 s, with jitter; and another try only after
 * {@link Code#UNAVAILABLE} and {@link Code#RESOURCE_EXHAUSTED}, the codes
 * that say the server did not process the request.
 *
 * <p>A timeout the caller gave with the options of the call is for the whole
 * of it: each attempt gets what is left, and when what is left does not reach
 * the next attempt the last failure is the failure of the call. A thread that
 * is interrupted while it waits between attempts gets the last failure too,
 * and stays interrupted.
 *
 * <p>Immutable: each {@code with} gives another.
 */
public final class Retry implements ClientInterceptor {

    static final Retry DEFAULT = new Retry(3, TimeUnit.MILLISECONDS.toNanos(50), TimeUnit.SECONDS.toNanos(1), true,
            Retry::unprocessed, Ticker.SYSTEM);

    private final int maxAttempts;
    private final long baseNanos;
    private final long maxNanos;
    private final boolean jitter;
    private final BiPredicate<String, VrpcException> retryable;
    private final Ticker ticker;

    private Retry(int maxAttempts, long baseNanos, long maxNanos, boolean jitter,
            BiPredicate<String, VrpcException> retryable, Ticker ticker) {
        this.maxAttempts = maxAttempts;
        this.baseNanos = baseNanos;
        this.maxNanos = maxNanos;
        this.jitter = jitter;
        this.retryable = retryable;
        this.ticker = ticker;
    }

    /** The default of what is worth another try: the server did not process the request. */
    private static boolean unprocessed(String method, VrpcException failure) {
        return failure.code() == Code.UNAVAILABLE || failure.code() == Code.RESOURCE_EXHAUSTED;
    }

    /**
     * With this many attempts in all, the first included; 3 by default. Less
     * than 1 is taken as 1: no retry.
     */
    public Retry withMaxAttempts(int attempts) {
        return new Retry(Math.max(1, attempts), baseNanos, maxNanos, jitter, retryable, ticker);
    }

    /**
     * With this wait between attempts, in milliseconds: {@code base} before
     * the second attempt, then twice the wait before, up to {@code max}. 50
     * and 1000 by default.
     *
     * @throws IllegalArgumentException a wait is negative
     */
    public Retry withBackoffMillis(long baseMillis, long maxMillis) {
        if (baseMillis < 0 || maxMillis < 0) {
            throw new IllegalArgumentException("backoff " + baseMillis + " ms to " + maxMillis + " ms");
        }
        return new Retry(maxAttempts, TimeUnit.MILLISECONDS.toNanos(baseMillis), TimeUnit.MILLISECONDS.toNanos(maxMillis),
                jitter, retryable, ticker);
    }

    /**
     * With or without jitter: with it, on by default, a wait is a random time
     * between half of it and all of it, so that clients that failed together
     * do not try again together.
     */
    public Retry withJitter(boolean on) {
        return new Retry(maxAttempts, baseNanos, maxNanos, on, retryable, ticker);
    }

    /**
     * With another rule for whether a failed call to a method is worth
     * another try. The default tries again only after
     * {@link Code#UNAVAILABLE} and {@link Code#RESOURCE_EXHAUSTED}: the
     * server did not process the request, so another try is safe whether or
     * not the call is idempotent. A rule that includes codes after which the
     * call may have run makes the retry at-least-once: keep it to idempotent
     * methods.
     */
    public Retry withRetryable(BiPredicate<String, VrpcException> retryable) {
        return new Retry(maxAttempts, baseNanos, maxNanos, jitter, Objects.requireNonNull(retryable, "retryable"), ticker);
    }

    /** With another clock: for a test. */
    Retry withTicker(Ticker ticker) {
        return new Retry(maxAttempts, baseNanos, maxNanos, jitter, retryable, ticker);
    }

    @Override
    public Value intercept(String method, Value request, CallOptions options, Invoker next) {
        final Budget budget = Budget.of(options, ticker);
        CallOptions attemptOptions = options;
        long delay = baseNanos;
        for (int attempt = 1;; attempt++) {
            final VrpcException failure;
            try {
                return next.invoke(method, request, attemptOptions);
            } catch (VrpcException e) {
                failure = e;
            }
            if (attempt >= maxAttempts || !retryable.test(method, failure)) {
                throw failure;
            }
            long wait = delay;
            if (jitter && wait > 0) {
                wait = wait / 2 + ThreadLocalRandom.current().nextLong(wait / 2 + 1);
            }
            // The caller's time is for the whole call. When it ends before the
            // wait does there is no next attempt, and nothing to wait for.
            if (budget.bounded() && budget.leftNanos() <= wait) {
                throw failure;
            }
            try {
                ticker.sleep(wait);
            } catch (InterruptedException e) {
                // The caller gave up: the last failure is what the call came to.
                Thread.currentThread().interrupt();
                throw failure;
            }
            if (budget.bounded()) {
                final long left = budget.leftNanos();
                if (left <= 0) {
                    throw failure;
                }
                attemptOptions = budget.optionsWith(left);
            }
            delay = delay > Long.MAX_VALUE / 2 ? Long.MAX_VALUE : delay * 2;
            if (delay > maxNanos) {
                delay = maxNanos;
            }
        }
    }

    @Override
    public String toString() {
        return "Retry[attempts=" + maxAttempts + ", backoff=" + TimeUnit.NANOSECONDS.toMillis(baseNanos) + ".."
                + TimeUnit.NANOSECONDS.toMillis(maxNanos) + " ms, jitter=" + jitter + "]";
    }
}
