/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.resilience;

import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.ClientInterceptor;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.VrpcException;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * The policies: each call here gives an interceptor for the client's builder.
 * See the {@linkplain com.arpabet.valuerpc.resilience package} for how they
 * go together.
 */
public final class Resilience {

    private Resilience() {
    }

    /**
     * Makes a failed call again, with a wait that doubles between the
     * attempts: three attempts, 50 ms to 1 s with jitter, after
     * {@link Code#UNAVAILABLE} and {@link Code#RESOURCE_EXHAUSTED}, unless
     * its {@code with} methods say otherwise.
     */
    public static Retry retry() {
        return Retry.DEFAULT;
    }

    /**
     * Stops calling a method that keeps failing, and finds out after a while
     * whether it is back: opens after 5 failures in a row, for 10 s, and one
     * probe decides, unless its {@code with} methods say otherwise. A breaker
     * of its own each time this is called.
     */
    public static CircuitBreaker circuitBreaker() {
        return new CircuitBreaker();
    }

    /**
     * Bounds each call that goes through it, or each attempt when it is
     * inside a retry: the call may take this long, in place of the timeout of
     * the client. A call whose own options give it less keeps that. The
     * timeout is sent to the server as the call's deadline, so a handler that
     * cooperates sees it too.
     *
     * @throws IllegalArgumentException the timeout is not positive
     */
    public static ClientInterceptor timeoutMillis(long millis) {
        if (millis <= 0) {
            throw new IllegalArgumentException("timeout " + millis + " ms");
        }
        return (method, request, options, next) -> {
            final long own = options.timeoutMillis();
            return next.invoke(method, request, own > 0 && own <= millis ? options : options.withTimeoutMillis(millis));
        };
    }

    /**
     * Answers a call that failed with what the fallback gives instead: a
     * cached or a default value, or another failure.
     */
    public static ClientInterceptor fallback(Fallback fallback) {
        Objects.requireNonNull(fallback, "fallback");
        return (method, request, options, next) -> {
            final VrpcException failure;
            try {
                return next.invoke(method, request, options);
            } catch (VrpcException e) {
                failure = e;
            }
            return fallback.apply(method, request, options, failure);
        };
    }

    /**
     * Lets no more than this many calls through at once, so that one slow
     * dependency does not take every thread and connection there is. A call
     * waits for a place, in the order the calls came. With a timeout of its
     * own it waits no longer than that, and what it waited is taken from the
     * time the call may take; a call whose thread is interrupted stops
     * waiting. Either fails with {@link Code#RESOURCE_EXHAUSTED}. Less than 1
     * is taken as 1.
     */
    public static ClientInterceptor bulkhead(int maxConcurrent) {
        return bulkhead(maxConcurrent, Ticker.SYSTEM);
    }

    static ClientInterceptor bulkhead(int maxConcurrent, Ticker ticker) {
        // Fair: a place goes to the call that has waited longest.
        final Semaphore places = new Semaphore(Math.max(1, maxConcurrent), true);
        return (method, request, options, next) -> {
            CallOptions callOptions = options;
            // A place that is free is taken without waiting, which a thread
            // that is interrupted can do too: its call is then what fails,
            // as canceled, and not as a bulkhead that is full.
            if (places.hasQueuedThreads() || !places.tryAcquire()) {
                final Budget budget = Budget.of(options, ticker);
                try {
                    if (!budget.bounded()) {
                        places.acquire();
                    } else if (!places.tryAcquire(Math.max(0, budget.leftNanos()), TimeUnit.NANOSECONDS)) {
                        throw full(method, "deadline exceeded", null);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw full(method, "canceled", e);
                }
                if (budget.bounded()) {
                    final long left = budget.leftNanos();
                    if (left <= 0) {
                        places.release();
                        throw full(method, "deadline exceeded", null);
                    }
                    callOptions = budget.optionsWith(left);
                }
            }
            try {
                return next.invoke(method, request, callOptions);
            } finally {
                places.release();
            }
        };
    }

    private static VrpcException full(String method, String why, Throwable cause) {
        return new VrpcException(Code.RESOURCE_EXHAUSTED, "bulkhead full for " + quoted(method) + ": " + why, cause);
    }

    /**
     * Lets this many calls a second through, and a burst of up to
     * {@code burst} at once. A call over the limit fails at once with
     * {@link Code#RESOURCE_EXHAUSTED}: load is shed, not queued. A burst of
     * less than 1 is taken as 1.
     *
     * @throws IllegalArgumentException the rate is negative or not a number
     */
    public static ClientInterceptor rateLimit(double perSecond, int burst) {
        return rateLimit(perSecond, burst, Ticker.SYSTEM);
    }

    static ClientInterceptor rateLimit(double perSecond, int burst, Ticker ticker) {
        if (!(perSecond >= 0) || Double.isInfinite(perSecond)) {
            throw new IllegalArgumentException("rate " + perSecond + " a second");
        }
        final TokenBucket bucket = new TokenBucket(perSecond, Math.max(1, burst), ticker);
        return (method, request, options, next) -> {
            if (!bucket.allow()) {
                throw new VrpcException(Code.RESOURCE_EXHAUSTED, "rate limit exceeded for " + quoted(method));
            }
            return next.invoke(method, request, options);
        };
    }

    /**
     * A bucket of tokens, one for each call, that fills at a steady rate up
     * to its size; it is filled when it is asked, for the time since it was
     * asked last.
     */
    private static final class TokenBucket {
        private final double perSecond;
        private final double max;
        private final Ticker ticker;
        private double tokens;
        private long last;

        TokenBucket(double perSecond, int burst, Ticker ticker) {
            this.perSecond = perSecond;
            this.max = burst;
            this.ticker = ticker;
            this.tokens = burst;
            this.last = ticker.nanoTime();
        }

        synchronized boolean allow() {
            final long now = ticker.nanoTime();
            tokens += (now - last) / 1e9 * perSecond;
            if (tokens > max) {
                tokens = max;
            }
            last = now;
            if (tokens >= 1) {
                tokens--;
                return true;
            }
            return false;
        }
    }

    /** The name of a method as a failure names it, as the Go implementation's {@code %q}. */
    static String quoted(String method) {
        return '"' + method + '"';
    }
}
