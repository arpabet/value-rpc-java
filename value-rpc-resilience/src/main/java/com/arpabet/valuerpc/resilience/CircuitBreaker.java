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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Stops calling a method that keeps failing. Each method has a breaker of its
 * own: after a number of failures in a row it opens, and calls fail at once
 * with {@link Code#UNAVAILABLE} without being made; after a cooldown it lets
 * a few calls through as probes, and closes when all of them succeed, or
 * opens again when one fails. It is the Go implementation's
 * {@code resilience.CircuitBreaker}.
 *
 * <p>By default: 5 failures in a row open it, it stays open for 10 s, and one
 * probe decides. A failure is any failure but a fault of the caller
 * ({@link Code#INVALID_ARGUMENT}, {@link Code#NOT_FOUND},
 * {@link Code#UNAUTHENTICATED}, {@link Code#CANCELED}), which says the
 * request was bad, not that the peer is unwell.
 *
 * <p>Each {@code with} gives another breaker, which knows nothing of the
 * calls this one has seen.
 */
public final class CircuitBreaker implements ClientInterceptor {

    private enum State {
        /** Calls go through; failures are counted. */
        CLOSED,
        /** Calls fail at once, until the cooldown is over. */
        OPEN,
        /** A few calls go through, to find out whether the peer is back. */
        HALF_OPEN
    }

    private final int threshold;
    private final long cooldownNanos;
    private final int maxProbes;
    private final Predicate<VrpcException> isFailure;
    private final Ticker ticker;

    private final ConcurrentHashMap<String, Breaker> breakers = new ConcurrentHashMap<String, Breaker>();

    CircuitBreaker() {
        this(5, TimeUnit.SECONDS.toNanos(10), 1, CircuitBreaker::unwell, Ticker.SYSTEM);
    }

    private CircuitBreaker(int threshold, long cooldownNanos, int maxProbes, Predicate<VrpcException> isFailure,
            Ticker ticker) {
        this.threshold = threshold;
        this.cooldownNanos = cooldownNanos;
        this.maxProbes = maxProbes;
        this.isFailure = isFailure;
        this.ticker = ticker;
    }

    /** The default of what counts as a failure: anything but a fault of the caller. */
    private static boolean unwell(VrpcException failure) {
        final Code code = failure.code();
        return code != Code.INVALID_ARGUMENT && code != Code.NOT_FOUND && code != Code.UNAUTHENTICATED
                && code != Code.CANCELED;
    }

    /**
     * With this many failures in a row opening the breaker; 5 by default.
     * Less than 1 is taken as 1.
     */
    public CircuitBreaker withFailureThreshold(int failures) {
        return new CircuitBreaker(Math.max(1, failures), cooldownNanos, maxProbes, isFailure, ticker);
    }

    /**
     * With the breaker staying open this long before it lets probes through,
     * in milliseconds; 10 000 by default.
     *
     * @throws IllegalArgumentException the cooldown is negative
     */
    public CircuitBreaker withCooldownMillis(long millis) {
        if (millis < 0) {
            throw new IllegalArgumentException("cooldown " + millis + " ms");
        }
        return new CircuitBreaker(threshold, TimeUnit.MILLISECONDS.toNanos(millis), maxProbes, isFailure, ticker);
    }

    /**
     * With this many probes let through once the cooldown is over; 1 by
     * default. The breaker closes only when all of them succeed. Less than 1
     * is taken as 1.
     */
    public CircuitBreaker withHalfOpenProbes(int probes) {
        return new CircuitBreaker(threshold, cooldownNanos, Math.max(1, probes), isFailure, ticker);
    }

    /**
     * With another rule for whether a failed call counts as a failure for the
     * breaker. The default counts any failure but a fault of the caller:
     * {@link Code#INVALID_ARGUMENT}, {@link Code#NOT_FOUND},
     * {@link Code#UNAUTHENTICATED} and {@link Code#CANCELED}.
     */
    public CircuitBreaker withIsFailure(Predicate<VrpcException> isFailure) {
        return new CircuitBreaker(threshold, cooldownNanos, maxProbes, Objects.requireNonNull(isFailure, "isFailure"),
                ticker);
    }

    /** With another clock: for a test. */
    CircuitBreaker withTicker(Ticker ticker) {
        return new CircuitBreaker(threshold, cooldownNanos, maxProbes, isFailure, ticker);
    }

    @Override
    public Value intercept(String method, Value request, CallOptions options, Invoker next) {
        final Breaker breaker = breakers.computeIfAbsent(method, m -> new Breaker());
        if (!breaker.allow()) {
            throw new VrpcException(Code.UNAVAILABLE, "circuit breaker open for " + Resilience.quoted(method));
        }
        // Whatever ends the call is recorded: a probe that was let through and
        // never reported would hold the breaker half open for good. What is
        // thrown that is not a failure of the call counts as one.
        boolean failed = true;
        try {
            final Value result = next.invoke(method, request, options);
            failed = false;
            return result;
        } catch (VrpcException e) {
            failed = isFailure.test(e);
            throw e;
        } finally {
            breaker.record(failed);
        }
    }

    /** The breaker of one method. */
    private final class Breaker {
        private State state = State.CLOSED;
        /** Failures in a row while closed. */
        private int consecutive;
        private long openedAt;
        // While half open: how many probes were let through, which caps how
        // many may run at once, and how many of them succeeded. They are two
        // counts, as in the Go implementation: one count for both would close
        // the breaker after a single success, whatever the number of probes.
        private int admitted;
        private int succeeded;

        /** Whether a call may be made; moves from open to half open once the cooldown is over. */
        synchronized boolean allow() {
            if (state == State.OPEN) {
                if (ticker.nanoTime() - openedAt >= cooldownNanos) {
                    state = State.HALF_OPEN;
                    admitted = 1;
                    succeeded = 0;
                    return true;
                }
                return false;
            }
            if (state == State.HALF_OPEN) {
                if (admitted < maxProbes) {
                    admitted++;
                    return true;
                }
                return false;
            }
            return true;
        }

        /** Takes the outcome of a call into the state. */
        synchronized void record(boolean failed) {
            if (state == State.CLOSED) {
                if (!failed) {
                    consecutive = 0;
                } else if (++consecutive >= threshold) {
                    trip();
                }
            } else if (state == State.HALF_OPEN) {
                if (failed) {
                    trip(); // one probe that fails opens it again at once
                } else if (++succeeded >= maxProbes) {
                    state = State.CLOSED;
                    consecutive = 0;
                }
            }
            // Open: a call that was let through before it opened; it changes nothing.
        }

        private void trip() {
            state = State.OPEN;
            openedAt = ticker.nanoTime();
            admitted = 0;
            succeeded = 0;
        }
    }

    @Override
    public String toString() {
        return "CircuitBreaker[threshold=" + threshold + ", cooldown=" + TimeUnit.NANOSECONDS.toMillis(cooldownNanos)
                + " ms, probes=" + maxProbes + "]";
    }
}
