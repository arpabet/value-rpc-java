/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.resilience;

import com.arpabet.valuerpc.CallOptions;
import java.util.concurrent.TimeUnit;

/**
 * The time a caller gave a call with its options, as a policy that waits or
 * calls more than once has to see it: a deadline, counted from when the call
 * reached the policy. It is what the deadline of a {@code context.Context}
 * is to the Go implementation.
 *
 * <p>Only a timeout the caller gave bounds anything here. Without one each
 * call the policy makes has the timeout of the client, and with none at all
 * it has none: neither is known to an interceptor, and neither is a deadline
 * for the whole.
 */
final class Budget {

    private static final long NANOS_PER_MILLI = 1_000_000L;

    private final CallOptions options;
    private final Ticker ticker;
    private final boolean bounded;
    private final long deadline;

    private Budget(CallOptions options, Ticker ticker) {
        this.options = options;
        this.ticker = ticker;
        final long millis = options.timeoutMillis();
        // A timeout too long to count in nanoseconds bounds nothing in practice.
        this.bounded = millis > 0 && millis < Long.MAX_VALUE / (4 * NANOS_PER_MILLI);
        this.deadline = bounded ? ticker.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis) : 0;
    }

    /** The budget of a call that reaches a policy now. */
    static Budget of(CallOptions options, Ticker ticker) {
        return new Budget(options, ticker);
    }

    /** Whether the caller gave the call a timeout. */
    boolean bounded() {
        return bounded;
    }

    /** What is left of a bounded budget, in nanoseconds; not positive when it is used up. */
    long leftNanos() {
        return deadline - ticker.nanoTime();
    }

    /**
     * The options for a call made with this much left: the caller's, with
     * what is left as the timeout. Rounded up to a millisecond, since a
     * timeout of 0 would mean the timeout of the client.
     */
    CallOptions optionsWith(long leftNanos) {
        final long millis = Math.max(1, (leftNanos + NANOS_PER_MILLI - 1) / NANOS_PER_MILLI);
        return millis >= options.timeoutMillis() ? options : options.withTimeoutMillis(millis);
    }
}
