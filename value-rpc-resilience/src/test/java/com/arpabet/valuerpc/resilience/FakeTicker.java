/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.resilience;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A clock a test moves by hand: waiting takes no time and moves it forward,
 * and what was waited is kept.
 */
final class FakeTicker implements Ticker {

    private long now = 1_000_000_007L;
    private final List<Long> sleeps = new ArrayList<Long>();

    @Override
    public synchronized long nanoTime() {
        return now;
    }

    @Override
    public synchronized void sleep(long nanos) throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        sleeps.add(nanos);
        now += Math.max(0, nanos);
    }

    synchronized void advanceMillis(long millis) {
        now += TimeUnit.MILLISECONDS.toNanos(millis);
    }

    synchronized void advanceNanos(long nanos) {
        now += nanos;
    }

    /** Every wait so far, in nanoseconds. */
    synchronized List<Long> sleeps() {
        return new ArrayList<Long>(sleeps);
    }

    /** Every wait so far, in milliseconds. */
    synchronized List<Long> sleepsMillis() {
        final List<Long> out = new ArrayList<Long>();
        for (long nanos : sleeps) {
            out.add(TimeUnit.NANOSECONDS.toMillis(nanos));
        }
        return out;
    }
}
