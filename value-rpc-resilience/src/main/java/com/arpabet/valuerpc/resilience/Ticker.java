/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.resilience;

import java.util.concurrent.TimeUnit;

/**
 * Time as the policies see it: what they measure with and wait by, so that a
 * test can stand in for it and need not wait.
 */
interface Ticker {

    /** A time that only goes forward, in nanoseconds, to take differences of. */
    long nanoTime();

    /** Waits; not positive for no wait. */
    void sleep(long nanos) throws InterruptedException;

    /** The clock of the system. */
    Ticker SYSTEM = new Ticker() {
        @Override
        public long nanoTime() {
            return System.nanoTime();
        }

        @Override
        public void sleep(long nanos) throws InterruptedException {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            if (nanos > 0) {
                TimeUnit.NANOSECONDS.sleep(nanos);
            }
        }
    };
}
