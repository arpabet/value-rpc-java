/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The sender's side of the flow control of a stream. A sender acquires one
 * credit before each value it sends, and waits when it has none until the
 * receiver grants more. The receiver grants only as its application takes
 * values, so a sender never outruns a receiver: nothing is lost, nothing is
 * buffered without bound, and only the thread that sends this one stream
 * waits. It is the Go implementation's {@code valuerpc.CreditGate}.
 */
public final class CreditGate {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private long credit;
    private boolean closed;

    /**
     * Waits for a credit and takes it.
     *
     * @return false when the gate was closed: the stream is gone
     */
    public boolean acquire() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (credit <= 0 && !closed) {
                changed.await();
            }
            if (closed) {
                return false;
            }
            credit--;
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Adds credits and wakes a sender that waits for one. Not positive adds nothing. */
    public void grant(long n) {
        if (n <= 0) {
            return;
        }
        lock.lock();
        try {
            credit += n;
            if (credit < 0) {
                credit = Long.MAX_VALUE;
            }
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Closes the gate for good: a sender that waits, and any later one, gets false. */
    public void close() {
        lock.lock();
        try {
            closed = true;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** The credits not taken yet. */
    public long available() {
        lock.lock();
        try {
            return credit;
        } finally {
            lock.unlock();
        }
    }

    public boolean isClosed() {
        lock.lock();
        try {
            return closed;
        } finally {
            lock.unlock();
        }
    }
}
