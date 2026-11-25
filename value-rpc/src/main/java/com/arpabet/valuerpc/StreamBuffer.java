/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.Value;
import java.util.ArrayDeque;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The receiver's side of a stream: the values that have arrived and that the
 * application has not taken yet. The thread that reads the connection puts
 * values in and never waits, so a slow reader of one stream holds up no other
 * request on the connection; the application takes them out at its own pace,
 * on its own thread, and each value it takes is reported, which is when the
 * sender is granted credit for another.
 *
 * <p>It holds no more than its capacity. A sender that keeps to its credit
 * never fills it; one that does not has the stream failed, rather than
 * memory. It does the work of the Go implementation's {@code StreamPump} and
 * of the channel that pump fills, without a thread of its own.
 */
public final class StreamBuffer {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final ArrayDeque<Value> queue = new ArrayDeque<Value>();
    private final int capacity;
    private final Runnable onTake;

    /** No more values will be put in; what is queued is still to be taken. */
    private boolean ended;
    /** Abandoned: nothing more is given out. */
    private boolean stopped;
    private boolean overflowed;

    /**
     * @param capacity the most values held at once
     * @param onTake   called once for each value the application takes, on
     *                 its thread; may be null
     */
    public StreamBuffer(int capacity, Runnable onTake) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity " + capacity);
        }
        this.capacity = capacity;
        this.onTake = onTake;
    }

    /**
     * Puts a value in, without waiting.
     *
     * @return false when the value was not taken in: the stream has ended or
     *         was stopped, or the buffer is full, which ends it (see
     *         {@link #overflowed})
     */
    public boolean offer(Value value) {
        lock.lock();
        try {
            if (ended || stopped) {
                return false;
            }
            if (queue.size() >= capacity) {
                overflowed = true;
                ended = true;
                changed.signalAll();
                return false;
            }
            queue.addLast(value);
            changed.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** No more values will come: the application takes what is queued, then sees the end. */
    public void end() {
        lock.lock();
        try {
            ended = true;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Abandons the stream: what is queued is dropped and the application sees the end at once. */
    public void stop() {
        lock.lock();
        try {
            stopped = true;
            ended = true;
            queue.clear();
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Whether the buffer was filled, by a sender that sent more than it had credit for. */
    public boolean overflowed() {
        lock.lock();
        try {
            return overflowed;
        } finally {
            lock.unlock();
        }
    }

    /** Whether everything has been taken and nothing more will come. */
    public boolean finished() {
        lock.lock();
        try {
            return stopped || (ended && queue.isEmpty());
        } finally {
            lock.unlock();
        }
    }

    /** The number of values waiting to be taken. */
    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    /** Takes the next value, waiting for it. Null at the end of the stream. */
    public Value take() throws InterruptedException {
        try {
            return take(0);
        } catch (TimeoutException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Takes the next value, waiting for it no longer than the time given.
     *
     * @param nanos how long to wait; not positive waits without a limit
     * @return the value, or null at the end of the stream
     * @throws TimeoutException when no value came and the stream did not end in that time
     */
    public Value take(long nanos) throws InterruptedException, TimeoutException {
        final Value v;
        lock.lockInterruptibly();
        try {
            long left = nanos;
            while (queue.isEmpty() && !ended && !stopped) {
                if (nanos <= 0) {
                    changed.await();
                } else {
                    if (left <= 0) {
                        throw new TimeoutException();
                    }
                    left = changed.awaitNanos(left);
                }
            }
            if (stopped || queue.isEmpty()) {
                return null;
            }
            v = queue.pollFirst();
        } finally {
            lock.unlock();
        }
        if (onTake != null) {
            onTake.run();
        }
        return v;
    }
}
