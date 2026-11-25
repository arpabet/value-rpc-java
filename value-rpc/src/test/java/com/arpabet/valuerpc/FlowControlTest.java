/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.value.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The two ends of the flow control of a stream: the buffer of the receiver
 * (the tests of pump_test.go) and the gate of the sender.
 */
@Timeout(20)
class FlowControlTest {

    @Test
    void theBufferGivesValuesInOrderAndThenTheEnd() throws Exception {
        final AtomicInteger taken = new AtomicInteger();
        final StreamBuffer buf = new StreamBuffer(100, taken::incrementAndGet);
        for (int i = 0; i < 50; i++) {
            assertTrue(buf.offer(Value.ofLong(i)));
        }
        buf.end();
        assertFalse(buf.offer(Value.ofLong(99)), "nothing goes in after the end");
        assertFalse(buf.finished(), "what is queued is still to be taken");
        for (int i = 0; i < 50; i++) {
            assertEquals(Value.ofLong(i), buf.take());
        }
        assertNull(buf.take());
        assertNull(buf.take(), "the end stays the end");
        assertTrue(buf.finished());
        assertEquals(50, taken.get(), "each value taken is reported once, the end is not");
        assertFalse(buf.overflowed());
    }

    @Test
    void puttingInNeverWaitsForASlowReader() throws Exception {
        // The thread that reads the connection must not be held by one stream.
        final StreamBuffer buf = new StreamBuffer(1000, null);
        final long start = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            assertTrue(buf.offer(Value.ofLong(i)));
        }
        assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5));
        assertEquals(1000, buf.size());
    }

    @Test
    void aBufferThatIsFilledEndsTheStream() throws Exception {
        final StreamBuffer buf = new StreamBuffer(3, null);
        assertTrue(buf.offer(Value.ofLong(1)));
        assertTrue(buf.offer(Value.ofLong(2)));
        assertTrue(buf.offer(Value.ofLong(3)));
        assertFalse(buf.overflowed());
        assertFalse(buf.offer(Value.ofLong(4)), "the sender sent more than it had credit for");
        assertTrue(buf.overflowed());
        assertFalse(buf.offer(Value.ofLong(5)));
        // What was taken in is still given out, then the end.
        assertEquals(Value.ofLong(1), buf.take());
        assertEquals(Value.ofLong(2), buf.take());
        assertEquals(Value.ofLong(3), buf.take());
        assertNull(buf.take());
    }

    @Test
    void stoppingDropsWhatIsQueuedAndWakesTheReader() throws Exception {
        final StreamBuffer buf = new StreamBuffer(10, null);
        buf.offer(Value.ofLong(1));
        buf.stop();
        assertNull(buf.take());
        assertTrue(buf.finished());
        assertFalse(buf.offer(Value.ofLong(2)));

        final StreamBuffer waiting = new StreamBuffer(10, null);
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicBoolean gotEnd = new AtomicBoolean();
        final Thread reader = new Thread(() -> {
            try {
                gotEnd.set(waiting.take() == null);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            done.countDown();
        });
        reader.start();
        Thread.sleep(50);
        waiting.stop();
        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertTrue(gotEnd.get());
    }

    @Test
    void aReaderWaitsForAValueNoLongerThanItSays() throws Exception {
        final StreamBuffer buf = new StreamBuffer(10, null);
        assertThrows(TimeoutException.class, () -> buf.take(TimeUnit.MILLISECONDS.toNanos(30)));
        new Thread(() -> {
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                return;
            }
            buf.offer(Value.utf8("late"));
        }).start();
        assertEquals(Value.utf8("late"), buf.take(TimeUnit.SECONDS.toNanos(5)));
        buf.end();
        assertNull(buf.take(TimeUnit.SECONDS.toNanos(5)), "the end is not a timeout");
    }

    @Test
    void manyValuesCrossBetweenTwoThreadsInOrder() throws Exception {
        final int n = 20000;
        final StreamBuffer buf = new StreamBuffer(64, null);
        final CreditGate gate = new CreditGate();
        gate.grant(64);
        final List<Long> got = new ArrayList<Long>();
        final Thread reader = new Thread(() -> {
            try {
                for (Value v = buf.take(); v != null; v = buf.take()) {
                    got.add(((com.arpabet.value.NumberValue) v).longValue());
                    gate.grant(1); // the receiver grants as its application takes
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        reader.start();
        for (int i = 0; i < n; i++) {
            assertTrue(gate.acquire());
            assertTrue(buf.offer(Value.ofLong(i)), "a sender that keeps to its credit never fills the buffer");
        }
        buf.end();
        reader.join(15000);
        assertEquals(n, got.size());
        for (int i = 0; i < n; i++) {
            assertEquals(i, got.get(i).longValue());
        }
        assertFalse(buf.overflowed());
    }

    @Test
    void theGateHoldsASenderUntilCreditIsGranted() throws Exception {
        final CreditGate gate = new CreditGate();
        final AtomicInteger sent = new AtomicInteger();
        final Thread sender = new Thread(() -> {
            try {
                while (gate.acquire()) {
                    sent.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        sender.start();
        Thread.sleep(50);
        assertEquals(0, sent.get(), "no credit, nothing sent");
        gate.grant(3);
        waitFor(() -> sent.get() == 3);
        Thread.sleep(30);
        assertEquals(3, sent.get(), "and no more than was granted");
        gate.grant(0);
        gate.grant(-5);
        assertEquals(0, gate.available());
        gate.grant(2);
        waitFor(() -> sent.get() == 5);
        gate.close();
        sender.join(5000);
        assertFalse(sender.isAlive(), "closing the gate lets a waiting sender go");
        assertFalse(gate.acquire(), "and it stays closed");
        assertTrue(gate.isClosed());
    }

    private static void waitFor(java.util.function.BooleanSupplier what) throws InterruptedException {
        final long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!what.getAsBoolean()) {
            if (System.nanoTime() > until) {
                throw new AssertionError("waited too long");
            }
            Thread.sleep(2);
        }
    }
}
