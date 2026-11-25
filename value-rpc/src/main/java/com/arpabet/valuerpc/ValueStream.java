/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.Value;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The values a peer sends on a stream, to be taken one after another. It
 * stands where the Go implementation gives a channel.
 *
 * <pre>{@code
 * try (ValueStream events = client.getStream("events.tail", topic, CallOptions.DEFAULT)) {
 *     for (Value event : events) {
 *         handle(event);
 *     }
 * }
 * }</pre>
 *
 * Taking a value is what lets the peer send another: the sender has credit
 * for as many values as this side has room for, and gets more as they are
 * taken. A reader that is slow slows its sender down and loses nothing.
 *
 * <p>A stream is read by one thread at a time.
 */
public interface ValueStream extends Iterable<Value>, AutoCloseable {

    /**
     * Takes the next value, waiting for it.
     *
     * @return the value, or null when the stream has ended
     * @throws VrpcException the stream failed: the peer said so, or the connection was lost
     */
    Value take() throws InterruptedException;

    /**
     * Takes the next value, waiting for it no longer than the time given.
     *
     * @return the value, or null when the stream has ended
     * @throws TimeoutException no value came and the stream did not end in that time
     * @throws VrpcException    the stream failed
     */
    Value take(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException;

    /** The id of the request that opened the stream. */
    long requestId();

    /**
     * Stops reading. A stream that has not ended is cancelled, which the peer
     * is told; one that has ended is left as it is.
     */
    @Override
    void close();

    /**
     * The values as an iteration, for a {@code for} loop. A thread that is
     * interrupted while it waits for a value cancels the stream and gets a
     * {@link VrpcException} with {@link Code#CANCELED}; it stays interrupted.
     */
    @Override
    default Iterator<Value> iterator() {
        return new Iterator<Value>() {
            private Value next;
            private boolean ended;

            @Override
            public boolean hasNext() {
                if (next != null) {
                    return true;
                }
                if (ended) {
                    return false;
                }
                try {
                    next = take();
                } catch (InterruptedException e) {
                    close();
                    Thread.currentThread().interrupt();
                    throw new VrpcException(Code.CANCELED, "interrupted while reading a stream", e);
                }
                ended = next == null;
                return !ended;
            }

            @Override
            public Value next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                final Value v = next;
                next = null;
                return v;
            }
        };
    }
}
