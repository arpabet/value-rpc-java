/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.Value;

/**
 * The values this side sends a peer on a stream, given one after another. It
 * stands where the Go implementation takes a channel to drain.
 *
 * <pre>{@code
 * try (ValueSink lines = client.putStream("logs.upload", null, CallOptions.DEFAULT)) {
 *     for (String line : log) {
 *         lines.send(Value.utf8(line));
 *     }
 * }                                    // closing ends the stream
 * }</pre>
 *
 * {@link #send} waits while the peer has no room: the peer grants credit for
 * as many values as it can take, and more as its application takes them. A
 * sender that is fast is slowed down to its reader, and nothing is lost.
 */
public interface ValueSink extends AutoCloseable {

    /**
     * Sends a value, waiting until the peer has room for it.
     *
     * @param value not null, and not the null value: a stream does not carry it
     * @throws VrpcException the stream is gone: it was closed or cancelled,
     *         the peer failed it, or the connection was lost
     */
    void send(Value value) throws InterruptedException;

    /** The id of the request that opened the stream. */
    long requestId();

    /**
     * Ends the stream: the peer sees its end after the values sent. Closing
     * twice is closing once.
     */
    @Override
    void close();

    /** Gives the stream up, which the peer is told: what it has not read yet may be dropped. */
    void cancel();
}
