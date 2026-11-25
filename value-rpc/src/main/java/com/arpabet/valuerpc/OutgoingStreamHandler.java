/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.Value;

/**
 * Serves a stream to the peer: what a peer opens with
 * {@link Peer#getStream}. It is the Go implementation's
 * {@code valuerpc.OutgoingStream}.
 *
 * <pre>{@code
 * server.addOutgoingStream("count", TypeDef.ANY, (context, args, out) -> {
 *     for (long i = 1; i <= 10; i++) {
 *         out.send(Value.ofLong(i));
 *     }
 * });                                  // returning ends the stream
 * }</pre>
 *
 * Where the Go handler returns a channel and leaves a goroutine to fill it,
 * this handler stays for as long as the stream lasts, on a thread of its own,
 * and the stream ends when it returns. The peer is told that the stream is
 * taken when the handler first sends, or returns; a handler that throws
 * before that refuses the stream, and the peer's request fails with what it
 * threw. {@link ValueSink#send} waits while the peer has no room. Once the
 * peer has cancelled the stream, or the connection is lost, it throws a
 * {@link VrpcException} that says which, and the thread is interrupted.
 */
@FunctionalInterface
public interface OutgoingStreamHandler {

    /**
     * @param args the arguments; the null value when the caller sent none
     * @param out  what is sent to the peer
     * @throws Exception the stream failed, which the peer is told: a
     *         {@link VrpcException} chooses the code, any other failure is
     *         {@link Code#INTERNAL}
     */
    void handle(RequestContext context, Value args, ValueSink out) throws Exception;
}
