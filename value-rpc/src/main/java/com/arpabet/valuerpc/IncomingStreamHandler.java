/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.Value;

/**
 * Serves a stream from the peer: what a peer opens with
 * {@link Peer#putStream}. It is the Go implementation's
 * {@code valuerpc.IncomingStream}.
 *
 * <pre>{@code
 * server.addIncomingStream("upload", TypeDef.ANY, (context, args, in) -> {
 *     for (Value line : in) {
 *         store(line);
 *     }
 * });
 * }</pre>
 *
 * The handler stays for as long as it reads, on a thread of its own. The peer
 * is told that the stream is taken when the handler first asks for a value,
 * or returns; a handler that throws before that refuses the stream. The peer
 * sends only what it was granted credit for, and is granted more as the
 * handler takes values, so a handler that is slow slows its sender down.
 *
 * <p>A handler reads to the end of the stream. One that returns before the
 * end gives up what the peer has not sent yet, and the peer is told to stop.
 * When the peer cancels the stream, or the connection is lost,
 * {@link ValueStream#take} throws a {@link VrpcException} that says which,
 * and the thread is interrupted.
 */
@FunctionalInterface
public interface IncomingStreamHandler {

    /**
     * @param args the arguments; the null value when the caller sent none
     * @param in   what the peer sends
     * @throws Exception the stream failed, which the peer is told while it
     *         still sends: a {@link VrpcException} chooses the code, any
     *         other failure is {@link Code#INTERNAL}
     */
    void handle(RequestContext context, Value args, ValueStream in) throws Exception;
}
