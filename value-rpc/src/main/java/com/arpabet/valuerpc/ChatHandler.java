/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.Value;

/**
 * Serves a stream in both directions at once: what a peer opens with
 * {@link Peer#chat}. It is the Go implementation's {@code valuerpc.Chat}.
 *
 * <pre>{@code
 * server.addChat("chat.echo", TypeDef.ANY, (context, args, in, out) -> {
 *     for (Value said : in) {
 *         out.send(Value.utf8("echo:" + said));
 *     }
 * });                                  // returning ends the chat
 * }</pre>
 *
 * The handler stays for as long as the chat lasts, on a thread of its own.
 * The peer ending what it sends ends {@code in} and nothing else: the handler
 * may still send. The chat ends when the handler returns, or closes
 * {@code out}; what the peer has not sent by then is not read.
 */
@FunctionalInterface
public interface ChatHandler {

    /**
     * @param args the arguments; the null value when the caller sent none
     * @param in   what the peer sends
     * @param out  what is sent to the peer
     * @throws Exception the chat failed, which the peer is told: a
     *         {@link VrpcException} chooses the code, any other failure is
     *         {@link Code#INTERNAL}
     */
    void handle(RequestContext context, Value args, ValueStream in, ValueSink out) throws Exception;
}
