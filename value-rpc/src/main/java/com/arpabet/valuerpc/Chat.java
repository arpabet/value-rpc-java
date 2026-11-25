/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

/**
 * A stream in both directions at once, opened by one request.
 *
 * <pre>{@code
 * try (Chat chat = client.chat("support.chat", ticket, CallOptions.DEFAULT)) {
 *     chat.outgoing().send(Value.utf8("hello"));
 *     chat.outgoing().close();                 // this side has said all it has to say
 *     for (Value answer : chat.incoming()) {   // the peer may still answer
 *         show(answer);
 *     }
 * }
 * }</pre>
 *
 * Ending what this side sends does not end what the peer sends; the peer
 * ending what it sends ends the chat.
 */
public interface Chat extends AutoCloseable {

    /** What the peer sends. */
    ValueStream incoming();

    /** What this side sends. */
    ValueSink outgoing();

    /** The id of the request that opened the chat. */
    long requestId();

    /** Leaves the chat. One that has not ended is cancelled, which the peer is told. */
    @Override
    void close();
}
