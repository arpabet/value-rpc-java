/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.client;

import com.arpabet.value.MapValue;

/**
 * Told what happens to the connection of a client. Every method is called on
 * a thread of the client and must not wait; they do nothing unless
 * overridden. It does the work of the Go implementation's
 * {@code ConnectionHandler} and {@code ErrorHandler}.
 */
public interface ClientListener {

    /** The server accepted the handshake: the client is connected, for the first time or again. */
    default void onConnected(MapValue handshakeResponse) {
    }

    /**
     * The connection was lost. The calls that waited on it have failed, and
     * the client connects again.
     *
     * @param cause why, or null when the peer closed it
     */
    default void onDisconnected(Throwable cause) {
    }

    /** The peer sent a message that is not one, or that answers nothing. It was dropped. */
    default void onProtocolError(MapValue message, String problem) {
    }

    /**
     * A handler of this client failed with something that is not an answer
     * while it served the server: the server was told {@code INTERNAL}.
     *
     * @param what the handler, as its caller is told: {@code function notify}
     */
    default void onHandlerFailure(String what, Throwable error) {
    }
}
