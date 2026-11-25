/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.server;

import com.arpabet.value.MapValue;

/**
 * Told what happens to the connections of a server, for a log: the library
 * writes none of its own. Every method is called on a thread of the server
 * and must not wait; they do nothing unless overridden. It stands where the
 * Go server takes a logger.
 */
public interface ServerListener {

    /**
     * A client was let in: its handshake was accepted.
     *
     * @param principal who it is, as the authenticator said; empty for nobody in particular
     * @param resumed   whether it is a session the server knew, on a new connection
     */
    default void onConnected(long clientId, String principal, String remoteAddress, boolean resumed) {
    }

    /**
     * The connection of a client that was let in is gone. Its requests were
     * cancelled, and its session is kept for a while, in case it reconnects.
     *
     * @param cause why; null when the client or the server closed it
     */
    default void onDisconnected(long clientId, String remoteAddress, Throwable cause) {
    }

    /**
     * A connection was closed before it was let in: there were too many, its
     * handshake was not one or did not come in time, or the authorizer or the
     * authenticator refused it.
     *
     * @param cause what the authorizer or the authenticator threw, or null
     */
    default void onRefused(String remoteAddress, String reason, Throwable cause) {
    }

    /** A client sent a message that is not one, or that belongs to nothing. It was dropped. */
    default void onProtocolError(String remoteAddress, MapValue message, String problem) {
    }

    /**
     * A handler threw something that is not a failure with a code: the caller
     * was told {@code INTERNAL}, and this is where the error itself goes.
     *
     * @param what the handler, as the caller is told: {@code function greet}
     */
    default void onHandlerFailure(String what, Throwable error) {
    }
}
