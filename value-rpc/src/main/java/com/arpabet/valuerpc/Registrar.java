/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

/**
 * What an end of a connection serves its peer, by name: calls, and the three
 * kinds of stream. It is the Go implementation's {@code valuerpc.Registrar}.
 *
 * <p>What a peer sends is checked against the {@link TypeDef} a name was
 * registered with before the handler sees it, and a call's result against
 * its own before the caller does; {@link TypeDef#ANY} lets anything through.
 * A name serves one pattern: a peer that asks for it by another is refused.
 */
public interface Registrar {

    /**
     * Serves a call.
     *
     * @param args   what the arguments must be
     * @param result what the result must be
     * @throws IllegalStateException the name is taken, where an end does not replace
     */
    void addFunction(String name, TypeDef args, TypeDef result, UnaryHandler handler);

    /** Serves a call that takes and gives anything. */
    default void addFunction(String name, UnaryHandler handler) {
        addFunction(name, TypeDef.ANY, TypeDef.ANY, handler);
    }

    /** Serves a stream to the peer. */
    void addOutgoingStream(String name, TypeDef args, OutgoingStreamHandler handler);

    /** Serves a stream from the peer. */
    void addIncomingStream(String name, TypeDef args, IncomingStreamHandler handler);

    /** Serves a stream in both directions. */
    void addChat(String name, TypeDef args, ChatHandler handler);
}
