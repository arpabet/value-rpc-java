/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import java.util.concurrent.ConcurrentHashMap;

/**
 * What an end of a connection serves, by name: the handlers behind a
 * {@link Registrar}. A server and a client each have one, and the
 * {@link Responder} of a connection looks a request up in it.
 *
 * <p>The two ends differ in one thing, as in the Go implementation: a server
 * refuses a name that is taken, and a client replaces what the name served.
 */
public final class HandlerRegistry implements Registrar {

    /** What a name serves: one pattern, with what its arguments and its result must be. */
    static final class Entry {
        final MessageType kind;
        final TypeDef args;
        final TypeDef result;
        final UnaryHandler unary;
        final OutgoingStreamHandler outgoing;
        final IncomingStreamHandler incoming;
        final ChatHandler chat;

        Entry(MessageType kind, TypeDef args, TypeDef result, UnaryHandler unary, OutgoingStreamHandler outgoing,
                IncomingStreamHandler incoming, ChatHandler chat) {
            this.kind = kind;
            this.args = args == null ? TypeDef.ANY : args;
            this.result = result == null ? TypeDef.ANY : result;
            this.unary = unary;
            this.outgoing = outgoing;
            this.incoming = incoming;
            this.chat = chat;
        }
    }

    private final ConcurrentHashMap<String, Entry> byName = new ConcurrentHashMap<String, Entry>();
    private final boolean replaces;

    /**
     * @param replaces whether a name registered again serves the new handler
     *                 from then on (a client), or is refused (a server)
     */
    public HandlerRegistry(boolean replaces) {
        this.replaces = replaces;
    }

    Entry get(String name) {
        return byName.get(name);
    }

    private void register(String name, Object handler, Entry entry) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("what is served needs a name");
        }
        if (handler == null) {
            throw new IllegalArgumentException("no handler for " + name);
        }
        if (replaces) {
            byName.put(name, entry);
        } else if (byName.putIfAbsent(name, entry) != null) {
            throw new IllegalStateException("function already exists: " + name);
        }
    }

    @Override
    public void addFunction(String name, TypeDef args, TypeDef result, UnaryHandler handler) {
        register(name, handler, new Entry(MessageType.FUNCTION_REQUEST, args, result, handler, null, null, null));
    }

    @Override
    public void addOutgoingStream(String name, TypeDef args, OutgoingStreamHandler handler) {
        register(name, handler, new Entry(MessageType.GET_STREAM_REQUEST, args, TypeDef.VOID, null, handler, null, null));
    }

    @Override
    public void addIncomingStream(String name, TypeDef args, IncomingStreamHandler handler) {
        register(name, handler, new Entry(MessageType.PUT_STREAM_REQUEST, args, TypeDef.VOID, null, null, handler, null));
    }

    @Override
    public void addChat(String name, TypeDef args, ChatHandler handler) {
        register(name, handler, new Entry(MessageType.CHAT_REQUEST, args, TypeDef.VOID, null, null, null, handler));
    }
}
