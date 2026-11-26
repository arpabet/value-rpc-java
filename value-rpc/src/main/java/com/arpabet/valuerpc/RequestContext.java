/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import java.util.Map;

/**
 * What a handler is told about the request it serves, besides its arguments:
 * who asks, what travels with the request, how long the caller waits, and
 * whether the caller still waits at all. It stands where a handler of the Go
 * implementation takes a {@code context.Context}
 * ({@code MetadataFromContext}, {@code PrincipalFromContext}, the deadline and
 * {@code Done}).
 *
 * <p>The library makes the contexts; a handler only reads them. A context is
 * safe to use from several threads, and stays readable after its handler has
 * returned.
 */
public interface RequestContext {

    /** The id of the request, which the peer chose. */
    long requestId();

    /** The name the request was made by: the function or the stream. */
    String method();

    /**
     * What travels with the request, such as a trace context. Empty when it
     * has none; never null, and not to be changed.
     */
    Map<String, String> metadata();

    /**
     * Who the peer is, as the authenticator of the server said when the
     * connection was made; empty when it said nobody, or there is no
     * authenticator. A handler goes by this, and not by a name in the
     * arguments, which anyone can write.
     */
    String principal();

    /** The address of the peer, for a log. */
    String remoteAddress();

    /**
     * The one who asks, as something to ask in turn: a connection is used in
     * both directions. On a server it is the client whose request this is,
     * for what that client registered; on a client, the server. It is the Go
     * implementation's {@code PeerFromContext}.
     *
     * <p>On a server the peer is the client across its connections: it may
     * be kept, by who the client is say, and asked later, from another
     * request. What is asked while the client is not connected fails with
     * {@link Code#UNAVAILABLE}, and so does what was under way when its
     * connection was lost.
     */
    Peer peer();

    /**
     * How long the caller still waits for this call, in milliseconds: what is
     * left of the time it allowed when it made the call. 0 once that time has
     * passed, and -1 when the caller named no time. Only a call has one; a
     * stream lasts until it ends.
     */
    long remainingMillis();

    /**
     * Whether nobody waits for the request any more: the peer cancelled it,
     * its time has passed, the connection was lost, or the server is closing.
     * A handler that takes long looks at this now and then, and stops.
     *
     * <p>The thread that runs the handler is interrupted at that moment, so
     * a handler that waits is woken, and need not ask.
     */
    boolean isCancelled();

    /**
     * Has something run when the request is cancelled, once, on a thread of
     * the library: to give up work that is under way elsewhere. It runs at
     * once when the request is cancelled already, and never when the request
     * ends without being cancelled.
     */
    void onCancel(Runnable callback);
}
