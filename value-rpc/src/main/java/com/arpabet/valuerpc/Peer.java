/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.Value;
import java.util.concurrent.CompletableFuture;

/**
 * The other end of an established connection, as something to call. vRPC is
 * symmetric: which side dialed is how the connection came to be, not what
 * either side may do on it, so a client and the handle a server has for a
 * client are both a peer, and code written against this reads the same on
 * either side. It is the Go implementation's {@code valuerpc.Peer}.
 *
 * <p>There are four patterns, all carried by one connection at once: a call,
 * a stream from the peer, a stream to the peer, and both together.
 */
public interface Peer {

    /**
     * Calls a function of the peer and waits for its result, with the
     * {@linkplain CallOptions#DEFAULT default options}.
     *
     * @param args the arguments; null for none
     * @return the result; null when the function returned nothing
     * @throws VrpcException the call failed; its {@link VrpcException#code()} says how
     */
    default Value callFunction(String name, Value args) {
        return callFunction(name, args, CallOptions.DEFAULT);
    }

    /**
     * Calls a function of the peer and waits for its result. A thread that is
     * interrupted while it waits cancels the call, which the peer is told,
     * and gets a {@link VrpcException} with {@link Code#CANCELED}; it stays
     * interrupted.
     */
    Value callFunction(String name, Value args, CallOptions options);

    /**
     * Calls a function of the peer without waiting. The future completes with
     * the result, or with the {@link VrpcException} the call failed with;
     * cancelling it cancels the call, which the peer is told.
     */
    CompletableFuture<Value> callFunctionAsync(String name, Value args, CallOptions options);

    /**
     * Opens a stream the peer sends on, and waits until the peer has taken
     * the request. The timeout of the options bounds that wait, not the
     * stream, which lasts until it ends or is closed.
     *
     * @throws VrpcException the peer refused the stream, or did not answer in time
     */
    ValueStream getStream(String name, Value args, CallOptions options);

    /**
     * Opens a stream this side sends on, and waits until the peer has taken
     * the request. Closing what is returned ends the stream.
     *
     * @throws VrpcException the peer refused the stream, or did not answer in time
     */
    ValueSink putStream(String name, Value args, CallOptions options);

    /**
     * Opens a stream in both directions, and waits until the peer has taken
     * the request.
     *
     * @throws VrpcException the peer refused the stream, or did not answer in time
     */
    Chat chat(String name, Value args, CallOptions options);
}
