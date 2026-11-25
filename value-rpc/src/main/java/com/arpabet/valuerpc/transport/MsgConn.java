/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.transport;

import com.arpabet.value.MapValue;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import javax.net.ssl.SSLSession;

/**
 * One established connection that carries messages: the seam between the
 * protocol and whatever moves its bytes. TCP, TLS, a WebSocket, a pair of
 * queues in one process each give one; the client and the server see only
 * this. It is the Go implementation's {@code valuerpc.MsgConn}.
 *
 * <p>A connection <em>hands over</em> what it reads instead of being asked for
 * it, so that a transport with a thread per connection and one that reads many
 * connections on a few threads both fit.
 */
public interface MsgConn extends Closeable {

    /** Who a connection hands its messages to. */
    interface Receiver {

        /**
         * A message arrived. Called for one message at a time, in the order
         * they arrived, on a thread of the transport: it must not wait.
         */
        void onMessage(MapValue message);

        /**
         * The connection is gone. Called once, after the last message.
         *
         * @param cause why; null when it was closed here, or ended between two
         *              messages
         */
        void onClosed(Throwable cause);
    }

    /** Begins to read. Called once; nothing is handed over before it. */
    void start(Receiver receiver);

    /**
     * Writes messages in their order, with one write where the transport can.
     * It returns when they are written, or handed to a transport that writes
     * on its own; it may wait while the peer does not read.
     *
     * @throws IOException the connection is gone
     */
    void write(List<MapValue> batch) throws IOException;

    /** The address of the peer, for a log. */
    String remoteAddress();

    /** Closes the connection. The receiver then learns of it; closing twice is closing once. */
    @Override
    void close();

    /**
     * The TLS session of the connection, with the certificates the peer
     * showed, or null when it does not run over TLS.
     */
    default SSLSession tlsSession() {
        return null;
    }
}
