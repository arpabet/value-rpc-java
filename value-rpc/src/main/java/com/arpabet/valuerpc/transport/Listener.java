/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.transport;

import java.io.Closeable;

/**
 * Accepts inbound connections: the server's side of the transport seam. It is
 * the Go implementation's {@code valuerpc.Listener}.
 */
public interface Listener extends Closeable {

    /** Who a listener hands its connections to. */
    interface Acceptor {

        /** A peer connected. Called on a thread of the transport: it must not wait. */
        void onConnection(MsgConn conn);
    }

    /** Begins to accept. Called once. */
    void start(Acceptor acceptor);

    /** The address it listens on, with the port it was given when it asked for any. */
    String address();

    /** Stops accepting. Connections it has handed over stay open. */
    @Override
    void close();
}
