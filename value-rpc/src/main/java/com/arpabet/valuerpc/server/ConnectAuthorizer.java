/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.server;

import com.arpabet.valuerpc.transport.MsgConn;

/**
 * Lets a connection in or not, before anything is read from it: by the
 * certificate its peer showed ({@link MsgConn#tlsSession}), or by where it
 * comes from. It is the Go implementation's
 * {@code valueserver.ConnectAuthorizer}.
 */
@FunctionalInterface
public interface ConnectAuthorizer {

    /**
     * @throws Exception the connection is refused: it is closed without a word
     */
    void authorize(MsgConn conn) throws Exception;
}
