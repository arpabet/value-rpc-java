/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.server;

import com.arpabet.value.Value;
import com.arpabet.valuerpc.transport.MsgConn;

/**
 * Says who a client is, from the credential its handshake carries: a token,
 * a key, whatever the two ends agreed on, and what a transport cannot show by
 * a certificate. It is asked at every handshake, also when a client
 * reconnects. It is the Go implementation's {@code valueserver.Authenticator}.
 *
 * <p>What it returns is the principal of the connection, which every handler
 * can read ({@link com.arpabet.valuerpc.RequestContext#principal}). The
 * session of a client is bound to it: a client that reconnects is taken for
 * the same session only when it is the same principal again, so that a link
 * of its hash chain that leaked lets nobody else in.
 */
@FunctionalInterface
public interface Authenticator {

    /**
     * @param conn       the connection, for its address or its TLS session
     * @param credential what the client sent; the null value when it sent nothing
     * @return who the client is; null or empty for nobody in particular
     * @throws Exception the client is refused. It is told that authentication
     *         failed and no more: why stays here
     */
    String authenticate(MsgConn conn, Value credential) throws Exception;
}
