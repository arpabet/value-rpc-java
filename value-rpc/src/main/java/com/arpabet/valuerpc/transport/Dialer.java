/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.transport;

import java.io.IOException;

/**
 * Makes an outbound connection: the client's side of the transport seam. A
 * client dials again every time its connection is lost. It is the Go
 * implementation's {@code valuerpc.Dialer}.
 */
public interface Dialer {

    /**
     * Establishes one connection.
     *
     * @param timeoutMillis how long establishing it may take; not positive for no limit
     * @throws IOException the peer could not be reached
     */
    MsgConn dial(long timeoutMillis) throws IOException;
}
