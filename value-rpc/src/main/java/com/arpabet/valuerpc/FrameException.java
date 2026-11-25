/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import java.io.IOException;

/**
 * What a peer sent is not a message: a frame longer than the limit, bytes that
 * are not a value, or a value that is not a map. The connection it came on is
 * of no more use.
 */
public class FrameException extends IOException {

    private static final long serialVersionUID = 1L;

    public FrameException(String message) {
        super(message);
    }

    public FrameException(String message, Throwable cause) {
        super(message, cause);
    }
}
