/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.NumberValue;
import com.arpabet.value.Value;

/**
 * The type of a message, the {@code t} of its envelope. The numbers are the
 * ones of the Go implementation ({@code valuerpc.MessageType}); 12 and 13 are
 * reserved, having been message types that no longer exist.
 */
public enum MessageType {
    HANDSHAKE_REQUEST(0),
    HANDSHAKE_RESPONSE(1),
    FUNCTION_REQUEST(2),
    FUNCTION_RESPONSE(3),
    GET_STREAM_REQUEST(4),
    PUT_STREAM_REQUEST(5),
    CHAT_REQUEST(6),
    ERROR_RESPONSE(7),
    STREAM_READY(8),
    STREAM_VALUE(9),
    STREAM_END(10),
    CANCEL_REQUEST(11),
    /**
     * Grants the sender of a stream leave to send that many more values. The
     * receiver grants a window when the stream opens and more as its
     * application takes values, so a sender never outruns a receiver.
     */
    STREAM_CREDIT(14);

    private static final MessageType[] BY_NUMBER = new MessageType[15];

    static {
        for (MessageType t : values()) {
            BY_NUMBER[t.number] = t;
        }
    }

    private final int number;
    private final NumberValue value;

    MessageType(int number) {
        this.number = number;
        this.value = Value.ofLong(number);
    }

    /** The number of the type on the wire. */
    public int number() {
        return number;
    }

    /** The number as the value an envelope holds. */
    public NumberValue value() {
        return value;
    }

    /** The type of a number read from the wire, or null for a number that is not a type. */
    public static MessageType of(long number) {
        return number >= 0 && number < BY_NUMBER.length ? BY_NUMBER[(int) number] : null;
    }

    /** Whether the message opens a request: a call or one of the three streams. */
    public boolean opensRequest() {
        return this == FUNCTION_REQUEST || opensStream();
    }

    /** Whether the message opens a stream. */
    public boolean opensStream() {
        return this == GET_STREAM_REQUEST || this == PUT_STREAM_REQUEST || this == CHAT_REQUEST;
    }
}
