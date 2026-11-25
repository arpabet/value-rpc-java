/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

/**
 * A failure with a {@link Code}. A handler throws one to choose the code its
 * caller sees, and a call that failed throws one, so that a caller branches on
 * {@link #code()} instead of matching a message. It is the Go implementation's
 * {@code valuerpc.Error}.
 */
public class VrpcException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final Code code;
    private final int number;
    private final String detail;

    /** A failure with a code and what went wrong. */
    public VrpcException(Code code, String detail) {
        this(code, code.number(), detail, null);
    }

    /** A failure with a code, what went wrong and what caused it. */
    public VrpcException(Code code, String detail, Throwable cause) {
        this(code, code.number(), detail, cause);
    }

    private VrpcException(Code code, int number, String detail, Throwable cause) {
        super("vrpc " + code + ": " + detail, cause);
        this.code = code;
        this.number = number;
        this.detail = detail == null ? "" : detail;
    }

    /**
     * The failure an error response carries: its number is kept as it came,
     * also when it is not a code this implementation knows, so that it can be
     * passed on unchanged.
     */
    public static VrpcException ofNumber(long number, String detail) {
        return new VrpcException(Code.of(number), (int) number, detail, null);
    }

    /** The code; {@link Code#UNKNOWN} for a number that is not one. */
    public Code code() {
        return code;
    }

    /** The number of the code as it is on the wire. */
    public int codeNumber() {
        return number;
    }

    /** What went wrong, without the code: the message that travels on the wire. */
    public String detail() {
        return detail;
    }

    /** The failure with a code that an error is or was caused by, or null. */
    public static VrpcException in(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof VrpcException) {
                return (VrpcException) t;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return null;
    }

    /**
     * The code of an error a handler threw: its own when it carries one, the
     * code of a deadline or of a cancellation when it is one of those, and
     * {@link Code#INTERNAL} otherwise.
     */
    public static Code handlerCode(Throwable error) {
        return Code.of(handlerCodeNumber(error));
    }

    /**
     * {@link #handlerCode} as the number that goes on the wire: of a failure
     * whose number is not a code this implementation knows, that number.
     */
    public static int handlerCodeNumber(Throwable error) {
        final VrpcException coded = in(error);
        if (coded != null && coded.codeNumber() != Code.UNKNOWN.number()) {
            return coded.codeNumber();
        }
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof TimeoutException) {
                return Code.DEADLINE_EXCEEDED.number();
            }
            if (t instanceof CancellationException || t instanceof InterruptedException) {
                return Code.CANCELED.number();
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return Code.INTERNAL.number();
    }

    /**
     * The message of an error a handler threw, as it travels on the wire: of
     * a failure with a code, what went wrong without the code, so that the
     * code is not said twice once the caller has made its own failure of it.
     */
    public static String handlerDetail(Throwable error) {
        if (error instanceof VrpcException) {
            return ((VrpcException) error).detail();
        }
        final String message = error.getMessage();
        return message != null ? message : error.getClass().getName();
    }
}
