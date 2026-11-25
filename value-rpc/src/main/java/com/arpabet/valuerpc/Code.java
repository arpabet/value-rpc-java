/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

/**
 * The machine-readable status of a call, carried on the wire so that a caller
 * branches on the kind of failure instead of matching its message. The set is
 * the one of the Go implementation ({@code valuerpc.Code}), a subset of the
 * gRPC status codes, with the same numbers.
 */
public enum Code {
    /** Not an error. */
    OK(0, "OK"),
    /** An unclassified failure, and any number this implementation does not know. */
    UNKNOWN(1, "Unknown"),
    /** The request was cancelled. */
    CANCELED(2, "Canceled"),
    /** A malformed request, or arguments that failed verification. */
    INVALID_ARGUMENT(3, "InvalidArgument"),
    /** The deadline passed before the call completed. */
    DEADLINE_EXCEEDED(4, "DeadlineExceeded"),
    /** No such function. */
    NOT_FOUND(5, "NotFound"),
    /** A limit was hit: busy, too many streams, flow-control credit exceeded. */
    RESOURCE_EXHAUSTED(6, "ResourceExhausted"),
    /** The transport or the stream failed; safe to retry. */
    UNAVAILABLE(7, "Unavailable"),
    /** Authentication failed. */
    UNAUTHENTICATED(8, "Unauthenticated"),
    /** An error inside the handler or the peer. */
    INTERNAL(9, "Internal");

    private static final Code[] BY_NUMBER = values();

    private final int number;
    private final String text;

    Code(int number, String text) {
        this.number = number;
        this.text = text;
    }

    /** The number of the code on the wire. */
    public int number() {
        return number;
    }

    /** The name the Go implementation prints, such as {@code NotFound}. */
    @Override
    public String toString() {
        return text;
    }

    /** The code of a number read from the wire; {@link #UNKNOWN} for one that is not a code. */
    public static Code of(long number) {
        return number >= 0 && number < BY_NUMBER.length ? BY_NUMBER[(int) number] : UNKNOWN;
    }

    /**
     * The code an error carries: the code of the {@link VrpcException} it is
     * or was caused by, {@link #OK} for no error, and {@link #UNKNOWN} for any
     * other error.
     */
    public static Code of(Throwable error) {
        if (error == null) {
            return OK;
        }
        final VrpcException e = VrpcException.in(error);
        return e == null ? UNKNOWN : e.code();
    }
}
