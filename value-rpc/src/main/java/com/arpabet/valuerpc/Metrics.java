/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

/**
 * What a client or a server reports about its calls, for whatever collects
 * metrics: a counter of requests and of errors by code, a gauge of requests
 * in flight, a histogram of how long they took, the rate of stream values,
 * the reconnects. It is the Go implementation's {@code valuerpc.Metrics}.
 *
 * <p>Every method is called from many threads at once and must be quick and
 * must not wait. They do nothing unless overridden.
 */
public interface Metrics {

    /** Reports nothing. */
    Metrics NONE = new Metrics() {
    };

    /** A call or a stream begins. */
    default void requestBegin(String method) {
    }

    /**
     * A call or a stream ends.
     *
     * @param code         how it ended; {@link Code#OK} when it succeeded
     * @param elapsedNanos how long it took
     */
    default void requestEnd(String method, Code code, long elapsedNanos) {
    }

    /** One value of a stream was sent or received. */
    default void streamValue(String method) {
    }

    /** A client tries to reconnect. */
    default void reconnect() {
    }
}
