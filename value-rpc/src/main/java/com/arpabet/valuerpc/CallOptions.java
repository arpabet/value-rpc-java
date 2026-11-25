/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What one call is made with besides its arguments: how long it may take and
 * what travels with it. It stands where the Go implementation takes a
 * {@code context.Context}. Immutable: each {@code with} gives another.
 */
public final class CallOptions {

    /** The timeout of the client, and no metadata of its own. */
    public static final CallOptions DEFAULT = new CallOptions(0, Collections.<String, String>emptyMap());

    private final long timeoutMillis;
    private final Map<String, String> metadata;

    private CallOptions(long timeoutMillis, Map<String, String> metadata) {
        this.timeoutMillis = timeoutMillis;
        this.metadata = metadata;
    }

    /** Options with a timeout; see {@link #withTimeoutMillis}. */
    public static CallOptions timeoutMillis(long millis) {
        return DEFAULT.withTimeoutMillis(millis);
    }

    /**
     * How long the call may take, in milliseconds: 0 for the timeout of the
     * client, negative for none. It bounds the call here and is sent to the
     * peer, which gives it to the handler as its deadline.
     */
    public long timeoutMillis() {
        return timeoutMillis;
    }

    /**
     * What travels with this call, on top of what the client sends with every
     * call: a trace context, say. Not to be changed.
     */
    public Map<String, String> metadata() {
        return metadata;
    }

    public CallOptions withTimeoutMillis(long millis) {
        return new CallOptions(millis, metadata);
    }

    /** With these entries as its metadata; null or empty for none. */
    public CallOptions withMetadata(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return new CallOptions(timeoutMillis, Collections.<String, String>emptyMap());
        }
        return new CallOptions(timeoutMillis,
                Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata)));
    }

    /** With one more entry of metadata. */
    public CallOptions withMetadata(String key, String value) {
        final Map<String, String> m = new LinkedHashMap<String, String>(metadata);
        m.put(key, value);
        return new CallOptions(timeoutMillis, Collections.unmodifiableMap(m));
    }
}
