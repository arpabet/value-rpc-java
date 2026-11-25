/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.transport;

import com.arpabet.valuerpc.WireCodec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * What a WebSocket listener is set up with, on top of what every transport
 * is ({@link TransportOptions}): the codecs it serves, the pages that may
 * connect to it, and how it keeps a silent connection. The defaults are
 * those of the Go implementation. Immutable: each {@code with} gives another.
 */
public final class WebSocketOptions {

    /**
     * The defaults: both codecs, pages of the same origin only, a ping every
     * 15 s, and 10 s for a connection to ask to be upgraded.
     */
    public static final WebSocketOptions DEFAULT = new WebSocketOptions(
            Collections.<WireCodec>emptyList(), Collections.<String>emptyList(), 15_000, 10_000);

    private final List<WireCodec> codecs;
    private final List<String> originPatterns;
    private final long pingIntervalMillis;
    private final long handshakeTimeoutMillis;

    private WebSocketOptions(List<WireCodec> codecs, List<String> originPatterns, long pingIntervalMillis,
            long handshakeTimeoutMillis) {
        this.codecs = codecs;
        this.originPatterns = originPatterns;
        this.pingIntervalMillis = pingIntervalMillis;
        this.handshakeTimeoutMillis = handshakeTimeoutMillis;
    }

    /**
     * The codecs the listener serves, in its order of preference; empty for
     * both, MessagePack first. A client is served the first of them that it
     * offers as a subprotocol, and one that offers none is served the first.
     */
    public List<WireCodec> codecs() {
        return codecs;
    }

    /**
     * The origins a browser may connect from, besides the one the listener
     * itself is reached by; empty for none. See {@link #withOriginPatterns}.
     */
    public List<String> originPatterns() {
        return originPatterns;
    }

    /** How long a connection may be silent before it is sent a ping; not positive for no pings. */
    public long pingIntervalMillis() {
        return pingIntervalMillis;
    }

    /** How long a connection has to ask to be upgraded before it is closed; not positive for no limit. */
    public long handshakeTimeoutMillis() {
        return handshakeTimeoutMillis;
    }

    /**
     * Serves these codecs only, in this order of preference: one of
     * {@link WireCodec#JSON} alone, say, for an endpoint that is JSON and
     * nothing else. None gives both back.
     */
    public WebSocketOptions withCodecs(WireCodec... codecs) {
        return new WebSocketOptions(Collections.unmodifiableList(new ArrayList<WireCodec>(Arrays.asList(codecs))),
                originPatterns, pingIntervalMillis, handshakeTimeoutMillis);
    }

    /**
     * Lets browsers connect from these origins too. A browser says which
     * page opened a connection (the {@code Origin} of the request), and a
     * page of another site must not be let use what the browser of a user
     * may reach: unless a pattern allows it, only a page that was served
     * from the host of the listener itself is let in. A client that is not a
     * browser sends no origin and is not held to this.
     *
     * <p>A pattern is matched against the host of the origin, with its port
     * when it has one, whatever the case: {@code app.example.com},
     * {@code *.example.com}, {@code localhost:*}. {@code *} stands for any
     * run of characters and {@code ?} for one, and {@code *} alone lets
     * every origin in. A pattern with {@code ://} in it is matched against
     * the origin with its scheme: {@code https://*.example.com}.
     */
    public WebSocketOptions withOriginPatterns(String... patterns) {
        return new WebSocketOptions(codecs, Collections.unmodifiableList(new ArrayList<String>(Arrays.asList(patterns))),
                pingIntervalMillis, handshakeTimeoutMillis);
    }

    /**
     * A connection that says nothing for this long is sent a ping, and is
     * closed when that long again passes without a word from it: a peer that
     * is gone is found out, and what lies between the two ends does not take
     * a connection for dead that is only quiet.
     */
    public WebSocketOptions withPingIntervalMillis(long millis) {
        return new WebSocketOptions(codecs, originPatterns, millis, handshakeTimeoutMillis);
    }

    public WebSocketOptions withHandshakeTimeoutMillis(long millis) {
        return new WebSocketOptions(codecs, originPatterns, pingIntervalMillis, millis);
    }
}
