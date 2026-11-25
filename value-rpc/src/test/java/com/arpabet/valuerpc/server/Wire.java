/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.server;

import com.arpabet.valuerpc.WireCodec;
import com.arpabet.valuerpc.transport.Dialer;
import com.arpabet.valuerpc.transport.TransportOptions;
import com.arpabet.valuerpc.transport.Transports;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** A transport a test runs over: where a server listens, and how a client reaches it. */
abstract class Wire {

    private static final AtomicInteger NAMES = new AtomicInteger();

    final String name;

    Wire(String name) {
        this.name = name;
    }

    /** An address for a server to listen on, another each time. */
    abstract String listen();

    /** A dialer to a server, by the address the server says it listens on. */
    abstract Dialer dialer(String serverAddress);

    @Override
    public String toString() {
        return name;
    }

    static final Wire MEM = new Wire("mem") {
        @Override
        String listen() {
            return "mem://server-test-" + NAMES.incrementAndGet();
        }

        @Override
        Dialer dialer(String serverAddress) {
            return Transports.dialer(serverAddress, TransportOptions.DEFAULT);
        }
    };

    static final Wire TCP = new Wire("tcp") {
        @Override
        String listen() {
            return "tcp://127.0.0.1:0";
        }

        @Override
        Dialer dialer(String serverAddress) {
            return Transports.dialer("tcp://" + serverAddress, TransportOptions.DEFAULT);
        }
    };

    /** WebSocket with a codec, reached by the WebSocket client of the JDK. */
    private static Wire webSocket(WireCodec codec) {
        return new Wire("ws " + codec.name()) {
            @Override
            String listen() {
                return "ws://127.0.0.1:0/rpc";
            }

            @Override
            Dialer dialer(String serverAddress) {
                return new JdkWebSocketDialer("ws://" + serverAddress, codec);
            }
        };
    }

    static final Wire WS_MSGPACK = webSocket(WireCodec.MSGPACK);

    static final Wire WS_JSON = webSocket(WireCodec.JSON);

    /** Every transport a server of the core listens on, and WebSocket in each of its codecs. */
    static List<Wire> all() {
        final List<Wire> all = new ArrayList<Wire>();
        all.add(MEM);
        all.add(TCP);
        all.add(WS_MSGPACK);
        all.add(WS_JSON);
        return all;
    }
}
