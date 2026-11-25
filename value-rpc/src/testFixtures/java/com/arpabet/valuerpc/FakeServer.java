/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.MapValue;
import com.arpabet.value.NumberValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.transport.Listener;
import com.arpabet.valuerpc.transport.MsgConn;
import com.arpabet.valuerpc.transport.TransportOptions;
import com.arpabet.valuerpc.transport.Transports;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * A server for the tests of the client that need no Go: it speaks the
 * protocol as the Go server does for a handshake and for a call, session
 * resumption by hash chain included, and can be told to misbehave. What it
 * was sent is kept, so that a test can look at the very messages.
 */
public final class FakeServer implements AutoCloseable {

    private static final Dialect D = Dialect.standard();

    /** Every handshake it was sent, in order. */
    public final List<MapValue> handshakes = new CopyOnWriteArrayList<MapValue>();
    /** Every other message it was sent, in order. */
    public final LinkedBlockingQueue<MapValue> received = new LinkedBlockingQueue<MapValue>();

    /** Refuse every handshake with this, as a server whose authenticator said no. */
    public volatile VrpcException refuseHandshake;
    /** Close a connection at its handshake without a word, as a server shown a wrong magic. */
    public volatile boolean silentAtHandshake;
    /** Never answer a handshake. */
    public volatile boolean muteAtHandshake;
    /** Accept handshakes and answer no request. */
    public volatile boolean mute;

    private final Listener listener;
    private final Map<String, Function<Value, Value>> functions = new ConcurrentHashMap<String, Function<Value, Value>>();
    /** The last link each client showed and was accepted with, by its id. */
    private final Map<Long, String> sessions = new ConcurrentHashMap<Long, String>();
    private final List<MsgConn> connections = new CopyOnWriteArrayList<MsgConn>();

    /** @param address where to listen: {@code mem://name} or {@code tcp://127.0.0.1:0} */
    public FakeServer(String address) throws IOException {
        this(Transports.listener(address, TransportOptions.DEFAULT));
    }

    /** A server on a listener of the test's own making: another transport. */
    public FakeServer(Listener listener) {
        this.listener = listener;
        function("greet", args -> Value.utf8("Hello, " + ((com.arpabet.value.ListValue) args).get(0) + "!"));
        function("echo", args -> args);
        function("fail", args -> {
            throw new VrpcException(Code.RESOURCE_EXHAUSTED, "deliberate failure");
        });
        listener.start(this::accept);
    }

    /** The address a client dials: of the transports that come with the core, with their scheme. */
    public String address() {
        final String a = listener.address();
        return a.startsWith("mem://") ? a : "tcp://" + a;
    }

    /** The address it listens on, as its listener gives it. */
    public String listenerAddress() {
        return listener.address();
    }

    /** A function it serves. What it throws with a code is the error the caller gets. */
    public void function(String name, Function<Value, Value> fn) {
        functions.put(name, fn);
    }

    /** The connections accepted so far. */
    public int connections() {
        return connections.size();
    }

    /** Sends a message of the test's own making on the latest connection. */
    public void push(MapValue message) throws IOException {
        connections.get(connections.size() - 1).write(Collections.singletonList(message));
    }

    /** Closes the latest connection from this side, as a link that drops. */
    public void drop() {
        connections.get(connections.size() - 1).close();
    }

    /** The next message it was sent that is not a handshake, or null when none comes in time. */
    public MapValue next(long millis) throws InterruptedException {
        return received.poll(millis, TimeUnit.MILLISECONDS);
    }

    private void accept(MsgConn conn) {
        connections.add(conn);
        conn.start(new MsgConn.Receiver() {
            private boolean shaken;

            @Override
            public void onMessage(MapValue message) {
                try {
                    if (!shaken) {
                        shaken = handshake(conn, message);
                    } else {
                        request(conn, message);
                    }
                } catch (IOException e) {
                    conn.close();
                }
            }

            @Override
            public void onClosed(Throwable cause) {
                // a test asks for what it wants to know
            }
        });
    }

    private boolean handshake(MsgConn conn, MapValue message) throws IOException {
        handshakes.add(message);
        if (muteAtHandshake) {
            return false;
        }
        if (silentAtHandshake || D.messageType(message) != MessageType.HANDSHAKE_REQUEST
                || !D.validMagicAndVersion(message)) {
            conn.close();
            return false;
        }
        final NumberValue rid = Value.ofLong(D.handshakeRequestId());
        if (refuseHandshake != null) {
            conn.write(Collections.singletonList(D.error(rid, refuseHandshake.code(), refuseHandshake.detail())));
            conn.close();
            return false;
        }
        // Resumption as the Go server decides it: the first link a client
        // shows is kept; a later one must come before the one kept, or be it.
        final long cid = Dialect.number(message, D.clientIdField()).longValue();
        final String token = message.containsKey(D.sessionTokenField())
                ? message.getString(D.sessionTokenField()).toString() : "";
        final String last = sessions.get(cid);
        if (last != null && HashChain.verifyStep(token, last, 0) == 0 && !HashChain.sameToken(token, last)) {
            conn.write(Collections.singletonList(D.error(rid, Code.UNAUTHENTICATED, "session resume rejected")));
            conn.close();
            return false;
        }
        sessions.put(cid, token);
        conn.write(Collections.singletonList(D.handshakeResponse(null)));
        return true;
    }

    private void request(MsgConn conn, MapValue message) throws IOException {
        received.add(message);
        if (mute || D.messageType(message) != MessageType.FUNCTION_REQUEST) {
            return;
        }
        final NumberValue rid = D.requestId(message);
        final String name = D.functionName(message);
        final Function<Value, Value> fn = functions.get(name);
        MapValue answer;
        if (fn == null) {
            answer = D.error(rid, Code.NOT_FOUND, "function not found " + name);
        } else {
            try {
                answer = D.functionResult(rid, fn.apply(D.arguments(message)));
            } catch (RuntimeException e) {
                answer = D.handlerError(rid, "function " + name, e);
            }
        }
        conn.write(Collections.singletonList(answer));
    }

    @Override
    public void close() {
        listener.close();
        for (MsgConn c : connections) {
            c.close();
        }
    }
}
