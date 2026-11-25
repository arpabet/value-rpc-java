/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.value.MapValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.Dialect;
import com.arpabet.valuerpc.MessageType;
import com.arpabet.valuerpc.transport.MsgConn;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A client that is no client: a connection a test writes messages of its own
 * making to, and reads the very messages the server answers with. For what a
 * well-behaved client never sends, and for the order of what a server sends.
 */
final class Raw implements MsgConn.Receiver, AutoCloseable {

    static final Dialect D = Dialect.standard();
    private static final AtomicLong IDS = new AtomicLong(7_000_000);

    final MsgConn conn;
    final long clientId = IDS.incrementAndGet();
    private final LinkedBlockingQueue<MapValue> got = new LinkedBlockingQueue<MapValue>();
    private final CountDownLatch closed = new CountDownLatch(1);

    Raw(MsgConn conn) {
        this.conn = conn;
        conn.start(this);
    }

    @Override
    public void onMessage(MapValue message) {
        got.add(message);
    }

    @Override
    public void onClosed(Throwable cause) {
        closed.countDown();
    }

    void send(MapValue... messages) throws IOException {
        conn.write(Arrays.asList(messages));
    }

    /** The next message of the server, which has to come. */
    MapValue next() throws InterruptedException {
        final MapValue m = got.poll(10, TimeUnit.SECONDS);
        assertNotNull(m, "the server sent nothing in time");
        return m;
    }

    /** The next message, which has to be of a type and for a request. */
    MapValue next(MessageType type, long requestId) throws InterruptedException {
        final MapValue m = next();
        assertEquals(type, D.messageType(m), () -> "the type of " + m);
        assertEquals(requestId, D.requestId(m).longValue(), () -> "the request of " + m);
        return m;
    }

    /** Asserts that the server sends nothing for a while. */
    void nothing(long millis) throws InterruptedException {
        final MapValue m = got.poll(millis, TimeUnit.MILLISECONDS);
        assertNull(m, "the server was to send nothing");
    }

    /** Asserts that the server closes the connection, having sent nothing more. */
    void closedByServer() throws InterruptedException {
        assertTrue(closed.await(10, TimeUnit.SECONDS), "the server did not close the connection");
        assertNull(got.poll(), "the server was to close without a word");
    }

    boolean isClosed() {
        return closed.getCount() == 0;
    }

    /** Shakes hands as a client that does not resume, and is let in. */
    Raw shake() throws IOException, InterruptedException {
        return shake("", null);
    }

    Raw shake(String token, Value credential) throws IOException, InterruptedException {
        send(D.handshakeRequest(clientId, token, credential));
        next(MessageType.HANDSHAKE_RESPONSE, 0);
        return this;
    }

    MapValue request(MessageType type, long id, String name, Value args) {
        return D.request(type, id, name, args, 0, null);
    }

    @Override
    public void close() {
        conn.close();
    }
}
