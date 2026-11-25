/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.quic;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.arpabet.value.MapValue;
import com.arpabet.valuerpc.transport.Listener;
import com.arpabet.valuerpc.transport.MsgConn;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The two ends of one QUIC connection, for a test of the transport itself: a
 * listener, the connection it accepted, and the one that was dialed, each
 * with what it was handed.
 */
final class Ends implements AutoCloseable {

    /** What a connection hands over, kept in its order. */
    static final class Recorder implements MsgConn.Receiver {
        private final LinkedBlockingQueue<MapValue> messages = new LinkedBlockingQueue<MapValue>();
        /** Completes when the connection is gone, with why. */
        final CompletableFuture<Optional<Throwable>> closed = new CompletableFuture<Optional<Throwable>>();

        @Override
        public void onMessage(MapValue message) {
            messages.add(message);
        }

        @Override
        public void onClosed(Throwable cause) {
            closed.complete(Optional.ofNullable(cause));
        }

        /** The next message, which has to come. */
        MapValue next() throws InterruptedException {
            final MapValue m = messages.poll(10, TimeUnit.SECONDS);
            assertNotNull(m, "a message was expected");
            return m;
        }

        /** Nothing comes for this long. */
        void nothing(long millis) throws InterruptedException {
            assertNull(messages.poll(millis, TimeUnit.MILLISECONDS), "no message was expected");
        }

        /** Why the connection went, which it has to: empty when in good order. */
        Optional<Throwable> whyClosed() throws Exception {
            return closed.get(10, TimeUnit.SECONDS);
        }
    }

    final Listener listener;
    final QuicConn client;
    final QuicConn server;
    final Recorder atClient = new Recorder();
    final Recorder atServer = new Recorder();

    Ends(QuicOptions serverOptions, QuicOptions clientOptions) throws Exception {
        listener = QuicTransport.listener("127.0.0.1:0", serverOptions);
        final CompletableFuture<MsgConn> accepted = new CompletableFuture<MsgConn>();
        listener.start(conn -> {
            conn.start(atServer);
            accepted.complete(conn);
        });
        try {
            client = (QuicConn) QuicTransport.dialer(listener.address(), clientOptions).dial(10_000);
            client.start(atClient);
            server = (QuicConn) accepted.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            listener.close();
            throw e;
        }
    }

    static void send(QuicConn from, MapValue... messages) throws Exception {
        for (MapValue m : messages) {
            from.write(Collections.singletonList(m));
        }
    }

    /** Waits until an end has this many streams of requests. */
    static void awaitStreams(QuicConn conn, int count) throws InterruptedException {
        final long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        int now = conn.streamCount();
        while (now != count) {
            if (System.nanoTime() > until) {
                throw new AssertionError(now + " streams, and not " + count + ", at " + conn);
            }
            Thread.sleep(2);
            now = conn.streamCount();
        }
    }

    /** Both ends have freed every stream. */
    void awaitNoStreams() throws InterruptedException {
        awaitStreams(client, 0);
        awaitStreams(server, 0);
    }

    @Override
    public void close() {
        client.close();
        server.close();
        listener.close();
    }
}
