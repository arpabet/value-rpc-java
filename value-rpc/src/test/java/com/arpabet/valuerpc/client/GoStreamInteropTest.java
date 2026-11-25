/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.arpabet.value.MapValue;
import com.arpabet.value.NumberValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.Chat;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.GoServer;
import com.arpabet.valuerpc.Metrics;
import com.arpabet.valuerpc.ValueSink;
import com.arpabet.valuerpc.ValueStream;
import com.arpabet.valuerpc.VrpcException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The three kinds of stream of the Java client against the Go server, over
 * TCP: the stream scenarios of the conformance suite of value-rpc-js, and the
 * flow control under load. Skipped where there is no Go toolchain.
 */
@Timeout(90)
class GoStreamInteropTest {

    /** A server of its own for each test: the upload counters are the server's. */
    private GoServer server;
    private VrpcClient client;

    @BeforeEach
    void start() throws Exception {
        assumeTrue(GoServer.available(), "no Go toolchain: the Go server cannot be built");
        server = GoServer.start();
        client = VrpcClient.builder().address(server.tcp()).timeoutMillis(4000).build();
    }

    @AfterEach
    void stop() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    private static long number(Value v) {
        return ((NumberValue) v).longValue();
    }

    private MapValue uploaded(long count) throws InterruptedException {
        // The server counts what it has taken from the stream, a little after it was sent.
        MapValue stats = null;
        for (int i = 0; i < 500; i++) {
            stats = (MapValue) client.callFunction("upload.stats", null);
            if (stats.getNumber("count").longValue() >= count) {
                break;
            }
            Thread.sleep(10);
        }
        return stats;
    }

    @Test
    void aStreamFromTheServerArrivesInOrderAndEnds() throws Exception {
        final List<Long> got = new ArrayList<Long>();
        try (ValueStream stream = client.getStream("count", Value.list(Value.ofLong(25)), CallOptions.DEFAULT)) {
            assertTrue(stream.requestId() > 0);
            for (Value v : stream) {
                got.add(number(v));
            }
            assertNull(stream.take(), "the end stays the end");
        }
        assertEquals(25, got.size());
        for (int i = 0; i < 25; i++) {
            assertEquals(i + 1, got.get(i).longValue());
        }
        assertEquals(0L, client.stats().get("pending").longValue(), "a stream that ended is retired");

        try (ValueStream empty = client.getStream("count", Value.list(Value.ofLong(0)), CallOptions.DEFAULT)) {
            assertNull(empty.take(), "a stream of nothing ends at once");
        }
    }

    @Test
    void aLongStreamLosesNothing() throws Exception {
        // Many times the window: the server sends only what it has credit for.
        final int n = 100_000;
        long sum = 0;
        long count = 0;
        long last = 0;
        try (ValueStream stream = client.getStream("count", Value.list(Value.ofLong(n)), CallOptions.DEFAULT)) {
            for (Value v : stream) {
                final long x = number(v);
                assertEquals(last + 1, x, "in order, none missing");
                last = x;
                sum += x;
                count++;
            }
        }
        assertEquals(n, count);
        assertEquals((long) n * (n + 1) / 2, sum);
    }

    @Test
    void aSmallWindowOnlyMeansMoreGrants() throws Exception {
        try (VrpcClient narrow = VrpcClient.builder().address(server.tcp()).streamWindow(3).build();
                ValueStream stream = narrow.getStream("count", Value.list(Value.ofLong(2000)), CallOptions.DEFAULT)) {
            long count = 0;
            for (Value v : stream) {
                assertEquals(++count, number(v));
            }
            assertEquals(2000, count);
        }
    }

    @Test
    void aSlowReaderHoldsUpNoOtherRequest() throws Exception {
        // A stream nobody reads: the server stops at the window, and the
        // connection goes on carrying calls and another stream.
        try (ValueStream unread = client.getStream("count", Value.list(Value.ofLong(1_000_000)), CallOptions.DEFAULT)) {
            for (int i = 0; i < 200; i++) {
                assertEquals(Value.ofLong(i), client.callFunction("echo", Value.ofLong(i)));
            }
            try (ValueStream other = client.getStream("count", Value.list(Value.ofLong(500)), CallOptions.DEFAULT)) {
                long count = 0;
                for (Value v : other) {
                    assertEquals(++count, number(v));
                }
                assertEquals(500, count);
            }
            // And the unread one is as it was left: its first value is still the first.
            assertEquals(Value.ofLong(1), unread.take());
            assertEquals(Value.ofLong(2), unread.take(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void closingAStreamEarlyCancelsIt() throws Exception {
        final ValueStream stream = client.getStream("count", Value.list(Value.ofLong(1_000_000)), CallOptions.DEFAULT);
        assertEquals(Value.ofLong(1), stream.take());
        assertEquals(Value.ofLong(2), stream.take());
        assertEquals(Value.ofLong(3), stream.take());
        stream.close();
        assertEquals(0L, client.stats().get("pending").longValue());
        assertSame(Code.CANCELED, assertThrows(VrpcException.class, stream::take).code());
        stream.close();
        // The server was told and stopped; the connection carries on.
        assertEquals(Value.utf8("Hello, after!"), client.callFunction("greet", Value.list(Value.utf8("after"))));
    }

    @Test
    void aStreamTheServerRefusesFailsToOpen() {
        // The handler said no, the function is not there, it is not a stream.
        assertSame(Code.INVALID_ARGUMENT, assertThrows(VrpcException.class,
                () -> client.getStream("count", Value.list(Value.ofLong(-1)), CallOptions.DEFAULT)).code());
        assertSame(Code.NOT_FOUND, assertThrows(VrpcException.class,
                () -> client.getStream("no.such.stream", null, CallOptions.DEFAULT)).code());
        assertSame(Code.INVALID_ARGUMENT, assertThrows(VrpcException.class,
                () -> client.getStream("greet", null, CallOptions.DEFAULT)).code());
        assertSame(Code.INVALID_ARGUMENT, assertThrows(VrpcException.class,
                () -> client.putStream("count", null, CallOptions.DEFAULT)).code());
        assertEquals(0L, client.stats().get("pending").longValue());
    }

    @Test
    void aStreamToTheServer() throws Exception {
        try (ValueSink sink = client.putStream("upload", Value.list(Value.utf8("batch")), CallOptions.DEFAULT)) {
            for (int i = 1; i <= 10; i++) {
                sink.send(Value.ofLong(i));
            }
            assertThrows(IllegalArgumentException.class, () -> sink.send(Value.ofNull()));
        }
        final MapValue stats = uploaded(10);
        assertEquals(10, stats.getNumber("count").longValue());
        assertEquals(55, stats.getNumber("sum").longValue());
        assertEquals(0L, client.stats().get("pending").longValue(), "a stream that was closed is retired");
    }

    @Test
    void aLongStreamToTheServerLosesNothing() throws Exception {
        // Many times the server's window: sending waits for its credit.
        final int n = 50_000;
        final ValueSink sink = client.putStream("upload", null, CallOptions.DEFAULT);
        for (int i = 1; i <= n; i++) {
            sink.send(Value.ofLong(i));
        }
        sink.close();
        sink.close();
        final MapValue stats = uploaded(n);
        assertEquals(n, stats.getNumber("count").longValue());
        assertEquals((long) n * (n + 1) / 2, stats.getNumber("sum").longValue());
        final VrpcException e = assertThrows(VrpcException.class, () -> sink.send(Value.ofLong(1)));
        assertSame(Code.CANCELED, e.code(), "a stream that was ended takes no more");
    }

    @Test
    void aChatAnswersEachMessage() throws Exception {
        try (Chat chat = client.chat("chat.echo", Value.list(Value.utf8("room-1")), CallOptions.DEFAULT)) {
            chat.outgoing().send(Value.utf8("one"));
            assertEquals(Value.utf8("echo:one"), chat.incoming().take());
            chat.outgoing().send(Value.utf8("two"));
            chat.outgoing().send(Value.utf8("three"));
            // This side has said all; the server answers what is left and ends.
            chat.outgoing().close();
            final List<String> rest = new ArrayList<String>();
            for (Value v : chat.incoming()) {
                rest.add(v.toString());
            }
            assertEquals("[echo:two, echo:three]", rest.toString());
        }
        assertEquals(0L, client.stats().get("pending").longValue());
    }

    @Test
    void aChatWhoseClientHasSaidItsLastStillGetsAllThatComesBack() throws Exception {
        // The client sends all it has and ends its side before it reads
        // anything: what comes back after that is more than the server had
        // credit for, and the client goes on granting it.
        final int n = 10_000;
        try (Chat chat = client.chat("chat.echo", null, CallOptions.DEFAULT)) {
            for (int i = 0; i < n; i++) {
                chat.outgoing().send(Value.ofLong(i));
            }
            chat.outgoing().close();
            int count = 0;
            for (Value v = chat.incoming().take(20, TimeUnit.SECONDS); v != null; v = chat.incoming().take(20, TimeUnit.SECONDS)) {
                assertEquals("echo:" + count, v.toString());
                count++;
            }
            assertEquals(n, count);
        }
        assertEquals(0L, client.stats().get("pending").longValue());
    }

    @Test
    void theServerEndingWhatItSendsEndsAChat() throws Exception {
        // This chat's server says nothing and ends its side at once: the chat
        // is over then, whatever the client still had to say.
        try (Chat chat = client.chat("chat.sink", null, CallOptions.DEFAULT)) {
            assertNull(chat.incoming().take(), "the server has ended");
            assertEquals(0L, client.stats().get("pending").longValue(), "and the chat with it");
            assertSame(Code.CANCELED, assertThrows(VrpcException.class, () -> chat.outgoing().send(Value.ofLong(1))).code());
            chat.outgoing().close();
        }
        assertEquals(Value.utf8("Hello, after!"), client.callFunction("greet", Value.list(Value.utf8("after"))));
    }

    @Test
    void aChatUnderLoad() throws Exception {
        final int n = 20_000;
        try (Chat chat = client.chat("chat.echo", null, CallOptions.DEFAULT)) {
            final Thread writer = new Thread(() -> {
                try {
                    for (int i = 0; i < n; i++) {
                        chat.outgoing().send(Value.ofLong(i));
                    }
                    chat.outgoing().close();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            writer.start();
            int count = 0;
            for (Value v : chat.incoming()) {
                assertEquals("echo:" + count, v.toString());
                count++;
            }
            assertEquals(n, count);
            writer.join(10_000);
        }
    }

    @Test
    void aLostConnectionFailsItsStreams() throws Exception {
        final ValueStream stream = client.getStream("count", Value.list(Value.ofLong(1_000_000)), CallOptions.DEFAULT);
        final ValueSink sink = client.putStream("upload", null, CallOptions.DEFAULT);
        assertEquals(Value.ofLong(1), stream.take());
        assertEquals(Value.utf8("kicking"), client.callFunction("kick", null));
        // What was buffered may still be there to take, then the stream says why it stops.
        VrpcException failed = null;
        for (int i = 0; i < 1_000_000 && failed == null; i++) {
            try {
                if (stream.take(10, TimeUnit.SECONDS) == null) {
                    break;
                }
            } catch (VrpcException e) {
                failed = e;
            } catch (TimeoutException e) {
                throw new AssertionError("the stream neither went on nor failed", e);
            }
        }
        assertSame(Code.UNAVAILABLE, failed.code());
        VrpcException sinkFailed = null;
        for (int i = 0; i < 100_000 && sinkFailed == null; i++) {
            try {
                sink.send(Value.ofLong(1));
            } catch (VrpcException e) {
                sinkFailed = e;
            }
        }
        assertSame(Code.UNAVAILABLE, sinkFailed.code());
        // The client is connected again, and streams anew.
        try (ValueStream again = client.getStream("count", Value.list(Value.ofLong(3)), CallOptions.DEFAULT)) {
            assertEquals(Value.ofLong(1), again.take());
        }
    }

    @Test
    void metricsCountAStreamOnceAndEachOfItsValues() throws Exception {
        final AtomicInteger begun = new AtomicInteger();
        final AtomicInteger ended = new AtomicInteger();
        final AtomicInteger values = new AtomicInteger();
        final Metrics metrics = new Metrics() {
            @Override
            public void requestBegin(String method) {
                begun.incrementAndGet();
            }

            @Override
            public void requestEnd(String method, Code code, long elapsedNanos) {
                assertSame(Code.OK, code);
                ended.incrementAndGet();
            }

            @Override
            public void streamValue(String method) {
                values.incrementAndGet();
            }
        };
        try (VrpcClient counted = VrpcClient.builder().address(server.tcp()).metrics(metrics).build()) {
            try (ValueStream stream = counted.getStream("count", Value.list(Value.ofLong(7)), CallOptions.DEFAULT)) {
                for (Value v : stream) {
                    assertTrue(number(v) > 0);
                }
            }
            try (ValueSink sink = counted.putStream("upload", null, CallOptions.DEFAULT)) {
                sink.send(Value.ofLong(1));
                sink.send(Value.ofLong(2));
            }
            assertEquals(2, begun.get());
            assertEquals(2, ended.get());
            assertEquals(9, values.get());
        }
    }
}
