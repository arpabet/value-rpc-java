/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.quic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import com.arpabet.valuerpc.ValueSink;
import com.arpabet.valuerpc.ValueStream;
import com.arpabet.valuerpc.VrpcException;
import com.arpabet.valuerpc.client.ClientListener;
import com.arpabet.valuerpc.client.VrpcClient;
import io.netty.handler.codec.quic.QuicSslContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The Java client over QUIC against the Go server over QUIC
 * ({@code valuequic.NewServer}): the tests of the Go transport
 * ({@code quic/transport_test.go}), and the scenarios the client runs over
 * TCP. Skipped where there is no Go toolchain.
 */
@Timeout(120)
class GoQuicInteropTest {

    private static GoServer server;
    private static QuicSslContext tls;

    @BeforeAll
    static void start() throws Exception {
        assumeTrue(GoServer.available(), "no Go toolchain: the Go server cannot be built");
        assumeTrue(QuicTransport.isAvailable(), "no native QUIC library for this platform");
        server = GoServer.start();
        // The certificate of the server is signed by an authority of its own.
        tls = QuicTransport.clientTls().trustManager(server.ca()).build();
    }

    @AfterAll
    static void stop() {
        if (server != null) {
            server.close();
        }
    }

    private static VrpcClient.Builder clientOf(String address) {
        return VrpcClient.builder().timeoutMillis(5000)
                .dialer(QuicTransport.dialer(address, QuicOptions.DEFAULT.withSslContext(tls)));
    }

    private static long number(Value v) {
        return ((NumberValue) v).longValue();
    }

    // TestQUIC_RoundTrip, and the unary case of TestQUIC_Matrix
    @Test
    void aCall() {
        try (VrpcClient client = clientOf(server.quic()).build()) {
            client.connect();
            assertTrue(client.isActive());
            assertEquals(Value.utf8("Hello, quic!"), client.callFunction("greet", Value.list(Value.utf8("quic"))));
            assertEquals(Value.ofLong(81), client.callFunction("add", Value.list(Value.ofLong(40), Value.ofLong(41))));
            assertNull(client.callFunction("nothing", null));
            assertEquals(Value.utf8("Hello, quic!"), client.callFunction("greet", Value.list(Value.utf8("quic"))),
                    "an address with its scheme");
        }
        try (VrpcClient client = clientOf("quic://" + server.quic()).build()) {
            assertEquals(Value.ofLong(3), client.callFunction("add", Value.list(Value.ofLong(1), Value.ofLong(2))));
        }
        // The client in one call, as valuequic.NewClient.
        try (VrpcClient client = QuicTransport.client(server.quic(), QuicOptions.DEFAULT.withSslContext(tls)).build()) {
            assertEquals(Value.ofLong(7), client.callFunction("add", Value.list(Value.ofLong(3), Value.ofLong(4))));
        }
    }

    @Test
    void failuresComeBackWithTheirCodeAndLeaveTheConnection() {
        try (VrpcClient client = clientOf(server.quic()).build()) {
            assertSame(Code.RESOURCE_EXHAUSTED, assertThrows(VrpcException.class, () -> client.callFunction("fail", null)).code());
            assertSame(Code.NOT_FOUND, assertThrows(VrpcException.class, () -> client.callFunction("no.such", null)).code());
            assertSame(Code.INVALID_ARGUMENT, assertThrows(VrpcException.class,
                    () -> client.callFunction("typed", Value.list(Value.ofLong(1)))).code());
            assertSame(Code.DEADLINE_EXCEEDED, assertThrows(VrpcException.class,
                    () -> client.callFunction("slow", null, CallOptions.timeoutMillis(150))).code());
            assertEquals(Value.utf8("Hello, still!"), client.callFunction("greet", Value.list(Value.utf8("still"))));
            assertEquals(0L, client.stats().get("reconnects").longValue());
        }
    }

    // TestQUIC_Matrix: serverStream
    @Test
    void aStreamFromTheServer() throws Exception {
        try (VrpcClient client = clientOf(server.quic()).build()) {
            final List<Long> got = new ArrayList<Long>();
            try (ValueStream stream = client.getStream("count", Value.list(Value.ofLong(5)), CallOptions.DEFAULT)) {
                for (Value v : stream) {
                    got.add(number(v));
                }
            }
            assertEquals(Arrays.asList(1L, 2L, 3L, 4L, 5L), got);
            assertSame(Code.INVALID_ARGUMENT, assertThrows(VrpcException.class,
                    () -> client.getStream("count", Value.list(Value.ofLong(-1)), CallOptions.DEFAULT)).code());
        }
    }

    // TestQUIC_LargeServerStream
    @Test
    void aLongStreamFromTheServerLosesNothing() throws Exception {
        try (VrpcClient client = clientOf(server.quic()).streamWindow(128).build();
                ValueStream stream = client.getStream("count", Value.list(Value.ofLong(50_000)), CallOptions.DEFAULT)) {
            long count = 0;
            for (Value v : stream) {
                assertEquals(++count, number(v), "in order, none missing");
            }
            assertEquals(50_000, count);
        }
    }

    // TestQUIC_MultipleConcurrentStreams
    @Test
    void severalStreamsAtOnceEachGetTheirOwn() throws Exception {
        try (VrpcClient client = clientOf(server.quic()).streamWindow(32).build()) {
            final List<CompletableFuture<Long>> sums = new ArrayList<CompletableFuture<Long>>();
            for (int s = 1; s <= 8; s++) {
                final long n = 200 * s;
                sums.add(CompletableFuture.supplyAsync(() -> {
                    long sum = 0;
                    long expected = 0;
                    try (ValueStream stream = client.getStream("count", Value.list(Value.ofLong(n)), CallOptions.DEFAULT)) {
                        for (Value v : stream) {
                            assertEquals(++expected, number(v), "a value of another stream");
                            sum += number(v);
                        }
                    }
                    return sum;
                }));
            }
            for (int s = 1; s <= 8; s++) {
                final long n = 200 * s;
                assertEquals(n * (n + 1) / 2, sums.get(s - 1).get(60, TimeUnit.SECONDS).longValue());
            }
        }
    }

    // TestQUIC_Matrix: clientStream, and TestQUIC_LargePutStream
    @Test
    void aStreamToTheServer() throws Exception {
        try (VrpcClient client = clientOf(server.quic()).build()) {
            client.callFunction("upload.reset", null);
            final int n = 20_000;
            try (ValueSink sink = client.putStream("upload", null, CallOptions.DEFAULT)) {
                for (int i = 1; i <= n; i++) {
                    sink.send(Value.ofLong(i));
                }
            }
            MapValue stats = null;
            for (int i = 0; i < 500; i++) {
                stats = (MapValue) client.callFunction("upload.stats", null);
                if (stats.getNumber("count").longValue() >= n) {
                    break;
                }
                Thread.sleep(10);
            }
            assertEquals(n, stats.getNumber("count").longValue());
            assertEquals((long) n * (n + 1) / 2, stats.getNumber("sum").longValue());
        }
    }

    // TestQUIC_Matrix: chat
    @Test
    void aChat() throws Exception {
        try (VrpcClient client = clientOf(server.quic()).build();
                Chat chat = client.chat("chat.echo", null, CallOptions.DEFAULT)) {
            chat.outgoing().send(Value.utf8("a"));
            assertEquals(Value.utf8("echo:a"), chat.incoming().take());
            chat.outgoing().send(Value.utf8("bb"));
            chat.outgoing().send(Value.utf8("ccc"));
            chat.outgoing().close();
            final List<String> rest = new ArrayList<String>();
            for (Value v : chat.incoming()) {
                rest.add(v.toString());
            }
            assertEquals("[echo:bb, echo:ccc]", rest.toString());
        }
    }

    @Test
    void aChatUnderLoad() throws Exception {
        final int n = 10_000;
        try (VrpcClient client = clientOf(server.quic()).build();
                Chat chat = client.chat("chat.echo", null, CallOptions.DEFAULT)) {
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

    // TestQUIC_Concurrent
    @Test
    void manyCallsAtOnceEachGetTheirOwnAnswer() throws Exception {
        try (VrpcClient client = clientOf(server.quic()).build()) {
            final int workers = 32;
            final int callsEach = 25;
            final List<CompletableFuture<Void>> done = new ArrayList<CompletableFuture<Void>>();
            for (int w = 0; w < workers; w++) {
                final int base = w * callsEach;
                done.add(CompletableFuture.runAsync(() -> {
                    for (int i = 0; i < callsEach; i++) {
                        final long n = base + i;
                        assertEquals(Value.ofLong(n + n), client.callFunction("add", Value.list(Value.ofLong(n), Value.ofLong(n))),
                                "the answer of another call");
                    }
                }));
            }
            for (CompletableFuture<Void> f : done) {
                f.get(60, TimeUnit.SECONDS);
            }
            assertEquals(0L, client.stats().get("reconnects").longValue());
        }
    }

    @Test
    void aLargeMessageBothWays() {
        // Two megabytes: more than a stream may carry before it is granted more.
        final char[] text = new char[2 * 1024 * 1024];
        Arrays.fill(text, 'q');
        final Value big = Value.utf8(new String(text));
        try (VrpcClient client = clientOf(server.quic()).timeoutMillis(20_000).build()) {
            assertEquals(big, client.callFunction("echo", big));
        }
    }

    // TestQUIC_StreamsAreFreed
    @Test
    void streamsAreFreedSoThatAFewAtOnceAreEnough() throws Exception {
        // This listener allows eight requests at once on a connection: one
        // stream that is not freed, on either side, and the calls stop.
        try (VrpcClient client = clientOf(server.quicSmall()).build()) {
            for (long i = 0; i < 50; i++) {
                assertEquals(Value.ofLong(i + 1), client.callFunction("add", Value.list(Value.ofLong(i), Value.ofLong(1))), "call " + i);
            }
            // And of each kind of stream, when it runs to its end.
            for (int i = 0; i < 12; i++) {
                try (ValueStream stream = client.getStream("count", Value.list(Value.ofLong(3)), CallOptions.DEFAULT)) {
                    long count = 0;
                    for (Value v : stream) {
                        assertEquals(++count, number(v));
                    }
                    assertEquals(3, count, "stream " + i);
                }
                try (ValueSink sink = client.putStream("upload", null, CallOptions.DEFAULT)) {
                    sink.send(Value.ofLong(i));
                }
                try (Chat chat = client.chat("chat.echo", null, CallOptions.DEFAULT)) {
                    chat.outgoing().send(Value.utf8("x"));
                    chat.outgoing().close();
                    assertEquals(Value.utf8("echo:x"), chat.incoming().take());
                    assertNull(chat.incoming().take());
                }
                // And a call that fails, and one that times out here.
                assertThrows(VrpcException.class, () -> client.callFunction("fail", null));
            }
            assertEquals(Value.utf8("Hello, end!"), client.callFunction("greet", Value.list(Value.utf8("end"))));
            assertEquals(0L, client.stats().get("reconnects").longValue(), "on one connection all the way");
        }
    }

    @Test
    void moreCallsAtOnceThanStreamsAllowedWaitTheirTurn() throws Exception {
        try (VrpcClient client = clientOf(server.quicSmall()).timeoutMillis(20_000).build()) {
            final List<CompletableFuture<Value>> calls = new ArrayList<CompletableFuture<Value>>();
            for (long i = 0; i < 200; i++) {
                calls.add(client.callFunctionAsync("add", Value.list(Value.ofLong(i), Value.ofLong(1000)), CallOptions.DEFAULT));
            }
            for (int i = 0; i < calls.size(); i++) {
                assertEquals(Value.ofLong(i + 1000), calls.get(i).get(60, TimeUnit.SECONDS), "call " + i);
            }
            assertEquals(0L, client.stats().get("reconnects").longValue());
        }
    }

    @Test
    void aDroppedConnectionIsResumedAsTheSameSession() throws Exception {
        final AtomicInteger connected = new AtomicInteger();
        try (VrpcClient client = clientOf(server.quic()).listener(new ClientListener() {
            @Override
            public void onConnected(MapValue handshakeResponse) {
                connected.incrementAndGet();
            }
        }).build()) {
            assertEquals(Value.utf8("kicking"), client.callFunction("kick", null));
            // The server closes the connection; the client connects again and
            // shows the next link of its chain, which the server takes.
            final long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (connected.get() < 2) {
                assertTrue(System.nanoTime() < until, "the client did not reconnect");
                Thread.sleep(10);
            }
            assertEquals(Value.utf8("Hello, again!"), client.callFunction("greet", Value.list(Value.utf8("again"))));
            assertTrue(client.stats().get("reconnects") >= 1);
        }
    }

    @Test
    void aSilentConnectionIsKeptByThePingsOfTheGoServer() throws Exception {
        // This side says nothing to keep the connection, which it would give
        // up after six seconds of silence: the Go server, which pings at half
        // the idle time the two agreed on, is what keeps it. (Six seconds and
        // not less: the Go implementation takes less than five as five, and
        // its pings would then come too late for this side.)
        final QuicOptions quiet = QuicOptions.DEFAULT.withSslContext(tls).withIdleTimeoutMillis(6000).withKeepAliveMillis(0);
        final AtomicInteger lost = new AtomicInteger();
        try (VrpcClient client = VrpcClient.builder().timeoutMillis(5000)
                .dialer(QuicTransport.dialer(server.quic(), quiet))
                .listener(new ClientListener() {
                    @Override
                    public void onDisconnected(Throwable cause) {
                        lost.incrementAndGet();
                    }
                }).build()) {
            assertEquals(Value.ofLong(2), client.callFunction("add", Value.list(Value.ofLong(1), Value.ofLong(1))));
            Thread.sleep(14_000);
            assertTrue(client.isActive(), "still connected after more than twice the idle time");
            assertEquals(0, lost.get());
            assertEquals(Value.ofLong(4), client.callFunction("add", Value.list(Value.ofLong(2), Value.ofLong(2))));
            assertEquals(0L, client.stats().get("reconnects").longValue());
        }
    }

    // TestQUIC_MutualAuth
    @Test
    void theServerSeesTheCertificateOfTheClient() {
        final QuicSslContext withCertificate = QuicTransport.clientTls().trustManager(server.ca())
                .keyManager(server.clientKey(), null, server.clientCertificate()).build();
        try (VrpcClient client = VrpcClient.builder().timeoutMillis(5000)
                .dialer(QuicTransport.dialer(server.quicMutualTls(), QuicOptions.DEFAULT.withSslContext(withCertificate)))
                .build()) {
            assertEquals(Value.utf8("vrpc-conformance-client"), client.callFunction("tls.cn", null),
                    "the name on the certificate, as the server's authorizer read it");
        }
        // Without mutual TLS the server sees none.
        try (VrpcClient client = clientOf(server.quic()).build()) {
            assertEquals(Value.utf8(""), client.callFunction("tls.cn", null));
        }
    }

    // TestQUIC_RejectsClientWithoutCert
    @Test
    void aServerThatWantsACertificateRefusesAClientWithout() {
        try (VrpcClient client = clientOf(server.quicMutualTls()).dialTimeoutMillis(5000).build()) {
            final VrpcException refused = assertThrows(VrpcException.class, () -> {
                client.connect();
                client.callFunction("greet", null);
            });
            assertSame(Code.UNAVAILABLE, refused.code(), refused.getMessage());
            assertFalse(client.isActive());
        }
    }

    @Test
    void aServerOfAnotherAuthorityOrNameIsRefused() throws Exception {
        // The authorities of the system do not know the one of the test.
        try (VrpcClient client = VrpcClient.builder().dialTimeoutMillis(5000)
                .dialer(QuicTransport.dialer(server.quic(), QuicOptions.DEFAULT)).build()) {
            final VrpcException untrusted = assertThrows(VrpcException.class, client::connect);
            assertSame(Code.UNAVAILABLE, untrusted.code());
            assertTrue(untrusted.detail().startsWith("dial: quic: dial "), untrusted.detail());
        }
        // The certificate is for 127.0.0.1 and localhost, and for no other name.
        try (VrpcClient client = VrpcClient.builder().dialTimeoutMillis(5000)
                .dialer(QuicTransport.dialer(server.quic(), QuicOptions.DEFAULT.withSslContext(tls).withServerName("example.com")))
                .build()) {
            assertSame(Code.UNAVAILABLE, assertThrows(VrpcException.class, client::connect).code());
        }
        final String port = server.quic().substring(server.quic().lastIndexOf(':') + 1);
        try (VrpcClient client = clientOf("localhost:" + port).build()) {
            assertEquals(Value.utf8("Hello, name!"), client.callFunction("greet", Value.list(Value.utf8("name"))));
        }
    }
}
