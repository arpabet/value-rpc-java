/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.quic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.value.MapValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.Dialect;
import com.arpabet.valuerpc.FakeServer;
import com.arpabet.valuerpc.MessageType;
import com.arpabet.valuerpc.VrpcException;
import com.arpabet.valuerpc.client.ClientListener;
import com.arpabet.valuerpc.client.VrpcClient;
import com.arpabet.valuerpc.transport.Dialer;
import com.arpabet.valuerpc.transport.Listener;
import com.arpabet.valuerpc.transport.MsgConn;
import com.arpabet.valuerpc.transport.TransportOptions;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The client over QUIC against a server of the test's own on a QUIC listener,
 * both of them this implementation: what the transport is set up with, and
 * what the client makes of it. No Go is needed.
 */
@Timeout(60)
class QuicClientTest {

    private static final Dialect D = Dialect.standard();
    private static Certs certs;

    @BeforeAll
    static void certificates() throws Exception {
        certs = new Certs();
    }

    private static FakeServer serverWith(QuicOptions options) throws IOException {
        return new FakeServer(QuicTransport.listener("127.0.0.1:0", options));
    }

    private static VrpcClient.Builder clientOf(FakeServer server, QuicOptions options) {
        return VrpcClient.builder().timeoutMillis(5000).dialTimeoutMillis(5000)
                .dialer(QuicTransport.dialer(server.listenerAddress(), options));
    }

    /** A dialer that remembers the connections it made, so that a test can look at one. */
    private static final class Watched implements Dialer {
        final Dialer dialer;
        final List<QuicConn> conns = new CopyOnWriteArrayList<QuicConn>();

        Watched(Dialer dialer) {
            this.dialer = dialer;
        }

        @Override
        public MsgConn dial(long timeoutMillis) throws IOException {
            final QuicConn conn = (QuicConn) dialer.dial(timeoutMillis);
            conns.add(conn);
            return conn;
        }
    }

    private static void waitFor(java.util.function.BooleanSupplier what, String forWhat) throws InterruptedException {
        final long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!what.getAsBoolean()) {
            if (System.nanoTime() > until) {
                throw new AssertionError("waited too long for " + forWhat);
            }
            Thread.sleep(5);
        }
    }

    @Test
    void callsOverQuic() throws Exception {
        try (FakeServer server = serverWith(certs.server());
                VrpcClient client = clientOf(server, certs.client()).build()) {
            assertFalse(client.isActive(), "nothing is dialed before it is needed");
            assertEquals(Value.utf8("Hello, quic!"), client.callFunction("greet", Value.list(Value.utf8("quic"))));
            assertTrue(client.isActive());
            assertEquals(Value.ofLong(7), client.callFunction("echo", Value.ofLong(7)));
            assertSame(Code.RESOURCE_EXHAUSTED, assertThrows(VrpcException.class, () -> client.callFunction("fail", null)).code());
            assertSame(Code.NOT_FOUND, assertThrows(VrpcException.class, () -> client.callFunction("no.such", null)).code());
            assertEquals(1, server.connections(), "one connection carries every call");
            // What the server was sent is what a client sends over any transport.
            assertEquals(D.request(MessageType.FUNCTION_REQUEST, 1, "greet", Value.list(Value.utf8("quic")), 5000, null),
                    server.next(2000));
        }
    }

    @Test
    void manyCallsAtOnceAndEveryStreamIsFreed() throws Exception {
        final Watched dialer;
        try (FakeServer server = serverWith(certs.server())) {
            dialer = new Watched(QuicTransport.dialer(server.listenerAddress(), certs.client()));
            try (VrpcClient client = VrpcClient.builder().timeoutMillis(10_000).dialer(dialer).build()) {
                final List<CompletableFuture<Value>> calls = new ArrayList<CompletableFuture<Value>>();
                for (long i = 0; i < 500; i++) {
                    calls.add(client.callFunctionAsync("echo", Value.ofLong(i), CallOptions.DEFAULT));
                }
                for (int i = 0; i < calls.size(); i++) {
                    assertEquals(Value.ofLong(i), calls.get(i).get(30, TimeUnit.SECONDS));
                }
                assertEquals(1, dialer.conns.size());
                Ends.awaitStreams(dialer.conns.get(0), 0);
                assertEquals(0L, client.stats().get("pending").longValue());
            }
        }
    }

    @Test
    void aLargeMessageBothWays() throws Exception {
        final char[] text = new char[4 * 1024 * 1024];
        Arrays.fill(text, 'q');
        final Value big = Value.utf8(new String(text));
        try (FakeServer server = serverWith(certs.server());
                VrpcClient client = clientOf(server, certs.client()).timeoutMillis(30_000).build()) {
            assertEquals(big, client.callFunction("echo", big));
        }
    }

    @Test
    void aTinyLimitOfStreamsIsEnoughForCallsOneAfterTheOther() throws Exception {
        // Eight requests at once, as the test of the Go transport has it: a
        // stream that is not freed, on either side, and the calls stop.
        try (FakeServer server = serverWith(certs.server().withMaxStreams(8));
                VrpcClient client = clientOf(server, certs.client()).build()) {
            for (long i = 0; i < 50; i++) {
                assertEquals(Value.ofLong(i), client.callFunction("echo", Value.ofLong(i)), "call " + i);
            }
            assertEquals(1, server.connections());
        }
    }

    @Test
    void aRequestThatIsOneTooManyWaitsUntilAStreamIsFree() throws Exception {
        try (FakeServer server = serverWith(certs.server().withMaxStreams(4))) {
            server.mute = true; // it takes the requests and answers when the test says so
            try (VrpcClient client = clientOf(server, certs.client()).timeoutMillis(20_000).build()) {
                client.connect();
                final List<CompletableFuture<Value>> calls = new ArrayList<CompletableFuture<Value>>();
                for (long i = 1; i <= 6; i++) {
                    calls.add(client.callFunctionAsync("echo", Value.ofLong(i), CallOptions.DEFAULT));
                }
                // Four requests have a stream; two wait for one.
                final List<MapValue> arrived = new ArrayList<MapValue>();
                for (int i = 0; i < 4; i++) {
                    final MapValue request = server.next(5000);
                    assertNotNull(request, "request " + i);
                    arrived.add(request);
                }
                assertEquals(null, server.next(300), "the fifth has no stream to travel on");
                // Two are answered: their streams are freed, and the two that waited go out.
                for (int i = 0; i < 2; i++) {
                    server.push(D.functionResult(D.requestId(arrived.get(i)), D.arguments(arrived.get(i))));
                }
                for (int i = 0; i < 2; i++) {
                    final MapValue request = server.next(5000);
                    assertNotNull(request, "a request that waited");
                    arrived.add(request);
                }
                for (int i = 2; i < arrived.size(); i++) {
                    server.push(D.functionResult(D.requestId(arrived.get(i)), D.arguments(arrived.get(i))));
                }
                for (int i = 0; i < calls.size(); i++) {
                    assertEquals(Value.ofLong(i + 1), calls.get(i).get(20, TimeUnit.SECONDS));
                }
                assertEquals(1, server.connections(), "it waited; it did not connect again");
            }
        }
    }

    @Test
    void aRequestThatFindsNoStreamInTimeGivesTheConnectionUp() throws Exception {
        // A server that frees no stream: the write that waits for one has the
        // write timeout of the transport, and then the connection is replaced.
        final QuicOptions impatient = certs.client().withTransport(TransportOptions.DEFAULT.withWriteTimeoutMillis(300));
        try (FakeServer server = serverWith(certs.server().withMaxStreams(2))) {
            server.mute = true;
            final AtomicInteger lost = new AtomicInteger();
            try (VrpcClient client = clientOf(server, impatient).timeoutMillis(10_000).listener(new ClientListener() {
                @Override
                public void onDisconnected(Throwable cause) {
                    lost.incrementAndGet();
                }
            }).build()) {
                client.connect();
                final List<CompletableFuture<Value>> calls = new ArrayList<CompletableFuture<Value>>();
                for (long i = 1; i <= 3; i++) {
                    calls.add(client.callFunctionAsync("echo", Value.ofLong(i), CallOptions.DEFAULT));
                }
                waitFor(() -> lost.get() >= 1, "the connection to be given up");
                for (CompletableFuture<Value> call : calls) {
                    try {
                        call.get(10, TimeUnit.SECONDS);
                    } catch (java.util.concurrent.ExecutionException e) {
                        assertSame(Code.UNAVAILABLE, ((VrpcException) e.getCause()).code());
                    }
                }
                waitFor(() -> server.connections() >= 2, "the client to connect again");
            }
        }
    }

    @Test
    void aDroppedConnectionIsDialedAgainAndResumed() throws Exception {
        try (FakeServer server = serverWith(certs.server());
                VrpcClient client = clientOf(server, certs.client()).build()) {
            assertEquals(Value.ofLong(1), client.callFunction("echo", Value.ofLong(1)));
            server.drop();
            waitFor(() -> server.connections() >= 2, "the client to connect again");
            assertEquals(Value.ofLong(2), client.callFunction("echo", Value.ofLong(2)));
            assertEquals(2, server.handshakes.size());
            // The second handshake shows the next link of the chain, not the anchor again.
            assertFalse(server.handshakes.get(0).getString("tok").equals(server.handshakes.get(1).getString("tok")));
            assertTrue(client.stats().get("reconnects") >= 1);
        }
    }

    @Test
    void aMessageLongerThanTheServerTakesFailsAtOnceAndTheClientGoesOn() throws Exception {
        final QuicOptions strict = certs.server().withTransport(TransportOptions.DEFAULT.withMaxFrameSize(512));
        try (FakeServer server = serverWith(strict);
                VrpcClient client = clientOf(server, certs.client()).build()) {
            assertEquals(Value.utf8("small"), client.callFunction("echo", Value.utf8("small")));
            final char[] text = new char[4096];
            Arrays.fill(text, 'x');
            final long start = System.nanoTime();
            final VrpcException refused = assertThrows(VrpcException.class,
                    () -> client.callFunction("echo", Value.utf8(new String(text))));
            assertSame(Code.UNAVAILABLE, refused.code(), "the connection was closed on it: not a timeout");
            assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(4), "and at once");
            assertEquals(Value.utf8("again"), client.callFunction("echo", Value.utf8("again")), "on a new connection");
        }
    }

    @Test
    void mutualTlsShowsTheServerTheCertificateOfTheClient() throws Exception {
        final AtomicReference<MsgConn> accepted = new AtomicReference<MsgConn>();
        final Listener quic = QuicTransport.listener("127.0.0.1:0", QuicOptions.DEFAULT.withSslContext(certs.serverMutualTls()));
        final Listener watching = new Listener() {
            @Override
            public void start(Acceptor acceptor) {
                quic.start(conn -> {
                    accepted.set(conn);
                    acceptor.onConnection(conn);
                });
            }

            @Override
            public String address() {
                return quic.address();
            }

            @Override
            public void close() {
                quic.close();
            }
        };
        try (FakeServer server = new FakeServer(watching)) {
            try (VrpcClient client = clientOf(server, QuicOptions.DEFAULT.withSslContext(certs.clientTlsWithCertificate())).build()) {
                assertEquals(Value.utf8("Hello, mtls!"), client.callFunction("greet", Value.list(Value.utf8("mtls"))));
                final X509Certificate shown = (X509Certificate) accepted.get().tlsSession().getPeerCertificates()[0];
                assertEquals("CN=vrpc-test-client", shown.getSubjectX500Principal().getName());
            }
            // A client that shows none is not let in.
            try (VrpcClient client = clientOf(server, certs.client()).build()) {
                final VrpcException refused = assertThrows(VrpcException.class, () -> {
                    client.connect();
                    client.callFunction("greet", null);
                });
                assertSame(Code.UNAVAILABLE, refused.code(), refused.getMessage());
            }
            assertEquals(1, server.connections(), "only the one with a certificate was handed over");
        }
    }

    @Test
    void aServerOfAnotherAuthorityOrNameIsNotTrusted() throws Exception {
        final Certs others = new Certs();
        try (FakeServer server = serverWith(certs.server())) {
            try (VrpcClient client = clientOf(server, others.client()).build()) {
                final VrpcException untrusted = assertThrows(VrpcException.class, client::connect);
                assertSame(Code.UNAVAILABLE, untrusted.code());
                assertTrue(untrusted.detail().startsWith("dial: quic: dial " + server.listenerAddress()), untrusted.detail());
            }
            try (VrpcClient client = clientOf(server, certs.client().withServerName("example.com")).build()) {
                assertSame(Code.UNAVAILABLE, assertThrows(VrpcException.class, client::connect).code());
            }
            // The name the certificate is for, given for an address it is not for.
            try (VrpcClient client = clientOf(server, certs.client().withServerName("localhost")).build()) {
                assertEquals(Value.ofLong(1), client.callFunction("echo", Value.ofLong(1)));
            }
            assertEquals(1, server.connections());
        }
    }

    @Test
    void aServerThatIsNotThereIsFoundOutByTheLimitOfTheHandshake() throws Exception {
        final String nobody;
        try (FakeServer server = serverWith(certs.server())) {
            nobody = server.listenerAddress();
        }
        try (VrpcClient client = VrpcClient.builder().dialTimeoutMillis(30_000)
                .dialer(QuicTransport.dialer(nobody, certs.client().withHandshakeTimeoutMillis(400))).build()) {
            final long start = System.nanoTime();
            final VrpcException gone = assertThrows(VrpcException.class, client::connect);
            assertSame(Code.UNAVAILABLE, gone.code());
            assertTrue(gone.detail().contains("no answer in 400 ms"), gone.detail());
            assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10));
        }
        // The limit of the dial applies when it is the shorter.
        try (VrpcClient client = VrpcClient.builder().dialTimeoutMillis(250)
                .dialer(QuicTransport.dialer(nobody, certs.client())).build()) {
            assertTrue(assertThrows(VrpcException.class, client::connect).detail().contains("no answer in 250 ms"));
        }
    }

    @Test
    void aConnectionThatIsSilentIsKeptWhileBothEndsSayTheyAreThere() throws Exception {
        // Given up after 600 ms of silence, and made to speak every 150 ms.
        final QuicOptions s = certs.server().withIdleTimeoutMillis(600).withKeepAliveMillis(150);
        final QuicOptions c = certs.client().withIdleTimeoutMillis(600).withKeepAliveMillis(150);
        try (FakeServer server = serverWith(s);
                VrpcClient client = clientOf(server, c).build()) {
            assertEquals(Value.ofLong(1), client.callFunction("echo", Value.ofLong(1)));
            Thread.sleep(2000);
            assertTrue(client.isActive(), "still the same connection");
            assertEquals(Value.ofLong(2), client.callFunction("echo", Value.ofLong(2)));
            assertEquals(1, server.connections());
            assertEquals(0L, client.stats().get("reconnects").longValue());
        }
    }

    @Test
    void aConnectionThatIsSilentAndNotKeptIsGivenUpAndDialedAgain() throws Exception {
        final QuicOptions s = certs.server().withIdleTimeoutMillis(400).withKeepAliveMillis(0);
        final QuicOptions c = certs.client().withIdleTimeoutMillis(400).withKeepAliveMillis(0);
        final List<String> why = new CopyOnWriteArrayList<String>();
        try (FakeServer server = serverWith(s);
                VrpcClient client = clientOf(server, c).listener(new ClientListener() {
                    @Override
                    public void onDisconnected(Throwable cause) {
                        why.add(String.valueOf(cause));
                    }
                }).build()) {
            assertEquals(Value.ofLong(1), client.callFunction("echo", Value.ofLong(1)));
            waitFor(() -> !why.isEmpty(), "the idle connection to be given up");
            assertTrue(why.get(0).contains("timed out"), why.get(0));
            // The client connects again by itself, and calls go on.
            assertEquals(Value.ofLong(2), client.callFunction("echo", Value.ofLong(2)));
            assertTrue(server.connections() >= 2);
        }
    }

    @Test
    void aListenerThatIsClosedKeepsItsConnectionsAndTakesNoNewOnes() throws Exception {
        final Listener listener = QuicTransport.listener("127.0.0.1:0", certs.server());
        try (FakeServer server = new FakeServer(listener);
                VrpcClient first = clientOf(server, certs.client()).build()) {
            assertEquals(Value.ofLong(1), first.callFunction("echo", Value.ofLong(1)));
            listener.close();
            listener.close();
            assertEquals(Value.ofLong(2), first.callFunction("echo", Value.ofLong(2)), "the connection it handed over is as it was");
            try (VrpcClient second = VrpcClient.builder().dialTimeoutMillis(2000)
                    .dialer(QuicTransport.dialer(listener.address(), certs.client().withHandshakeTimeoutMillis(1500))).build()) {
                assertSame(Code.UNAVAILABLE, assertThrows(VrpcException.class, second::connect).code());
            }
            assertEquals(1, server.connections());
            assertEquals(Value.ofLong(3), first.callFunction("echo", Value.ofLong(3)));
        }
    }

    @Test
    void eventLoopsOfOnesOwn() throws Exception {
        final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
        try {
            final QuicOptions s = certs.server().withEventLoopGroup(group, NioDatagramChannel.class);
            final QuicOptions c = certs.client().withEventLoopGroup(group, NioDatagramChannel.class);
            try (FakeServer server = serverWith(s);
                    VrpcClient client = clientOf(server, c).build()) {
                assertEquals(Value.ofLong(1), client.callFunction("echo", Value.ofLong(1)));
            }
            assertFalse(group.isShuttingDown(), "the transport does not shut down what it was given");
        } finally {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        }
        assertThrows(IllegalArgumentException.class, () -> QuicOptions.DEFAULT.withEventLoopGroup(group, null));
        assertThrows(IllegalArgumentException.class, () -> QuicOptions.DEFAULT.withEventLoopGroup(null, NioDatagramChannel.class));
    }

    @Test
    void whatATransportCannotBeMadeOf() throws Exception {
        // A listener has to have the certificate of a server.
        assertTrue(assertThrows(IOException.class, () -> QuicTransport.listener("127.0.0.1:0", QuicOptions.DEFAULT))
                .getMessage().contains("requires a TLS context"));
        assertThrows(IOException.class, () -> QuicTransport.listener("127.0.0.1:0", certs.client()));
        assertThrows(IOException.class, () -> QuicTransport.listener("127.0.0.1", certs.server()), "no port");
        assertThrows(IOException.class, () -> QuicTransport.listener("tcp://127.0.0.1:0", certs.server()), "another scheme");
        // A dialer says so when it dials, so that a client is told when it connects.
        assertThrows(IOException.class, () -> QuicTransport.dialer("127.0.0.1:1", certs.server()).dial(1000), "the context of a server");
        assertThrows(IOException.class, () -> QuicTransport.dialer("127.0.0.1:x", certs.client()).dial(1000));
        assertThrows(IOException.class, () -> QuicTransport.dialer("host.invalid:9000", certs.client()).dial(1000));
        assertEquals("quic://example.com:9000", QuicTransport.dialer("example.com:9000", QuicOptions.DEFAULT).toString());
        assertEquals("quic://example.com:9000", QuicTransport.dialer("quic://example.com:9000", QuicOptions.DEFAULT).toString());
        assertThrows(IllegalArgumentException.class, () -> QuicOptions.DEFAULT.withMaxStreams(0));
        assertThrows(NullPointerException.class, () -> QuicOptions.DEFAULT.withTransport(null));
        assertThrows(NullPointerException.class, () -> QuicOptions.DEFAULT.withDialect(null));
    }

    @Test
    void theDefaultsAreThoseOfTheGoImplementation() {
        final QuicOptions o = QuicOptions.DEFAULT;
        assertEquals("vrpc", QuicTransport.ALPN, "the protocol of the TLS handshake");
        assertEquals(1 << 16, o.maxStreams(), "MaxStreams");
        assertEquals(30_000, o.idleTimeoutMillis(), "MaxIdleTimeout");
        assertEquals(15_000, o.keepAliveMillis(), "KeepAlivePeriod");
        assertEquals(10_000, o.transport().writeTimeoutMillis());
        assertEquals(16 * 1024 * 1024, o.transport().maxFrameSize());
        assertSame(Dialect.standard(), o.dialect());
        assertEquals(null, o.sslContext());
        assertEquals(null, o.serverName());
        // Each with gives another, and leaves the one it was made of.
        final QuicOptions other = o.withMaxStreams(8).withIdleTimeoutMillis(1).withKeepAliveMillis(2).withHandshakeTimeoutMillis(3)
                .withServerName("n").withDialect(Dialect.standard()).withTransport(TransportOptions.DEFAULT.withMaxFrameSize(9));
        assertEquals(8, other.maxStreams());
        assertEquals(1, other.idleTimeoutMillis());
        assertEquals(2, other.keepAliveMillis());
        assertEquals(3, other.handshakeTimeoutMillis());
        assertEquals("n", other.serverName());
        assertEquals(9, other.transport().maxFrameSize());
        assertEquals(1 << 16, o.maxStreams());
        assertEquals(10_000, o.handshakeTimeoutMillis());
    }
}
