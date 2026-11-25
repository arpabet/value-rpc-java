/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.arpabet.value.MapValue;
import com.arpabet.value.NumberValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.GoServer;
import com.arpabet.valuerpc.ValueStream;
import com.arpabet.valuerpc.VrpcException;
import com.arpabet.valuerpc.transport.Dialer;
import com.arpabet.valuerpc.transport.TransportOptions;
import com.arpabet.valuerpc.transport.Transports;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * TLS before the library has a transport for it: a client over a TLS socket
 * of the application's own making, given to it as a stream, against the TLS
 * listener of the Go server ({@code valueserver.NewTLSServer}). The dialer
 * here is the one the README shows, line for line. Skipped where there is no
 * Go toolchain.
 */
@Timeout(60)
class GoTlsByOwnStreamTest {

    private static GoServer server;
    private static SSLSocketFactory trusting;

    @BeforeAll
    static void start() throws Exception {
        assumeTrue(GoServer.available(), "no Go toolchain: the Go server cannot be built");
        server = GoServer.start();
        // A context that trusts the authority of the server of the test. An
        // application whose server has a certificate of a public authority
        // takes SSLSocketFactory.getDefault().
        final KeyStore authorities = KeyStore.getInstance(KeyStore.getDefaultType());
        authorities.load(null, null);
        authorities.setCertificateEntry("ca", server.ca());
        final TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust.init(authorities);
        final SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, trust.getTrustManagers(), null);
        trusting = context.getSocketFactory();
    }

    @AfterAll
    static void stop() {
        if (server != null) {
            server.close();
        }
    }

    /** The dialer of the README: TLS to {@code host:port}, the certificate checked against the host. */
    private static Dialer tls(SSLSocketFactory factory, String host, int port) {
        return Transports.dialer(timeoutMillis -> {
            final int timeout = (int) Math.max(0, Math.min(timeoutMillis, Integer.MAX_VALUE));
            final Socket plain = new Socket();
            try {
                plain.connect(new InetSocketAddress(host, port), timeout);
                plain.setTcpNoDelay(true);
                plain.setSoTimeout(timeout); // bounds the handshake
                final SSLSocket socket = (SSLSocket) factory.createSocket(plain, host, port, true);
                final SSLParameters parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS"); // the certificate has to be for the host
                socket.setSSLParameters(parameters);
                socket.startHandshake();
                socket.setSoTimeout(0); // a connection may be silent for as long as it likes
                return new Transports.Stream(socket.getInputStream(), socket.getOutputStream(), socket, host + ":" + port);
            } catch (IOException | RuntimeException e) {
                plain.close();
                throw e;
            }
        }, TransportOptions.DEFAULT);
    }

    private static int port() {
        return Integer.parseInt(server.tls().substring(server.tls().lastIndexOf(':') + 1));
    }

    @Test
    void callsAndStreamsOverTls() throws Exception {
        try (VrpcClient client = VrpcClient.builder().dialer(tls(trusting, "127.0.0.1", port())).timeoutMillis(5000).build()) {
            assertEquals(Value.utf8("Hello, tls!"), client.callFunction("greet", Value.list(Value.utf8("tls"))));
            assertSame(Code.RESOURCE_EXHAUSTED, assertThrows(VrpcException.class, () -> client.callFunction("fail", null)).code());
            long sum = 0;
            try (ValueStream stream = client.getStream("count", Value.list(Value.ofLong(10_000)), CallOptions.DEFAULT)) {
                for (Value v : stream) {
                    sum += ((NumberValue) v).longValue();
                }
            }
            assertEquals(50_005_000L, sum);
        }
        // The certificate is for the name too.
        try (VrpcClient client = VrpcClient.builder().dialer(tls(trusting, "localhost", port())).timeoutMillis(5000).build()) {
            assertEquals(Value.ofLong(3), client.callFunction("add", Value.list(Value.ofLong(1), Value.ofLong(2))));
        }
    }

    @Test
    void theConnectionIsTls13AndItsCertificateTheServers() throws Exception {
        final AtomicReference<SSLSocket> seen = new AtomicReference<SSLSocket>();
        final int port = port();
        final Dialer watching = Transports.dialer(timeoutMillis -> {
            final Socket plain = new Socket();
            plain.connect(new InetSocketAddress("127.0.0.1", port), (int) timeoutMillis);
            final SSLSocket socket = (SSLSocket) trusting.createSocket(plain, "127.0.0.1", port, true);
            socket.startHandshake();
            seen.set(socket);
            return new Transports.Stream(socket.getInputStream(), socket.getOutputStream(), socket, "127.0.0.1:" + port);
        }, TransportOptions.DEFAULT);
        try (VrpcClient client = VrpcClient.builder().dialer(watching).timeoutMillis(5000).build()) {
            assertEquals(Value.utf8("Hello, tls!"), client.callFunction("greet", Value.list(Value.utf8("tls"))));
            assertEquals("TLSv1.3", seen.get().getSession().getProtocol());
            assertEquals(server.ca().getSubjectX500Principal(),
                    ((java.security.cert.X509Certificate) seen.get().getSession().getPeerCertificates()[0]).getIssuerX500Principal());
        }
    }

    @Test
    void aServerOfAnAuthorityThatIsNotTrustedOrOfAnotherNameIsRefused() {
        // The authorities of the system do not know the one of the test.
        final SSLSocketFactory system = (SSLSocketFactory) SSLSocketFactory.getDefault();
        try (VrpcClient client = VrpcClient.builder().dialer(tls(system, "127.0.0.1", port())).dialTimeoutMillis(5000).build()) {
            final VrpcException untrusted = assertThrows(VrpcException.class, client::connect);
            assertSame(Code.UNAVAILABLE, untrusted.code());
            assertTrue(untrusted.detail().startsWith("dial: "), untrusted.detail());
        }
        // The certificate is for 127.0.0.1 and localhost: checked against another name, it is refused.
        final int port = port();
        final Dialer wrongName = Transports.dialer(timeoutMillis -> {
            final Socket plain = new Socket();
            try {
                plain.connect(new InetSocketAddress("127.0.0.1", port), (int) timeoutMillis);
                final SSLSocket socket = (SSLSocket) trusting.createSocket(plain, "example.com", port, true);
                final SSLParameters parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                socket.setSSLParameters(parameters);
                socket.startHandshake();
                return new Transports.Stream(socket.getInputStream(), socket.getOutputStream(), socket, "example.com:" + port);
            } catch (IOException | RuntimeException e) {
                plain.close();
                throw e;
            }
        }, TransportOptions.DEFAULT);
        try (VrpcClient client = VrpcClient.builder().dialer(wrongName).dialTimeoutMillis(5000).build()) {
            assertSame(Code.UNAVAILABLE, assertThrows(VrpcException.class, client::connect).code());
        }
        // It is that one line that checks the name: without it a socket takes
        // any certificate of an authority it trusts, whoever it is for.
        final Dialer unchecked = Transports.dialer(timeoutMillis -> {
            final Socket plain = new Socket();
            plain.connect(new InetSocketAddress("127.0.0.1", port), (int) timeoutMillis);
            final SSLSocket socket = (SSLSocket) trusting.createSocket(plain, "example.com", port, true);
            socket.startHandshake();
            return new Transports.Stream(socket.getInputStream(), socket.getOutputStream(), socket, "example.com:" + port);
        }, TransportOptions.DEFAULT);
        try (VrpcClient client = VrpcClient.builder().dialer(unchecked).timeoutMillis(5000).build()) {
            assertEquals(Value.utf8("Hello, anyone!"), client.callFunction("greet", Value.list(Value.utf8("anyone"))));
        }
    }

    @Test
    void aDroppedConnectionIsDialedAgainOverTls() throws Exception {
        final AtomicInteger connected = new AtomicInteger();
        try (VrpcClient client = VrpcClient.builder().dialer(tls(trusting, "127.0.0.1", port())).timeoutMillis(5000)
                .listener(new ClientListener() {
                    @Override
                    public void onConnected(MapValue handshakeResponse) {
                        connected.incrementAndGet();
                    }
                }).build()) {
            assertEquals(Value.utf8("kicking"), client.callFunction("kick", null));
            final long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (connected.get() < 2) {
                assertTrue(System.nanoTime() < until, "the client did not reconnect");
                Thread.sleep(10);
            }
            assertEquals(Value.utf8("Hello, again!"), client.callFunction("greet", Value.list(Value.utf8("again"))));
        }
    }
}
