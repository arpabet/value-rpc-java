/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.quic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.arpabet.valuerpc.FakeServer;
import com.arpabet.valuerpc.GoServer;
import com.arpabet.valuerpc.transport.Listener;
import com.arpabet.valuerpc.transport.MsgConn;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The Go client over QUIC ({@code valuequic.NewClient}) against a QUIC
 * listener of this implementation: the other direction of
 * {@link GoQuicInteropTest}. The client is {@code conformance/client}, built
 * and run as a process; the server is the one of the tests, which answers
 * handshakes and calls. Skipped where there is no Go toolchain.
 */
@Timeout(180)
class GoClientInteropTest {

    private static Certs certs;
    private static File ca;

    @BeforeAll
    static void start() throws Exception {
        assumeTrue(GoServer.available(), "no Go toolchain: the Go client cannot be built");
        assumeTrue(QuicTransport.isAvailable(), "no native QUIC library for this platform");
        certs = new Certs();
        ca = certs.server.toTempRootCertPem();
        ca.deleteOnExit();
        GoServer.build("client");
    }

    /**
     * Runs the Go client against an address with scenarios, and gives what it
     * said: how it ended, and what it wrote to its standard output, which is
     * where it says how each scenario went. What its libraries log is not part
     * of that and goes to the log of the test, as the log of the Go server
     * does: on Linux quic-go warns there that the receive buffer of its socket
     * is smaller than it would like.
     */
    private static String goClient(String address, List<String> more, String... scenarios) throws Exception {
        final List<String> command = new ArrayList<String>();
        command.add(GoServer.build("client").toString());
        command.addAll(Arrays.asList("-quic", address, "-ca", ca.getAbsolutePath()));
        command.addAll(more);
        command.addAll(Arrays.asList(scenarios));
        final Process p = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        p.getOutputStream().close();
        final ByteArrayOutputStream said = new ByteArrayOutputStream();
        try (InputStream in = p.getInputStream()) {
            final byte[] buf = new byte[4096];
            for (int n = in.read(buf); n >= 0; n = in.read(buf)) {
                said.write(buf, 0, n);
            }
        }
        if (!p.waitFor(120, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IOException("the Go client did not end: " + said);
        }
        return "exit " + p.exitValue() + "\n" + new String(said.toByteArray(), StandardCharsets.UTF_8).trim();
    }

    private static final List<String> NONE = new ArrayList<String>();

    @Test
    void aGoClientCallsAJavaListener() throws Exception {
        try (FakeServer server = new FakeServer(QuicTransport.listener("127.0.0.1:0", certs.server()))) {
            assertEquals("exit 0\nOK calls", goClient(server.listenerAddress(), NONE, "calls"));
            assertEquals(1, server.connections());
            // The handshake of a Go client, as it came over QUIC: before anything else.
            assertEquals("vRPC", server.handshakes.get(0).getString("m").toString());
            assertEquals(1, server.handshakes.size());
        }
    }

    @Test
    void theStreamsOfAGoClientAreFreed() throws Exception {
        // Eight requests at once and no more: fifty calls one after the other
        // need every stream to be freed, by the Go client and by this listener.
        try (FakeServer server = new FakeServer(QuicTransport.listener("127.0.0.1:0", certs.server().withMaxStreams(8)))) {
            assertEquals("exit 0\nOK calls\nOK sequential 50",
                    goClient(server.listenerAddress(), NONE, "calls", "sequential:50"));
            assertEquals(1, server.connections(), "on one connection all the way");
        }
    }

    @Test
    void manyCallsOfAGoClientAtOnce() throws Exception {
        try (FakeServer server = new FakeServer(QuicTransport.listener("127.0.0.1:0", certs.server()))) {
            assertEquals("exit 0\nOK concurrent 32x25\nOK sequential 200\nOK concurrent 8x100",
                    goClient(server.listenerAddress(), NONE, "concurrent:32x25", "sequential:200", "concurrent:8x100"));
            assertEquals(1, server.connections());
        }
    }

    @Test
    void aLargeMessageOfAGoClientBothWays() throws Exception {
        try (FakeServer server = new FakeServer(QuicTransport.listener("127.0.0.1:0", certs.server()))) {
            // Two megabytes: several times what a stream carries before it is granted more.
            assertEquals("exit 0\nOK big 2097152\nOK calls",
                    goClient(server.listenerAddress(), NONE, "big:2097152", "calls"));
        }
    }

    @Test
    void aGoClientShowsItsCertificate() throws Exception {
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
        final File cert = certs.client.toTempCertChainPem();
        final File key = certs.client.toTempPrivateKeyPem();
        cert.deleteOnExit();
        key.deleteOnExit();
        try (FakeServer server = new FakeServer(watching)) {
            assertEquals("exit 0\nOK calls", goClient(server.listenerAddress(),
                    Arrays.asList("-cert", cert.getAbsolutePath(), "-key", key.getAbsolutePath()), "calls"));
            final X509Certificate shown = (X509Certificate) accepted.get().tlsSession().getPeerCertificates()[0];
            assertEquals("CN=vrpc-test-client", shown.getSubjectX500Principal().getName());

            // One that shows none does not get in, and is told at once: the
            // listener closes the connection on it, which the Go client notices
            // since v1.7.1 and does not wait out the time it may connect in.
            final long start = System.nanoTime();
            final String refused = goClient(server.listenerAddress(), NONE, "calls");
            final long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(refused.startsWith("exit 1\nFAILED connect"), refused);
            assertTrue(refused.contains("certificate required"), refused);
            assertTrue(took < 5000, "the refusal took " + took + " ms to reach the Go client");
            assertEquals(1, server.connections());
        }
    }

    @Test
    void aGoClientDoesNotTrustAnotherAuthority() throws Exception {
        final Certs others = new Certs();
        try (FakeServer server = new FakeServer(QuicTransport.listener("127.0.0.1:0", others.server()))) {
            final String refused = goClient(server.listenerAddress(), NONE, "calls");
            assertTrue(refused.startsWith("exit 1\nFAILED connect"), refused);
            assertEquals(0, server.connections());
        }
    }
}
