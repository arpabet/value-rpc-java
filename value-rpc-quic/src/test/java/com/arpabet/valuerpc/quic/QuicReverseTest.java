/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.quic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.arpabet.value.ListValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.ConformanceServer;
import com.arpabet.valuerpc.GoServer;
import com.arpabet.valuerpc.TypeDef;
import com.arpabet.valuerpc.client.VrpcClient;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A QUIC connection used the other way: the server asks its client, each of
 * its requests on a stream the server opens. The tests of
 * {@code quic/reverse_test.go} in the Go implementation, with a server and a
 * client of this library, and each against the Go one. Skipped where there is
 * no native QUIC library; the ones with Go, where there is no Go toolchain.
 */
@Timeout(120)
class QuicReverseTest {

    private static Certs certs;

    @BeforeAll
    static void start() throws Exception {
        assumeTrue(QuicTransport.isAvailable(), "no native QUIC library for this platform");
        certs = new Certs();
    }

    /** What the clients of the conformance suites serve; gives what {@code ingest} took. */
    private static List<Value> serve(VrpcClient client) {
        final List<Value> ingested = Collections.synchronizedList(new ArrayList<Value>());
        client.addFunction("notify", (ctx, args) -> Value.utf8("client-saw:" + ((ListValue) args).get(0)));
        client.addOutgoingStream("tail", TypeDef.ANY, (ctx, args, out) -> {
            final long n = ((ListValue) args).getNumber(0).longValue();
            for (long i = 0; i < n; i++) {
                out.send(Value.utf8("t" + i));
            }
        });
        client.addIncomingStream("ingest", TypeDef.ANY, (ctx, args, in) -> {
            for (Value v : in) {
                ingested.add(v);
            }
        });
        return ingested;
    }

    private static void asked(VrpcClient client, List<Value> ingested) throws InterruptedException {
        assertEquals(Value.utf8("client-saw:ping"), client.callFunction("reverse.call", Value.list(Value.utf8("ping"))));
        // More than a window of credit, each value a message on the stream the server opened.
        final ListValue pulled = (ListValue) client.callFunction("reverse.pull", Value.list(Value.ofLong(6000)));
        assertEquals(6000, pulled.size());
        assertEquals(Value.utf8("t5999"), pulled.get(5999));
        assertEquals(Value.utf8("pushed"), client.callFunction("reverse.push", Value.list(Value.utf8("go"))));
        final long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (ingested.size() < 3) {
            assertTrue(System.nanoTime() < until, "ingest took " + ingested.size() + " of 3 values");
            Thread.sleep(5);
        }
        assertEquals(Arrays.asList(Value.ofLong(100), Value.ofLong(200), Value.ofLong(300)), ingested);
        // Many requests, each on a stream of its own that is freed when it is over.
        for (int i = 0; i < 200; i++) {
            assertEquals(Value.utf8("client-saw:" + i), client.callFunction("reverse.call", Value.list(Value.ofLong(i))));
        }
        assertEquals(Value.utf8("Hello, still!"), client.callFunction("greet", Value.list(Value.utf8("still"))));
    }

    @Test
    void aServerOfThisLibraryAsksItsClient() throws Exception {
        try (ConformanceServer server = ConformanceServer.start(QuicTransport.listener("127.0.0.1:0", certs.server()),
                new ConformanceServer.Shared());
                VrpcClient client = VrpcClient.builder().timeoutMillis(10_000)
                        .dialer(QuicTransport.dialer(server.address(), certs.client())).build()) {
            asked(client, serve(client));
        }
    }

    @Test
    void theGoServerAsksAClientOfThisLibrary() throws Exception {
        assumeTrue(GoServer.available(), "no Go toolchain: the Go server cannot be built");
        try (GoServer server = GoServer.start();
                VrpcClient client = VrpcClient.builder().timeoutMillis(10_000).dialer(QuicTransport.dialer(server.quic(),
                        QuicOptions.DEFAULT.withSslContext(QuicTransport.clientTls().trustManager(server.ca()).build()))).build()) {
            asked(client, serve(client));
        }
    }

    @Test
    void aServerOfThisLibraryAsksAGoClient() throws Exception {
        assumeTrue(GoServer.available(), "no Go toolchain: the Go client cannot be built");
        final File ca = certs.server.toTempRootCertPem();
        ca.deleteOnExit();
        try (ConformanceServer server = ConformanceServer.start(QuicTransport.listener("127.0.0.1:0", certs.server()),
                new ConformanceServer.Shared())) {
            final List<String> command = new ArrayList<String>(Arrays.asList(GoServer.build("client").toString(),
                    "-quic", server.address(), "-ca", ca.getAbsolutePath(), "reverse", "calls"));
            final Process p = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
            p.getOutputStream().close();
            final ByteArrayOutputStream said = new ByteArrayOutputStream();
            try (InputStream in = p.getInputStream()) {
                final byte[] buf = new byte[4096];
                for (int n = in.read(buf); n >= 0; n = in.read(buf)) {
                    said.write(buf, 0, n);
                }
            }
            assertTrue(p.waitFor(60, TimeUnit.SECONDS), "the Go client did not end");
            assertEquals("exit 0\nOK reverse\nOK calls",
                    "exit " + p.exitValue() + "\n" + new String(said.toByteArray(), StandardCharsets.UTF_8).trim());
        }
    }
}
