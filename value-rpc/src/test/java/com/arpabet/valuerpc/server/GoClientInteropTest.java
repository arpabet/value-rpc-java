/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.arpabet.valuerpc.ConformanceServer;
import com.arpabet.valuerpc.GoServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The Go client against the server of this implementation: the reference as
 * the one that asks. The client is {@code conformance/client}, built and run
 * as a process; the server is {@link ConformanceServer}, the Java twin of the
 * Go conformance server. Over TCP, and over WebSocket in each of its codecs,
 * which is the WebSocket client of the Go implementation against the
 * WebSocket listener of this one. Skipped where there is no Go toolchain.
 */
@Timeout(180)
class GoClientInteropTest {

    /** A way for the Go client to reach a server: the flags that say where it listens. */
    private interface Reach {
        List<String> flags(String address);
    }

    private static final String[] TRANSPORTS = {"tcp", "ws msgpack", "ws json"};

    private static String listen(String transport) {
        return transport.equals("tcp") ? "tcp://127.0.0.1:0" : "ws://127.0.0.1:0/rpc";
    }

    private static Reach reach(String transport) {
        switch (transport) {
            case "tcp":
                return address -> Arrays.asList("-tcp", address);
            case "ws json":
                return address -> Arrays.asList("-ws", "ws://" + address, "-codec", "json");
            default:
                return address -> Arrays.asList("-ws", "ws://" + address);
        }
    }

    @BeforeAll
    static void build() throws Exception {
        assumeTrue(GoServer.available(), "no Go toolchain: the Go client cannot be built");
        GoServer.build("client");
    }

    /**
     * Runs the Go client with scenarios and gives what it said: how it ended,
     * and what it wrote to its standard output, which is where it says how
     * each scenario went.
     */
    private static String goClient(List<String> flags, String... scenarios) throws Exception {
        final List<String> command = new ArrayList<String>();
        command.add(GoServer.build("client").toString());
        command.addAll(flags);
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

    private static List<String> with(List<String> flags, String... more) {
        final List<String> all = new ArrayList<String>(flags);
        all.addAll(Arrays.asList(more));
        return all;
    }

    @Test
    void aGoClientCallsTheServer() throws Exception {
        for (String transport : TRANSPORTS) {
            try (ConformanceServer server = ConformanceServer.start(listen(transport), new ConformanceServer.Shared())) {
                final List<String> flags = reach(transport).flags(server.address());
                assertEquals("exit 0\nOK calls\nOK typed\nOK metadata\nOK whoami",
                        goClient(flags, "calls", "typed", "metadata", "whoami:"), "over " + transport);
                assertEquals("exit 0\nOK sequential 200\nOK concurrent 16x50\nOK big 1048576\nOK calls",
                        goClient(flags, "sequential:200", "concurrent:16x50", "big:1048576", "calls"), "over " + transport);
            }
        }
    }

    @Test
    void aGoClientStreamsBothWays() throws Exception {
        for (String transport : TRANSPORTS) {
            try (ConformanceServer server = ConformanceServer.start(listen(transport), new ConformanceServer.Shared())) {
                final List<String> flags = reach(transport).flags(server.address());
                assertEquals("exit 0\nOK streams\nOK upload 5000\nOK chat\nOK chatend 6000\nOK calls",
                        goClient(flags, "streams", "upload:5000", "chat", "chatend:6000", "calls"), "over " + transport);
            }
        }
    }

    @Test
    void theServerKeepsTheTimeAGoClientAllowsAndStopsWhatItGivesUp() throws Exception {
        for (String transport : TRANSPORTS) {
            try (ConformanceServer server = ConformanceServer.start(listen(transport), new ConformanceServer.Shared())) {
                final List<String> flags = reach(transport).flags(server.address());
                assertEquals("exit 0\nOK deadline\nOK cancel\nOK calls", goClient(flags, "deadline", "cancel", "calls"),
                        "over " + transport);
            }
        }
    }

    @Test
    void aGoClientIsWhoItsCredentialSays() throws Exception {
        for (String transport : TRANSPORTS) {
            try (ConformanceServer server = ConformanceServer.start(listen(transport), new ConformanceServer.Shared())) {
                final List<String> flags = reach(transport).flags(server.address());
                assertEquals("exit 0\nOK whoami tester", goClient(with(flags, "-credential", "secret"), "whoami:tester"),
                        "over " + transport);
                final String refused = goClient(with(flags, "-credential", "wrong", "-connect-timeout", "5000"), "whoami:tester");
                assertEquals("exit 1", refused.substring(0, 6), "over " + transport + ": " + refused);
                // The client is told that it was refused, and not left to wait for an answer.
                assertEquals(true, refused.contains("authentication failed"), "over " + transport + ": " + refused);
            }
        }
    }

    @Test
    void aGoClientThatIsDroppedComesBackAsTheSameSession() throws Exception {
        for (String transport : TRANSPORTS) {
            final LinkedBlockingQueue<String> events = new LinkedBlockingQueue<String>();
            final VrpcServer.Builder builder = VrpcServer.builder().address(listen(transport)).listener(new ServerListener() {
                @Override
                public void onConnected(long clientId, String principal, String remoteAddress, boolean resumed) {
                    events.add(resumed ? "resumed" : "connected");
                }

                @Override
                public void onRefused(String remoteAddress, String reason, Throwable cause) {
                    events.add("refused: " + reason);
                }
            });
            try (ConformanceServer server = ConformanceServer.start(builder, new ConformanceServer.Shared())) {
                assertEquals("exit 0\nOK calls\nOK resume\nOK calls",
                        goClient(reach(transport).flags(server.address()), "calls", "resume", "calls"), "over " + transport);
                assertEquals("connected", events.poll(5, TimeUnit.SECONDS), "over " + transport);
                // The link of the hash chain the Go client shows is one this server takes.
                assertEquals("resumed", events.poll(5, TimeUnit.SECONDS), "over " + transport);
                assertEquals(1L, server.server().stats().get("sessions"), "over " + transport);
            }
        }
    }
}
