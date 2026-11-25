/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The Go implementation as a peer for a test: the server of
 * {@code conformance/server}, built once and started as a process. A test
 * that needs it is skipped where there is no Go toolchain.
 */
public final class GoServer implements AutoCloseable {

    private static Path binary;
    private static Boolean hasGo;

    private final Process process;
    private final Map<String, String> listening = new HashMap<String, String>();

    private GoServer(Process process) {
        this.process = process;
    }

    private static Path conformanceDir() {
        // Gradle runs the tests in the directory of the module.
        return Paths.get("..", "conformance").toAbsolutePath().normalize();
    }

    /** Whether the server can be built here: there is a Go toolchain and the sources. */
    public static synchronized boolean available() {
        if (hasGo == null) {
            boolean ok = Files.isDirectory(conformanceDir().resolve("server"));
            if (ok) {
                try {
                    final Process p = new ProcessBuilder("go", "version").redirectErrorStream(true).start();
                    p.getOutputStream().close();
                    while (p.getInputStream().read() >= 0) {
                        // not interesting
                    }
                    ok = p.waitFor(30, TimeUnit.SECONDS) && p.exitValue() == 0;
                } catch (IOException e) {
                    ok = false;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    ok = false;
                }
            }
            hasGo = ok;
        }
        return hasGo;
    }

    private static synchronized Path binary() throws IOException, InterruptedException {
        if (binary == null) {
            final Path out = Paths.get("build", "conformance", "vrpc-conformance-server").toAbsolutePath();
            Files.createDirectories(out.getParent());
            final Process build = new ProcessBuilder("go", "build", "-o", out.toString(), "./server")
                    .directory(conformanceDir().toFile())
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.appendTo(new File(out.getParent().toFile(), "build.log")))
                    .start();
            if (!build.waitFor(5, TimeUnit.MINUTES) || build.exitValue() != 0) {
                throw new IOException("go build of the conformance server failed: see " + out.getParent() + "/build.log");
            }
            binary = out;
        }
        return binary;
    }

    /** Starts a server of its own and waits until it listens. */
    public static GoServer start() throws IOException, InterruptedException {
        final ProcessBuilder pb = new ProcessBuilder(binary().toString());
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        final GoServer server = new GoServer(pb.start());
        final CountDownLatch ready = new CountDownLatch(2);
        final Thread reader = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(server.process.getInputStream(), StandardCharsets.UTF_8))) {
                for (String line = r.readLine(); line != null; line = r.readLine()) {
                    final String[] words = line.trim().split(" ");
                    if (words.length == 3 && words[0].equals("LISTENING")) {
                        synchronized (server.listening) {
                            server.listening.put(words[1], words[2]);
                        }
                        ready.countDown();
                    }
                }
            } catch (IOException e) {
                // the process ended
            }
        }, "go-server-output");
        reader.setDaemon(true);
        reader.start();
        if (!ready.await(60, TimeUnit.SECONDS)) {
            server.close();
            throw new IOException("the conformance server did not start in time");
        }
        return server;
    }

    private String address(String transport) {
        synchronized (listening) {
            return listening.get(transport);
        }
    }

    /** The address of its TCP listener, {@code host:port}. */
    public String tcp() {
        return address("tcp");
    }

    /** The address of its WebSocket listener, {@code host:port/path}. */
    public String ws() {
        return address("ws");
    }

    /** Stops the server: it exits when its standard input ends. */
    @Override
    public void close() {
        try {
            process.getOutputStream().close();
        } catch (IOException e) {
            // it is stopped either way
        }
        try {
            if (!process.waitFor(3, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }
}
