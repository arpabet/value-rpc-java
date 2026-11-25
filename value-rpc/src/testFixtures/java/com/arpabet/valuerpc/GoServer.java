/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
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

    /** The commands built so far, by name. */
    private static final Map<String, Path> BUILT = new HashMap<String, Path>();
    private static Boolean hasGo;

    private final Process process;
    private final Map<String, String> listening = new HashMap<String, String>();
    /** What it says of its certificates, by name. Under the lock of {@code listening}. */
    private final Map<String, byte[]> tls = new HashMap<String, byte[]>();

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

    /**
     * Builds a command of {@code conformance/}, once: {@code server}, or
     * {@code client}, the Go implementation as a client.
     *
     * @return the program
     */
    public static synchronized Path build(String command) throws IOException, InterruptedException {
        Path out = BUILT.get(command);
        if (out == null) {
            out = Paths.get("build", "conformance", "vrpc-conformance-" + command).toAbsolutePath();
            Files.createDirectories(out.getParent());
            final Process build = new ProcessBuilder("go", "build", "-o", out.toString(), "./" + command)
                    .directory(conformanceDir().toFile())
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.appendTo(new File(out.getParent().toFile(), "build.log")))
                    .start();
            if (!build.waitFor(5, TimeUnit.MINUTES) || build.exitValue() != 0) {
                throw new IOException("go build of the conformance " + command + " failed: see " + out.getParent() + "/build.log");
            }
            BUILT.put(command, out);
        }
        return out;
    }

    /** Starts a server of its own and waits until it listens. */
    public static GoServer start() throws IOException, InterruptedException {
        final ProcessBuilder pb = new ProcessBuilder(build("server").toString());
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        final GoServer server = new GoServer(pb.start());
        final CountDownLatch ready = new CountDownLatch(1);
        final Thread reader = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(server.process.getInputStream(), StandardCharsets.UTF_8))) {
                for (String line = r.readLine(); line != null; line = r.readLine()) {
                    final String[] words = line.trim().split(" ");
                    if (words.length == 3 && words[0].equals("LISTENING")) {
                        synchronized (server.listening) {
                            server.listening.put(words[1], words[2]);
                        }
                    } else if (words.length == 3 && words[0].equals("TLS")) {
                        synchronized (server.listening) {
                            server.tls.put(words[1], Base64.getDecoder().decode(words[2]));
                        }
                    } else if (words.length == 1 && words[0].equals("READY")) {
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

    /** The address of its TLS listener, {@code host:port}: its certificate is signed by {@link #ca()}. */
    public String tls() {
        return address("tls");
    }

    /** The address of its WebSocket listener, {@code host:port/path}. */
    public String ws() {
        return address("ws");
    }

    /** The address of its QUIC listener, {@code host:port}. */
    public String quic() {
        return address("quic");
    }

    /** The address of the QUIC listener that allows eight requests at once on a connection. */
    public String quicSmall() {
        return address("quic-small");
    }

    /** The address of the QUIC listener that wants a certificate of its client. */
    public String quicMutualTls() {
        return address("quic-mtls");
    }

    private byte[] tls(String what) {
        synchronized (listening) {
            return tls.get(what);
        }
    }

    /** The certificate authority that signed the certificate of the TLS and the QUIC listeners. */
    public X509Certificate ca() {
        return certificate(tls("ca"));
    }

    /** A certificate for a client, signed by {@link #ca()}; its name is {@code vrpc-conformance-client}. */
    public X509Certificate clientCertificate() {
        return certificate(tls("client-cert"));
    }

    /** The key of {@link #clientCertificate()}. */
    public PrivateKey clientKey() {
        try {
            return KeyFactory.getInstance("EC").generatePrivate(new PKCS8EncodedKeySpec(tls("client-key")));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static X509Certificate certificate(byte[] der) {
        try {
            return (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(new ByteArrayInputStream(der));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
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
