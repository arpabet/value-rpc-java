/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * Certificates for the tests of the transports over TLS, made when a test
 * needs them: one for a server at {@code 127.0.0.1} and {@code localhost},
 * one for a client, and one for a server by another name. They are made by
 * the {@code keytool} of the JDK that runs the test, so that the tests need
 * no library for it; each is signed by itself, and trusted by being named.
 */
public final class Pki {

    private static final char[] PASSWORD = "changeit".toCharArray();

    private static Pki shared;

    private final KeyStore server;
    private final KeyStore client;
    private final KeyStore elsewhere;

    private Pki() throws IOException, GeneralSecurityException, InterruptedException {
        final Path dir = Files.createTempDirectory("vrpc-pki");
        try {
            server = make(dir, "server", "CN=127.0.0.1", "SAN=ip:127.0.0.1,dns:localhost");
            client = make(dir, "client", "CN=vrpc-test-client", null);
            elsewhere = make(dir, "elsewhere", "CN=elsewhere.example", "SAN=dns:elsewhere.example");
        } finally {
            try (java.util.stream.Stream<Path> files = Files.list(dir)) {
                for (Path f : (Iterable<Path>) files::iterator) {
                    Files.deleteIfExists(f);
                }
            }
            Files.deleteIfExists(dir);
        }
    }

    /** The certificates, made once for all the tests of a run. */
    public static synchronized Pki get() {
        if (shared == null) {
            try {
                shared = new Pki();
            } catch (IOException | GeneralSecurityException e) {
                throw new IllegalStateException("the certificates of the tests could not be made", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        return shared;
    }

    private static KeyStore make(Path dir, String alias, String name, String extension)
            throws IOException, GeneralSecurityException, InterruptedException {
        final Path file = dir.resolve(alias + ".p12");
        final List<String> command = new ArrayList<String>(Arrays.asList(
                Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                "-genkeypair", "-alias", alias, "-keyalg", "EC", "-groupname", "secp256r1", "-dname", name,
                "-validity", "2", "-storetype", "PKCS12", "-keystore", file.toString(),
                "-storepass", new String(PASSWORD), "-keypass", new String(PASSWORD)));
        if (extension != null) {
            command.add("-ext");
            command.add(extension);
        }
        final Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
        final byte[] said;
        try (InputStream in = p.getInputStream()) {
            said = in.readAllBytes();
        }
        if (!p.waitFor(60, TimeUnit.SECONDS) || p.exitValue() != 0) {
            p.destroyForcibly();
            throw new IOException("keytool: " + new String(said, StandardCharsets.UTF_8));
        }
        final KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(file)) {
            store.load(in, PASSWORD);
        }
        return store;
    }

    /** A context that shows the certificate of a store, or none, and trusts the certificates named. */
    private static SSLContext context(KeyStore shows, X509Certificate... trusted) {
        try {
            KeyManagerFactory keys = null;
            if (shows != null) {
                keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keys.init(shows, PASSWORD);
            }
            final KeyStore authorities = KeyStore.getInstance(KeyStore.getDefaultType());
            authorities.load(null, null);
            for (int i = 0; i < trusted.length; i++) {
                authorities.setCertificateEntry("trusted-" + i, trusted[i]);
            }
            final TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(authorities);
            final SSLContext context = SSLContext.getInstance("TLS");
            context.init(keys == null ? null : keys.getKeyManagers(), trust.getTrustManagers(), null);
            return context;
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static X509Certificate certificate(KeyStore store) {
        try {
            return (X509Certificate) store.getCertificate(store.aliases().nextElement());
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The context of a server at {@code 127.0.0.1} or {@code localhost}, which trusts the certificate of the client. */
    public SSLContext server() {
        return context(server, certificate(client));
    }

    /** The context of a server whose certificate is for another host than the one it is reached by. */
    public SSLContext serverElsewhere() {
        return context(elsewhere, certificate(client));
    }

    /** The context of a client that trusts the server, and has no certificate of its own. */
    public SSLContext client() {
        return context(null, certificate(server), certificate(elsewhere));
    }

    /** The context of a client that trusts the server and shows its certificate. */
    public SSLContext clientWithCertificate() {
        return context(client, certificate(server));
    }

    /** The context of a client that trusts another certificate than the one of the server. */
    public SSLContext clientThatTrustsAnother() {
        return context(null, certificate(client));
    }

    /** A context that trusts a certificate named, and shows none: for a server of another making. */
    public static SSLContext trusting(X509Certificate authority) {
        return context(null, authority);
    }

    /** A context that trusts a certificate named and shows one of its own, given with its key. */
    public static SSLContext trusting(X509Certificate authority, X509Certificate shows, PrivateKey key) {
        try {
            final KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
            store.load(null, null);
            store.setKeyEntry("own", key, PASSWORD, new java.security.cert.Certificate[] {shows});
            return context(store, authority);
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public X509Certificate serverCertificate() {
        return certificate(server);
    }

    public X509Certificate clientCertificate() {
        return certificate(client);
    }

    private static void pem(Path file, String label, byte[] der) throws IOException {
        try (OutputStream out = Files.newOutputStream(file)) {
            out.write(("-----BEGIN " + label + "-----\n").getBytes(StandardCharsets.US_ASCII));
            out.write(Base64.getMimeEncoder(64, new byte[] {'\n'}).encode(der));
            out.write(("\n-----END " + label + "-----\n").getBytes(StandardCharsets.US_ASCII));
        }
    }

    /**
     * Writes the certificate of the server, and the certificate and the key
     * of the client, as PEM files: for a client of another making to be
     * given. They are {@code server.pem}, {@code client.pem} and
     * {@code client-key.pem} in the directory.
     */
    public void writePem(Path dir) throws IOException {
        try {
            pem(dir.resolve("server.pem"), "CERTIFICATE", certificate(server).getEncoded());
            pem(dir.resolve("client.pem"), "CERTIFICATE", certificate(client).getEncoded());
            pem(dir.resolve("client-key.pem"), "PRIVATE KEY", client.getKey("client", PASSWORD).getEncoded());
        } catch (GeneralSecurityException e) {
            throw new IOException(e);
        }
    }
}
