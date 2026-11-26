/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.transport;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/**
 * TLS: the protocol over a TLS socket, framed as over TCP, and what the
 * WebSocket transport puts under {@code wss://}. It is the Go
 * implementation's {@code NewTLSDialer} and {@code NewTLSListener}.
 *
 * <p>The keys and the trust are those of the {@link SSLContext} of the
 * options. The handshake of a connection has a time of its own, so that a
 * peer that connects and says nothing does not hold a thread.
 */
final class Tls {

    /** How long a client that connected to a listener has for the TLS handshake. */
    static final long HANDSHAKE_MILLIS = 10_000;

    private Tls() {
    }

    private static int bounded(long millis) {
        return millis <= 0 ? 0 : (int) Math.min(millis, Integer.MAX_VALUE);
    }

    /** The host of {@code host:port}, without the brackets of an IPv6 address. */
    static String hostOf(String hostPort) {
        final int colon = hostPort.lastIndexOf(':');
        String host = colon < 0 ? hostPort : hostPort.substring(0, colon);
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        return host.isEmpty() ? "localhost" : host;
    }

    /**
     * TLS over a socket that is connected, as the side that dialed: the
     * handshake within the time given, and the certificate of the server
     * checked against the host.
     */
    static SSLSocket client(Socket plain, String host, TransportOptions options, long timeoutMillis) throws IOException {
        final SSLContext context;
        try {
            context = options.tls() != null ? options.tls() : SSLContext.getDefault();
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("no TLS on this platform", e);
        }
        plain.setSoTimeout(bounded(timeoutMillis)); // bounds the handshake
        final SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket(plain, host, plain.getPort(), true);
        socket.setUseClientMode(true);
        // A socket does not check by itself that the certificate is for the
        // host it was opened to; the Go implementation and a browser do.
        final SSLParameters parameters = socket.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        socket.setSSLParameters(parameters);
        socket.startHandshake();
        plain.setSoTimeout(0); // a connection may be silent for as long as it likes
        return socket;
    }

    /** TLS over a socket that was accepted, as the side that listens. */
    static SSLSocket server(Socket plain, TransportOptions options, long timeoutMillis) throws IOException {
        final SSLContext context = options.tls();
        if (context == null) {
            throw new IOException("a tls listener requires an SSLContext with a server certificate (TransportOptions.withTls)");
        }
        plain.setSoTimeout(bounded(timeoutMillis));
        final InetSocketAddress peer = (InetSocketAddress) plain.getRemoteSocketAddress();
        final SSLSocket socket = (SSLSocket) context.getSocketFactory()
                .createSocket(plain, peer.getAddress().getHostAddress(), peer.getPort(), true);
        socket.setUseClientMode(false);
        if (options.clientAuth()) {
            socket.setNeedClientAuth(true);
        }
        socket.startHandshake();
        plain.setSoTimeout(0);
        return socket;
    }

    /**
     * What closes a TLS connection: the socket under it first. Closing a TLS
     * socket says goodbye to the peer, and waits to say it for a writer that
     * a peer which does not read is holding up; closing the socket under it
     * never waits, and lets that writer go.
     */
    static Closeable closer(Socket plain, SSLSocket socket) {
        return () -> {
            try {
                plain.close();
            } finally {
                socket.close();
            }
        };
    }

    static MsgConn conn(Socket plain, SSLSocket socket, TransportOptions options) throws IOException {
        return new StreamConn(socket.getInputStream(), socket.getOutputStream(), closer(plain, socket),
                String.valueOf(plain.getRemoteSocketAddress()), options, socket.getSession());
    }

    static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // closing is all that was wanted
        }
    }

    static Dialer dialer(String hostPort, TransportOptions options) {
        return new Dialer() {
            @Override
            public MsgConn dial(long timeoutMillis) throws IOException {
                final long deadline = timeoutMillis > 0 ? System.nanoTime() + timeoutMillis * 1_000_000 : 0;
                final Socket plain = Tcp.connect(hostPort, options, timeoutMillis);
                try {
                    final long left = deadline == 0 ? 0 : Math.max(1, (deadline - System.nanoTime()) / 1_000_000);
                    return conn(plain, client(plain, hostOf(hostPort), options, left), options);
                } catch (IOException | RuntimeException e) {
                    closeQuietly(plain);
                    throw e;
                }
            }

            @Override
            public String toString() {
                return "tls://" + hostPort;
            }
        };
    }

    static Listener listen(String hostPort, TransportOptions options) throws IOException {
        if (options.tls() == null) {
            throw new IOException("a tls listener requires an SSLContext with a server certificate (TransportOptions.withTls)");
        }
        final ServerSocket server = new ServerSocket();
        try {
            server.setReuseAddress(true);
            server.bind(Tcp.address(hostPort, true));
        } catch (IOException | RuntimeException e) {
            server.close();
            throw e;
        }
        return new Tcp.SocketListener(server, "tls") {
            @Override
            void accepted(Socket plain, Acceptor acceptor) {
                // The handshake is on a thread of its own: a client that takes its time must not hold up the next.
                Threads.daemon("vrpc-tls-handshake " + plain.getRemoteSocketAddress(), () -> {
                    try {
                        Tcp.tune(plain, options);
                        acceptor.onConnection(conn(plain, server(plain, options, HANDSHAKE_MILLIS), options));
                    } catch (IOException | RuntimeException e) {
                        closeQuietly(plain);
                    }
                }).start();
            }
        };
    }
}
