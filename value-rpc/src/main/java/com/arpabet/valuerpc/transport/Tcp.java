/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TCP: the framed stream on a socket. A connection has {@code TCP_NODELAY}
 * set, as a Go connection has by default, so that a small message leaves when
 * it is written.
 */
final class Tcp {

    private Tcp() {
    }

    /** A host and a port, from {@code host:port}, {@code :port} or {@code [v6]:port}. */
    static InetSocketAddress address(String hostPort, boolean resolve) throws IOException {
        final int colon = hostPort.lastIndexOf(':');
        if (colon < 0) {
            throw new IOException("no port in the address " + hostPort);
        }
        String host = hostPort.substring(0, colon);
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        final int port;
        try {
            port = Integer.parseInt(hostPort.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IOException("the port of the address " + hostPort + " is not a number");
        }
        if (port < 0 || port > 65535) {
            throw new IOException("the port of the address " + hostPort + " is out of range");
        }
        if (!resolve) {
            return InetSocketAddress.createUnresolved(host.isEmpty() ? "localhost" : host, port);
        }
        return host.isEmpty() ? new InetSocketAddress(port) : new InetSocketAddress(host, port);
    }

    static void tune(Socket socket, TransportOptions options) throws SocketException {
        socket.setTcpNoDelay(true);
        if (options.keepAlive()) {
            socket.setKeepAlive(true);
        }
    }

    /** Connects a socket to {@code host:port}, through the SOCKS5 proxy of the options when they have one. */
    static Socket connect(String hostPort, TransportOptions options, long timeoutMillis) throws IOException {
        final Socket socket;
        final InetSocketAddress to;
        if (options.socksProxy() != null) {
            final InetSocketAddress proxy = options.socksProxy();
            socket = new Socket(new Proxy(Proxy.Type.SOCKS, new InetSocketAddress(proxy.getHostString(), proxy.getPort())));
            // The proxy is given the name and resolves it.
            to = address(hostPort, false);
        } else {
            socket = new Socket();
            final InetSocketAddress given = address(hostPort, true);
            // To dial ":port" is to dial this host.
            to = given.getAddress() != null && given.getAddress().isAnyLocalAddress()
                    ? new InetSocketAddress("localhost", given.getPort())
                    : given;
        }
        try {
            socket.connect(to, timeoutMillis <= 0 ? 0 : (int) Math.min(timeoutMillis, Integer.MAX_VALUE));
            tune(socket, options);
            return socket;
        } catch (IOException | RuntimeException e) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // the connect error is the one to report
            }
            throw e;
        }
    }

    static MsgConn conn(Socket socket, TransportOptions options) throws IOException {
        return new StreamConn(socket.getInputStream(), socket.getOutputStream(), socket,
                String.valueOf(socket.getRemoteSocketAddress()), options);
    }

    static final class TcpDialer implements Dialer {
        private final String hostPort;
        private final TransportOptions options;

        TcpDialer(String hostPort, TransportOptions options) {
            this.hostPort = hostPort;
            this.options = options;
        }

        @Override
        public MsgConn dial(long timeoutMillis) throws IOException {
            return conn(connect(hostPort, options, timeoutMillis), options);
        }

        @Override
        public String toString() {
            return "tcp://" + hostPort;
        }
    }

    /** Accepts sockets on a thread of its own and has {@link #accepted} make a connection of each. */
    abstract static class SocketListener implements Listener {
        private final ServerSocket server;
        private final String scheme;
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();

        SocketListener(ServerSocket server, String scheme) {
            this.server = server;
            this.scheme = scheme;
        }

        /**
         * Makes the connection of an accepted socket and hands it to the
         * acceptor, at once or when the socket has been made ready on another
         * thread: this one accepts, and must not wait. What it throws drops
         * that socket only.
         */
        abstract void accepted(Socket socket, Acceptor acceptor) throws IOException;

        @Override
        public void start(Acceptor acceptor) {
            if (!started.compareAndSet(false, true)) {
                throw new IllegalStateException("the listener was started already");
            }
            Threads.daemon("vrpc-accept " + address(), () -> accept(acceptor)).start();
        }

        private void accept(Acceptor acceptor) {
            long backoff = 0;
            while (!closed.get()) {
                final Socket socket;
                try {
                    socket = server.accept();
                    backoff = 0;
                } catch (IOException e) {
                    if (closed.get()) {
                        return;
                    }
                    // An error that stays, such as no file descriptors left,
                    // must not spin: wait, longer each time, up to a second.
                    backoff = backoff == 0 ? 5 : Math.min(backoff * 2, 1000);
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException interrupted) {
                        return;
                    }
                    continue;
                }
                try {
                    accepted(socket, acceptor);
                } catch (IOException | RuntimeException e) {
                    try {
                        socket.close();
                    } catch (IOException ignored) {
                        // it is dropped either way
                    }
                }
            }
        }

        @Override
        public String address() {
            final InetSocketAddress a = (InetSocketAddress) server.getLocalSocketAddress();
            final String host = a.getAddress().isAnyLocalAddress() ? "" : a.getAddress().getHostAddress();
            return (host.indexOf(':') >= 0 ? "[" + host + "]" : host) + ":" + a.getPort();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                try {
                    server.close();
                } catch (IOException ignored) {
                    // closing is all that was wanted
                }
            }
        }

        @Override
        public String toString() {
            return scheme + "://" + address();
        }
    }

    static Listener listen(String hostPort, TransportOptions options) throws IOException {
        final ServerSocket server = new ServerSocket();
        try {
            server.setReuseAddress(true);
            server.bind(address(hostPort, true));
        } catch (IOException | RuntimeException e) {
            server.close();
            throw e;
        }
        return new SocketListener(server, "tcp") {
            @Override
            void accepted(Socket socket, Acceptor acceptor) throws IOException {
                tune(socket, options);
                acceptor.onConnection(conn(socket, options));
            }
        };
    }
}
