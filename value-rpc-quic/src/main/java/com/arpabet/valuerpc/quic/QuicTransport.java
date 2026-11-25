/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.quic;

import com.arpabet.valuerpc.client.VrpcClient;
import com.arpabet.valuerpc.transport.Dialer;
import com.arpabet.valuerpc.transport.Listener;
import com.arpabet.valuerpc.transport.MsgConn;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.Quic;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicClientCodecBuilder;
import io.netty.handler.codec.quic.QuicCodecBuilder;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;

/**
 * vRPC over QUIC: the Go implementation's
 * {@code go.arpabet.com/value-rpc/quic}, and a peer of it.
 *
 * <pre>{@code
 * // a client: the certificate of the server is checked against the system's authorities
 * VrpcClient client = QuicTransport.client("example.com:9000", QuicOptions.DEFAULT).build();
 *
 * // a listener, for a server
 * Listener listener = QuicTransport.listener("0.0.0.0:9000", QuicOptions.DEFAULT
 *         .withSslContext(QuicTransport.serverTls(keyFile, null, certificateChainFile).build()));
 * }</pre>
 *
 * Each request has a QUIC stream of its own, so requests have flow control
 * and ordering of their own, with TLS 1.3 and connection migration besides.
 * QUIC is always encrypted: a listener needs a certificate, and with
 * {@code clientAuth} on its context it asks the client for one, which
 * {@link MsgConn#tlsSession()} then shows.
 *
 * <p>The transport is Netty's QUIC codec, which is native code: it runs on a
 * JVM on Linux, macOS and Windows, and not on Android.
 */
public final class QuicTransport {

    /** The protocol a connection names in its TLS handshake, which both ends have to agree on. */
    public static final String ALPN = "vrpc";

    // What a peer may send before it is granted more, as the Go implementation
    // starts out: the windows grow while they are too small for what comes.
    private static final long STREAM_WINDOW = 512 * 1024;
    private static final long CONNECTION_WINDOW = 768 * 1024;

    /** Datagrams carry nothing but the fact that the connection is alive: a few may wait. */
    private static final int DATAGRAM_QUEUE = 8;

    private QuicTransport() {
    }

    /** Whether QUIC can be used here: the native library of this platform is there and loads. */
    public static boolean isAvailable() {
        return Quic.isAvailable();
    }

    private static void ensureAvailable() throws IOException {
        if (!Quic.isAvailable()) {
            throw new IOException("quic: no native library for this platform: " + Quic.unavailabilityCause(),
                    Quic.unavailabilityCause());
        }
    }

    // --- TLS -----------------------------------------------------------------------

    /**
     * The TLS context of a client, to add to: authorities of one's own with
     * {@code trustManager}, a certificate to show the server with
     * {@code keyManager}. It names the protocol already.
     */
    public static QuicSslContextBuilder clientTls() {
        return QuicSslContextBuilder.forClient().applicationProtocols(ALPN);
    }

    /**
     * The TLS context of a server with this key and certificate chain, to add
     * to: {@code clientAuth} and {@code trustManager} for mutual TLS. It
     * names the protocol already.
     */
    public static QuicSslContextBuilder serverTls(PrivateKey key, String keyPassword, X509Certificate... chain) {
        return QuicSslContextBuilder.forServer(key, keyPassword, chain).applicationProtocols(ALPN);
    }

    /** As {@link #serverTls(PrivateKey, String, X509Certificate...)}, with the key and the chain in PEM files. */
    public static QuicSslContextBuilder serverTls(File keyFile, String keyPassword, File certificateChainFile) {
        return QuicSslContextBuilder.forServer(keyFile, keyPassword, certificateChainFile).applicationProtocols(ALPN);
    }

    /** As {@link #serverTls(PrivateKey, String, X509Certificate...)}, with the keys of a key store. */
    public static QuicSslContextBuilder serverTls(KeyManagerFactory keys, String keyPassword) {
        return QuicSslContextBuilder.forServer(keys, keyPassword).applicationProtocols(ALPN);
    }

    // --- what both ends are set up with ----------------------------------------------

    private static <B extends QuicCodecBuilder<B>> B configure(B builder, QuicOptions options) {
        if (options.idleTimeoutMillis() > 0) {
            builder.maxIdleTimeout(options.idleTimeoutMillis(), TimeUnit.MILLISECONDS);
        }
        return builder
                .initialMaxData(CONNECTION_WINDOW)
                .initialMaxStreamDataBidirectionalLocal(STREAM_WINDOW)
                .initialMaxStreamDataBidirectionalRemote(STREAM_WINDOW)
                .initialMaxStreamsBidirectional(options.maxStreams())
                .datagram(DATAGRAM_QUEUE, DATAGRAM_QUEUE);
    }

    /** The event loops of the transports that were given none: a few daemon threads, made when first needed. */
    private static final class Shared {
        static final EventLoopGroup GROUP = new MultiThreadIoEventLoopGroup(
                Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors())),
                new DefaultThreadFactory("vrpc-quic", true), NioIoHandler.newFactory());
    }

    private static EventLoopGroup group(QuicOptions options) {
        return options.eventLoopGroup() != null ? options.eventLoopGroup() : Shared.GROUP;
    }

    private static Class<? extends DatagramChannel> channelType(QuicOptions options) {
        return options.channelType() != null ? options.channelType() : NioDatagramChannel.class;
    }

    /** {@code host:port} or {@code quic://host:port} as its two parts. */
    private static String[] hostAndPort(String address) throws IOException {
        String hostPort = address;
        final int scheme = address.indexOf("://");
        if (scheme >= 0) {
            if (!address.startsWith("quic://")) {
                throw new IOException("not a quic address: " + address);
            }
            hostPort = address.substring(scheme + 3);
        }
        final int colon = hostPort.lastIndexOf(':');
        if (colon < 0) {
            throw new IOException("no port in the address " + address);
        }
        String host = hostPort.substring(0, colon);
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        final int port;
        try {
            port = Integer.parseInt(hostPort.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IOException("the port of the address " + address + " is not a number");
        }
        if (port < 0 || port > 65535) {
            throw new IOException("the port of the address " + address + " is out of range");
        }
        return new String[] {host, Integer.toString(port)};
    }

    private static String text(InetSocketAddress a) {
        final String host = a.getAddress() != null ? a.getAddress().getHostAddress() : a.getHostString();
        return (host.indexOf(':') >= 0 ? "[" + host + "]" : host) + ":" + a.getPort();
    }

    // --- dialing -------------------------------------------------------------------

    /**
     * A dialer of a QUIC server, for the builder of a client. It is the Go
     * implementation's {@code valuequic.NewDialer}.
     *
     * @param address {@code host:port} or {@code quic://host:port}
     */
    public static Dialer dialer(String address, QuicOptions options) {
        return new QuicDialer(address, options);
    }

    /**
     * The builder of a client of a QUIC server, with the dialer and the
     * dialect of the options set: what is left is the client's own. It is
     * the Go implementation's {@code valuequic.NewClient}.
     *
     * @param address {@code host:port} or {@code quic://host:port}
     */
    public static VrpcClient.Builder client(String address, QuicOptions options) {
        return VrpcClient.builder().dialer(dialer(address, options)).dialect(options.dialect());
    }

    private static final class QuicDialer implements Dialer {
        private final String address;
        private final QuicOptions options;
        private QuicSslContext ssl;

        QuicDialer(String address, QuicOptions options) {
            this.address = address;
            this.options = options;
            this.ssl = options.sslContext();
        }

        /** The context given, or one that checks the server against the authorities of the system. */
        private synchronized QuicSslContext ssl() throws IOException {
            if (ssl == null) {
                ssl = clientTls().build();
            }
            if (ssl.isServer()) {
                throw new IOException("quic: a dialer was given the TLS context of a server");
            }
            return ssl;
        }

        @Override
        public MsgConn dial(long timeoutMillis) throws IOException {
            ensureAvailable();
            final String[] a = hostAndPort(address);
            final String host = a[0].isEmpty() ? "localhost" : a[0];
            final int port = Integer.parseInt(a[1]);
            final InetSocketAddress remote = new InetSocketAddress(host, port);
            if (remote.isUnresolved()) {
                throw new UnknownHostException(host);
            }
            final QuicSslContext context = ssl();
            // The name the certificate has to be for, and the one the server is asked for.
            final String serverName = options.serverName() != null ? options.serverName() : host;
            final ChannelHandler codec = configure(new QuicClientCodecBuilder(), options)
                    .sslEngineProvider(quic -> context.newEngine(quic.alloc(), serverName, port))
                    .build();

            // The handshake has a limit of its own, as in the Go implementation,
            // so that a server that is not there is found out before long.
            long limit = options.handshakeTimeoutMillis();
            if (timeoutMillis > 0 && (limit <= 0 || timeoutMillis < limit)) {
                limit = timeoutMillis;
            }
            Channel udp = null;
            try {
                // A socket of its own for the connection, on any port.
                final ChannelFuture bound = new Bootstrap().group(group(options)).channel(channelType(options))
                        .handler(codec).bind(0);
                udp = bound.channel();
                bound.await();
                if (!bound.isSuccess()) {
                    final Throwable why = bound.cause();
                    throw new IOException("quic: dial " + address + ": " + why, why);
                }
                final Future<QuicChannel> connecting = QuicChannel.newBootstrap(udp)
                        .handler(new ChannelInitializer<QuicChannel>() {
                            @Override
                            protected void initChannel(QuicChannel ch) {
                                new QuicConn(ch, true, options, null);
                            }
                        })
                        .streamHandler(QuicConn.peerStreams())
                        .remoteAddress(remote)
                        .connect();
                if (limit > 0) {
                    if (!connecting.await(limit, TimeUnit.MILLISECONDS)) {
                        connecting.cancel(false);
                        throw new IOException("quic: dial " + address + ": no answer in " + limit + " ms");
                    }
                } else {
                    connecting.await();
                }
                if (!connecting.isSuccess()) {
                    final Throwable why = connecting.cause();
                    throw new IOException("quic: dial " + address + ": " + why, why);
                }
                final QuicChannel quic = connecting.getNow();
                // A connection that was dialed has a socket of its own, which goes with it.
                final Channel socket = udp;
                quic.closeFuture().addListener(f -> socket.close());
                udp = null;
                return quic.attr(QuicConn.KEY).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted while dialing " + address);
            } finally {
                if (udp != null) {
                    udp.close();
                }
            }
        }

        @Override
        public String toString() {
            return address.startsWith("quic://") ? address : "quic://" + address;
        }
    }

    // --- listening -----------------------------------------------------------------

    /**
     * A listener for QUIC connections, for a server. The options have to
     * carry the TLS context of a server, with its certificate. It is the Go
     * implementation's {@code valuequic.NewListener}.
     *
     * @param address {@code host:port} or {@code quic://host:port}; port 0 for any
     * @throws IOException there is no TLS context, or the address cannot be bound
     */
    public static Listener listener(String address, QuicOptions options) throws IOException {
        ensureAvailable();
        final QuicSslContext ssl = options.sslContext();
        if (ssl == null) {
            throw new IOException("quic: a listener requires a TLS context with a server certificate");
        }
        if (!ssl.isServer()) {
            throw new IOException("quic: a listener was given the TLS context of a client");
        }
        final String[] a = hostAndPort(address);
        final int port = Integer.parseInt(a[1]);
        final InetSocketAddress local = a[0].isEmpty() ? new InetSocketAddress(port) : new InetSocketAddress(a[0], port);
        if (local.isUnresolved()) {
            throw new UnknownHostException(a[0]);
        }
        return new QuicListener(local, options, ssl);
    }

    private static final class QuicListener implements Listener {
        private final Channel udp;
        private final Object lock = new Object();
        /** Connections that were established before there was an acceptor. Under the lock. */
        private final ArrayDeque<QuicConn> early = new ArrayDeque<QuicConn>();
        private Acceptor acceptor;
        private boolean closed;
        /** The connections that are not over: the socket is theirs too. Under the lock. */
        private int live;

        QuicListener(InetSocketAddress local, QuicOptions options, QuicSslContext ssl) throws IOException {
            final ChannelHandler codec = configure(new QuicServerCodecBuilder(), options)
                    .sslContext(ssl)
                    .handler(new ChannelInitializer<QuicChannel>() {
                        @Override
                        protected void initChannel(QuicChannel ch) {
                            connecting(ch, options);
                        }
                    })
                    .streamHandler(QuicConn.peerStreams())
                    .build();
            final ChannelFuture bound = new Bootstrap().group(group(options)).channel(channelType(options))
                    .handler(codec).bind(local);
            try {
                bound.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                bound.channel().close();
                throw new InterruptedIOException("interrupted while binding " + text(local));
            }
            if (!bound.isSuccess()) {
                final Throwable why = bound.cause();
                throw why instanceof IOException ? (IOException) why : new IOException("quic: listen " + text(local) + ": " + why, why);
            }
            this.udp = bound.channel();
        }

        /** A peer is connecting: its connection is set up, and handed over once it is established. */
        private void connecting(QuicChannel ch, QuicOptions options) {
            synchronized (lock) {
                if (closed) {
                    // No longer accepting: the peer is told at once.
                    ch.close();
                    return;
                }
                live++;
            }
            ch.closeFuture().addListener(f -> {
                synchronized (lock) {
                    if (--live == 0 && closed) {
                        udp.close();
                    }
                }
            });
            new QuicConn(ch, false, options, this::established);
        }

        private void established(QuicConn conn) {
            final Acceptor to;
            synchronized (lock) {
                if (closed) {
                    conn.close();
                    return;
                }
                if (acceptor == null) {
                    early.add(conn);
                    return;
                }
                to = acceptor;
            }
            to.onConnection(conn);
        }

        @Override
        public void start(Acceptor acceptor) {
            final ArrayDeque<QuicConn> waiting;
            synchronized (lock) {
                if (this.acceptor != null) {
                    throw new IllegalStateException("the listener was started already");
                }
                this.acceptor = acceptor;
                waiting = new ArrayDeque<QuicConn>(early);
                early.clear();
            }
            for (QuicConn conn : waiting) {
                acceptor.onConnection(conn);
            }
        }

        @Override
        public String address() {
            return text((InetSocketAddress) udp.localAddress());
        }

        @Override
        public void close() {
            final ArrayDeque<QuicConn> unclaimed;
            synchronized (lock) {
                if (closed) {
                    return;
                }
                closed = true;
                unclaimed = new ArrayDeque<QuicConn>(early);
                early.clear();
                // The connections it has handed over stay open, and the socket
                // with them; it is closed with the last of them.
                if (live == 0) {
                    udp.close();
                }
            }
            for (QuicConn conn : unclaimed) {
                conn.close();
            }
        }

        @Override
        public String toString() {
            return "quic://" + address();
        }
    }
}
