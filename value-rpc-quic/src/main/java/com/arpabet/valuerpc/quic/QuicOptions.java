/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.quic;

import com.arpabet.valuerpc.Dialect;
import com.arpabet.valuerpc.transport.TransportOptions;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.handler.codec.quic.QuicSslContext;
import java.util.Objects;

/**
 * What a QUIC transport is set up with. The defaults are those of the Go
 * implementation ({@code quic/transport.go}). Immutable: each {@code with}
 * gives another.
 */
public final class QuicOptions {

    /**
     * The defaults: up to 65 536 requests at once on a connection, which is
     * given up after 30 s of silence and kept from being silent every 15 s;
     * frames up to 16 MiB and a write within 10 s, as on the other
     * transports; the certificate of a server checked against the
     * certificate authorities of the system and its name.
     */
    public static final QuicOptions DEFAULT = new QuicOptions(null, null, 1 << 16, 30_000, 15_000, 10_000,
            TransportOptions.DEFAULT, Dialect.standard(), null, null);

    private final QuicSslContext sslContext;
    private final String serverName;
    private final long maxStreams;
    private final long idleTimeoutMillis;
    private final long keepAliveMillis;
    private final long handshakeTimeoutMillis;
    private final TransportOptions transport;
    private final Dialect dialect;
    private final EventLoopGroup eventLoopGroup;
    private final Class<? extends DatagramChannel> channelType;

    private QuicOptions(QuicSslContext sslContext, String serverName, long maxStreams, long idleTimeoutMillis,
            long keepAliveMillis, long handshakeTimeoutMillis, TransportOptions transport, Dialect dialect,
            EventLoopGroup eventLoopGroup, Class<? extends DatagramChannel> channelType) {
        this.sslContext = sslContext;
        this.serverName = serverName;
        this.maxStreams = maxStreams;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.keepAliveMillis = keepAliveMillis;
        this.handshakeTimeoutMillis = handshakeTimeoutMillis;
        this.transport = transport;
        this.dialect = dialect;
        this.eventLoopGroup = eventLoopGroup;
        this.channelType = channelType;
    }

    /** The TLS context, or null: see {@link #withSslContext}. */
    public QuicSslContext sslContext() {
        return sslContext;
    }

    /** The name the certificate of the server is checked against, or null for the host of the address. */
    public String serverName() {
        return serverName;
    }

    /** How many requests the peer may have under way at once on a connection: each is a stream. */
    public long maxStreams() {
        return maxStreams;
    }

    /** How long a connection may be silent before it is given up; not positive for no limit. */
    public long idleTimeoutMillis() {
        return idleTimeoutMillis;
    }

    /** How often a connection that is silent is made to speak; not positive for never. See {@link #withKeepAliveMillis}. */
    public long keepAliveMillis() {
        return keepAliveMillis;
    }

    /** How long the handshake of a connection may take; not positive for no limit but the one of the dial. */
    public long handshakeTimeoutMillis() {
        return handshakeTimeoutMillis;
    }

    /** The longest frame taken from the peer, and how long a write may take. */
    public TransportOptions transport() {
        return transport;
    }

    /** The names of the fields on the wire: the transport reads the type and the request id of a message. */
    public Dialect dialect() {
        return dialect;
    }

    /** The event loops the transport runs on, or null for those it shares. */
    public EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    /** The kind of UDP channel made on the event loops, or null for NIO. */
    public Class<? extends DatagramChannel> channelType() {
        return channelType;
    }

    /**
     * With this TLS context. QUIC is always encrypted, and a listener has to
     * have one, with the certificate of the server; see
     * {@link QuicTransport#serverTls}. A dialer without one checks the
     * certificate of the server against the certificate authorities of the
     * system; one of its own gives it other authorities, or a certificate to
     * show the server (mutual TLS); see {@link QuicTransport#clientTls}.
     *
     * <p>A context has to name the protocol, {@link QuicTransport#ALPN}, as
     * the ones of {@link QuicTransport} do.
     */
    public QuicOptions withSslContext(QuicSslContext sslContext) {
        return new QuicOptions(sslContext, serverName, maxStreams, idleTimeoutMillis, keepAliveMillis, handshakeTimeoutMillis,
                transport, dialect, eventLoopGroup, channelType);
    }

    /**
     * With this name to check the certificate of the server against, and to
     * tell the server which name it is asked for, in place of the host of the
     * address. Null for the host of the address.
     */
    public QuicOptions withServerName(String serverName) {
        return new QuicOptions(sslContext, serverName, maxStreams, idleTimeoutMillis, keepAliveMillis, handshakeTimeoutMillis,
                transport, dialect, eventLoopGroup, channelType);
    }

    /**
     * With the peer allowed this many requests under way at once on a
     * connection. One more waits until one is over.
     *
     * @throws IllegalArgumentException it is not positive
     */
    public QuicOptions withMaxStreams(long maxStreams) {
        if (maxStreams <= 0) {
            throw new IllegalArgumentException("max streams " + maxStreams);
        }
        return new QuicOptions(sslContext, serverName, maxStreams, idleTimeoutMillis, keepAliveMillis, handshakeTimeoutMillis,
                transport, dialect, eventLoopGroup, channelType);
    }

    /**
     * With a connection given up after it was silent this long; not positive
     * for never. The shorter of the two ends' applies. A Go peer takes less
     * than five seconds as five, and times its PINGs by that: with less than
     * that here, they come too late to keep the connection.
     */
    public QuicOptions withIdleTimeoutMillis(long millis) {
        return new QuicOptions(sslContext, serverName, maxStreams, millis, keepAliveMillis, handshakeTimeoutMillis,
                transport, dialect, eventLoopGroup, channelType);
    }

    /**
     * With a connection that is silent made to speak this often, so that it
     * is not given up as idle and a peer that is gone is found out; not
     * positive for never. Less than the idle timeout, to be of use.
     *
     * <p>What is sent is a QUIC datagram, since Netty has no way to send the
     * PING the Go implementation sends, and a datagram is only sent to a peer
     * that takes them: another one of this implementation. A Go peer does
     * not, and keeps the connection itself with the PINGs it sends, unless it
     * was told not to: then, with a Go peer, nothing keeps a silent
     * connection, and it is given up when the idle timeout is over.
     */
    public QuicOptions withKeepAliveMillis(long millis) {
        return new QuicOptions(sslContext, serverName, maxStreams, idleTimeoutMillis, millis, handshakeTimeoutMillis,
                transport, dialect, eventLoopGroup, channelType);
    }

    /**
     * With the handshake of a connection given this long, on top of the limit
     * a dial is given: 10 s by default, as in the Go implementation, so that
     * a server that is not there is found out before long. Not positive for
     * the limit of the dial alone.
     */
    public QuicOptions withHandshakeTimeoutMillis(long millis) {
        return new QuicOptions(sslContext, serverName, maxStreams, idleTimeoutMillis, keepAliveMillis, millis,
                transport, dialect, eventLoopGroup, channelType);
    }

    /** With these limits of a frame and of a write. */
    public QuicOptions withTransport(TransportOptions transport) {
        return new QuicOptions(sslContext, serverName, maxStreams, idleTimeoutMillis, keepAliveMillis, handshakeTimeoutMillis,
                Objects.requireNonNull(transport, "transport"), dialect, eventLoopGroup, channelType);
    }

    /**
     * With this dialect. The transport gives each request a stream, so it
     * reads the type and the request id of every message: a client or a
     * server with a dialect of its own gives the same one here.
     */
    public QuicOptions withDialect(Dialect dialect) {
        return new QuicOptions(sslContext, serverName, maxStreams, idleTimeoutMillis, keepAliveMillis, handshakeTimeoutMillis,
                transport, Objects.requireNonNull(dialect, "dialect"), eventLoopGroup, channelType);
    }

    /**
     * With event loops of one's own, and the kind of UDP channel that goes
     * with them: those of an application that runs Netty already, or epoll
     * in place of NIO. The transport does not shut them down. Null for the
     * ones it shares, which are NIO.
     */
    public QuicOptions withEventLoopGroup(EventLoopGroup group, Class<? extends DatagramChannel> channelType) {
        if ((group == null) != (channelType == null)) {
            throw new IllegalArgumentException("event loops and the kind of channel go together");
        }
        return new QuicOptions(sslContext, serverName, maxStreams, idleTimeoutMillis, keepAliveMillis, handshakeTimeoutMillis,
                transport, dialect, group, channelType);
    }
}
