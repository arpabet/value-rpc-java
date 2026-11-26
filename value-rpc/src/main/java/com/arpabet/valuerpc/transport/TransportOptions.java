/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.transport;

import com.arpabet.valuerpc.Frames;
import java.net.InetSocketAddress;
import javax.net.ssl.SSLContext;

/**
 * What the transports that come with the library are set up with. The
 * defaults are those of the Go implementation. Immutable: each {@code with}
 * gives another.
 */
public final class TransportOptions {

    /** The defaults: frames up to 16 MiB, a write within 10 s, keepalive on, TLS as the platform has it. */
    public static final TransportOptions DEFAULT =
            new TransportOptions(Frames.MAX_FRAME_SIZE, 10_000, true, null, null, false);

    private final int maxFrameSize;
    private final long writeTimeoutMillis;
    private final boolean keepAlive;
    private final InetSocketAddress socksProxy;
    private final SSLContext tls;
    private final boolean clientAuth;

    private TransportOptions(int maxFrameSize, long writeTimeoutMillis, boolean keepAlive, InetSocketAddress socksProxy,
            SSLContext tls, boolean clientAuth) {
        this.maxFrameSize = maxFrameSize;
        this.writeTimeoutMillis = writeTimeoutMillis;
        this.keepAlive = keepAlive;
        this.socksProxy = socksProxy;
        this.tls = tls;
        this.clientAuth = clientAuth;
    }

    /** The longest message taken from the peer; not positive for no limit. */
    public int maxFrameSize() {
        return maxFrameSize;
    }

    /** How long one write may take before the connection is given up; not positive for no limit. */
    public long writeTimeoutMillis() {
        return writeTimeoutMillis;
    }

    /** Whether TCP keepalive is on, so that a peer that is gone is found out. */
    public boolean keepAlive() {
        return keepAlive;
    }

    /** The SOCKS5 proxy a TCP connection is made through, or null. */
    public InetSocketAddress socksProxy() {
        return socksProxy;
    }

    /** The keys and the trust of TLS, or null for those of the platform. See {@link #withTls}. */
    public SSLContext tls() {
        return tls;
    }

    /** Whether a TLS listener asks every client for a certificate. See {@link #withClientAuth}. */
    public boolean clientAuth() {
        return clientAuth;
    }

    public TransportOptions withMaxFrameSize(int maxFrameSize) {
        return new TransportOptions(maxFrameSize, writeTimeoutMillis, keepAlive, socksProxy, tls, clientAuth);
    }

    public TransportOptions withWriteTimeoutMillis(long writeTimeoutMillis) {
        return new TransportOptions(maxFrameSize, writeTimeoutMillis, keepAlive, socksProxy, tls, clientAuth);
    }

    public TransportOptions withKeepAlive(boolean keepAlive) {
        return new TransportOptions(maxFrameSize, writeTimeoutMillis, keepAlive, socksProxy, tls, clientAuth);
    }

    /** Through a SOCKS5 proxy, which is given the name of the host and resolves it. */
    public TransportOptions withSocksProxy(String host, int port) {
        return new TransportOptions(maxFrameSize, writeTimeoutMillis, keepAlive,
                host == null ? null : InetSocketAddress.createUnresolved(host, port), tls, clientAuth);
    }

    /**
     * The keys and the trust the transports over TLS ({@code tls://},
     * {@code wss://}) are made with. A listener needs one, with the
     * certificate it shows. A dialer without one trusts what the platform
     * trusts, the public authorities, which is what a server with a
     * certificate of one of them needs; one with a context trusts what the
     * context trusts, and shows the certificate of the context to a server
     * that asks for one.
     *
     * <p>A dialer checks that the certificate of the server is for the host
     * of the address, as a browser does and as the Go implementation does: a
     * socket of the platform would not by itself.
     */
    public TransportOptions withTls(SSLContext context) {
        return new TransportOptions(maxFrameSize, writeTimeoutMillis, keepAlive, socksProxy, context, clientAuth);
    }

    /**
     * Whether a TLS listener lets in only clients that show a certificate
     * its context trusts: TLS in both directions. What a client showed is
     * with the connection ({@link MsgConn#tlsSession()}), for an authorizer
     * or an authenticator of a server to go by.
     */
    public TransportOptions withClientAuth(boolean need) {
        return new TransportOptions(maxFrameSize, writeTimeoutMillis, keepAlive, socksProxy, tls, need);
    }
}
