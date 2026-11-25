/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.transport;

import com.arpabet.valuerpc.Frames;
import java.net.InetSocketAddress;

/**
 * What the transports that come with the library are set up with. The
 * defaults are those of the Go implementation. Immutable: each {@code with}
 * gives another.
 */
public final class TransportOptions {

    /** The defaults: frames up to 16 MiB, a write within 10 s, keepalive on. */
    public static final TransportOptions DEFAULT = new TransportOptions(Frames.MAX_FRAME_SIZE, 10_000, true, null);

    private final int maxFrameSize;
    private final long writeTimeoutMillis;
    private final boolean keepAlive;
    private final InetSocketAddress socksProxy;

    private TransportOptions(int maxFrameSize, long writeTimeoutMillis, boolean keepAlive, InetSocketAddress socksProxy) {
        this.maxFrameSize = maxFrameSize;
        this.writeTimeoutMillis = writeTimeoutMillis;
        this.keepAlive = keepAlive;
        this.socksProxy = socksProxy;
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

    public TransportOptions withMaxFrameSize(int maxFrameSize) {
        return new TransportOptions(maxFrameSize, writeTimeoutMillis, keepAlive, socksProxy);
    }

    public TransportOptions withWriteTimeoutMillis(long writeTimeoutMillis) {
        return new TransportOptions(maxFrameSize, writeTimeoutMillis, keepAlive, socksProxy);
    }

    public TransportOptions withKeepAlive(boolean keepAlive) {
        return new TransportOptions(maxFrameSize, writeTimeoutMillis, keepAlive, socksProxy);
    }

    /** Through a SOCKS5 proxy, which is given the name of the host and resolves it. */
    public TransportOptions withSocksProxy(String host, int port) {
        return new TransportOptions(maxFrameSize, writeTimeoutMillis, keepAlive,
                host == null ? null : InetSocketAddress.createUnresolved(host, port));
    }
}
