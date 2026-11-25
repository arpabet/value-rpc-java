/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.transport;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The transports that come with the library, by address. The scheme of an
 * address names the transport, as in the Go implementation
 * ({@code valuerpc.NewDialer}, {@code NewListener}):
 *
 * <pre>
 *   host:port, tcp://host:port     TCP
 *   mem://name                     inside this process
 * </pre>
 *
 * and a transport of one's own is a {@link Dialer} or a {@link Listener}, for
 * which {@link #dialer(StreamSource, TransportOptions)} and
 * {@link StreamConn} frame any stream of bytes. QUIC is such a transport: it
 * comes with the module {@code value-rpc-quic}, which keeps its dependencies
 * out of this one, and {@code quic://} here says so.
 */
public final class Transports {

    private Transports() {
    }

    /** An address as its scheme and the rest; {@code tcp} when it has no scheme. */
    static String[] split(String address) {
        final int at = address.indexOf("://");
        return at < 0 ? new String[] {"tcp", address} : new String[] {address.substring(0, at), address.substring(at + 3)};
    }

    /**
     * A dialer for an address. An address of a transport that is not there
     * gives a dialer that fails when it dials, so that a client is told when
     * it connects.
     */
    public static Dialer dialer(String address, TransportOptions options) {
        final String[] a = split(address);
        switch (a[0]) {
            case "tcp":
            case "tcp4":
            case "tcp6":
                return new Tcp.TcpDialer(a[1], options);
            case "mem":
                return Mem.dialer(a[1]);
            case "quic":
                return failing("quic:// is provided by the separate module com.arpabet:value-rpc-quic (QuicTransport.dialer)");
            default:
                return failing("unsupported dial network " + a[0] + " in address " + address);
        }
    }

    /** A listener on an address. */
    public static Listener listener(String address, TransportOptions options) throws IOException {
        final String[] a = split(address);
        switch (a[0]) {
            case "tcp":
            case "tcp4":
            case "tcp6":
                return Tcp.listen(a[1], options);
            case "mem":
                return Mem.listen(a[1]);
            case "quic":
                throw new IOException(
                        "quic:// is provided by the separate module com.arpabet:value-rpc-quic (QuicTransport.listener)");
            default:
                throw new IOException("unsupported listen network " + a[0] + " in address " + address);
        }
    }

    private static Dialer failing(String why) {
        return timeoutMillis -> {
            throw new IOException(why);
        };
    }

    /** A stream of bytes to a peer, however it was made. */
    public interface StreamSource {

        /** Establishes the stream; called for every dial, also when a client reconnects. */
        Stream open(long timeoutMillis) throws IOException;
    }

    /** The two directions of a stream of bytes, and what closes them. */
    public static final class Stream {
        final InputStream in;
        final OutputStream out;
        final Closeable closer;
        final String remoteAddress;

        /**
         * @param closer closes both directions, and makes a read that waits return
         */
        public Stream(InputStream in, OutputStream out, Closeable closer, String remoteAddress) {
            this.in = in;
            this.out = out;
            this.closer = closer;
            this.remoteAddress = remoteAddress;
        }
    }

    /**
     * A dialer over streams an application establishes itself: a tunnel, a
     * pipe, a transport that hides what it carries. The protocol frames the
     * stream and uses it as it is. It is the Go implementation's
     * {@code NewFuncDialer}.
     */
    public static Dialer dialer(StreamSource source, TransportOptions options) {
        return timeoutMillis -> {
            final Stream s = source.open(timeoutMillis);
            return new StreamConn(s.in, s.out, s.closer, s.remoteAddress, options);
        };
    }

    /**
     * A dialer for one stream that is there already: it gives the connection
     * once, and fails when asked again, since it cannot be made again. It is
     * the Go implementation's {@code NewSingleConnDialer}.
     */
    public static Dialer single(Stream stream, TransportOptions options) {
        final AtomicBoolean used = new AtomicBoolean();
        return timeoutMillis -> {
            if (!used.compareAndSet(false, true)) {
                throw new IOException("value-rpc: connection already consumed");
            }
            return new StreamConn(stream.in, stream.out, stream.closer, stream.remoteAddress, options);
        };
    }
}
