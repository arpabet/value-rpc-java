/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.transport;

import com.arpabet.value.Limits;
import com.arpabet.value.MapValue;
import com.arpabet.value.ValueException;
import com.arpabet.valuerpc.Frames;
import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.SSLSession;

/**
 * A connection over a stream of bytes: messages framed with their length
 * (see {@link Frames}) on a pair of streams, read by a thread of its own. It
 * carries the protocol over TCP and TLS, and over any stream an application
 * establishes itself, which is the seam for a transport of one's own. It is
 * the Go implementation's {@code valuerpc.NewMsgConn}.
 */
public final class StreamConn implements MsgConn {

    private final InputStream in;
    private final OutputStream out;
    private final Closeable closer;
    private final String remoteAddress;
    private final int maxFrameSize;
    private final long writeTimeoutMillis;
    private final SSLSession tls;

    private final ReentrantLock writeLock = new ReentrantLock();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * @param in            what the peer sends
     * @param out           what is sent to the peer
     * @param closer        closes both, and makes a read that waits return
     * @param remoteAddress the address of the peer, for a log
     */
    public StreamConn(InputStream in, OutputStream out, Closeable closer, String remoteAddress, TransportOptions options) {
        this(in, out, closer, remoteAddress, options, null);
    }

    StreamConn(InputStream in, OutputStream out, Closeable closer, String remoteAddress, TransportOptions options,
            SSLSession tls) {
        this.in = new BufferedInputStream(in, 64 * 1024);
        this.out = out;
        this.closer = closer;
        this.remoteAddress = remoteAddress == null ? "" : remoteAddress;
        this.maxFrameSize = options.maxFrameSize();
        this.writeTimeoutMillis = options.writeTimeoutMillis();
        this.tls = tls;
    }

    @Override
    public void start(Receiver receiver) {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("the connection was started already");
        }
        Threads.daemon("vrpc-read " + remoteAddress, () -> read(receiver)).start();
    }

    private void read(Receiver receiver) {
        Throwable cause = null;
        try {
            for (;;) {
                final MapValue message = Frames.read(in, maxFrameSize, Limits.DEFAULT);
                if (message == null) {
                    break;
                }
                receiver.onMessage(message);
            }
        } catch (IOException e) {
            // A read that failed because the connection was closed here is the close.
            cause = closed.get() ? null : e;
        } catch (RuntimeException e) {
            cause = e;
        } finally {
            close();
            receiver.onClosed(cause);
        }
    }

    @Override
    public void write(List<MapValue> batch) throws IOException {
        if (batch.isEmpty()) {
            return;
        }
        if (closed.get()) {
            throw new IOException("the connection is closed");
        }
        final byte[] frames;
        try {
            frames = Frames.encode(batch);
        } catch (ValueException e) {
            throw new IOException(e.getMessage(), e);
        }
        writeLock.lock();
        try {
            // A write to a socket has no timeout of its own: a peer that does
            // not read would hold the writer for good. The connection is
            // closed instead, which makes the write fail.
            final ScheduledFuture<?> guard = writeTimeoutMillis > 0
                    ? Threads.TIMER.schedule(this::close, writeTimeoutMillis, TimeUnit.MILLISECONDS)
                    : null;
            try {
                out.write(frames);
                out.flush();
            } finally {
                if (guard != null) {
                    guard.cancel(false);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String remoteAddress() {
        return remoteAddress;
    }

    @Override
    public SSLSession tlsSession() {
        return tls;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                closer.close();
            } catch (IOException ignored) {
                // closing is all that was wanted
            }
        }
    }

    @Override
    public String toString() {
        return "StreamConn[" + remoteAddress + "]";
    }
}
