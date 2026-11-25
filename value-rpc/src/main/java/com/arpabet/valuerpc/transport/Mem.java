/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.transport;

import com.arpabet.value.MapValue;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The transport inside one process: a listener registered under a name, and
 * connections that hand messages over as they are, with no bytes in between.
 * It is for tests, and for putting services in one program now that are to be
 * on a socket later, which then changes an address and nothing else. It is
 * the Go implementation's in-memory transport ({@code mem://name}).
 */
final class Mem {

    /** How many messages may wait for a reader before a writer waits: the buffer of a socket. */
    private static final int INBOX = 1024;

    private static final ConcurrentHashMap<String, MemListener> REGISTRY = new ConcurrentHashMap<String, MemListener>();

    private static final Object CLOSE = new Object();

    private Mem() {
    }

    static Listener listen(String name) throws IOException {
        final MemListener l = new MemListener(name);
        if (REGISTRY.putIfAbsent(name, l) != null) {
            throw new IOException("mem listener " + name + " already registered");
        }
        return l;
    }

    static Dialer dialer(String name) {
        return new Dialer() {
            @Override
            public MsgConn dial(long timeoutMillis) throws IOException {
                final MemListener l = REGISTRY.get(name);
                if (l == null) {
                    throw new IOException("no mem listener registered at " + name);
                }
                final AtomicBoolean closed = new AtomicBoolean();
                final MemConn client = new MemConn("mem://" + name, closed);
                final MemConn server = new MemConn("mem://" + name + "(peer)", closed);
                client.peer = server;
                server.peer = client;
                l.offer(server);
                return client;
            }

            @Override
            public String toString() {
                return "mem://" + name;
            }
        };
    }

    private static final class MemListener implements Listener {
        private final String name;
        private final ArrayDeque<MemConn> waiting = new ArrayDeque<MemConn>();
        private Acceptor acceptor;
        private boolean closed;

        MemListener(String name) {
            this.name = name;
        }

        @Override
        public void start(Acceptor acceptor) {
            final ArrayDeque<MemConn> early;
            synchronized (this) {
                if (this.acceptor != null) {
                    throw new IllegalStateException("the listener was started already");
                }
                this.acceptor = acceptor;
                early = new ArrayDeque<MemConn>(waiting);
                waiting.clear();
            }
            for (MemConn c : early) {
                acceptor.onConnection(c);
            }
        }

        void offer(MemConn conn) throws IOException {
            final Acceptor a;
            synchronized (this) {
                if (closed) {
                    throw new IOException("mem listener " + name + " closed");
                }
                a = acceptor;
                if (a == null) {
                    // Dialed before the server began to accept: it is handed over then.
                    waiting.add(conn);
                    return;
                }
            }
            a.onConnection(conn);
        }

        @Override
        public String address() {
            return "mem://" + name;
        }

        @Override
        public void close() {
            synchronized (this) {
                closed = true;
            }
            REGISTRY.remove(name, this);
        }

        @Override
        public String toString() {
            return address();
        }
    }

    /** One end of a connection. Closing either end closes both. */
    private static final class MemConn implements MsgConn {
        private final String address;
        private final AtomicBoolean closed;
        private final LinkedBlockingQueue<Object> inbox = new LinkedBlockingQueue<Object>(INBOX);
        private final AtomicBoolean started = new AtomicBoolean();
        private volatile MemConn peer;

        MemConn(String address, AtomicBoolean closed) {
            this.address = address;
            this.closed = closed;
        }

        @Override
        public void start(Receiver receiver) {
            if (!started.compareAndSet(false, true)) {
                throw new IllegalStateException("the connection was started already");
            }
            Threads.daemon("vrpc-read " + address, () -> {
                Throwable cause = null;
                try {
                    for (;;) {
                        // What was written before the close is still read, as
                        // on a socket; then the mark in the inbox tells of the
                        // close. The wait has an end in case the mark found no
                        // room.
                        final Object next = inbox.poll(1, TimeUnit.SECONDS);
                        if (next == CLOSE || (next == null && closed.get())) {
                            break;
                        }
                        if (next != null) {
                            receiver.onMessage((MapValue) next);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (RuntimeException e) {
                    cause = e;
                } finally {
                    close();
                    receiver.onClosed(cause);
                }
            }).start();
        }

        @Override
        public void write(List<MapValue> batch) throws IOException {
            final MemConn to = peer;
            for (MapValue message : batch) {
                // A message is handed over as it is, so it must not change after.
                final MapValue m = message.toImmutable();
                try {
                    // A full inbox holds the writer, as a full socket does,
                    // until the reader takes or the connection closes.
                    do {
                        if (closed.get()) {
                            throw new IOException("the connection is closed");
                        }
                    } while (!to.inbox.offer(m, 50, TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while writing", e);
                }
            }
        }

        @Override
        public String remoteAddress() {
            return address;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                // The end that closes reads no more. The other end still reads
                // what was written to it, and then learns of the close.
                inbox.clear();
                inbox.offer(CLOSE);
                final MemConn other = peer;
                if (other != null) {
                    other.inbox.offer(CLOSE);
                }
            }
        }

        @Override
        public String toString() {
            return "MemConn[" + address + "]";
        }
    }
}
