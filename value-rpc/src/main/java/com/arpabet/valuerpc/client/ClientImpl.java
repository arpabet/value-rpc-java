/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.client;

import com.arpabet.value.MapValue;
import com.arpabet.value.NumberValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.Chat;
import com.arpabet.valuerpc.ChatHandler;
import com.arpabet.valuerpc.ClientInterceptor;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.Dialect;
import com.arpabet.valuerpc.Frames;
import com.arpabet.valuerpc.HandlerRegistry;
import com.arpabet.valuerpc.HashChain;
import com.arpabet.valuerpc.IncomingStreamHandler;
import com.arpabet.valuerpc.Initiator;
import com.arpabet.valuerpc.MessageType;
import com.arpabet.valuerpc.Metrics;
import com.arpabet.valuerpc.OutgoingStreamHandler;
import com.arpabet.valuerpc.Peer;
import com.arpabet.valuerpc.Responder;
import com.arpabet.valuerpc.TypeDef;
import com.arpabet.valuerpc.UnaryHandler;
import com.arpabet.valuerpc.ValueSink;
import com.arpabet.valuerpc.ValueStream;
import com.arpabet.valuerpc.VrpcException;
import com.arpabet.valuerpc.transport.Dialer;
import com.arpabet.valuerpc.transport.MsgConn;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The client. One connection carries every request: a thread of the transport
 * reads it and hands each message to {@link #dispatch}, which never waits,
 * and a thread of the client writes what is queued, several messages at once
 * when several wait. What the client asks of the server is with its
 * {@link Initiator}; what the server asks of the client, with the
 * {@link Responder} of the connection it came on.
 */
final class ClientImpl implements VrpcClient {

    private final Dialer dialer;
    private final Dialect dialect;
    private final long dialTimeoutMillis;
    private final int sendQueue;
    private final Metrics metrics;
    private final ClientListener listener;
    private final ClientInterceptor.Invoker invoker;
    private final boolean intercepted;

    /** Chosen at random, as the Go client chooses it: 63 bits. */
    private final long clientId = ThreadLocalRandom.current().nextLong() & Long.MAX_VALUE;

    private volatile long timeoutMillis;
    private volatile Value credential;

    /** What the server may ask of this client, by name. A name registered again replaces: as in the Go client. */
    private final HandlerRegistry registry = new HandlerRegistry(true);
    /** What this client asks of the server. */
    private final Initiator initiator;
    /** How what the server asks is served, on whichever connection it comes. */
    private final Responder.Setup serving;
    private final AtomicLong reconnects = new AtomicLong();

    private final ReentrantLock connectLock = new ReentrantLock();
    private final AtomicReference<Connection> connection = new AtomicReference<Connection>();
    private volatile boolean closed;

    /** The chain that proves on a reconnect that this is the client of the session. Under connectLock. */
    private HashChain chain;
    /** Whether a server has accepted a handshake: from then on a reconnect shows the next link, not the anchor. */
    private volatile boolean established;

    private final ScheduledThreadPoolExecutor timer;
    private volatile ExecutorService executor;

    ClientImpl(Builder b) {
        this.dialer = b.dialer;
        this.dialect = b.dialect;
        this.dialTimeoutMillis = b.dialTimeoutMillis;
        this.sendQueue = b.sendQueue;
        this.metrics = b.metrics;
        this.listener = b.listener;
        this.timeoutMillis = b.timeoutMillis;
        this.credential = b.credential;
        this.intercepted = !b.interceptors.isEmpty();
        this.timer = new ScheduledThreadPoolExecutor(1, r -> daemon("vrpc-client-timer", r));
        this.timer.setRemoveOnCancelPolicy(true);
        this.initiator = new Initiator(new Initiator.Setup(true, dialect, metrics, timer, b.streamWindow,
                () -> timeoutMillis, b.metadata, 0), new Initiator.Host() {
                    @Override
                    public Initiator.Link connected() {
                        return ClientImpl.this.connected();
                    }

                    @Override
                    public VrpcException closed() {
                        return closed ? clientClosed() : null;
                    }

                    @Override
                    public void protocolError(MapValue message, String problem) {
                        listener.onProtocolError(message, problem);
                    }
                });
        this.invoker = ClientInterceptor.chain(initiator::callFunction, new ArrayList<ClientInterceptor>(b.interceptors));
        // A client limits the calls it serves at once, and no streams: as the Go client.
        this.serving = new Responder.Setup(false, dialect, registry, metrics, timer, this::runHandler,
                b.maxConcurrentInbound, 0, b.streamWindow, null);
    }

    /** What goes wrong while the server is served goes to the listener of the client. */
    private final Responder.Events servingEvents = new Responder.Events() {
        @Override
        public void protocolError(MapValue message, String problem) {
            listener.onProtocolError(message, problem);
        }

        @Override
        public void handlerFailure(String where, Throwable error) {
            listener.onHandlerFailure(where, error);
        }
    };

    private static Thread daemon(String name, Runnable task) {
        final Thread t = new Thread(task, name);
        t.setDaemon(true);
        return t;
    }

    private ExecutorService executor() {
        ExecutorService e = executor;
        if (e == null) {
            synchronized (this) {
                e = executor;
                if (e == null) {
                    e = Executors.newCachedThreadPool(r -> daemon("vrpc-client-worker", r));
                    executor = e;
                }
            }
        }
        return e;
    }

    /** Hands a handler to a thread of the client, which has them only once the server asks something. */
    private void runHandler(Runnable task) {
        if (closed) {
            throw new RejectedExecutionException("client closed");
        }
        executor().execute(task);
    }

    // --- what it is ----------------------------------------------------------------

    @Override
    public long clientId() {
        return clientId;
    }

    @Override
    public boolean isActive() {
        final Connection c = connection.get();
        return c != null && c.isOpen();
    }

    @Override
    public void setTimeoutMillis(long millis) {
        this.timeoutMillis = millis;
    }

    @Override
    public void setCredential(Value credential) {
        this.credential = credential;
    }

    @Override
    public Map<String, Long> stats() {
        final Map<String, Long> out = new LinkedHashMap<String, Long>();
        out.put("requests", initiator.requests());
        out.put("reconnects", reconnects.get());
        out.put("pending", (long) initiator.pending());
        final Connection c = connection.get();
        out.put("sendingLen", c == null ? 0L : (long) c.outbox.size());
        out.put("sendingCap", (long) sendQueue);
        return out;
    }

    private static VrpcException clientClosed() {
        return new VrpcException(Code.UNAVAILABLE, "client closed");
    }

    // --- connecting ----------------------------------------------------------------

    /**
     * The link of the hash chain the next handshake shows: the anchor until a
     * server has accepted a handshake, and from then on the next link, on
     * every reconnect, whether or not the handshake before got through. A
     * link is never shown twice; the server makes up for the ones it missed.
     */
    private String handshakeToken() {
        if (chain == null) {
            chain = new HashChain();
        }
        if (!established) {
            return chain.anchor();
        }
        final String link = chain.next();
        if (link == null) {
            throw new VrpcException(Code.UNAVAILABLE,
                    "session resumption chain exhausted; close this client and connect as a new one");
        }
        return link;
    }

    @Override
    public void connect() {
        connectLock.lock();
        try {
            if (closed) {
                throw clientClosed();
            }
            final Connection current = connection.get();
            if (current != null && current.isOpen()) {
                return;
            }
            final long deadline = dialTimeoutMillis > 0 ? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(dialTimeoutMillis) : 0;
            final String token = handshakeToken();
            final MsgConn mc;
            try {
                mc = dialer.dial(dialTimeoutMillis);
            } catch (IOException e) {
                throw new VrpcException(Code.UNAVAILABLE, "dial: " + e.getMessage(), e);
            }
            final Connection c = new Connection(mc);
            c.start();
            // The connection counts as established only when the server has
            // accepted the handshake: a call must not go out before, and a
            // refusal is the error of connecting, not a lost connection.
            c.send(dialect.handshakeRequest(clientId, token, credential));
            final MapValue response;
            try {
                response = deadline == 0 ? c.handshake.get()
                        : c.handshake.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                c.close();
                throw new VrpcException(Code.DEADLINE_EXCEEDED, "handshake: no answer in " + dialTimeoutMillis + " ms");
            } catch (InterruptedException e) {
                c.close();
                Thread.currentThread().interrupt();
                throw new VrpcException(Code.CANCELED, "handshake: interrupted", e);
            } catch (ExecutionException e) {
                c.close();
                throw (VrpcException) e.getCause();
            }
            if (closed) {
                c.close();
                throw clientClosed();
            }
            connection.set(c);
            if (!c.isOpen()) {
                // Lost between the handshake and here: nobody else would notice.
                connection.compareAndSet(c, null);
                throw new VrpcException(Code.UNAVAILABLE, "connection lost after the handshake");
            }
            listener.onConnected(response);
        } finally {
            connectLock.unlock();
        }
    }

    private Connection connected() {
        Connection c = connection.get();
        if (c == null || !c.isOpen()) {
            connect();
            c = connection.get();
            if (c == null) {
                throw closed ? clientClosed() : new VrpcException(Code.UNAVAILABLE, "no connection");
            }
        }
        return c;
    }

    /**
     * A connection was lost. What was asked on it cannot be answered any more
     * and fails at once, rather than each call waiting out its timeout, and
     * what the server asked on it is given up, as the server has given it up;
     * then the client connects again, so that the server can reach it.
     */
    private void connectionLost(Connection c, Throwable cause) {
        if (closed || !connection.compareAndSet(c, null)) {
            return;
        }
        c.close();
        reconnects.incrementAndGet();
        metrics.reconnect();
        // Requests made from here on belong to the next connection.
        initiator.failOn(c, null);
        c.responder.lost("connection lost");
        listener.onDisconnected(cause);
        try {
            connect();
        } catch (VrpcException e) {
            // The next call connects.
        }
    }

    @Override
    public void close() {
        closed = true;
        final Connection c = connection.getAndSet(null);
        if (c != null) {
            c.close();
            c.responder.lost("client closed");
        }
        initiator.failAll(clientClosed());
        timer.shutdownNow();
        final ExecutorService e = executor;
        if (e != null) {
            e.shutdown();
        }
    }

    // --- what it asks ---------------------------------------------------------------

    @Override
    public Value callFunction(String name, Value args, CallOptions options) {
        return invoker.invoke(name, args, options == null ? CallOptions.DEFAULT : options);
    }

    @Override
    public CompletableFuture<Value> callFunctionAsync(String name, Value args, CallOptions options) {
        final CallOptions o = options == null ? CallOptions.DEFAULT : options;
        if (!intercepted) {
            return initiator.callFunctionAsync(name, args, o);
        }
        // Interceptors are written to wait for the call: they run on a
        // thread of the client. Cancelling the future interrupts it, which
        // cancels the call that is under way, and has an interceptor that
        // waits, between the attempts of a retry say, stop waiting.
        final CompletableFuture<Value> future = new CompletableFuture<Value>();
        if (closed) {
            future.completeExceptionally(clientClosed());
            return future;
        }
        final Future<?> chain;
        try {
            chain = executor().submit(() -> {
                try {
                    future.complete(invoker.invoke(name, args, o));
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        } catch (RejectedExecutionException e) {
            future.completeExceptionally(clientClosed());
            return future;
        }
        future.whenComplete((value, error) -> {
            if (future.isCancelled()) {
                chain.cancel(true);
            }
        });
        return future;
    }

    @Override
    public ValueStream getStream(String name, Value args, CallOptions options) {
        return initiator.getStream(name, args, options);
    }

    @Override
    public ValueSink putStream(String name, Value args, CallOptions options) {
        return initiator.putStream(name, args, options);
    }

    @Override
    public Chat chat(String name, Value args, CallOptions options) {
        return initiator.chat(name, args, options);
    }

    // --- what it serves --------------------------------------------------------------

    @Override
    public void addFunction(String name, TypeDef args, TypeDef result, UnaryHandler handler) {
        registry.addFunction(name, args, result, handler);
    }

    @Override
    public void addOutgoingStream(String name, TypeDef args, OutgoingStreamHandler handler) {
        registry.addOutgoingStream(name, args, handler);
    }

    @Override
    public void addIncomingStream(String name, TypeDef args, IncomingStreamHandler handler) {
        registry.addIncomingStream(name, args, handler);
    }

    @Override
    public void addChat(String name, TypeDef args, ChatHandler handler) {
        registry.addChat(name, args, handler);
    }

    // --- what the server sends -----------------------------------------------------

    /** Handles a message. It runs on the thread that reads the connection and must not wait. */
    private void dispatch(Connection c, MapValue message) {
        final NumberValue typeNumber = dialect.typeNumber(message);
        if (typeNumber == null) {
            listener.onProtocolError(message, "message type not found");
            return;
        }
        final MessageType type = MessageType.of(typeNumber.longValue());
        if (type == MessageType.HANDSHAKE_RESPONSE) {
            established = true;
            c.handshake.complete(message);
            return;
        }
        final NumberValue idValue = dialect.requestId(message);
        if (idValue == null) {
            listener.onProtocolError(message, "request id not found");
            return;
        }
        final long id = idValue.longValue();
        if (type != null && type.opensRequest()) {
            // The server asks something of this client.
            if (id >= 0) {
                // A request of the server has a negative id; any other would be taken for an answer.
                listener.onProtocolError(message, "server-initiated request must use a negative request id");
                return;
            }
            c.responder.onMessage(type, idValue, message);
            return;
        }
        if (type == MessageType.ERROR_RESPONSE && id == dialect.handshakeRequestId() && !c.handshake.isDone()) {
            // The server refuses the handshake, and says why before it closes.
            final VrpcException refused = dialect.errorOf(message);
            c.handshake.completeExceptionally(
                    VrpcException.ofNumber(refused.codeNumber(), "handshake: " + refused.detail()));
            return;
        }
        if (id < 0) {
            // About a request the server opened here.
            c.responder.onMessage(type, idValue, message);
            return;
        }
        initiator.onMessage(type, id, message);
    }

    // --- the connection ------------------------------------------------------------

    private final class Connection implements MsgConn.Receiver, Initiator.Link, Responder.Link {
        final MsgConn conn;
        final ArrayBlockingQueue<MapValue> outbox = new ArrayBlockingQueue<MapValue>(sendQueue);
        /** What the server asks on this connection. */
        final Responder responder = new Responder(serving, this, servingEvents);
        /** Completes with the server's answer to the handshake, or with why there is none. */
        final CompletableFuture<MapValue> handshake = new CompletableFuture<MapValue>();
        /** Closed by this client, on purpose. */
        private volatile boolean closedHere;
        /** Lost: the peer closed it, or a read or a write failed. */
        private volatile boolean gone;
        private volatile Thread sender;

        Connection(MsgConn conn) {
            this.conn = conn;
        }

        void start() {
            conn.start(this);
            final Thread t = daemon("vrpc-send " + conn.remoteAddress(), this::sendLoop);
            sender = t;
            t.start();
        }

        @Override
        public boolean isOpen() {
            return !closedHere && !gone;
        }

        @Override
        public String remoteAddress() {
            return conn.remoteAddress();
        }

        /** A client does not authenticate its server by a handshake: nobody is named. */
        @Override
        public String principal() {
            return "";
        }

        @Override
        public Peer peer() {
            return ClientImpl.this;
        }

        /** Queues a message, waiting while the queue is full. False when the connection is gone. */
        @Override
        public boolean send(MapValue message) {
            try {
                while (isOpen()) {
                    if (outbox.offer(message, 100, TimeUnit.MILLISECONDS)) {
                        return true;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return false;
        }

        /** Queues a message if there is room at once: for what may be dropped. */
        @Override
        public boolean trySend(MapValue message) {
            return isOpen() && outbox.offer(message);
        }

        private void sendLoop() {
            final ArrayList<MapValue> batch = new ArrayList<MapValue>(Frames.MAX_COALESCE);
            try {
                while (isOpen()) {
                    final MapValue first = outbox.poll(1, TimeUnit.SECONDS);
                    if (first == null) {
                        continue;
                    }
                    // What already waits goes out with it in one write; nothing is waited for.
                    batch.clear();
                    batch.add(first);
                    outbox.drainTo(batch, Frames.MAX_COALESCE - 1);
                    conn.write(batch);
                }
            } catch (InterruptedException e) {
                // closed
            } catch (IOException e) {
                lost(e);
            } catch (RuntimeException e) {
                lost(e);
            }
        }

        @Override
        public void onMessage(MapValue message) {
            dispatch(this, message);
        }

        @Override
        public void onClosed(Throwable cause) {
            lost(cause);
        }

        private void lost(Throwable cause) {
            if (closedHere) {
                return;
            }
            gone = true;
            handshake.completeExceptionally(new VrpcException(Code.UNAVAILABLE,
                    "handshake: connection closed" + (cause == null ? "" : ": " + cause.getMessage()), cause));
            connectionLost(this, cause);
        }

        void close() {
            closedHere = true;
            conn.close();
            final Thread t = sender;
            if (t != null && t != Thread.currentThread()) {
                t.interrupt();
            }
        }
    }
}
