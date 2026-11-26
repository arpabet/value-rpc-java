/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.server;

import com.arpabet.value.MapValue;
import com.arpabet.value.NumberValue;
import com.arpabet.value.StringValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.ChatHandler;
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
import com.arpabet.valuerpc.VrpcException;
import com.arpabet.valuerpc.transport.Listener;
import com.arpabet.valuerpc.transport.MsgConn;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The server. A connection has a thread of the transport that reads it and
 * hands each message to {@link Connection#dispatch}, which never waits, and a
 * thread of the server that writes what is queued for it, several messages
 * at once when several wait. What a client asks is with the
 * {@link Responder} of the connection it came on: a request is a number and
 * a handler on a thread of its own, and a message that carries the number is
 * handed to it.
 *
 * <p>A client is a session, known by the id its handshake carries, and a
 * session has one connection at a time. The requests of a client belong to
 * the connection they came on: when it is lost, or the client comes back on
 * another, they are cancelled, since the client has given them up too. What
 * the server asks of a client is with the {@link Initiator} of its session,
 * and fails when the connection it went out on is gone.
 */
final class ServerImpl implements VrpcServer {

    /** A connection before its handshake, while it is looked at, once it is let in, and when it is gone. */
    private static final int WAITING = 0;
    private static final int SHAKING = 1;
    private static final int OPEN = 2;
    private static final int OVER = 3;

    /** How many messages a client may send behind its handshake before the handshake is answered. */
    private static final int MAX_EARLY = 64;

    /** How long a client has to take a stream the server opens on it, unless the one who opens it says: as in the Go server. */
    private static final long REVERSE_ESTABLISH_MILLIS = 10_000;

    /** What a connection is closed with when its client came back on another, and what the server had asked on it is told. */
    private static final String REPLACED = "connection replaced by reconnect";
    private static final String REPLACED_REVERSE = "connection replaced by reconnect; reverse request cannot complete";

    private final Listener listener;
    private final Dialect dialect;
    private final ConnectAuthorizer authorizer;
    private final Authenticator authenticator;
    private final long maxConnections;
    private final int sendQueue;
    private final long handshakeTimeoutMillis;
    private final long sessionRetentionMillis;
    private final long closeFlushMillis;
    private final Metrics metrics;
    private final ServerListener events;

    /** What is served, by name. A name is registered once: as in the Go server. */
    private final HandlerRegistry registry = new HandlerRegistry(false);
    /** How what a client asks is served, on whichever connection it comes. */
    private final Responder.Setup serving;
    /** How a client is asked. */
    private final Initiator.Setup asking;
    /** The clients, by the id of their handshake. */
    private final ConcurrentHashMap<Long, Session> sessions = new ConcurrentHashMap<Long, Session>();
    /** Every connection that is not gone, let in or not. */
    private final Set<Connection> connections = ConcurrentHashMap.<Connection>newKeySet();
    private final AtomicLong live = new AtomicLong();
    private final AtomicLong taken = new AtomicLong();

    private final ScheduledThreadPoolExecutor timer;
    private final Executor executor;
    /** The threads of the handlers when the server has its own; null when it was given an executor. */
    private final ExecutorService ownExecutor;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closing = new AtomicBoolean();
    private volatile boolean closed;

    ServerImpl(Builder b) {
        this.listener = b.listener;
        this.dialect = b.dialect;
        this.authorizer = b.authorizer;
        this.authenticator = b.authenticator;
        this.maxConnections = b.maxConnections;
        this.sendQueue = b.sendQueue;
        this.handshakeTimeoutMillis = b.handshakeTimeoutMillis;
        this.sessionRetentionMillis = b.sessionRetentionMillis;
        this.closeFlushMillis = b.closeFlushMillis;
        this.metrics = b.metrics;
        this.events = b.events;
        this.timer = new ScheduledThreadPoolExecutor(1, r -> daemon("vrpc-server-timer", r));
        this.timer.setRemoveOnCancelPolicy(true);
        if (b.executor != null) {
            this.executor = b.executor;
            this.ownExecutor = null;
        } else {
            this.ownExecutor = Executors.newCachedThreadPool(r -> daemon("vrpc-server-worker", r));
            this.executor = ownExecutor;
        }
        this.serving = new Responder.Setup(true, dialect, registry, metrics, timer, executor, b.maxConcurrentRequests,
                b.maxConcurrentStreams, b.streamWindow, taken);
        // A server names no time of its own for what it asks: a call is
        // bounded by its caller, a stream by the time a client has to take it.
        this.asking = new Initiator.Setup(false, dialect, metrics, timer, b.streamWindow, null, null,
                REVERSE_ESTABLISH_MILLIS);
    }

    private static Thread daemon(String name, Runnable task) {
        final Thread t = new Thread(task, name);
        t.setDaemon(true);
        return t;
    }

    /** Hands a task to a thread of the handlers. False when there is none to be had. */
    private boolean run(Runnable task) {
        try {
            executor.execute(task);
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
    }

    // --- what it serves ------------------------------------------------------------

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

    // --- running -------------------------------------------------------------------

    @Override
    public void start() {
        if (closed) {
            throw new IllegalStateException("the server is closed");
        }
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("the server was started already");
        }
        listener.start(this::accept);
    }

    @Override
    public String address() {
        return listener.address();
    }

    @Override
    public Map<String, Long> stats() {
        final Map<String, Long> out = new LinkedHashMap<String, Long>();
        out.put("connections", live.get());
        out.put("sessions", (long) sessions.size());
        out.put("requests", taken.get());
        return out;
    }

    /** A peer connected. On a thread of the transport: it must not wait. */
    private void accept(MsgConn conn) {
        if (closed) {
            conn.close();
            return;
        }
        if (maxConnections > 0 && live.get() >= maxConnections) {
            // One too many is closed at once, and the ones that are there are served on.
            conn.close();
            events.onRefused(conn.remoteAddress(), "too many connections (max " + maxConnections + ")", null);
            return;
        }
        final Connection c = new Connection(conn);
        live.incrementAndGet();
        connections.add(c);
        if (closed) {
            // Closed meanwhile: closing may have gone through the connections before this one was among them.
            c.close("server closed");
            return;
        }
        c.begin();
    }

    @Override
    public void close() {
        if (!closing.compareAndSet(false, true)) {
            return;
        }
        closed = true;
        listener.close();
        // What is queued is written before anything is torn down: the answer
        // of a call whose handler has returned must not be lost with the
        // connection it was about to leave on. A handler that still runs is
        // not waited for.
        if (closeFlushMillis > 0) {
            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(closeFlushMillis);
            for (Connection c : connections) {
                c.flush(deadline);
            }
        }
        for (Connection c : connections) {
            c.close("server closed");
        }
        for (Session s : sessions.values()) {
            s.end();
        }
        sessions.clear();
        timer.shutdownNow();
        if (ownExecutor != null) {
            // The handlers were interrupted when their connections closed; their threads end as they return.
            ownExecutor.shutdown();
        }
    }

    // --- sessions ------------------------------------------------------------------

    /**
     * A client, across its connections: who it is, the last link of its hash
     * chain that was accepted, and the connection it is on now.
     */
    private final class Session implements Initiator.Host {
        final long clientId;
        final String principal;
        /** What the server asks of this client: the peer a handler is given, across the connections of the client. */
        final Initiator peer = new Initiator(asking, this);
        /** The last link accepted: the next one shown must hash forward to it. */
        private String token;
        private Connection connection;
        private ScheduledFuture<?> reap;
        /** Dropped, or being dropped: it takes no connection any more. */
        private boolean reaped;

        Session(long clientId, String principal, String token, Connection connection) {
            this.clientId = clientId;
            this.principal = principal;
            this.token = token;
            this.connection = connection;
        }

        /**
         * Whether a link proves that who shows it is the client of this
         * session: it comes before the last one accepted, which it then
         * replaces, or it is that one again, shown by a client whose
         * handshake was accepted and never learned of it.
         */
        synchronized boolean proves(String presented) {
            if (HashChain.verifyStep(presented, token, HashChain.DEFAULT_RESYNC_WINDOW) > 0) {
                token = presented;
                return true;
            }
            return HashChain.sameToken(presented, token);
        }

        /** Takes the connection a client came back on. False when the session was dropped meanwhile. */
        boolean replace(Connection next) {
            final Connection old;
            synchronized (this) {
                if (reaped) {
                    return false;
                }
                if (reap != null) {
                    reap.cancel(false);
                    reap = null;
                }
                old = connection;
                connection = next;
            }
            if (old != null && old != next) {
                // What was asked on it cannot be answered there any more, and the client has given it up.
                old.close(REPLACED);
            }
            return true;
        }

        /** The connection to ask the client on. */
        @Override
        public Initiator.Link connected() {
            final Connection c;
            final boolean dropped;
            synchronized (this) {
                c = connection;
                dropped = reaped;
            }
            if (closed) {
                throw new VrpcException(Code.UNAVAILABLE, "server closed");
            }
            if (dropped || c == null || !c.isOpen()) {
                throw new VrpcException(Code.UNAVAILABLE, dropped ? "client closed" : "client is not connected");
            }
            return c;
        }

        @Override
        public VrpcException closed() {
            if (closed) {
                return new VrpcException(Code.UNAVAILABLE, "server closed");
            }
            final boolean dropped;
            synchronized (this) {
                dropped = reaped;
            }
            return dropped ? new VrpcException(Code.UNAVAILABLE, "client closed") : null;
        }

        @Override
        public void protocolError(MapValue message, String problem) {
            final Connection c;
            synchronized (this) {
                c = connection;
            }
            events.onProtocolError(c == null ? "" : c.remote, message, problem);
        }

        /**
         * The connection of the session is gone. The session is kept for a
         * while, for its client to come back to; one that does not is
         * dropped, or every client that ever connected would be remembered.
         */
        void gone(Connection c) {
            synchronized (this) {
                if (reaped || connection != c || closed || sessionRetentionMillis < 0) {
                    return;
                }
                if (reap != null) {
                    reap.cancel(false);
                }
                try {
                    reap = timer.schedule(() -> expire(c), sessionRetentionMillis, TimeUnit.MILLISECONDS);
                } catch (RejectedExecutionException e) {
                    // the server is closing, and drops every session
                }
            }
        }

        private void expire(Connection c) {
            synchronized (this) {
                if (reaped || connection != c) {
                    return; // the client came back
                }
                reaped = true;
            }
            sessions.remove(clientId, this);
            peer.failAll(new VrpcException(Code.UNAVAILABLE, "client closed"));
        }

        void end() {
            synchronized (this) {
                reaped = true;
                if (reap != null) {
                    reap.cancel(false);
                    reap = null;
                }
            }
            peer.failAll(new VrpcException(Code.UNAVAILABLE, "server closed"));
        }
    }

    /**
     * The session of a handshake: a new one for a client that is not known,
     * and the one it had for a client that proves to be the same, which then
     * moves to this connection. Null when the client is known and does not
     * prove it: an id alone, which anyone can send, takes over nobody's
     * session.
     */
    private Session attach(long clientId, String token, String principal, Connection c) {
        for (;;) {
            final Session known = sessions.get(clientId);
            if (known == null) {
                // The first link a client shows is the anchor of its chain. One that shows none cannot come back.
                final Session fresh = new Session(clientId, principal, token, c);
                if (sessions.putIfAbsent(clientId, fresh) == null) {
                    return fresh;
                }
                continue; // another connection of the same id was first: it is the one to prove against
            }
            // Who it is comes first, so that someone else does not even move the chain on.
            if (!known.principal.equals(principal) || !known.proves(token) || !known.replace(c)) {
                return null;
            }
            c.resumed = true;
            return known;
        }
    }

    // --- connections ---------------------------------------------------------------

    private final class Connection implements MsgConn.Receiver, Initiator.Link, Responder.Link, Responder.Events {
        final MsgConn conn;
        final String remote;
        /** What the client asks on this connection. */
        final Responder responder = new Responder(serving, this, this);

        private final Object shaking = new Object();
        /** Under {@code shaking}. */
        private int stage = WAITING;
        /** What came behind the handshake before it was answered. Under {@code shaking}. */
        private ArrayList<MapValue> early;
        private volatile boolean open;
        private volatile ScheduledFuture<?> handshakeTimer;
        private volatile Session session;
        volatile boolean resumed;

        /** What waits to be written; there is none before the client is let in. */
        private volatile ArrayBlockingQueue<MapValue> outbox;
        private volatile Thread sender;
        /** The messages taken in to be written, and the ones written: the same when nothing waits. */
        private final AtomicLong queued = new AtomicLong();
        private final AtomicLong written = new AtomicLong();
        private final ReentrantLock flushLock = new ReentrantLock();
        private final Condition flushed = flushLock.newCondition();
        private volatile boolean flushing;

        private final AtomicBoolean over = new AtomicBoolean();
        /** What its requests are told when it is gone. */
        private volatile String why = "connection lost";

        Connection(MsgConn conn) {
            this.conn = conn;
            this.remote = conn.remoteAddress();
        }

        // --- what the two halves of the engine ask of a connection ---

        /** Whether the server can ask on it: its client is let in, and it is not gone. */
        @Override
        public boolean isOpen() {
            return outbox != null && !over.get();
        }

        @Override
        public String remoteAddress() {
            return remote;
        }

        @Override
        public String principal() {
            final Session s = session;
            return s == null ? "" : s.principal;
        }

        @Override
        public Peer peer() {
            final Session s = session;
            return s == null ? null : s.peer;
        }

        @Override
        public void protocolError(MapValue message, String problem) {
            events.onProtocolError(remote, message, problem);
        }

        @Override
        public void handlerFailure(String where, Throwable error) {
            events.onHandlerFailure(where, error);
        }

        /** Begins to read, once the authorizer has let the connection in. */
        void begin() {
            if (handshakeTimeoutMillis > 0) {
                try {
                    // The time is for the handshake to arrive: what an authenticator takes is its own.
                    handshakeTimer = timer.schedule(() -> {
                        synchronized (shaking) {
                            if (stage != WAITING) {
                                return;
                            }
                        }
                        refuse(null, "no handshake in " + handshakeTimeoutMillis + " ms", null);
                    }, handshakeTimeoutMillis, TimeUnit.MILLISECONDS);
                } catch (RejectedExecutionException e) {
                    close("server closed");
                    return;
                }
            }
            if (authorizer == null) {
                conn.start(this);
                return;
            }
            // The authorizer is the application's and may wait: not on the thread that accepts.
            final boolean handed = run(() -> {
                try {
                    authorizer.authorize(conn);
                } catch (Exception | Error e) {
                    refuse(null, "refused by the authorizer", e);
                    return;
                }
                if (!over.get()) {
                    conn.start(this);
                }
            });
            if (!handed) {
                close("server closed");
            }
        }

        // --- reading ---

        @Override
        public void onMessage(MapValue message) {
            if (open) {
                dispatch(message);
                return;
            }
            final String reason;
            synchronized (shaking) {
                switch (stage) {
                    case WAITING:
                        stage = SHAKING;
                        if (run(() -> shake(message))) {
                            return;
                        }
                        reason = "no thread for the handshake";
                        break;
                    case SHAKING:
                        // A client waits for the answer before it asks; one that does not is let ask a little.
                        if (early == null) {
                            early = new ArrayList<MapValue>();
                        }
                        if (early.size() < MAX_EARLY) {
                            early.add(message);
                            return;
                        }
                        reason = "the answer to the handshake was not waited for";
                        break;
                    case OVER:
                        return;
                    default:
                        // Let in while this waited to look: what waited before it has been handed on.
                        dispatch(message);
                        return;
                }
            }
            refuse(null, reason, null);
        }

        @Override
        public void onClosed(Throwable cause) {
            if (over.compareAndSet(false, true)) {
                teardown(cause);
            }
        }

        /** The handshake, on a thread of the handlers: the authenticator is the application's and may wait. */
        private void shake(MapValue request) {
            final ScheduledFuture<?> t = handshakeTimer;
            if (t != null) {
                t.cancel(false);
            }
            final Session s;
            try {
                s = handshake(request);
            } catch (RuntimeException | Error e) {
                refuse(null, "the handshake failed", e);
                return;
            }
            if (s == null) {
                return;
            }
            session = s;
            // The answer to the handshake is queued before the queue is there
            // for anyone else: nothing the server asks of this client goes
            // out before it. The server has no token to give: the client owns
            // the chain.
            final ArrayBlockingQueue<MapValue> box = new ArrayBlockingQueue<MapValue>(sendQueue);
            box.add(dialect.handshakeResponse(null));
            queued.incrementAndGet();
            outbox = box;
            final Thread w = daemon("vrpc-send " + remote, this::sendLoop);
            sender = w;
            w.start();
            synchronized (shaking) {
                if (stage == SHAKING) {
                    stage = OPEN;
                    final ArrayList<MapValue> waited = early;
                    early = null;
                    if (waited != null) {
                        for (MapValue m : waited) {
                            dispatch(m);
                        }
                    }
                    open = true;
                }
            }
            if (over.get()) {
                // Lost while it was let in: what tears a connection down may
                // have run before the session was there to be told.
                s.gone(this);
                return;
            }
            events.onConnected(s.clientId, s.principal, remote, resumed);
        }

        /**
         * Looks at a handshake and gives the session of its client, or
         * refuses the connection and gives null.
         */
        private Session handshake(MapValue request) {
            // What is not a handshake of this dialect is answered with
            // silence: a probe must learn nothing from how it is refused.
            if (dialect.messageType(request) != MessageType.HANDSHAKE_REQUEST || !dialect.validMagicAndVersion(request)) {
                refuse(null, "not a handshake", null);
                return null;
            }
            // From here on a refusal says why, in words that give nothing away.
            final NumberValue rid = Value.ofLong(dialect.handshakeRequestId());
            String principal = "";
            if (authenticator != null) {
                try {
                    final String p = authenticator.authenticate(conn, request.get(dialect.authField()));
                    principal = p == null ? "" : p;
                } catch (Exception | Error e) {
                    refuse(dialect.error(rid, Code.UNAUTHENTICATED, "authentication failed"), "authentication failed", e);
                    return null;
                }
            }
            final NumberValue cid = Dialect.number(request, dialect.clientIdField());
            if (cid == null) {
                refuse(dialect.error(rid, Code.INVALID_ARGUMENT, "malformed handshake"), "a handshake without a client id", null);
                return null;
            }
            final StringValue tok = Dialect.string(request, dialect.sessionTokenField());
            final Session s = attach(cid.longValue(), tok == null ? "" : tok.toString(), principal, this);
            if (s == null) {
                refuse(dialect.error(rid, Code.UNAUTHENTICATED, "session resume rejected"),
                        "client " + cid.longValue() + " did not prove to be the one of its session", null);
            }
            return s;
        }

        /** Closes a connection that is not let in, after telling it why when there is something to tell. */
        private void refuse(MapValue answer, String reason, Throwable cause) {
            if (!over.compareAndSet(false, true)) {
                return;
            }
            if (answer != null) {
                try {
                    // Nothing else is written before a client is let in.
                    conn.write(Collections.singletonList(answer));
                } catch (IOException | RuntimeException e) {
                    // it is closed either way
                }
            }
            conn.close();
            teardown(null);
            events.onRefused(remote, reason, cause);
        }

        /** Handles a message of a client that is let in. On the thread that reads the connection: it must not wait. */
        private void dispatch(MapValue message) {
            final NumberValue typeNumber = dialect.typeNumber(message);
            if (typeNumber == null) {
                events.onProtocolError(remote, message, "message type not found");
                return;
            }
            final NumberValue idValue = dialect.requestId(message);
            if (idValue == null) {
                events.onProtocolError(remote, message, "request id not found");
                return;
            }
            final long id = idValue.longValue();
            final MessageType type = MessageType.of(typeNumber.longValue());
            if (id > 0) {
                // What the client asks, and what it sends for a request of its own.
                responder.onMessage(type, idValue, message);
                return;
            }
            if (id < 0 && (type == null || !type.opensRequest())) {
                // About a request this server opened on the client.
                final Session s = session;
                if (s != null) {
                    s.peer.onMessage(type, id, message);
                }
                return;
            }
            if (type == MessageType.CANCEL_REQUEST || type == MessageType.FUNCTION_RESPONSE
                    || type == MessageType.ERROR_RESPONSE) {
                // With the id of the handshake: nothing waits for it.
                return;
            }
            // A request of a client has a positive id: 0 is the handshake, and the negative ones are the server's to give.
            trySend(dialect.error(idValue, Code.INVALID_ARGUMENT, "client-initiated request id must be positive"));
            events.onProtocolError(remote, message, "client request id must be positive");
        }

        // --- writing ---

        /** Queues a message, waiting while the queue is full. False when the connection is gone. */
        @Override
        public boolean send(MapValue message) {
            final ArrayBlockingQueue<MapValue> box = outbox;
            if (box == null) {
                return false;
            }
            try {
                while (!over.get()) {
                    if (box.offer(message, 100, TimeUnit.MILLISECONDS)) {
                        queued.incrementAndGet();
                        return true;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return false;
        }

        /** Queues a message if there is room at once: for the thread that reads, which must not wait. */
        @Override
        public boolean trySend(MapValue message) {
            final ArrayBlockingQueue<MapValue> box = outbox;
            if (box != null && !over.get() && box.offer(message)) {
                queued.incrementAndGet();
                return true;
            }
            return false;
        }

        private void sendLoop() {
            final ArrayBlockingQueue<MapValue> box = outbox;
            final ArrayList<MapValue> batch = new ArrayList<MapValue>(Frames.MAX_COALESCE);
            try {
                while (!over.get()) {
                    final MapValue first = box.poll(1, TimeUnit.SECONDS);
                    if (first == null) {
                        continue;
                    }
                    // What already waits goes out with it in one write; nothing is waited for.
                    batch.clear();
                    batch.add(first);
                    box.drainTo(batch, Frames.MAX_COALESCE - 1);
                    conn.write(batch);
                    written.addAndGet(batch.size());
                    if (flushing) {
                        flushLock.lock();
                        try {
                            flushed.signalAll();
                        } finally {
                            flushLock.unlock();
                        }
                    }
                }
            } catch (InterruptedException e) {
                // closed
            } catch (IOException | RuntimeException e) {
                if (over.compareAndSet(false, true)) {
                    conn.close();
                    teardown(e);
                }
            }
        }

        /**
         * Waits until what was queued when it was asked has been written.
         * It ends when the connection is gone, and at the time given: a peer
         * that does not read must not hold up the closing of a server.
         */
        void flush(long deadlineNanos) {
            final long target = queued.get();
            flushing = true;
            flushLock.lock();
            try {
                while (written.get() < target && !over.get()) {
                    final long left = deadlineNanos - System.nanoTime();
                    if (left <= 0) {
                        return;
                    }
                    flushed.awaitNanos(left);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                flushLock.unlock();
            }
        }

        // --- the end ---

        /** Closes the connection from this side. */
        void close(String reason) {
            if (over.compareAndSet(false, true)) {
                why = reason;
                conn.close();
                teardown(null);
            }
        }

        /** What follows a connection being gone, however it went. Once. */
        private void teardown(Throwable cause) {
            final boolean wasOpen;
            synchronized (shaking) {
                wasOpen = stage == OPEN;
                stage = OVER;
                early = null;
            }
            final ScheduledFuture<?> t = handshakeTimer;
            if (t != null) {
                t.cancel(false);
            }
            final Thread w = sender;
            if (w != null && w != Thread.currentThread()) {
                w.interrupt();
            }
            connections.remove(this);
            live.decrementAndGet();
            // Nobody waits for its requests any more: their handlers are told, and the streams end.
            responder.lost(why);
            final Session s = session;
            if (s != null) {
                // What the server asked on it cannot be answered any more either.
                s.peer.failOn(this, REPLACED.equals(why) ? REPLACED_REVERSE : null);
                s.gone(this);
            }
            flushLock.lock();
            try {
                flushed.signalAll();
            } finally {
                flushLock.unlock();
            }
            if (wasOpen && s != null) {
                events.onDisconnected(s.clientId, remote, cause);
            }
        }
    }
}
