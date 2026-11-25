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
import com.arpabet.valuerpc.CreditGate;
import com.arpabet.valuerpc.Dialect;
import com.arpabet.valuerpc.Frames;
import com.arpabet.valuerpc.HashChain;
import com.arpabet.valuerpc.IncomingStreamHandler;
import com.arpabet.valuerpc.MessageType;
import com.arpabet.valuerpc.Metrics;
import com.arpabet.valuerpc.OutgoingStreamHandler;
import com.arpabet.valuerpc.RequestContext;
import com.arpabet.valuerpc.StreamBuffer;
import com.arpabet.valuerpc.TypeDef;
import com.arpabet.valuerpc.UnaryHandler;
import com.arpabet.valuerpc.ValueSink;
import com.arpabet.valuerpc.ValueStream;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The server. A connection has a thread of the transport that reads it and
 * hands each message to {@link Connection#dispatch}, which never waits, and a
 * thread of the server that writes what is queued for it, several messages
 * at once when several wait. A request is a number and a handler on a thread
 * of its own; a message that carries the number is handed to it.
 *
 * <p>A client is a session, known by the id its handshake carries, and a
 * session has one connection at a time. The requests of a client belong to
 * the connection they came on: when it is lost, or the client comes back on
 * another, they are cancelled, since the client has given them up too.
 */
final class ServerImpl implements VrpcServer {

    /** A connection before its handshake, while it is looked at, once it is let in, and when it is gone. */
    private static final int WAITING = 0;
    private static final int SHAKING = 1;
    private static final int OPEN = 2;
    private static final int OVER = 3;

    /** How many messages a client may send behind its handshake before the handshake is answered. */
    private static final int MAX_EARLY = 64;

    private final Listener listener;
    private final Dialect dialect;
    private final ConnectAuthorizer authorizer;
    private final Authenticator authenticator;
    private final long maxConnections;
    private final long maxRequests;
    private final long maxStreams;
    private final int sendQueue;
    private final int streamWindow;
    private final long handshakeTimeoutMillis;
    private final long sessionRetentionMillis;
    private final long closeFlushMillis;
    private final Metrics metrics;
    private final ServerListener events;

    /** What is served, by name. */
    private final ConcurrentHashMap<String, Registered> functions = new ConcurrentHashMap<String, Registered>();
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
        this.maxRequests = b.maxConcurrentRequests;
        this.maxStreams = b.maxConcurrentStreams;
        this.sendQueue = b.sendQueue;
        this.streamWindow = b.streamWindow;
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

    /** What a name serves: one pattern, with what its arguments and its result must be. */
    private static final class Registered {
        final MessageType kind;
        final TypeDef args;
        final TypeDef result;
        final UnaryHandler unary;
        final OutgoingStreamHandler outgoing;
        final IncomingStreamHandler incoming;
        final ChatHandler chat;

        Registered(MessageType kind, TypeDef args, TypeDef result, UnaryHandler unary, OutgoingStreamHandler outgoing,
                IncomingStreamHandler incoming, ChatHandler chat) {
            this.kind = kind;
            this.args = args == null ? TypeDef.ANY : args;
            this.result = result == null ? TypeDef.ANY : result;
            this.unary = unary;
            this.outgoing = outgoing;
            this.incoming = incoming;
            this.chat = chat;
        }
    }

    private void register(String name, Object handler, Registered fn) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("what is served needs a name");
        }
        if (handler == null) {
            throw new IllegalArgumentException("no handler for " + name);
        }
        // A server refuses a name twice, where a client replaces: as in the Go implementation.
        if (functions.putIfAbsent(name, fn) != null) {
            throw new IllegalStateException("function already exists: " + name);
        }
    }

    @Override
    public void addFunction(String name, TypeDef args, TypeDef result, UnaryHandler handler) {
        register(name, handler, new Registered(MessageType.FUNCTION_REQUEST, args, result, handler, null, null, null));
    }

    @Override
    public void addOutgoingStream(String name, TypeDef args, OutgoingStreamHandler handler) {
        register(name, handler, new Registered(MessageType.GET_STREAM_REQUEST, args, TypeDef.VOID, null, handler, null, null));
    }

    @Override
    public void addIncomingStream(String name, TypeDef args, IncomingStreamHandler handler) {
        register(name, handler, new Registered(MessageType.PUT_STREAM_REQUEST, args, TypeDef.VOID, null, null, handler, null));
    }

    @Override
    public void addChat(String name, TypeDef args, ChatHandler handler) {
        register(name, handler, new Registered(MessageType.CHAT_REQUEST, args, TypeDef.VOID, null, null, null, handler));
    }

    /** The number the Go implementation prints for a pattern, in the answer to a request of the wrong one. */
    private static int pattern(MessageType kind) {
        switch (kind) {
            case FUNCTION_REQUEST:
                return 0;
            case GET_STREAM_REQUEST:
                return 1;
            case PUT_STREAM_REQUEST:
                return 2;
            default:
                return 3;
        }
    }

    /** What a handler is called in the answer to its caller: {@code function greet}. */
    private static String where(MessageType kind, String name) {
        switch (kind) {
            case FUNCTION_REQUEST:
                return "function " + name;
            case GET_STREAM_REQUEST:
                return "out stream " + name;
            case PUT_STREAM_REQUEST:
                return "in stream " + name;
            default:
                return "chat " + name;
        }
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
    private final class Session {
        final long clientId;
        final String principal;
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
                old.close("connection replaced by reconnect");
            }
            return true;
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
        }

        synchronized void end() {
            reaped = true;
            if (reap != null) {
                reap.cancel(false);
                reap = null;
            }
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

    private final class Connection implements MsgConn.Receiver {
        final MsgConn conn;
        final String remote;
        /** The requests that are not over, by their id. */
        final ConcurrentHashMap<Long, Serving> requests = new ConcurrentHashMap<Long, Serving>();
        /** The handlers that run and have not taken their stream yet: what the limit of requests counts. */
        final AtomicLong inFlight = new AtomicLong();
        final AtomicLong liveStreams = new AtomicLong();

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
            outbox = new ArrayBlockingQueue<MapValue>(sendQueue);
            final Thread w = daemon("vrpc-send " + remote, this::sendLoop);
            sender = w;
            w.start();
            // The server has no token to give: the client owns the chain.
            send(dialect.handshakeResponse(null));
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
            final Serving serving = requests.get(id);
            if (serving != null) {
                serving.onMessage(type, message);
                return;
            }
            if (type == MessageType.CANCEL_REQUEST || type == MessageType.FUNCTION_RESPONSE
                    || type == MessageType.ERROR_RESPONSE) {
                // For a request that is over: a cancel that crossed its answer, an answer nobody waits for.
                return;
            }
            if (id <= 0) {
                // 0 is the handshake, and the negative ones are the server's to give.
                trySend(dialect.error(idValue, Code.INVALID_ARGUMENT, "client-initiated request id must be positive"));
                events.onProtocolError(remote, message, "client request id must be positive");
                return;
            }
            if (type == null) {
                events.onProtocolError(remote, message, "unknown message type");
                return;
            }
            if (!type.opensRequest()) {
                // A value or a credit of a stream that is over: on its way when the stream ended.
                return;
            }
            // A handler slot is taken, or the request refused: it is never
            // queued, and the connection is read on either way.
            if (inFlight.incrementAndGet() > maxRequests && maxRequests > 0) {
                inFlight.decrementAndGet();
                trySend(dialect.error(idValue, Code.RESOURCE_EXHAUSTED,
                        "server busy: too many concurrent requests (max " + maxRequests + ")"));
                return;
            }
            taken.incrementAndGet();
            final Serving s = new Serving(this, idValue, type, message);
            // Known before its handler runs, so that a cancel always finds it.
            requests.put(id, s);
            if (over.get()) {
                s.lost(why);
            }
            if (!run(s::serve)) {
                s.refuse(Code.UNAVAILABLE, closed ? "server closed" : "server busy: no thread for the request");
            }
        }

        // --- writing ---

        /** Queues a message, waiting while the queue is full. False when the connection is gone. */
        boolean send(MapValue message) {
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
        boolean trySend(MapValue message) {
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
            for (Serving s : requests.values()) {
                s.lost(why);
            }
            final Session s = session;
            if (s != null) {
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

    // --- requests ------------------------------------------------------------------

    /**
     * A request that is not over, and what its handler is told about it. It
     * is made on the thread that reads, when the request arrives, and served
     * on a thread of the handlers.
     *
     * <p>A request is over for the peer when it is retired: taken out of the
     * requests of its connection, by whoever ends it, once. A call is retired
     * when its handler has returned. A stream is retired the moment it ends,
     * by either side or with the connection; its handler may still be on its
     * way out then, and learns of the end from its stream.
     */
    private final class Serving implements RequestContext {
        final Connection c;
        final NumberValue idValue;
        final long id;
        final MessageType kind;
        final MapValue request;
        final String name;
        final String where;
        final long startNanos = System.nanoTime();

        /** What the peer sends; null when it sends nothing. */
        final StreamBuffer in;
        /** The credit to send with; null when this side sends nothing. */
        final CreditGate out;

        private final AtomicBoolean retired = new AtomicBoolean();
        /** The slot of a handler, held until the call is over or the stream is taken. */
        private final AtomicBoolean slot = new AtomicBoolean(true);
        /** Counted among the open streams of the connection. Under the monitor. */
        private boolean counted;

        private volatile Map<String, String> metadata;
        /** When the time the caller allowed is up, in nanoseconds of the clock; 0 for none. */
        private volatile long deadlineNanos;
        private volatile ScheduledFuture<?> deadline;

        private volatile boolean cancelled;
        /** Nobody is to be answered: the peer gave the request up, or is gone. */
        private volatile boolean quiet;
        private volatile boolean deadlinePassed;
        /** Why a stream is gone, for a handler that asks its stream. */
        private volatile VrpcException error;
        /** What runs when the request is cancelled. Under the monitor. */
        private ArrayList<Runnable> callbacks;
        /** The thread of the handler, while it runs: the one to interrupt. Under the monitor. */
        private Thread thread;

        /** Guards the step that tells the peer the stream is taken, so that nothing of the stream leaves before it. */
        private final Object taking = new Object();
        private boolean established;
        private volatile boolean outEnded;
        /** Values taken since credit was last granted. Only the thread that reads the stream touches it. */
        private long takenSinceGrant;

        Serving(Connection c, NumberValue idValue, MessageType kind, MapValue request) {
            this.c = c;
            this.idValue = idValue;
            this.id = idValue.longValue();
            this.kind = kind;
            this.request = request;
            final String n = dialect.functionName(request);
            this.name = n == null ? "" : n;
            this.where = ServerImpl.where(kind, name);
            this.in = kind == MessageType.PUT_STREAM_REQUEST || kind == MessageType.CHAT_REQUEST
                    ? new StreamBuffer(streamWindow, this::taken1) : null;
            this.out = kind == MessageType.GET_STREAM_REQUEST || kind == MessageType.CHAT_REQUEST
                    ? new CreditGate() : null;
            metrics.requestBegin(name);
        }

        // --- what a handler is told ---

        @Override
        public long requestId() {
            return id;
        }

        @Override
        public String method() {
            return name;
        }

        @Override
        public Map<String, String> metadata() {
            Map<String, String> md = metadata;
            if (md == null) {
                md = dialect.metadata(request);
                metadata = md;
            }
            return md;
        }

        @Override
        public String principal() {
            final Session s = c.session;
            return s == null ? "" : s.principal;
        }

        @Override
        public String remoteAddress() {
            return c.remote;
        }

        @Override
        public long remainingMillis() {
            final long at = deadlineNanos;
            if (at == 0) {
                return -1;
            }
            return Math.max(0, TimeUnit.NANOSECONDS.toMillis(at - System.nanoTime()));
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public void onCancel(Runnable callback) {
            if (callback == null) {
                return;
            }
            synchronized (this) {
                if (!cancelled) {
                    if (callbacks == null) {
                        callbacks = new ArrayList<Runnable>(2);
                    }
                    callbacks.add(callback);
                    return;
                }
            }
            run(callback);
        }

        // --- how it ends ---

        private void releaseSlot() {
            if (slot.compareAndSet(true, false)) {
                c.inFlight.decrementAndGet();
            }
        }

        /**
         * Takes the request out of the ones that are not over. Whoever takes
         * it out is the one to say how it ended: every way a request can end
         * comes here, and only the first gets true.
         */
        private boolean retire(Code code) {
            return retire(code, null);
        }

        /**
         * Retires the request and leaves with it why it is gone, in one step
         * for whoever looks at it under its monitor: a handler that finds its
         * stream over can then say why, also when the one that ended it has
         * not yet come to let go of what waits on the stream.
         */
        private boolean retire(Code code, VrpcException reason) {
            final boolean wasCounted;
            synchronized (this) {
                if (!retired.compareAndSet(false, true)) {
                    return false;
                }
                error = reason;
                wasCounted = counted;
                counted = false;
            }
            c.requests.remove(id, this);
            releaseSlot();
            if (wasCounted) {
                c.liveStreams.decrementAndGet();
            }
            final ScheduledFuture<?> t = deadline;
            if (t != null) {
                t.cancel(false);
            }
            metrics.requestEnd(name, code, System.nanoTime() - startNanos);
            return true;
        }

        /**
         * Nobody waits for the request any more. The handler is told: by the
         * flag, by what it asked to have run, and by an interrupt, which
         * wakes one that waits.
         *
         * @param silently nobody is there to be answered either
         */
        private void cancel(boolean silently, boolean byDeadline) {
            final ArrayList<Runnable> run;
            synchronized (this) {
                if (silently) {
                    quiet = true;
                }
                if (cancelled) {
                    return;
                }
                deadlinePassed = byDeadline;
                cancelled = true;
                if (thread != null) {
                    thread.interrupt();
                }
                run = callbacks;
                callbacks = null;
            }
            if (run != null) {
                for (Runnable r : run) {
                    ServerImpl.this.run(r);
                }
            }
        }

        /** Lets go of whoever waits on the stream: they ask why, and are told. */
        private void drop() {
            if (in != null) {
                in.stop();
            }
            if (out != null) {
                out.close();
            }
        }

        /** The connection is gone, or the server closes. */
        void lost(String reason) {
            final boolean stream = kind != MessageType.FUNCTION_REQUEST
                    && retire(Code.UNAVAILABLE, new VrpcException(Code.UNAVAILABLE, reason));
            // In this order: a handler that is woken, by the interrupt or by
            // its stream, finds its request cancelled and can ask why.
            cancel(true, false);
            if (stream) {
                drop();
            }
        }

        /** Refused before it ran, on the thread that reads: the answer is sent if there is room for it at once. */
        void refuse(Code code, String text) {
            if (retire(code)) {
                c.trySend(dialect.error(idValue, code, text));
            }
        }

        private void fail(Code code, String text) {
            if (retire(code)) {
                c.send(dialect.error(idValue, code, text));
            }
        }

        /** A message of the client for this request. On the thread that reads the connection: it must not wait. */
        void onMessage(MessageType type, MapValue message) {
            if (type == null) {
                events.onProtocolError(c.remote, message, "unknown message type");
                return;
            }
            switch (type) {
                case CANCEL_REQUEST:
                    if (kind == MessageType.FUNCTION_REQUEST) {
                        // The handler is told, and the call is over when it has returned: it is not answered.
                        cancel(true, false);
                    } else if (retire(Code.CANCELED,
                            new VrpcException(Code.CANCELED, "request " + id + " canceled by peer"))) {
                        cancel(true, false);
                        drop();
                    }
                    break;
                case STREAM_VALUE: {
                    if (in == null) {
                        events.onProtocolError(c.remote, message, "a value for a request that takes none");
                        break;
                    }
                    final Value v = dialect.streamValueOf(message);
                    if (v == null) {
                        break;
                    }
                    if (in.offer(v)) {
                        metrics.streamValue(name);
                    } else if (in.overflowed()) {
                        // The client sent more than it had credit for: the
                        // stream is cut short, and both sides are told.
                        final VrpcException truncated = new VrpcException(Code.RESOURCE_EXHAUSTED,
                                "inbound stream " + id + " truncated: client exceeded flow-control credit");
                        if (!retire(Code.RESOURCE_EXHAUSTED, truncated)) {
                            break;
                        }
                        c.trySend(dialect.error(idValue, Code.RESOURCE_EXHAUSTED, truncated.detail()));
                        cancel(true, false);
                        drop();
                    }
                    break;
                }
                case STREAM_END: {
                    if (in == null) {
                        events.onProtocolError(c.remote, message, "an end for a request that takes no values");
                        break;
                    }
                    // An end may carry a last value.
                    final Value last = dialect.streamValueOf(message);
                    if (last != null && in.offer(last)) {
                        metrics.streamValue(name);
                    }
                    in.end();
                    if (kind == MessageType.PUT_STREAM_REQUEST) {
                        // The stream is over for the client. The handler
                        // takes what has arrived at its own pace, and is not
                        // disturbed.
                        retire(Code.OK);
                    }
                    // A chat goes on: the client ending what it sends ends nothing else.
                    break;
                }
                case STREAM_CREDIT: {
                    final NumberValue credit = dialect.credit(message);
                    if (out != null && credit != null) {
                        out.grant(credit.longValue());
                    }
                    break;
                }
                default:
                    events.onProtocolError(c.remote, message, "unknown message type");
            }
        }

        // --- serving ---

        /** Serves the request, on a thread of the handlers, to the end of its handler. */
        void serve() {
            synchronized (this) {
                if (cancelled) {
                    // Given up before it began.
                    retire(Code.CANCELED);
                    return;
                }
                thread = Thread.currentThread();
            }
            try {
                admitted();
            } catch (RuntimeException | Error e) {
                left();
                retire(Code.INTERNAL);
                drop();
                events.onHandlerFailure(where, e);
            }
        }

        /** The handler has returned: nothing interrupts this thread on its behalf from here on. */
        private void left() {
            synchronized (this) {
                thread = null;
            }
            Thread.interrupted();
        }

        /**
         * Checks the request against what is registered, in the order of the
         * Go server, and runs its handler.
         */
        private void admitted() {
            if (dialect.functionName(request) == null) {
                left();
                fail(Code.INVALID_ARGUMENT, "function name field not found");
                return;
            }
            final Registered fn = functions.get(name);
            if (fn == null) {
                left();
                fail(Code.NOT_FOUND, "function not found " + name);
                return;
            }
            final Value args = dialect.arguments(request);
            if (!fn.args.verify(args)) {
                left();
                fail(Code.INVALID_ARGUMENT, "function '" + name + "' invalid args");
                return;
            }
            if (fn.kind != kind) {
                left();
                fail(Code.INVALID_ARGUMENT, "function wrong type " + name + ", expected " + pattern(fn.kind)
                        + ", actual " + pattern(kind));
                return;
            }
            if (kind == MessageType.FUNCTION_REQUEST) {
                call(fn, args);
            } else {
                stream(fn, args);
            }
        }

        private void call(Registered fn, Value args) {
            // The time the caller allows is the deadline of the handler: of a call only.
            final long sla = dialect.slaMillis(request);
            if (sla > 0) {
                deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(sla);
                try {
                    deadline = timer.schedule(() -> cancel(false, true), sla, TimeUnit.MILLISECONDS);
                } catch (RejectedExecutionException e) {
                    // the server is closing, which cancels the call itself
                }
            }
            Value result = null;
            Throwable thrown = null;
            try {
                result = fn.unary.handle(this, args);
            } catch (Throwable t) {
                thrown = t;
            }
            left();
            final MapValue answer;
            final Code code;
            if (thrown != null) {
                answer = failure(thrown);
                code = Code.of(dialect.errorOf(answer).codeNumber());
            } else if (!fn.result.verify(result)) {
                answer = dialect.error(idValue, Code.INTERNAL, "function '" + name + "' invalid results");
                code = Code.INTERNAL;
            } else {
                answer = dialect.functionResult(idValue, result);
                code = Code.OK;
            }
            if (retire(code) && !quiet) {
                c.send(answer);
            }
        }

        private void stream(Registered fn, Value args) {
            // A stream counts among the open ones from here to its end.
            final boolean room;
            synchronized (this) {
                if (retired.get()) {
                    room = true; // over already: it is not counted, and what follows finds it so
                } else if (c.liveStreams.incrementAndGet() > maxStreams && maxStreams > 0) {
                    c.liveStreams.decrementAndGet();
                    room = false;
                } else {
                    counted = true;
                    room = true;
                }
            }
            if (!room) {
                left();
                fail(Code.RESOURCE_EXHAUSTED, "server busy: too many concurrent streams (max " + maxStreams + ")");
                return;
            }
            if (in != null && !retired.get()) {
                // The client may send this many values. The credit comes before the stream is said to be taken.
                c.send(dialect.streamCredit(idValue, streamWindow));
            }
            Throwable thrown = null;
            try {
                switch (kind) {
                    case GET_STREAM_REQUEST:
                        fn.outgoing.handle(this, args, outgoing);
                        break;
                    case PUT_STREAM_REQUEST:
                        fn.incoming.handle(this, args, incoming);
                        break;
                    default:
                        fn.chat.handle(this, args, incoming, outgoing);
                }
            } catch (Throwable t) {
                thrown = t;
            }
            left();
            if (thrown != null) {
                // Before the stream was taken this is what the client gets in
                // place of it; after, it ends the stream.
                final MapValue answer = failure(thrown);
                if (retire(Code.of(dialect.errorOf(answer).codeNumber()))) {
                    if (!quiet) {
                        c.send(answer);
                    }
                    drop();
                }
                return;
            }
            ready();
            if (kind == MessageType.PUT_STREAM_REQUEST) {
                // The handler is done. What the client has not sent yet is not wanted, and it is told to stop.
                giveUp(Code.OK);
            } else {
                endOut();
            }
        }

        /**
         * The answer to a handler that threw. One that was interrupted
         * because its request was cancelled says so as the Go server does,
         * whose handler returns the error of its context.
         */
        private MapValue failure(Throwable thrown) {
            int number = VrpcException.handlerCodeNumber(thrown);
            String detail = VrpcException.handlerDetail(thrown);
            final VrpcException coded = VrpcException.in(thrown);
            final boolean chosen = coded != null && coded.codeNumber() != Code.UNKNOWN.number();
            if (!chosen && number == Code.CANCELED.number() && cancelled) {
                if (deadlinePassed) {
                    number = Code.DEADLINE_EXCEEDED.number();
                    detail = "context deadline exceeded";
                } else {
                    detail = "context canceled";
                }
            }
            if (number == Code.INTERNAL.number() && !chosen) {
                events.onHandlerFailure(where, thrown);
            }
            return dialect.error(idValue, number, where + ": " + detail);
        }

        // --- streams ---

        /**
         * Tells the client that the stream is taken, once: when the handler
         * first reads or sends, or when it returns. Until then a handler that
         * throws refuses the stream. From here on the stream no longer holds
         * the slot of a handler.
         */
        private void ready() {
            synchronized (taking) {
                if (established) {
                    return;
                }
                established = true;
                releaseSlot();
                if (!retired.get()) {
                    c.send(dialect.streamReady(idValue));
                }
            }
        }

        /** Why this stream takes no more: what ended it, or else that it is over. */
        private VrpcException gone() {
            final VrpcException e;
            synchronized (this) {
                e = error;
            }
            return e != null ? e : new VrpcException(Code.CANCELED, "stream " + id + " is closed");
        }

        /** The handler took a value: the client may send another, told in batches of half the window. */
        private void taken1() {
            takenSinceGrant++;
            if (takenSinceGrant >= Math.max(1, streamWindow / 2)) {
                final long n = takenSinceGrant;
                takenSinceGrant = 0;
                if (!retired.get()) {
                    c.send(dialect.streamCredit(idValue, n));
                }
            }
        }

        /**
         * What a handler that was interrupted while it waited on its stream
         * is told: why the stream is gone, when it is, since that is why it
         * was interrupted. Any other interrupt is the handler's own.
         */
        private InterruptedException woken(InterruptedException e) {
            final VrpcException why;
            synchronized (this) {
                why = error;
            }
            if (why == null) {
                return e;
            }
            Thread.currentThread().interrupt();
            throw why;
        }

        private Value takeValue(long nanos) throws InterruptedException, TimeoutException {
            ready();
            final Value v;
            try {
                v = in.take(nanos);
            } catch (InterruptedException e) {
                throw woken(e);
            }
            if (v == null) {
                final VrpcException e = error;
                if (e != null) {
                    throw e;
                }
            }
            return v;
        }

        private void sendValue(Value value) throws InterruptedException {
            if (value == null || value.isNull()) {
                throw new IllegalArgumentException("a stream does not carry null");
            }
            ready();
            if (outEnded || retired.get()) {
                throw gone();
            }
            // Wait for the client to have room: only this thread waits.
            final boolean room;
            try {
                room = out.acquire();
            } catch (InterruptedException e) {
                throw woken(e);
            }
            if (!room) {
                throw gone();
            }
            if (retired.get()) {
                throw gone();
            }
            if (!c.send(dialect.streamValue(idValue, value))) {
                throw gone();
            }
            metrics.streamValue(name);
        }

        /** This side has no more to send, which ends the stream, of either kind. */
        private void endOut() {
            synchronized (taking) {
                if (outEnded) {
                    return;
                }
                outEnded = true;
            }
            ready();
            if (retire(Code.OK)) {
                c.send(dialect.streamEnd(idValue, null));
                drop();
            }
        }

        /** The handler gives the stream up before its end, which the client is told. */
        private void giveUp(Code code) {
            if (retire(code)) {
                c.send(dialect.cancelRequest(idValue));
                drop();
            }
        }

        final ValueStream incoming = new ValueStream() {
            @Override
            public Value take() throws InterruptedException {
                try {
                    return takeValue(0);
                } catch (TimeoutException e) {
                    throw new AssertionError(e);
                }
            }

            @Override
            public Value take(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
                return takeValue(Math.max(1, unit.toNanos(timeout)));
            }

            @Override
            public long requestId() {
                return id;
            }

            @Override
            public void close() {
                giveUp(Code.CANCELED);
            }
        };

        final ValueSink outgoing = new ValueSink() {
            @Override
            public void send(Value value) throws InterruptedException {
                sendValue(value);
            }

            @Override
            public long requestId() {
                return id;
            }

            @Override
            public void close() {
                endOut();
            }

            @Override
            public void cancel() {
                giveUp(Code.CANCELED);
            }
        };
    }
}
