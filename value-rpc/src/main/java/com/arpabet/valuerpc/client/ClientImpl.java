/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.client;

import com.arpabet.value.MapValue;
import com.arpabet.value.NullValue;
import com.arpabet.value.NumberValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.Chat;
import com.arpabet.valuerpc.ClientInterceptor;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.CreditGate;
import com.arpabet.valuerpc.Dialect;
import com.arpabet.valuerpc.Frames;
import com.arpabet.valuerpc.HashChain;
import com.arpabet.valuerpc.MessageType;
import com.arpabet.valuerpc.Metrics;
import com.arpabet.valuerpc.StreamBuffer;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * The client. One connection carries every request: a thread of the transport
 * reads it and hands each message to {@link #dispatch}, which never waits,
 * and a thread of the client writes what the callers queue, several messages
 * at once when several wait. A request is a number and something that waits;
 * a message that carries the number is handed to what waits.
 */
final class ClientImpl implements VrpcClient {

    private final Dialer dialer;
    private final Dialect dialect;
    private final long dialTimeoutMillis;
    private final int sendQueue;
    private final int streamWindow;
    private final Supplier<Map<String, String>> metadata;
    private final Metrics metrics;
    private final ClientListener listener;
    private final ClientInterceptor.Invoker invoker;
    private final boolean intercepted;

    /** Chosen at random, as the Go client chooses it: 63 bits. */
    private final long clientId = ThreadLocalRandom.current().nextLong() & Long.MAX_VALUE;

    private volatile long timeoutMillis;
    private volatile Value credential;

    /** The requests that are not over, by their id. */
    private final ConcurrentHashMap<Long, Pending> pending = new ConcurrentHashMap<Long, Pending>();
    /** The ids a client gives its requests are 1, 2, 3 and so on; a server's are negative. */
    private final AtomicLong lastRequest = new AtomicLong();
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
        this.streamWindow = b.streamWindow;
        this.metadata = b.metadata;
        this.metrics = b.metrics;
        this.listener = b.listener;
        this.timeoutMillis = b.timeoutMillis;
        this.credential = b.credential;
        this.intercepted = !b.interceptors.isEmpty();
        this.invoker = ClientInterceptor.chain(this::invokeUnary, new ArrayList<ClientInterceptor>(b.interceptors));
        this.timer = new ScheduledThreadPoolExecutor(1, r -> daemon("vrpc-client-timer", r));
        this.timer.setRemoveOnCancelPolicy(true);
    }

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
        out.put("requests", lastRequest.get());
        out.put("reconnects", reconnects.get());
        out.put("pending", (long) pending.size());
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
     * A connection was lost. What waited on it cannot be answered any more
     * and fails at once, rather than each call waiting out its timeout; then
     * the client connects again, so that the server can reach it.
     */
    private void connectionLost(Connection c, Throwable cause) {
        if (closed || !connection.compareAndSet(c, null)) {
            return;
        }
        c.close();
        reconnects.incrementAndGet();
        metrics.reconnect();
        // Requests made from here on belong to the next connection.
        failAll(lastRequest.get(), "connection lost during reconnect");
        listener.onDisconnected(cause);
        try {
            connect();
        } catch (VrpcException e) {
            // The next call connects.
        }
    }

    private void failAll(long upTo, String why) {
        for (Pending p : pending.values()) {
            if (p.id <= upTo) {
                fail(p, new VrpcException(Code.UNAVAILABLE, why));
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        final Connection c = connection.getAndSet(null);
        if (c != null) {
            c.close();
        }
        failAll(Long.MAX_VALUE, "client closed");
        timer.shutdownNow();
        final ExecutorService e = executor;
        if (e != null) {
            e.shutdown();
        }
    }

    // --- requests ------------------------------------------------------------------

    /** A request that is not over: a call that waits for its answer, or a stream. */
    private abstract class Pending {
        final long id;
        final String name;
        final long startNanos = System.nanoTime();
        volatile ScheduledFuture<?> timeout;

        Pending(long id, String name) {
            this.id = id;
            this.name = name;
        }

        /** A message of the peer for this request. On the thread that reads the connection: it must not wait. */
        abstract void onMessage(MessageType type, MapValue message);

        /** The request failed; it has been retired already. */
        abstract void failed(VrpcException error);

        /** Whether the peer has taken the request, after which its timeout no longer applies. */
        boolean taken() {
            return false;
        }
    }

    private long timeoutOf(CallOptions options) {
        final long t = options.timeoutMillis() != 0 ? options.timeoutMillis() : timeoutMillis;
        return t < 0 ? 0 : t;
    }

    private Map<String, String> metadataOf(CallOptions options) {
        Map<String, String> md = null;
        if (metadata != null) {
            final Map<String, String> every = metadata.get();
            if (every != null && !every.isEmpty()) {
                md = new LinkedHashMap<String, String>(every);
            }
        }
        if (!options.metadata().isEmpty()) {
            if (md == null) {
                return options.metadata();
            }
            md.putAll(options.metadata());
        }
        return md;
    }

    /** Registers a request and sends what opens it. */
    private void open(Pending p, MessageType type, Value args, CallOptions options) {
        final Connection c = connected();
        final long timeout = timeoutOf(options);
        // A client always sends the arguments, null when there are none.
        final MapValue request = dialect.request(type, p.id, p.name, args == null ? NullValue.NULL : args, timeout,
                metadataOf(options));
        pending.put(p.id, p);
        metrics.requestBegin(p.name);
        if (timeout > 0) {
            try {
                p.timeout = timer.schedule(() -> timedOut(p), timeout, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                fail(p, clientClosed());
                return;
            }
        }
        if (!c.send(request)) {
            fail(p, closed ? clientClosed() : new VrpcException(Code.UNAVAILABLE, "connection lost"));
        }
    }

    /**
     * Takes a request out of the ones that are not over. Whoever takes it out
     * is the one to say how it ended: every way a request can end comes here,
     * and only the first gets true.
     */
    private boolean retire(Pending p, Code code) {
        if (!pending.remove(p.id, p)) {
            return false;
        }
        final ScheduledFuture<?> t = p.timeout;
        if (t != null) {
            t.cancel(false);
        }
        metrics.requestEnd(p.name, code, System.nanoTime() - p.startNanos);
        return true;
    }

    private void fail(Pending p, VrpcException error) {
        if (retire(p, error.code())) {
            p.failed(error);
        }
    }

    /** The caller gave up: the peer is told, and the request is retired here, since a cancel is not answered. */
    private void cancel(Pending p) {
        if (retire(p, Code.CANCELED)) {
            tellCancel(p.id);
            p.failed(new VrpcException(Code.CANCELED, "request " + p.id + " canceled"));
        }
    }

    private void timedOut(Pending p) {
        if (!p.taken() && retire(p, Code.DEADLINE_EXCEEDED)) {
            tellCancel(p.id);
            p.failed(new VrpcException(Code.DEADLINE_EXCEEDED, "timeout"));
        }
    }

    private void tellCancel(long id) {
        final Connection c = connection.get();
        if (c != null) {
            c.trySend(dialect.cancelRequest(Value.ofLong(id)));
        }
    }

    private static VrpcException thrown(ExecutionException e) {
        final Throwable cause = e.getCause();
        return cause instanceof VrpcException ? (VrpcException) cause
                : new VrpcException(Code.UNKNOWN, String.valueOf(cause), cause);
    }

    // --- calls ---------------------------------------------------------------------

    private final class PendingCall extends Pending {
        final CompletableFuture<Value> future = new CompletableFuture<Value>();

        PendingCall(long id, String name) {
            super(id, name);
        }

        @Override
        void onMessage(MessageType type, MapValue message) {
            if (type == MessageType.FUNCTION_RESPONSE) {
                if (retire(this, Code.OK)) {
                    future.complete(dialect.result(message));
                }
            } else if (type == MessageType.ERROR_RESPONSE) {
                fail(this, dialect.errorOf(message));
            } else if (type == MessageType.CANCEL_REQUEST) {
                fail(this, new VrpcException(Code.CANCELED, "request " + id + " canceled by peer"));
            } else {
                // A message of a stream for a call: not delivered.
                listener.onProtocolError(message, "message type not supported");
            }
        }

        @Override
        void failed(VrpcException error) {
            future.completeExceptionally(error);
        }
    }

    @Override
    public Value callFunction(String name, Value args, CallOptions options) {
        return invoker.invoke(name, args, options == null ? CallOptions.DEFAULT : options);
    }

    @Override
    public CompletableFuture<Value> callFunctionAsync(String name, Value args, CallOptions options) {
        final CallOptions o = options == null ? CallOptions.DEFAULT : options;
        if (intercepted) {
            // Interceptors are written to wait for the call: they run on a thread of the client.
            return CompletableFuture.supplyAsync(() -> invoker.invoke(name, args, o), executor());
        }
        final PendingCall p = new PendingCall(lastRequest.incrementAndGet(), name);
        try {
            open(p, MessageType.FUNCTION_REQUEST, args, o);
        } catch (VrpcException e) {
            p.future.completeExceptionally(e);
            return p.future;
        }
        p.future.whenComplete((value, error) -> {
            if (p.future.isCancelled()) {
                cancel(p);
            }
        });
        return p.future;
    }

    /** The call itself: the innermost step of the interceptors. */
    private Value invokeUnary(String name, Value args, CallOptions options) {
        final PendingCall p = new PendingCall(lastRequest.incrementAndGet(), name);
        open(p, MessageType.FUNCTION_REQUEST, args, options);
        try {
            return p.future.get();
        } catch (InterruptedException e) {
            cancel(p);
            Thread.currentThread().interrupt();
            throw new VrpcException(Code.CANCELED, "the call was interrupted", e);
        } catch (ExecutionException e) {
            throw thrown(e);
        }
    }

    // --- streams -------------------------------------------------------------------

    /**
     * A stream this client opened: what the server sends is buffered for the
     * application, which is granted to the server as credit; what the
     * application sends waits at a gate for the credit the server grants.
     */
    private final class StreamCall extends Pending {
        final MessageType kind;
        /** Completes when the server has taken the request; fails when the stream fails first. */
        final CompletableFuture<Void> ready = new CompletableFuture<Void>();
        /** What the server sends; null when it sends nothing. */
        final StreamBuffer in;
        /** The credit to send with; null when this side sends nothing. */
        final CreditGate out;
        private volatile VrpcException error;
        /** This side has ended what it sends. */
        private volatile boolean outEnded;
        /** Values taken since credit was last granted. Only the thread that reads the stream touches it. */
        private long takenSinceGrant;

        StreamCall(long id, String name, MessageType kind) {
            super(id, name);
            this.kind = kind;
            this.in = kind == MessageType.PUT_STREAM_REQUEST ? null : new StreamBuffer(streamWindow, this::taken1);
            this.out = kind == MessageType.GET_STREAM_REQUEST ? null : new CreditGate();
        }

        @Override
        boolean taken() {
            return ready.isDone();
        }

        @Override
        void onMessage(MessageType type, MapValue message) {
            switch (type) {
                case STREAM_READY:
                    ready.complete(null);
                    break;
                case STREAM_VALUE: {
                    final Value v = dialect.streamValueOf(message);
                    if (in == null) {
                        listener.onProtocolError(message, "message type not supported");
                    } else if (v != null) {
                        if (in.offer(v)) {
                            metrics.streamValue(name);
                        } else if (in.overflowed()) {
                            // The server sent more than it had credit for:
                            // the stream is cut short, and says so.
                            if (retire(this, Code.RESOURCE_EXHAUSTED)) {
                                tellCancel(id);
                                failed(new VrpcException(Code.RESOURCE_EXHAUSTED,
                                        "stream " + id + " truncated: server exceeded flow-control credit"));
                            }
                        }
                    }
                    break;
                }
                case STREAM_END: {
                    // The server ending what it sends ends the stream, of
                    // whatever kind; an end may carry a last value.
                    if (in != null) {
                        final Value last = dialect.streamValueOf(message);
                        if (last != null && in.offer(last)) {
                            metrics.streamValue(name);
                        }
                    }
                    if (retire(this, Code.OK)) {
                        ready.complete(null);
                        if (in != null) {
                            in.end();
                        }
                        if (out != null) {
                            out.close();
                        }
                    }
                    break;
                }
                case STREAM_CREDIT: {
                    final NumberValue credit = dialect.credit(message);
                    if (out != null && credit != null) {
                        out.grant(credit.longValue());
                    }
                    break;
                }
                case ERROR_RESPONSE:
                    fail(this, dialect.errorOf(message));
                    break;
                case CANCEL_REQUEST:
                    fail(this, new VrpcException(Code.CANCELED, "request " + id + " canceled by peer"));
                    break;
                default:
                    listener.onProtocolError(message, "message type not supported");
            }
        }

        @Override
        void failed(VrpcException e) {
            error = e;
            ready.completeExceptionally(e);
            if (in != null) {
                in.stop();
            }
            if (out != null) {
                out.close();
            }
        }

        /** The application took a value: the server may send another, told in batches of half the window. */
        private void taken1() {
            takenSinceGrant++;
            if (takenSinceGrant >= Math.max(1, streamWindow / 2)) {
                final long n = takenSinceGrant;
                takenSinceGrant = 0;
                grant(n);
            }
        }

        void grant(long n) {
            final Connection c = connection.get();
            if (c != null && pending.get(id) == this) {
                c.send(dialect.streamCredit(Value.ofLong(id), n));
            }
        }

        private VrpcException gone() {
            final VrpcException e = error;
            return e != null ? e : new VrpcException(Code.CANCELED, "stream " + id + " is closed");
        }

        Value take(long nanos) throws InterruptedException, TimeoutException {
            final Value v = in.take(nanos);
            if (v == null) {
                final VrpcException e = error;
                if (e != null) {
                    throw e;
                }
            }
            return v;
        }

        void send(Value value) throws InterruptedException {
            if (value == null || value.isNull()) {
                throw new IllegalArgumentException("a stream does not carry null");
            }
            if (outEnded) {
                throw gone();
            }
            // Wait for the server to have room: only this thread waits.
            if (!out.acquire()) {
                throw gone();
            }
            final Connection c = connection.get();
            if (c == null || pending.get(id) != this || !c.send(dialect.streamValue(Value.ofLong(id), value))) {
                throw gone();
            }
            metrics.streamValue(name);
        }

        /** This side has no more to send. For a stream to the server that is its end; a chat goes on. */
        void endOut() {
            if (outEnded) {
                return;
            }
            outEnded = true;
            if (pending.get(id) != this) {
                return;
            }
            final Connection c = connection.get();
            if (c != null) {
                c.send(dialect.streamEnd(Value.ofLong(id), null));
            }
            if (kind == MessageType.PUT_STREAM_REQUEST && retire(this, Code.OK)) {
                out.close();
            }
        }

        final ValueStream incoming = new ValueStream() {
            @Override
            public Value take() throws InterruptedException {
                try {
                    return StreamCall.this.take(0);
                } catch (TimeoutException e) {
                    throw new AssertionError(e);
                }
            }

            @Override
            public Value take(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
                return StreamCall.this.take(Math.max(1, unit.toNanos(timeout)));
            }

            @Override
            public long requestId() {
                return id;
            }

            @Override
            public void close() {
                cancel(StreamCall.this);
            }
        };

        final ValueSink outgoing = new ValueSink() {
            @Override
            public void send(Value value) throws InterruptedException {
                StreamCall.this.send(value);
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
                ClientImpl.this.cancel(StreamCall.this);
            }
        };
    }

    /** Opens a stream and waits until the server has taken it. */
    private StreamCall openStream(MessageType kind, String name, Value args, CallOptions options) {
        final StreamCall s = new StreamCall(lastRequest.incrementAndGet(), name, kind);
        open(s, kind, args, options == null ? CallOptions.DEFAULT : options);
        try {
            s.ready.get();
        } catch (InterruptedException e) {
            cancel(s);
            Thread.currentThread().interrupt();
            throw new VrpcException(Code.CANCELED, "interrupted while opening a stream", e);
        } catch (ExecutionException e) {
            throw thrown(e);
        }
        // The timeout bounded the opening; the stream itself lasts until it ends.
        final ScheduledFuture<?> t = s.timeout;
        if (t != null) {
            t.cancel(false);
        }
        if (s.in != null) {
            // The server sends only what it has credit for: grant it the window.
            s.grant(streamWindow);
        }
        return s;
    }

    @Override
    public ValueStream getStream(String name, Value args, CallOptions options) {
        return openStream(MessageType.GET_STREAM_REQUEST, name, args, options).incoming;
    }

    @Override
    public ValueSink putStream(String name, Value args, CallOptions options) {
        return openStream(MessageType.PUT_STREAM_REQUEST, name, args, options).outgoing;
    }

    @Override
    public Chat chat(String name, Value args, CallOptions options) {
        final StreamCall s = openStream(MessageType.CHAT_REQUEST, name, args, options);
        return new Chat() {
            @Override
            public ValueStream incoming() {
                return s.incoming;
            }

            @Override
            public ValueSink outgoing() {
                return s.outgoing;
            }

            @Override
            public long requestId() {
                return s.id;
            }

            @Override
            public void close() {
                cancel(s);
            }
        };
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
        if (type != null && type.opensRequest()) {
            serve(c, message);
            return;
        }
        final NumberValue idValue = dialect.requestId(message);
        if (idValue == null) {
            listener.onProtocolError(message, "request id not found");
            return;
        }
        final long id = idValue.longValue();
        if (type == MessageType.ERROR_RESPONSE && id == dialect.handshakeRequestId() && !c.handshake.isDone()) {
            // The server refuses the handshake, and says why before it closes.
            final VrpcException refused = dialect.errorOf(message);
            c.handshake.completeExceptionally(
                    VrpcException.ofNumber(refused.codeNumber(), "handshake: " + refused.detail()));
            return;
        }
        final Pending p = pending.get(id);
        if (p == null) {
            listener.onProtocolError(message, "request not found");
            return;
        }
        if (type == null) {
            listener.onProtocolError(message, "message type not supported");
            return;
        }
        p.onMessage(type, message);
    }

    /**
     * A request the server opened on this client. Nothing is registered to
     * serve one yet, so it is answered as a function that is not there.
     */
    private void serve(Connection c, MapValue request) {
        final NumberValue id = dialect.requestId(request);
        if (id == null) {
            listener.onProtocolError(request, "request id not found");
            return;
        }
        if (id.longValue() >= 0) {
            // A request of the server has a negative id; any other would be taken for an answer.
            listener.onProtocolError(request, "server-initiated request must use a negative request id");
            return;
        }
        final String name = dialect.functionName(request);
        if (name == null) {
            c.trySend(dialect.error(id, Code.INVALID_ARGUMENT, "function name field not found"));
            return;
        }
        c.trySend(dialect.error(id, Code.NOT_FOUND, "function not found " + name));
    }

    // --- the connection ------------------------------------------------------------

    private final class Connection implements MsgConn.Receiver {
        final MsgConn conn;
        final ArrayBlockingQueue<MapValue> outbox = new ArrayBlockingQueue<MapValue>(sendQueue);
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

        boolean isOpen() {
            return !closedHere && !gone;
        }

        /** Queues a message, waiting while the queue is full. False when the connection is gone. */
        boolean send(MapValue message) {
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
        boolean trySend(MapValue message) {
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
