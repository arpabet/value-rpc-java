/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.MapValue;
import com.arpabet.value.NullValue;
import com.arpabet.value.NumberValue;
import com.arpabet.value.Value;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * The half of the engine that asks: the requests one end has open on its
 * peer, each a number and something that waits for the messages that carry
 * it. Both ends have one: a client for what it asks its server, and a server
 * one for each client, for what it asks of that client. It is part of the
 * engine, used by {@code VrpcClient} and {@code VrpcServer}; an application
 * has the {@link Peer} those give it and does not make one.
 *
 * <p>A request belongs to the connection it went out on: when that is lost,
 * the request fails at once, since its answer cannot come any more.
 */
public final class Initiator implements Peer {

    /** A connection, as far as asking needs it. */
    public interface Link {

        /** Queues a message, waiting while there is no room. False when the connection is gone. */
        boolean send(MapValue message);

        /** Queues a message if there is room at once: for what may be dropped. */
        boolean trySend(MapValue message);

        /** Whether it still carries messages. */
        boolean isOpen();
    }

    /** The end the requests are made from. */
    public interface Host {

        /**
         * The connection to ask on, made first where the end makes its own.
         *
         * @throws VrpcException there is none to be had
         */
        Link connected();

        /** Why nothing can be asked any more, once the end is closed; null until then. */
        VrpcException closed();

        /** The peer sent a message that answers nothing. It was dropped. */
        void protocolError(MapValue message, String problem);
    }

    /** What an initiator is set up with. */
    public static final class Setup {
        final boolean dialing;
        final Dialect dialect;
        final Metrics metrics;
        final ScheduledExecutorService timer;
        final int streamWindow;
        final LongSupplier timeoutMillis;
        final Supplier<Map<String, String>> metadata;
        final long establishMillis;

        /**
         * @param dialing         whether this end dialed the connection (a
         *                        client) or accepted it (a server). A client
         *                        numbers its requests 1, 2, 3 and so on, sends
         *                        arguments it has none of as null, names the
         *                        time it allows and sends metadata with every
         *                        request; a server numbers them -1, -2, -3,
         *                        leaves out arguments it has none of, names no
         *                        time and sends metadata with a call only: as
         *                        the two ends of the Go implementation
         * @param streamWindow    how many values of a stream from the peer may wait to be taken
         * @param timeoutMillis   how long a request may take when its options
         *                        do not say, asked for each one; null, or not
         *                        positive, for no limit
         * @param metadata        what travels with every request of a client, asked for each one; null for nothing
         * @param establishMillis how long a server waits for a client to take
         *                        a stream when the options do not say; not positive for no limit
         */
        public Setup(boolean dialing, Dialect dialect, Metrics metrics, ScheduledExecutorService timer, int streamWindow,
                LongSupplier timeoutMillis, Supplier<Map<String, String>> metadata, long establishMillis) {
            this.dialing = dialing;
            this.dialect = dialect;
            this.metrics = metrics;
            this.timer = timer;
            this.streamWindow = streamWindow;
            this.timeoutMillis = timeoutMillis;
            this.metadata = metadata;
            this.establishMillis = establishMillis;
        }
    }

    /** What a request is told that was on a connection when it was lost, on a client as the Go client words it. */
    private static final String LOST_DIALING = "connection lost during reconnect";
    private static final String LOST_ACCEPTING = "connection lost; reverse request cannot complete";

    private final Setup setup;
    private final Host host;
    private final Dialect dialect;
    private final Metrics metrics;

    /** The requests that are not over, by their id. */
    private final ConcurrentHashMap<Long, Pending> pending = new ConcurrentHashMap<Long, Pending>();
    private final AtomicLong lastRequest = new AtomicLong();

    public Initiator(Setup setup, Host host) {
        this.setup = setup;
        this.host = host;
        this.dialect = setup.dialect;
        this.metrics = setup.metrics;
    }

    /** How many requests were made. */
    public long requests() {
        return lastRequest.get();
    }

    /** How many requests are not over. */
    public int pending() {
        return pending.size();
    }

    /** The ids a client gives its requests are 1, 2, 3 and so on; a server's are negative. 0 is the handshake. */
    private long nextId() {
        final long n = lastRequest.incrementAndGet();
        return setup.dialing ? n : -n;
    }

    private VrpcException closedOr(VrpcException otherwise) {
        final VrpcException closed = host.closed();
        return closed != null ? closed : otherwise;
    }

    private VrpcException lost() {
        return closedOr(new VrpcException(Code.UNAVAILABLE, setup.dialing ? LOST_DIALING : LOST_ACCEPTING));
    }

    // --- requests ------------------------------------------------------------------

    /** A request that is not over: a call that waits for its answer, or a stream. */
    private abstract class Pending {
        final long id;
        final NumberValue idValue;
        final String name;
        final long startNanos = System.nanoTime();
        /** The connection the request went out on; null until it has one. */
        volatile Link link;
        volatile ScheduledFuture<?> timeout;
        /**
         * Why the request failed; null while it is not over, and when it
         * ended well. Written under the monitor of the request, in the step
         * that takes it out of the pending ones ({@code retireFailed}).
         */
        volatile VrpcException error;

        Pending(long id, String name) {
            this.id = id;
            this.idValue = Value.ofLong(id);
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

        /** What a request that ran out of time is told. */
        VrpcException late() {
            return new VrpcException(Code.DEADLINE_EXCEEDED, "timeout");
        }
    }

    /**
     * How long a request may take: what its options say, and failing that
     * the timeout of a client. A server names none of its own: a call of it
     * is bounded by its caller, and a stream by the time a client has to
     * take it.
     */
    private long timeoutOf(CallOptions options, boolean stream) {
        long t = options.timeoutMillis();
        if (t == 0) {
            if (setup.dialing) {
                t = setup.timeoutMillis == null ? 0 : setup.timeoutMillis.getAsLong();
            } else {
                t = stream ? setup.establishMillis : 0;
            }
        }
        return t < 0 ? 0 : t;
    }

    private Map<String, String> metadataOf(CallOptions options, boolean stream) {
        if (!setup.dialing) {
            // A server sends metadata with a call only.
            return stream || options.metadata().isEmpty() ? null : options.metadata();
        }
        Map<String, String> md = null;
        if (setup.metadata != null) {
            final Map<String, String> every = setup.metadata.get();
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
        final Link c = host.connected();
        p.link = c;
        final boolean stream = type != MessageType.FUNCTION_REQUEST;
        final long timeout = timeoutOf(options, stream);
        // A client always sends the arguments, null when there are none, and
        // the time it allows; a server leaves both out.
        final MapValue request = dialect.request(type, p.id, p.name,
                args == null && setup.dialing ? NullValue.NULL : args, setup.dialing ? timeout : 0,
                metadataOf(options, stream));
        pending.put(p.id, p);
        metrics.requestBegin(p.name);
        if (timeout > 0) {
            try {
                p.timeout = setup.timer.schedule(() -> timedOut(p), timeout, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                fail(p, closedOr(new VrpcException(Code.UNAVAILABLE, "closed")));
                return;
            }
        }
        if (!c.send(request)) {
            fail(p, closedOr(new VrpcException(Code.UNAVAILABLE, "connection lost")));
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
        retired(p, code);
        return true;
    }

    /**
     * Retires a request that failed, and leaves with it why. For whoever
     * looks at the request under its monitor the two are one step: a stream
     * that finds itself taken out of the pending ones can say why
     * ({@code StreamCall.gone}), also when the one that failed it has not yet
     * come to tell it ({@code Pending.failed}). Without that, an application
     * that sent on a stream between the two was told the stream is closed,
     * where its connection had been lost.
     */
    private boolean retireFailed(Pending p, VrpcException error) {
        synchronized (p) {
            if (!pending.remove(p.id, p)) {
                return false;
            }
            p.error = error;
        }
        retired(p, error.code());
        return true;
    }

    /** What follows taking a request out, of whichever kind. Not under its monitor: the metrics are the application's. */
    private void retired(Pending p, Code code) {
        final ScheduledFuture<?> t = p.timeout;
        if (t != null) {
            t.cancel(false);
        }
        metrics.requestEnd(p.name, code, System.nanoTime() - p.startNanos);
    }

    private void fail(Pending p, VrpcException error) {
        if (retireFailed(p, error)) {
            p.failed(error);
        }
    }

    /** The caller gave up: the peer is told, and the request is retired here, since a cancel is not answered. */
    private void cancel(Pending p) {
        if (pending.get(p.id) != p) {
            return; // over already: closing a stream that has ended comes here
        }
        final VrpcException canceled = new VrpcException(Code.CANCELED, "request " + p.id + " canceled");
        if (retireFailed(p, canceled)) {
            tellCancel(p);
            p.failed(canceled);
        }
    }

    private void timedOut(Pending p) {
        if (p.taken() || pending.get(p.id) != p) {
            return;
        }
        final VrpcException timeout = p.late();
        if (retireFailed(p, timeout)) {
            tellCancel(p);
            p.failed(timeout);
        }
    }

    private void tellCancel(Pending p) {
        final Link c = p.link;
        if (c != null) {
            c.trySend(dialect.cancelRequest(p.idValue));
        }
    }

    private static VrpcException thrown(ExecutionException e) {
        final Throwable cause = e.getCause();
        return cause instanceof VrpcException ? (VrpcException) cause
                : new VrpcException(Code.UNKNOWN, String.valueOf(cause), cause);
    }

    // --- what the end tells it -------------------------------------------------------

    /**
     * A message of the peer about a request this end opened. On the thread
     * that reads the connection: it does not wait.
     *
     * @param type null for a number that is no message type
     */
    public void onMessage(MessageType type, long id, MapValue message) {
        final Pending p = pending.get(id);
        if (p == null) {
            host.protocolError(message, "request not found");
            return;
        }
        if (type == null) {
            host.protocolError(message, "message type not supported");
            return;
        }
        p.onMessage(type, message);
    }

    /**
     * A connection was lost. What went out on it cannot be answered any more
     * and fails at once, with {@code UNAVAILABLE}, rather than each request
     * waiting out its time.
     *
     * @param why what the requests are told; null for the words of the end
     */
    public void failOn(Link lostLink, String why) {
        for (Pending p : pending.values()) {
            if (p.link == lostLink) {
                fail(p, why == null ? lost() : new VrpcException(Code.UNAVAILABLE, why));
            }
        }
    }

    /** The end closes: every request fails. */
    public void failAll(VrpcException error) {
        for (Pending p : pending.values()) {
            fail(p, error);
        }
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
                host.protocolError(message, "message type not supported");
            }
        }

        @Override
        void failed(VrpcException error) {
            future.completeExceptionally(error);
        }
    }

    /**
     * Calls a function of the peer and waits for its result. A thread that
     * is interrupted while it waits cancels the call, which the peer is
     * told, and stays interrupted.
     */
    @Override
    public Value callFunction(String name, Value args, CallOptions options) {
        final PendingCall p = new PendingCall(nextId(), name);
        open(p, MessageType.FUNCTION_REQUEST, args, options == null ? CallOptions.DEFAULT : options);
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

    @Override
    public CompletableFuture<Value> callFunctionAsync(String name, Value args, CallOptions options) {
        final PendingCall p = new PendingCall(nextId(), name);
        try {
            open(p, MessageType.FUNCTION_REQUEST, args, options == null ? CallOptions.DEFAULT : options);
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

    // --- streams -------------------------------------------------------------------

    /**
     * A stream this end opened: what the peer sends is buffered for the
     * application, which is granted to the peer as credit; what the
     * application sends waits at a gate for the credit the peer grants.
     */
    private final class StreamCall extends Pending {
        final MessageType kind;
        /** Completes when the peer has taken the request; fails when the stream fails first. */
        final CompletableFuture<Void> ready = new CompletableFuture<Void>();
        /** What the peer sends; null when it sends nothing. */
        final StreamBuffer in;
        /** The credit to send with; null when this side sends nothing. */
        final CreditGate out;
        /** This side has ended what it sends. */
        private volatile boolean outEnded;
        /** Values taken since credit was last granted. Only the thread that reads the stream touches it. */
        private long takenSinceGrant;

        StreamCall(long id, String name, MessageType kind) {
            super(id, name);
            this.kind = kind;
            this.in = kind == MessageType.PUT_STREAM_REQUEST ? null : new StreamBuffer(setup.streamWindow, this::taken1);
            this.out = kind == MessageType.GET_STREAM_REQUEST ? null : new CreditGate();
        }

        @Override
        boolean taken() {
            return ready.isDone();
        }

        @Override
        VrpcException late() {
            return setup.dialing ? super.late()
                    : new VrpcException(Code.DEADLINE_EXCEEDED, "reverse stream " + id + ": client did not ack");
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
                        host.protocolError(message, "message type not supported");
                    } else if (v != null) {
                        if (in.offer(v)) {
                            metrics.streamValue(name);
                        } else if (in.overflowed()) {
                            // The peer sent more than it had credit for: the
                            // stream is cut short, and says so.
                            final VrpcException truncated = new VrpcException(Code.RESOURCE_EXHAUSTED, setup.dialing
                                    ? "stream " + id + " truncated: server exceeded flow-control credit"
                                    : "reverse stream " + id + " truncated: peer exceeded flow-control credit");
                            if (retireFailed(this, truncated)) {
                                tellCancel(this);
                                failed(truncated);
                            }
                        }
                    }
                    break;
                }
                case STREAM_END: {
                    // The peer ending what it sends ends the stream, of
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
                    host.protocolError(message, "message type not supported");
            }
        }

        @Override
        void failed(VrpcException e) {
            // Why is with the stream already (retireFailed): what follows
            // lets go of whoever waits on it, and they ask.
            ready.completeExceptionally(e);
            if (in != null) {
                in.stop();
            }
            if (out != null) {
                out.close();
            }
        }

        /** The application took a value: the peer may send another, told in batches of half the window. */
        private void taken1() {
            takenSinceGrant++;
            if (takenSinceGrant >= Math.max(1, setup.streamWindow / 2)) {
                final long n = takenSinceGrant;
                takenSinceGrant = 0;
                grant(n);
            }
        }

        void grant(long n) {
            final Link c = link;
            if (c != null && pending.get(id) == this) {
                c.send(dialect.streamCredit(idValue, n));
            }
        }

        /** Why this stream takes no more: what failed it, or else that it is over. */
        private VrpcException gone() {
            final VrpcException e;
            // Under the monitor a failed stream is retired under: one that is
            // being failed as this asks has been given its reason by the time
            // this is let in.
            synchronized (this) {
                e = error;
            }
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
            // Wait for the peer to have room: only this thread waits.
            if (!out.acquire()) {
                throw gone();
            }
            if (pending.get(id) != this) {
                throw gone();
            }
            final Link c = link;
            if (c == null || !c.send(dialect.streamValue(idValue, value))) {
                if (c == null || !c.isOpen()) {
                    // There is no connection to send on: it is lost, or the
                    // end is closed, and this stream with it. What deals
                    // with that may not have come to this stream yet, so it
                    // is said here: the reason is the same whoever says it.
                    fail(this, lost());
                }
                throw gone();
            }
            metrics.streamValue(name);
        }

        /** This side has no more to send. For a stream to the peer that is its end; a chat goes on. */
        void endOut() {
            if (outEnded) {
                return;
            }
            outEnded = true;
            if (pending.get(id) != this) {
                return;
            }
            final Link c = link;
            if (c != null) {
                c.send(dialect.streamEnd(idValue, null));
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
                Initiator.this.cancel(StreamCall.this);
            }
        };
    }

    /** Opens a stream and waits until the peer has taken it. */
    private StreamCall openStream(MessageType kind, String name, Value args, CallOptions options) {
        final StreamCall s = new StreamCall(nextId(), name, kind);
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
            // The peer sends only what it has credit for: grant it the window.
            s.grant(setup.streamWindow);
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
}
