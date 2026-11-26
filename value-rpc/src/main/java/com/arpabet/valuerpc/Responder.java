/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.MapValue;
import com.arpabet.value.NumberValue;
import com.arpabet.value.Value;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The half of the engine that answers: the requests a peer opened on one
 * connection, each served by its handler on a thread of its own. Both ends
 * have one for each connection: a server for what its client asks, and a
 * client for what the server asks of it. It is part of the engine, used by
 * {@code VrpcServer} and {@code VrpcClient}; an application registers
 * handlers with those and does not make one.
 *
 * <p>A request is over for the peer when it is retired: taken out of the
 * requests of its connection, by whoever ends it, once. A call is retired
 * when its handler has returned. A stream is retired the moment it ends, by
 * either side or with the connection; its handler may still be on its way
 * out then, and learns of the end from its stream.
 *
 * <p>The requests of a connection end with it: when it is lost they are
 * cancelled and their handlers interrupted, since the peer has given them up
 * too.
 */
public final class Responder {

    /** The connection the requests come on and are answered on. */
    public interface Link {

        /** Queues a message, waiting while there is no room. False when the connection is gone. */
        boolean send(MapValue message);

        /** Queues a message if there is room at once: for the thread that reads, which must not wait. */
        boolean trySend(MapValue message);

        /** The address of the peer, for a log. */
        String remoteAddress();

        /** Who the peer is, as the authenticator said; empty for nobody. */
        String principal();

        /** The peer as something to ask: what a handler gets from {@link RequestContext#peer()}. */
        Peer peer();
    }

    /** Told what goes wrong while requests are served. Called on threads of the engine: it must not wait. */
    public interface Events {

        /** The peer sent a message that is not one, or that fits no request. It was dropped. */
        void protocolError(MapValue message, String problem);

        /** A handler failed with something that is not an answer: its caller was told {@code INTERNAL}. */
        void handlerFailure(String where, Throwable error);
    }

    /** What every responder of an end is set up with. */
    public static final class Setup {
        final boolean accepting;
        final Dialect dialect;
        final HandlerRegistry registry;
        final Metrics metrics;
        final ScheduledExecutorService timer;
        final Executor executor;
        final long maxRequests;
        final long maxStreams;
        final int streamWindow;
        final AtomicLong taken;

        /**
         * @param accepting    whether this end accepted the connection (a
         *                     server) or dialed it (a client): the two answer
         *                     in different words, check in a different order
         *                     and count different requests against their
         *                     limit, as the two ends of the Go implementation
         * @param maxRequests  how many handlers may run at once for one
         *                     connection, 0 for any number: of every pattern
         *                     on a server, of calls on a client
         * @param maxStreams   how many streams one connection may have open at once, 0 for any number
         * @param streamWindow how many values of a stream from the peer may wait to be taken
         * @param taken        counts every request taken in; null to count none
         */
        public Setup(boolean accepting, Dialect dialect, HandlerRegistry registry, Metrics metrics,
                ScheduledExecutorService timer, Executor executor, long maxRequests, long maxStreams, int streamWindow,
                AtomicLong taken) {
            this.accepting = accepting;
            this.dialect = dialect;
            this.registry = registry;
            this.metrics = metrics;
            this.timer = timer;
            this.executor = executor;
            this.maxRequests = maxRequests;
            this.maxStreams = maxStreams;
            this.streamWindow = streamWindow;
            this.taken = taken;
        }
    }

    private final Setup setup;
    private final Link link;
    private final Events events;
    private final Dialect dialect;
    private final Metrics metrics;

    /** The requests that are not over, by their id. */
    private final ConcurrentHashMap<Long, Serving> requests = new ConcurrentHashMap<Long, Serving>();
    /** The handlers that run and have not taken their stream yet: what the limit of requests counts. */
    private final AtomicLong inFlight = new AtomicLong();
    private final AtomicLong liveStreams = new AtomicLong();

    private volatile boolean gone;
    /** What its requests are told when the connection is gone. */
    private volatile String why = "connection lost";

    public Responder(Setup setup, Link link, Events events) {
        this.setup = setup;
        this.link = link;
        this.events = events;
        this.dialect = setup.dialect;
        this.metrics = setup.metrics;
    }

    /** How many requests are not over. */
    public int serving() {
        return requests.size();
    }

    /** Hands a task to a thread of the handlers. False when there is none to be had. */
    private boolean run(Runnable task) {
        try {
            setup.executor.execute(task);
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
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

    /**
     * A message of the peer about a request it opened: the one that opens
     * it, or one for it while it is served. The end has looked at the id
     * before: it is one the peer may give. On the thread that reads the
     * connection: it does not wait.
     *
     * @param type null for a number that is no message type
     */
    public void onMessage(MessageType type, NumberValue idValue, MapValue message) {
        final Serving serving = requests.get(idValue.longValue());
        if (serving != null) {
            serving.onMessage(type, message);
            return;
        }
        if (type == null) {
            events.protocolError(message, "unknown message type");
            return;
        }
        if (!type.opensRequest()) {
            // For a request that is over: a cancel that crossed its answer, a
            // value or a credit that was on its way when the stream ended.
            return;
        }
        // A handler slot is taken, or the request refused: it is never
        // queued, and the connection is read on either way. A server counts
        // every request; a client the calls, as the Go one does.
        final boolean counts = setup.accepting || type == MessageType.FUNCTION_REQUEST;
        if (counts && inFlight.incrementAndGet() > setup.maxRequests && setup.maxRequests > 0) {
            inFlight.decrementAndGet();
            link.trySend(dialect.error(idValue, Code.RESOURCE_EXHAUSTED, setup.accepting
                    ? "server busy: too many concurrent requests (max " + setup.maxRequests + ")"
                    : "client busy: too many concurrent reverse calls (max " + setup.maxRequests + ")"));
            return;
        }
        if (setup.taken != null) {
            setup.taken.incrementAndGet();
        }
        final Serving s = new Serving(idValue, type, message, counts);
        // Known before its handler runs, so that a cancel always finds it.
        requests.put(s.id, s);
        if (gone) {
            s.lost(why);
        }
        if (!run(s::serve)) {
            s.refuse(Code.UNAVAILABLE, gone ? why : (setup.accepting ? "server" : "client") + " busy: no thread for the request");
        }
    }

    /**
     * The connection is gone, or the end closes. Nobody waits for its
     * requests any more: their handlers are told, and the streams end.
     */
    public void lost(String reason) {
        why = reason;
        gone = true;
        for (Serving s : requests.values()) {
            s.lost(reason);
        }
    }

    /** A request that is not over, and what its handler is told about it. */
    private final class Serving implements RequestContext {
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
        private final AtomicBoolean slot;
        /** Counted among the open streams of the connection. Under the monitor. */
        private boolean counted;

        private volatile Map<String, String> metadata;
        /** When the time the caller allowed is up, in nanoseconds of the clock; 0 for none. */
        private volatile long deadlineNanos;
        private volatile ScheduledFuture<?> deadline;

        private volatile boolean cancelled;
        /** The handler was told of it: interrupted, and what it asked to have run was run. Under the monitor. */
        private boolean cancelTold;
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

        Serving(NumberValue idValue, MessageType kind, MapValue request, boolean holdsSlot) {
            this.idValue = idValue;
            this.id = idValue.longValue();
            this.kind = kind;
            this.request = request;
            final String n = dialect.functionName(request);
            this.name = n == null ? "" : n;
            this.where = Responder.where(kind, name);
            this.slot = new AtomicBoolean(holdsSlot);
            this.in = kind == MessageType.PUT_STREAM_REQUEST || kind == MessageType.CHAT_REQUEST
                    ? new StreamBuffer(setup.streamWindow, this::taken1) : null;
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
            return link.principal();
        }

        @Override
        public String remoteAddress() {
            return link.remoteAddress();
        }

        @Override
        public Peer peer() {
            return link.peer();
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
                inFlight.decrementAndGet();
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
         * not yet come to let go of what waits on the stream. A request that
         * is gone for a reason is one nobody waits for: it is cancelled in
         * the same step, so that a handler which finds its stream over finds
         * its request cancelled too. Whoever retires it so tells the handler
         * next ({@link #cancel}).
         */
        private boolean retire(Code code, VrpcException reason) {
            final boolean wasCounted;
            synchronized (this) {
                if (!retired.compareAndSet(false, true)) {
                    return false;
                }
                error = reason;
                if (reason != null) {
                    quiet = true;
                    cancelled = true;
                }
                wasCounted = counted;
                counted = false;
            }
            requests.remove(id, this);
            releaseSlot();
            if (wasCounted) {
                liveStreams.decrementAndGet();
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
            final ArrayList<Runnable> toRun;
            synchronized (this) {
                if (silently) {
                    quiet = true;
                }
                if (cancelTold) {
                    return;
                }
                cancelTold = true;
                if (!cancelled) {
                    deadlinePassed = byDeadline;
                    cancelled = true;
                }
                if (thread != null) {
                    thread.interrupt();
                }
                toRun = callbacks;
                callbacks = null;
            }
            if (toRun != null) {
                for (Runnable r : toRun) {
                    run(r);
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

        /** The connection is gone, or the end closes. */
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
                link.trySend(dialect.error(idValue, code, text));
            }
        }

        private void fail(Code code, String text) {
            if (retire(code)) {
                link.send(dialect.error(idValue, code, text));
            }
        }

        /** A message of the peer for this request. On the thread that reads the connection: it must not wait. */
        void onMessage(MessageType type, MapValue message) {
            if (type == null) {
                events.protocolError(message, "unknown message type");
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
                        events.protocolError(message, "a value for a request that takes none");
                        break;
                    }
                    final Value v = dialect.streamValueOf(message);
                    if (v == null) {
                        break;
                    }
                    if (in.offer(v)) {
                        metrics.streamValue(name);
                    } else if (in.overflowed()) {
                        // The peer sent more than it had credit for: the
                        // stream is cut short, and both sides are told.
                        final VrpcException truncated = new VrpcException(Code.RESOURCE_EXHAUSTED,
                                "inbound stream " + id + " truncated: " + (setup.accepting ? "client" : "peer")
                                        + " exceeded flow-control credit");
                        if (!retire(Code.RESOURCE_EXHAUSTED, truncated)) {
                            break;
                        }
                        link.trySend(dialect.error(idValue, Code.RESOURCE_EXHAUSTED, truncated.detail()));
                        cancel(true, false);
                        drop();
                    }
                    break;
                }
                case STREAM_END: {
                    if (in == null) {
                        events.protocolError(message, "an end for a request that takes no values");
                        break;
                    }
                    // An end may carry a last value.
                    final Value last = dialect.streamValueOf(message);
                    if (last != null && in.offer(last)) {
                        metrics.streamValue(name);
                    }
                    in.end();
                    if (kind == MessageType.PUT_STREAM_REQUEST) {
                        // The stream is over for the peer. The handler takes
                        // what has arrived at its own pace, and is not
                        // disturbed.
                        retire(Code.OK);
                    }
                    // A chat goes on: the peer ending what it sends ends nothing else.
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
                    events.protocolError(message, "unknown message type");
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
                events.handlerFailure(where, e);
            }
        }

        /** The handler has returned: nothing interrupts this thread on its behalf from here on. */
        private void left() {
            synchronized (this) {
                thread = null;
            }
            Thread.interrupted();
        }

        private void refused(Code code, String text) {
            left();
            fail(code, text);
        }

        /**
         * Checks the request against what is registered and runs its
         * handler. A server looks at the arguments and then at the pattern,
         * a client the other way round, each in its own words: as the two
         * ends of the Go implementation do.
         */
        private void admitted() {
            if (dialect.functionName(request) == null) {
                refused(Code.INVALID_ARGUMENT, "function name field not found");
                return;
            }
            final HandlerRegistry.Entry fn = setup.registry.get(name);
            if (fn == null) {
                refused(Code.NOT_FOUND, "function not found " + name);
                return;
            }
            final Value args = dialect.arguments(request);
            if (setup.accepting) {
                if (!fn.args.verify(args)) {
                    refused(Code.INVALID_ARGUMENT, "function '" + name + "' invalid args");
                    return;
                }
                if (fn.kind != kind) {
                    refused(Code.INVALID_ARGUMENT, "function wrong type " + name + ", expected " + pattern(fn.kind)
                            + ", actual " + pattern(kind));
                    return;
                }
            } else {
                if (fn.kind != kind) {
                    refused(Code.INVALID_ARGUMENT, kind == MessageType.FUNCTION_REQUEST
                            ? "function '" + name + "' is not unary" : "function '" + name + "' wrong stream type");
                    return;
                }
                if (!fn.args.verify(args)) {
                    refused(Code.INVALID_ARGUMENT, kind == MessageType.FUNCTION_REQUEST
                            ? "function '" + name + "' invalid args" : "stream '" + name + "' invalid args");
                    return;
                }
            }
            if (kind == MessageType.FUNCTION_REQUEST) {
                call(fn, args);
            } else {
                stream(fn, args);
            }
        }

        private void call(HandlerRegistry.Entry fn, Value args) {
            // The time the caller allows is the deadline of the handler: of a
            // call only, and on a server only, since a server names no time.
            final long sla = setup.accepting ? dialect.slaMillis(request) : 0;
            if (sla > 0) {
                deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(sla);
                try {
                    deadline = setup.timer.schedule(() -> cancel(false, true), sla, TimeUnit.MILLISECONDS);
                } catch (RejectedExecutionException e) {
                    // the end is closing, which cancels the call itself
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
                answer = dialect.error(idValue, Code.INTERNAL,
                        "function '" + name + (setup.accepting ? "' invalid results" : "' invalid result"));
                code = Code.INTERNAL;
            } else {
                answer = dialect.functionResult(idValue, result);
                code = Code.OK;
            }
            if (retire(code) && !quiet) {
                link.send(answer);
            }
        }

        private void stream(HandlerRegistry.Entry fn, Value args) {
            // A stream counts among the open ones from here to its end.
            final boolean room;
            synchronized (this) {
                if (retired.get()) {
                    room = true; // over already: it is not counted, and what follows finds it so
                } else if (liveStreams.incrementAndGet() > setup.maxStreams && setup.maxStreams > 0) {
                    liveStreams.decrementAndGet();
                    room = false;
                } else {
                    counted = true;
                    room = true;
                }
            }
            if (!room) {
                refused(Code.RESOURCE_EXHAUSTED,
                        "server busy: too many concurrent streams (max " + setup.maxStreams + ")");
                return;
            }
            if (in != null && !retired.get()) {
                // The peer may send this many values. The credit comes before the stream is said to be taken.
                link.send(dialect.streamCredit(idValue, setup.streamWindow));
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
                // Before the stream was taken this is what the peer gets in
                // place of it; after, it ends the stream.
                final MapValue answer = failure(thrown);
                if (retire(Code.of(dialect.errorOf(answer).codeNumber()))) {
                    if (!quiet) {
                        link.send(answer);
                    }
                    drop();
                }
                return;
            }
            ready();
            if (kind == MessageType.PUT_STREAM_REQUEST) {
                // The handler is done. What the peer has not sent yet is not wanted, and it is told to stop.
                giveUp(Code.OK);
            } else {
                endOut();
            }
        }

        /**
         * The answer to a handler that threw. One that was interrupted
         * because its request was cancelled says so as the Go one does,
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
                events.handlerFailure(where, thrown);
            }
            return dialect.error(idValue, number, where + ": " + detail);
        }

        // --- streams ---

        /**
         * Tells the peer that the stream is taken, once: when the handler
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
                    link.send(dialect.streamReady(idValue));
                }
            }
        }

        /** Why this stream takes no more: what ended it, or else that it is over. */
        private VrpcException streamGone() {
            final VrpcException e;
            synchronized (this) {
                e = error;
            }
            return e != null ? e : new VrpcException(Code.CANCELED, "stream " + id + " is closed");
        }

        /** The handler took a value: the peer may send another, told in batches of half the window. */
        private void taken1() {
            takenSinceGrant++;
            if (takenSinceGrant >= Math.max(1, setup.streamWindow / 2)) {
                final long n = takenSinceGrant;
                takenSinceGrant = 0;
                if (!retired.get()) {
                    link.send(dialect.streamCredit(idValue, n));
                }
            }
        }

        /**
         * What a handler that was interrupted while it waited on its stream
         * is told: why the stream is gone, when it is, since that is why it
         * was interrupted. Any other interrupt is the handler's own.
         */
        private InterruptedException woken(InterruptedException e) {
            final VrpcException reason;
            synchronized (this) {
                reason = error;
            }
            if (reason == null) {
                return e;
            }
            Thread.currentThread().interrupt();
            throw reason;
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
                throw streamGone();
            }
            // Wait for the peer to have room: only this thread waits.
            final boolean room;
            try {
                room = out.acquire();
            } catch (InterruptedException e) {
                throw woken(e);
            }
            if (!room) {
                throw streamGone();
            }
            if (retired.get()) {
                throw streamGone();
            }
            if (!link.send(dialect.streamValue(idValue, value))) {
                throw streamGone();
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
                link.send(dialect.streamEnd(idValue, null));
                drop();
            }
        }

        /** The handler gives the stream up before its end, which the peer is told. */
        private void giveUp(Code code) {
            if (retire(code)) {
                link.send(dialect.cancelRequest(idValue));
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
