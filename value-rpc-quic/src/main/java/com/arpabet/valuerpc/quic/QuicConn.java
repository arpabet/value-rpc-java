/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.quic;

import com.arpabet.value.Limits;
import com.arpabet.value.MapValue;
import com.arpabet.value.NumberValue;
import com.arpabet.value.ValueException;
import com.arpabet.valuerpc.Dialect;
import com.arpabet.valuerpc.FrameException;
import com.arpabet.valuerpc.Frames;
import com.arpabet.valuerpc.MessageType;
import com.arpabet.valuerpc.transport.MsgConn;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoop;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.quic.DefaultQuicStreamFrame;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicConnectionCloseEvent;
import io.netty.handler.codec.quic.QuicDatagramExtensionEvent;
import io.netty.handler.codec.quic.QuicException;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamLimitChangedEvent;
import io.netty.handler.codec.quic.QuicStreamResetException;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.handler.codec.quic.QuicTransportError;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

/**
 * A QUIC connection as the one connection that carries messages. Each request
 * has a QUIC stream of its own: the side that makes the request opens it with
 * the message that opens the request, and every later message of the request,
 * in either direction, travels on it, framed as on TCP. So requests have flow
 * control and ordering of their own, and one that is slow holds up no other.
 * What arrives on all the streams is handed to the one receiver. It is the Go
 * implementation's {@code quicMsgConn}, stream for stream, and the two talk
 * to each other.
 *
 * <p>Everything about the streams happens on the event loop of the
 * connection, which is one thread: there are no locks.
 */
final class QuicConn implements MsgConn {

    /** The connection of a QUIC channel, for the streams the peer opens on it. */
    static final AttributeKey<QuicConn> KEY = AttributeKey.valueOf("vrpc.quic.conn");

    /** The first stream a client opens, which carries its handshake. */
    private static final long FIRST_CLIENT_STREAM = 0;

    /** The error a connection is closed with when the peer sent what is not a message. */
    private static final int PROTOCOL_VIOLATION = 1;

    /**
     * How long the end of this side's half of a stream waits for the peer to
     * end its own first, in milliseconds: see {@link #fin}. A peer ends its
     * half with its last message or right after it, so this is seldom waited
     * out.
     */
    private static final long PEER_END_WAIT_MILLIS = 250;

    private final QuicChannel channel;
    private final EventLoop loop;
    private final boolean client;
    private final Dialect dialect;
    private final int maxFrameSize;
    private final long writeTimeoutMillis;
    private final long keepAliveMillis;

    private final AtomicBoolean started = new AtomicBoolean();
    /** Closed from this side. */
    private final AtomicBoolean closed = new AtomicBoolean();

    // --- from here on: the event loop only -----------------------------------------

    /** The streams of the requests that are not over, by request id. */
    private final Map<Long, StreamState> streams = new HashMap<Long, StreamState>();
    /** Streams to open that wait for the peer to allow another one, in their order. */
    private final ArrayDeque<StreamState> waitingForCredit = new ArrayDeque<StreamState>();
    /** Streams of the peer that are not read yet. */
    private final List<QuicStreamChannel> paused = new ArrayList<QuicStreamChannel>();
    private Receiver receiver;
    /** On the accepting side: the first message, which is the handshake, was handed over. */
    private boolean handshakeSeen;
    /** The connection is gone. */
    private boolean over;
    /** The receiver was told so. */
    private boolean told;
    /** Why it is gone, when not because it was closed here or by the peer in good order. */
    private Throwable cause;
    private ScheduledFuture<?> keepAlive;
    /** The peer takes datagrams, which is how a silent connection is kept. */
    private boolean datagrams;
    /** Told once the connection is established: how a listener learns of one to hand over. */
    private final Consumer<QuicConn> whenActive;

    /** The stream of one request. */
    private static final class StreamState {
        /** This side made the request and opened the stream. */
        final boolean originatedHere;
        long rid;
        /** The stream is the one of {@code rid} in the map. */
        boolean mapped;
        /** The message that opened the request is known. */
        boolean registered;
        /** The message that opened the request; null when it was not a type this implementation knows. */
        MessageType request;
        /** Null while a stream this side opens is not there yet. */
        QuicStreamChannel ch;
        /** What waits for the stream to be there, in its order. */
        ArrayDeque<Outgoing> backlog;
        /** Nothing more is sent on this side's half: it is ended, or about to be, or the peer wants no more of it. */
        boolean writeClosed;
        /** The end of this side's half was written, with the last message or by itself, or need not be. */
        boolean endWritten;
        /** Set while the end of this side's half waits for the peer to end its own: see {@link QuicConn#fin}. */
        ScheduledFuture<?> endWaits;

        StreamState(boolean originatedHere) {
            this.originatedHere = originatedHere;
        }
    }

    /** A message on its way out. */
    private static final class Outgoing {
        final long rid;
        final MessageType type;
        final ByteBuf frame;
        Batch batch;
        /** The last this side sends on the stream: its half ends with it. */
        boolean last;

        Outgoing(long rid, MessageType type, ByteBuf frame) {
            this.rid = rid;
            this.type = type;
            this.frame = frame;
        }
    }

    /** The messages of one write: done when each is written or dropped. */
    private static final class Batch {
        private final Promise<Void> done;
        private int left;
        private Throwable failure;

        Batch(Promise<Void> done, int count) {
            this.done = done;
            this.left = count;
        }

        void one(Throwable failed) {
            if (failed != null && failure == null) {
                failure = failed;
            }
            if (--left == 0) {
                if (failure != null) {
                    done.tryFailure(failure);
                } else {
                    done.trySuccess(null);
                }
            }
        }
    }

    /**
     * @param channel    the connection; this is called while it is set up, on its event loop
     * @param client     whether this side dialed
     * @param whenActive told once the connection is established, on its event loop; may be null
     */
    QuicConn(QuicChannel channel, boolean client, QuicOptions options, Consumer<QuicConn> whenActive) {
        this.whenActive = whenActive;
        this.channel = channel;
        this.loop = channel.eventLoop();
        this.client = client;
        this.dialect = options.dialect();
        this.maxFrameSize = options.transport().maxFrameSize();
        this.writeTimeoutMillis = options.transport().writeTimeoutMillis();
        this.keepAliveMillis = options.keepAliveMillis();
        channel.attr(KEY).set(this);
        channel.pipeline().addLast(new ConnectionHandler());
    }

    /** Sets up a stream the peer opened: called for each, by the listener and by the dialer. */
    static ChannelInitializer<QuicStreamChannel> peerStreams() {
        return new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
                final QuicConn conn = ch.parent().attr(KEY).get();
                if (conn == null) {
                    ch.close();
                    return;
                }
                final StreamState st = new StreamState(false);
                st.ch = ch;
                conn.attach(ch, st);
            }
        };
    }

    // --- the connection ------------------------------------------------------------

    private final class ConnectionHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            startKeepAlive();
            if (whenActive != null) {
                whenActive.accept(QuicConn.this);
            }
            super.channelActive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof ByteBuf) {
                // A datagram: what keeps the connection from being silent. It carries nothing.
                ReferenceCountUtil.release(msg);
            } else {
                // A stream the peer opened, on its way to where it is set up.
                ctx.fireChannelRead(msg);
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof QuicStreamLimitChangedEvent) {
                creditChanged();
            } else if (evt instanceof QuicDatagramExtensionEvent) {
                // The peer takes datagrams: see startKeepAlive.
                datagrams = ((QuicDatagramExtensionEvent) evt).maxLength() > 0;
            } else if (evt instanceof QuicConnectionCloseEvent) {
                final QuicConnectionCloseEvent close = (QuicConnectionCloseEvent) evt;
                // A peer that closes in good order says so with no error.
                if (cause == null && !(close.isApplicationClose() && close.error() == 0)) {
                    cause = new IOException("quic: the connection was closed: " + close);
                }
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) {
            if (cause == null) {
                cause = error;
            }
            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            gone();
            super.channelInactive(ctx);
        }
    }

    /** Keeps a connection that is silent from being given up as idle. */
    private void startKeepAlive() {
        if (keepAliveMillis <= 0) {
            return;
        }
        keepAlive = loop.scheduleAtFixedRate(() -> {
            if (over) {
                return;
            }
            // The Go implementation sends a PING, which Netty has no way to
            // send. A datagram does what a PING does, for a peer that takes
            // datagrams: this implementation. A Go peer does not, and sends
            // its own PINGs, which keep the connection for both.
            if (datagrams) {
                channel.writeAndFlush(Unpooled.wrappedBuffer(new byte[] {0}));
            }
        }, keepAliveMillis, keepAliveMillis, TimeUnit.MILLISECONDS);
    }

    /** The connection is gone: what waited fails, and the receiver is told. */
    private void gone() {
        if (over) {
            return;
        }
        over = true;
        if (keepAlive != null) {
            keepAlive.cancel(false);
        }
        if (cause == null && channel.isTimedOut()) {
            cause = new IOException("quic: the connection timed out");
        }
        final Throwable why = cause != null ? cause : new ClosedChannelException();
        for (StreamState st : streams.values()) {
            dropBacklog(st, why);
            if (st.endWaits != null) {
                st.endWaits.cancel(false);
                st.endWaits = null;
            }
        }
        streams.clear();
        waitingForCredit.clear();
        paused.clear();
        tell();
    }

    private void tell() {
        if (told || receiver == null) {
            return;
        }
        told = true;
        receiver.onClosed(closed.get() ? null : cause);
    }

    /** The peer sent what is not a message: the connection is of no more use. */
    private void violated(Throwable why) {
        if (over) {
            return;
        }
        if (cause == null) {
            cause = why;
        }
        final String reason = String.valueOf(why.getMessage());
        channel.close(true, PROTOCOL_VIOLATION,
                Unpooled.wrappedBuffer(reason.substring(0, Math.min(reason.length(), 100)).getBytes(StandardCharsets.UTF_8)));
    }

    // --- reading -------------------------------------------------------------------

    /**
     * Whether a stream may be read. Nothing is, before there is a receiver.
     * On the accepting side, before the handshake was handed over, only the
     * stream that carries it is: a client opens it first, and the server has
     * to see the handshake before any request, whatever order the streams
     * arrive in.
     */
    private boolean mayRead(QuicStreamChannel ch) {
        return receiver != null && (client || handshakeSeen || ch.streamId() == FIRST_CLIENT_STREAM);
    }

    private void attach(QuicStreamChannel ch, StreamState st) {
        // Each side ends its half when it has sent its last: a half that the
        // peer ended does not end this one.
        ch.config().setAllowHalfClosure(true);
        if (!mayRead(ch)) {
            ch.config().setAutoRead(false);
            paused.add(ch);
        }
        ch.pipeline().addLast(new FrameReader(maxFrameSize), new StreamHandler(st));
    }

    /** Reads the streams that may be read now. */
    private void resume() {
        if (paused.isEmpty()) {
            return;
        }
        final List<QuicStreamChannel> waiting = new ArrayList<QuicStreamChannel>(paused);
        for (QuicStreamChannel ch : waiting) {
            if (mayRead(ch)) {
                paused.remove(ch);
                ch.config().setAutoRead(true);
            }
        }
    }

    /** Frames of a stream: four bytes of length and a MessagePack map, as on TCP. */
    private static final class FrameReader extends ByteToMessageDecoder {
        private final int maxFrameSize;

        FrameReader(int maxFrameSize) {
            this.maxFrameSize = maxFrameSize;
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            if (in.readableBytes() < 4) {
                return;
            }
            final long length = in.getUnsignedInt(in.readerIndex());
            if (maxFrameSize > 0 && length > maxFrameSize) {
                throw new FrameException("frame too large: " + length + " bytes (max " + maxFrameSize + ")");
            }
            if (length > Integer.MAX_VALUE - 8) {
                throw new FrameException("frame too large: " + length + " bytes");
            }
            if (in.readableBytes() - 4 < length) {
                return;
            }
            in.skipBytes(4);
            final byte[] payload = new byte[(int) length];
            in.readBytes(payload);
            out.add(Frames.decode(payload, 0, payload.length, Limits.DEFAULT));
        }
    }

    private final class StreamHandler extends ChannelInboundHandlerAdapter {
        private final StreamState st;

        StreamHandler(StreamState st) {
            this.st = st;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            received(st, (MapValue) msg);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt == ChannelInputShutdownEvent.INSTANCE) {
                peerDone(st);
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) {
            final Throwable why = error instanceof DecoderException && error.getCause() != null ? error.getCause() : error;
            if (why instanceof QuicStreamResetException) {
                // The peer gave up its half of this stream; the others are as they were.
                peerDone(st);
            } else if (why instanceof FrameException) {
                violated(why);
            } else if (!(why instanceof ClosedChannelException)) {
                violated(new IOException("quic: stream " + ctx.channel() + ": " + why, why));
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            // Both halves are ended: the stream is over, and its request with it.
            paused.remove(ctx.channel());
            if (st.endWaits != null) {
                st.endWaits.cancel(false);
                st.endWaits = null;
            }
            if (st.mapped && streams.get(st.rid) == st) {
                streams.remove(st.rid);
            }
            super.channelInactive(ctx);
        }
    }

    private void received(StreamState st, MapValue message) {
        if (told || receiver == null) {
            return;
        }
        final NumberValue typeNumber = dialect.typeNumber(message);
        final MessageType type = typeNumber == null ? null : MessageType.of(typeNumber.longValue());
        if (!st.registered) {
            // The first message of a stream the peer opened says which request it is of.
            st.registered = true;
            st.request = type;
            final NumberValue rid = dialect.requestId(message);
            if (rid != null && !streams.containsKey(rid.longValue())) {
                st.rid = rid.longValue();
                st.mapped = true;
                streams.put(st.rid, st);
            }
        }
        final boolean handshake = !client && !handshakeSeen;
        receiver.onMessage(message);
        if (handshake) {
            // Only now the other streams: reading them hands over what they
            // hold at once, and the handshake has to come first.
            handshakeSeen = true;
            resume();
        }
        if (type != null && StreamRules.finOnRead(type, st.request, st.originatedHere)) {
            fin(st);
        }
    }

    /** The peer ended its half of a stream. */
    private void peerDone(StreamState st) {
        if (st.endWaits != null) {
            // The end of this half waited for this.
            writeEnd(st, st.ch);
        } else if (!st.mapped) {
            // A stream that carries no request this side knows of: nothing
            // will ever be sent on it, so its other half ends too.
            fin(st);
        }
    }

    // --- writing -------------------------------------------------------------------

    @Override
    public void write(List<MapValue> batch) throws IOException {
        if (batch.isEmpty()) {
            return;
        }
        if (closed.get()) {
            throw new IOException("the connection is closed");
        }
        final Outgoing[] out = new Outgoing[batch.size()];
        try {
            for (int i = 0; i < out.length; i++) {
                final MapValue message = batch.get(i);
                final NumberValue rid = dialect.requestId(message);
                if (rid == null) {
                    throw new IOException("quic: outgoing message has no request id");
                }
                final NumberValue typeNumber = dialect.typeNumber(message);
                out[i] = new Outgoing(rid.longValue(), typeNumber == null ? null : MessageType.of(typeNumber.longValue()),
                        Unpooled.wrappedBuffer(Frames.encode(message)));
            }
        } catch (ValueException e) {
            throw new IOException(e.getMessage(), e);
        }
        final Promise<Void> done = loop.newPromise();
        if (loop.inEventLoop()) {
            // From the receiver: it must not wait, and what it writes is
            // handed to the streams and written from here.
            writeOnLoop(out, done);
            return;
        }
        loop.execute(() -> writeOnLoop(out, done));
        try {
            // A write has no timeout of its own: a peer that does not read, or
            // allows no further stream, would hold the writer for good. The
            // connection is closed instead.
            if (writeTimeoutMillis > 0) {
                if (!done.await(writeTimeoutMillis, TimeUnit.MILLISECONDS)) {
                    close();
                    throw new IOException("quic: write timed out after " + writeTimeoutMillis + " ms");
                }
            } else {
                done.await();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted while writing");
        }
        if (!done.isSuccess()) {
            final Throwable why = done.cause();
            throw why instanceof IOException ? (IOException) why : new IOException(String.valueOf(why), why);
        }
    }

    private void writeOnLoop(Outgoing[] out, Promise<Void> done) {
        if (over) {
            for (Outgoing o : out) {
                o.frame.release();
            }
            done.tryFailure(new ClosedChannelException());
            return;
        }
        final Batch batch = new Batch(done, out.length);
        final List<QuicStreamChannel> touched = new ArrayList<QuicStreamChannel>(Math.min(out.length, 8));
        for (Outgoing o : out) {
            o.batch = batch;
            StreamState st = streams.get(o.rid);
            if (st == null) {
                if (!opens(o)) {
                    // A late message of a request whose stream is over on both
                    // sides: there is nowhere for it to go, and nobody waits for it.
                    o.frame.release();
                    batch.one(null);
                    continue;
                }
                st = open(o.rid, o.type);
            }
            o.last = o.type != null && StreamRules.finOnWrite(st.originatedHere, o.type);
            send(st, o, touched);
        }
        for (QuicStreamChannel ch : touched) {
            ch.flush();
        }
    }

    /** Whether a message is one this side opens a stream with: the first of a request it makes. */
    private boolean opens(Outgoing o) {
        return StreamRules.originates(client, o.rid) && o.type != null
                && (o.type == MessageType.HANDSHAKE_REQUEST || o.type.opensRequest());
    }

    private void send(StreamState st, Outgoing o, List<QuicStreamChannel> touched) {
        if (st.writeClosed) {
            // This side's half is ended: a credit that comes after the request
            // finished, say. It is dropped, as the Go implementation drops it.
            o.frame.release();
            o.batch.one(null);
            return;
        }
        if (o.last) {
            // Nothing follows it, whether it is written now or waits for its
            // stream, and the end of this half travels with it.
            st.writeClosed = true;
            st.endWritten = true;
        }
        if (st.ch == null) {
            st.backlog.add(o);
            return;
        }
        final QuicStreamChannel ch = st.ch;
        write(st, ch, o);
        if (!touched.contains(ch)) {
            touched.add(ch);
        }
    }

    /**
     * Writes a message to its stream. The last one this side sends carries
     * the end of its half with it, in the one frame, so that the end is
     * acknowledged with the bytes of the message; an end that is sent by
     * itself, with no bytes, is what {@link #fin} has to take care with.
     */
    private void write(StreamState st, QuicStreamChannel ch, Outgoing o) {
        final Batch batch = o.batch;
        final Object what = o.last ? new DefaultQuicStreamFrame(o.frame, true) : o.frame;
        ch.write(what).addListener((ChannelFuture f) -> {
            if (f.isSuccess()) {
                batch.one(null);
            } else if (over || !channel.isActive()) {
                batch.one(f.cause());
            } else {
                // The stream, not the connection: the peer wants no more of
                // this request. The other requests are not its concern.
                st.writeClosed = true;
                st.endWritten = true;
                batch.one(null);
            }
        });
    }

    /**
     * Ends this side's half of a stream after something it read: there is no
     * message for the end to travel with, so it is sent by itself. That takes
     * care, on two counts, because of how the QUIC library keeps its books.
     *
     * <p>By what its packet logs show, it takes a stream to be finished, and
     * forgets it, once the end of the peer's half was read and every byte
     * this side sent was acknowledged. An end of this half that has no bytes
     * of its own does not hold it back: when it was written but not yet sent
     * and the stream is then forgotten, it is never sent, and the peer waits
     * for it as long as the connection lasts. (Of forty ends written while
     * the answers to forty calls were read, three were sent.)
     *
     * <p>So the end is written when the peer's own end has been read, which a
     * peer sends with its last message or right after it: nothing is left to
     * read that could have the stream forgotten in between. A peer that does
     * not end its half is waited for a moment and no longer. And it is
     * written when the event loop is done with what it is reading, not in the
     * middle of it: the library sends nothing while it works through packets
     * that arrived together, and what is written between two rounds of
     * reading is sent in the same breath.
     */
    private void fin(StreamState st) {
        if (st.writeClosed) {
            return;
        }
        st.writeClosed = true;
        final QuicStreamChannel ch = st.ch;
        if (ch == null) {
            return; // a stream that is not there yet has read nothing
        }
        if (ch.isInputShutdown()) {
            writeEnd(st, ch);
        } else {
            st.endWaits = loop.schedule(() -> writeEnd(st, ch), PEER_END_WAIT_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    /** Writes the end of this side's half by itself, once. */
    private void writeEnd(StreamState st, QuicStreamChannel ch) {
        if (st.endWaits != null) {
            st.endWaits.cancel(false);
            st.endWaits = null;
        }
        if (st.endWritten) {
            return;
        }
        st.endWritten = true;
        loop.execute(() -> {
            if (ch.isActive() && !ch.isOutputShutdown()) {
                ch.shutdownOutput();
            }
        });
    }

    private StreamState open(long rid, MessageType type) {
        final StreamState st = new StreamState(true);
        st.rid = rid;
        st.mapped = true;
        st.registered = true;
        st.request = type;
        st.backlog = new ArrayDeque<Outgoing>(2);
        streams.put(rid, st);
        if (waitingForCredit.isEmpty()) {
            create(st);
        } else {
            // In their order: one that came later does not overtake.
            waitingForCredit.add(st);
        }
        return st;
    }

    private void create(StreamState st) {
        channel.createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInitializer<QuicStreamChannel>() {
            @Override
            protected void initChannel(QuicStreamChannel ch) {
                attach(ch, st);
            }
        }).addListener((Future<QuicStreamChannel> f) -> {
            if (f.isSuccess()) {
                created(st, f.getNow());
            } else if (!over && noCredit(f.cause())) {
                // The peer allows no further stream until one is over.
                waitingForCredit.addFirst(st);
            } else {
                if (streams.get(st.rid) == st) {
                    streams.remove(st.rid);
                }
                dropBacklog(st, f.cause());
            }
        });
    }

    private static boolean noCredit(Throwable error) {
        return error instanceof QuicException && ((QuicException) error).error() == QuicTransportError.STREAM_LIMIT_ERROR;
    }

    /** A stream this side opened is there: what waited for it is written, in its order. */
    private void created(StreamState st, QuicStreamChannel ch) {
        if (over) {
            dropBacklog(st, new ClosedChannelException());
            return;
        }
        st.ch = ch;
        for (Outgoing o = st.backlog.poll(); o != null; o = st.backlog.poll()) {
            write(st, ch, o);
        }
        st.backlog = null;
        ch.flush();
    }

    private static void dropBacklog(StreamState st, Throwable why) {
        if (st.backlog == null) {
            return;
        }
        for (Outgoing o = st.backlog.poll(); o != null; o = st.backlog.poll()) {
            o.frame.release();
            o.batch.one(why);
        }
    }

    /** The peer allows more streams: the ones that waited are opened, in their order. */
    private void creditChanged() {
        while (!over && !waitingForCredit.isEmpty() && channel.peerAllowedStreams(QuicStreamType.BIDIRECTIONAL) > 0) {
            final StreamState st = waitingForCredit.poll();
            create(st);
            if (waitingForCredit.peekFirst() == st) {
                return; // refused again: until the next time
            }
        }
    }

    // --- the seam ------------------------------------------------------------------

    @Override
    public void start(Receiver receiver) {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("the connection was started already");
        }
        loop.execute(() -> {
            this.receiver = receiver;
            if (over) {
                tell();
            } else {
                resume();
            }
        });
    }

    @Override
    public String remoteAddress() {
        final SocketAddress remote = channel.remoteSocketAddress();
        if (remote instanceof InetSocketAddress) {
            final InetSocketAddress a = (InetSocketAddress) remote;
            final String host = a.getAddress() != null ? a.getAddress().getHostAddress() : a.getHostString();
            return (host.indexOf(':') >= 0 ? "[" + host + "]" : host) + ":" + a.getPort();
        }
        return remote == null ? "" : remote.toString();
    }

    @Override
    public SSLSession tlsSession() {
        final SSLEngine engine = channel.sslEngine();
        return engine == null ? null : engine.getSession();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // In good order, with no error: as the Go implementation closes.
            channel.close(true, 0, Unpooled.EMPTY_BUFFER);
        }
    }

    /** The QUIC connection underneath. */
    QuicChannel channel() {
        return channel;
    }

    /** How many requests have a stream at this moment: for a test. */
    int streamCount() throws InterruptedException {
        try {
            return loop.submit(streams::size).get();
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public String toString() {
        return "QuicConn[" + remoteAddress() + "]";
    }
}
