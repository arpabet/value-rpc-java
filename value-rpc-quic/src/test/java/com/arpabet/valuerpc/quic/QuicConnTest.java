/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.quic;

import static com.arpabet.valuerpc.quic.Ends.send;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.value.MapValue;
import com.arpabet.value.NumberValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.Dialect;
import com.arpabet.valuerpc.FrameException;
import com.arpabet.valuerpc.MessageType;
import com.arpabet.valuerpc.transport.Listener;
import com.arpabet.valuerpc.transport.MsgConn;
import com.arpabet.valuerpc.transport.TransportOptions;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicClientCodecBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The transport by itself, both ends of it here: which stream a message
 * travels on, when each side ends its half, and what the two ends are told.
 * The conversations are those of the protocol, message for message, as the
 * client and the server would hold them.
 */
@Timeout(60)
class QuicConnTest {

    private static final Dialect D = Dialect.standard();
    private static Certs certs;

    @BeforeAll
    static void certificates() throws Exception {
        certs = new Certs();
    }

    private static Ends ends() throws Exception {
        return new Ends(certs.server(), certs.client());
    }

    private static NumberValue id(long requestId) {
        return Value.ofLong(requestId);
    }

    private static MapValue request(MessageType type, long requestId, String name) {
        return D.request(type, requestId, name, Value.ofNull(), 0, null);
    }

    /** The handshake, which every conversation begins with; its stream is freed with its answer. */
    private static void handshake(Ends e) throws Exception {
        final MapValue hello = D.handshakeRequest(42, "", null);
        send(e.client, hello);
        assertEquals(hello, e.atServer.next());
        send(e.server, D.handshakeResponse(null));
        assertEquals(D.handshakeResponse(null), e.atClient.next());
        e.awaitNoStreams();
    }

    @Test
    void aCallHasAStreamOfItsOwnWhichIsFreedWithItsAnswer() throws Exception {
        try (Ends e = ends()) {
            handshake(e);
            final MapValue call = D.request(MessageType.FUNCTION_REQUEST, 1, "greet", Value.list(Value.utf8("quic")), 5000, null);
            send(e.client, call);
            assertEquals(call, e.atServer.next());
            Ends.awaitStreams(e.server, 1);
            Ends.awaitStreams(e.client, 1);
            send(e.server, D.functionResult(id(1), Value.utf8("Hello, quic!")));
            assertEquals(D.functionResult(id(1), Value.utf8("Hello, quic!")), e.atClient.next());
            e.awaitNoStreams();

            // A call that fails is over as one that succeeds.
            send(e.client, request(MessageType.FUNCTION_REQUEST, 2, "fail"));
            e.atServer.next();
            send(e.server, D.error(id(2), Code.RESOURCE_EXHAUSTED, "deliberate failure"));
            assertEquals(D.error(id(2), Code.RESOURCE_EXHAUSTED, "deliberate failure"), e.atClient.next());
            e.awaitNoStreams();
        }
    }

    @Test
    void manyRequestsAtOnceEachOnItsStream() throws Exception {
        try (Ends e = ends()) {
            handshake(e);
            final List<MapValue> calls = new ArrayList<MapValue>();
            for (long rid = 1; rid <= 40; rid++) {
                calls.add(D.request(MessageType.FUNCTION_REQUEST, rid, "echo", Value.ofLong(rid), 0, null));
            }
            e.client.write(calls); // one write, forty streams
            final boolean[] seen = new boolean[41];
            for (int i = 0; i < 40; i++) {
                final MapValue m = e.atServer.next();
                final int rid = (int) D.requestId(m).longValue();
                assertFalse(seen[rid], "request " + rid + " twice");
                seen[rid] = true;
                assertEquals(Value.ofLong(rid), D.arguments(m));
            }
            Ends.awaitStreams(e.server, 40);
            // Answered in the other order, each answer finds its stream.
            final List<MapValue> answers = new ArrayList<MapValue>();
            for (long rid = 40; rid >= 1; rid--) {
                answers.add(D.functionResult(id(rid), Value.ofLong(rid * 2)));
            }
            e.server.write(answers);
            for (int i = 0; i < 40; i++) {
                final MapValue m = e.atClient.next();
                assertEquals(Value.ofLong(D.requestId(m).longValue() * 2), D.result(m));
            }
            e.awaitNoStreams();
        }
    }

    @Test
    void aStreamFromTheServerIsFreedWhenItEnds() throws Exception {
        try (Ends e = ends()) {
            handshake(e);
            send(e.client, request(MessageType.GET_STREAM_REQUEST, 1, "count"));
            e.atServer.next();
            send(e.server, D.streamReady(id(1)));
            assertEquals(D.streamReady(id(1)), e.atClient.next());
            // The client grants credit on the stream of the request, after its request.
            send(e.client, D.streamCredit(id(1), 16));
            assertEquals(D.streamCredit(id(1), 16), e.atServer.next());
            for (long v = 1; v <= 3; v++) {
                send(e.server, D.streamValue(id(1), Value.ofLong(v)));
                assertEquals(D.streamValue(id(1), Value.ofLong(v)), e.atClient.next());
            }
            Ends.awaitStreams(e.client, 1);
            send(e.server, D.streamEnd(id(1), null));
            assertEquals(D.streamEnd(id(1), null), e.atClient.next());
            e.awaitNoStreams();
        }
    }

    @Test
    void aStreamToTheServerIsFreedWhenItEnds() throws Exception {
        try (Ends e = ends()) {
            handshake(e);
            send(e.client, request(MessageType.PUT_STREAM_REQUEST, 1, "upload"));
            e.atServer.next();
            // Being ready does not end the server's half: it goes on granting credit.
            send(e.server, D.streamReady(id(1)), D.streamCredit(id(1), 2));
            e.atClient.next();
            e.atClient.next();
            send(e.client, D.streamValue(id(1), Value.ofLong(1)), D.streamValue(id(1), Value.ofLong(2)));
            e.atServer.next();
            e.atServer.next();
            send(e.server, D.streamCredit(id(1), 2));
            assertEquals(D.streamCredit(id(1), 2), e.atClient.next());
            send(e.client, D.streamValue(id(1), Value.ofLong(3)), D.streamEnd(id(1), null));
            assertEquals(D.streamValue(id(1), Value.ofLong(3)), e.atServer.next());
            assertEquals(D.streamEnd(id(1), null), e.atServer.next());
            e.awaitNoStreams();
        }
    }

    @Test
    void theOneWhoAnswersAChatEndsItAndItsStreamIsFreed() throws Exception {
        try (Ends e = ends()) {
            handshake(e);
            send(e.client, request(MessageType.CHAT_REQUEST, 1, "chat"));
            e.atServer.next();
            send(e.server, D.streamReady(id(1)), D.streamValue(id(1), Value.utf8("bye")), D.streamEnd(id(1), null));
            e.atClient.next();
            e.atClient.next();
            assertEquals(D.streamEnd(id(1), null), e.atClient.next());
            // The server ending what it sends ends the chat, also for a client
            // that had not said its last: nothing more is sent, by either.
            e.awaitNoStreams();
            send(e.client, D.streamValue(id(1), Value.utf8("too late")), D.streamEnd(id(1), null));
            e.atServer.nothing(200);
            e.awaitNoStreams();
        }
    }

    @Test
    void aChatWhoseClientEndsFirstStaysOpenForTheServer() throws Exception {
        try (Ends e = ends()) {
            handshake(e);
            send(e.client, request(MessageType.CHAT_REQUEST, 1, "chat"));
            e.atServer.next();
            send(e.server, D.streamReady(id(1)));
            e.atClient.next();
            send(e.client, D.streamValue(id(1), Value.utf8("one")), D.streamEnd(id(1), null));
            e.atServer.next();
            assertEquals(D.streamEnd(id(1), null), e.atServer.next());
            Ends.awaitStreams(e.server, 1);
            for (long v = 1; v <= 3; v++) {
                send(e.server, D.streamValue(id(1), Value.ofLong(v)));
                assertEquals(D.streamValue(id(1), Value.ofLong(v)), e.atClient.next());
                // The client has said its last, and still grants the server
                // credit for what the server sends.
                send(e.client, D.streamCredit(id(1), 1));
                assertEquals(D.streamCredit(id(1), 1), e.atServer.next());
            }
            Ends.awaitStreams(e.client, 1);
            send(e.server, D.streamEnd(id(1), null));
            assertEquals(D.streamEnd(id(1), null), e.atClient.next());
            e.awaitNoStreams();
        }
    }

    @Test
    void aChatTheServerOpensGoesByTheSameRulesTheOtherWayRound() throws Exception {
        try (Ends e = ends()) {
            handshake(e);
            send(e.server, request(MessageType.CHAT_REQUEST, -1, "chat"));
            e.atClient.next();
            send(e.client, D.streamReady(id(-1)));
            e.atServer.next();
            // The server, which opened it, says its last first, and goes on
            // granting the client credit for what the client still sends.
            send(e.server, D.streamValue(id(-1), Value.utf8("one")), D.streamEnd(id(-1), null));
            e.atClient.next();
            assertEquals(D.streamEnd(id(-1), null), e.atClient.next());
            for (long v = 1; v <= 3; v++) {
                send(e.client, D.streamValue(id(-1), Value.ofLong(v)));
                assertEquals(D.streamValue(id(-1), Value.ofLong(v)), e.atServer.next());
                send(e.server, D.streamCredit(id(-1), 1));
                assertEquals(D.streamCredit(id(-1), 1), e.atClient.next());
            }
            Ends.awaitStreams(e.client, 1);
            Ends.awaitStreams(e.server, 1);
            // The client, which answers, ends the chat.
            send(e.client, D.streamEnd(id(-1), null));
            assertEquals(D.streamEnd(id(-1), null), e.atServer.next());
            e.awaitNoStreams();
        }
    }

    @Test
    void theServerOpensTheStreamOfARequestItMakes() throws Exception {
        try (Ends e = ends()) {
            handshake(e);
            // A call from the server to its client: the server's ids are negative.
            final MapValue call = D.request(MessageType.FUNCTION_REQUEST, -1, "notify", Value.utf8("ping"), 0, null);
            send(e.server, call);
            assertEquals(call, e.atClient.next());
            Ends.awaitStreams(e.client, 1);
            send(e.client, D.functionResult(id(-1), Value.utf8("pong")));
            assertEquals(D.functionResult(id(-1), Value.utf8("pong")), e.atServer.next());
            e.awaitNoStreams();

            // And a stream it pulls from its client.
            send(e.server, request(MessageType.GET_STREAM_REQUEST, -2, "tail"));
            e.atClient.next();
            send(e.client, D.streamReady(id(-2)));
            e.atServer.next();
            send(e.server, D.streamCredit(id(-2), 4));
            e.atClient.next();
            send(e.client, D.streamValue(id(-2), Value.ofLong(1)), D.streamEnd(id(-2), null));
            e.atServer.next();
            assertEquals(D.streamEnd(id(-2), null), e.atServer.next());
            e.awaitNoStreams();
        }
    }

    @Test
    void aCancellationFreesTheStreamOnBothSides() throws Exception {
        try (Ends e = ends()) {
            handshake(e);
            // Of a stream from the server, of a chat and of a call: a cancellation is not answered.
            long rid = 0;
            for (MessageType type : new MessageType[] {MessageType.GET_STREAM_REQUEST, MessageType.CHAT_REQUEST,
                    MessageType.FUNCTION_REQUEST, MessageType.PUT_STREAM_REQUEST}) {
                rid++;
                send(e.client, request(type, rid, "it"));
                e.atServer.next();
                if (type.opensStream()) {
                    send(e.server, D.streamReady(id(rid)));
                    e.atClient.next();
                }
                send(e.client, D.cancelRequest(id(rid)));
                assertEquals(D.cancelRequest(id(rid)), e.atServer.next());
                e.awaitNoStreams();
            }
            // The server may cancel a request of its client too.
            send(e.client, request(MessageType.FUNCTION_REQUEST, 9, "slow"));
            e.atServer.next();
            send(e.server, D.cancelRequest(id(9)));
            assertEquals(D.cancelRequest(id(9)), e.atClient.next());
            e.awaitNoStreams();
        }
    }

    @Test
    void aFailureFreesTheStreamWhateverTheRequest() throws Exception {
        try (Ends e = ends()) {
            handshake(e);
            long rid = 0;
            for (MessageType type : new MessageType[] {MessageType.CHAT_REQUEST, MessageType.PUT_STREAM_REQUEST,
                    MessageType.GET_STREAM_REQUEST}) {
                rid++;
                send(e.client, request(type, rid, "it"));
                e.atServer.next();
                send(e.server, D.streamReady(id(rid)));
                e.atClient.next();
                send(e.server, D.error(id(rid), Code.INTERNAL, "boom"));
                assertEquals(D.error(id(rid), Code.INTERNAL, "boom"), e.atClient.next());
                e.awaitNoStreams();
            }
        }
    }

    @Test
    void whatComesAfterARequestIsOverGoesNowhereAndOpensNoStream() throws Exception {
        try (Ends e = ends()) {
            handshake(e);
            send(e.client, request(MessageType.GET_STREAM_REQUEST, 1, "count"));
            e.atServer.next();
            send(e.server, D.streamReady(id(1)), D.streamEnd(id(1), null));
            e.atClient.next();
            e.atClient.next();
            e.awaitNoStreams();
            // A credit and a cancellation that were on their way: late, of no
            // use to anybody, and not the first message of a new stream.
            send(e.client, D.streamCredit(id(1), 8), D.cancelRequest(id(1)));
            send(e.server, D.streamValue(id(1), Value.ofLong(1)));
            e.atServer.nothing(200);
            e.atClient.nothing(50);
            e.awaitNoStreams();
            // The connection is none the worse.
            send(e.client, request(MessageType.FUNCTION_REQUEST, 2, "echo"));
            assertEquals(2, D.requestId(e.atServer.next()).longValue());
        }
    }

    @Test
    void whatIsSentAfterAHalfWasEndedIsDropped() throws Exception {
        try (Ends e = ends()) {
            handshake(e);
            // A stream to the server: with the end of what it sends, the client
            // has nothing more to say on it, and its half is ended.
            send(e.client, request(MessageType.PUT_STREAM_REQUEST, 1, "upload"));
            e.atServer.next();
            send(e.server, D.streamReady(id(1)), D.streamCredit(id(1), 8));
            e.atClient.next();
            e.atClient.next();
            send(e.client, D.streamValue(id(1), Value.utf8("one")), D.streamEnd(id(1), null));
            e.atServer.next();
            assertEquals(D.streamEnd(id(1), null), e.atServer.next());
            // What it is given to send after that is not sent, and fails nothing.
            send(e.client, D.streamValue(id(1), Value.utf8("too late")), D.streamCredit(id(1), 4));
            e.atServer.nothing(200);
            e.awaitNoStreams();
            // The other requests are none the worse.
            send(e.client, request(MessageType.FUNCTION_REQUEST, 2, "echo"));
            assertEquals(2, D.requestId(e.atServer.next()).longValue());
        }
    }

    @Test
    void aMessageLongerThanAStreamMayCarryAtOnceArrivesWhole() throws Exception {
        try (Ends e = ends()) {
            handshake(e);
            // Three megabytes: several times the window of a stream and of the connection.
            final char[] text = new char[3 * 1024 * 1024];
            Arrays.fill(text, 'x');
            final MapValue big = D.request(MessageType.FUNCTION_REQUEST, 1, "echo", Value.utf8(new String(text)), 0, null);
            send(e.client, big);
            assertEquals(big, e.atServer.next());
            send(e.server, D.functionResult(id(1), D.arguments(big)));
            assertEquals(D.arguments(big), D.result(e.atClient.next()));
            e.awaitNoStreams();
        }
    }

    @Test
    void aFrameLongerThanTheLimitEndsTheConnection() throws Exception {
        final QuicOptions strict = certs.server().withTransport(TransportOptions.DEFAULT.withMaxFrameSize(512));
        try (Ends e = new Ends(strict, certs.client())) {
            handshake(e);
            final char[] text = new char[4096];
            Arrays.fill(text, 'x');
            send(e.client, D.request(MessageType.FUNCTION_REQUEST, 1, "recv", Value.utf8(new String(text)), 0, null));
            final Optional<Throwable> atServer = e.atServer.whyClosed();
            assertTrue(atServer.isPresent() && atServer.get() instanceof FrameException, String.valueOf(atServer));
            assertTrue(atServer.get().getMessage().startsWith("frame too large: "), atServer.get().getMessage());
            e.atServer.nothing(10);
            // The other end is told that the connection was closed on it.
            final Optional<Throwable> atClient = e.atClient.whyClosed();
            assertTrue(atClient.isPresent(), "a connection that was closed on it, not one it closed");
            assertThrows(IOException.class, () -> send(e.client, request(MessageType.FUNCTION_REQUEST, 2, "echo")));
        }
    }

    @Test
    void bytesThatAreNoMessageEndTheConnection() throws Exception {
        try (Ends e = ends()) {
            handshake(e);
            // A frame that says five bytes of a string follow, and has one.
            final QuicStreamChannel raw = e.client.channel().createStream(QuicStreamType.BIDIRECTIONAL,
                    new ChannelInboundHandlerAdapter()).get(5, TimeUnit.SECONDS);
            raw.writeAndFlush(Unpooled.wrappedBuffer(new byte[] {0, 0, 0, 2, (byte) 0xa5, 'x'})).get(5, TimeUnit.SECONDS);
            final Optional<Throwable> why = e.atServer.whyClosed();
            assertTrue(why.isPresent() && why.get() instanceof FrameException, String.valueOf(why));
        }
    }

    @Test
    void closingTellsBothEndsAndFailsWhatIsWrittenAfter() throws Exception {
        try (Ends e = ends()) {
            handshake(e);
            assertEquals(Optional.empty(), Optional.ofNullable(e.atClient.closed.getNow(null)).flatMap(o -> o));
            e.client.close();
            e.client.close(); // closing twice is closing once
            assertEquals(Optional.empty(), e.atClient.whyClosed(), "closed here: no cause");
            assertEquals(Optional.empty(), e.atServer.whyClosed(), "closed by the peer in good order: no cause");
            assertThrows(IOException.class, () -> send(e.client, request(MessageType.FUNCTION_REQUEST, 1, "echo")));
            assertThrows(IOException.class, () -> send(e.server, D.functionResult(id(1), null)));
        }
    }

    @Test
    void whatAnEndIsAndWhoItTalksTo() throws Exception {
        try (Ends e = ends()) {
            assertEquals(e.listener.address(), e.client.remoteAddress(), "the client sees the address it dialed");
            assertTrue(e.server.remoteAddress().startsWith("127.0.0.1:"), e.server.remoteAddress());
            assertNotNull(e.client.tlsSession(), "QUIC is always over TLS");
            assertEquals("TLSv1.3", e.client.tlsSession().getProtocol());
            final X509Certificate shown = (X509Certificate) e.client.tlsSession().getPeerCertificates()[0];
            assertEquals(certs.server.getCertificate(), shown, "the certificate the server showed");
            assertThrows(IllegalStateException.class, () -> e.client.start(e.atClient), "started once");
            assertTrue(e.listener.toString().startsWith("quic://127.0.0.1:"), e.listener.toString());
        }
    }

    @Test
    void theEndOfAHalfWaitsForThePeersEndButNotForLong() throws Exception {
        // A server of the test's own, on Netty, which ends its half when the
        // test says so: with its answer, as a peer does, or not at all.
        final java.util.concurrent.ConcurrentHashMap<Long, QuicStreamChannel> opened =
                new java.util.concurrent.ConcurrentHashMap<Long, QuicStreamChannel>();
        final java.util.concurrent.ConcurrentHashMap<Long, CompletableFuture<Long>> ended =
                new java.util.concurrent.ConcurrentHashMap<Long, CompletableFuture<Long>>();
        final java.util.concurrent.LinkedBlockingQueue<Long> arrived = new java.util.concurrent.LinkedBlockingQueue<Long>();
        final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        Channel udp = null;
        QuicConn client = null;
        // The wait is a quarter of a second; two seconds here, so that a
        // machine that stalls for a moment does not blur what is being told apart.
        final long wait = 2000;
        QuicConn.peerEndWaitMillis = wait;
        try {
            udp = new Bootstrap().group(group).channel(NioDatagramChannel.class)
                    .handler(new io.netty.handler.codec.quic.QuicServerCodecBuilder()
                            .sslContext(certs.serverTls())
                            .initialMaxData(1 << 20).initialMaxStreamDataBidirectionalLocal(1 << 20)
                            .initialMaxStreamDataBidirectionalRemote(1 << 20).initialMaxStreamsBidirectional(100)
                            .handler(new ChannelInboundHandlerAdapter())
                            .streamHandler(new io.netty.channel.ChannelInitializer<QuicStreamChannel>() {
                                @Override
                                protected void initChannel(QuicStreamChannel ch) {
                                    ch.config().setAllowHalfClosure(true);
                                    opened.put(ch.streamId(), ch);
                                    ended.put(ch.streamId(), new CompletableFuture<Long>());
                                    ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                        @Override
                                        public void channelRead(io.netty.channel.ChannelHandlerContext ctx, Object msg) {
                                            io.netty.util.ReferenceCountUtil.release(msg);
                                            arrived.add(ch.streamId());
                                        }

                                        @Override
                                        public void userEventTriggered(io.netty.channel.ChannelHandlerContext ctx, Object evt) {
                                            if (evt == io.netty.channel.socket.ChannelInputShutdownEvent.INSTANCE) {
                                                ended.get(ch.streamId()).complete(System.nanoTime());
                                            }
                                        }
                                    });
                                }
                            })
                            .build())
                    .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
            final int port = ((InetSocketAddress) udp.localAddress()).getPort();
            client = (QuicConn) QuicTransport.dialer("127.0.0.1:" + port, certs.client()).dial(10_000);
            final Ends.Recorder atClient = new Ends.Recorder();
            client.start(atClient);

            // The answer and the end of the server's half together: the client
            // has read the end when it reads the answer, and ends its half at once.
            send(client, D.handshakeRequest(1, "", null));
            assertEquals(Long.valueOf(0), arrived.poll(10, TimeUnit.SECONDS));
            long answered = System.nanoTime();
            opened.get(0L).writeAndFlush(new io.netty.handler.codec.quic.DefaultQuicStreamFrame(
                    Unpooled.wrappedBuffer(com.arpabet.valuerpc.Frames.encode(D.handshakeResponse(null))), true));
            assertEquals(D.handshakeResponse(null), atClient.next());
            long waited = TimeUnit.NANOSECONDS.toMillis(ended.get(0L).get(10, TimeUnit.SECONDS) - answered);
            assertTrue(waited < wait / 2, "the end of the client's half came after " + waited + " ms");
            Ends.awaitStreams(client, 0);

            // The answer without the end of the server's half: the client
            // waits for that end a moment, and then ends its half all the same.
            send(client, request(MessageType.FUNCTION_REQUEST, 1, "greet"));
            assertEquals(Long.valueOf(4), arrived.poll(10, TimeUnit.SECONDS));
            answered = System.nanoTime();
            opened.get(4L).writeAndFlush(Unpooled.wrappedBuffer(
                    com.arpabet.valuerpc.Frames.encode(D.functionResult(id(1), Value.utf8("hello")))));
            assertEquals(D.functionResult(id(1), Value.utf8("hello")), atClient.next());
            waited = TimeUnit.NANOSECONDS.toMillis(ended.get(4L).get(20, TimeUnit.SECONDS) - answered);
            assertTrue(waited >= wait - 100 && waited < wait + 8000, "the end of the client's half came after " + waited + " ms");
            // What the client is given to send on it meanwhile, and after, is dropped.
            send(client, D.cancelRequest(id(1)));
            Ends.awaitStreams(client, 1);
            // When the server ends its half at last, the stream is over.
            opened.get(4L).shutdownOutput();
            Ends.awaitStreams(client, 0);

            // And the one that ends its half before the client is done waiting: at once then.
            send(client, request(MessageType.FUNCTION_REQUEST, 2, "greet"));
            assertEquals(Long.valueOf(8), arrived.poll(10, TimeUnit.SECONDS));
            answered = System.nanoTime();
            opened.get(8L).writeAndFlush(Unpooled.wrappedBuffer(
                    com.arpabet.valuerpc.Frames.encode(D.functionResult(id(2), Value.utf8("hello")))));
            atClient.next();
            Thread.sleep(40);
            opened.get(8L).shutdownOutput();
            waited = TimeUnit.NANOSECONDS.toMillis(ended.get(8L).get(10, TimeUnit.SECONDS) - answered);
            assertTrue(waited >= 30 && waited < wait / 2, "the end of the client's half came after " + waited + " ms");
            Ends.awaitStreams(client, 0);
            atClient.nothing(20);
        } finally {
            QuicConn.peerEndWaitMillis = 250;
            if (client != null) {
                client.close();
            }
            if (udp != null) {
                udp.close();
            }
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        }
    }

    @Test
    void theHandshakeIsHandedOverBeforeWhatCameOnOtherStreams() throws Exception {
        // A client of the test's own, on Netty: it opens the stream of the
        // handshake first, as every client does, but sends on a later stream
        // before it sends on that one.
        final Listener listener = QuicTransport.listener("127.0.0.1:0", certs.server());
        final Ends.Recorder atServer = new Ends.Recorder();
        final CompletableFuture<MsgConn> accepted = new CompletableFuture<MsgConn>();
        listener.start(conn -> {
            conn.start(atServer);
            accepted.complete(conn);
        });
        final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try {
            final Channel udp = new Bootstrap().group(group).channel(NioDatagramChannel.class)
                    .handler(new QuicClientCodecBuilder()
                            .sslEngineProvider(q -> certs.clientTls().newEngine(q.alloc(), "127.0.0.1", 1))
                            .initialMaxData(1 << 20).initialMaxStreamDataBidirectionalLocal(1 << 20)
                            .initialMaxStreamDataBidirectionalRemote(1 << 20).initialMaxStreamsBidirectional(100)
                            .build())
                    .bind(0).sync().channel();
            final String[] hostPort = listener.address().split(":");
            final QuicChannel quic = QuicChannel.newBootstrap(udp).streamHandler(new ChannelInboundHandlerAdapter())
                    .remoteAddress(new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])))
                    .connect().get(10, TimeUnit.SECONDS);
            final QuicStreamChannel first = quic.createStream(QuicStreamType.BIDIRECTIONAL,
                    new ChannelInboundHandlerAdapter()).get(5, TimeUnit.SECONDS);
            final QuicStreamChannel second = quic.createStream(QuicStreamType.BIDIRECTIONAL,
                    new ChannelInboundHandlerAdapter()).get(5, TimeUnit.SECONDS);
            assertEquals(0, first.streamId());
            assertEquals(4, second.streamId());

            final MapValue call = request(MessageType.FUNCTION_REQUEST, 1, "greet");
            second.writeAndFlush(Unpooled.wrappedBuffer(com.arpabet.valuerpc.Frames.encode(call))).get(5, TimeUnit.SECONDS);
            accepted.get(10, TimeUnit.SECONDS);
            atServer.nothing(300); // the request waits: the handshake has not come
            final MapValue hello = D.handshakeRequest(7, "", null);
            first.writeAndFlush(Unpooled.wrappedBuffer(com.arpabet.valuerpc.Frames.encode(hello))).get(5, TimeUnit.SECONDS);
            assertEquals(hello, atServer.next(), "the handshake first");
            assertEquals(call, atServer.next(), "then what waited");
            quic.close().get(5, TimeUnit.SECONDS);
            udp.close();
        } finally {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
            listener.close();
            if (accepted.isDone()) {
                accepted.get().close();
            }
        }
    }
}
