/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.value.Kind;
import com.arpabet.value.ListValue;
import com.arpabet.value.MapValue;
import com.arpabet.value.NumberValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.Chat;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.ConformanceServer;
import com.arpabet.valuerpc.Dialect;
import com.arpabet.valuerpc.FakeServer;
import com.arpabet.valuerpc.HashChain;
import com.arpabet.valuerpc.MessageType;
import com.arpabet.valuerpc.Peer;
import com.arpabet.valuerpc.TypeDef;
import com.arpabet.valuerpc.ValueSink;
import com.arpabet.valuerpc.ValueStream;
import com.arpabet.valuerpc.VrpcException;
import com.arpabet.valuerpc.client.ClientListener;
import com.arpabet.valuerpc.client.VrpcClient;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A connection used the other way: the server asks its client. The server and
 * the client of this library against each other over the in-memory transport,
 * over TCP and over WebSocket in both of its codecs, and each against a peer
 * of the test's own for what goes over the wire and for what a peer of this
 * library never sends.
 */
@Timeout(60)
class ReverseTest {

    private static final Dialect D = Raw.D;

    private interface Scenario {
        void run(ConformanceServer server, Wire wire) throws Exception;
    }

    private static void overEachTransport(Scenario scenario) throws Exception {
        overEachTransport(builder -> {
        }, scenario);
    }

    private static void overEachTransport(Consumer<VrpcServer.Builder> setup, Scenario scenario) throws Exception {
        for (Wire wire : Wire.all()) {
            final VrpcServer.Builder builder = VrpcServer.builder().address(wire.listen()).transportOptions(wire.options());
            setup.accept(builder);
            try (ConformanceServer server = ConformanceServer.start(builder, new ConformanceServer.Shared())) {
                // Asks the client by the pattern and the name it is given, and says how that went.
                server.server().addFunction("ask", (ctx, args) -> Value.utf8(ask(ctx.peer(), (ListValue) args)));
                scenario.run(server, wire);
            } catch (AssertionError | Exception e) {
                throw new AssertionError("over " + wire + ": " + e, e);
            }
        }
    }

    /** One request of a server to its client: {@code [pattern, name, argument]}, answered {@code ok:...} or {@code CODE:...}. */
    private static String ask(Peer peer, ListValue how) throws InterruptedException {
        final String pattern = how.get(0).toString();
        final String name = how.get(1).toString();
        final Value arg = how.size() > 2 ? how.get(2) : null;
        try {
            switch (pattern) {
                case "call":
                    return "ok:" + peer.callFunction(name, arg);
                case "get": {
                    final StringBuilder all = new StringBuilder("ok:");
                    try (ValueStream in = peer.getStream(name, arg, CallOptions.DEFAULT)) {
                        for (Value v : in) {
                            all.append(v).append(' ');
                        }
                    }
                    return all.toString().trim();
                }
                case "put":
                    try (ValueSink out = peer.putStream(name, arg, CallOptions.DEFAULT)) {
                        out.send(Value.utf8("one"));
                    }
                    return "ok:";
                default:
                    try (Chat chat = peer.chat(name, arg, CallOptions.DEFAULT)) {
                        chat.outgoing().send(Value.utf8("one"));
                        chat.outgoing().close();
                        final StringBuilder all = new StringBuilder("ok:");
                        for (Value v : chat.incoming()) {
                            all.append(v).append(' ');
                        }
                        return all.toString().trim();
                    }
            }
        } catch (VrpcException e) {
            return e.code().name() + ":" + e.detail();
        }
    }

    private static Value how(String pattern, String name) {
        return Value.list(Value.utf8(pattern), Value.utf8(name));
    }

    private static Value how(String pattern, String name, Value arg) {
        return Value.list(Value.utf8(pattern), Value.utf8(name), arg);
    }

    private static VrpcClient.Builder clientOf(ConformanceServer server, Wire wire) {
        return VrpcClient.builder().dialer(wire.dialer(server.address())).timeoutMillis(10_000).dialTimeoutMillis(5000);
    }

    private static Raw rawOf(ConformanceServer server, Wire wire) throws IOException {
        return new Raw(wire.dialer(server.address()).dial(5000));
    }

    private static void waitFor(BooleanSupplier what) throws InterruptedException {
        final long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!what.getAsBoolean()) {
            if (System.nanoTime() > until) {
                throw new AssertionError("waited too long");
            }
            Thread.sleep(2);
        }
    }

    private static long number(Value args, int index) {
        return ((ListValue) args).getNumber(index).longValue();
    }

    /** What the clients of the conformance suites serve: {@code notify}, {@code tail} and {@code ingest}, which gives what it took. */
    private static List<Value> serve(VrpcClient client) {
        final List<Value> ingested = Collections.synchronizedList(new ArrayList<Value>());
        client.addFunction("notify", (ctx, args) -> Value.utf8("client-saw:" + ((ListValue) args).get(0)));
        client.addOutgoingStream("tail", TypeDef.ANY, (ctx, args, out) -> {
            final long n = number(args, 0);
            for (long i = 0; i < n; i++) {
                out.send(Value.utf8("t" + i));
            }
        });
        client.addIncomingStream("ingest", TypeDef.ANY, (ctx, args, in) -> {
            for (Value v : in) {
                ingested.add(v);
            }
        });
        return ingested;
    }

    /** A handler that waits until it is cancelled, and says when it began and how it ended. */
    private static final class Waiting {
        final CountDownLatch began = new CountDownLatch(1);
        final LinkedBlockingQueue<String> ended = new LinkedBlockingQueue<String>();

        Value handle(com.arpabet.valuerpc.RequestContext ctx, Value args) {
            began.countDown();
            try {
                Thread.sleep(30_000);
                ended.add("slept");
            } catch (InterruptedException e) {
                ended.add("interrupted, cancelled=" + ctx.isCancelled());
            }
            return null;
        }
    }

    // --- the four patterns -----------------------------------------------------------

    @Test
    void aServerAsksItsClientByEveryPattern() throws Exception {
        overEachTransport((server, wire) -> {
            try (VrpcClient client = clientOf(server, wire).build()) {
                final List<Value> ingested = serve(client);
                client.addChat("talk", TypeDef.ANY, (ctx, args, in, out) -> {
                    for (Value v : in) {
                        out.send(Value.utf8("heard:" + v));
                    }
                });

                // A call: the server answers its client with what the client answered it.
                assertEquals(Value.utf8("client-saw:ping"), client.callFunction("reverse.call", Value.list(Value.utf8("ping"))));
                // A stream from the client, read by the server to its end.
                assertEquals(Value.list(Value.utf8("t0"), Value.utf8("t1"), Value.utf8("t2"), Value.utf8("t3")),
                        client.callFunction("reverse.pull", Value.list(Value.ofLong(4))));
                // A stream to the client.
                assertEquals(Value.utf8("pushed"), client.callFunction("reverse.push", Value.list(Value.utf8("go"))));
                waitFor(() -> ingested.size() == 3);
                assertEquals(java.util.Arrays.asList(Value.ofLong(100), Value.ofLong(200), Value.ofLong(300)), ingested);
                // Both at once.
                assertEquals(Value.utf8("ok:heard:one"), client.callFunction("ask", how("chat", "talk")));

                // What the client asks goes on beside it, on the same connection.
                assertEquals(Value.utf8("Hello, both!"), client.callFunction("greet", Value.list(Value.utf8("both"))));
                assertEquals(0L, client.stats().get("pending"));
            }
        });
    }

    @Test
    void aClientAnswersWhatItDoesNotServeInItsOwnWords() throws Exception {
        overEachTransport((server, wire) -> {
            final LinkedBlockingQueue<String> failures = new LinkedBlockingQueue<String>();
            try (VrpcClient client = clientOf(server, wire).listener(new ClientListener() {
                @Override
                public void onHandlerFailure(String what, Throwable error) {
                    failures.add(what + ": " + error.getMessage());
                }
            }).build()) {
                serve(client);
                client.addFunction("typed", TypeDef.list(TypeDef.STRING), TypeDef.NUMBER, (ctx, args) -> ((ListValue) args).get(0));
                client.addOutgoingStream("typed.tail", TypeDef.list(TypeDef.NUMBER), (ctx, args, out) -> {
                });
                client.addFunction("fail", (ctx, args) -> {
                    throw new VrpcException(Code.RESOURCE_EXHAUSTED, "deliberate failure");
                });
                client.addFunction("boom", (ctx, args) -> {
                    throw new IllegalStateException("boom");
                });
                client.addOutgoingStream("refuse", TypeDef.ANY, (ctx, args, out) -> {
                    throw new VrpcException(Code.NOT_FOUND, "nothing to tail");
                });

                assertEquals(Value.utf8("NOT_FOUND:function not found nowhere"), client.callFunction("ask", how("call", "nowhere")));
                assertEquals(Value.utf8("NOT_FOUND:function not found nowhere"), client.callFunction("ask", how("get", "nowhere")));
                // A name serves one pattern. A client looks at the pattern first, and says it in the words of the Go client.
                assertEquals(Value.utf8("INVALID_ARGUMENT:function 'tail' is not unary"), client.callFunction("ask", how("call", "tail")));
                assertEquals(Value.utf8("INVALID_ARGUMENT:function 'notify' wrong stream type"),
                        client.callFunction("ask", how("get", "notify")));
                assertEquals(Value.utf8("INVALID_ARGUMENT:function 'tail' wrong stream type"),
                        client.callFunction("ask", how("put", "tail")));
                // The arguments are checked before the handler sees them, and the result before the server does.
                assertEquals(Value.utf8("INVALID_ARGUMENT:function 'typed' invalid args"),
                        client.callFunction("ask", how("call", "typed", Value.list(Value.ofLong(1)))));
                assertEquals(Value.utf8("INVALID_ARGUMENT:stream 'typed.tail' invalid args"),
                        client.callFunction("ask", how("get", "typed.tail", Value.list(Value.utf8("x")))));
                assertEquals(Value.utf8("INTERNAL:function 'typed' invalid result"),
                        client.callFunction("ask", how("call", "typed", Value.list(Value.utf8("x")))));
                // What a handler throws: its own code, or internal with its message, which the listener is given.
                assertEquals(Value.utf8("RESOURCE_EXHAUSTED:function fail: deliberate failure"),
                        client.callFunction("ask", how("call", "fail")));
                assertEquals(Value.utf8("INTERNAL:function boom: boom"), client.callFunction("ask", how("call", "boom")));
                assertEquals("function boom: boom", failures.poll(5, TimeUnit.SECONDS));
                // A stream its handler refuses is not opened.
                assertEquals(Value.utf8("NOT_FOUND:out stream refuse: nothing to tail"), client.callFunction("ask", how("get", "refuse")));

                // A name registered again serves the new handler: a client replaces, where a server refuses.
                client.addFunction("notify", (ctx, args) -> Value.utf8("replaced"));
                assertEquals(Value.utf8("ok:replaced"), client.callFunction("ask", how("call", "notify", Value.list(Value.utf8("x")))));
                assertThrows(IllegalStateException.class, () -> server.server().addFunction("ask", (ctx, args) -> null));
            }
        });
    }

    @Test
    void aHandlerOfAClientAsksTheServerInTurn() throws Exception {
        overEachTransport((server, wire) -> {
            try (VrpcClient client = clientOf(server, wire).build()) {
                // While it serves the server, the client asks the server, which is the peer of its context.
                client.addFunction("sum", (ctx, args) -> ctx.peer().callFunction("add", args));
                client.addFunction("who", (ctx, args) -> Value.utf8("principal=" + ctx.principal() + " id=" + (ctx.requestId() < 0)
                        + " remaining=" + ctx.remainingMillis() + " md=" + ctx.metadata()));
                assertEquals(Value.utf8("ok:42"),
                        client.callFunction("ask", how("call", "sum", Value.list(Value.ofLong(40), Value.ofLong(2)))));
                // A server names no time for a call and is nobody to a client; its requests have negative ids.
                assertEquals(Value.utf8("ok:principal= id=true remaining=-1 md={}"), client.callFunction("ask", how("call", "who")));
            }
        });
    }

    @Test
    void manyValuesGoBothWaysAtThePaceOfTheReader() throws Exception {
        overEachTransport(builder -> builder.streamWindow(64), (server, wire) -> {
            server.server().addFunction("pull.sum", (ctx, args) -> {
                long sum = 0;
                long count = 0;
                try (ValueStream in = ctx.peer().getStream("numbers", args, CallOptions.DEFAULT)) {
                    for (Value v : in) {
                        sum += ((NumberValue) v).longValue();
                        count++;
                    }
                }
                return Value.list(Value.ofLong(count), Value.ofLong(sum));
            });
            server.server().addFunction("push.many", (ctx, args) -> {
                final long n = number(args, 0);
                try (ValueSink out = ctx.peer().putStream("ingest", null, CallOptions.DEFAULT)) {
                    for (long i = 1; i <= n; i++) {
                        out.send(Value.ofLong(i));
                    }
                }
                return null;
            });
            try (VrpcClient client = clientOf(server, wire).streamWindow(32).build()) {
                final List<Value> ingested = serve(client);
                client.addOutgoingStream("numbers", TypeDef.ANY, (ctx, args, out) -> {
                    final long n = number(args, 0);
                    for (long i = 1; i <= n; i++) {
                        out.send(Value.ofLong(i));
                    }
                });
                // Far more than either window: the sender waits for the credit of the reader, and nothing is lost.
                assertEquals(Value.list(Value.ofLong(5000), Value.ofLong(5000L * 5001 / 2)),
                        client.callFunction("pull.sum", Value.list(Value.ofLong(5000))));
                client.callFunction("push.many", Value.list(Value.ofLong(5000)));
                waitFor(() -> ingested.size() == 5000);
                assertEquals(Value.ofLong(1), ingested.get(0));
                assertEquals(Value.ofLong(5000), ingested.get(4999));
            }
        });
    }

    // --- giving up -------------------------------------------------------------------

    @Test
    void whatTheServerGivesUpIsCancelledAtTheClient() throws Exception {
        overEachTransport((server, wire) -> {
            // The handler of the server is interrupted when its own call is cancelled, which cancels what it asked.
            server.server().addFunction("relay", (ctx, args) -> ctx.peer().callFunction("wait", null));
            // The one who asks names a time: the call fails when it has passed, and the client is told to stop.
            server.server().addFunction("impatient", (ctx, args) -> {
                try {
                    return ctx.peer().callFunction("wait", null, CallOptions.timeoutMillis(150));
                } catch (VrpcException e) {
                    return Value.utf8(e.code().name() + ":" + e.detail());
                }
            });
            try (VrpcClient client = clientOf(server, wire).build()) {
                Waiting waiting = new Waiting();
                client.addFunction("wait", waiting::handle);
                final CompletableFuture<Value> relayed = client.callFunctionAsync("relay", null, CallOptions.DEFAULT);
                assertTrue(waiting.began.await(10, TimeUnit.SECONDS));
                relayed.cancel(true);
                assertEquals("interrupted, cancelled=true", waiting.ended.poll(10, TimeUnit.SECONDS));

                waiting = new Waiting();
                client.addFunction("wait", waiting::handle);
                assertEquals(Value.utf8("DEADLINE_EXCEEDED:timeout"), client.callFunction("impatient", null));
                assertEquals("interrupted, cancelled=true", waiting.ended.poll(10, TimeUnit.SECONDS));

                // A stream the server stops reading is cancelled at the client, whose handler is told by its stream.
                final LinkedBlockingQueue<String> tailEnded = new LinkedBlockingQueue<String>();
                client.addOutgoingStream("endless", TypeDef.ANY, (ctx, args, out) -> {
                    try {
                        for (long i = 0;; i++) {
                            out.send(Value.ofLong(i));
                        }
                    } catch (VrpcException e) {
                        tailEnded.add(e.code().name() + " cancelled=" + ctx.isCancelled());
                        throw e;
                    }
                });
                server.server().addFunction("peek", (ctx, args) -> {
                    try (ValueStream in = ctx.peer().getStream("endless", null, CallOptions.DEFAULT)) {
                        return in.take();
                    }
                });
                assertEquals(Value.ofLong(0), client.callFunction("peek", null));
                assertEquals("CANCELED cancelled=true", tailEnded.poll(10, TimeUnit.SECONDS));
                waitFor(() -> client.stats().get("pending") == 0);
            }
        });
    }

    @Test
    void aClientServesOnlySoManyCallsAtOnce() throws Exception {
        overEachTransport((server, wire) -> {
            try (VrpcClient client = clientOf(server, wire).maxConcurrentInbound(1).build()) {
                final Waiting waiting = new Waiting();
                client.addFunction("wait", waiting::handle);
                serve(client);
                server.server().addFunction("relay", (ctx, args) -> ctx.peer().callFunction("wait", null));
                final CompletableFuture<Value> first = client.callFunctionAsync("relay", null, CallOptions.DEFAULT);
                assertTrue(waiting.began.await(10, TimeUnit.SECONDS));
                // One too many is refused, in the words of the Go client, and the one that runs is not disturbed.
                assertEquals(Value.utf8("RESOURCE_EXHAUSTED:client busy: too many concurrent reverse calls (max 1)"),
                        client.callFunction("ask", how("call", "notify", Value.list(Value.utf8("x")))));
                // Streams are not counted.
                assertEquals(Value.utf8("ok:t0 t1"), client.callFunction("ask", how("get", "tail", Value.list(Value.ofLong(2)))));
                first.cancel(true);
                assertEquals("interrupted, cancelled=true", waiting.ended.poll(10, TimeUnit.SECONDS));
                // The slot is free again.
                waitFor(() -> Value.utf8("ok:client-saw:x")
                        .equals(client.callFunction("ask", how("call", "notify", Value.list(Value.utf8("x"))))));
            }
        });
    }

    // --- the peer across connections -------------------------------------------------

    @Test
    void aPeerThatWasKeptIsAskedLaterAndAcrossAReconnect() throws Exception {
        overEachTransport((server, wire) -> {
            final AtomicReference<Peer> kept = new AtomicReference<Peer>();
            server.server().addFunction("keep", (ctx, args) -> {
                kept.set(ctx.peer());
                return null;
            });
            try (VrpcClient client = clientOf(server, wire).build()) {
                serve(client);
                client.callFunction("keep", null);
                final Peer peer = kept.get();
                assertNotNull(peer);
                // From a thread that serves nothing, long after the request the peer came with.
                assertEquals(Value.utf8("client-saw:later"), peer.callFunction("notify", Value.list(Value.utf8("later"))));

                // The client is dropped and comes back as the same session: the peer is the same, on the new connection.
                server.drop();
                waitFor(() -> client.stats().get("reconnects") == 1 && client.isActive());
                waitFor(() -> {
                    try {
                        return Value.utf8("client-saw:again").equals(peer.callFunction("notify", Value.list(Value.utf8("again"))));
                    } catch (VrpcException e) {
                        // Between the two connections there is none to ask on.
                        assertEquals(Code.UNAVAILABLE, e.code(), e.detail());
                        return false;
                    }
                });
                assertEquals(1L, server.server().stats().get("sessions"));

                // A client that is gone for good cannot be asked, and whoever asks is told at once.
                client.close();
                waitFor(() -> server.server().stats().get("connections") == 0);
                final VrpcException e = assertThrows(VrpcException.class, () -> peer.callFunction("notify", Value.list(Value.utf8("x"))));
                assertEquals(Code.UNAVAILABLE, e.code());
                assertEquals("client is not connected", e.detail());
                assertEquals(Code.UNAVAILABLE,
                        assertThrows(VrpcException.class, () -> peer.getStream("tail", null, CallOptions.DEFAULT)).code());
            }
        });
    }

    @Test
    void whatWasAskedOnALostConnectionFailsAtOnceOnBothSides() throws Exception {
        overEachTransport((server, wire) -> {
            final AtomicReference<Peer> kept = new AtomicReference<Peer>();
            server.server().addFunction("keep", (ctx, args) -> {
                kept.set(ctx.peer());
                return null;
            });
            try (VrpcClient client = clientOf(server, wire).build()) {
                final Waiting waiting = new Waiting();
                client.addFunction("wait", waiting::handle);
                final LinkedBlockingQueue<String> ingestEnded = new LinkedBlockingQueue<String>();
                client.addIncomingStream("ingest", TypeDef.ANY, (ctx, args, in) -> {
                    try {
                        for (Value v : in) {
                            // taken and dropped
                        }
                        ingestEnded.add("ended");
                    } catch (VrpcException e) {
                        ingestEnded.add(e.code().name() + ":" + e.detail() + " cancelled=" + ctx.isCancelled());
                    }
                });
                client.callFunction("keep", null);
                final Peer peer = kept.get();
                final CompletableFuture<Value> call = peer.callFunctionAsync("wait", null, CallOptions.DEFAULT);
                final ValueSink stream = peer.putStream("ingest", null, CallOptions.DEFAULT);
                stream.send(Value.ofLong(1));
                assertTrue(waiting.began.await(10, TimeUnit.SECONDS));

                server.drop();

                // The server: a call without a time of its own would wait for ever otherwise.
                final ExecutionException failed = assertThrows(ExecutionException.class, () -> call.get(10, TimeUnit.SECONDS));
                final VrpcException e = (VrpcException) failed.getCause();
                assertEquals(Code.UNAVAILABLE, e.code());
                assertEquals("connection lost; reverse request cannot complete", e.detail());
                final VrpcException sent = assertThrows(VrpcException.class, () -> {
                    for (int i = 0; i < 100_000; i++) {
                        stream.send(Value.ofLong(i));
                    }
                });
                assertEquals(Code.UNAVAILABLE, sent.code(), sent.detail());
                // The client: its handlers are told, as nobody waits for them any more.
                assertEquals("interrupted, cancelled=true", waiting.ended.poll(10, TimeUnit.SECONDS));
                assertEquals("UNAVAILABLE:connection lost cancelled=true", ingestEnded.poll(10, TimeUnit.SECONDS));
            }
        });
    }

    // --- on the wire -----------------------------------------------------------------

    @Test
    void whatAServerAsksIsNumberedBelowZeroAndSaysNoMoreThanTheGoServer() throws Exception {
        overEachTransport((server, wire) -> {
            server.server().addFunction("meta", (ctx, args) ->
                    ctx.peer().callFunction("notify", null, CallOptions.DEFAULT.withMetadata("trace", "t1").withTimeoutMillis(5000)));
            try (Raw raw = rawOf(server, wire).shake()) {
                // The arguments come as they were given, and the time the caller allows is not sent.
                raw.send(raw.request(MessageType.FUNCTION_REQUEST, 1, "reverse.call", Value.list(Value.utf8("ping"))));
                MapValue asked = raw.next(MessageType.FUNCTION_REQUEST, -1);
                assertEquals("notify", D.functionName(asked));
                assertEquals(Value.list(Value.utf8("ping")), D.arguments(asked));
                assertEquals(0, D.slaMillis(asked));
                assertTrue(D.metadata(asked).isEmpty());
                raw.send(D.functionResult(Value.ofLong(-1), Value.utf8("pong")));
                assertEquals(Value.utf8("pong"), D.result(raw.next(MessageType.FUNCTION_RESPONSE, 1)));

                // Arguments there are none of are left out; metadata travels with a call, and no time with it.
                raw.send(raw.request(MessageType.FUNCTION_REQUEST, 2, "meta", null));
                asked = raw.next(MessageType.FUNCTION_REQUEST, -2);
                assertFalse(asked.containsKey(D.argumentsField()), "arguments of none");
                assertEquals(Collections.singletonMap("trace", "t1"), D.metadata(asked));
                assertEquals(0, D.slaMillis(asked));
                raw.send(D.error(Value.ofLong(-2), Code.NOT_FOUND, "function not found notify"));
                final VrpcException e = D.errorOf(raw.next(MessageType.ERROR_RESPONSE, 2));
                assertEquals(Code.NOT_FOUND, e.code());
                assertEquals("function meta: function not found notify", e.detail());

                // A stream from the client: taken, then the window as credit, then its values.
                raw.send(raw.request(MessageType.FUNCTION_REQUEST, 3, "reverse.pull", Value.list(Value.ofLong(2))));
                raw.next(MessageType.GET_STREAM_REQUEST, -3);
                raw.send(D.streamReady(Value.ofLong(-3)));
                final MapValue credit = raw.next(MessageType.STREAM_CREDIT, -3);
                assertEquals(VrpcServer.DEFAULT_STREAM_WINDOW, D.credit(credit).longValue());
                raw.send(D.streamValue(Value.ofLong(-3), Value.utf8("a")), D.streamEnd(Value.ofLong(-3), Value.utf8("b")));
                assertEquals(Value.list(Value.utf8("a"), Value.utf8("b")), D.result(raw.next(MessageType.FUNCTION_RESPONSE, 3)));

                // A stream to the client waits for the credit of the client, and ends after its last value.
                raw.send(raw.request(MessageType.FUNCTION_REQUEST, 4, "reverse.push", Value.list(Value.utf8("go"))));
                raw.next(MessageType.PUT_STREAM_REQUEST, -4);
                raw.send(D.streamCredit(Value.ofLong(-4), 2), D.streamReady(Value.ofLong(-4)));
                assertEquals(Value.ofLong(100), D.streamValueOf(raw.next(MessageType.STREAM_VALUE, -4)));
                assertEquals(Value.ofLong(200), D.streamValueOf(raw.next(MessageType.STREAM_VALUE, -4)));
                raw.nothing(150);
                raw.send(D.streamCredit(Value.ofLong(-4), 5));
                assertEquals(Value.ofLong(300), D.streamValueOf(raw.next(MessageType.STREAM_VALUE, -4)));
                // The end of the stream and the answer of the call are on two threads: in either order.
                final MapValue one = raw.next();
                final MapValue other = raw.next();
                final MapValue end = D.messageType(one) == MessageType.STREAM_END ? one : other;
                final MapValue answer = end == one ? other : one;
                assertEquals(MessageType.STREAM_END, D.messageType(end));
                assertEquals(-4, D.requestId(end).longValue());
                assertEquals(Value.utf8("pushed"), D.result(answer));
            }
        });
    }

    @Test
    void aClientThatDoesNotTakeAStreamIsNotWaitedForLong() throws Exception {
        overEachTransport((server, wire) -> {
            server.server().addFunction("open", (ctx, args) -> {
                try (ValueStream in = ctx.peer().getStream("tail", null, CallOptions.timeoutMillis(200))) {
                    return Value.utf8("opened");
                } catch (VrpcException e) {
                    return Value.utf8(e.code().name() + ":" + e.detail());
                }
            });
            try (Raw raw = rawOf(server, wire).shake()) {
                raw.send(raw.request(MessageType.FUNCTION_REQUEST, 1, "open", null));
                raw.next(MessageType.GET_STREAM_REQUEST, -1);
                // The client says nothing: the server gives the stream up, tells the client so, and tells who asked.
                raw.next(MessageType.CANCEL_REQUEST, -1);
                assertEquals(Value.utf8("DEADLINE_EXCEEDED:reverse stream -1: client did not ack"),
                        D.result(raw.next(MessageType.FUNCTION_RESPONSE, 1)));
                // What the client says of it afterwards answers nothing, and disturbs nothing.
                raw.send(D.streamReady(Value.ofLong(-1)), D.streamValue(Value.ofLong(-1), Value.utf8("late")));
                raw.send(raw.request(MessageType.FUNCTION_REQUEST, 2, "greet", Value.list(Value.utf8("still"))));
                assertEquals(Value.utf8("Hello, still!"), D.result(raw.next(MessageType.FUNCTION_RESPONSE, 2)));
            }
        });
    }

    @Test
    void whatWasAskedFailsWhenItsClientComesBackOnAnotherConnection() throws Exception {
        overEachTransport((server, wire) -> {
            final AtomicReference<Peer> kept = new AtomicReference<Peer>();
            server.server().addFunction("keep", (ctx, args) -> {
                kept.set(ctx.peer());
                return null;
            });
            final HashChain chain = new HashChain();
            try (Raw first = rawOf(server, wire).shake(chain.anchor(), null)) {
                first.send(first.request(MessageType.FUNCTION_REQUEST, 1, "keep", null));
                first.next(MessageType.FUNCTION_RESPONSE, 1);
                final CompletableFuture<Value> call = kept.get().callFunctionAsync("notify", null, CallOptions.DEFAULT);
                first.next(MessageType.FUNCTION_REQUEST, -1);

                // The same client on another connection, with the next link of its chain: the first is closed.
                try (Raw second = rawOf(server, wire)) {
                    second.send(D.handshakeRequest(first.clientId, chain.next(), null));
                    second.next(MessageType.HANDSHAKE_RESPONSE, 0);
                    final ExecutionException failed = assertThrows(ExecutionException.class, () -> call.get(10, TimeUnit.SECONDS));
                    final VrpcException e = (VrpcException) failed.getCause();
                    assertEquals(Code.UNAVAILABLE, e.code());
                    // In the words of the Go server.
                    assertEquals("connection replaced by reconnect; reverse request cannot complete", e.detail());

                    // What it asks from here on goes to the new connection, numbered on from where it was.
                    final CompletableFuture<Value> again = kept.get().callFunctionAsync("notify", null, CallOptions.DEFAULT);
                    second.next(MessageType.FUNCTION_REQUEST, -2);
                    second.send(D.functionResult(Value.ofLong(-2), Value.utf8("here")));
                    assertEquals(Value.utf8("here"), again.get(10, TimeUnit.SECONDS));
                }
            }
        });
    }

    // --- a client against a server of the test's own ---------------------------------

    @Test
    void aClientTakesNoRequestOfAServerThatIsNotNumberedBelowZero() throws Exception {
        final LinkedBlockingQueue<String> problems = new LinkedBlockingQueue<String>();
        try (FakeServer server = new FakeServer("tcp://127.0.0.1:0");
                VrpcClient client = VrpcClient.builder().address(server.address()).listener(new ClientListener() {
                    @Override
                    public void onProtocolError(MapValue message, String problem) {
                        problems.add(problem);
                    }
                }).build()) {
            server.function("greet", args -> Value.utf8("hello"));
            serve(client);
            client.connect();
            // An id that could be one of the client's own is taken for nothing, and not answered.
            server.push(D.request(MessageType.FUNCTION_REQUEST, 5, "notify", Value.list(Value.utf8("x")), 0, null));
            assertEquals("server-initiated request must use a negative request id", problems.poll(5, TimeUnit.SECONDS));
            server.push(D.request(MessageType.GET_STREAM_REQUEST, 0, "tail", Value.list(Value.ofLong(1)), 0, null));
            assertEquals("server-initiated request must use a negative request id", problems.poll(5, TimeUnit.SECONDS));
            assertNull(server.next(200), "the client was to answer nothing");

            // One that is numbered as it should be is served, whatever went before.
            server.push(D.request(MessageType.FUNCTION_REQUEST, -9, "notify", Value.list(Value.utf8("x")), 0, null));
            MapValue answer = server.next(5000);
            assertEquals(MessageType.FUNCTION_RESPONSE, D.messageType(answer));
            assertEquals(-9, D.requestId(answer).longValue());
            assertEquals(Value.utf8("client-saw:x"), D.result(answer));

            // A stream to the client: the credit comes before the stream is said to be taken.
            server.push(D.request(MessageType.PUT_STREAM_REQUEST, -10, "ingest", null, 0, null));
            answer = server.next(5000);
            assertEquals(MessageType.STREAM_CREDIT, D.messageType(answer));
            assertEquals(VrpcClient.DEFAULT_STREAM_WINDOW, D.credit(answer).longValue());
            assertEquals(MessageType.STREAM_READY, D.messageType(server.next(5000)));
            server.push(D.streamEnd(Value.ofLong(-10), null));

            // A request without a name, and one for a stream that is over: refused, and dropped.
            server.push(MapValue.builder().put(D.messageTypeField(), MessageType.FUNCTION_REQUEST.value())
                    .put(D.requestIdField(), -11).build());
            answer = server.next(5000);
            assertEquals(Code.INVALID_ARGUMENT, D.errorOf(answer).code());
            assertEquals("function name field not found", D.errorOf(answer).detail());
            server.push(D.streamValue(Value.ofLong(-10), Value.utf8("late")));
            assertNull(server.next(200));
            assertEquals(Value.utf8("hello"), client.callFunction("greet", null));
            assertEquals(Kind.STRING, client.callFunction("greet", null).kind());
        }
    }
}
