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

import com.arpabet.value.MapValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.Chat;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.ConformanceServer;
import com.arpabet.valuerpc.Dialect;
import com.arpabet.valuerpc.HashChain;
import com.arpabet.valuerpc.MessageType;
import com.arpabet.valuerpc.Metrics;
import com.arpabet.valuerpc.TypeDef;
import com.arpabet.valuerpc.ValueSink;
import com.arpabet.valuerpc.ValueStream;
import com.arpabet.valuerpc.VrpcException;
import com.arpabet.valuerpc.client.VrpcClient;
import com.arpabet.valuerpc.transport.Listener;
import com.arpabet.valuerpc.transport.MsgConn;
import com.arpabet.valuerpc.transport.TransportOptions;
import com.arpabet.valuerpc.transport.Transports;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The server against the client of this library, over the in-memory
 * transport, over TCP and over WebSocket in both of its codecs, and against a
 * connection of the test's own for what a client never sends and for the
 * order of what a server sends: whether or not a Go toolchain is at hand.
 */
@Timeout(60)
class ServerTest {

    private static final Dialect D = Raw.D;

    /** A scenario, run over each transport against a server with the functions of the conformance server. */
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
                scenario.run(server, wire);
            } catch (AssertionError | Exception e) {
                throw new AssertionError("over " + wire + ": " + e, e);
            }
        }
    }

    private static VrpcClient.Builder clientOf(ConformanceServer server, Wire wire) {
        return VrpcClient.builder().dialer(wire.dialer(server.address())).timeoutMillis(10_000).dialTimeoutMillis(5000);
    }

    private static Raw rawOf(ConformanceServer server, Wire wire) throws IOException {
        return new Raw(wire.dialer(server.address()).dial(5000));
    }

    private static VrpcException failure(Runnable call) {
        return assertThrows(VrpcException.class, call::run);
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

    private static Value list(long... numbers) {
        final Value[] items = new Value[numbers.length];
        for (int i = 0; i < numbers.length; i++) {
            items[i] = Value.ofLong(numbers[i]);
        }
        return Value.list(items);
    }

    // --- calls ---------------------------------------------------------------------

    @Test
    void servesCallsOfEveryOutcome() throws Exception {
        overEachTransport((server, wire) -> {
            try (VrpcClient client = clientOf(server, wire).build()) {
                assertEquals(Value.utf8("Hello, world!"), client.callFunction("greet", Value.list(Value.utf8("world"))));
                assertEquals(Value.ofLong(42), client.callFunction("add", list(40, 2)));
                final Value nested = MapValue.builder()
                        .put("bytes", Value.raw(new byte[] {0, 1, (byte) 254, (byte) 255}))
                        .put("list", Value.list(Value.ofLong(1), Value.utf8("two"), Value.ofNull(), Value.ofBool(true), Value.ofDouble(1.5)))
                        .put("nested", MapValue.builder().put("deep", MapValue.builder().put("key", "value").build()).build())
                        .build();
                assertEquals(nested, client.callFunction("echo", nested));
                assertNull(client.callFunction("nothing", null), "a function that returns nothing");
                assertNull(client.callFunction("echo", null), "the null value is no result");
                assertEquals(Value.utf8("ok"), client.callFunction("typed", Value.list(Value.utf8("a"), Value.ofLong(1))));
                assertEquals(Value.utf8("ok"), client.callFunction("typed", Value.list(Value.utf8("a"), Value.ofNull())),
                        "an argument that may be left out is null");

                // The code a handler chose, and what it said.
                VrpcException e = failure(() -> client.callFunction("fail", null));
                assertEquals(Code.RESOURCE_EXHAUSTED, e.code());
                assertEquals("function fail: deliberate failure", e.detail());
                // Any other failure of a handler is internal, with its message.
                e = failure(() -> client.callFunction("boom", null));
                assertEquals(Code.INTERNAL, e.code());
                assertEquals("function boom: boom", e.detail());
                e = failure(() -> client.callFunction("no.such.function", null));
                assertEquals(Code.NOT_FOUND, e.code());
                assertEquals("function not found no.such.function", e.detail());
                // The arguments are checked before the handler sees them.
                e = failure(() -> client.callFunction("typed", Value.list(Value.ofLong(1))));
                assertEquals(Code.INVALID_ARGUMENT, e.code());
                assertEquals("function 'typed' invalid args", e.detail());
                // A name serves one pattern.
                e = failure(() -> client.getStream("greet", null, CallOptions.DEFAULT));
                assertEquals(Code.INVALID_ARGUMENT, e.code());
                assertEquals("function wrong type greet, expected 0, actual 1", e.detail());
                e = failure(() -> client.callFunction("count", null));
                assertEquals("function wrong type count, expected 1, actual 0", e.detail());
                e = failure(() -> client.putStream("chat.echo", null, CallOptions.DEFAULT));
                assertEquals("function wrong type chat.echo, expected 3, actual 2", e.detail());

                // The connection is none the worse for any of it.
                assertEquals(Value.utf8("Hello, again!"), client.callFunction("greet", Value.list(Value.utf8("again"))));
                assertEquals(1L, server.server().stats().get("connections"));
            }
        });
    }

    @Test
    void aResultIsCheckedAgainstWhatWasRegistered() throws Exception {
        overEachTransport((server, wire) -> {
            server.server().addFunction("number", TypeDef.ANY, TypeDef.NUMBER, (ctx, args) -> args);
            try (VrpcClient client = clientOf(server, wire).build()) {
                assertEquals(Value.ofLong(5), client.callFunction("number", Value.ofLong(5)));
                final VrpcException e = failure(() -> client.callFunction("number", Value.utf8("five")));
                assertEquals(Code.INTERNAL, e.code());
                assertEquals("function 'number' invalid results", e.detail());
            }
        });
    }

    @Test
    void aHandlerIsToldWhoAsksAndWhatTravelsWithTheRequest() throws Exception {
        overEachTransport((server, wire) -> {
            try (VrpcClient client = clientOf(server, wire)
                    .metadata(() -> Collections.singletonMap("traceparent", "00-t")).build()) {
                assertEquals(MapValue.builder().put("traceparent", "00-t").put("extra", "x").build(),
                        client.callFunction("md.echo", null, CallOptions.DEFAULT.withMetadata("extra", "x")));
                assertEquals(Value.utf8(""), client.callFunction("whoami", null), "nobody in particular");
            }
            try (VrpcClient client = clientOf(server, wire).credential(Value.utf8("secret")).build()) {
                assertEquals(Value.utf8("tester"), client.callFunction("whoami", null));
            }
            try (VrpcClient client = clientOf(server, wire).credential(Value.utf8("wrong")).build()) {
                final VrpcException e = failure(client::connect);
                assertEquals(Code.UNAUTHENTICATED, e.code());
                assertEquals("handshake: authentication failed", e.detail(), "why stays with the server");
            }
        });
    }

    @Test
    void theTimeTheCallerAllowsIsTheDeadlineOfTheHandler() throws Exception {
        overEachTransport((server, wire) -> {
            final LinkedBlockingQueue<String> seen = new LinkedBlockingQueue<String>();
            server.server().addFunction("watch", (ctx, args) -> {
                try {
                    Thread.sleep(30_000);
                    return Value.utf8("late");
                } catch (InterruptedException e) {
                    seen.add("interrupted cancelled=" + ctx.isCancelled() + " left=" + ctx.remainingMillis());
                    throw e;
                }
            });
            try (VrpcClient client = clientOf(server, wire).build()) {
                final long left = ((com.arpabet.value.NumberValue) client.callFunction("deadline", null,
                        CallOptions.timeoutMillis(3000))).longValue();
                assertTrue(left > 0 && left <= 3000, "the time left is what the caller allowed: " + left);
                assertEquals(Value.ofLong(-1), client.callFunction("deadline", null, CallOptions.timeoutMillis(-1)),
                        "a caller that names no time");

                final VrpcException e = failure(() -> client.callFunction("watch", null, CallOptions.timeoutMillis(150)));
                assertEquals(Code.DEADLINE_EXCEEDED, e.code());
                assertEquals("interrupted cancelled=true left=0", seen.poll(5, TimeUnit.SECONDS),
                        "the handler is woken when its time is up");
            }
            // What the server itself answers, to a client that waits longer than it said.
            try (Raw raw = rawOf(server, wire).shake()) {
                raw.send(D.request(MessageType.FUNCTION_REQUEST, 1, "slow", Value.ofNull(), 100, null));
                final MapValue answer = raw.next(MessageType.ERROR_RESPONSE, 1);
                assertEquals(Code.DEADLINE_EXCEEDED, D.errorOf(answer).code());
                assertEquals("function slow: context deadline exceeded", D.errorOf(answer).detail());
            }
        });
    }

    @Test
    void aCallThatIsCancelledHasItsHandlerInterruptedAndIsNotAnswered() throws Exception {
        overEachTransport((server, wire) -> {
            final CountDownLatch running = new CountDownLatch(2);
            final LinkedBlockingQueue<String> seen = new LinkedBlockingQueue<String>();
            server.server().addFunction("hang", (ctx, args) -> {
                ctx.onCancel(() -> seen.add("told"));
                running.countDown();
                try {
                    Thread.sleep(30_000);
                    return Value.utf8("late");
                } catch (InterruptedException e) {
                    seen.add("interrupted cancelled=" + ctx.isCancelled());
                    throw e;
                }
            });
            try (VrpcClient client = clientOf(server, wire).build(); Raw raw = rawOf(server, wire).shake()) {
                final CompletableFuture<Value> call = client.callFunctionAsync("hang", null, CallOptions.DEFAULT);
                raw.send(raw.request(MessageType.FUNCTION_REQUEST, 1, "hang", Value.ofNull()));
                assertTrue(running.await(5, TimeUnit.SECONDS));

                call.cancel(true);
                raw.send(D.cancelRequest(Value.ofLong(1)));
                final List<String> all = new ArrayList<String>();
                for (int i = 0; i < 4; i++) {
                    all.add(seen.poll(5, TimeUnit.SECONDS));
                }
                Collections.sort(all);
                assertEquals("[interrupted cancelled=true, interrupted cancelled=true, told, told]", all.toString());
                // A cancel is never answered: the caller has gone.
                raw.nothing(200);
                // And a cancel for a call that is over is nothing.
                raw.send(D.cancelRequest(Value.ofLong(1)), raw.request(MessageType.FUNCTION_REQUEST, 2, "greet", Value.ofNull()));
                raw.next(MessageType.FUNCTION_RESPONSE, 2);
            }
        });
    }

    @Test
    void manyCallsAtOnceEachGetTheirOwnAnswer() throws Exception {
        overEachTransport((server, wire) -> {
            try (VrpcClient client = clientOf(server, wire).build()) {
                final int workers = 8;
                final int each = 300;
                final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<Throwable>();
                final List<Thread> threads = new ArrayList<Thread>();
                for (int w = 0; w < workers; w++) {
                    final int base = w * each;
                    final Thread t = new Thread(() -> {
                        try {
                            for (int i = 0; i < each; i++) {
                                final long n = base + i;
                                assertEquals(Value.ofLong(n), client.callFunction("echo", Value.ofLong(n)));
                            }
                        } catch (Throwable e) {
                            failures.add(e);
                        }
                    });
                    threads.add(t);
                    t.start();
                }
                for (Thread t : threads) {
                    t.join(30_000);
                }
                assertTrue(failures.isEmpty(), () -> String.valueOf(failures.peek()));
                assertTrue(server.server().stats().get("requests") >= workers * each);
            }
        });
    }

    // --- streams -------------------------------------------------------------------

    @Test
    void streamsToTheClientAtThePaceOfTheClient() throws Exception {
        overEachTransport((server, wire) -> {
            // A window of three: the server may never be more than three values ahead.
            try (VrpcClient client = clientOf(server, wire).streamWindow(3).build()) {
                long expected = 1;
                try (ValueStream numbers = client.getStream("count", list(20_000), CallOptions.DEFAULT)) {
                    for (Value v : numbers) {
                        assertEquals(Value.ofLong(expected++), v);
                    }
                }
                assertEquals(20_001, expected, "every value, in order");

                try (ValueStream none = client.getStream("count", list(0), CallOptions.DEFAULT)) {
                    assertNull(none.take(), "a stream may end before it has said anything");
                }
                // A handler that throws before it sends refuses the stream.
                final VrpcException e = failure(() -> client.getStream("count", list(-1), CallOptions.DEFAULT));
                assertEquals(Code.INVALID_ARGUMENT, e.code());
                assertEquals("out stream count: count must not be negative", e.detail());
            }
        });
    }

    @Test
    void aStreamTheClientGivesUpStopsItsHandler() throws Exception {
        overEachTransport((server, wire) -> {
            final LinkedBlockingQueue<String> seen = new LinkedBlockingQueue<String>();
            server.server().addOutgoingStream("forever", TypeDef.ANY, (ctx, args, out) -> {
                try {
                    for (long i = 1;; i++) {
                        out.send(Value.ofLong(i));
                    }
                } catch (VrpcException | InterruptedException e) {
                    seen.add(e.getClass().getSimpleName() + " cancelled=" + ctx.isCancelled());
                    throw e;
                }
            });
            try (VrpcClient client = clientOf(server, wire).streamWindow(8).build()) {
                final ValueStream s = client.getStream("forever", null, CallOptions.DEFAULT);
                assertEquals(Value.ofLong(1), s.take());
                assertEquals(Value.ofLong(2), s.take());
                s.close();
                final String how = seen.poll(5, TimeUnit.SECONDS);
                assertNotNull(how, "the handler was not stopped");
                assertTrue(how.endsWith("cancelled=true"), how);
                // The stream no longer counts, and the connection serves on.
                assertEquals(Value.utf8("Hello, ?!"), client.callFunction("greet", null));
            }
        });
    }

    @Test
    void aStreamThatFailsOnTheWaySaysSo() throws Exception {
        overEachTransport((server, wire) -> {
            server.server().addOutgoingStream("partial", TypeDef.ANY, (ctx, args, out) -> {
                out.send(Value.ofLong(1));
                out.send(Value.ofLong(2));
                throw new VrpcException(Code.UNAVAILABLE, "the source went away");
            });
            try (VrpcClient client = clientOf(server, wire).build();
                    ValueStream s = client.getStream("partial", null, CallOptions.DEFAULT)) {
                final List<Value> got = new ArrayList<Value>();
                final VrpcException e = assertThrows(VrpcException.class, () -> {
                    for (Value v = s.take(); v != null; v = s.take()) {
                        got.add(v);
                    }
                });
                assertEquals(Code.UNAVAILABLE, e.code());
                assertEquals("out stream partial: the source went away", e.detail());
                assertTrue(got.size() <= 2, "what was sent before it failed may have been dropped, and nothing was added");
            }
        });
    }

    @Test
    void takesAStreamFromTheClientAtThePaceOfItsHandler() throws Exception {
        // A window of four on the server: the client may never be more than four values ahead.
        overEachTransport(builder -> builder.streamWindow(4), (server, wire) -> {
            try (VrpcClient client = clientOf(server, wire).build()) {
                long sum = 0;
                try (ValueSink numbers = client.putStream("upload", Value.list(Value.utf8("batch")), CallOptions.DEFAULT)) {
                    for (long i = 1; i <= 5000; i++) {
                        numbers.send(Value.ofLong(i));
                        sum += i;
                    }
                }
                final long want = sum;
                waitFor(() -> {
                    final MapValue stats = (MapValue) client.callFunction("upload.stats", null);
                    return stats.getNumber("count").longValue() == 5000 && stats.getNumber("sum").longValue() == want;
                });
            }
        });
    }

    @Test
    void aHandlerThatReturnsBeforeTheEndOfAStreamTellsTheClientToStop() throws Exception {
        overEachTransport(builder -> builder.streamWindow(4), (server, wire) -> {
            final AtomicLong taken = new AtomicLong();
            server.server().addIncomingStream("first", TypeDef.ANY, (ctx, args, in) -> {
                if (in.take() != null) {
                    taken.incrementAndGet();
                }
            });
            server.server().addIncomingStream("refused", TypeDef.ANY, (ctx, args, in) -> {
                throw new VrpcException(Code.INVALID_ARGUMENT, "not today");
            });
            try (VrpcClient client = clientOf(server, wire).build()) {
                final ValueSink sink = client.putStream("first", null, CallOptions.DEFAULT);
                final VrpcException e = assertThrows(VrpcException.class, () -> {
                    for (long i = 1; i <= 1000; i++) {
                        sink.send(Value.ofLong(i));
                    }
                });
                assertEquals(Code.CANCELED, e.code(), "the server gave the stream up");
                assertEquals(1, taken.get());

                // A handler that throws before it reads refuses the stream.
                final VrpcException refused = failure(() -> client.putStream("refused", null, CallOptions.DEFAULT));
                assertEquals(Code.INVALID_ARGUMENT, refused.code());
                assertEquals("in stream refused: not today", refused.detail());
            }
        });
    }

    @Test
    void chatsBothWaysAndGoesOnWhenTheClientHasSaidItsLast() throws Exception {
        overEachTransport(builder -> builder.streamWindow(16), (server, wire) -> {
            try (VrpcClient client = clientOf(server, wire).streamWindow(16).build()) {
                try (Chat chat = client.chat("chat.echo", Value.list(Value.utf8("room")), CallOptions.DEFAULT)) {
                    chat.outgoing().send(Value.utf8("one"));
                    chat.outgoing().send(Value.utf8("two"));
                    chat.outgoing().send(Value.utf8("three"));
                    chat.outgoing().close();
                    final List<Value> got = new ArrayList<Value>();
                    for (Value v : chat.incoming()) {
                        got.add(v);
                    }
                    assertEquals(Value.list(Value.utf8("echo:one"), Value.utf8("echo:two"), Value.utf8("echo:three")),
                            Value.listOf(got));
                }
                // Many values each way, with windows of sixteen: a writer and a reader at once.
                final int n = 4000;
                try (Chat chat = client.chat("chat.echo", null, CallOptions.DEFAULT)) {
                    final Thread writer = new Thread(() -> {
                        try {
                            for (int i = 0; i < n; i++) {
                                chat.outgoing().send(Value.ofLong(i));
                            }
                            chat.outgoing().close();
                        } catch (InterruptedException | RuntimeException e) {
                            // the reader misses the values
                        }
                    });
                    writer.start();
                    int i = 0;
                    for (Value v : chat.incoming()) {
                        assertEquals(Value.utf8("echo:" + i++), v);
                    }
                    assertEquals(n, i);
                    writer.join(10_000);
                }
                // A handler that ends its side ends the chat, whatever the client still has to say.
                try (Chat chat = client.chat("chat.sink", null, CallOptions.DEFAULT)) {
                    assertNull(chat.incoming().take());
                }
            }
        });
    }

    @Test
    void aHandlerMayEndItsStreamBeforeItReturns() throws Exception {
        overEachTransport((server, wire) -> {
            final CountDownLatch release = new CountDownLatch(1);
            server.server().addOutgoingStream("two", TypeDef.ANY, (ctx, args, out) -> {
                out.send(Value.ofLong(1));
                out.send(Value.ofLong(2));
                out.close();
                out.close(); // closing twice is closing once
                assertThrows(VrpcException.class, () -> out.send(Value.ofLong(3)));
                release.await(10, TimeUnit.SECONDS);
            });
            try (VrpcClient client = clientOf(server, wire).build();
                    ValueStream s = client.getStream("two", null, CallOptions.DEFAULT)) {
                assertEquals(Value.ofLong(1), s.take());
                assertEquals(Value.ofLong(2), s.take());
                assertNull(s.take(5, TimeUnit.SECONDS), "the stream ended while its handler was still there");
            } finally {
                release.countDown();
            }
        });
    }

    // --- the order of what a server sends ----------------------------------------------

    @Test
    void aStreamToTheClientIsReadyFirstAndSendsOnlyWhatItHasCreditFor() throws Exception {
        overEachTransport((server, wire) -> {
            try (Raw raw = rawOf(server, wire).shake()) {
                raw.send(raw.request(MessageType.GET_STREAM_REQUEST, 1, "count", list(5)));
                raw.next(MessageType.STREAM_READY, 1);
                raw.nothing(150);
                raw.send(D.streamCredit(Value.ofLong(1), 2));
                assertEquals(Value.ofLong(1), D.streamValueOf(raw.next(MessageType.STREAM_VALUE, 1)));
                assertEquals(Value.ofLong(2), D.streamValueOf(raw.next(MessageType.STREAM_VALUE, 1)));
                raw.nothing(150);
                raw.send(D.streamCredit(Value.ofLong(1), 100));
                for (long i = 3; i <= 5; i++) {
                    assertEquals(Value.ofLong(i), D.streamValueOf(raw.next(MessageType.STREAM_VALUE, 1)));
                }
                assertEquals(D.streamEnd(Value.ofLong(1), null), raw.next(MessageType.STREAM_END, 1));
            }
        });
    }

    @Test
    void aStreamFromTheClientIsGrantedCreditBeforeItIsReadyAndMoreAsItIsTaken() throws Exception {
        overEachTransport(builder -> builder.streamWindow(4), (server, wire) -> {
            try (Raw raw = rawOf(server, wire).shake()) {
                raw.send(raw.request(MessageType.PUT_STREAM_REQUEST, 1, "upload", Value.ofNull()));
                assertEquals(4, D.credit(raw.next(MessageType.STREAM_CREDIT, 1)).longValue(), "the window, first");
                raw.next(MessageType.STREAM_READY, 1);
                long granted = 4;
                long sent = 0;
                while (sent < 40) {
                    while (sent < granted && sent < 40) {
                        raw.send(D.streamValue(Value.ofLong(1), Value.ofLong(++sent)));
                    }
                    if (sent < 40) {
                        // Granted again in halves of the window, as the handler takes values.
                        granted += D.credit(raw.next(MessageType.STREAM_CREDIT, 1)).longValue();
                    }
                }
                raw.send(D.streamEnd(Value.ofLong(1), null));
                waitFor(() -> server.server().stats().get("requests") >= 1);
                // The end of what the client sends ends the stream: nothing answers it.
                raw.send(raw.request(MessageType.FUNCTION_REQUEST, 2, "upload.stats", Value.ofNull()));
                MapValue stats = null;
                for (int i = 0; i < 200 && stats == null; i++) {
                    final MapValue m = raw.next();
                    if (D.messageType(m) == MessageType.STREAM_CREDIT) {
                        continue; // a grant that was on its way
                    }
                    assertEquals(MessageType.FUNCTION_RESPONSE, D.messageType(m), () -> String.valueOf(m));
                    final MapValue s = (MapValue) D.result(m);
                    if (s.getNumber("count").longValue() == 40) {
                        stats = s;
                    } else {
                        Thread.sleep(10);
                        raw.send(raw.request(MessageType.FUNCTION_REQUEST, 2 + i + 1, "upload.stats", Value.ofNull()));
                    }
                }
                assertNotNull(stats);
                assertEquals(820, stats.getNumber("sum").longValue());
            }
        });
    }

    @Test
    void aClientThatSendsMoreThanItWasGrantedHasItsStreamCutShort() throws Exception {
        overEachTransport(builder -> builder.streamWindow(4), (server, wire) -> {
            final CountDownLatch hold = new CountDownLatch(1);
            final LinkedBlockingQueue<String> seen = new LinkedBlockingQueue<String>();
            server.server().addIncomingStream("stuck", TypeDef.ANY, (ctx, args, in) -> {
                try {
                    in.take();
                    hold.await();
                    in.take();
                } catch (VrpcException | InterruptedException e) {
                    seen.add(e.getClass().getSimpleName());
                }
            });
            try (Raw raw = rawOf(server, wire).shake()) {
                raw.send(raw.request(MessageType.PUT_STREAM_REQUEST, 1, "stuck", Value.ofNull()));
                raw.next(MessageType.STREAM_CREDIT, 1);
                raw.next(MessageType.STREAM_READY, 1);
                // One value is taken, four fill the window, and the sixth is one too many; on a machine too
                // busy to take the first in time, the fifth is.
                for (long i = 1; i <= 6; i++) {
                    raw.send(D.streamValue(Value.ofLong(1), Value.ofLong(i)));
                    if (i == 1) {
                        Thread.sleep(100);
                    }
                }
                final MapValue answer = raw.next(MessageType.ERROR_RESPONSE, 1);
                assertEquals(Code.RESOURCE_EXHAUSTED, D.errorOf(answer).code());
                assertEquals("inbound stream 1 truncated: client exceeded flow-control credit", D.errorOf(answer).detail());
                assertNotNull(seen.poll(5, TimeUnit.SECONDS), "the handler is told as well");
            } finally {
                hold.countDown();
            }
        });
    }

    @Test
    void whatIsNotARequestOfAClientIsRefusedOrDropped() throws Exception {
        overEachTransport((server, wire) -> {
            try (Raw raw = rawOf(server, wire).shake()) {
                // The ids of a client are positive: 0 is the handshake, and the negative ones are the server's.
                raw.send(raw.request(MessageType.FUNCTION_REQUEST, -5, "greet", Value.ofNull()));
                MapValue answer = raw.next(MessageType.ERROR_RESPONSE, -5);
                assertEquals(Code.INVALID_ARGUMENT, D.errorOf(answer).code());
                assertEquals("client-initiated request id must be positive", D.errorOf(answer).detail());
                raw.send(raw.request(MessageType.FUNCTION_REQUEST, 0, "greet", Value.ofNull()));
                raw.next(MessageType.ERROR_RESPONSE, 0);

                // A request without a name.
                raw.send(MapValue.builder().put("t", MessageType.FUNCTION_REQUEST.value()).put("rid", 3).build());
                answer = raw.next(MessageType.ERROR_RESPONSE, 3);
                assertEquals(Code.INVALID_ARGUMENT, D.errorOf(answer).code());
                assertEquals("function name field not found", D.errorOf(answer).detail());

                // What belongs to nothing is dropped, and the connection serves on.
                raw.send(MapValue.builder().put("rid", 4).build(),
                        MapValue.builder().put("t", MessageType.FUNCTION_REQUEST.value()).build(),
                        MapValue.builder().put("t", 99).put("rid", 5).build(),
                        D.streamValue(Value.ofLong(77), Value.ofLong(1)),
                        D.streamCredit(Value.ofLong(78), 5),
                        D.functionResult(Value.ofLong(-9), Value.ofLong(1)),
                        raw.request(MessageType.FUNCTION_REQUEST, 6, "greet", Value.list(Value.utf8("still"))));
                assertEquals(Value.utf8("Hello, still!"), D.result(raw.next(MessageType.FUNCTION_RESPONSE, 6)));
            }
        });
    }

    // --- limits --------------------------------------------------------------------

    @Test
    void oneRequestTooManyIsRefusedAndNotQueued() throws Exception {
        overEachTransport(builder -> builder.maxConcurrentRequests(2).maxConcurrentStreams(1), (server, wire) -> {
            final CountDownLatch release = new CountDownLatch(1);
            final CountDownLatch held = new CountDownLatch(2);
            server.server().addFunction("hold", (ctx, args) -> {
                held.countDown();
                release.await(20, TimeUnit.SECONDS);
                return args;
            });
            server.server().addOutgoingStream("open", TypeDef.ANY, (ctx, args, out) -> {
                out.send(Value.ofLong(1));
                Thread.sleep(30_000);
            });
            try (VrpcClient client = clientOf(server, wire).build()) {
                final CompletableFuture<Value> a = client.callFunctionAsync("hold", Value.ofLong(1), CallOptions.DEFAULT);
                final CompletableFuture<Value> b = client.callFunctionAsync("hold", Value.ofLong(2), CallOptions.DEFAULT);
                assertTrue(held.await(5, TimeUnit.SECONDS));
                VrpcException e = failure(() -> client.callFunction("greet", null));
                assertEquals(Code.RESOURCE_EXHAUSTED, e.code());
                assertEquals("server busy: too many concurrent requests (max 2)", e.detail());
                release.countDown();
                assertEquals(Value.ofLong(1), a.get(5, TimeUnit.SECONDS));
                assertEquals(Value.ofLong(2), b.get(5, TimeUnit.SECONDS));

                // A stream that is taken no longer holds the slot of a handler,
                // and counts among the open streams until it ends.
                final ValueStream first = client.getStream("open", null, CallOptions.DEFAULT);
                assertEquals(Value.ofLong(1), first.take());
                e = failure(() -> client.getStream("open", null, CallOptions.DEFAULT));
                assertEquals(Code.RESOURCE_EXHAUSTED, e.code());
                assertEquals("server busy: too many concurrent streams (max 1)", e.detail());
                assertEquals(Value.ofLong(3), client.callFunction("add", list(1, 2)));
                assertEquals(Value.ofLong(3), client.callFunction("add", list(2, 1)));
                first.close();
                // Once the first has ended there is room for another.
                waitFor(() -> {
                    try {
                        client.getStream("open", null, CallOptions.DEFAULT).close();
                        return true;
                    } catch (VrpcException busy) {
                        return false;
                    }
                });
            } finally {
                release.countDown();
            }
        });
    }

    @Test
    void oneConnectionTooManyIsClosedAtOnce() throws Exception {
        overEachTransport(builder -> builder.maxConnections(1), (server, wire) -> {
            try (VrpcClient first = clientOf(server, wire).build()) {
                first.connect();
                try (VrpcClient second = clientOf(server, wire).build()) {
                    assertEquals(Code.UNAVAILABLE, failure(second::connect).code());
                }
                assertEquals(Value.utf8("Hello, ?!"), first.callFunction("greet", null), "the one that is there is served on");
            }
            try (VrpcClient third = clientOf(server, wire).build()) {
                waitFor(() -> {
                    try {
                        third.connect();
                        return true;
                    } catch (VrpcException full) {
                        return false;
                    }
                });
            }
        });
    }

    // --- the handshake -----------------------------------------------------------------

    @Test
    void whatIsNotAHandshakeIsAnsweredWithSilence() throws Exception {
        overEachTransport((server, wire) -> {
            // A request where the handshake belongs.
            try (Raw raw = rawOf(server, wire)) {
                raw.send(raw.request(MessageType.FUNCTION_REQUEST, 1, "greet", Value.ofNull()));
                raw.closedByServer();
            }
            // Another magic.
            try (Raw raw = rawOf(server, wire)) {
                raw.send(D.toBuilder().magic("vRPX").build().handshakeRequest(raw.clientId, "", null));
                raw.closedByServer();
            }
            // A version of the future.
            try (Raw raw = rawOf(server, wire)) {
                raw.send(D.toBuilder().version(2.0).build().handshakeRequest(raw.clientId, "", null));
                raw.closedByServer();
            }
            // From here on a client has shown that it speaks the protocol, and is told why it is refused.
            try (Raw raw = rawOf(server, wire)) {
                raw.send(MapValue.builder().put("m", "vRPC").put("v", 1.0).put("t", 0).put("rid", 0).build());
                final MapValue answer = raw.next(MessageType.ERROR_RESPONSE, 0);
                assertEquals(Code.INVALID_ARGUMENT, D.errorOf(answer).code());
                assertEquals("malformed handshake", D.errorOf(answer).detail());
                raw.closedByServer();
            }
            // The version as JSON writes it, a whole number, is the same version.
            try (Raw raw = rawOf(server, wire)) {
                raw.send(MapValue.builder().put("m", "vRPC").put("v", 1).put("t", 0).put("rid", 0).put("cid", raw.clientId).build());
                raw.next(MessageType.HANDSHAKE_RESPONSE, 0);
            }
            assertEquals(Value.utf8("Hello, ?!"), callOnce(server, wire, "greet"));
        });
    }

    private static Value callOnce(ConformanceServer server, Wire wire, String name) {
        try (VrpcClient client = clientOf(server, wire).build()) {
            return client.callFunction(name, null);
        }
    }

    @Test
    void aConnectionThatSendsNoHandshakeIsClosed() throws Exception {
        overEachTransport(builder -> builder.handshakeTimeoutMillis(150), (server, wire) -> {
            try (Raw raw = rawOf(server, wire)) {
                final long started = System.nanoTime();
                raw.closedByServer();
                final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
                assertTrue(millis >= 100 && millis < 5000, "after the time it was given: " + millis + " ms");
            }
            // One that has shaken hands may be silent for as long as it likes.
            try (Raw raw = rawOf(server, wire).shake()) {
                Thread.sleep(400);
                assertFalse(raw.isClosed());
                raw.send(raw.request(MessageType.FUNCTION_REQUEST, 1, "greet", Value.ofNull()));
                raw.next(MessageType.FUNCTION_RESPONSE, 1);
            }
        });
    }

    @Test
    void whatAClientAsksBehindItsHandshakeIsServedInItsOrder() throws Exception {
        for (Wire wire : Wire.all()) {
            final VrpcServer server = VrpcServer.builder().address(wire.listen()).transportOptions(wire.options())
                    .authenticator((conn, credential) -> {
                        Thread.sleep(200); // an authenticator that asks someone
                        return "slowly";
                    })
                    .build();
            server.addFunction("whoami", (ctx, args) -> Value.utf8(ctx.principal() + ":" + args));
            server.start();
            try (Raw raw = new Raw(wire.dialer(server.address()).dial(5000))) {
                raw.send(D.handshakeRequest(raw.clientId, "", null),
                        raw.request(MessageType.FUNCTION_REQUEST, 1, "whoami", Value.ofLong(1)),
                        raw.request(MessageType.FUNCTION_REQUEST, 2, "whoami", Value.ofLong(2)));
                raw.next(MessageType.HANDSHAKE_RESPONSE, 0);
                final List<String> answers = new ArrayList<String>();
                answers.add(String.valueOf(D.result(raw.next())));
                answers.add(String.valueOf(D.result(raw.next())));
                Collections.sort(answers);
                assertEquals("[slowly:1, slowly:2]", answers.toString());
            } finally {
                server.close();
            }
        }
    }

    // --- sessions ------------------------------------------------------------------

    /** What a server tells its listener, as words to compare. */
    private static final class Told implements ServerListener {
        final LinkedBlockingQueue<String> events = new LinkedBlockingQueue<String>();

        @Override
        public void onConnected(long clientId, String principal, String remoteAddress, boolean resumed) {
            events.add((resumed ? "resumed " : "connected ") + clientId);
        }

        @Override
        public void onDisconnected(long clientId, String remoteAddress, Throwable cause) {
            events.add("disconnected " + clientId);
        }

        @Override
        public void onRefused(String remoteAddress, String reason, Throwable cause) {
            events.add("refused: " + reason);
        }

        String next() throws InterruptedException {
            return events.poll(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void aClientThatReconnectsIsTheSameSession() throws Exception {
        final Told told = new Told();
        overEachTransport(builder -> builder.listener(told), (server, wire) -> {
            told.events.clear();
            try (VrpcClient client = clientOf(server, wire).build()) {
                assertEquals(Value.utf8("Hello, ?!"), client.callFunction("greet", null));
                assertEquals("connected " + client.clientId(), told.next());
                server.drop();
                assertEquals("disconnected " + client.clientId(), told.next());
                // The client connects again by itself, and shows the next link of its chain.
                assertEquals("resumed " + client.clientId(), told.next());
                assertEquals(Value.utf8("Hello, ?!"), client.callFunction("greet", null));
                assertEquals(1L, server.server().stats().get("sessions"));
                assertEquals(1L, server.server().stats().get("connections"));
            }
        });
    }

    @Test
    void anIdAloneTakesOverNobodysSession() throws Exception {
        overEachTransport((server, wire) -> {
            final HashChain chain = new HashChain(16);
            final long cid;
            try (Raw first = rawOf(server, wire).shake(chain.anchor(), Value.utf8("secret"))) {
                cid = first.clientId;
                first.send(first.request(MessageType.FUNCTION_REQUEST, 1, "whoami", Value.ofNull()));
                assertEquals(Value.utf8("tester"), D.result(first.next(MessageType.FUNCTION_RESPONSE, 1)));

                // Someone who knows the id and no link.
                refusedResume(rawOf(server, wire), cid, "", Value.utf8("secret"));
                refusedResume(rawOf(server, wire), cid, new HashChain(4).anchor(), Value.utf8("secret"));
                // The right link, shown by someone else: the session is its principal's.
                final String link = chain.next();
                refusedResume(rawOf(server, wire), cid, link, null);
                assertFalse(first.isClosed(), "the client of the session was not disturbed");

                // The client itself, on a new connection: the one before is closed, and it is let in.
                try (Raw again = rawOf(server, wire)) {
                    again.send(D.handshakeRequest(cid, chain.next(), Value.utf8("secret")));
                    again.next(MessageType.HANDSHAKE_RESPONSE, 0);
                    waitFor(first::isClosed);
                    // A link that was shown is worth nothing to who saw it.
                    refusedResume(rawOf(server, wire), cid, link, Value.utf8("secret"));
                    again.send(again.request(MessageType.FUNCTION_REQUEST, 1, "whoami", Value.ofNull()));
                    assertEquals(Value.utf8("tester"), D.result(again.next(MessageType.FUNCTION_RESPONSE, 1)));
                }
            }
            assertEquals(1L, server.server().stats().get("sessions"));
        });
    }

    private static void refusedResume(Raw raw, long cid, String token, Value credential) throws Exception {
        try (Raw r = raw) {
            r.send(D.handshakeRequest(cid, token, credential));
            final MapValue answer = r.next(MessageType.ERROR_RESPONSE, 0);
            assertEquals(Code.UNAUTHENTICATED, D.errorOf(answer).code());
            assertEquals("session resume rejected", D.errorOf(answer).detail());
            r.closedByServer();
        }
    }

    @Test
    void theSessionOfAClientThatDoesNotComeBackIsDropped() throws Exception {
        overEachTransport(builder -> builder.sessionRetentionMillis(1500), (server, wire) -> {
            try (VrpcClient client = clientOf(server, wire).build()) {
                client.connect();
                assertEquals(1L, server.server().stats().get("sessions"));
            }
            waitFor(() -> server.server().stats().get("connections") == 0);
            assertEquals(1L, server.server().stats().get("sessions"), "kept for a while, in case it comes back");
            waitFor(() -> server.server().stats().get("sessions") == 0);
        });
    }

    // --- the end of a connection, and of a server ----------------------------------------

    @Test
    void aLostConnectionCancelsWhatWasAskedOnIt() throws Exception {
        overEachTransport((server, wire) -> {
            final CountDownLatch running = new CountDownLatch(2);
            final LinkedBlockingQueue<String> seen = new LinkedBlockingQueue<String>();
            server.server().addFunction("hang", (ctx, args) -> {
                running.countDown();
                try {
                    Thread.sleep(30_000);
                    return null;
                } catch (InterruptedException e) {
                    seen.add("call cancelled=" + ctx.isCancelled());
                    throw e;
                }
            });
            server.server().addIncomingStream("wait", TypeDef.ANY, (ctx, args, in) -> {
                running.countDown();
                try {
                    in.take();
                    seen.add("stream ended");
                } catch (VrpcException e) {
                    seen.add("stream " + e.code() + " cancelled=" + ctx.isCancelled());
                }
            });
            final VrpcClient client = clientOf(server, wire).build();
            client.callFunctionAsync("hang", null, CallOptions.DEFAULT);
            client.putStream("wait", null, CallOptions.DEFAULT);
            assertTrue(running.await(5, TimeUnit.SECONDS));
            client.close();
            final List<String> all = new ArrayList<String>();
            all.add(seen.poll(5, TimeUnit.SECONDS));
            all.add(seen.poll(5, TimeUnit.SECONDS));
            Collections.sort(all);
            assertEquals("[call cancelled=true, stream Unavailable cancelled=true]", all.toString());
            waitFor(() -> server.server().stats().get("connections") == 0);
        });
    }

    /** A listener whose connections take their time to write: a peer that reads slowly. */
    private static final class SlowListener implements Listener {
        private final Listener inner;
        private final long millis;

        SlowListener(Listener inner, long millis) {
            this.inner = inner;
            this.millis = millis;
        }

        @Override
        public void start(Acceptor acceptor) {
            inner.start(conn -> acceptor.onConnection(new MsgConn() {
                @Override
                public void start(Receiver receiver) {
                    conn.start(receiver);
                }

                @Override
                public void write(List<MapValue> batch) throws IOException {
                    try {
                        Thread.sleep(millis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    conn.write(batch);
                }

                @Override
                public String remoteAddress() {
                    return conn.remoteAddress();
                }

                @Override
                public void close() {
                    conn.close();
                }
            }));
        }

        @Override
        public String address() {
            return inner.address();
        }

        @Override
        public void close() {
            inner.close();
        }
    }

    @Test
    void closingWritesTheAnswersThatAreQueuedBeforeItClosesTheConnections() throws Exception {
        for (Wire wire : Wire.all()) {
            // With the wait, a write that takes a fifth of a second; without
            // it, one that takes two, so that the close finds it under way
            // however busy the machine is.
            for (long flush : new long[] {10_000, 0}) {
                final AtomicInteger returned = new AtomicInteger();
                final VrpcServer server = VrpcServer.builder()
                        .listen(new SlowListener(Transports.listener(wire.listen(), wire.options()), flush > 0 ? 200 : 2000))
                        .closeFlushMillis(flush)
                        .metrics(new Metrics() {
                            @Override
                            public void requestEnd(String method, Code code, long elapsedNanos) {
                                returned.incrementAndGet();
                            }
                        })
                        .build();
                server.addFunction("echo", (ctx, args) -> args);
                server.start();
                try (VrpcClient client = VrpcClient.builder().dialer(wire.dialer(server.address())).timeoutMillis(10_000).build()) {
                    client.connect();
                    final List<CompletableFuture<Value>> calls = new ArrayList<CompletableFuture<Value>>();
                    for (int i = 0; i < 20; i++) {
                        calls.add(client.callFunctionAsync("echo", Value.ofLong(i), CallOptions.DEFAULT));
                    }
                    // Every handler has returned, and its answer waits behind a write that takes its time.
                    waitFor(() -> returned.get() == 20);
                    Thread.sleep(200);
                    server.close();
                    int answered = 0;
                    for (int i = 0; i < calls.size(); i++) {
                        try {
                            assertEquals(Value.ofLong(i), calls.get(i).get(10, TimeUnit.SECONDS));
                            answered++;
                        } catch (ExecutionException e) {
                            assertEquals(Code.UNAVAILABLE, Code.of(e.getCause()));
                        }
                    }
                    if (flush > 0) {
                        assertEquals(20, answered, "over " + wire + ": a call the server had answered was lost with its connection");
                    } else {
                        assertTrue(answered < 20, "over " + wire + ": without the wait, what is queued is dropped");
                    }
                } finally {
                    server.close();
                }
            }
        }
    }

    @Test
    void closingCancelsWhatStillRuns() throws Exception {
        overEachTransport((server, wire) -> {
            final CountDownLatch running = new CountDownLatch(1);
            final LinkedBlockingQueue<String> seen = new LinkedBlockingQueue<String>();
            server.server().addFunction("hang", (ctx, args) -> {
                running.countDown();
                try {
                    Thread.sleep(30_000);
                    return null;
                } catch (InterruptedException e) {
                    seen.add("cancelled=" + ctx.isCancelled());
                    throw e;
                }
            });
            try (VrpcClient client = clientOf(server, wire).build()) {
                final CompletableFuture<Value> call = client.callFunctionAsync("hang", null, CallOptions.DEFAULT);
                assertTrue(running.await(5, TimeUnit.SECONDS));
                server.close();
                assertEquals("cancelled=true", seen.poll(5, TimeUnit.SECONDS));
                final ExecutionException e = assertThrows(ExecutionException.class, () -> call.get(5, TimeUnit.SECONDS));
                assertEquals(Code.UNAVAILABLE, Code.of(e.getCause()));
                server.close(); // closing twice is closing once
                assertEquals(0L, server.server().stats().get("connections"));
            }
        });
    }

    // --- what it is told and tells ---------------------------------------------------------

    @Test
    void reportsItsRequestsToTheMetrics() throws Exception {
        final ConcurrentLinkedQueue<String> seen = new ConcurrentLinkedQueue<String>();
        final AtomicLong values = new AtomicLong();
        final Metrics metrics = new Metrics() {
            @Override
            public void requestBegin(String method) {
                seen.add("begin " + method);
            }

            @Override
            public void requestEnd(String method, Code code, long elapsedNanos) {
                seen.add("end " + method + " " + code);
            }

            @Override
            public void streamValue(String method) {
                values.incrementAndGet();
            }
        };
        overEachTransport(builder -> builder.metrics(metrics), (server, wire) -> {
            seen.clear();
            values.set(0);
            try (VrpcClient client = clientOf(server, wire).build()) {
                client.callFunction("greet", null);
                failure(() -> client.callFunction("fail", null));
                failure(() -> client.callFunction("nowhere", null));
                try (ValueStream s = client.getStream("count", list(3), CallOptions.DEFAULT)) {
                    for (Value v : s) {
                        assertNotNull(v);
                    }
                }
                waitFor(() -> seen.size() == 8);
                assertEquals("[begin greet, end greet OK, begin fail, end fail ResourceExhausted, begin nowhere, "
                        + "end nowhere NotFound, begin count, end count OK]", seen.toString());
                assertEquals(3, values.get());
            }
        });
    }

    @Test
    void aNameIsServedOnceAndAServerStartsOnce() throws Exception {
        final VrpcServer server = VrpcServer.builder().address(Wire.MEM.listen()).build();
        try {
            server.addFunction("one", (ctx, args) -> args);
            assertThrows(IllegalStateException.class, () -> server.addFunction("one", (ctx, args) -> args));
            assertThrows(IllegalStateException.class,
                    () -> server.addOutgoingStream("one", TypeDef.ANY, (ctx, args, out) -> { }));
            assertThrows(IllegalArgumentException.class, () -> server.addFunction("", (ctx, args) -> args));
            assertThrows(IllegalArgumentException.class, () -> server.addFunction("none", null));
            server.start();
            assertThrows(IllegalStateException.class, server::start);
            // What is to be served may be added while the server runs.
            server.addFunction("two", (ctx, args) -> Value.ofLong(2));
            try (VrpcClient client = VrpcClient.builder().address(server.address()).build()) {
                assertEquals(Value.ofLong(2), client.callFunction("two", null));
            }
        } finally {
            server.close();
        }
        assertThrows(IllegalStateException.class, () -> VrpcServer.builder().build());
    }
}
