/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.value.MapValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.ClientInterceptor;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.Dialect;
import com.arpabet.valuerpc.FakeServer;
import com.arpabet.valuerpc.HashChain;
import com.arpabet.valuerpc.MessageType;
import com.arpabet.valuerpc.Metrics;
import com.arpabet.valuerpc.VrpcException;
import com.arpabet.valuerpc.transport.TransportOptions;
import com.arpabet.valuerpc.transport.Transports;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The client against a server of the test's own ({@link FakeServer}), over
 * the in-memory transport and over TCP: what it sends, and what it does with
 * what it is sent, whether or not a Go toolchain is at hand.
 */
@Timeout(30)
class ClientTest {

    private static final Dialect D = Dialect.standard();
    private static final AtomicInteger NAMES = new AtomicInteger();

    /** A scenario, run over each transport. */
    private interface Scenario {
        void run(FakeServer server) throws Exception;
    }

    private static void overEachTransport(Scenario scenario) throws Exception {
        for (String address : new String[] {"mem://client-test-" + NAMES.incrementAndGet(), "tcp://127.0.0.1:0"}) {
            try (FakeServer server = new FakeServer(address)) {
                scenario.run(server);
            }
        }
    }

    private static VrpcClient.Builder clientOf(FakeServer server) {
        return VrpcClient.builder().address(server.address()).timeoutMillis(5000).dialTimeoutMillis(5000);
    }

    private static VrpcException failure(Runnable call) {
        return assertThrows(VrpcException.class, call::run);
    }

    private static void waitFor(java.util.function.BooleanSupplier what) throws InterruptedException {
        final long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!what.getAsBoolean()) {
            if (System.nanoTime() > until) {
                throw new AssertionError("waited too long");
            }
            Thread.sleep(2);
        }
    }

    @Test
    void connectsWhenFirstUsedAndCalls() throws Exception {
        overEachTransport(server -> {
            try (VrpcClient client = clientOf(server).build()) {
                assertFalse(client.isActive(), "nothing is dialed before it is needed");
                assertEquals(Value.utf8("Hello, world!"), client.callFunction("greet", Value.list(Value.utf8("world"))));
                assertTrue(client.isActive());
                assertEquals(1, server.connections());
                assertEquals(Value.ofLong(7), client.callFunction("echo", Value.ofLong(7)));
                assertNull(client.callFunction("echo", null), "a null result is no result");
                assertEquals(1, server.connections(), "one connection carries every call");
            }
        });
    }

    @Test
    void whatARequestLooksLikeOnTheWire() throws Exception {
        overEachTransport(server -> {
            try (VrpcClient client = clientOf(server).timeoutMillis(1500)
                    .metadata(() -> Collections.singletonMap("traceparent", "00-t")).build()) {
                client.callFunction("echo", null);
                client.callFunction("echo", Value.list(Value.ofLong(1)), CallOptions.timeoutMillis(250).withMetadata("extra", "x"));
                client.callFunction("echo", Value.utf8("no deadline"), CallOptions.timeoutMillis(-1));

                final MapValue hs = server.handshakes.get(0);
                assertEquals(D.handshakeRequest(client.clientId(), hs.getString("tok").toString(), null), hs);
                assertEquals(64, hs.getString("tok").toString().length(), "the anchor of the hash chain");

                // The ids are 1, 2, 3; the arguments are always there, null when
                // there are none; the timeout travels as the deadline.
                assertEquals(D.request(MessageType.FUNCTION_REQUEST, 1, "echo", Value.ofNull(), 1500,
                        Collections.singletonMap("traceparent", "00-t")), server.next(2000));
                final MapValue second = server.next(2000);
                assertEquals(2, D.requestId(second).longValue());
                assertEquals(250, D.slaMillis(second));
                assertEquals(2, D.metadata(second).size());
                assertEquals("x", D.metadata(second).get("extra"));
                final MapValue third = server.next(2000);
                assertEquals(3, D.requestId(third).longValue());
                assertFalse(third.containsKey("sla"), "no timeout, no deadline");
            }
        });
    }

    @Test
    void errorsComeBackWithTheirCode() throws Exception {
        overEachTransport(server -> {
            try (VrpcClient client = clientOf(server).build()) {
                final VrpcException e = failure(() -> client.callFunction("fail", null));
                assertSame(Code.RESOURCE_EXHAUSTED, e.code());
                assertEquals("function fail: deliberate failure", e.detail());
                assertSame(Code.NOT_FOUND, failure(() -> client.callFunction("nope", null)).code());

                final CompletableFuture<Value> async = client.callFunctionAsync("fail", null, null);
                final Throwable cause = assertThrows(ExecutionException.class, () -> async.get(5, TimeUnit.SECONDS)).getCause();
                assertSame(Code.RESOURCE_EXHAUSTED, ((VrpcException) cause).code());
            }
        });
    }

    @Test
    void aTimeoutTellsTheServerToStop() throws Exception {
        overEachTransport(server -> {
            server.mute = true;
            try (VrpcClient client = clientOf(server).build()) {
                final long start = System.nanoTime();
                final VrpcException e = failure(() -> client.callFunction("greet", null, CallOptions.timeoutMillis(100)));
                assertSame(Code.DEADLINE_EXCEEDED, e.code());
                assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(3));
                assertSame(MessageType.FUNCTION_REQUEST, D.messageType(server.next(2000)));
                assertEquals(D.cancelRequest(Value.ofLong(1)), server.next(2000), "the server is told, with the id of the call");
                assertEquals(0L, client.stats().get("pending").longValue(), "and the call is retired here");

                // Cancelling says the same.
                final CompletableFuture<Value> f = client.callFunctionAsync("greet", null, null);
                assertSame(MessageType.FUNCTION_REQUEST, D.messageType(server.next(2000)));
                f.cancel(true);
                assertEquals(D.cancelRequest(Value.ofLong(2)), server.next(2000));
            }
        });
    }

    @Test
    void theServerMayCancelACall() throws Exception {
        overEachTransport(server -> {
            server.mute = true;
            try (VrpcClient client = clientOf(server).build()) {
                final CompletableFuture<Value> f = client.callFunctionAsync("greet", null, null);
                assertNotNull(server.next(2000));
                server.push(D.cancelRequest(Value.ofLong(1)));
                final Throwable cause = assertThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS)).getCause();
                assertSame(Code.CANCELED, ((VrpcException) cause).code());
                assertTrue(cause.getMessage().contains("canceled by peer"), cause.getMessage());
            }
        });
    }

    @Test
    void aRefusedHandshakeIsTheErrorOfConnecting() throws Exception {
        overEachTransport(server -> {
            server.refuseHandshake = new VrpcException(Code.UNAUTHENTICATED, "authentication failed");
            try (VrpcClient client = clientOf(server).credential(Value.utf8("wrong")).build()) {
                final VrpcException e = failure(client::connect);
                assertSame(Code.UNAUTHENTICATED, e.code());
                assertEquals("handshake: authentication failed", e.detail());
                assertFalse(client.isActive());
                assertEquals(Value.utf8("wrong"), server.handshakes.get(0).get("auth"));

                // It was not a lost connection: nothing reconnects behind the caller's back.
                Thread.sleep(100);
                assertEquals(1, server.connections());
                server.refuseHandshake = null;
                client.setCredential(Value.utf8("right"));
                assertEquals(Value.ofLong(1), client.callFunction("echo", Value.ofLong(1)));
                assertEquals(Value.utf8("right"), server.handshakes.get(1).get("auth"));
            }
        });
    }

    @Test
    void aHandshakeThatGetsNoAnswerOrACloseFailsTheConnect() throws Exception {
        overEachTransport(server -> {
            server.silentAtHandshake = true;
            try (VrpcClient client = clientOf(server).build()) {
                assertSame(Code.UNAVAILABLE, failure(client::connect).code());
            }
            server.silentAtHandshake = false;
            server.muteAtHandshake = true;
            try (VrpcClient client = clientOf(server).dialTimeoutMillis(200).build()) {
                final long start = System.nanoTime();
                assertSame(Code.DEADLINE_EXCEEDED, failure(client::connect).code());
                assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(3));
            }
        });
    }

    @Test
    void aLostConnectionFailsWhatWaitedAndIsResumed() throws Exception {
        overEachTransport(server -> {
            final ConcurrentLinkedQueue<String> events = new ConcurrentLinkedQueue<String>();
            server.mute = true;
            try (VrpcClient client = clientOf(server).credential(Value.utf8("secret")).listener(new ClientListener() {
                @Override
                public void onConnected(MapValue handshakeResponse) {
                    events.add("connected");
                }

                @Override
                public void onDisconnected(Throwable cause) {
                    events.add("disconnected");
                }
            }).build()) {
                final CompletableFuture<Value> waiting = client.callFunctionAsync("greet", null, null);
                assertNotNull(server.next(2000));
                server.drop();
                final Throwable cause = assertThrows(ExecutionException.class, () -> waiting.get(5, TimeUnit.SECONDS)).getCause();
                assertSame(Code.UNAVAILABLE, ((VrpcException) cause).code(), "at once, not after its timeout");
                assertEquals("connection lost during reconnect", ((VrpcException) cause).detail());

                // It connects again by itself, as the same client, with the
                // next link of its chain and with its credential.
                waitFor(() -> server.handshakes.size() == 2 && client.isActive());
                server.mute = false;
                assertEquals(Value.ofLong(5), client.callFunction("echo", Value.ofLong(5)));
                server.drop();
                waitFor(() -> server.handshakes.size() == 3 && client.isActive());
                assertEquals(Value.ofLong(6), client.callFunction("echo", Value.ofLong(6)));

                final String anchor = server.handshakes.get(0).getString("tok").toString();
                final String link1 = server.handshakes.get(1).getString("tok").toString();
                final String link2 = server.handshakes.get(2).getString("tok").toString();
                assertEquals(1, HashChain.verifyStep(link1, anchor, 0), "the first link hashes to the anchor");
                assertEquals(1, HashChain.verifyStep(link2, link1, 0), "the second to the first");
                for (MapValue hs : server.handshakes) {
                    assertEquals(client.clientId(), hs.getNumber("cid").longValue());
                    assertEquals(Value.utf8("secret"), hs.get("auth"));
                }
                assertEquals(2L, client.stats().get("reconnects").longValue());
                assertEquals("[connected, disconnected, connected, disconnected, connected]", events.toString());
            }
        });
    }

    @Test
    void whatAnswersNothingIsDropped() throws Exception {
        overEachTransport(server -> {
            final ConcurrentLinkedQueue<String> problems = new ConcurrentLinkedQueue<String>();
            try (VrpcClient client = clientOf(server).listener(new ClientListener() {
                @Override
                public void onProtocolError(MapValue message, String problem) {
                    problems.add(problem);
                }
            }).build()) {
                client.connect();
                server.push(D.functionResult(Value.ofLong(999), Value.ofLong(1)));
                server.push(MapValue.builder().put("rid", 1).build());
                server.push(MapValue.builder().put("t", 3).build());
                server.push(D.request(MessageType.FUNCTION_REQUEST, 5, "notify", null, 0, null));
                waitFor(() -> problems.size() == 4);
                assertEquals("[request not found, message type not found, request id not found, "
                        + "server-initiated request must use a negative request id]", problems.toString());
                // The connection is none the worse for it.
                assertEquals(Value.ofLong(1), client.callFunction("echo", Value.ofLong(1)));
            }
        });
    }

    @Test
    void aRequestOfTheServerIsAnsweredAsNotThere() throws Exception {
        overEachTransport(server -> {
            try (VrpcClient client = clientOf(server).build()) {
                client.connect();
                server.push(D.request(MessageType.FUNCTION_REQUEST, -1, "notify", Value.list(), 0, null));
                final MapValue answer = server.next(2000);
                assertSame(MessageType.ERROR_RESPONSE, D.messageType(answer));
                assertEquals(-1, D.requestId(answer).longValue());
                assertSame(Code.NOT_FOUND, D.errorOf(answer).code());
            }
        });
    }

    @Test
    void interceptorsWrapACallTheFirstOutermost() throws Exception {
        overEachTransport(server -> {
            final List<String> order = new ArrayList<String>();
            final AtomicInteger attempts = new AtomicInteger();
            final ClientInterceptor outer = (method, request, options, next) -> {
                order.add("outer in");
                final Value v = next.invoke(method, request, options);
                order.add("outer out");
                return v;
            };
            final ClientInterceptor retry = (method, request, options, next) -> {
                order.add("retry in");
                for (int i = 0;; i++) {
                    attempts.incrementAndGet();
                    try {
                        return next.invoke(method, request, options);
                    } catch (VrpcException e) {
                        if (e.code() != Code.RESOURCE_EXHAUSTED || i == 2) {
                            throw e;
                        }
                    }
                }
            };
            final AtomicInteger failures = new AtomicInteger(2);
            server.function("flaky", args -> {
                if (failures.getAndDecrement() > 0) {
                    throw new VrpcException(Code.RESOURCE_EXHAUSTED, "busy");
                }
                return Value.utf8("done");
            });
            try (VrpcClient client = clientOf(server).interceptors(outer).interceptors(retry).build()) {
                assertEquals(Value.utf8("done"), client.callFunction("flaky", null));
                assertEquals("[outer in, retry in, outer out]", order.toString());
                assertEquals(3, attempts.get());
                assertEquals(Value.utf8("done"), client.callFunctionAsync("flaky", null, null).get(5, TimeUnit.SECONDS));
            }
            // One that answers itself: the server never hears of the call.
            try (VrpcClient client = clientOf(server)
                    .interceptors((method, request, options, next) -> Value.utf8("cached")).build()) {
                assertEquals(Value.utf8("cached"), client.callFunction("greet", null));
                assertEquals(0, server.connections() - 1, "only the client above connected");
            }
        });
    }

    @Test
    void metricsCountEveryCallOnceWithItsCode() throws Exception {
        overEachTransport(server -> {
            final ConcurrentLinkedQueue<String> seen = new ConcurrentLinkedQueue<String>();
            final Metrics metrics = new Metrics() {
                @Override
                public void requestBegin(String method) {
                    seen.add("begin " + method);
                }

                @Override
                public void requestEnd(String method, Code code, long elapsedNanos) {
                    assertTrue(elapsedNanos >= 0);
                    seen.add("end " + method + " " + code);
                }
            };
            try (VrpcClient client = clientOf(server).metrics(metrics).build()) {
                client.callFunction("echo", null);
                failure(() -> client.callFunction("fail", null));
                failure(() -> client.callFunction("nope", null));
                server.mute = true;
                failure(() -> client.callFunction("echo", null, CallOptions.timeoutMillis(50)));
                assertEquals("[begin echo, end echo OK, begin fail, end fail ResourceExhausted, begin nope, end nope NotFound, "
                        + "begin echo, end echo DeadlineExceeded]", seen.toString());
            }
        });
    }

    @Test
    void closingFailsWhatWaits() throws Exception {
        overEachTransport(server -> {
            server.mute = true;
            final VrpcClient client = clientOf(server).build();
            final CompletableFuture<Value> waiting = client.callFunctionAsync("greet", null, null);
            assertNotNull(server.next(2000));
            client.close();
            final Throwable cause = assertThrows(ExecutionException.class, () -> waiting.get(5, TimeUnit.SECONDS)).getCause();
            assertSame(Code.UNAVAILABLE, ((VrpcException) cause).code());
            assertEquals("client closed", ((VrpcException) cause).detail());
            assertEquals("client closed", failure(() -> client.callFunction("greet", null)).detail());
            assertEquals("client closed", failure(client::connect).detail());
        });
    }

    @Test
    void addressesAndTheirTransports() throws Exception {
        // In memory: a name is taken once, and a name nobody listens on cannot be dialed.
        try (FakeServer server = new FakeServer("mem://taken")) {
            assertEquals("mem://taken", server.address());
            assertThrows(IOException.class, () -> Transports.listener("mem://taken", TransportOptions.DEFAULT));
        }
        Transports.listener("mem://taken", TransportOptions.DEFAULT).close();
        try (VrpcClient nobody = VrpcClient.builder().address("mem://nobody").build()) {
            final VrpcException e = failure(nobody::connect);
            assertSame(Code.UNAVAILABLE, e.code());
            assertTrue(e.detail().contains("no mem listener"), e.detail());
        }
        // TCP: port 0 asks for any, and the address then says which.
        try (FakeServer server = new FakeServer("tcp://127.0.0.1:0")) {
            assertTrue(server.address().matches("tcp://127\\.0\\.0\\.1:[1-9][0-9]*"), server.address());
            // A bare host:port is TCP as well.
            try (VrpcClient client = VrpcClient.builder().address(server.address().substring("tcp://".length())).build()) {
                assertEquals(Value.ofLong(1), client.callFunction("echo", Value.ofLong(1)));
            }
        }
        assertThrows(IOException.class, () -> Transports.listener("carrier-pigeon://x", TransportOptions.DEFAULT));
        assertThrows(IllegalStateException.class, () -> VrpcClient.builder().build());
    }
}
