/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.arpabet.value.MapValue;
import com.arpabet.value.NumberValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.GoServer;
import com.arpabet.valuerpc.Metrics;
import com.arpabet.valuerpc.VrpcException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The Java client against the Go server ({@code conformance/server}), over
 * TCP: the scenarios of the conformance suite of value-rpc-js that are calls.
 * Skipped where there is no Go toolchain.
 */
@Timeout(60)
class GoInteropTest {

    private static GoServer server;
    private static VrpcClient client;

    @BeforeAll
    static void start() throws Exception {
        assumeTrue(GoServer.available(), "no Go toolchain: the Go server cannot be built");
        server = GoServer.start();
        client = VrpcClient.builder()
                .address("tcp://" + server.tcp())
                .timeoutMillis(4000)
                .metadata(() -> Collections.singletonMap("traceparent", "00-t"))
                .build();
        client.connect();
    }

    @AfterAll
    static void stop() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    private static VrpcException failure(Runnable call) {
        return assertThrows(VrpcException.class, call::run);
    }

    @Test
    void greet() {
        assertEquals(Value.utf8("Hello, world!"), client.callFunction("greet", Value.list(Value.utf8("world"))));
        assertTrue(client.isActive());
        assertTrue(client.clientId() >= 0);
    }

    @Test
    void addOverTheWholeRangeOfALong() {
        assertEquals(Value.ofLong(42), client.callFunction("add", Value.list(Value.ofLong(40), Value.ofLong(2))));
        assertEquals(Value.ofLong(Long.MAX_VALUE),
                client.callFunction("add", Value.list(Value.ofLong(Long.MAX_VALUE - 5), Value.ofLong(5))));
        assertEquals(Value.ofLong(-7), client.callFunction("add", Value.list(Value.ofLong(-10), Value.ofLong(3))));
    }

    @Test
    void echoRoundTripsEveryKind() {
        final Value payload = MapValue.builder()
                .put("bytes", new byte[] {0, 1, (byte) 254, (byte) 255})
                .put("list", Value.list(Value.ofLong(1), Value.utf8("two"), Value.ofNull(), Value.ofBool(true), Value.ofDouble(1.5)))
                .put("nested", MapValue.builder().put("deep", MapValue.builder().put("key", "value").build()).build())
                .put("big", 1L << 60)
                .put("bigint", Value.ofBigInt(BigInteger.ONE.shiftLeft(70)))
                .put("decimal", Value.ofDecimal(new BigDecimal("1.10")))
                .put("when", Value.timestamp(1793345319L, 123_456_789))
                .put("sparse", Value.emptySparseList().putAt(5, Value.utf8("five")))
                .put("ext", Value.ext(7, new byte[] {1, 2}))
                .put("text", "bell " + (char) 7 + " quote \" and " + new String(Character.toChars(0x1F600)))
                .build();
        assertEquals(payload, client.callFunction("echo", payload));
        assertEquals(Value.list(), client.callFunction("echo", Value.list()));
        assertEquals(Value.utf8(""), client.callFunction("echo", Value.utf8("")));
    }

    @Test
    void aFunctionThatReturnsNothing() {
        assertNull(client.callFunction("nothing", null));
        // No arguments are sent as null, and null comes back as no result.
        assertNull(client.callFunction("echo", null));
    }

    @Test
    void aCodedErrorArrivesWithItsCode() {
        final VrpcException e = failure(() -> client.callFunction("fail", null));
        assertSame(Code.RESOURCE_EXHAUSTED, e.code());
        assertEquals("function fail: deliberate failure", e.detail());
        assertEquals("vrpc ResourceExhausted: function fail: deliberate failure", e.getMessage());

        final VrpcException plain = failure(() -> client.callFunction("boom", null));
        assertSame(Code.INTERNAL, plain.code());
        assertEquals("function boom: boom", plain.detail());
    }

    @Test
    void anUnknownFunctionIsNotFound() {
        final VrpcException e = failure(() -> client.callFunction("no.such.fn", null));
        assertSame(Code.NOT_FOUND, e.code());
        assertTrue(e.detail().contains("no.such.fn"), e.detail());
    }

    @Test
    void theServerVerifiesTheArguments() {
        assertEquals(Value.utf8("ok"), client.callFunction("typed", Value.list(Value.utf8("a"), Value.ofLong(1))));
        assertEquals(Value.utf8("ok"), client.callFunction("typed", Value.list(Value.utf8("a"), Value.ofNull())));
        assertSame(Code.INVALID_ARGUMENT, failure(() -> client.callFunction("typed", Value.list(Value.ofLong(1)))).code());
        assertSame(Code.INVALID_ARGUMENT, failure(() -> client.callFunction("typed", null)).code());
    }

    @Test
    void aTimeoutHereCancelsTheCallThere() {
        final long start = System.nanoTime();
        final VrpcException e = failure(() -> client.callFunction("slow", null, CallOptions.timeoutMillis(150)));
        assertSame(Code.DEADLINE_EXCEEDED, e.code());
        assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(3), "it did not wait for the handler");
        // The connection is as good as before.
        assertEquals(Value.utf8("Hello, again!"), client.callFunction("greet", Value.list(Value.utf8("again"))));
    }

    @Test
    void theServerGetsTheTimeoutAsTheDeadlineOfTheHandler() {
        final long left = ((NumberValue) client.callFunction("deadline", null, CallOptions.timeoutMillis(800))).longValue();
        assertTrue(left > 0 && left <= 800, "the handler had " + left + " ms");
        // No timeout: no deadline is sent.
        assertEquals(Value.ofLong(-1), client.callFunction("deadline", null, CallOptions.timeoutMillis(-1)));
    }

    @Test
    void cancellingTheFutureCancelsTheCall() throws Exception {
        final CompletableFuture<Value> f = client.callFunctionAsync("slow", null, CallOptions.timeoutMillis(8000));
        Thread.sleep(50);
        assertFalse(f.isDone());
        assertTrue(f.cancel(true));
        assertThrows(CancellationException.class, f::get);
        assertEquals(Value.ofLong(3), client.callFunctionAsync("add", Value.list(Value.ofLong(1), Value.ofLong(2)),
                CallOptions.DEFAULT).get(5, TimeUnit.SECONDS));
    }

    @Test
    void interruptingTheCallerCancelsTheCall() throws Exception {
        final AtomicReference<Throwable> thrown = new AtomicReference<Throwable>();
        final AtomicReference<Boolean> stillInterrupted = new AtomicReference<Boolean>();
        final Thread caller = new Thread(() -> {
            try {
                client.callFunction("slow", null, CallOptions.timeoutMillis(8000));
            } catch (Throwable t) {
                thrown.set(t);
                stillInterrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        caller.start();
        Thread.sleep(100);
        caller.interrupt();
        caller.join(5000);
        assertFalse(caller.isAlive());
        assertSame(Code.CANCELED, ((VrpcException) thrown.get()).code());
        assertTrue(stillInterrupted.get());
    }

    @Test
    void metadataReachesTheHandler() {
        final Value md = client.callFunction("md.echo", Value.list(), CallOptions.DEFAULT.withMetadata("extra", "x"));
        assertEquals(MapValue.builder().put("traceparent", "00-t").put("extra", "x").build(), md);
        assertEquals(MapValue.builder().put("traceparent", "00-t").build(), client.callFunction("md.echo", null));
    }

    @Test
    void manyCallsAtOnceOnOneConnection() throws Exception {
        final int n = 2000;
        final List<CompletableFuture<Value>> calls = new ArrayList<CompletableFuture<Value>>(n);
        for (int i = 0; i < n; i++) {
            calls.add(client.callFunctionAsync("add", Value.list(Value.ofLong(i), Value.ofLong(1000)), CallOptions.DEFAULT));
        }
        for (int i = 0; i < n; i++) {
            assertEquals(Value.ofLong(i + 1000), calls.get(i).get(20, TimeUnit.SECONDS));
        }
        final int threads = 16;
        final CountDownLatch done = new CountDownLatch(threads);
        final AtomicInteger wrong = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            final int base = t * 100_000;
            new Thread(() -> {
                try {
                    for (int i = 0; i < 200; i++) {
                        final Value v = client.callFunction("echo", Value.ofLong(base + i));
                        if (!Value.ofLong(base + i).equals(v)) {
                            wrong.incrementAndGet();
                        }
                    }
                } catch (RuntimeException e) {
                    wrong.incrementAndGet();
                } finally {
                    done.countDown();
                }
            }).start();
        }
        assertTrue(done.await(30, TimeUnit.SECONDS));
        assertEquals(0, wrong.get());
    }

    @Test
    void aLargeMessage() {
        final byte[] megabyte = new byte[1 << 20];
        for (int i = 0; i < megabyte.length; i++) {
            megabyte[i] = (byte) (i * 31);
        }
        assertEquals(Value.raw(megabyte), client.callFunction("echo", Value.raw(megabyte)));
    }

    @Test
    void theHandshakeCarriesTheCredential() {
        try (VrpcClient authed = VrpcClient.builder().address(server.tcp()).credential(Value.utf8("secret")).build()) {
            assertEquals(Value.utf8("tester"), authed.callFunction("whoami", null));
        }
        // Without one the server takes the client as nobody in particular.
        assertEquals(Value.utf8(""), client.callFunction("whoami", null));

        try (VrpcClient refused = VrpcClient.builder().address(server.tcp()).credential(Value.utf8("wrong"))
                .dialTimeoutMillis(3000).build()) {
            final VrpcException e = failure(refused::connect);
            assertSame(Code.UNAUTHENTICATED, e.code());
            assertTrue(e.detail().contains("authentication failed"), e.detail());
            assertFalse(refused.isActive());
            // A call does not get further than connecting did.
            assertSame(Code.UNAUTHENTICATED, failure(() -> refused.callFunction("greet", null)).code());
        }
    }

    @Test
    void aServerThatIsNotThereIsUnavailable() {
        try (VrpcClient nowhere = VrpcClient.builder().address("127.0.0.1:1").dialTimeoutMillis(2000).build()) {
            assertSame(Code.UNAVAILABLE, failure(nowhere::connect).code());
            assertSame(Code.UNAVAILABLE, failure(() -> nowhere.callFunction("greet", null)).code());
        }
        try (VrpcClient odd = VrpcClient.builder().address("quic://127.0.0.1:1").build()) {
            assertSame(Code.UNAVAILABLE, failure(odd::connect).code());
        }
    }

    @Test
    void aClosedClientFailsItsCallsAndStaysClosed() throws Exception {
        final VrpcClient c = VrpcClient.builder().address(server.tcp()).timeoutMillis(8000).build();
        final CompletableFuture<Value> waiting = c.callFunctionAsync("slow", null, CallOptions.DEFAULT);
        Thread.sleep(50);
        c.close();
        final Throwable cause = assertThrows(java.util.concurrent.ExecutionException.class,
                () -> waiting.get(5, TimeUnit.SECONDS)).getCause();
        assertSame(Code.UNAVAILABLE, ((VrpcException) cause).code());
        assertSame(Code.UNAVAILABLE, failure(() -> c.callFunction("greet", null)).code());
        assertFalse(c.isActive());
        c.close();
    }

    @Test
    void aDroppedConnectionIsResumedAsTheSameSession() throws Exception {
        final ConcurrentLinkedQueue<String> events = new ConcurrentLinkedQueue<String>();
        final CountDownLatch first = new CountDownLatch(1);
        final CountDownLatch second = new CountDownLatch(2);
        final CountDownLatch third = new CountDownLatch(3);
        final AtomicInteger reconnects = new AtomicInteger();
        final VrpcClient c = VrpcClient.builder().address(server.tcp()).timeoutMillis(4000)
                .metrics(new Metrics() {
                    @Override
                    public void reconnect() {
                        reconnects.incrementAndGet();
                    }
                })
                .listener(new ClientListener() {
                    @Override
                    public void onConnected(MapValue handshakeResponse) {
                        events.add("connected");
                        first.countDown();
                        second.countDown();
                        third.countDown();
                    }

                    @Override
                    public void onDisconnected(Throwable cause) {
                        events.add("disconnected");
                    }
                })
                .build();
        try {
            final long id = c.clientId();
            assertEquals(Value.utf8("Hello, one!"), c.callFunction("greet", Value.list(Value.utf8("one"))));
            assertTrue(first.await(5, TimeUnit.SECONDS));

            // The server closes the connection. The client connects again and
            // shows the next link of its chain: the server takes it for the
            // same session, which a wrong link would not get.
            assertEquals(Value.utf8("kicking"), c.callFunction("kick", null));
            assertTrue(second.await(10, TimeUnit.SECONDS), "reconnected: " + events);
            assertEquals(id, c.clientId());
            assertEquals(Value.utf8("Hello, two!"), c.callFunction("greet", Value.list(Value.utf8("two"))));

            // And once more: each reconnect shows a link of its own.
            assertEquals(Value.utf8("kicking"), c.callFunction("kick", null));
            assertTrue(third.await(10, TimeUnit.SECONDS), "reconnected again: " + events);
            assertEquals(Value.utf8("Hello, three!"), c.callFunction("greet", Value.list(Value.utf8("three"))));

            assertEquals(2, reconnects.get());
            assertEquals(2L, c.stats().get("reconnects").longValue());
            assertEquals(5L, c.stats().get("requests").longValue());
            assertEquals(0L, c.stats().get("pending").longValue());
        } finally {
            c.close();
        }
    }
}
