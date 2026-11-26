/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.arpabet.value.ListValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.GoServer;
import com.arpabet.valuerpc.TypeDef;
import com.arpabet.valuerpc.VrpcException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The client as a peer of the Go server: the reference asks, and this client
 * answers. The server is {@code conformance/server}, whose
 * {@code reverse.call}, {@code reverse.pull} and {@code reverse.push} ask the
 * client that called them for {@code notify}, {@code tail} and
 * {@code ingest}. Skipped where there is no Go toolchain.
 */
@Timeout(90)
class GoReverseInteropTest {

    private GoServer server;
    private VrpcClient client;
    private final List<Value> ingested = Collections.synchronizedList(new ArrayList<Value>());

    @BeforeEach
    void start() throws Exception {
        assumeTrue(GoServer.available(), "no Go toolchain: the Go server cannot be built");
        server = GoServer.start();
        client = VrpcClient.builder().address(server.tcp()).timeoutMillis(10_000).build();
        client.addFunction("notify", (ctx, args) -> Value.utf8("client-saw:" + ((ListValue) args).get(0)));
        client.addOutgoingStream("tail", TypeDef.ANY, (ctx, args, out) -> {
            final long n = ((ListValue) args).getNumber(0).longValue();
            for (long i = 0; i < n; i++) {
                out.send(Value.utf8("t" + i));
            }
        });
        client.addIncomingStream("ingest", TypeDef.ANY, (ctx, args, in) -> {
            for (Value v : in) {
                ingested.add(v);
            }
        });
    }

    @AfterEach
    void stop() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    private void waitFor(java.util.function.BooleanSupplier what) throws InterruptedException {
        final long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!what.getAsBoolean()) {
            assertTrue(System.nanoTime() < until, "waited too long");
            Thread.sleep(5);
        }
    }

    @Test
    void theGoServerAsksThisClientByEveryPattern() throws Exception {
        assertEquals(Value.utf8("client-saw:ping"), client.callFunction("reverse.call", Value.list(Value.utf8("ping"))));
        assertEquals(Value.list(Value.utf8("t0"), Value.utf8("t1"), Value.utf8("t2"), Value.utf8("t3")),
                client.callFunction("reverse.pull", Value.list(Value.ofLong(4))));
        assertEquals(Value.utf8("pushed"), client.callFunction("reverse.push", Value.list(Value.utf8("go"))));
        waitFor(() -> ingested.size() == 3);
        assertEquals(Arrays.asList(Value.ofLong(100), Value.ofLong(200), Value.ofLong(300)), ingested);
        assertEquals(0L, client.stats().get("pending"));
    }

    @Test
    void moreValuesThanTheServerGrantsAtOnceAllArrive() {
        // The Go server grants 4096 values at first and more as its handler takes them: the client waits for it.
        final ListValue all = (ListValue) client.callFunction("reverse.pull", Value.list(Value.ofLong(20_000)));
        assertEquals(20_000, all.size());
        assertEquals(Value.utf8("t0"), all.get(0));
        assertEquals(Value.utf8("t19999"), all.get(19_999));
    }

    @Test
    void theGoServerIsToldWhatThisClientDoesNotServe() {
        // Its handler gives the answer of the client on, under its own name: in the words of the Go client.
        VrpcException e = assertThrows(VrpcException.class, () -> client.callFunction("reverse.pull.none", null));
        assertEquals(Code.NOT_FOUND, e.code());
        client.addFunction("tail", (ctx, args) -> null);
        e = assertThrows(VrpcException.class, () -> client.callFunction("reverse.pull", Value.list(Value.ofLong(1))));
        assertEquals(Code.INVALID_ARGUMENT, e.code());
        assertEquals("function reverse.pull: function 'tail' wrong stream type", e.detail());
        client.addOutgoingStream("notify", TypeDef.ANY, (ctx, args, out) -> {
        });
        e = assertThrows(VrpcException.class, () -> client.callFunction("reverse.call", Value.list(Value.utf8("x"))));
        assertEquals(Code.INVALID_ARGUMENT, e.code());
        assertEquals("function reverse.call: function 'notify' is not unary", e.detail());
        client.addFunction("notify", (ctx, args) -> {
            throw new VrpcException(Code.RESOURCE_EXHAUSTED, "not now");
        });
        e = assertThrows(VrpcException.class, () -> client.callFunction("reverse.call", Value.list(Value.utf8("x"))));
        assertEquals(Code.RESOURCE_EXHAUSTED, e.code());
        assertEquals("function reverse.call: function notify: not now", e.detail());
    }

    @Test
    void aClientThatWasDroppedIsAskedAgainOnceItIsBack() throws Exception {
        assertEquals(Value.utf8("client-saw:before"), client.callFunction("reverse.call", Value.list(Value.utf8("before"))));
        assertEquals(Value.utf8("kicking"), client.callFunction("kick", null));
        waitFor(() -> client.stats().get("reconnects") >= 1 && client.isActive());
        // The same session, on the next connection: what it registered is still served.
        assertEquals(Value.utf8("client-saw:after"), client.callFunction("reverse.call", Value.list(Value.utf8("after"))));
        assertEquals(Value.list(Value.utf8("t0"), Value.utf8("t1")), client.callFunction("reverse.pull", Value.list(Value.ofLong(2))));
    }
}
