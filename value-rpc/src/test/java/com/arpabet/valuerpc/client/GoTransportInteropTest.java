/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.arpabet.value.ListValue;
import com.arpabet.value.MapValue;
import com.arpabet.value.NumberValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.CallOptions;
import com.arpabet.valuerpc.Chat;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.GoServer;
import com.arpabet.valuerpc.Pki;
import com.arpabet.valuerpc.TypeDef;
import com.arpabet.valuerpc.ValueSink;
import com.arpabet.valuerpc.ValueStream;
import com.arpabet.valuerpc.VrpcException;
import com.arpabet.valuerpc.WireCodec;
import com.arpabet.valuerpc.transport.Dialer;
import com.arpabet.valuerpc.transport.MsgConn;
import com.arpabet.valuerpc.transport.TransportOptions;
import com.arpabet.valuerpc.transport.Transports;
import com.arpabet.valuerpc.transport.WebSocketOptions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The client over the transports that are not plain TCP, against the Go
 * server: TLS, WebSocket in each of its codecs, and WebSocket over TLS. The
 * WebSocket client here is the one of this library against the WebSocket
 * server of the Go implementation, and the certificate of the server is one
 * the Go server made. Skipped where there is no Go toolchain.
 */
@Timeout(120)
class GoTransportInteropTest {

    private GoServer server;

    @BeforeEach
    void start() throws Exception {
        assumeTrue(GoServer.available(), "no Go toolchain: the Go server cannot be built");
        server = GoServer.start();
    }

    @AfterEach
    void stop() {
        if (server != null) {
            server.close();
        }
    }

    /** The transports, by name: each a dialer to the listener of the Go server for it. */
    private Map<String, Dialer> transports() {
        final TransportOptions trusting = TransportOptions.DEFAULT.withTls(Pki.trusting(server.ca()));
        final Map<String, Dialer> all = new LinkedHashMap<String, Dialer>();
        all.put("tls", Transports.dialer("tls://" + server.tls(), trusting));
        all.put("ws msgpack", Transports.dialer("ws://" + server.ws(), TransportOptions.DEFAULT));
        all.put("ws json", Transports.webSocketDialer("ws://" + server.ws(), TransportOptions.DEFAULT,
                WebSocketOptions.DEFAULT.withCodecs(WireCodec.JSON)));
        all.put("wss msgpack", Transports.dialer("wss://" + server.wss(), trusting));
        all.put("wss json", Transports.webSocketDialer("wss://" + server.wss(), trusting,
                WebSocketOptions.DEFAULT.withCodecs(WireCodec.JSON)));
        return all;
    }

    private interface Scenario {
        void run(VrpcClient client, String transport) throws Exception;
    }

    private void overEachTransport(Scenario scenario) throws Exception {
        for (Map.Entry<String, Dialer> t : transports().entrySet()) {
            try (VrpcClient client = VrpcClient.builder().dialer(t.getValue()).timeoutMillis(10_000).dialTimeoutMillis(10_000).build()) {
                scenario.run(client, t.getKey());
            } catch (AssertionError | Exception e) {
                throw new AssertionError("over " + t.getKey() + ": " + e, e);
            }
        }
    }

    private static void waitFor(java.util.function.BooleanSupplier what) throws InterruptedException {
        final long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!what.getAsBoolean()) {
            assertTrue(System.nanoTime() < until, "waited too long");
            Thread.sleep(5);
        }
    }

    @Test
    void callsOfEveryOutcome() throws Exception {
        overEachTransport((client, transport) -> {
            assertEquals(Value.utf8("Hello, " + transport + "!"), client.callFunction("greet", Value.list(Value.utf8(transport))));
            assertEquals(Value.ofLong(42), client.callFunction("add", Value.list(Value.ofLong(40), Value.ofLong(2))));
            final Value nested = MapValue.builder()
                    .put("bytes", Value.raw(new byte[] {0, 1, (byte) 254, (byte) 255}))
                    .put("list", Value.list(Value.ofLong(1), Value.utf8("two"), Value.ofNull(), Value.ofBool(true), Value.ofDouble(1.5)))
                    .put("nested", MapValue.builder().put("deep", MapValue.builder().put("key", "value").build()).build())
                    .build();
            assertEquals(nested, client.callFunction("echo", nested));
            assertNull(client.callFunction("echo", null));
            VrpcException e = assertThrows(VrpcException.class, () -> client.callFunction("fail", null));
            assertEquals(Code.RESOURCE_EXHAUSTED, e.code());
            e = assertThrows(VrpcException.class, () -> client.callFunction("no.such.function", null));
            assertEquals(Code.NOT_FOUND, e.code());
            // A megabyte in one message, each way.
            final byte[] big = new byte[1 << 20];
            for (int i = 0; i < big.length; i++) {
                big[i] = (byte) (i * 31);
            }
            assertEquals(Value.raw(big), client.callFunction("echo", Value.raw(big)));
        });
    }

    @Test
    void streamsBothWays() throws Exception {
        overEachTransport((client, transport) -> {
            long want = 1;
            try (ValueStream count = client.getStream("count", Value.list(Value.ofLong(10_000)), CallOptions.DEFAULT)) {
                for (Value v : count) {
                    assertEquals(want++, ((NumberValue) v).longValue());
                }
            }
            assertEquals(10_001, want);
            client.callFunction("upload.reset", null);
            try (ValueSink upload = client.putStream("upload", null, CallOptions.DEFAULT)) {
                for (long i = 1; i <= 5000; i++) {
                    upload.send(Value.ofLong(i));
                }
            }
            waitFor(() -> ((MapValue) client.callFunction("upload.stats", null)).getNumber("count").longValue() == 5000);
            assertEquals(5000L * 5001 / 2, ((MapValue) client.callFunction("upload.stats", null)).getNumber("sum").longValue());
            try (Chat chat = client.chat("chat.echo", null, CallOptions.DEFAULT)) {
                for (String word : new String[] {"one", "two", "three"}) {
                    chat.outgoing().send(Value.utf8(word));
                }
                chat.outgoing().close();
                final List<Value> heard = new ArrayList<Value>();
                for (Value v : chat.incoming()) {
                    heard.add(v);
                }
                assertEquals(java.util.Arrays.asList(Value.utf8("echo:one"), Value.utf8("echo:two"), Value.utf8("echo:three")), heard);
            }
        });
    }

    @Test
    void theServerAsksBackAndTheClientComesBackAfterADrop() throws Exception {
        overEachTransport((client, transport) -> {
            final List<Value> ingested = Collections.synchronizedList(new ArrayList<Value>());
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
            assertEquals(Value.utf8("client-saw:ping"), client.callFunction("reverse.call", Value.list(Value.utf8("ping"))));
            assertEquals(6000, ((ListValue) client.callFunction("reverse.pull", Value.list(Value.ofLong(6000)))).size());
            assertEquals(Value.utf8("pushed"), client.callFunction("reverse.push", Value.list(Value.utf8("go"))));
            waitFor(() -> ingested.size() == 3);

            // Dropped by the server, the client connects again as the same session, and is asked again.
            final long id = client.clientId();
            assertEquals(Value.utf8("kicking"), client.callFunction("kick", null));
            waitFor(() -> client.stats().get("reconnects") >= 1 && client.isActive());
            assertEquals(Value.utf8("Hello, again!"), client.callFunction("greet", Value.list(Value.utf8("again"))));
            assertEquals(Value.utf8("client-saw:after"), client.callFunction("reverse.call", Value.list(Value.utf8("after"))));
            assertEquals(id, client.clientId());
        });
    }

    @Test
    void aServerWhoseCertificateIsNotTrustedIsNotConnectedTo() throws Exception {
        final TransportOptions byThePlatform = TransportOptions.DEFAULT;
        for (String address : new String[] {"tls://" + server.tls(), "wss://" + server.wss()}) {
            try (VrpcClient client = VrpcClient.builder().address(address).transportOptions(byThePlatform).dialTimeoutMillis(5000).build()) {
                final VrpcException e = assertThrows(VrpcException.class, client::connect, address);
                assertEquals(Code.UNAVAILABLE, e.code());
            }
            // With the authority of the server it is: the session of TLS is with the connection.
            try (MsgConn conn = Transports.dialer(address, byThePlatform.withTls(Pki.trusting(server.ca()))).dial(5000)) {
                assertNotNull(conn.tlsSession(), address);
            }
        }
        // A server that speaks JSON only would refuse MessagePack; this one takes both, and says which it took.
        try (MsgConn conn = Transports.dialer("ws://" + server.ws(), TransportOptions.DEFAULT).dial(5000)) {
            assertNull(conn.tlsSession());
        }
    }
}
