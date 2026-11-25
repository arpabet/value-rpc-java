/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.value.Limits;
import com.arpabet.value.MapValue;
import com.arpabet.value.NullValue;
import com.arpabet.value.NumberValue;
import com.arpabet.value.Value;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Every message of the protocol, built here, is the bytes the Go
 * implementation builds for it ({@code frames.tsv}): the envelope in
 * MessagePack, its JSON, its frame on a stream, and a batch of frames.
 */
class EnvelopeGoldenTest {

    private static final String ANCHOR = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    private static NumberValue id(long n) {
        return Value.ofLong(n);
    }

    private static Map<String, String> md(String... pairs) {
        final Map<String, String> m = new LinkedHashMap<String, String>();
        for (int i = 0; i < pairs.length; i += 2) {
            m.put(pairs[i], pairs[i + 1]);
        }
        return m;
    }

    /** The messages of {@code envelopes()} in conformance/golden/main.go, by name. */
    static Map<String, MapValue> envelopes() {
        final Dialect d = Dialect.standard();
        final Map<String, MapValue> m = new LinkedHashMap<String, MapValue>();
        m.put("handshake-request", d.handshakeRequest(712398211L, ANCHOR, null));
        m.put("handshake-request-no-token", d.handshakeRequest(1, "", null));
        m.put("handshake-request-auth", d.handshakeRequest(712398211L, ANCHOR, Value.utf8("secret")));
        m.put("handshake-request-auth-map", d.handshakeRequest(42, null,
                MapValue.builder().put("user", "ann").put("key", new byte[] {1, 2, 3}).build()));
        m.put("handshake-request-auth-null", d.handshakeRequest(42, "", NullValue.NULL));
        m.put("handshake-request-max-cid", d.handshakeRequest(Long.MAX_VALUE, ANCHOR, null));
        m.put("handshake-response", d.handshakeResponse(""));
        m.put("handshake-response-token", d.handshakeResponse("abc"));

        m.put("request-reverse-no-args", d.request(MessageType.FUNCTION_REQUEST, -1, "notify", null, 0, null));
        m.put("request-reverse-args",
                d.request(MessageType.FUNCTION_REQUEST, -2, "notify", Value.list(Value.utf8("ping")), 0, null));
        m.put("request-reverse-md", d.request(MessageType.FUNCTION_REQUEST, -3, "notify",
                Value.list(Value.ofLong(1)), 0, md("traceparent", "00-t")));
        m.put("request-client",
                d.request(MessageType.FUNCTION_REQUEST, 1, "greet", Value.list(Value.utf8("world")), 5000, null));
        m.put("request-client-no-args", d.request(MessageType.FUNCTION_REQUEST, 2, "ping", NullValue.NULL, 1000, null));
        m.put("request-client-no-sla", d.request(MessageType.FUNCTION_REQUEST, 3, "ping", NullValue.NULL, 0, null));
        m.put("request-client-md", d.request(MessageType.FUNCTION_REQUEST, 4, "md.echo", Value.emptyList(), 250,
                md("traceparent", "00-t", "extra", "x")));
        m.put("request-get-stream",
                d.request(MessageType.GET_STREAM_REQUEST, 5, "count", Value.list(Value.ofLong(25)), 1000, null));
        m.put("request-put-stream",
                d.request(MessageType.PUT_STREAM_REQUEST, 6, "upload", Value.list(Value.utf8("batch")), 1000, null));
        m.put("request-chat",
                d.request(MessageType.CHAT_REQUEST, 7, "chat.echo", Value.list(Value.utf8("room-1")), 1000, null));
        m.put("request-reverse-get-stream",
                d.request(MessageType.GET_STREAM_REQUEST, -4, "tail", Value.list(Value.ofLong(4)), 0, null));
        m.put("request-reverse-put-stream", d.request(MessageType.PUT_STREAM_REQUEST, -5, "ingest", null, 0, null));
        m.put("request-reverse-chat", d.request(MessageType.CHAT_REQUEST, -6, "talk", NullValue.NULL, 0, null));

        m.put("result", d.functionResult(id(1), Value.utf8("Hello, world!")));
        m.put("result-nothing", d.functionResult(id(2), null));
        m.put("result-null", d.functionResult(id(3), NullValue.NULL));
        m.put("result-map", d.functionResult(id(-1), MapValue.builder()
                .put("sum", 55).put("count", 10).put("ratio", 0.5).put("raw", new byte[] {0, (byte) 255}).build()));
        m.put("error", d.error(id(3), Code.NOT_FOUND, "function not found no.such.fn"));
        m.put("error-handshake", d.error(id(d.handshakeRequestId()), Code.UNAUTHENTICATED, "authentication failed"));
        m.put("error-handler-coded", d.handlerError(id(4), "function fail",
                new VrpcException(Code.RESOURCE_EXHAUSTED, "deliberate failure")));
        m.put("error-handler-plain", d.handlerError(id(5), "function boom", new IllegalStateException("boom")));

        m.put("stream-ready", d.streamReady(id(5)));
        m.put("stream-value", d.streamValue(id(5), Value.ofLong(1)));
        m.put("stream-value-nothing", d.streamValue(id(5), null));
        m.put("stream-value-reverse", d.streamValue(id(-4), Value.utf8("t0")));
        m.put("stream-end", d.streamEnd(id(5), null));
        m.put("stream-end-value", d.streamEnd(id(5), Value.utf8("last")));
        m.put("cancel", d.cancelRequest(id(5)));
        m.put("stream-credit", d.streamCredit(id(5), 4096));
        m.put("stream-credit-reverse", d.streamCredit(id(-4), 2048));
        return m;
    }

    @Test
    void everyEnvelopeIsTheBytesGoBuilds() throws Exception {
        final Map<String, MapValue> ours = envelopes();
        final Set<String> seen = new HashSet<String>();
        int frames = 0;
        int batches = 0;
        for (String[] f : Golden.lines("frames.tsv")) {
            switch (f[0]) {
                case "envelope": {
                    final MapValue msg = ours.get(f[1]);
                    assertNotNull(msg, "the generator has an envelope this test does not build: " + f[1]);
                    seen.add(f[1]);
                    assertEquals(f[2], msg.hex(), "the bytes of " + f[1]);
                    assertEquals(Golden.text(f[3]), msg.jsonify(), "the JSON of " + f[1]);
                    // And what Go wrote reads back as the message, in both codecs.
                    assertEquals(msg, WireCodec.MSGPACK.decode(Golden.unhex(f[2])), f[1]);
                    assertEquals(msg.hex(), Golden.hex(WireCodec.MSGPACK.encode(msg)), f[1]);
                    break;
                }
                case "frame": {
                    final MapValue msg = ours.get(f[1]);
                    assertEquals(f[2], Golden.hex(Frames.encode(msg)), "the frame of " + f[1]);
                    final ByteArrayInputStream in = new ByteArrayInputStream(Golden.unhex(f[2]));
                    assertEquals(msg, Frames.read(in, Frames.MAX_FRAME_SIZE, Limits.DEFAULT));
                    assertNull(Frames.read(in, Frames.MAX_FRAME_SIZE, Limits.DEFAULT), "the end of the stream");
                    frames++;
                    break;
                }
                case "batch": {
                    final List<MapValue> msgs = new ArrayList<MapValue>();
                    for (String name : f[1].split(",")) {
                        msgs.add(ours.get(name));
                    }
                    assertEquals(f[2], Golden.hex(Frames.encode(msgs)), "the batch " + f[1]);
                    final ByteArrayInputStream in = new ByteArrayInputStream(Golden.unhex(f[2]));
                    for (MapValue msg : msgs) {
                        assertEquals(msg, Frames.read(in, Frames.MAX_FRAME_SIZE, Limits.DEFAULT));
                    }
                    assertNull(Frames.read(in, Frames.MAX_FRAME_SIZE, Limits.DEFAULT));
                    batches++;
                    break;
                }
                default:
                    throw new AssertionError("frames.tsv: " + f[0]);
            }
        }
        assertEquals(ours.keySet(), seen, "every envelope built here is one the generator writes");
        assertTrue(frames > 0 && batches > 0);
    }

    @Test
    void jsonReadsBackAsTheEnvelope() throws Exception {
        // The JSON codec does not tell every kind apart: binary comes back, a
        // double that JSON writes as an integer comes back as a long. An
        // envelope without such a double reads back as it was.
        final Map<String, MapValue> ours = envelopes();
        for (String[] f : Golden.lines("frames.tsv")) {
            if (!f[0].equals("envelope")) {
                continue;
            }
            final MapValue msg = ours.get(f[1]);
            final MapValue back = WireCodec.JSON.decode(Golden.bytes(f[3]));
            assertArrayEquals(Golden.bytes(f[3]), WireCodec.JSON.encode(msg), f[1]);
            if (f[1].startsWith("handshake")) {
                // The version, 1.0, is written 1 and read as the long 1.
                assertTrue(Dialect.standard().validMagicAndVersion(back), f[1]);
                assertEquals(msg.put("v", Value.ofLong(1)), back, f[1]);
            } else if (f[1].equals("result-map")) {
                assertEquals(msg, back, f[1]);
            } else {
                assertEquals(msg, back, f[1]);
            }
        }
    }
}
