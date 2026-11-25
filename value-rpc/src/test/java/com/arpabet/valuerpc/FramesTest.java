/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.value.Limits;
import com.arpabet.value.MapValue;
import com.arpabet.value.Value;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Framing on a stream and the two wire codecs: the tests of rpc_test.go,
 * batch_test.go, fuzz_test.go and wirecodec_test.go of the Go implementation.
 */
class FramesTest {

    private static final Dialect D = Dialect.standard();

    private static MapValue read(byte[] bytes, int maxFrameSize) throws IOException {
        return Frames.read(new ByteArrayInputStream(bytes), maxFrameSize, Limits.DEFAULT);
    }

    @Test
    void aFrameIsItsLengthAndItsBytes() throws Exception {
        final MapValue msg = D.request(MessageType.FUNCTION_REQUEST, 1, "greet", Value.list(Value.utf8("world")), 0, null);
        final byte[] frame = Frames.encode(msg);
        final byte[] payload = msg.pack();
        assertEquals(payload.length + 4, frame.length);
        assertEquals(payload.length, (frame[0] & 0xff) << 24 | (frame[1] & 0xff) << 16 | (frame[2] & 0xff) << 8 | frame[3] & 0xff);
        assertEquals(msg, read(frame, Frames.MAX_FRAME_SIZE));
        assertEquals(msg, read(frame, 0), "not positive is no limit");
    }

    @Test
    void theEndOfAStreamBetweenFramesIsNotAnError() throws Exception {
        assertNull(read(new byte[0], Frames.MAX_FRAME_SIZE));
    }

    @Test
    void theEndOfAStreamInsideAFrameIs() {
        final byte[] frame = Frames.encode(D.streamReady(Value.ofLong(1)));
        assertThrows(EOFException.class, () -> read(Arrays.copyOf(frame, 2), Frames.MAX_FRAME_SIZE));
        assertThrows(EOFException.class, () -> read(Arrays.copyOf(frame, 4), Frames.MAX_FRAME_SIZE));
        assertThrows(EOFException.class, () -> read(Arrays.copyOf(frame, frame.length - 1), Frames.MAX_FRAME_SIZE));
    }

    @Test
    void aFrameLongerThanTheLimitIsRefusedBeforeItIsRead() {
        final byte[] frame = Frames.encode(D.streamValue(Value.ofLong(1), Value.raw(new byte[5000])));
        final FrameException e = assertThrows(FrameException.class, () -> read(frame, 1024));
        assertTrue(e.getMessage().contains("frame too large"), e.getMessage());
        // Four bytes that claim four gigabytes cost nothing but themselves.
        final byte[] liar = {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 1, 2, 3};
        assertThrows(FrameException.class, () -> read(liar, Frames.MAX_FRAME_SIZE));
        assertThrows(FrameException.class, () -> read(liar, 0));
        // And a length that is allowed but not delivered ends as a short stream.
        final byte[] short16 = {0, (byte) 0xff, (byte) 0xff, (byte) 0xff, 1, 2, 3};
        assertThrows(EOFException.class, () -> read(short16, Frames.MAX_FRAME_SIZE));
    }

    @Test
    void whatIsNotAMapIsNotAMessage() {
        final byte[] list = Value.list(Value.ofLong(1)).pack();
        final byte[] frame = new byte[4 + list.length];
        frame[3] = (byte) list.length;
        System.arraycopy(list, 0, frame, 4, list.length);
        FrameException e = assertThrows(FrameException.class, () -> read(frame, Frames.MAX_FRAME_SIZE));
        assertEquals("expected msgpack map", e.getMessage());
        // No bytes, and bytes that are not MessagePack.
        assertThrows(FrameException.class, () -> read(new byte[] {0, 0, 0, 0}, Frames.MAX_FRAME_SIZE));
        e = assertThrows(FrameException.class, () -> read(new byte[] {0, 0, 0, 1, (byte) 0xa5}, Frames.MAX_FRAME_SIZE));
        assertTrue(e.getMessage().startsWith("msgpack unpack"), e.getMessage());
    }

    @Test
    void aBatchIsItsFramesOneAfterAnother() throws Exception {
        final List<MapValue> msgs = Arrays.asList(
                D.streamReady(Value.ofLong(5)), D.streamValue(Value.ofLong(5), Value.ofLong(1)),
                D.streamValue(Value.ofLong(5), Value.utf8("two")), D.streamEnd(Value.ofLong(5), null));
        final byte[] batch = Frames.encode(msgs);
        int at = 0;
        for (MapValue m : msgs) {
            final byte[] one = Frames.encode(m);
            assertEquals(Golden.hex(one), Golden.hex(Arrays.copyOfRange(batch, at, at + one.length)));
            at += one.length;
        }
        assertEquals(batch.length, at);
        final InputStream in = new ByteArrayInputStream(batch);
        for (MapValue m : msgs) {
            assertEquals(m, Frames.read(in, Frames.MAX_FRAME_SIZE, Limits.DEFAULT));
        }
        assertNull(Frames.read(in, Frames.MAX_FRAME_SIZE, Limits.DEFAULT));
        assertEquals(Golden.hex(Frames.encode(msgs.get(0))), Golden.hex(Frames.encode(Collections.singletonList(msgs.get(0)))));
    }

    @Test
    void anythingAPeerSendsEndsInAMessageOrAnIOException() {
        // FuzzReadMessage: the path from bytes to a message throws nothing else.
        final Random rng = new Random(20261124);
        final byte[] good = Frames.encode(D.request(MessageType.FUNCTION_REQUEST, 1, "f", Value.list(Value.ofLong(1)), 5, null));
        for (int i = 0; i < 20000; i++) {
            final byte[] bytes;
            if (i % 3 == 0) {
                bytes = good.clone();
                bytes[rng.nextInt(bytes.length)] ^= (byte) (1 << rng.nextInt(8));
            } else {
                bytes = new byte[rng.nextInt(40)];
                rng.nextBytes(bytes);
                if (bytes.length >= 4 && i % 3 == 1) {
                    bytes[0] = 0;
                    bytes[1] = 0;
                    bytes[2] = 0;
                    bytes[3] = (byte) Math.max(0, bytes.length - 4);
                }
            }
            try {
                read(bytes, 1 << 16);
            } catch (IOException expected) {
                // not a message
            }
            try {
                WireCodec.JSON.decode(bytes);
            } catch (FrameException expected) {
                // not a message
            }
        }
    }

    @Test
    void theTwoCodecs() throws Exception {
        assertEquals("msgpack", WireCodec.MSGPACK.name());
        assertEquals("vrpc.msgpack", WireCodec.MSGPACK.subprotocol());
        assertFalse(WireCodec.MSGPACK.textFrames());
        assertEquals("json", WireCodec.JSON.name());
        assertEquals("vrpc.json", WireCodec.JSON.subprotocol());
        assertTrue(WireCodec.JSON.textFrames());

        final MapValue msg = D.request(MessageType.FUNCTION_REQUEST, 1, "user.get",
                Value.list(Value.ofLong(42), Value.utf8("x"), Value.ofNull(), Value.ofBool(true),
                        Value.raw(new byte[] {0, (byte) 255}), MapValue.builder().put("deep", 1.25).build()),
                5000, null);
        assertEquals(msg, WireCodec.MSGPACK.decode(WireCodec.MSGPACK.encode(msg)));
        assertEquals(msg, WireCodec.JSON.decode(WireCodec.JSON.encode(msg)));
        assertEquals("{\"args\": [42,\"x\",null,true,\"base64,AP8\",{\"deep\": 1.25}],\"fn\": \"user.get\",\"rid\": 1,\"sla\": 5000,\"t\": 2}",
                new String(WireCodec.JSON.encode(msg), StandardCharsets.UTF_8));
    }

    @Test
    void aCodecTakesOnlyAMap() {
        assertThrows(FrameException.class, () -> WireCodec.JSON.decode("[1,2,3]".getBytes(StandardCharsets.UTF_8)));
        assertThrows(FrameException.class, () -> WireCodec.JSON.decode("\"text\"".getBytes(StandardCharsets.UTF_8)));
        assertThrows(FrameException.class, () -> WireCodec.JSON.decode("null".getBytes(StandardCharsets.UTF_8)));
        assertThrows(FrameException.class, () -> WireCodec.JSON.decode("{".getBytes(StandardCharsets.UTF_8)));
        assertThrows(FrameException.class, () -> WireCodec.JSON.decode(new byte[0]));
        assertThrows(FrameException.class, () -> WireCodec.MSGPACK.decode(Value.list().pack()));
        assertThrows(FrameException.class, () -> WireCodec.MSGPACK.decode(new byte[0]));
        // A message in the other codec is not one in this.
        final MapValue msg = D.streamReady(Value.ofLong(1));
        assertThrows(FrameException.class, () -> WireCodec.JSON.decode(WireCodec.MSGPACK.encode(msg)));
        assertThrows(FrameException.class, () -> WireCodec.MSGPACK.decode(WireCodec.JSON.encode(msg)));
    }

    @Test
    void anEnvelopeIsJsonWhateverTextItCarries() throws Exception {
        // A control character, a quote in a key, a double that is not finite:
        // before value v1.7.0 these gave a frame that no JSON reader took.
        final String text = "bell " + (char) 7 + ", escape " + (char) 27 + "[0m, tab " + (char) 11 + ", del " + (char) 127
                + ", quote \" and backslash \\, beyond the basic plane " + new String(Character.toChars(0xE0001));
        final MapValue msg = D.request(MessageType.FUNCTION_REQUEST, 1, "echo",
                MapValue.builder().put(text, text).put("line\nbreak", "tab\there").build(), 0,
                Collections.singletonMap("trace", text));
        assertEquals(msg, WireCodec.JSON.decode(WireCodec.JSON.encode(msg)));

        final MapValue numbers = D.functionResult(Value.ofLong(1), Value.list(
                Value.ofDouble(Double.POSITIVE_INFINITY), Value.ofDouble(Double.NEGATIVE_INFINITY), Value.ofDouble(Double.NaN)));
        assertEquals(Value.list(Value.ofNull(), Value.ofNull(), Value.ofNull()),
                WireCodec.JSON.decode(WireCodec.JSON.encode(numbers)).get("res"));
    }

    @Test
    void theCodecAServerServes() {
        // With both enabled: JSON to a client that asked for it, MessagePack to every other.
        assertSame(WireCodec.MSGPACK, WireCodec.forSubprotocol("", null));
        assertSame(WireCodec.MSGPACK, WireCodec.forSubprotocol(null, null));
        assertSame(WireCodec.MSGPACK, WireCodec.forSubprotocol("vrpc.msgpack", null));
        assertSame(WireCodec.JSON, WireCodec.forSubprotocol("vrpc.json", null));
        assertSame(WireCodec.MSGPACK, WireCodec.forSubprotocol("something.else", Collections.<WireCodec>emptyList()));
        // A restricted set: what was asked for when it is enabled, else the first.
        final List<WireCodec> jsonOnly = Collections.singletonList(WireCodec.JSON);
        assertSame(WireCodec.JSON, WireCodec.forSubprotocol("vrpc.json", jsonOnly));
        assertSame(WireCodec.JSON, WireCodec.forSubprotocol("", jsonOnly));
        assertSame(WireCodec.JSON, WireCodec.forSubprotocol("vrpc.msgpack", jsonOnly));
        final List<WireCodec> both = Arrays.asList(WireCodec.JSON, WireCodec.MSGPACK);
        assertSame(WireCodec.MSGPACK, WireCodec.forSubprotocol("vrpc.msgpack", both));
        assertSame(WireCodec.JSON, WireCodec.forSubprotocol("", both));
    }

    @Test
    void jsonNestedTooDeeplyIsRefused() {
        final StringBuilder deep = new StringBuilder();
        for (int i = 0; i < 1500; i++) {
            deep.append("{\"k\":");
        }
        deep.append("1");
        for (int i = 0; i < 1500; i++) {
            deep.append('}');
        }
        assertThrows(FrameException.class, () -> WireCodec.JSON.decode(deep.toString().getBytes(StandardCharsets.UTF_8)));
        assertThrows(com.arpabet.value.ValueException.class,
                () -> WireJson.parse(deep.toString().getBytes(StandardCharsets.UTF_8), Limits.DEFAULT.withMaxDepth(100)));
    }
}
