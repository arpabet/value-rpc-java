/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.value.MapValue;
import com.arpabet.value.NullValue;
import com.arpabet.value.Value;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/** The envelope: the tests of protocol_test.go and dialect_test.go of the Go implementation. */
class DialectTest {

    private static final Dialect D = Dialect.standard();

    @Test
    void handshakeRequestRoundTrip() throws Exception {
        final MapValue req = D.handshakeRequest(12345, "tok", null);
        final MapValue back = Frames.decode(req.pack(), 0, req.pack().length, com.arpabet.value.Limits.DEFAULT);
        assertEquals(req, back);
        assertSame(MessageType.HANDSHAKE_REQUEST, D.messageType(back));
        assertEquals(0, D.requestId(back).longValue());
        assertEquals(12345, Dialect.number(back, "cid").longValue());
        assertEquals("tok", Dialect.string(back, "tok").toString());
        assertTrue(D.validMagicAndVersion(back));
    }

    @Test
    void handshakeLeavesOutAnEmptyTokenAndNoCredential() {
        for (String token : new String[] {null, ""}) {
            final MapValue req = D.handshakeRequest(1, token, null);
            assertFalse(req.containsKey("tok"));
            assertFalse(req.containsKey("auth"));
        }
        assertTrue(D.handshakeRequest(1, null, NullValue.NULL).containsKey("auth"), "a null credential is one");
        assertFalse(D.handshakeResponse("").containsKey("tok"));
    }

    @Test
    void magicAndVersion() {
        assertTrue(D.validMagicAndVersion(D.handshakeRequest(1, null, null)));
        assertTrue(D.validMagicAndVersion(D.handshakeResponse(null)));
        // Another magic, or none.
        assertFalse(D.validMagicAndVersion(D.handshakeRequest(1, null, null).put("m", Value.utf8("gRPC"))));
        assertFalse(D.validMagicAndVersion(D.handshakeRequest(1, null, null).remove("m")));
        assertFalse(D.validMagicAndVersion(D.handshakeRequest(1, null, null).put("m", Value.ofLong(1))));
        // A newer version is refused; an older one, and the same as an integer, are taken.
        assertFalse(D.validMagicAndVersion(D.handshakeRequest(1, null, null).put("v", Value.ofDouble(2.0))));
        assertFalse(D.validMagicAndVersion(D.handshakeRequest(1, null, null).put("v", Value.ofDouble(1.1))));
        assertTrue(D.validMagicAndVersion(D.handshakeRequest(1, null, null).put("v", Value.ofDouble(0.9))));
        assertTrue(D.validMagicAndVersion(D.handshakeRequest(1, null, null).put("v", Value.ofLong(1))));
        // No version, or one that is not a number.
        assertFalse(D.validMagicAndVersion(D.handshakeRequest(1, null, null).remove("v")));
        assertFalse(D.validMagicAndVersion(D.handshakeRequest(1, null, null).put("v", Value.utf8("1.0"))));
    }

    @Test
    void aFieldThatIsMissingIsNotZero() {
        // getNumber gives 0 for a missing key, which is the type of a
        // handshake: a message without a type must not be read as one.
        final MapValue noType = MapValue.builder().put("rid", 7).build();
        assertNull(D.messageType(noType));
        assertNull(D.typeNumber(noType));
        assertEquals(0, noType.getNumber("t").longValue());
        assertNull(D.requestId(MapValue.builder().put("t", 2).build()));
        assertNull(D.messageType(MapValue.builder().put("t", "2").build()), "a type that is not a number");
        assertNull(D.messageType(MapValue.builder().put("t", 12).build()), "a reserved number");
        assertNull(D.messageType(MapValue.builder().put("t", 99).build()));
        assertEquals(99, D.typeNumber(MapValue.builder().put("t", 99).build()).longValue());
        assertSame(MessageType.STREAM_CREDIT, D.messageType(MapValue.builder().put("t", 14).build()));
        assertNull(D.functionName(MapValue.builder().put("fn", 5).build()));
    }

    @Test
    void messageTypesHaveTheNumbersOfTheWire() {
        final int[] numbers = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14};
        final MessageType[] types = MessageType.values();
        assertEquals(numbers.length, types.length);
        for (int i = 0; i < types.length; i++) {
            assertEquals(numbers[i], types[i].number());
            assertEquals(numbers[i], types[i].value().longValue());
            assertSame(types[i], MessageType.of(numbers[i]));
        }
        assertNull(MessageType.of(12));
        assertNull(MessageType.of(13));
        assertNull(MessageType.of(-1));
        assertNull(MessageType.of(15));
    }

    @Test
    void whatARequestCarriesAndWhatItLeavesOut() {
        final MapValue bare = D.request(MessageType.FUNCTION_REQUEST, -1, "notify", null, 0, null);
        assertEquals(MapValue.builder().put("t", 2).put("rid", -1).put("fn", "notify").build(), bare);
        assertTrue(D.arguments(bare).isNull(), "no arguments read as the null value");
        assertEquals(0, D.slaMillis(bare));
        assertTrue(D.metadata(bare).isEmpty());

        final Map<String, String> md = new LinkedHashMap<String, String>();
        md.put("traceparent", "00-t");
        final MapValue full = D.request(MessageType.CHAT_REQUEST, 9, "chat", Value.list(Value.ofLong(1)), 250, md);
        assertSame(MessageType.CHAT_REQUEST, D.messageType(full));
        assertEquals("chat", D.functionName(full));
        assertEquals(Value.list(Value.ofLong(1)), D.arguments(full));
        assertEquals(250, D.slaMillis(full));
        assertEquals(md, D.metadata(full));
        // Not positive: no deadline is sent. Empty metadata: none is sent.
        assertFalse(D.request(MessageType.FUNCTION_REQUEST, 1, "f", null, -5, Collections.<String, String>emptyMap())
                .containsKey("sla"));
        assertFalse(D.request(MessageType.FUNCTION_REQUEST, 1, "f", null, 0, Collections.<String, String>emptyMap())
                .containsKey("md"));
    }

    @Test
    void metadataKeepsTheStringsOnly() {
        final MapValue msg = MapValue.builder()
                .put("md", MapValue.builder().put("a", "x").put("n", 5).put("b", "y").build())
                .build();
        final Map<String, String> md = D.metadata(msg);
        assertEquals(2, md.size());
        assertEquals("x", md.get("a"));
        assertEquals("y", md.get("b"));
        assertThrows(UnsupportedOperationException.class, () -> md.put("c", "z"));
        assertTrue(D.metadata(MapValue.builder().put("md", "not a map").build()).isEmpty());
        assertNull(Dialect.encodeMetadata(null));
        assertNull(Dialect.encodeMetadata(Collections.<String, String>emptyMap()));
    }

    @Test
    void aResultOrAStreamValueThatIsNullIsNone() {
        assertNull(D.result(D.functionResult(Value.ofLong(1), null)));
        assertNull(D.result(D.functionResult(Value.ofLong(1), NullValue.NULL)));
        assertEquals(Value.ofLong(5), D.result(D.functionResult(Value.ofLong(1), Value.ofLong(5))));
        assertNull(D.streamValueOf(D.streamValue(Value.ofLong(1), null)));
        assertNull(D.streamValueOf(D.streamEnd(Value.ofLong(1), null)));
        assertEquals(Value.utf8("last"), D.streamValueOf(D.streamEnd(Value.ofLong(1), Value.utf8("last"))));
        assertEquals(4096, D.credit(D.streamCredit(Value.ofLong(1), 4096)).longValue());
        assertNull(D.credit(D.streamReady(Value.ofLong(1))));
    }

    @Test
    void errorsTravelWithTheirCode() {
        final VrpcException e = D.errorOf(D.error(Value.ofLong(3), Code.NOT_FOUND, "function not found x"));
        assertSame(Code.NOT_FOUND, e.code());
        assertEquals("function not found x", e.detail());
        assertEquals("vrpc NotFound: function not found x", e.getMessage());
        assertSame(Code.NOT_FOUND, Code.of(e));
        assertSame(Code.NOT_FOUND, Code.of(new IllegalStateException("wrapped", e)));
        assertSame(Code.OK, Code.of((Throwable) null));
        assertSame(Code.UNKNOWN, Code.of(new IllegalStateException("plain")));

        // A response without a code is Unknown; a number that is no code is kept.
        assertSame(Code.UNKNOWN, D.errorOf(MapValue.builder().put("t", 7).put("rid", 1).put("err", "x").build()).code());
        final VrpcException odd = D.errorOf(D.error(Value.ofLong(1), 42, "odd"));
        assertSame(Code.UNKNOWN, odd.code());
        assertEquals(42, odd.codeNumber());
        assertEquals(42, Dialect.number(D.handlerError(Value.ofLong(1), "function f", odd), "code").longValue());
        assertEquals("", D.errorOf(MapValue.builder().put("code", 9).build()).detail());
    }

    @Test
    void whatAHandlerThrewBecomesACode() {
        assertSame(Code.INTERNAL, VrpcException.handlerCode(new IllegalStateException("boom")));
        assertSame(Code.DEADLINE_EXCEEDED, VrpcException.handlerCode(new TimeoutException()));
        assertSame(Code.CANCELED, VrpcException.handlerCode(new CancellationException()));
        assertSame(Code.CANCELED, VrpcException.handlerCode(new InterruptedException()));
        assertSame(Code.CANCELED, VrpcException.handlerCode(new IllegalStateException(new InterruptedException())));
        assertSame(Code.NOT_FOUND, VrpcException.handlerCode(new VrpcException(Code.NOT_FOUND, "x")));
        assertSame(Code.NOT_FOUND,
                VrpcException.handlerCode(new IllegalStateException(new VrpcException(Code.NOT_FOUND, "x"))));
        // A failure that says Unknown says nothing: it is an internal error.
        assertSame(Code.INTERNAL, VrpcException.handlerCode(new VrpcException(Code.UNKNOWN, "x")));

        // The code is not said twice: the message of a coded failure travels without it.
        final MapValue coded = D.handlerError(Value.ofLong(4), "function fail",
                new VrpcException(Code.RESOURCE_EXHAUSTED, "deliberate failure"));
        assertEquals("function fail: deliberate failure", coded.getString("err").toString());
        assertEquals(6, coded.getNumber("code").longValue());
        assertEquals("function f: java.lang.IllegalStateException",
                D.handlerError(Value.ofLong(1), "function f", new IllegalStateException()).getString("err").toString());
    }

    @Test
    void aDialectOfOnesOwnIsIncompatibleByDesign() throws Exception {
        final Dialect custom = D.toBuilder().magic("xQ9").messageTypeField("a").requestIdField("b")
                .functionNameField("c").argumentsField("d").magicField("e").versionField("f").clientIdField("g")
                .build();
        final MapValue hs = custom.handshakeRequest(7, null, null);
        assertTrue(custom.validMagicAndVersion(hs));
        assertFalse(D.validMagicAndVersion(hs), "the standard dialect does not take it for a handshake");
        assertFalse(custom.validMagicAndVersion(D.handshakeRequest(7, null, null)));
        assertNull(D.messageType(hs));
        assertSame(MessageType.HANDSHAKE_REQUEST, custom.messageType(hs));

        final MapValue req = custom.request(MessageType.FUNCTION_REQUEST, 1, "greet", Value.list(), 0, null);
        assertEquals(MapValue.builder().put("a", 2).put("b", 1).put("c", "greet").put("d", Value.list()).build(), req);
        assertNotEquals(D.request(MessageType.FUNCTION_REQUEST, 1, "greet", Value.list(), 0, null), req);
        assertEquals("greet", custom.functionName(req));
        assertNull(D.functionName(req));
        // The standard one is what it was.
        assertEquals("vRPC", D.magic());
        assertEquals("t", D.messageTypeField());
        assertThrows(IllegalArgumentException.class, () -> D.toBuilder().magic(""));
    }
}
