/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.Kind;
import com.arpabet.value.MapValue;
import com.arpabet.value.NullValue;
import com.arpabet.value.NumberValue;
import com.arpabet.value.StringValue;
import com.arpabet.value.Value;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The names of the fields of a message, with its magic and version, and the
 * messages built from them: the envelope of vRPC. Every message is a map, and
 * this is what writes and reads those maps.
 *
 * <p>{@link #standard()} is the wire format of the Go implementation
 * ({@code valuerpc.NewDialect}). An application may rename the fields and the
 * magic to make its protocol incompatible with the standard one by design;
 * both peers must then use the same dialect. Renaming removes the keywords a
 * filter looks for and hides nothing else: it is no substitute for TLS.
 *
 * <p>A dialect is immutable. The messages it builds are immutable maps with
 * their keys in order, so the same message is the same bytes in every
 * implementation.
 */
public final class Dialect {

    private static final Dialect STANDARD = new Builder().build();

    private final String magic;
    private final double version;
    private final long handshakeRequestId;
    private final String messageTypeField;
    private final String magicField;
    private final String versionField;
    private final String requestIdField;
    private final String timeoutField;
    private final String clientIdField;
    private final String sessionTokenField;
    private final String authField;
    private final String functionNameField;
    private final String argumentsField;
    private final String resultField;
    private final String errorField;
    private final String codeField;
    private final String creditField;
    private final String metadataField;
    private final String valueField;

    private final NumberValue handshakeId;

    private Dialect(Builder b) {
        this.magic = b.magic;
        this.version = b.version;
        this.handshakeRequestId = b.handshakeRequestId;
        this.messageTypeField = b.messageTypeField;
        this.magicField = b.magicField;
        this.versionField = b.versionField;
        this.requestIdField = b.requestIdField;
        this.timeoutField = b.timeoutField;
        this.clientIdField = b.clientIdField;
        this.sessionTokenField = b.sessionTokenField;
        this.authField = b.authField;
        this.functionNameField = b.functionNameField;
        this.argumentsField = b.argumentsField;
        this.resultField = b.resultField;
        this.errorField = b.errorField;
        this.codeField = b.codeField;
        this.creditField = b.creditField;
        this.metadataField = b.metadataField;
        this.valueField = b.valueField;
        this.handshakeId = Value.ofLong(b.handshakeRequestId);
    }

    /** The standard dialect: the default wire format. */
    public static Dialect standard() {
        return STANDARD;
    }

    /** A builder that starts as this dialect, to change some of it. */
    public Builder toBuilder() {
        return new Builder(this);
    }

    // --- what it is made of ----------------------------------------------------

    /** The magic a handshake carries, {@code vRPC}. */
    public String magic() {
        return magic;
    }

    /** The version a handshake carries, 1.0. A peer accepts any version up to its own. */
    public double version() {
        return version;
    }

    /**
     * The request id of the handshake, 0: the one integer that is neither
     * positive nor negative. The requests a client opens are numbered 1, 2, 3
     * and the ones a server opens -1, -2, -3, so the two never meet.
     */
    public long handshakeRequestId() {
        return handshakeRequestId;
    }

    public String messageTypeField() {
        return messageTypeField;
    }

    public String magicField() {
        return magicField;
    }

    public String versionField() {
        return versionField;
    }

    public String requestIdField() {
        return requestIdField;
    }

    public String timeoutField() {
        return timeoutField;
    }

    public String clientIdField() {
        return clientIdField;
    }

    public String sessionTokenField() {
        return sessionTokenField;
    }

    public String authField() {
        return authField;
    }

    public String functionNameField() {
        return functionNameField;
    }

    public String argumentsField() {
        return argumentsField;
    }

    public String resultField() {
        return resultField;
    }

    public String errorField() {
        return errorField;
    }

    public String codeField() {
        return codeField;
    }

    public String creditField() {
        return creditField;
    }

    public String metadataField() {
        return metadataField;
    }

    public String valueField() {
        return valueField;
    }

    // --- the handshake ---------------------------------------------------------

    /**
     * The handshake a client opens a connection with.
     *
     * @param clientId   the id the client chose for its session
     * @param token      the link of the client's hash chain that lets it
     *                   resume the session (see {@link HashChain}); null or
     *                   empty for a client that does not resume
     * @param credential what the server's authenticator checks; null for none
     */
    public MapValue handshakeRequest(long clientId, String token, Value credential) {
        final MapValue.Builder b = MapValue.builder()
                .put(magicField, magic)
                .put(versionField, version)
                .put(messageTypeField, MessageType.HANDSHAKE_REQUEST.value())
                .put(requestIdField, handshakeId)
                .put(clientIdField, clientId);
        if (token != null && !token.isEmpty()) {
            b.put(sessionTokenField, token);
        }
        if (credential != null) {
            b.put(authField, credential);
        }
        return b.build();
    }

    /**
     * The answer of a server that accepted a handshake. The client owns the
     * hash chain, so the server has no token to give and {@code token} is
     * null or empty; it is there for a dialect that echoes one.
     */
    public MapValue handshakeResponse(String token) {
        final MapValue.Builder b = MapValue.builder()
                .put(magicField, magic)
                .put(versionField, version)
                .put(messageTypeField, MessageType.HANDSHAKE_RESPONSE.value())
                .put(requestIdField, handshakeId);
        if (token != null && !token.isEmpty()) {
            b.put(sessionTokenField, token);
        }
        return b.build();
    }

    /**
     * Reports whether a handshake carries the magic of this dialect and a
     * version it supports: any number up to its own, whatever its type, since
     * the JSON codec writes 1.0 as 1.
     */
    public boolean validMagicAndVersion(MapValue request) {
        final StringValue m = string(request, magicField);
        if (m == null || !m.toString().equals(magic)) {
            return false;
        }
        final NumberValue v = number(request, versionField);
        return v != null && !(v.doubleValue() > version);
    }

    // --- requests --------------------------------------------------------------

    /**
     * A request that opens a call or a stream.
     *
     * @param type      {@link MessageType#FUNCTION_REQUEST} or one of the three that open a stream
     * @param requestId positive from a client, negative from a server
     * @param name      the function
     * @param args      the arguments; null leaves the field out, the null value sends it as null
     * @param slaMillis the time the caller allows, in milliseconds; left out unless positive
     * @param metadata  what travels with the request, such as a trace context; left out when null or empty
     */
    public MapValue request(MessageType type, long requestId, String name, Value args, long slaMillis,
            Map<String, String> metadata) {
        final MapValue.Builder b = MapValue.builder()
                .put(messageTypeField, type.value())
                .put(requestIdField, requestId)
                .put(functionNameField, name);
        if (args != null) {
            b.put(argumentsField, args);
        }
        if (slaMillis > 0) {
            b.put(timeoutField, slaMillis);
        }
        final MapValue md = encodeMetadata(metadata);
        if (md != null) {
            b.put(metadataField, md);
        }
        return b.build();
    }

    /** The answer of a call. A null result leaves the field out: the call returned nothing. */
    public MapValue functionResult(NumberValue requestId, Value result) {
        final MapValue.Builder b = MapValue.builder()
                .put(messageTypeField, MessageType.FUNCTION_RESPONSE.value())
                .put(requestIdField, requestId);
        if (result != null) {
            b.put(resultField, result);
        }
        return b.build();
    }

    /** The answer of a request that failed, with the number of its {@link Code}. */
    public MapValue error(NumberValue requestId, int codeNumber, String message) {
        return MapValue.builder()
                .put(messageTypeField, MessageType.ERROR_RESPONSE.value())
                .put(requestIdField, requestId)
                .put(codeField, codeNumber)
                .put(errorField, message == null ? "" : message)
                .build();
    }

    /** The answer of a request that failed. */
    public MapValue error(NumberValue requestId, Code code, String message) {
        return error(requestId, code.number(), message);
    }

    /**
     * The answer of a request whose handler threw: the code the error carries
     * (see {@link VrpcException#handlerCode}), and {@code where: message}.
     *
     * @param where what failed, such as {@code function greet}
     */
    public MapValue handlerError(NumberValue requestId, String where, Throwable error) {
        return error(requestId, VrpcException.handlerCodeNumber(error),
                where + ": " + VrpcException.handlerDetail(error));
    }

    // --- streams ---------------------------------------------------------------

    /** What the responder of a stream sends once its handler has taken the stream. */
    public MapValue streamReady(NumberValue requestId) {
        return MapValue.builder()
                .put(messageTypeField, MessageType.STREAM_READY.value())
                .put(requestIdField, requestId)
                .build();
    }

    /** One value of a stream. */
    public MapValue streamValue(NumberValue requestId, Value value) {
        return MapValue.builder()
                .put(messageTypeField, MessageType.STREAM_VALUE.value())
                .put(requestIdField, requestId)
                .put(valueField, value == null ? NullValue.NULL : value)
                .build();
    }

    /** The end of a stream, with a last value unless it is null. */
    public MapValue streamEnd(NumberValue requestId, Value value) {
        final MapValue.Builder b = MapValue.builder()
                .put(messageTypeField, MessageType.STREAM_END.value())
                .put(requestIdField, requestId);
        if (value != null) {
            b.put(valueField, value);
        }
        return b.build();
    }

    /** Cancels a request. It is not answered. */
    public MapValue cancelRequest(NumberValue requestId) {
        return MapValue.builder()
                .put(messageTypeField, MessageType.CANCEL_REQUEST.value())
                .put(requestIdField, requestId)
                .build();
    }

    /** Grants the sender of a stream leave to send {@code credit} more values. */
    public MapValue streamCredit(NumberValue requestId, long credit) {
        return MapValue.builder()
                .put(messageTypeField, MessageType.STREAM_CREDIT.value())
                .put(requestIdField, requestId)
                .put(creditField, credit)
                .build();
    }

    // --- reading ---------------------------------------------------------------

    /**
     * The number under a field, or null when there is none or it is not a
     * number. {@link MapValue#getNumber} gives 0 for a field that is missing,
     * which reads a message without a type as a handshake: whether a field is
     * there is asked here.
     */
    public static NumberValue number(MapValue message, String field) {
        final Value v = message.get(field);
        return v.kind() == Kind.NUMBER ? (NumberValue) v : null;
    }

    /** The string under a field, text or binary, or null when there is none or it is not a string. */
    public static StringValue string(MapValue message, String field) {
        final Value v = message.get(field);
        return v.kind() == Kind.STRING ? (StringValue) v : null;
    }

    /** The number of the type of a message, or null when it has none. */
    public NumberValue typeNumber(MapValue message) {
        return number(message, messageTypeField);
    }

    /** The type of a message, or null when it has none or one this implementation does not know. */
    public MessageType messageType(MapValue message) {
        final NumberValue n = number(message, messageTypeField);
        return n == null ? null : MessageType.of(n.longValue());
    }

    /** The request id of a message, or null when it has none. */
    public NumberValue requestId(MapValue message) {
        return number(message, requestIdField);
    }

    /** The name of the function a request is for, or null when it has none. */
    public String functionName(MapValue message) {
        final StringValue s = string(message, functionNameField);
        return s == null ? null : s.toString();
    }

    /** The arguments of a request; the null value when it has none. */
    public Value arguments(MapValue message) {
        return message.get(argumentsField);
    }

    /** The result of a call, or null when it returned nothing: the field is missing or null. */
    public Value result(MapValue message) {
        final Value v = message.get(resultField);
        return v.isNull() ? null : v;
    }

    /** The value a stream message carries, or null when it carries none: the field is missing or null. */
    public Value streamValueOf(MapValue message) {
        final Value v = message.get(valueField);
        return v.isNull() ? null : v;
    }

    /** The credit a message grants, or null when it grants none. */
    public NumberValue credit(MapValue message) {
        return number(message, creditField);
    }

    /** The time the caller allows, in milliseconds, or 0 when the request does not say. */
    public long slaMillis(MapValue message) {
        final NumberValue n = number(message, timeoutField);
        return n == null ? 0 : n.longValue();
    }

    /** The failure an error response carries. A response without a code is {@link Code#UNKNOWN}. */
    public VrpcException errorOf(MapValue message) {
        final NumberValue code = number(message, codeField);
        final Value err = message.get(errorField);
        return VrpcException.ofNumber(code == null ? Code.UNKNOWN.number() : code.longValue(),
                err.isNull() ? "" : err.toString());
    }

    /**
     * What travels with a request: the entries of its metadata that are
     * strings. Empty when it has none; never null, and not to be changed.
     */
    public Map<String, String> metadata(MapValue message) {
        final Value v = message.get(metadataField);
        if (v.kind() != Kind.MAP) {
            return Collections.emptyMap();
        }
        final MapValue m = (MapValue) v;
        if (m.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<String, String> out = new LinkedHashMap<String, String>(Math.max(4, m.size() * 2));
        for (MapValue.Entry e : m) {
            if (e.value().kind() == Kind.STRING) {
                out.put(e.key(), e.value().toString());
            }
        }
        return Collections.unmodifiableMap(out);
    }

    /** The metadata of a request as the map an envelope holds, or null when there is none. */
    public static MapValue encodeMetadata(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return null;
        }
        final MapValue.Builder b = MapValue.builder();
        for (Map.Entry<String, String> e : metadata.entrySet()) {
            if (e.getKey() != null && e.getValue() != null) {
                b.put(e.getKey(), e.getValue());
            }
        }
        return b.build();
    }

    // --- building one ----------------------------------------------------------

    /** Builds a dialect. It starts as the standard one. */
    public static final class Builder {
        private String magic = "vRPC";
        private double version = 1.0;
        private long handshakeRequestId = 0;
        private String messageTypeField = "t";
        private String magicField = "m";
        private String versionField = "v";
        private String requestIdField = "rid";
        private String timeoutField = "sla";
        private String clientIdField = "cid";
        private String sessionTokenField = "tok";
        private String authField = "auth";
        private String functionNameField = "fn";
        private String argumentsField = "args";
        private String resultField = "res";
        private String errorField = "err";
        private String codeField = "code";
        private String creditField = "cr";
        private String metadataField = "md";
        private String valueField = "val";

        public Builder() {
        }

        private Builder(Dialect d) {
            this.magic = d.magic;
            this.version = d.version;
            this.handshakeRequestId = d.handshakeRequestId;
            this.messageTypeField = d.messageTypeField;
            this.magicField = d.magicField;
            this.versionField = d.versionField;
            this.requestIdField = d.requestIdField;
            this.timeoutField = d.timeoutField;
            this.clientIdField = d.clientIdField;
            this.sessionTokenField = d.sessionTokenField;
            this.authField = d.authField;
            this.functionNameField = d.functionNameField;
            this.argumentsField = d.argumentsField;
            this.resultField = d.resultField;
            this.errorField = d.errorField;
            this.codeField = d.codeField;
            this.creditField = d.creditField;
            this.metadataField = d.metadataField;
            this.valueField = d.valueField;
        }

        public Builder magic(String magic) {
            this.magic = name(magic);
            return this;
        }

        public Builder version(double version) {
            this.version = version;
            return this;
        }

        public Builder handshakeRequestId(long id) {
            this.handshakeRequestId = id;
            return this;
        }

        public Builder messageTypeField(String name) {
            this.messageTypeField = name(name);
            return this;
        }

        public Builder magicField(String name) {
            this.magicField = name(name);
            return this;
        }

        public Builder versionField(String name) {
            this.versionField = name(name);
            return this;
        }

        public Builder requestIdField(String name) {
            this.requestIdField = name(name);
            return this;
        }

        public Builder timeoutField(String name) {
            this.timeoutField = name(name);
            return this;
        }

        public Builder clientIdField(String name) {
            this.clientIdField = name(name);
            return this;
        }

        public Builder sessionTokenField(String name) {
            this.sessionTokenField = name(name);
            return this;
        }

        public Builder authField(String name) {
            this.authField = name(name);
            return this;
        }

        public Builder functionNameField(String name) {
            this.functionNameField = name(name);
            return this;
        }

        public Builder argumentsField(String name) {
            this.argumentsField = name(name);
            return this;
        }

        public Builder resultField(String name) {
            this.resultField = name(name);
            return this;
        }

        public Builder errorField(String name) {
            this.errorField = name(name);
            return this;
        }

        public Builder codeField(String name) {
            this.codeField = name(name);
            return this;
        }

        public Builder creditField(String name) {
            this.creditField = name(name);
            return this;
        }

        public Builder metadataField(String name) {
            this.metadataField = name(name);
            return this;
        }

        public Builder valueField(String name) {
            this.valueField = name(name);
            return this;
        }

        private static String name(String s) {
            if (s == null || s.isEmpty()) {
                throw new IllegalArgumentException("a field name and the magic cannot be empty");
            }
            return s;
        }

        public Dialect build() {
            return new Dialect(this);
        }
    }
}
