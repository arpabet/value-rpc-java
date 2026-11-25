/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.Kind;
import com.arpabet.value.Limits;
import com.arpabet.value.MapValue;
import com.arpabet.value.Value;
import com.arpabet.value.ValueException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * How a message is written on a WebSocket connection: MessagePack in binary
 * frames, which is also the one codec of every other transport, or JSON in
 * text frames. The two sides agree on it once, when the connection opens, by
 * WebSocket subprotocol: a client offers the token of its codec and requires
 * the server to echo it, and a server serves MessagePack to a client that
 * offers nothing. Everything above the transport sees maps whichever codec
 * carries them. It is the Go implementation's {@code valuerpc.WireCodec}.
 */
public interface WireCodec {

    /** The subprotocol token of {@link #MSGPACK}. */
    String SUBPROTOCOL_MSGPACK = "vrpc.msgpack";

    /** The subprotocol token of {@link #JSON}. */
    String SUBPROTOCOL_JSON = "vrpc.json";

    /** MessagePack in binary frames: the canonical bytes of the map. The default. */
    WireCodec MSGPACK = new WireCodec() {
        @Override
        public String name() {
            return "msgpack";
        }

        @Override
        public String subprotocol() {
            return SUBPROTOCOL_MSGPACK;
        }

        @Override
        public boolean textFrames() {
            return false;
        }

        @Override
        public byte[] encode(MapValue message) {
            return message.pack();
        }

        @Override
        public MapValue decode(byte[] data) throws FrameException {
            return Frames.decode(data, 0, data.length, Limits.DEFAULT);
        }

        @Override
        public String toString() {
            return name();
        }
    };

    /**
     * JSON in text frames, for peers that have no MessagePack at hand. It is
     * the JSON of {@link Value#jsonify()}: binary as a {@code base64,} string,
     * a big integer or a decimal as a hexadecimal string, a timestamp as its
     * RFC 3339 text, and a double that is NaN or infinite as null. Of these
     * only binary reads back as what it was: a string is never given a type
     * by what it says, so a big integer, a decimal and a timestamp arrive as
     * text. MessagePack carries every kind exactly.
     */
    WireCodec JSON = new WireCodec() {
        @Override
        public String name() {
            return "json";
        }

        @Override
        public String subprotocol() {
            return SUBPROTOCOL_JSON;
        }

        @Override
        public boolean textFrames() {
            return true;
        }

        @Override
        public byte[] encode(MapValue message) {
            return message.jsonify().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public MapValue decode(byte[] data) throws FrameException {
            final Value v;
            try {
                v = WireJson.parse(data, Limits.DEFAULT);
            } catch (ValueException e) {
                throw new FrameException(e.getMessage(), e);
            }
            if (v.kind() != Kind.MAP) {
                throw new FrameException("expected json object envelope");
            }
            return (MapValue) v;
        }

        @Override
        public String toString() {
            return name();
        }
    };

    /** {@code msgpack} or {@code json}. */
    String name();

    /** The WebSocket subprotocol token a client offers, and a server echoes, to select the codec. */
    String subprotocol();

    /** Whether messages travel in text frames rather than binary ones. */
    boolean textFrames();

    /** The bytes of a message: of a text frame, its UTF-8. */
    byte[] encode(MapValue message);

    /**
     * The message the bytes of a frame are.
     *
     * @throws FrameException the bytes are not a message in this codec
     */
    MapValue decode(byte[] data) throws FrameException;

    /**
     * The codec a server serves a connection with, given the subprotocol that
     * was agreed on (empty or null when the client offered none).
     *
     * @param enabled the codecs the server enables, in its order of
     *                preference; null or empty for both. With both, JSON is
     *                served to a client that asked for it and MessagePack to
     *                every other; with a restricted set, a client that asked
     *                for none of it gets the first.
     */
    static WireCodec forSubprotocol(String subprotocol, List<WireCodec> enabled) {
        final String asked = subprotocol == null ? "" : subprotocol;
        if (enabled == null || enabled.isEmpty()) {
            return SUBPROTOCOL_JSON.equals(asked) ? JSON : MSGPACK;
        }
        for (WireCodec c : enabled) {
            if (c.subprotocol().equals(asked)) {
                return c;
            }
        }
        return enabled.get(0);
    }
}
