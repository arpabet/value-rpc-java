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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

/**
 * The framing of messages on a byte stream (TCP, TLS, a Unix socket, any
 * stream of one's own):
 *
 * <pre>
 *   [4-byte big-endian length N][N bytes: the MessagePack of one map]
 * </pre>
 *
 * The length does not count its own four bytes. It is the framing of the Go
 * implementation ({@code valuerpc.NewMsgConn}). A WebSocket carries one
 * message per frame of its own and has no length prefix.
 */
public final class Frames {

    /**
     * The longest message taken from a peer unless a connection says
     * otherwise: 16 MiB. Four bytes of length could ask for four gigabytes.
     */
    public static final int MAX_FRAME_SIZE = 16 * 1024 * 1024;

    /**
     * The most messages a sender writes at once. It writes together what is
     * already waiting, never waiting for more, so a burst costs one write.
     */
    public static final int MAX_COALESCE = 64;

    /** What is allocated before any of a frame has arrived. */
    private static final int FIRST_CHUNK = 64 * 1024;

    private Frames() {
    }

    /** The frame of a message: its length and its bytes. */
    public static byte[] encode(MapValue message) {
        final byte[] payload = message.pack();
        final byte[] frame = new byte[4 + payload.length];
        putLength(frame, 0, payload.length);
        System.arraycopy(payload, 0, frame, 4, payload.length);
        return frame;
    }

    /** The frames of several messages one after another, to be written with one write. */
    public static byte[] encode(List<MapValue> messages) {
        if (messages.size() == 1) {
            return encode(messages.get(0));
        }
        final byte[][] payloads = new byte[messages.size()][];
        long total = 0;
        for (int i = 0; i < payloads.length; i++) {
            payloads[i] = messages.get(i).pack();
            total += 4L + payloads[i].length;
        }
        if (total > Integer.MAX_VALUE - 8) {
            throw new ValueException("a batch of " + total + " bytes is too long to write at once");
        }
        final byte[] out = new byte[(int) total];
        int at = 0;
        for (byte[] payload : payloads) {
            putLength(out, at, payload.length);
            System.arraycopy(payload, 0, out, at + 4, payload.length);
            at += 4 + payload.length;
        }
        return out;
    }

    private static void putLength(byte[] out, int at, int n) {
        out[at] = (byte) (n >>> 24);
        out[at + 1] = (byte) (n >>> 16);
        out[at + 2] = (byte) (n >>> 8);
        out[at + 3] = (byte) n;
    }

    /**
     * Reads the next message of a stream.
     *
     * @param maxFrameSize the longest message taken; not positive for no limit
     * @return the message, or null when the stream ended between two messages
     * @throws FrameException what came is not a message
     * @throws EOFException   the stream ended inside a message
     */
    public static MapValue read(InputStream in, int maxFrameSize, Limits limits) throws IOException {
        final int first = in.read();
        if (first < 0) {
            return null;
        }
        long n = first & 0xffL;
        for (int i = 0; i < 3; i++) {
            final int b = in.read();
            if (b < 0) {
                throw new EOFException("the stream ended inside the length of a frame");
            }
            n = n << 8 | b;
        }
        if (maxFrameSize > 0 && n > maxFrameSize) {
            throw new FrameException("frame too large: " + n + " bytes (max " + maxFrameSize + ")");
        }
        if (n > Integer.MAX_VALUE - 8) {
            throw new FrameException("frame too large: " + n + " bytes");
        }
        final byte[] payload = readFully(in, (int) n);
        return decode(payload, 0, payload.length, limits);
    }

    /**
     * Reads {@code n} bytes. The array grows as the bytes arrive, so that a
     * length a peer claims and does not deliver allocates little.
     */
    private static byte[] readFully(InputStream in, int n) throws IOException {
        byte[] buf = new byte[Math.min(n, FIRST_CHUNK)];
        int have = 0;
        while (have < n) {
            if (have == buf.length) {
                buf = Arrays.copyOf(buf, (int) Math.min((long) n, (long) buf.length * 2));
            }
            final int got = in.read(buf, have, buf.length - have);
            if (got < 0) {
                throw new EOFException("the stream ended inside a frame: " + have + " of " + n + " bytes");
            }
            have += got;
        }
        return buf;
    }

    /**
     * The message that the bytes of a frame, or of a WebSocket frame, are.
     *
     * @throws FrameException the bytes are not a value, or not a map
     */
    public static MapValue decode(byte[] payload, int offset, int length, Limits limits) throws FrameException {
        final Value v;
        try {
            v = Value.unpack(payload, offset, length, limits);
        } catch (ValueException e) {
            throw new FrameException("msgpack unpack, " + e.getMessage(), e);
        }
        if (v.kind() != Kind.MAP) {
            throw new FrameException("expected msgpack map");
        }
        return (MapValue) v;
    }
}
