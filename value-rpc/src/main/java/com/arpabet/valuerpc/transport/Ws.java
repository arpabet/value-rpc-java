/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.transport;

import com.arpabet.value.MapValue;
import com.arpabet.value.ValueException;
import com.arpabet.valuerpc.FrameException;
import com.arpabet.valuerpc.WireCodec;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * WebSocket, the side that listens: what a browser, or any client that has a
 * WebSocket and no socket, connects to. A message travels as one WebSocket
 * message, with no length before it: MessagePack in a binary frame or JSON in
 * a text one, as the two ends agree by subprotocol when the connection opens
 * (see {@link WireCodec}). It is the Go implementation's
 * {@code NewWebSocketListener}.
 *
 * <p>This is the protocol of RFC 6455 on a socket, with a thread for each
 * connection, as the TCP listener has: the request that asks to be upgraded,
 * frames and their masks, messages in several frames, ping and pong, and the
 * close. It takes no extension, so nothing is compressed, and it does not
 * serve TLS.
 */
final class Ws {

    /** What RFC 6455 has a server hash with the key of a client, to show that it is a WebSocket server. */
    private static final String GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    /** The most a request to be upgraded may take, with its headers. */
    private static final int MAX_HEAD = 16 * 1024;

    private static final int CONTINUATION = 0x0;
    private static final int TEXT = 0x1;
    private static final int BINARY = 0x2;
    private static final int CLOSE = 0x8;
    private static final int PING = 0x9;
    private static final int PONG = 0xA;

    /** Why a connection is closed, as a close frame says it. */
    private static final int NORMAL = 1000;
    private static final int PROTOCOL_ERROR = 1002;
    private static final int UNSUPPORTED_DATA = 1003;
    private static final int TOO_BIG = 1009;

    /** How long a connection that was closed here waits for its peer to close too. */
    private static final int CLOSE_WAIT_MILLIS = 1000;

    /** What is allocated before any of a frame has arrived. */
    private static final int FIRST_CHUNK = 64 * 1024;

    private static final byte[] NOTHING = new byte[0];

    private Ws() {
    }

    /** A listener on {@code host:port/path}; the path is {@code /} when there is none. */
    static Listener listen(String hostPortPath, TransportOptions options, WebSocketOptions ws) throws IOException {
        final int slash = hostPortPath.indexOf('/');
        final String hostPort = slash < 0 ? hostPortPath : hostPortPath.substring(0, slash);
        final String path = slash < 0 ? "/" : hostPortPath.substring(slash);
        final ServerSocket server = new ServerSocket();
        try {
            server.setReuseAddress(true);
            server.bind(Tcp.address(hostPort, true));
        } catch (IOException | RuntimeException e) {
            server.close();
            throw e;
        }
        return new Tcp.SocketListener(server, "ws") {
            @Override
            void accepted(Socket socket, Acceptor acceptor) {
                // The request of a client is read on a thread of its own: one
                // that takes its time must not hold up the next.
                Threads.daemon("vrpc-ws-upgrade " + socket.getRemoteSocketAddress(), () -> {
                    MsgConn conn = null;
                    try {
                        conn = upgrade(socket, path, options, ws);
                    } catch (IOException | RuntimeException e) {
                        // dropped below
                    }
                    if (conn == null) {
                        closeQuietly(socket);
                    } else {
                        acceptor.onConnection(conn);
                    }
                }).start();
            }

            @Override
            public String address() {
                return super.address() + ("/".equals(path) ? "" : path);
            }
        };
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // closing is all that was wanted
        }
    }

    // --- the request to be upgraded ------------------------------------------------

    /**
     * Reads the request of a client and answers it. Gives the connection, or
     * null when the request was refused, which the client has been told.
     */
    private static MsgConn upgrade(Socket socket, String path, TransportOptions options, WebSocketOptions ws)
            throws IOException {
        Tcp.tune(socket, options);
        final long timeout = ws.handshakeTimeoutMillis();
        final long deadline = timeout > 0 ? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout) : 0;
        socket.setSoTimeout(timeout > 0 ? (int) Math.min(timeout, Integer.MAX_VALUE) : 0);
        final InputStream in = new BufferedInputStream(socket.getInputStream(), 64 * 1024);
        final OutputStream out = socket.getOutputStream();

        final Map<String, String> headers = new HashMap<String, String>();
        final String[] line = readHead(in, deadline, headers);
        if (line == null) {
            refuse(out, "400 Bad Request", null);
            return null;
        }
        if (!line[0].equals("GET")) {
            refuse(out, "405 Method Not Allowed", "Allow: GET\r\n");
            return null;
        }
        final int query = line[1].indexOf('?');
        if (!(query < 0 ? line[1] : line[1].substring(0, query)).equals(path)) {
            refuse(out, "404 Not Found", null);
            return null;
        }
        if (!hasToken(headers.get("connection"), "upgrade") || !hasToken(headers.get("upgrade"), "websocket")) {
            refuse(out, "426 Upgrade Required", "Connection: Upgrade\r\nUpgrade: websocket\r\n");
            return null;
        }
        if (!"13".equals(trimmed(headers.get("sec-websocket-version")))) {
            refuse(out, "400 Bad Request", "Sec-WebSocket-Version: 13\r\n");
            return null;
        }
        final String key = trimmed(headers.get("sec-websocket-key"));
        if (key.isEmpty()) {
            refuse(out, "400 Bad Request", null);
            return null;
        }
        if (!originAllowed(headers.get("origin"), headers.get("host"), ws.originPatterns())) {
            refuse(out, "403 Forbidden", null);
            return null;
        }

        // The first of the codecs of this side that the client offers, by the
        // name the client gave it; a client that offers none is told none.
        final List<WireCodec> enabled = ws.codecs().isEmpty() ? Arrays.asList(WireCodec.MSGPACK, WireCodec.JSON) : ws.codecs();
        String agreed = null;
        final List<String> offered = tokens(headers.get("sec-websocket-protocol"));
        for (WireCodec c : enabled) {
            for (String o : offered) {
                if (agreed == null && o.equalsIgnoreCase(c.subprotocol())) {
                    agreed = o;
                }
            }
        }
        final WireCodec codec = WireCodec.forSubprotocol(agreed == null ? "" : agreed.toLowerCase(Locale.ROOT), ws.codecs());

        final StringBuilder answer = new StringBuilder(192)
                .append("HTTP/1.1 101 Switching Protocols\r\n")
                .append("Upgrade: websocket\r\n")
                .append("Connection: Upgrade\r\n")
                .append("Sec-WebSocket-Accept: ").append(accept(key)).append("\r\n");
        if (agreed != null) {
            answer.append("Sec-WebSocket-Protocol: ").append(agreed).append("\r\n");
        }
        answer.append("\r\n");
        out.write(answer.toString().getBytes(StandardCharsets.ISO_8859_1));
        out.flush();
        socket.setSoTimeout(0);
        return new WsConn(socket, in, out, codec, options, ws);
    }

    private static void refuse(OutputStream out, String status, String headers) throws IOException {
        out.write(("HTTP/1.1 " + status + "\r\n" + (headers == null ? "" : headers)
                + "Connection: close\r\nContent-Length: 0\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1));
        out.flush();
    }

    /**
     * Reads the head of a request, to its empty line.
     *
     * @param headers takes the headers, by their names in lower case; one
     *                that comes twice has its values joined with a comma
     * @return the method and what is asked for, or null when what came is not a request
     */
    private static String[] readHead(InputStream in, long deadlineNanos, Map<String, String> headers) throws IOException {
        final ByteArrayOutputStream head = new ByteArrayOutputStream(512);
        int ends = 0; // how many of the four bytes that end a head have come
        while (ends < 4) {
            if (deadlineNanos != 0 && System.nanoTime() > deadlineNanos) {
                throw new SocketTimeoutException("the request took too long");
            }
            final int b = in.read();
            if (b < 0) {
                throw new EOFException("the stream ended inside the request");
            }
            if (head.size() >= MAX_HEAD) {
                return null;
            }
            head.write(b);
            if (b == (ends % 2 == 0 ? '\r' : '\n')) {
                ends++;
            } else {
                ends = b == '\r' ? 1 : 0;
            }
        }
        final String[] lines = new String(head.toByteArray(), StandardCharsets.ISO_8859_1).split("\r\n");
        final String[] first = lines.length == 0 ? new String[0] : lines[0].split(" ");
        if (first.length != 3 || !first[2].startsWith("HTTP/1.")) {
            return null;
        }
        for (int i = 1; i < lines.length; i++) {
            final int colon = lines[i].indexOf(':');
            if (colon <= 0) {
                return null;
            }
            final String name = lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT);
            final String value = lines[i].substring(colon + 1).trim();
            final String before = headers.get(name);
            headers.put(name, before == null ? value : before + ", " + value);
        }
        return first;
    }

    private static String trimmed(String value) {
        return value == null ? "" : value.trim();
    }

    /** The words of a header that is a list, such as {@code keep-alive, Upgrade}. */
    private static List<String> tokens(String value) {
        final List<String> out = new ArrayList<String>();
        if (value != null) {
            for (String t : value.split(",")) {
                final String s = t.trim();
                if (!s.isEmpty()) {
                    out.add(s);
                }
            }
        }
        return out;
    }

    private static boolean hasToken(String value, String token) {
        for (String t : tokens(value)) {
            if (t.equalsIgnoreCase(token)) {
                return true;
            }
        }
        return false;
    }

    /** What a server answers the key of a client with: the SHA-1 of the key and a fixed text, in base64. */
    private static String accept(String key) {
        try {
            final MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            return Base64.getEncoder().encodeToString(sha1.digest((key + GUID).getBytes(StandardCharsets.ISO_8859_1)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("no SHA-1", e);
        }
    }

    /**
     * Whether a request may come from the page it says it comes from. A
     * request without an origin is not from a browser and is let in; one from
     * the host it is itself sent to is the page of this server; any other has
     * to match a pattern. This is what keeps a page of another site from
     * using a server that the browser of a user can reach.
     */
    static boolean originAllowed(String origin, String host, List<String> patterns) {
        if (origin == null || origin.trim().isEmpty()) {
            return true;
        }
        final String whole = origin.trim().toLowerCase(Locale.ROOT);
        final int scheme = whole.indexOf("://");
        String from = scheme < 0 ? "" : whole.substring(scheme + 3);
        final int slash = from.indexOf('/');
        if (slash >= 0) {
            from = from.substring(0, slash);
        }
        if (!from.isEmpty() && host != null && from.equalsIgnoreCase(host.trim())) {
            return true;
        }
        for (String p : patterns) {
            final String pattern = p.toLowerCase(Locale.ROOT);
            if (matches(pattern, 0, pattern.contains("://") ? whole : from, 0)) {
                return true;
            }
        }
        return false;
    }

    /** Whether a text is what a pattern describes, in which {@code *} is any run of characters and {@code ?} any one. */
    private static boolean matches(String pattern, int p, String text, int t) {
        while (p < pattern.length()) {
            final char c = pattern.charAt(p);
            if (c == '*') {
                for (int skip = t; skip <= text.length(); skip++) {
                    if (matches(pattern, p + 1, text, skip)) {
                        return true;
                    }
                }
                return false;
            }
            if (t >= text.length() || (c != '?' && c != text.charAt(t))) {
                return false;
            }
            p++;
            t++;
        }
        return t == text.length();
    }

    // --- frames --------------------------------------------------------------------

    /** A frame as a server sends it: whole, and without a mask. */
    private static byte[] frame(int opcode, byte[] payload) {
        final byte[] out = new byte[headLength(payload.length) + payload.length];
        final int at = putHead(out, 0, opcode, payload.length);
        System.arraycopy(payload, 0, out, at, payload.length);
        return out;
    }

    private static int headLength(int payload) {
        return payload < 126 ? 2 : payload <= 0xffff ? 4 : 10;
    }

    private static int putHead(byte[] out, int at, int opcode, int payload) {
        out[at++] = (byte) (0x80 | opcode);
        if (payload < 126) {
            out[at++] = (byte) payload;
        } else if (payload <= 0xffff) {
            out[at++] = 126;
            out[at++] = (byte) (payload >>> 8);
            out[at++] = (byte) payload;
        } else {
            out[at++] = 127;
            out[at++] = 0;
            out[at++] = 0;
            out[at++] = 0;
            out[at++] = 0;
            out[at++] = (byte) (payload >>> 24);
            out[at++] = (byte) (payload >>> 16);
            out[at++] = (byte) (payload >>> 8);
            out[at++] = (byte) payload;
        }
        return at;
    }

    /** The payload of a close frame: why, as its number. */
    private static byte[] status(int code) {
        return new byte[] {(byte) (code >>> 8), (byte) code};
    }

    /** What a peer sent breaks the protocol: the connection is closed, and the peer told why by this number. */
    private static final class Violation extends FrameException {
        private static final long serialVersionUID = 1L;
        final int code;

        Violation(int code, String message) {
            super(message);
            this.code = code;
        }
    }

    /** One connection: a socket that carries WebSocket frames, read by a thread of its own. */
    private static final class WsConn implements MsgConn {
        private final Socket socket;
        private final InputStream in;
        private final OutputStream out;
        private final WireCodec codec;
        private final String remoteAddress;
        private final int maxFrameSize;
        private final long writeTimeoutMillis;
        private final int pingIntervalMillis;

        private final ReentrantLock writeLock = new ReentrantLock();
        private final AtomicBoolean started = new AtomicBoolean();
        /** Closed from this side: nothing more is written, and nothing more handed over. */
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicBoolean socketClosed = new AtomicBoolean();
        /** A ping was sent and nothing has come since. Only the thread that reads touches it. */
        private boolean pinged;

        WsConn(Socket socket, InputStream in, OutputStream out, WireCodec codec, TransportOptions options,
                WebSocketOptions ws) {
            this.socket = socket;
            this.in = in;
            this.out = out;
            this.codec = codec;
            this.remoteAddress = String.valueOf(socket.getRemoteSocketAddress());
            this.maxFrameSize = options.maxFrameSize();
            this.writeTimeoutMillis = options.writeTimeoutMillis();
            this.pingIntervalMillis = (int) Math.max(0, Math.min(ws.pingIntervalMillis(), Integer.MAX_VALUE));
        }

        @Override
        public void start(Receiver receiver) {
            if (!started.compareAndSet(false, true)) {
                throw new IllegalStateException("the connection was started already");
            }
            Threads.daemon("vrpc-read " + remoteAddress, () -> read(receiver)).start();
        }

        // --- reading ---

        private void read(Receiver receiver) {
            Throwable cause = null;
            try {
                // A read that waits this long finds the connection silent, and asks the peer whether it is there.
                socket.setSoTimeout(pingIntervalMillis);
                byte[] message = null; // the frames so far of a message that comes in several
                int kind = 0;
                for (;;) {
                    final int b0 = readByte();
                    if (b0 < 0) {
                        break; // the peer is gone, between two frames
                    }
                    final int b1 = readByte();
                    if (b1 < 0) {
                        throw new EOFException("the stream ended inside a frame");
                    }
                    final boolean fin = (b0 & 0x80) != 0;
                    final int opcode = b0 & 0x0f;
                    if ((b0 & 0x70) != 0) {
                        throw new Violation(PROTOCOL_ERROR, "a frame with the bits of an extension, and none was agreed on");
                    }
                    if ((b1 & 0x80) == 0) {
                        throw new Violation(PROTOCOL_ERROR, "a frame of a client without a mask");
                    }
                    long length = b1 & 0x7f;
                    if (length == 126) {
                        length = readNumber(2);
                    } else if (length == 127) {
                        length = readNumber(8);
                        if (length < 0) {
                            throw new Violation(PROTOCOL_ERROR, "a frame of a length that is not one");
                        }
                    }
                    final byte[] mask = new byte[4];
                    readFully(mask, 4);
                    pinged = false;

                    if (opcode >= CLOSE) {
                        if (!fin || length > 125) {
                            throw new Violation(PROTOCOL_ERROR, "a control frame in parts, or too long");
                        }
                        final byte[] payload = readPayload((int) length, mask);
                        if (opcode == PING) {
                            control(PONG, payload);
                        } else if (opcode == CLOSE) {
                            // The peer closes, or answers the close of this side.
                            if (closed.compareAndSet(false, true)) {
                                control(CLOSE, payload.length >= 2 ? Arrays.copyOf(payload, 2) : NOTHING);
                            }
                            break;
                        } else if (opcode != PONG) {
                            throw new Violation(PROTOCOL_ERROR, "a frame of a kind that is not known: " + opcode);
                        }
                        continue;
                    }

                    if (opcode == CONTINUATION) {
                        if (message == null) {
                            throw new Violation(PROTOCOL_ERROR, "the next part of a message that was not begun");
                        }
                    } else if (opcode == TEXT || opcode == BINARY) {
                        if (message != null) {
                            throw new Violation(PROTOCOL_ERROR, "a message begun inside another");
                        }
                        kind = opcode;
                    } else {
                        throw new Violation(PROTOCOL_ERROR, "a frame of a kind that is not known: " + opcode);
                    }
                    final long total = (message == null ? 0 : message.length) + length;
                    if ((maxFrameSize > 0 && total > maxFrameSize) || total > Integer.MAX_VALUE - 8) {
                        throw new Violation(TOO_BIG, "frame too large: " + total + " bytes (max " + maxFrameSize + ")");
                    }
                    final byte[] payload = readPayload((int) length, mask);
                    if (message == null) {
                        message = payload;
                    } else {
                        final byte[] joined = Arrays.copyOf(message, message.length + payload.length);
                        System.arraycopy(payload, 0, joined, message.length, payload.length);
                        message = joined;
                    }
                    if (!fin) {
                        continue;
                    }
                    final byte[] whole = message;
                    message = null;
                    if (closed.get()) {
                        continue; // closed here: what still comes is not handed over
                    }
                    if (codec.textFrames() != (kind == TEXT)) {
                        // The mistake a client of one's own making makes first:
                        // JSON in text frames, and the codec never agreed on.
                        throw new Violation(UNSUPPORTED_DATA, kind == TEXT
                                ? "expected a binary websocket frame, got text; JSON codec not negotiated (offer subprotocol \""
                                        + WireCodec.SUBPROTOCOL_JSON + "\")"
                                : "expected a text websocket frame for the json codec, got binary");
                    }
                    receiver.onMessage(codec.decode(whole));
                }
            } catch (Violation e) {
                cause = e;
                if (closed.compareAndSet(false, true)) {
                    try {
                        control(CLOSE, status(e.code));
                    } catch (IOException ignored) {
                        // it is closed either way
                    }
                }
            } catch (IOException e) {
                // A read that failed because the connection was closed here is the close.
                cause = closed.get() ? null : e;
            } catch (RuntimeException e) {
                cause = e;
            } finally {
                closed.set(true);
                closeSocket();
                receiver.onClosed(cause);
            }
        }

        /** One byte, or -1 at the end of the stream. */
        private int readByte() throws IOException {
            for (;;) {
                try {
                    return in.read();
                } catch (SocketTimeoutException e) {
                    silent();
                }
            }
        }

        private void readFully(byte[] buf, int length) throws IOException {
            int have = 0;
            while (have < length) {
                final int got;
                try {
                    got = in.read(buf, have, length - have);
                } catch (SocketTimeoutException e) {
                    silent();
                    continue;
                }
                if (got < 0) {
                    throw new EOFException("the stream ended inside a frame");
                }
                have += got;
            }
        }

        private long readNumber(int bytes) throws IOException {
            final byte[] b = new byte[bytes];
            readFully(b, bytes);
            long n = 0;
            for (int i = 0; i < bytes; i++) {
                n = n << 8 | (b[i] & 0xff);
            }
            return n;
        }

        /**
         * Reads the payload of a frame and takes its mask off. The array
         * grows as the bytes arrive, so that a length a peer claims and does
         * not deliver allocates little.
         */
        private byte[] readPayload(int length, byte[] mask) throws IOException {
            byte[] buf = new byte[Math.min(length, FIRST_CHUNK)];
            int have = 0;
            while (have < length) {
                if (have == buf.length) {
                    buf = Arrays.copyOf(buf, (int) Math.min((long) length, (long) buf.length * 2));
                }
                final int got;
                try {
                    got = in.read(buf, have, buf.length - have);
                } catch (SocketTimeoutException e) {
                    silent();
                    continue;
                }
                if (got < 0) {
                    throw new EOFException("the stream ended inside a frame: " + have + " of " + length + " bytes");
                }
                have += got;
            }
            for (int i = 0; i < length; i++) {
                buf[i] ^= mask[i & 3];
            }
            return buf;
        }

        /**
         * Nothing came for as long as a connection may be silent. The peer
         * is asked whether it is there; one that was asked already, and has
         * said nothing since, is gone.
         */
        private void silent() throws IOException {
            if (closed.get()) {
                throw new IOException("the peer did not answer the close");
            }
            if (pinged) {
                throw new IOException("the peer did not answer a ping in " + pingIntervalMillis + " ms");
            }
            pinged = true;
            control(PING, NOTHING);
        }

        // --- writing ---

        @Override
        public void write(List<MapValue> batch) throws IOException {
            if (batch.isEmpty()) {
                return;
            }
            if (closed.get()) {
                throw new IOException("the connection is closed");
            }
            // One frame for each message, and all of them in one write.
            final int opcode = codec.textFrames() ? TEXT : BINARY;
            final byte[][] payloads = new byte[batch.size()][];
            long total = 0;
            try {
                for (int i = 0; i < payloads.length; i++) {
                    payloads[i] = codec.encode(batch.get(i));
                    total += headLength(payloads[i].length) + payloads[i].length;
                }
            } catch (ValueException e) {
                throw new IOException(codec.name() + " encode, " + e.getMessage(), e);
            }
            if (total > Integer.MAX_VALUE - 8) {
                throw new IOException("a batch of " + total + " bytes is too long to write at once");
            }
            final byte[] frames = new byte[(int) total];
            int at = 0;
            for (byte[] payload : payloads) {
                at = putHead(frames, at, opcode, payload.length);
                System.arraycopy(payload, 0, frames, at, payload.length);
                at += payload.length;
            }
            writeLock.lock();
            try {
                if (closed.get()) {
                    throw new IOException("the connection is closed");
                }
                send(frames);
            } finally {
                writeLock.unlock();
            }
        }

        /** Sends a frame that is not a message: a ping, its answer, or the close. */
        private void control(int opcode, byte[] payload) throws IOException {
            writeLock.lock();
            try {
                send(frame(opcode, payload));
            } finally {
                writeLock.unlock();
            }
        }

        /** Writes, under the lock of the writers. */
        private void send(byte[] bytes) throws IOException {
            // A write to a socket has no timeout of its own: a peer that does
            // not read would hold the writer for good. The connection is
            // closed instead, which makes the write fail.
            final ScheduledFuture<?> guard = writeTimeoutMillis > 0
                    ? Threads.TIMER.schedule(this::closeSocket, writeTimeoutMillis, TimeUnit.MILLISECONDS)
                    : null;
            try {
                out.write(bytes);
                out.flush();
            } finally {
                if (guard != null) {
                    guard.cancel(false);
                }
            }
        }

        @Override
        public String remoteAddress() {
            return remoteAddress;
        }

        // --- the end ---

        /**
         * Closes the connection as WebSocket has it: the peer is sent a
         * close frame, and the socket is closed when the peer has answered
         * with its own, which the thread that reads sees, or has not for a
         * while. A browser then knows that the server closed, and not that
         * the connection broke.
         */
        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            // A writer that is held by a peer that does not read is not waited for.
            if (started.get() && writeLock.tryLock()) {
                boolean said = false;
                final ScheduledFuture<?> guard =
                        Threads.TIMER.schedule(this::closeSocket, CLOSE_WAIT_MILLIS, TimeUnit.MILLISECONDS);
                try {
                    out.write(frame(CLOSE, status(NORMAL)));
                    out.flush();
                    socket.shutdownOutput();
                    said = true;
                } catch (IOException e) {
                    // closed below
                } finally {
                    writeLock.unlock();
                }
                if (said) {
                    return;
                }
                guard.cancel(false);
            }
            closeSocket();
        }

        private void closeSocket() {
            if (socketClosed.compareAndSet(false, true)) {
                closeQuietly(socket);
            }
        }

        @Override
        public String toString() {
            return "WsConn[" + remoteAddress + "]";
        }
    }
}
