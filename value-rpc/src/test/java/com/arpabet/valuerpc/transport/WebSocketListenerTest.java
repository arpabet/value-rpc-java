/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.transport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.value.MapValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.FrameException;
import com.arpabet.valuerpc.WireCodec;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The WebSocket listener against a socket of the test's own, which says what
 * a client says byte for byte: the request to be upgraded and what answers
 * it, the frames, and how a connection is kept and ended. What the listener
 * carries, the protocol, is tested with a client that is none of this
 * library's ({@code ServerTest}, over the WebSocket client of the JDK).
 */
@Timeout(60)
class WebSocketListenerTest {

    private static final int TEXT = 1;
    private static final int BINARY = 2;
    private static final int CLOSE = 8;
    private static final int PING = 9;
    private static final int PONG = 10;

    /** What a listener hands over: its connections, and for each the messages and the end. */
    private static final class Served implements AutoCloseable, Listener.Acceptor {
        final Listener listener;
        final LinkedBlockingQueue<MsgConn> conns = new LinkedBlockingQueue<MsgConn>();
        final LinkedBlockingQueue<MapValue> messages = new LinkedBlockingQueue<MapValue>();
        /** How each connection ended: the cause, or "closed" for none. */
        final LinkedBlockingQueue<Object> ends = new LinkedBlockingQueue<Object>();

        Served(WebSocketOptions ws, TransportOptions options) throws IOException {
            listener = Transports.webSocketListener("127.0.0.1:0/rpc", options, ws);
            listener.start(this);
        }

        Served(WebSocketOptions ws) throws IOException {
            this(ws, TransportOptions.DEFAULT);
        }

        @Override
        public void onConnection(MsgConn conn) {
            conn.start(new MsgConn.Receiver() {
                @Override
                public void onMessage(MapValue message) {
                    messages.add(message);
                }

                @Override
                public void onClosed(Throwable cause) {
                    ends.add(cause == null ? "closed" : cause);
                }
            });
            conns.add(conn);
        }

        /** {@code host:port}, without the path. */
        String hostPort() {
            final String a = listener.address();
            return a.substring(0, a.indexOf('/'));
        }

        MsgConn conn() throws InterruptedException {
            final MsgConn c = conns.poll(10, TimeUnit.SECONDS);
            assertNotNull(c, "no connection was handed over");
            return c;
        }

        MapValue message() throws InterruptedException {
            final MapValue m = messages.poll(10, TimeUnit.SECONDS);
            assertNotNull(m, "no message was handed over");
            return m;
        }

        Object end() throws InterruptedException {
            final Object e = ends.poll(10, TimeUnit.SECONDS);
            assertNotNull(e, "the end of the connection was not told");
            return e;
        }

        @Override
        public void close() {
            listener.close();
            for (MsgConn c : conns) {
                c.close();
            }
        }
    }

    /** One frame as it was read. */
    private static final class Frame {
        final boolean fin;
        final int opcode;
        final byte[] payload;
        final boolean masked;

        Frame(boolean fin, int opcode, byte[] payload, boolean masked) {
            this.fin = fin;
            this.opcode = opcode;
            this.payload = payload;
            this.masked = masked;
        }

        /** The number of a close frame. */
        int code() {
            return (payload[0] & 0xff) << 8 | (payload[1] & 0xff);
        }
    }

    /** A client that writes what it is told to, byte for byte. */
    private static final class Client implements AutoCloseable {
        final Socket socket = new Socket();
        final InputStream in;
        final OutputStream out;
        String status;
        final Map<String, String> headers = new HashMap<String, String>();

        Client(String hostPort) throws IOException {
            final int colon = hostPort.lastIndexOf(':');
            socket.connect(new InetSocketAddress(hostPort.substring(0, colon), Integer.parseInt(hostPort.substring(colon + 1))), 5000);
            socket.setSoTimeout(10_000);
            in = socket.getInputStream();
            out = socket.getOutputStream();
        }

        /** Asks to be upgraded, as a browser does, with more headers or others in their place. */
        Client upgrade(String hostPort, String... more) throws IOException {
            final Map<String, String> h = new java.util.LinkedHashMap<String, String>();
            h.put("Host", hostPort);
            h.put("Upgrade", "websocket");
            h.put("Connection", "keep-alive, Upgrade");
            h.put("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
            h.put("Sec-WebSocket-Version", "13");
            for (int i = 0; i < more.length; i += 2) {
                if (more[i + 1] == null) {
                    h.remove(more[i]);
                } else {
                    h.put(more[i], more[i + 1]);
                }
            }
            final StringBuilder request = new StringBuilder("GET /rpc HTTP/1.1\r\n");
            for (Map.Entry<String, String> e : h.entrySet()) {
                request.append(e.getKey()).append(": ").append(e.getValue()).append("\r\n");
            }
            return request(request.append("\r\n").toString());
        }

        /** Sends a request as it is, and reads the head of the answer. */
        Client request(String text) throws IOException {
            out.write(text.getBytes(StandardCharsets.ISO_8859_1));
            out.flush();
            final ByteArrayOutputStream head = new ByteArrayOutputStream();
            while (!head.toString("ISO-8859-1").endsWith("\r\n\r\n")) {
                final int b = in.read();
                if (b < 0) {
                    break;
                }
                head.write(b);
            }
            final String[] lines = head.toString("ISO-8859-1").split("\r\n");
            status = lines[0];
            for (int i = 1; i < lines.length; i++) {
                final int colon = lines[i].indexOf(':');
                headers.put(lines[i].substring(0, colon).toLowerCase(Locale.ROOT), lines[i].substring(colon + 1).trim());
            }
            return this;
        }

        void send(boolean fin, int opcode, byte[] payload) throws IOException {
            send(fin, opcode, payload, true);
        }

        void send(boolean fin, int opcode, byte[] payload, boolean masked) throws IOException {
            final ByteArrayOutputStream frame = new ByteArrayOutputStream();
            frame.write((fin ? 0x80 : 0) | opcode);
            final int maskBit = masked ? 0x80 : 0;
            if (payload.length < 126) {
                frame.write(maskBit | payload.length);
            } else if (payload.length <= 0xffff) {
                frame.write(maskBit | 126);
                frame.write(payload.length >>> 8);
                frame.write(payload.length);
            } else {
                frame.write(maskBit | 127);
                for (int shift = 56; shift >= 0; shift -= 8) {
                    frame.write((int) ((long) payload.length >>> shift));
                }
            }
            if (masked) {
                final byte[] mask = {0x11, 0x22, 0x33, 0x44};
                frame.write(mask, 0, 4);
                for (int i = 0; i < payload.length; i++) {
                    frame.write(payload[i] ^ mask[i & 3]);
                }
            } else {
                frame.write(payload, 0, payload.length);
            }
            out.write(frame.toByteArray());
            out.flush();
        }

        /** The next frame of the server; null when it has closed the socket. */
        Frame read() throws IOException {
            final int b0 = in.read();
            if (b0 < 0) {
                return null;
            }
            final int b1 = need();
            long length = b1 & 0x7f;
            if (length == 126) {
                length = need() << 8 | need();
            } else if (length == 127) {
                length = 0;
                for (int i = 0; i < 8; i++) {
                    length = length << 8 | need();
                }
            }
            final boolean masked = (b1 & 0x80) != 0;
            final byte[] payload = new byte[(int) length];
            int have = 0;
            while (have < payload.length) {
                final int got = in.read(payload, have, payload.length - have);
                if (got < 0) {
                    throw new EOFException();
                }
                have += got;
            }
            return new Frame((b0 & 0x80) != 0, b0 & 0x0f, payload, masked);
        }

        private int need() throws IOException {
            final int b = in.read();
            if (b < 0) {
                throw new EOFException();
            }
            return b;
        }

        /** Asserts that the server closes the socket, having sent nothing more. */
        void closedByServer() throws IOException {
            assertEquals(-1, in.read(), "the server was to close the socket");
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private static MapValue hello(String what) {
        return MapValue.builder().put("t", 2).put("rid", 1).put("fn", what).build();
    }

    // --- the request to be upgraded ----------------------------------------------------

    @Test
    void answersTheKeyOfAClientAndTheSubprotocolItIsServed() throws Exception {
        try (Served served = new Served(WebSocketOptions.DEFAULT)) {
            final String host = served.hostPort();
            // The example of RFC 6455.
            try (Client c = new Client(host).upgrade(host)) {
                assertEquals("HTTP/1.1 101 Switching Protocols", c.status);
                assertEquals("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", c.headers.get("sec-websocket-accept"));
                assertEquals("websocket", c.headers.get("upgrade"));
                assertEquals("Upgrade", c.headers.get("connection"));
                assertNull(c.headers.get("sec-websocket-protocol"), "a client that offers none is told none, and is served MessagePack");
                c.send(true, BINARY, hello("binary").pack());
                assertEquals(hello("binary"), served.message());
            }
            // A client is served the first of the codecs of the server that it offers, whatever its own order.
            try (Client c = new Client(host).upgrade(host, "Sec-WebSocket-Protocol", "vrpc.json, vrpc.msgpack")) {
                assertEquals("vrpc.msgpack", c.headers.get("sec-websocket-protocol"));
            }
            try (Client c = new Client(host).upgrade(host, "Sec-WebSocket-Protocol", "other, vrpc.json")) {
                assertEquals("vrpc.json", c.headers.get("sec-websocket-protocol"));
                c.send(true, TEXT, hello("text").jsonify().getBytes(StandardCharsets.UTF_8));
                assertEquals(hello("text"), served.message());
                final MsgConn conn = lastOf(served);
                conn.write(Collections.singletonList(hello("answer")));
                final Frame f = c.read();
                assertEquals(TEXT, f.opcode);
                assertEquals(hello("answer").jsonify(), new String(f.payload, StandardCharsets.UTF_8));
            }
            try (Client c = new Client(host).upgrade(host, "Sec-WebSocket-Protocol", "something.else")) {
                assertEquals("HTTP/1.1 101 Switching Protocols", c.status);
                assertNull(c.headers.get("sec-websocket-protocol"), "what the server does not serve it does not echo");
            }
        }
        // A server of one codec serves it to whoever comes, and echoes it only to who asked for it.
        try (Served served = new Served(WebSocketOptions.DEFAULT.withCodecs(WireCodec.JSON))) {
            final String host = served.hostPort();
            try (Client c = new Client(host).upgrade(host, "Sec-WebSocket-Protocol", "vrpc.msgpack")) {
                assertNull(c.headers.get("sec-websocket-protocol"));
                c.send(true, TEXT, hello("json only").jsonify().getBytes(StandardCharsets.UTF_8));
                assertEquals(hello("json only"), served.message());
            }
        }
    }

    private static MsgConn lastOf(Served served) throws InterruptedException {
        MsgConn last = served.conn();
        for (MsgConn next = served.conns.poll(); next != null; next = served.conns.poll()) {
            last = next;
        }
        return last;
    }

    @Test
    void refusesWhatIsNotARequestToBeUpgraded() throws Exception {
        try (Served served = new Served(WebSocketOptions.DEFAULT)) {
            final String host = served.hostPort();
            try (Client c = new Client(host).request("GET /elsewhere HTTP/1.1\r\nHost: " + host
                    + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: a2V5\r\nSec-WebSocket-Version: 13\r\n\r\n")) {
                assertEquals("HTTP/1.1 404 Not Found", c.status);
                c.closedByServer();
            }
            try (Client c = new Client(host).request("POST /rpc HTTP/1.1\r\nHost: " + host + "\r\n\r\n")) {
                assertEquals("HTTP/1.1 405 Method Not Allowed", c.status);
            }
            // A page that is fetched, and not a socket that is opened.
            try (Client c = new Client(host).request("GET /rpc?x=1 HTTP/1.1\r\nHost: " + host + "\r\n\r\n")) {
                assertEquals("HTTP/1.1 426 Upgrade Required", c.status);
                assertEquals("websocket", c.headers.get("upgrade"));
            }
            try (Client c = new Client(host).upgrade(host, "Sec-WebSocket-Version", "8")) {
                assertEquals("HTTP/1.1 400 Bad Request", c.status);
                assertEquals("13", c.headers.get("sec-websocket-version"));
            }
            try (Client c = new Client(host).upgrade(host, "Sec-WebSocket-Key", null)) {
                assertEquals("HTTP/1.1 400 Bad Request", c.status);
            }
            try (Client c = new Client(host).request("\u0016\u0003\u0001 not http at all\r\n\r\n")) {
                assertEquals("HTTP/1.1 400 Bad Request", c.status);
                c.closedByServer();
            }
            assertTrue(served.conns.isEmpty(), "none of them was handed over");
            // The query of a request is not its path.
            try (Client c = new Client(host).request("GET /rpc?token=1 HTTP/1.1\r\nHost: " + host
                    + "\r\nUpgrade: WebSocket\r\nConnection: upgrade\r\nSec-WebSocket-Key: a2V5\r\nSec-WebSocket-Version: 13\r\n\r\n")) {
                assertEquals("HTTP/1.1 101 Switching Protocols", c.status);
            }
        }
    }

    @Test
    void aConnectionThatDoesNotAskInTimeIsClosed() throws Exception {
        try (Served served = new Served(WebSocketOptions.DEFAULT.withHandshakeTimeoutMillis(150))) {
            try (Client c = new Client(served.hostPort())) {
                final long started = System.nanoTime();
                c.closedByServer();
                final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
                assertTrue(millis >= 100 && millis < 5000, "after the time it was given: " + millis + " ms");
            }
        }
    }

    @Test
    void letsInThePagesOfItsOwnOriginAndOfThePatternsItWasGiven() throws Exception {
        final List<String> none = Collections.<String>emptyList();
        // No origin: not a browser.
        assertTrue(Ws.originAllowed(null, "api.example.com", none));
        assertTrue(Ws.originAllowed("", "api.example.com", none));
        // The page of the server itself, whatever the case and the scheme.
        assertTrue(Ws.originAllowed("https://API.example.com", "api.example.com", none));
        assertTrue(Ws.originAllowed("http://localhost:8790", "localhost:8790", none));
        // Another site, another port, a page with no origin of its own.
        assertFalse(Ws.originAllowed("https://evil.example", "api.example.com", none));
        assertFalse(Ws.originAllowed("http://localhost:5173", "localhost:8790", none));
        assertFalse(Ws.originAllowed("null", "api.example.com", none));
        // Patterns.
        assertTrue(Ws.originAllowed("https://app.example.com", "api.example.com", Arrays.asList("app.example.com")));
        assertTrue(Ws.originAllowed("https://App.Example.com", "api.example.com", Arrays.asList("*.EXAMPLE.com")));
        assertFalse(Ws.originAllowed("https://example.com.evil.net", "api.example.com", Arrays.asList("*.example.com")));
        assertTrue(Ws.originAllowed("http://localhost:5173", "127.0.0.1:8790", Arrays.asList("localhost:*", "127.0.0.1:*")));
        assertFalse(Ws.originAllowed("http://localhost.evil.net:5173", "127.0.0.1:8790", Arrays.asList("localhost:*")));
        assertTrue(Ws.originAllowed("http://a.b", "c.d", Arrays.asList("?.b")));
        assertTrue(Ws.originAllowed("https://anything.at.all", "api.example.com", Arrays.asList("*")));
        assertTrue(Ws.originAllowed("null", "api.example.com", Arrays.asList("*")));
        // With a scheme, a pattern is for the whole origin.
        assertTrue(Ws.originAllowed("https://app.example.com", "api.example.com", Arrays.asList("https://*.example.com")));
        assertFalse(Ws.originAllowed("http://app.example.com", "api.example.com", Arrays.asList("https://*.example.com")));

        try (Served served = new Served(WebSocketOptions.DEFAULT.withOriginPatterns("localhost:*"))) {
            final String host = served.hostPort();
            try (Client c = new Client(host).upgrade(host, "Origin", "http://" + host)) {
                assertEquals("HTTP/1.1 101 Switching Protocols", c.status);
            }
            try (Client c = new Client(host).upgrade(host, "Origin", "http://localhost:5173")) {
                assertEquals("HTTP/1.1 101 Switching Protocols", c.status);
            }
            try (Client c = new Client(host).upgrade(host, "Origin", "https://evil.example")) {
                assertEquals("HTTP/1.1 403 Forbidden", c.status);
                c.closedByServer();
            }
        }
    }

    // --- frames ------------------------------------------------------------------------

    @Test
    void carriesMessagesOfEveryLengthAndInSeveralFrames() throws Exception {
        try (Served served = new Served(WebSocketOptions.DEFAULT)) {
            final String host = served.hostPort();
            try (Client c = new Client(host).upgrade(host)) {
                final MsgConn conn = served.conn();
                // A length in seven bits, in two bytes and in eight.
                for (int size : new int[] {10, 300, 70_000, 1_100_000}) {
                    final char[] text = new char[size];
                    Arrays.fill(text, 'x');
                    final MapValue m = hello(new String(text));
                    c.send(true, BINARY, m.pack());
                    assertEquals(m, served.message());
                    conn.write(Collections.singletonList(m));
                    final Frame f = c.read();
                    assertTrue(f.fin);
                    assertEquals(BINARY, f.opcode);
                    assertFalse(f.masked, "a server does not mask");
                    assertArrayEquals(m.pack(), f.payload);
                }
                // One message in three frames, and a ping between them, which is answered at once.
                final byte[] packed = hello("in parts").pack();
                c.send(false, BINARY, Arrays.copyOfRange(packed, 0, 3));
                c.send(true, PING, new byte[] {1, 2, 3});
                c.send(false, 0, Arrays.copyOfRange(packed, 3, 5));
                c.send(true, 0, Arrays.copyOfRange(packed, 5, packed.length));
                final Frame pong = c.read();
                assertEquals(PONG, pong.opcode);
                assertArrayEquals(new byte[] {1, 2, 3}, pong.payload);
                assertEquals(hello("in parts"), served.message());
                // Several messages are written at once, each in a frame of its own.
                conn.write(Arrays.asList(hello("one"), hello("two"), hello("three")));
                assertArrayEquals(hello("one").pack(), c.read().payload);
                assertArrayEquals(hello("two").pack(), c.read().payload);
                assertArrayEquals(hello("three").pack(), c.read().payload);
                // A pong nobody asked for is nothing.
                c.send(true, PONG, new byte[0]);
                c.send(true, BINARY, hello("still").pack());
                assertEquals(hello("still"), served.message());
            }
        }
    }

    /** What the server does with a frame it must not take: the number of its close, and what its receiver is told. */
    private static Object[] violation(WebSocketOptions ws, TransportOptions options, boolean fin, int opcode, byte[] payload,
            boolean masked) throws Exception {
        try (Served served = new Served(ws, options)) {
            final String host = served.hostPort();
            try (Client c = new Client(host).upgrade(host)) {
                c.send(fin, opcode, payload, masked);
                final Frame close = c.read();
                assertNotNull(close, "the client is told why before the socket closes");
                assertEquals(CLOSE, close.opcode);
                final Object end = served.end();
                c.closedByServer();
                return new Object[] {close.code(), end};
            }
        }
    }

    @Test
    void closesAConnectionThatBreaksTheProtocolAndSaysWhy() throws Exception {
        final WebSocketOptions ws = WebSocketOptions.DEFAULT;
        final TransportOptions options = TransportOptions.DEFAULT;
        // JSON where MessagePack was agreed on: the mistake of a client of one's own.
        Object[] got = violation(ws, options, true, TEXT, "{\"t\":2}".getBytes(StandardCharsets.UTF_8), true);
        assertEquals(1003, got[0]);
        assertEquals("expected a binary websocket frame, got text; JSON codec not negotiated (offer subprotocol \"vrpc.json\")",
                ((FrameException) got[1]).getMessage());
        got = violation(ws.withCodecs(WireCodec.JSON), options, true, BINARY, hello("x").pack(), true);
        assertEquals(1003, got[0]);
        assertEquals("expected a text websocket frame for the json codec, got binary", ((FrameException) got[1]).getMessage());
        // A client masks what it sends.
        got = violation(ws, options, true, BINARY, hello("x").pack(), false);
        assertEquals(1002, got[0]);
        // The next part of nothing; a kind of frame that is not one; a ping in parts.
        assertEquals(1002, violation(ws, options, true, 0, new byte[] {1}, true)[0]);
        assertEquals(1002, violation(ws, options, true, 3, new byte[] {1}, true)[0]);
        assertEquals(1002, violation(ws, options, false, PING, new byte[] {1}, true)[0]);
        assertEquals(1002, violation(ws, options, true, PING, new byte[126], true)[0]);
        // A message longer than the limit, which is read no further than its length.
        got = violation(ws, options.withMaxFrameSize(1024), true, BINARY, new byte[2000], true);
        assertEquals(1009, got[0]);
        assertEquals("frame too large: 2000 bytes (max 1024)", ((FrameException) got[1]).getMessage());
        // What is not a message in the codec ends the connection too.
        try (Served served = new Served(ws)) {
            final String host = served.hostPort();
            try (Client c = new Client(host).upgrade(host)) {
                c.send(true, BINARY, Value.utf8("not a map").pack());
                assertTrue(served.end() instanceof FrameException);
                c.closedByServer();
            }
        }
    }

    // --- keeping and ending a connection -----------------------------------------------

    @Test
    void asksASilentPeerWhetherItIsThereAndClosesOneThatIsNot() throws Exception {
        try (Served served = new Served(WebSocketOptions.DEFAULT.withPingIntervalMillis(500))) {
            final String host = served.hostPort();
            try (Client c = new Client(host).upgrade(host)) {
                // Silent: asked. It answers, and is asked again when it has been silent again.
                for (int i = 0; i < 3; i++) {
                    final Frame ping = c.read();
                    assertEquals(PING, ping.opcode);
                    c.send(true, PONG, ping.payload);
                }
                // Anything it says shows that it is there, a message as well as a pong.
                assertEquals(PING, c.read().opcode);
                c.send(true, BINARY, hello("here").pack());
                assertEquals(hello("here"), served.message());
                assertEquals(PING, c.read().opcode);
                assertTrue(served.ends.isEmpty());
                // It says nothing more: gone.
                final long started = System.nanoTime();
                c.closedByServer();
                assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 5000);
                final Object end = served.end();
                assertTrue(end instanceof IOException && String.valueOf(end).contains("did not answer a ping"), String.valueOf(end));
            }
        }
        // Without pings a silent connection is left alone.
        try (Served served = new Served(WebSocketOptions.DEFAULT.withPingIntervalMillis(0))) {
            final String host = served.hostPort();
            try (Client c = new Client(host).upgrade(host)) {
                c.socket.setSoTimeout(400);
                try {
                    c.read();
                    throw new AssertionError("the server was to say nothing");
                } catch (java.net.SocketTimeoutException expected) {
                    // silence
                }
                assertTrue(served.ends.isEmpty());
            }
        }
    }

    @Test
    void endsAConnectionAsWebSocketHasIt() throws Exception {
        try (Served served = new Served(WebSocketOptions.DEFAULT)) {
            final String host = served.hostPort();
            // The client closes: it is answered with the same number, and the socket is closed.
            try (Client c = new Client(host).upgrade(host)) {
                served.conn();
                c.send(true, CLOSE, new byte[] {0x03, (byte) 0xe9, 'b', 'y', 'e'}); // 1001, going away
                final Frame answer = c.read();
                assertEquals(CLOSE, answer.opcode);
                assertEquals(1001, answer.code());
                c.closedByServer();
                assertEquals("closed", served.end(), "a client that says goodbye is not an error");
            }
            // The server closes: the client is told so, with the number of a normal end.
            try (Client c = new Client(host).upgrade(host)) {
                final MsgConn conn = served.conn();
                conn.write(Collections.singletonList(hello("last words")));
                conn.close();
                conn.close(); // closing twice is closing once
                assertArrayEquals(hello("last words").pack(), c.read().payload);
                final Frame close = c.read();
                assertEquals(CLOSE, close.opcode);
                assertEquals(1000, close.code());
                // What the client still says is not handed over, and its goodbye ends the connection.
                c.send(true, BINARY, hello("too late").pack());
                c.send(true, CLOSE, new byte[] {0x03, (byte) 0xe8});
                c.closedByServer();
                assertEquals("closed", served.end());
                assertTrue(served.messages.isEmpty());
                try {
                    conn.write(Collections.singletonList(hello("after")));
                    throw new AssertionError("a closed connection writes nothing");
                } catch (IOException expected) {
                    // closed
                }
            }
            // A client that does not answer the close is not waited for long.
            try (Client c = new Client(host).upgrade(host)) {
                final MsgConn conn = served.conn();
                conn.close();
                assertEquals(CLOSE, c.read().opcode);
                final long started = System.nanoTime();
                c.closedByServer();
                assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 5000);
                assertEquals("closed", served.end());
            }
            // A client that just goes away, between two frames.
            try (Client c = new Client(host).upgrade(host)) {
                served.conn();
            }
            assertEquals("closed", served.end());
        }
    }

    @Test
    void anAddressNamesTheTransport() throws Exception {
        final Listener byScheme = Transports.listener("ws://127.0.0.1:0/rpc", TransportOptions.DEFAULT);
        try {
            assertTrue(byScheme.address().matches("127\\.0\\.0\\.1:\\d+/rpc"), byScheme.address());
            assertEquals("ws://" + byScheme.address(), byScheme.toString());
        } finally {
            byScheme.close();
        }
        // Without a path it is the root, which an address leaves out.
        final Listener root = Transports.webSocketListener("ws://127.0.0.1:0", TransportOptions.DEFAULT, WebSocketOptions.DEFAULT);
        try {
            assertTrue(root.address().matches("127\\.0\\.0\\.1:\\d+"), root.address());
            root.start(conn -> { });
            try (Client c = new Client(root.address()).request("GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n"
                    + "Connection: Upgrade\r\nSec-WebSocket-Key: a2V5\r\nSec-WebSocket-Version: 13\r\n\r\n")) {
                assertEquals("HTTP/1.1 101 Switching Protocols", c.status);
            }
        } finally {
            root.close();
        }
        // A client of this library does not dial WebSocket yet, and says so when it is asked to.
        try {
            Transports.dialer("ws://127.0.0.1:1/rpc", TransportOptions.DEFAULT).dial(1000);
            throw new AssertionError("there is no WebSocket dialer yet");
        } catch (IOException expected) {
            assertTrue(expected.getMessage().contains("not dialed yet"), expected.getMessage());
        }
    }
}
