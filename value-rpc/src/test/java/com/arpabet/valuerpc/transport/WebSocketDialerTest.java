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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.value.MapValue;
import com.arpabet.valuerpc.WireCodec;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The WebSocket client, against a server of the test's own on a socket: what
 * it asks to be upgraded with, what it takes for an answer, and the frames it
 * sends and reads. What it does against the servers of this library and of
 * the Go one is in the tests of those.
 */
@Timeout(30)
class WebSocketDialerTest {

    private static final int CONTINUATION = 0;
    private static final int TEXT = 1;
    private static final int BINARY = 2;
    private static final int CLOSE = 8;
    private static final int PING = 9;
    private static final int PONG = 10;

    private static final MapValue HELLO = MapValue.builder().put("t", 3).put("rid", 1).put("res", "hello").build();

    /** A server that takes one connection, and is told by the test what to answer and what to send. */
    private static final class Server implements AutoCloseable {
        private final ServerSocket listening = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        private Socket socket;
        private InputStream in;
        private OutputStream out;
        String requestLine;
        final Map<String, String> headers = new HashMap<String, String>();

        Server() throws IOException {
        }

        String address() {
            return "127.0.0.1:" + listening.getLocalPort();
        }

        /** Takes the connection and reads what it asks. */
        Server accept() throws IOException {
            socket = listening.accept();
            socket.setSoTimeout(10_000);
            in = new BufferedInputStream(socket.getInputStream());
            out = socket.getOutputStream();
            final ByteArrayOutputStream head = new ByteArrayOutputStream();
            while (!head.toString("ISO-8859-1").endsWith("\r\n\r\n")) {
                final int b = in.read();
                if (b < 0) {
                    throw new EOFException("the client closed inside its request");
                }
                head.write(b);
            }
            final String[] lines = head.toString("ISO-8859-1").split("\r\n");
            requestLine = lines[0];
            for (int i = 1; i < lines.length; i++) {
                final int colon = lines[i].indexOf(':');
                headers.put(lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT), lines[i].substring(colon + 1).trim());
            }
            return this;
        }

        void answer(String text) throws IOException {
            out.write(text.getBytes(StandardCharsets.ISO_8859_1));
            out.flush();
        }

        /** Upgrades the connection as a server does, taking the subprotocol the client offers. */
        Server upgrade() throws Exception {
            return upgrade(headers.get("sec-websocket-protocol"));
        }

        Server upgrade(String subprotocol) throws Exception {
            final String accept = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-1").digest(
                    (headers.get("sec-websocket-key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").getBytes(StandardCharsets.ISO_8859_1)));
            answer("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
                    + accept + "\r\n" + (subprotocol == null ? "" : "Sec-WebSocket-Protocol: " + subprotocol + "\r\n") + "\r\n");
            return this;
        }

        void send(boolean fin, int opcode, byte[] payload, boolean masked) throws IOException {
            final ByteArrayOutputStream frame = new ByteArrayOutputStream();
            frame.write((fin ? 0x80 : 0) | opcode);
            final int maskBit = masked ? 0x80 : 0;
            if (payload.length < 126) {
                frame.write(maskBit | payload.length);
            } else {
                frame.write(maskBit | 126);
                frame.write(payload.length >>> 8);
                frame.write(payload.length);
            }
            if (masked) {
                frame.write(new byte[4]);
            }
            frame.write(payload);
            out.write(frame.toByteArray());
            out.flush();
        }

        void send(int opcode, byte[] payload) throws IOException {
            send(true, opcode, payload, false);
        }

        /** The next frame of the client: its kind, whether it was masked, and its payload with the mask taken off. */
        Object[] frame() throws IOException {
            final int b0 = in.read();
            if (b0 < 0) {
                return null;
            }
            final int b1 = in.read();
            int length = b1 & 0x7f;
            if (length == 126) {
                length = in.read() << 8 | in.read();
            }
            final boolean masked = (b1 & 0x80) != 0;
            final byte[] mask = new byte[4];
            if (masked) {
                assertEquals(4, in.readNBytes(mask, 0, 4));
            }
            final byte[] payload = in.readNBytes(length);
            for (int i = 0; i < payload.length; i++) {
                payload[i] ^= mask[i & 3];
            }
            return new Object[] {b0 & 0x0f, masked, payload, mask};
        }

        @Override
        public void close() throws IOException {
            if (socket != null) {
                socket.close();
            }
            listening.close();
        }
    }

    /** What a connection hands over. */
    private static final class Got implements MsgConn.Receiver {
        final LinkedBlockingQueue<MapValue> messages = new LinkedBlockingQueue<MapValue>();
        final CompletableFuture<String> closed = new CompletableFuture<String>();

        @Override
        public void onMessage(MapValue message) {
            messages.add(message);
        }

        @Override
        public void onClosed(Throwable cause) {
            closed.complete(cause == null ? "closed" : cause.getMessage());
        }
    }

    private static Dialer dialer(Server server, String path, WireCodec codec) {
        return Transports.webSocketDialer("ws://" + server.address() + path, TransportOptions.DEFAULT,
                codec == null ? WebSocketOptions.DEFAULT : WebSocketOptions.DEFAULT.withCodecs(codec));
    }

    /** Dials on a thread of its own, while the test plays the server. */
    private static CompletableFuture<MsgConn> dial(Dialer dialer, long timeoutMillis) {
        final CompletableFuture<MsgConn> conn = new CompletableFuture<MsgConn>();
        final Thread t = new Thread(() -> {
            try {
                conn.complete(dialer.dial(timeoutMillis));
            } catch (IOException | RuntimeException e) {
                conn.completeExceptionally(e);
            }
        }, "test-dial");
        t.setDaemon(true);
        t.start();
        return conn;
    }

    private static String failure(CompletableFuture<MsgConn> dialing) throws Exception {
        try {
            dialing.get(10, TimeUnit.SECONDS).close();
            throw new AssertionError("the dial was to fail");
        } catch (java.util.concurrent.ExecutionException e) {
            assertTrue(e.getCause() instanceof IOException, String.valueOf(e.getCause()));
            return e.getCause().getMessage();
        }
    }

    @Test
    void asksToBeUpgradedAsRfc6455HasIt() throws Exception {
        try (Server server = new Server()) {
            final CompletableFuture<MsgConn> dialing = dial(dialer(server, "/rpc?room=1", null), 5000);
            server.accept();
            assertEquals("GET /rpc?room=1 HTTP/1.1", server.requestLine);
            assertEquals(server.address(), server.headers.get("host"));
            assertEquals("websocket", server.headers.get("upgrade"));
            assertEquals("Upgrade", server.headers.get("connection"));
            assertEquals("13", server.headers.get("sec-websocket-version"));
            assertEquals(16, Base64.getDecoder().decode(server.headers.get("sec-websocket-key")).length);
            // The codec is offered by its name, MessagePack unless another was asked for; no extension is.
            assertEquals("vrpc.msgpack", server.headers.get("sec-websocket-protocol"));
            assertNull(server.headers.get("sec-websocket-extensions"));
            server.upgrade();
            try (MsgConn conn = dialing.get(10, TimeUnit.SECONDS)) {
                assertNull(conn.tlsSession());
                assertTrue(conn.remoteAddress().contains("127.0.0.1"), conn.remoteAddress());
            }
        }
        // Without a path it asks for the root; JSON is offered by its own name.
        try (Server server = new Server()) {
            final CompletableFuture<MsgConn> dialing = dial(dialer(server, "", WireCodec.JSON), 5000);
            server.accept();
            assertEquals("GET / HTTP/1.1", server.requestLine);
            assertEquals("vrpc.json", server.headers.get("sec-websocket-protocol"));
            server.upgrade();
            dialing.get(10, TimeUnit.SECONDS).close();
        }
    }

    @Test
    void connectsOnlyToWhatAnswersAsAWebSocketServerOfItsCodec() throws Exception {
        try (Server server = new Server()) {
            final CompletableFuture<MsgConn> dialing = dial(dialer(server, "/rpc", null), 5000);
            server.accept().answer("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
            assertEquals("websocket dial: the server did not upgrade the connection: 404 Not Found", failure(dialing));
        }
        try (Server server = new Server()) {
            final CompletableFuture<MsgConn> dialing = dial(dialer(server, "/rpc", null), 5000);
            server.accept().answer("SSH-2.0-OpenSSH\r\n\r\n");
            assertEquals("websocket dial: what answered is not an HTTP server", failure(dialing));
        }
        try (Server server = new Server()) {
            final CompletableFuture<MsgConn> dialing = dial(dialer(server, "/rpc", null), 5000);
            // An answer that was not made from the key of this request: a cache, or something that is no WebSocket server.
            server.accept().answer("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                    + "Sec-WebSocket-Accept: bm90IHRoZSBhbnN3ZXI=\r\nSec-WebSocket-Protocol: vrpc.msgpack\r\n\r\n");
            assertEquals("websocket dial: what answered is not a WebSocket server", failure(dialing));
        }
        // A server that names no codec predates the negotiation, and one that names another speaks another:
        // said in the words of the Go client.
        try (Server server = new Server()) {
            final CompletableFuture<MsgConn> dialing = dial(dialer(server, "/rpc", null), 5000);
            server.accept().upgrade(null);
            assertEquals("server did not negotiate wire codec \"vrpc.msgpack\" (got subprotocol \"\"); "
                    + "it predates the codec negotiation or does not enable this codec", failure(dialing));
        }
        try (Server server = new Server()) {
            final CompletableFuture<MsgConn> dialing = dial(dialer(server, "/rpc", WireCodec.JSON), 5000);
            server.accept().upgrade("vrpc.msgpack");
            assertTrue(failure(dialing).startsWith("server did not negotiate wire codec \"vrpc.json\" (got subprotocol \"vrpc.msgpack\")"));
        }
        try (Server server = new Server()) {
            final CompletableFuture<MsgConn> dialing = dial(dialer(server, "/rpc", null), 5000);
            server.accept();
            final String accept = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-1").digest(
                    (server.headers.get("sec-websocket-key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").getBytes(StandardCharsets.ISO_8859_1)));
            server.answer("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
                    + accept + "\r\nSec-WebSocket-Protocol: vrpc.msgpack\r\nSec-WebSocket-Extensions: permessage-deflate\r\n\r\n");
            assertTrue(failure(dialing).contains("an extension that was not asked for"));
        }
    }

    @Test
    void aServerThatDoesNotAnswerIsNotWaitedForBeyondTheTimeOfTheDial() throws Exception {
        try (Server server = new Server()) {
            final long began = System.nanoTime();
            final CompletableFuture<MsgConn> dialing = dial(dialer(server, "/rpc", null), 300);
            server.accept();
            final String why = failure(dialing);
            final long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began);
            assertTrue(took >= 250 && took < 5000, "took " + took + " ms: " + why);
        }
        // Nothing listens there: the dial fails, and says so as the dial of a socket does.
        final int free;
        try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            free = s.getLocalPort();
        }
        assertThrows(IOException.class,
                () -> Transports.dialer("ws://127.0.0.1:" + free + "/rpc", TransportOptions.DEFAULT).dial(2000));
    }

    @Test
    void masksWhatItSendsAndTakesNothingThatIsMasked() throws Exception {
        try (Server server = new Server()) {
            final CompletableFuture<MsgConn> dialing = dial(dialer(server, "/rpc", null), 5000);
            server.accept().upgrade();
            final Got got = new Got();
            try (MsgConn conn = dialing.get(10, TimeUnit.SECONDS)) {
                conn.start(got);
                // Two messages in one write are two frames, each with a mask of its own.
                conn.write(Arrays.asList(HELLO, HELLO));
                final Object[] first = server.frame();
                final Object[] second = server.frame();
                assertEquals(BINARY, first[0]);
                assertEquals(true, first[1], "a client masks every frame");
                assertEquals(HELLO, WireCodec.MSGPACK.decode((byte[]) first[2]));
                assertEquals(HELLO, WireCodec.MSGPACK.decode((byte[]) second[2]));
                assertFalse(Arrays.equals((byte[]) first[3], (byte[]) second[3]), "the same mask twice");

                // What a server sends has no mask: in one frame, or in several, with a ping between two of them.
                final byte[] payload = WireCodec.MSGPACK.encode(HELLO);
                server.send(BINARY, payload);
                assertEquals(HELLO, got.messages.poll(10, TimeUnit.SECONDS));
                server.send(false, BINARY, Arrays.copyOfRange(payload, 0, 3), false);
                server.send(PING, new byte[] {7, 8});
                server.send(true, CONTINUATION, Arrays.copyOfRange(payload, 3, payload.length), false);
                final Object[] pong = server.frame();
                assertEquals(PONG, pong[0]);
                assertEquals(true, pong[1]);
                assertArrayEquals(new byte[] {7, 8}, (byte[]) pong[2]);
                assertEquals(HELLO, got.messages.poll(10, TimeUnit.SECONDS));

                // A frame with a mask is not one of a server: the connection is closed, and the peer told why.
                server.send(true, BINARY, payload, true);
                final Object[] close = server.frame();
                assertEquals(CLOSE, close[0]);
                final byte[] code = (byte[]) close[2];
                assertEquals(1002, (code[0] & 0xff) << 8 | (code[1] & 0xff));
                assertEquals("a frame of a server with a mask", got.closed.get(10, TimeUnit.SECONDS));
                assertNull(got.messages.poll());
            }
        }
    }

    @Test
    void speaksJsonInTextFramesWhenThatWasAgreedOn() throws Exception {
        try (Server server = new Server()) {
            final CompletableFuture<MsgConn> dialing = dial(dialer(server, "/rpc", WireCodec.JSON), 5000);
            server.accept().upgrade();
            final Got got = new Got();
            try (MsgConn conn = dialing.get(10, TimeUnit.SECONDS)) {
                conn.start(got);
                conn.write(Collections.singletonList(HELLO));
                final Object[] frame = server.frame();
                assertEquals(TEXT, frame[0]);
                // As the Go implementation writes JSON: the keys in order, a space behind each colon.
                assertEquals("{\"res\": \"hello\",\"rid\": 1,\"t\": 3}", new String((byte[]) frame[2], StandardCharsets.UTF_8));
                server.send(TEXT, "{\"t\":3,\"rid\":1,\"res\":\"hello\"}".getBytes(StandardCharsets.UTF_8));
                assertEquals(HELLO, got.messages.poll(10, TimeUnit.SECONDS));
                // A frame of the other kind is not of the codec the two agreed on.
                server.send(BINARY, WireCodec.MSGPACK.encode(HELLO));
                assertEquals(CLOSE, server.frame()[0]);
                assertNotNull(got.closed.get(10, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void closesAsWebSocketHasItFromEitherSide() throws Exception {
        // The server closes: the client answers with its own close, and the connection ends without a cause.
        try (Server server = new Server()) {
            final CompletableFuture<MsgConn> dialing = dial(dialer(server, "/rpc", null), 5000);
            server.accept().upgrade();
            final Got got = new Got();
            try (MsgConn conn = dialing.get(10, TimeUnit.SECONDS)) {
                conn.start(got);
                server.send(CLOSE, new byte[] {0x03, (byte) 0xe8});
                final Object[] close = server.frame();
                assertEquals(CLOSE, close[0]);
                assertArrayEquals(new byte[] {0x03, (byte) 0xe8}, (byte[]) close[2]);
                assertEquals("closed", got.closed.get(10, TimeUnit.SECONDS));
                assertThrows(IOException.class, () -> conn.write(Collections.singletonList(HELLO)));
            }
        }
        // The client closes: it says so in a frame, and the socket is closed once the server has answered.
        try (Server server = new Server()) {
            final CompletableFuture<MsgConn> dialing = dial(dialer(server, "/rpc", null), 5000);
            server.accept().upgrade();
            final Got got = new Got();
            final MsgConn conn = dialing.get(10, TimeUnit.SECONDS);
            conn.start(got);
            conn.close();
            final Object[] close = server.frame();
            assertEquals(CLOSE, close[0]);
            assertEquals(true, close[1]);
            server.send(CLOSE, new byte[] {0x03, (byte) 0xe8});
            assertEquals("closed", got.closed.get(10, TimeUnit.SECONDS));
            assertNull(server.frame(), "the client was to close the socket");
        }
    }

    @Test
    void anAddressNamesTheTransportAndItsPort() {
        assertEquals("ws://example.com/rpc", Transports.dialer("ws://example.com/rpc", TransportOptions.DEFAULT).toString());
        assertEquals("wss://example.com:8443",
                Transports.webSocketDialer("wss://example.com:8443", TransportOptions.DEFAULT, WebSocketOptions.DEFAULT).toString());
        assertEquals("tls://example.com:9443", Transports.dialer("tls://example.com:9443", TransportOptions.DEFAULT).toString());
    }
}
