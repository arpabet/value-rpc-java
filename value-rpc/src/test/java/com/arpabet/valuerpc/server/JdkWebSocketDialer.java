/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.server;

import com.arpabet.value.MapValue;
import com.arpabet.valuerpc.FrameException;
import com.arpabet.valuerpc.WireCodec;
import com.arpabet.valuerpc.transport.Dialer;
import com.arpabet.valuerpc.transport.MsgConn;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A way for a client of the tests to reach a WebSocket listener: the
 * WebSocket client of the JDK, which is none of this library's making, so
 * that the listener is tested against an implementation of the protocol that
 * is not its own. The library has no WebSocket client yet; when it has, this
 * stays as the second opinion.
 *
 * <p>As the clients of the other implementations do, it offers the
 * subprotocol of its codec and requires the server to echo it.
 */
final class JdkWebSocketDialer implements Dialer {

    private final URI uri;
    private final WireCodec codec;
    /** The subprotocols to offer; null for the one of the codec. */
    private final String[] offer;
    private final String origin;

    JdkWebSocketDialer(String url, WireCodec codec) {
        this(url, codec, null, null);
    }

    JdkWebSocketDialer(String url, WireCodec codec, String[] offer, String origin) {
        this.uri = URI.create(url);
        this.codec = codec;
        this.offer = offer;
        this.origin = origin;
    }

    @Override
    public MsgConn dial(long timeoutMillis) throws IOException {
        final Conn conn = new Conn(codec, uri.toString());
        WebSocket.Builder b = HttpClient.newHttpClient().newWebSocketBuilder();
        final String[] protocols = offer != null ? offer : new String[] {codec.subprotocol()};
        if (protocols.length == 1) {
            b = b.subprotocols(protocols[0]);
        } else if (protocols.length > 1) {
            b = b.subprotocols(protocols[0], java.util.Arrays.copyOfRange(protocols, 1, protocols.length));
        }
        if (origin != null) {
            b = b.header("Origin", origin);
        }
        try {
            final WebSocket ws = b.buildAsync(uri, conn).get(timeoutMillis <= 0 ? 30_000 : timeoutMillis, TimeUnit.MILLISECONDS);
            if (offer == null && !codec.subprotocol().equals(ws.getSubprotocol())) {
                ws.abort();
                throw new IOException("the server did not negotiate " + codec.subprotocol() + " (got \"" + ws.getSubprotocol() + "\")");
            }
            conn.ws = ws;
            conn.subprotocol = ws.getSubprotocol();
            return conn;
        } catch (ExecutionException e) {
            throw new IOException("websocket dial " + uri + ": " + e.getCause(), e.getCause());
        } catch (TimeoutException e) {
            throw new IOException("websocket dial " + uri + " timed out");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while dialing", e);
        }
    }

    static final class Conn implements MsgConn, WebSocket.Listener {
        private final WireCodec codec;
        private final String address;
        volatile WebSocket ws;
        /** The subprotocol the server chose; empty when it chose none. */
        volatile String subprotocol;
        /** How the server closed: the number of its close frame, 0 while it has not. */
        volatile int closeCode;

        private final ByteArrayOutputStream partial = new ByteArrayOutputStream();
        /** What arrived before anyone was there to take it, and whether the end has. Under the monitor. */
        private final ArrayDeque<MapValue> early = new ArrayDeque<MapValue>();
        private Receiver receiver;
        private boolean ended;
        private Throwable endCause;
        private boolean told;

        Conn(WireCodec codec, String address) {
            this.codec = codec;
            this.address = address;
        }

        @Override
        public void start(Receiver r) {
            final boolean over;
            final Throwable cause;
            synchronized (this) {
                receiver = r;
                for (MapValue m : early) {
                    r.onMessage(m);
                }
                early.clear();
                over = ended && !told;
                cause = endCause;
                if (over) {
                    told = true;
                }
            }
            if (over) {
                r.onClosed(cause);
            }
        }

        private void message(byte[] bytes, boolean text, WebSocket socket) {
            if (text != codec.textFrames()) {
                end(new FrameException("a frame of the wrong kind for " + codec.name()));
                socket.abort();
                return;
            }
            final MapValue m;
            try {
                m = codec.decode(bytes);
            } catch (FrameException e) {
                end(e);
                socket.abort();
                return;
            }
            synchronized (this) {
                if (receiver == null) {
                    early.add(m);
                    return;
                }
                receiver.onMessage(m);
            }
        }

        private void end(Throwable cause) {
            final Receiver r;
            synchronized (this) {
                if (ended) {
                    return;
                }
                ended = true;
                endCause = cause;
                r = receiver;
                if (r != null) {
                    told = true;
                }
            }
            if (r != null) {
                r.onClosed(cause);
            }
        }

        @Override
        public CompletionStage<?> onBinary(WebSocket socket, ByteBuffer data, boolean last) {
            final byte[] chunk = new byte[data.remaining()];
            data.get(chunk);
            partial.write(chunk, 0, chunk.length);
            if (last) {
                final byte[] whole = partial.toByteArray();
                partial.reset();
                message(whole, false, socket);
            }
            socket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onText(WebSocket socket, CharSequence data, boolean last) {
            final byte[] chunk = data.toString().getBytes(StandardCharsets.UTF_8);
            partial.write(chunk, 0, chunk.length);
            if (last) {
                final byte[] whole = partial.toByteArray();
                partial.reset();
                message(whole, true, socket);
            }
            socket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket socket, int statusCode, String reason) {
            closeCode = statusCode;
            end(null);
            return null;
        }

        @Override
        public void onError(WebSocket socket, Throwable error) {
            end(error);
        }

        @Override
        public void write(List<MapValue> batch) throws IOException {
            final WebSocket socket = ws;
            try {
                // One send at a time is all the client of the JDK takes.
                synchronized (partial) {
                    for (MapValue m : batch) {
                        final byte[] bytes = codec.encode(m);
                        if (codec.textFrames()) {
                            socket.sendText(new String(bytes, StandardCharsets.UTF_8), true).get(30, TimeUnit.SECONDS);
                        } else {
                            socket.sendBinary(ByteBuffer.wrap(bytes), true).get(30, TimeUnit.SECONDS);
                        }
                    }
                }
            } catch (ExecutionException e) {
                throw new IOException(String.valueOf(e.getCause()), e.getCause());
            } catch (TimeoutException e) {
                throw new IOException("the write took too long");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while writing", e);
            } catch (RuntimeException e) {
                throw new IOException(String.valueOf(e), e);
            }
        }

        @Override
        public String remoteAddress() {
            return address;
        }

        @Override
        public void close() {
            final WebSocket socket = ws;
            if (socket != null) {
                // Says goodbye as a browser does, and does not wait for the answer.
                socket.sendClose(WebSocket.NORMAL_CLOSURE, "").whenComplete((w, e) -> socket.abort());
            }
            end(null);
        }
    }
}
