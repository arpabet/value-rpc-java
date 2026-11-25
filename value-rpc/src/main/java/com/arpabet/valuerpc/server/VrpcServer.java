/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.server;

import com.arpabet.valuerpc.Dialect;
import com.arpabet.valuerpc.Metrics;
import com.arpabet.valuerpc.Registrar;
import com.arpabet.valuerpc.transport.Listener;
import com.arpabet.valuerpc.transport.TransportOptions;
import com.arpabet.valuerpc.transport.Transports;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * A vRPC server: the Go implementation's {@code valueserver.Server}.
 *
 * <pre>{@code
 * VrpcServer server = VrpcServer.builder().address("tcp://0.0.0.0:9000").build();
 * server.addFunction("greet", (context, args) -> Value.utf8("Hello, " + ((ListValue) args).get(0) + "!"));
 * server.start();
 * ...
 * server.close();
 * }</pre>
 *
 * It serves the four patterns to every client that connects: calls, and the
 * three kinds of stream, with the flow control of the protocol. A client
 * that reconnects is taken for the same session when it shows the next link
 * of its hash chain. A handler runs on a thread of its own for as long as its
 * request lasts, and a request that is cancelled, or whose connection is
 * lost, has its handler interrupted.
 *
 * <p>What is not there yet: a server cannot call its clients.
 */
public interface VrpcServer extends Registrar, Closeable {

    /** The default number of requests of one client that may be served at once. */
    long DEFAULT_MAX_CONCURRENT_REQUESTS = 4096;

    /** The default number of messages that may wait to be written to one client. */
    int DEFAULT_SEND_QUEUE = 4096;

    /** The default number of values of a stream that may wait to be taken: the credit a client is granted. */
    int DEFAULT_STREAM_WINDOW = 4096;

    /** The default time a connection has to send its handshake: ten seconds. */
    long DEFAULT_HANDSHAKE_TIMEOUT_MILLIS = 10_000;

    /** The default time the session of a client that is gone is kept, for it to reconnect: two minutes. */
    long DEFAULT_SESSION_RETENTION_MILLIS = 120_000;

    /** The default time closing waits for what is queued to be written: two seconds. */
    long DEFAULT_CLOSE_FLUSH_MILLIS = 2000;

    static Builder builder() {
        return new Builder();
    }

    /**
     * Begins to accept connections. What is to be served is registered
     * before; more may be registered while the server runs.
     *
     * @throws IllegalStateException it was started already
     */
    void start();

    /**
     * The address the server listens on, as its transport gives it: with the
     * port it was given when it asked for any.
     */
    String address();

    /**
     * What the server holds at this moment, by name: {@code connections},
     * {@code sessions} (those of the clients that are connected, and of those
     * that left not long ago), and {@code requests}, the number taken in
     * since it started.
     */
    Map<String, Long> stats();

    /**
     * Closes the server: it accepts no more, writes what is queued for its
     * clients, the answers of the calls that are done above all, and closes
     * their connections, which cancels every request still running. A closed
     * server stays closed.
     */
    @Override
    void close();

    /** Builds a server. */
    final class Builder {
        Listener listener;
        String address;
        TransportOptions transport = TransportOptions.DEFAULT;
        Dialect dialect = Dialect.standard();
        ConnectAuthorizer authorizer;
        Authenticator authenticator;
        long maxConnections;
        long maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;
        long maxConcurrentStreams;
        int sendQueue = DEFAULT_SEND_QUEUE;
        int streamWindow = DEFAULT_STREAM_WINDOW;
        long handshakeTimeoutMillis = DEFAULT_HANDSHAKE_TIMEOUT_MILLIS;
        long sessionRetentionMillis = DEFAULT_SESSION_RETENTION_MILLIS;
        long closeFlushMillis = DEFAULT_CLOSE_FLUSH_MILLIS;
        Metrics metrics = Metrics.NONE;
        ServerListener events = new ServerListener() {
        };
        Executor executor;

        Builder() {
        }

        /**
         * Where to listen: {@code host:port} or {@code tcp://host:port} for
         * TCP, {@code ws://host:port/path} for WebSocket, {@code mem://name}
         * for clients in this process. Port 0 asks for any port that is
         * free; {@link VrpcServer#address} then says which.
         */
        public Builder address(String address) {
            this.address = address;
            this.listener = null;
            return this;
        }

        /** A transport of one's own, in place of an address: a listener that was made already. */
        public Builder listen(Listener listener) {
            this.listener = listener;
            this.address = null;
            return this;
        }

        /** How the transport an address names is set up. */
        public Builder transportOptions(TransportOptions options) {
            this.transport = options;
            return this;
        }

        /** The names of the fields on the wire; both ends must use the same. */
        public Builder dialect(Dialect dialect) {
            this.dialect = dialect;
            return this;
        }

        /** Lets a connection in or not, before its handshake. */
        public Builder connectAuthorizer(ConnectAuthorizer authorizer) {
            this.authorizer = authorizer;
            return this;
        }

        /** Says who a client is, from the credential of its handshake. Without one every client is let in. */
        public Builder authenticator(Authenticator authenticator) {
            this.authenticator = authenticator;
            return this;
        }

        /** How many connections may be open at once; one too many is closed at once. Not positive for no limit. */
        public Builder maxConnections(long connections) {
            this.maxConnections = connections;
            return this;
        }

        /**
         * How many requests of one client may be served at once. One too
         * many is refused with {@code RESOURCE_EXHAUSTED} and is not queued.
         * A stream counts until its handler has taken it. Not positive for
         * no limit.
         */
        public Builder maxConcurrentRequests(long requests) {
            this.maxConcurrentRequests = requests;
            return this;
        }

        /** How many streams of one client may be open at once. Not positive for no limit, which is the default. */
        public Builder maxConcurrentStreams(long streams) {
            this.maxConcurrentStreams = streams;
            return this;
        }

        /** How many messages may wait to be written to one client before a handler that sends waits. */
        public Builder sendQueue(int capacity) {
            if (capacity <= 0) {
                throw new IllegalArgumentException("send queue " + capacity);
            }
            this.sendQueue = capacity;
            return this;
        }

        /**
         * How many values of a stream from a client may wait here to be
         * taken. The client is granted credit for that many and no more, and
         * gets more as the handler takes them.
         */
        public Builder streamWindow(int values) {
            if (values <= 0) {
                throw new IllegalArgumentException("stream window " + values);
            }
            this.streamWindow = values;
            return this;
        }

        /** How long a connection has to send its handshake before it is closed; not positive for no limit. */
        public Builder handshakeTimeoutMillis(long millis) {
            this.handshakeTimeoutMillis = millis;
            return this;
        }

        /**
         * How long the session of a client whose connection is gone is kept,
         * so that it can reconnect as the same session. 0 keeps none;
         * negative keeps every session until the server closes.
         */
        public Builder sessionRetentionMillis(long millis) {
            this.sessionRetentionMillis = millis;
            return this;
        }

        /**
         * How long closing waits for the messages that are queued to be
         * written before it closes the connections. The wait ends as soon as
         * they are written. Not positive closes at once, and drops them.
         */
        public Builder closeFlushMillis(long millis) {
            this.closeFlushMillis = millis;
            return this;
        }

        public Builder metrics(Metrics metrics) {
            this.metrics = metrics == null ? Metrics.NONE : metrics;
            return this;
        }

        /** Told what happens to the connections. */
        public Builder listener(ServerListener listener) {
            if (listener != null) {
                this.events = listener;
            }
            return this;
        }

        /**
         * What runs the handlers, each for as long as its request lasts. It
         * must give every task a thread of its own without making it wait
         * for another to end: one that queues would let a stream that is
         * open hold up a call. Without one the server has threads of its
         * own, as many as there are requests. It is not shut down when the
         * server closes.
         */
        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        /**
         * Makes the server, and binds the address it was given.
         *
         * @throws IOException the address cannot be listened on
         */
        public VrpcServer build() throws IOException {
            if (listener == null) {
                if (address == null) {
                    throw new IllegalStateException("a server needs an address or a listener");
                }
                listener = Transports.listener(address, transport);
            }
            return new ServerImpl(this);
        }
    }
}
