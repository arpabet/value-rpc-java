/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.client;

import com.arpabet.value.Value;
import com.arpabet.valuerpc.ClientInterceptor;
import com.arpabet.valuerpc.Dialect;
import com.arpabet.valuerpc.Metrics;
import com.arpabet.valuerpc.Peer;
import com.arpabet.valuerpc.Registrar;
import com.arpabet.valuerpc.VrpcException;
import com.arpabet.valuerpc.transport.Dialer;
import com.arpabet.valuerpc.transport.TransportOptions;
import com.arpabet.valuerpc.transport.Transports;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A client of a vRPC server: the Go implementation's
 * {@code valueclient.Client}.
 *
 * <pre>{@code
 * VrpcClient client = VrpcClient.builder().address("tcp://example.com:9000").build();
 * Value greeting = client.callFunction("greet", Value.list(Value.utf8("world")));
 * client.close();
 * }</pre>
 *
 * A client connects when it is first used, and again after its connection is
 * lost. It is safe to use from many threads, and one connection carries all
 * their calls at once.
 *
 * <p>A connection is used in both directions. What a client registers
 * ({@link Registrar}) its server may ask of it, once the client is
 * connected: a call, or any of the three streams. A name registered again
 * serves the new handler from then on.
 *
 * <pre>{@code
 * client.addFunction("notify", (context, args) -> {
 *     show(args);
 *     return null;
 * });
 * client.connect();                    // the server can reach it from here on
 * }</pre>
 */
public interface VrpcClient extends Peer, Registrar, Closeable {

    /** The default time a call may take: one second, as in the Go implementation. */
    long DEFAULT_TIMEOUT_MILLIS = 1000;

    /** The default time connecting may take: thirty seconds. */
    long DEFAULT_DIAL_TIMEOUT_MILLIS = 30_000;

    /** The default number of messages that may wait to be written. */
    int DEFAULT_SEND_QUEUE = 1024;

    /** The default number of values of a stream that may wait to be taken: the credit a sender is granted. */
    int DEFAULT_STREAM_WINDOW = 4096;

    /** The default number of calls of the server that may be served at once. */
    long DEFAULT_MAX_CONCURRENT_INBOUND = 1024;

    static Builder builder() {
        return new Builder();
    }

    /** The id the client chose for its session, which a handshake carries. */
    long clientId();

    /**
     * Connects, unless it is connected: dials, and exchanges the handshake.
     * A call does this itself; connecting first tells apart a server that
     * cannot be reached, or that refuses the credential, from a call that
     * fails.
     *
     * @throws VrpcException {@code UNAVAILABLE} when the server cannot be
     *         reached, {@code UNAUTHENTICATED} when it refuses the handshake
     */
    void connect();

    /** Whether there is a connection at this moment. */
    boolean isActive();

    /**
     * Sets how long a call may take unless its options say otherwise, in
     * milliseconds; not positive for no limit.
     */
    void setTimeoutMillis(long millis);

    /**
     * Sets what a handshake carries for the server's authenticator, also when
     * the client reconnects. Null for none.
     */
    void setCredential(Value credential);

    /** The number of requests made and of reconnects, by name. */
    Map<String, Long> stats();

    /**
     * Closes the client: its connection, and every call that waits, which
     * fails with {@code UNAVAILABLE}. A closed client stays closed.
     */
    @Override
    void close();

    /** Builds a client. */
    final class Builder {
        Dialer dialer;
        String address;
        TransportOptions transport = TransportOptions.DEFAULT;
        Dialect dialect = Dialect.standard();
        long timeoutMillis = DEFAULT_TIMEOUT_MILLIS;
        long dialTimeoutMillis = DEFAULT_DIAL_TIMEOUT_MILLIS;
        int sendQueue = DEFAULT_SEND_QUEUE;
        int streamWindow = DEFAULT_STREAM_WINDOW;
        long maxConcurrentInbound = DEFAULT_MAX_CONCURRENT_INBOUND;
        Value credential;
        Supplier<Map<String, String>> metadata;
        Metrics metrics = Metrics.NONE;
        ClientListener listener = new ClientListener() {
        };
        final List<ClientInterceptor> interceptors = new ArrayList<ClientInterceptor>();

        Builder() {
        }

        /**
         * The address of the server: {@code host:port} or
         * {@code tcp://host:port} for TCP, {@code mem://name} for a server in
         * this process.
         */
        public Builder address(String address) {
            this.address = address;
            this.dialer = null;
            return this;
        }

        /** A transport of one's own, in place of an address. */
        public Builder dialer(Dialer dialer) {
            this.dialer = dialer;
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

        /** How long a call may take unless its options say otherwise; not positive for no limit. */
        public Builder timeoutMillis(long millis) {
            this.timeoutMillis = millis;
            return this;
        }

        /** How long connecting may take, the handshake included; not positive for no limit. */
        public Builder dialTimeoutMillis(long millis) {
            this.dialTimeoutMillis = millis;
            return this;
        }

        /** How many messages may wait to be written before a caller waits. */
        public Builder sendQueue(int capacity) {
            if (capacity <= 0) {
                throw new IllegalArgumentException("send queue " + capacity);
            }
            this.sendQueue = capacity;
            return this;
        }

        /**
         * How many values of a stream may wait here to be taken. The server
         * is granted credit for that many and no more, and gets more as the
         * application takes them.
         */
        public Builder streamWindow(int values) {
            if (values <= 0) {
                throw new IllegalArgumentException("stream window " + values);
            }
            this.streamWindow = values;
            return this;
        }

        /**
         * How many calls of the server this client serves at once; 0 for any
         * number. One too many is answered {@code RESOURCE_EXHAUSTED}, and
         * the ones that run are not disturbed. Streams are not counted.
         */
        public Builder maxConcurrentInbound(long calls) {
            if (calls < 0) {
                throw new IllegalArgumentException("max concurrent inbound " + calls);
            }
            this.maxConcurrentInbound = calls;
            return this;
        }

        /** What a handshake carries for the server's authenticator. */
        public Builder credential(Value credential) {
            this.credential = credential;
            return this;
        }

        /**
         * What travels with every request, asked for each one: a trace
         * context, say. A call adds its own with its options.
         */
        public Builder metadata(Supplier<Map<String, String>> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder metrics(Metrics metrics) {
            this.metrics = metrics == null ? Metrics.NONE : metrics;
            return this;
        }

        /** Told what happens to the connection. */
        public Builder listener(ClientListener listener) {
            if (listener != null) {
                this.listener = listener;
            }
            return this;
        }

        /** Interceptors around every call, the first given outermost. Given again, they add. */
        public Builder interceptors(ClientInterceptor... interceptors) {
            this.interceptors.addAll(Arrays.asList(interceptors));
            return this;
        }

        public VrpcClient build() {
            if (dialer == null) {
                if (address == null) {
                    throw new IllegalStateException("a client needs an address or a dialer");
                }
                dialer = Transports.dialer(address, transport);
            }
            return new ClientImpl(this);
        }
    }
}
