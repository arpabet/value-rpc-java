/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc;

import com.arpabet.value.Kind;
import com.arpabet.value.ListValue;
import com.arpabet.value.MapValue;
import com.arpabet.value.NumberValue;
import com.arpabet.value.Value;
import com.arpabet.valuerpc.server.VrpcServer;
import com.arpabet.valuerpc.transport.Listener;
import com.arpabet.valuerpc.transport.MsgConn;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

/**
 * The Java server as a peer for the tests of the clients: the functions of
 * {@code conformance/server}, the Go one, by the same names and with the same
 * answers, so that what is asked of one can be asked of the other, the ones
 * that ask the client back among them ({@code reverse.call},
 * {@code reverse.pull}, {@code reverse.push}).
 *
 * <p>As a program it listens on TCP and on WebSocket, prints one line for
 * each when it is ready, and one when all of them are,
 *
 * <pre>
 *   LISTENING tcp 127.0.0.1:53124
 *   LISTENING ws 127.0.0.1:53125/rpc
 *   READY
 * </pre>
 *
 * and exits when its standard input ends, as the Go one does.
 */
public final class ConformanceServer implements AutoCloseable {

    /** What the functions of every listener have in common. */
    public static final class Shared {
        public final AtomicLong sum = new AtomicLong();
        public final AtomicLong count = new AtomicLong();
        private final Map<String, Long> flaky = new HashMap<String, Long>();
    }

    private final VrpcServer server;
    /** The latest connection, which {@code kick} closes from the server's side. */
    private final AtomicReference<MsgConn> lastConn;
    /** The name on the certificate the latest client showed over TLS; empty when it showed none. */
    private final AtomicReference<String> lastCn;

    private ConformanceServer(VrpcServer server, AtomicReference<MsgConn> lastConn, AtomicReference<String> lastCn) {
        this.server = server;
        this.lastConn = lastConn;
        this.lastCn = lastCn;
    }

    /** The common name of the certificate a connection was made with, or empty when it was made with none. */
    private static String commonName(MsgConn conn) {
        final SSLSession tls = conn.tlsSession();
        if (tls == null) {
            return "";
        }
        try {
            final String name = ((X509Certificate) tls.getPeerCertificates()[0]).getSubjectX500Principal().getName();
            for (String part : name.split(",")) {
                if (part.trim().startsWith("CN=")) {
                    return part.trim().substring(3);
                }
            }
            return name;
        } catch (SSLPeerUnverifiedException e) {
            return "";
        }
    }

    /** A server with the functions, on an address: {@code tcp://127.0.0.1:0}, {@code mem://name}, {@code ws://127.0.0.1:0/rpc}. */
    public static ConformanceServer start(String address, Shared state) throws IOException {
        return start(VrpcServer.builder().address(address), state);
    }

    /** A server with the functions, on a listener of the caller's own making. */
    public static ConformanceServer start(Listener listener, Shared state) throws IOException {
        return start(VrpcServer.builder().listen(listener), state);
    }

    /**
     * A server with the functions, on a builder the caller has set up: with
     * its limits, say. The authorizer and the authenticator are set here.
     */
    public static ConformanceServer start(VrpcServer.Builder builder, Shared state) throws IOException {
        final AtomicReference<MsgConn> lastConn = new AtomicReference<MsgConn>();
        final AtomicReference<String> lastCn = new AtomicReference<String>("");
        final VrpcServer server = builder
                .connectAuthorizer(conn -> {
                    lastConn.set(conn);
                    lastCn.set(commonName(conn));
                })
                // A credential, when there is one, has to be "secret".
                .authenticator((conn, credential) -> {
                    if (credential == null || credential.isNull()) {
                        return "";
                    }
                    if (credential.kind() == Kind.STRING && credential.toString().equals("secret")) {
                        return "tester";
                    }
                    throw new SecurityException("bad credential");
                })
                .build();
        register(server, state, lastConn);
        // The name on the certificate of the latest client, as the Go server has it.
        server.addFunction("tls.cn", (ctx, args) -> Value.utf8(lastCn.get()));
        server.start();
        return new ConformanceServer(server, lastConn, lastCn);
    }

    /** The name on the certificate the latest client showed over TLS; empty when it showed none. */
    public String lastClientName() {
        return lastCn.get();
    }

    public VrpcServer server() {
        return server;
    }

    /** The address it listens on, as its listener gives it. */
    public String address() {
        return server.address();
    }

    /** Closes the latest connection from the server's side, as a link that drops. */
    public void drop() {
        final MsgConn c = lastConn.get();
        if (c != null) {
            c.close();
        }
    }

    @Override
    public void close() {
        server.close();
    }

    private static long number(Value args, int index) {
        return ((ListValue) args).getNumber(index).longValue();
    }

    private static void register(VrpcServer srv, Shared state, AtomicReference<MsgConn> lastConn) {
        // --- calls ---
        srv.addFunction("greet", (ctx, args) -> {
            String name = "?";
            if (args.kind() == Kind.LIST && ((ListValue) args).size() > 0) {
                name = ((ListValue) args).get(0).toString();
            }
            return Value.utf8("Hello, " + name + "!");
        });

        srv.addFunction("add", (ctx, args) -> Value.ofLong(number(args, 0) + number(args, 1)));

        srv.addFunction("echo", (ctx, args) -> args);

        srv.addFunction("nothing", (ctx, args) -> null);

        srv.addFunction("typed", TypeDef.list(TypeDef.STRING, TypeDef.NUMBER_OPT), TypeDef.STRING,
                (ctx, args) -> Value.utf8("ok"));

        srv.addFunction("fail", (ctx, args) -> {
            throw new VrpcException(Code.RESOURCE_EXHAUSTED, "deliberate failure");
        });

        srv.addFunction("boom", (ctx, args) -> {
            throw new IllegalStateException("boom");
        });

        // Waits five seconds unless its request is cancelled first.
        srv.addFunction("slow", (ctx, args) -> {
            Thread.sleep(5000);
            return Value.utf8("late");
        });

        // The time the handler has left, in milliseconds: the caller's
        // timeout as the server took it. -1 when the caller named none.
        srv.addFunction("deadline", (ctx, args) -> Value.ofLong(ctx.remainingMillis()));

        srv.addFunction("md.echo", (ctx, args) -> {
            final MapValue.Builder b = MapValue.builder();
            for (Map.Entry<String, String> e : ctx.metadata().entrySet()) {
                b.put(e.getKey(), e.getValue());
            }
            return b.build();
        });

        srv.addFunction("whoami", (ctx, args) -> Value.utf8(ctx.principal()));

        // Fails as a server that is not up yet, [key, failures]: unavailable
        // for the first calls with a key, and from then on the number of the call.
        srv.addFunction("flaky", (ctx, args) -> {
            if (args.kind() != Kind.LIST || ((ListValue) args).size() < 2) {
                throw new VrpcException(Code.INVALID_ARGUMENT, "flaky wants [key, failures]");
            }
            final String key = ((ListValue) args).get(0).toString();
            final long failures = number(args, 1);
            final long n;
            synchronized (state.flaky) {
                final Long before = state.flaky.get(key);
                n = (before == null ? 0 : before) + 1;
                state.flaky.put(key, n);
            }
            if (n <= failures) {
                throw new VrpcException(Code.UNAVAILABLE, "warming up, call " + n);
            }
            return Value.ofLong(n);
        });

        srv.addFunction("kick", (ctx, args) -> {
            final Thread t = new Thread(() -> {
                try {
                    Thread.sleep(50); // let the answer leave first
                } catch (InterruptedException e) {
                    return;
                }
                final MsgConn c = lastConn.get();
                if (c != null) {
                    c.close();
                }
            }, "conformance-kick");
            t.setDaemon(true);
            t.start();
            return Value.utf8("kicking");
        });

        // --- a stream to the client ---
        srv.addOutgoingStream("count", TypeDef.ANY, (ctx, args, out) -> {
            long n = 0;
            if (args.kind() == Kind.LIST && ((ListValue) args).size() > 0) {
                n = number(args, 0);
            }
            if (n < 0) {
                throw new VrpcException(Code.INVALID_ARGUMENT, "count must not be negative");
            }
            for (long i = 1; i <= n; i++) {
                out.send(Value.ofLong(i));
            }
        });

        // --- a stream from the client ---
        srv.addIncomingStream("upload", TypeDef.ANY, (ctx, args, in) -> {
            for (Value v : in) {
                if (v.kind() == Kind.NUMBER) {
                    state.sum.addAndGet(((NumberValue) v).longValue());
                    state.count.incrementAndGet();
                }
            }
        });

        srv.addFunction("upload.stats", (ctx, args) -> MapValue.builder()
                .put("sum", state.sum.get())
                .put("count", state.count.get())
                .build());

        srv.addFunction("upload.reset", (ctx, args) -> {
            state.sum.set(0);
            state.count.set(0);
            return null;
        });

        // --- both at once: each message comes back with a prefix ---
        srv.addChat("chat.echo", TypeDef.ANY, (ctx, args, in, out) -> {
            for (Value v : in) {
                out.send(Value.utf8("echo:" + v));
            }
        });

        // Says nothing: its side of the chat ends at once, and with it the chat.
        srv.addChat("chat.sink", TypeDef.ANY, (ctx, args, in, out) -> {
            out.close();
            for (Value v : in) {
                if (v.kind() == Kind.NUMBER) {
                    state.sum.addAndGet(((NumberValue) v).longValue());
                    state.count.incrementAndGet();
                }
            }
        });

        registerReverse(srv);
    }

    /** The server as a program: see the class. */
    /**
     * The functions that ask the client back, on the connection the call
     * came on: what the client registered as {@code notify}, {@code tail}
     * and {@code ingest}.
     */
    private static void registerReverse(VrpcServer srv) {
        // Calls the client, and answers with what it answered.
        srv.addFunction("reverse.call", (ctx, args) -> ctx.peer().callFunction("notify", args));

        // Reads a stream of the client to its end, and answers with all of it.
        srv.addFunction("reverse.pull", (ctx, args) -> {
            final List<Value> all = new ArrayList<Value>();
            try (ValueStream tail = ctx.peer().getStream("tail", args, CallOptions.DEFAULT)) {
                for (Value v : tail) {
                    all.add(v);
                }
            }
            return Value.list(all.toArray(new Value[0]));
        });

        // Sends the client three values, and answers once they are on their way.
        srv.addFunction("reverse.push", (ctx, args) -> {
            try (ValueSink ingest = ctx.peer().putStream("ingest", args, CallOptions.DEFAULT)) {
                for (long i = 1; i <= 3; i++) {
                    ingest.send(Value.ofLong(i * 100));
                }
            }
            return Value.utf8("pushed");
        });
    }

    public static void main(String[] argv) throws IOException {
        final Shared state = new Shared();
        final List<ConformanceServer> servers = new ArrayList<ConformanceServer>();
        final ConformanceServer tcp = start(env("VRPC_TCP_ADDR", "tcp://127.0.0.1:0"), state);
        servers.add(tcp);
        final ConformanceServer ws = start(env("VRPC_WS_ADDR", "ws://127.0.0.1:0/rpc"), state);
        servers.add(ws);
        System.out.println("LISTENING tcp " + tcp.address());
        System.out.println("LISTENING ws " + ws.address());
        System.out.println("READY");
        System.out.flush();
        // Whoever started this closes its standard input when it is done.
        while (System.in.read() >= 0) {
            // nothing is said to it
        }
        for (ConformanceServer s : servers) {
            s.close();
        }
    }

    private static String env(String name, String otherwise) {
        final String v = System.getenv(name);
        return v == null || v.isEmpty() ? otherwise : v;
    }
}
