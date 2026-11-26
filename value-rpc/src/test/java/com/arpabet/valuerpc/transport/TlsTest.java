/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.value.Value;
import com.arpabet.valuerpc.Code;
import com.arpabet.valuerpc.Pki;
import com.arpabet.valuerpc.VrpcException;
import com.arpabet.valuerpc.client.VrpcClient;
import com.arpabet.valuerpc.server.VrpcServer;
import java.io.IOException;
import java.net.Socket;
import java.security.cert.X509Certificate;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The transports over TLS, {@code tls://} and {@code wss://}, with a server
 * and a client of this library: who is let in and who is not, by what each
 * side shows and trusts. That the protocol runs over them as over any other
 * transport is in the tests of the server, which run over them too.
 */
@Timeout(60)
class TlsTest {

    private static final Pki PKI = Pki.get();
    private static final String[] SCHEMES = {"tls://", "wss://"};

    private static VrpcServer server(String scheme, TransportOptions options) throws IOException {
        final VrpcServer server = VrpcServer.builder().address(scheme + "127.0.0.1:0").transportOptions(options)
                // Who a client is by its certificate, when it showed one.
                .authenticator((conn, credential) -> {
                    final SSLSession tls = conn.tlsSession();
                    assertNotNull(tls, "a connection over TLS has a session");
                    try {
                        return ((X509Certificate) tls.getPeerCertificates()[0]).getSubjectX500Principal().getName();
                    } catch (javax.net.ssl.SSLPeerUnverifiedException e) {
                        return "nobody";
                    }
                }).build();
        server.addFunction("whoami", (ctx, args) -> Value.utf8(ctx.principal()));
        server.start();
        return server;
    }

    private static VrpcClient client(String scheme, VrpcServer server, String host, TransportOptions options) {
        final String port = server.address().substring(server.address().indexOf(':'));
        return VrpcClient.builder().address(scheme + host + port).transportOptions(options).dialTimeoutMillis(5000)
                .timeoutMillis(5000).build();
    }

    private static TransportOptions with(javax.net.ssl.SSLContext context) {
        return TransportOptions.DEFAULT.withTls(context);
    }

    @Test
    void aServerIsKnownByItsCertificate() throws Exception {
        for (String scheme : SCHEMES) {
            try (VrpcServer server = server(scheme, with(PKI.server()))) {
                // By the address of the certificate, and by its name: the certificate is for both.
                for (String host : new String[] {"127.0.0.1", "localhost"}) {
                    try (VrpcClient client = client(scheme, server, host, with(PKI.client()))) {
                        assertEquals(Value.utf8("nobody"), client.callFunction("whoami", null), scheme + host);
                    }
                }
                // A client that does not trust who signed the certificate does not connect.
                try (VrpcClient client = client(scheme, server, "127.0.0.1", with(PKI.clientThatTrustsAnother()))) {
                    final VrpcException e = assertThrows(VrpcException.class, client::connect, scheme);
                    assertEquals(Code.UNAVAILABLE, e.code());
                }
                // Nor one that goes by the authorities of the platform, which did not sign it.
                try (VrpcClient client = client(scheme, server, "127.0.0.1", TransportOptions.DEFAULT)) {
                    assertEquals(Code.UNAVAILABLE, assertThrows(VrpcException.class, client::connect, scheme).code());
                }
            }
        }
    }

    @Test
    void aCertificateForAnotherHostIsNotTaken() throws Exception {
        for (String scheme : SCHEMES) {
            // The client trusts the certificate; it is for elsewhere.example, and the server was reached as 127.0.0.1.
            try (VrpcServer server = server(scheme, with(PKI.serverElsewhere()));
                    VrpcClient client = client(scheme, server, "127.0.0.1", with(PKI.client()))) {
                final VrpcException e = assertThrows(VrpcException.class, client::connect, scheme);
                assertEquals(Code.UNAVAILABLE, e.code());
                assertTrue(e.detail().contains("127.0.0.1") || e.detail().toLowerCase().contains("subject alternative"),
                        e.detail());
            }
        }
    }

    @Test
    void aServerThatAsksForACertificateLetsInOnlyWhoShowsOneItTrusts() throws Exception {
        for (String scheme : SCHEMES) {
            try (VrpcServer server = server(scheme, with(PKI.server()).withClientAuth(true))) {
                try (VrpcClient client = client(scheme, server, "127.0.0.1", with(PKI.clientWithCertificate()))) {
                    // The certificate the client showed is with the connection, for the server to say who it is.
                    assertEquals(Value.utf8("CN=vrpc-test-client"), client.callFunction("whoami", null), scheme);
                }
                try (VrpcClient client = client(scheme, server, "127.0.0.1", with(PKI.client()))) {
                    assertEquals(Code.UNAVAILABLE, assertThrows(VrpcException.class, client::connect, scheme).code());
                }
                // The one who was let in has a session, kept a while for it to come back to; the other never had one.
                assertEquals(1L, server.stats().get("sessions"), scheme);
            }
        }
    }

    @Test
    void aClientThatSaysNothingOrNothingOfTlsHoldsUpNobody() throws Exception {
        for (String scheme : SCHEMES) {
            try (VrpcServer server = server(scheme, with(PKI.server()))) {
                final int port = Integer.parseInt(server.address().replaceAll("^.*:(\\d+).*$", "$1"));
                // One that connects and says nothing, and one that speaks without TLS.
                try (Socket silent = new Socket("127.0.0.1", port); Socket plain = new Socket("127.0.0.1", port)) {
                    plain.getOutputStream().write("GET /rpc HTTP/1.1\r\nHost: x\r\n\r\n".getBytes("ISO-8859-1"));
                    plain.getOutputStream().flush();
                    try (VrpcClient client = client(scheme, server, "127.0.0.1", with(PKI.client()))) {
                        assertEquals(Value.utf8("nobody"), client.callFunction("whoami", null), scheme);
                    }
                    // What is not TLS is closed.
                    plain.setSoTimeout(5000);
                    while (plain.getInputStream().read() >= 0) {
                        // whatever the server said of it
                    }
                }
            }
        }
    }

    @Test
    void aListenerOverTlsNeedsACertificate() {
        for (String scheme : SCHEMES) {
            final IOException e = assertThrows(IOException.class,
                    () -> Transports.listener(scheme + "127.0.0.1:0", TransportOptions.DEFAULT).close());
            assertTrue(e.getMessage().contains("requires an SSLContext with a server certificate"), e.getMessage());
        }
    }

    @Test
    void bothSidesSeeWhatTheOtherShowed() throws Exception {
        final LinkedBlockingQueue<String> seen = new LinkedBlockingQueue<String>();
        for (String scheme : SCHEMES) {
            final Listener listener = Transports.listener(scheme + "127.0.0.1:0", with(PKI.server()).withClientAuth(true));
            try {
                listener.start(conn -> {
                    try {
                        seen.add(((X509Certificate) conn.tlsSession().getPeerCertificates()[0]).getSubjectX500Principal().getName());
                    } catch (Exception e) {
                        seen.add(String.valueOf(e));
                    }
                    conn.close();
                });
                final String address = scheme + listener.address();
                try (MsgConn conn = Transports.dialer(address, with(PKI.clientWithCertificate())).dial(5000)) {
                    assertEquals("CN=127.0.0.1",
                            ((X509Certificate) conn.tlsSession().getPeerCertificates()[0]).getSubjectX500Principal().getName());
                }
                assertEquals("CN=vrpc-test-client", seen.poll(10, TimeUnit.SECONDS), scheme);
            } finally {
                listener.close();
            }
        }
    }
}
