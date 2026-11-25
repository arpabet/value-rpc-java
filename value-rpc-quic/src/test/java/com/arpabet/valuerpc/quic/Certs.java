/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.quic;

import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.ssl.ClientAuth;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;

/**
 * A certificate authority made for a test, and what it signed: a certificate
 * for a server on this host, and one for a client. They are those of the
 * tests of the Go implementation ({@code genCertPair}).
 */
final class Certs {

    final X509Bundle ca;
    final X509Bundle server;
    final X509Bundle client;

    Certs() throws Exception {
        ca = new CertificateBuilder().subject("CN=vrpc-test-ca").setIsCertificateAuthority(true).ecp256()
                .buildSelfSigned();
        server = new CertificateBuilder().subject("CN=127.0.0.1").addSanIpAddress("127.0.0.1").addSanDnsName("localhost")
                .addExtendedKeyUsageServerAuth().ecp256().buildIssuedBy(ca);
        client = new CertificateBuilder().subject("CN=vrpc-test-client").addExtendedKeyUsageClientAuth().ecp256()
                .buildIssuedBy(ca);
    }

    /** The context of a server with the certificate. */
    QuicSslContext serverTls() {
        return QuicTransport.serverTls(server.getKeyPair().getPrivate(), null, server.getCertificatePath()).build();
    }

    /** The context of a server that wants a certificate of its client, signed by the authority. */
    QuicSslContext serverMutualTls() {
        return QuicTransport.serverTls(server.getKeyPair().getPrivate(), null, server.getCertificatePath())
                .trustManager(ca.getCertificate()).clientAuth(ClientAuth.REQUIRE).build();
    }

    /** The context of a client that trusts the authority. */
    QuicSslContext clientTls() {
        return QuicTransport.clientTls().trustManager(ca.getCertificate()).build();
    }

    /** The context of a client that trusts the authority and shows its certificate. */
    QuicSslContext clientTlsWithCertificate() {
        return QuicTransport.clientTls().trustManager(ca.getCertificate())
                .keyManager(client.getKeyPair().getPrivate(), null, client.getCertificatePath()).build();
    }

    /** Options of a listener with the certificate. */
    QuicOptions server() {
        return QuicOptions.DEFAULT.withSslContext(serverTls());
    }

    /** Options of a dialer that trusts the authority. */
    QuicOptions client() {
        return QuicOptions.DEFAULT.withSslContext(clientTls());
    }
}
