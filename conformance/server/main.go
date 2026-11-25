/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Command server is the Go implementation as a peer for the tests of the Java
// one: a real value-rpc server with every pattern, and the calls a server makes
// back to its client. Its functions are those of the conformance server of
// value-rpc-js, so the same scenarios run against it.
//
// It listens on TCP, on WebSocket and on QUIC, prints one line for each when
// it is ready, and one when all of them are,
//
//	LISTENING tcp 127.0.0.1:53124
//	LISTENING tls 127.0.0.1:53129
//	LISTENING ws 127.0.0.1:53125/rpc
//	LISTENING quic 127.0.0.1:53126
//	LISTENING quic-small 127.0.0.1:53127
//	LISTENING quic-mtls 127.0.0.1:53128
//	TLS ca <the certificate authority of the QUIC listeners, DER in base64>
//	TLS client-cert <a certificate for a client, DER in base64>
//	TLS client-key <its key, PKCS #8 in base64>
//	READY
//
// and exits when its standard input ends. The TLS and the QUIC listeners have
// a certificate for 127.0.0.1 and localhost that the authority signed, made
// when the server starts. "quic-small" allows eight requests at once on a
// connection, so that a stream that is not freed is soon missed; "quic-mtls"
// wants a certificate of the client.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.arpabet.com/value"
	valuequic "go.arpabet.com/value-rpc/quic"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

func main() {
	logger := zap.NewNop()
	if os.Getenv("VRPC_CONFORMANCE_VERBOSE") != "" {
		logger, _ = zap.NewDevelopment()
	}

	tcp, err := valueserver.NewServer(env("VRPC_TCP_ADDR", "127.0.0.1:0"), logger)
	if err != nil {
		fail("bind tcp", err)
	}
	ws, err := valueserver.NewWebSocketServer(env("VRPC_WS_ADDR", "127.0.0.1:0"), "/rpc", logger)
	if err != nil {
		fail("bind ws", err)
	}

	pki, err := newPKI()
	if err != nil {
		fail("certificates", err)
	}
	tlsSrv, err := valueserver.NewTLSServer(env("VRPC_TLS_ADDR", "127.0.0.1:0"),
		&tls.Config{Certificates: []tls.Certificate{pki.server}}, logger)
	if err != nil {
		fail("bind tls", err)
	}
	quicAddr := env("VRPC_QUIC_ADDR", "127.0.0.1:0")
	quic, err := valuequic.NewServer(quicAddr, &tls.Config{Certificates: []tls.Certificate{pki.server}}, logger)
	if err != nil {
		fail("bind quic", err)
	}
	// The number of streams a listener allows is read when it is made.
	streams := valuequic.MaxStreams
	valuequic.MaxStreams = 8
	quicSmall, err := valuequic.NewServer("127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{pki.server}}, logger)
	valuequic.MaxStreams = streams
	if err != nil {
		fail("bind quic-small", err)
	}
	quicMTLS, err := valuequic.NewServer("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{pki.server},
		ClientCAs:    pki.pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, logger)
	if err != nil {
		fail("bind quic-mtls", err)
	}

	state := &shared{}
	servers := []valueserver.Server{tcp, tlsSrv, ws, quic, quicSmall, quicMTLS}
	for _, srv := range servers {
		register(srv, state)
		go func(srv valueserver.Server) {
			if err := srv.Run(); err != nil {
				fmt.Fprintln(os.Stderr, "run:", err)
			}
		}(srv)
	}

	fmt.Printf("LISTENING tcp %s\n", tcp.Addr().String())
	fmt.Printf("LISTENING tls %s\n", tlsSrv.Addr().String())
	fmt.Printf("LISTENING ws %s/rpc\n", ws.Addr().String())
	fmt.Printf("LISTENING quic %s\n", quic.Addr().String())
	fmt.Printf("LISTENING quic-small %s\n", quicSmall.Addr().String())
	fmt.Printf("LISTENING quic-mtls %s\n", quicMTLS.Addr().String())
	fmt.Printf("TLS ca %s\n", base64.StdEncoding.EncodeToString(pki.caDER))
	fmt.Printf("TLS client-cert %s\n", base64.StdEncoding.EncodeToString(pki.clientDER))
	fmt.Printf("TLS client-key %s\n", base64.StdEncoding.EncodeToString(pki.clientKey))
	fmt.Println("READY")
	os.Stdout.Sync()

	// The test that started this closes its standard input when it is done.
	io.Copy(io.Discard, os.Stdin)
	for _, srv := range servers {
		srv.Close()
	}
}

// pki is a certificate authority made for one run, and what it signed: a
// certificate for the QUIC listeners and one for a client.
type pki struct {
	pool      *x509.CertPool
	caDER     []byte
	server    tls.Certificate
	clientDER []byte
	clientKey []byte // PKCS #8
}

func newPKI() (*pki, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "vrpc-conformance-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, caKey.Public(), caKey)
	if err != nil {
		return nil, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	issue := func(tmpl *x509.Certificate) ([]byte, *ecdsa.PrivateKey, error) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, nil, err
		}
		tmpl.NotBefore = time.Now().Add(-time.Hour)
		tmpl.NotAfter = time.Now().Add(24 * time.Hour)
		tmpl.KeyUsage = x509.KeyUsageDigitalSignature
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, key.Public(), caKey)
		return der, key, err
	}
	serverDER, serverKey, err := issue(&x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	})
	if err != nil {
		return nil, err
	}
	clientDER, clientKey, err := issue(&x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "vrpc-conformance-client"},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	if err != nil {
		return nil, err
	}
	clientPKCS8, err := x509.MarshalPKCS8PrivateKey(clientKey)
	if err != nil {
		return nil, err
	}
	return &pki{
		pool:      pool,
		caDER:     caDER,
		server:    tls.Certificate{Certificate: [][]byte{serverDER}, PrivateKey: serverKey},
		clientDER: clientDER,
		clientKey: clientPKCS8,
	}, nil
}

func env(name, otherwise string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return otherwise
}

func fail(what string, err error) {
	fmt.Fprintln(os.Stderr, what+":", err)
	os.Exit(1)
}

// shared is what the functions of every listener have in common.
type shared struct {
	sum   atomic.Int64
	count atomic.Int64

	mu    sync.Mutex
	flaky map[string]int64 // how often "flaky" was called, by key
}

func register(srv valueserver.Server, state *shared) {
	// The latest connection, which "kick" closes from the server's side: a
	// real drop, to reconnect and resume the session from. And the name on
	// the certificate its client showed, when it showed one.
	var lastConn atomic.Value
	var lastCN atomic.Value
	lastCN.Store("")
	srv.SetConnectAuthorizer(func(conn valuerpc.MsgConn) error {
		lastConn.Store(conn)
		cn := ""
		if certs, ok := valuerpc.PeerCertificates(conn); ok && len(certs) > 0 {
			cn = certs[0].Subject.CommonName
		}
		lastCN.Store(cn)
		return nil
	})

	// A credential, when there is one, has to be "secret".
	srv.SetAuthenticator(func(conn valuerpc.MsgConn, credential value.Value) (string, error) {
		if credential == nil || credential.Kind() == value.NULL {
			return "", nil
		}
		if s, ok := credential.(value.String); ok && s.String() == "secret" {
			return "tester", nil
		}
		return "", fmt.Errorf("bad credential")
	})

	any := valuerpc.Any

	// --- calls ---
	srv.AddFunction("greet", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		name := "?"
		if l, ok := args.(value.List); ok && l.Len() > 0 {
			name = l.GetAt(0).String()
		}
		return value.Utf8("Hello, " + name + "!"), nil
	})

	srv.AddFunction("add", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		l := args.(value.List)
		return value.Long(l.GetAt(0).(value.Number).Long() + l.GetAt(1).(value.Number).Long()), nil
	})

	srv.AddFunction("echo", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		return args, nil
	})

	srv.AddFunction("nothing", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		return nil, nil
	})

	srv.AddFunction("typed", valuerpc.List(valuerpc.String, valuerpc.NumberOpt), valuerpc.String,
		func(ctx context.Context, args value.Value) (value.Value, error) {
			return value.Utf8("ok"), nil
		})

	srv.AddFunction("fail", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		return nil, valuerpc.NewError(valuerpc.CodeResourceExhausted, "deliberate failure")
	})

	srv.AddFunction("boom", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		return nil, fmt.Errorf("boom")
	})

	// Waits five seconds unless its context ends first, and says which.
	srv.AddFunction("slow", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		select {
		case <-time.After(5 * time.Second):
			return value.Utf8("late"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	// The time the handler has left, in milliseconds: the caller's SLA as the
	// server took it. -1 when the context has no deadline.
	srv.AddFunction("deadline", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		if dl, ok := ctx.Deadline(); ok {
			return value.Long(time.Until(dl).Milliseconds()), nil
		}
		return value.Long(-1), nil
	})

	srv.AddFunction("md.echo", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		md := valuerpc.MetadataFromContext(ctx)
		m := make(map[string]value.Value, len(md))
		for k, v := range md {
			m[k] = value.Utf8(v)
		}
		return value.ImmutableMapOf(m), nil
	})

	srv.AddFunction("whoami", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		return value.Utf8(valuerpc.PrincipalFromContext(ctx)), nil
	})

	// The name on the certificate the client of the latest connection showed.
	srv.AddFunction("tls.cn", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		return value.Utf8(lastCN.Load().(string)), nil
	})

	// Fails as a server that is not up yet, [key, failures]: unavailable for
	// the first calls with a key, and from then on the number of the call.
	srv.AddFunction("flaky", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		l, ok := args.(value.List)
		if !ok || l.Len() < 2 {
			return nil, valuerpc.NewError(valuerpc.CodeInvalidArgument, "flaky wants [key, failures]")
		}
		key := l.GetAt(0).String()
		failures := l.GetAt(1).(value.Number).Long()
		state.mu.Lock()
		if state.flaky == nil {
			state.flaky = make(map[string]int64)
		}
		state.flaky[key]++
		n := state.flaky[key]
		state.mu.Unlock()
		if n <= failures {
			return nil, valuerpc.NewError(valuerpc.CodeUnavailable, "warming up, call %d", n)
		}
		return value.Long(n), nil
	})

	srv.AddFunction("kick", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		go func() {
			time.Sleep(50 * time.Millisecond) // let the answer leave first
			if c := lastConn.Load(); c != nil {
				c.(valuerpc.MsgConn).Close()
			}
		}()
		return value.Utf8("kicking"), nil
	})

	// --- a stream to the client ---
	srv.AddOutgoingStream("count", any, func(ctx context.Context, args value.Value) (<-chan value.Value, error) {
		n := int64(0)
		if l, ok := args.(value.List); ok && l.Len() > 0 {
			n = l.GetAt(0).(value.Number).Long()
		}
		if n < 0 {
			return nil, valuerpc.NewError(valuerpc.CodeInvalidArgument, "count must not be negative")
		}
		out := make(chan value.Value)
		go func() {
			defer close(out)
			for i := int64(1); i <= n; i++ {
				select {
				case out <- value.Long(i):
				case <-ctx.Done():
					return
				}
			}
		}()
		return out, nil
	})

	// --- a stream from the client ---
	srv.AddIncomingStream("upload", any, func(ctx context.Context, args value.Value, inC <-chan value.Value) error {
		go func() {
			for v := range inC {
				if n, ok := v.(value.Number); ok {
					state.sum.Add(n.Long())
					state.count.Add(1)
				}
			}
		}()
		return nil
	})

	srv.AddFunction("upload.stats", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		return value.ImmutableMapOf(map[string]value.Value{
			"sum":   value.Long(state.sum.Load()),
			"count": value.Long(state.count.Load()),
		}), nil
	})

	srv.AddFunction("upload.reset", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		state.sum.Store(0)
		state.count.Store(0)
		return nil, nil
	})

	// --- both at once: each message comes back with a prefix ---
	srv.AddChat("chat.echo", any, func(ctx context.Context, args value.Value, inC <-chan value.Value) (<-chan value.Value, error) {
		out := make(chan value.Value)
		go func() {
			defer close(out)
			for v := range inC {
				select {
				case out <- value.Utf8("echo:" + v.String()):
				case <-ctx.Done():
					return
				}
			}
		}()
		return out, nil
	})

	// Takes what the client sends, counts it as "upload" does, and says
	// nothing: its side of the chat ends at once, and the client's goes on.
	srv.AddChat("chat.sink", any, func(ctx context.Context, args value.Value, inC <-chan value.Value) (<-chan value.Value, error) {
		out := make(chan value.Value)
		close(out)
		go func() {
			for v := range inC {
				if n, ok := v.(value.Number); ok {
					state.sum.Add(n.Long())
					state.count.Add(1)
				}
			}
		}()
		return out, nil
	})

	// --- the server calls its client ---
	srv.AddFunction("reverse.call", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		peer, ok := valueserver.PeerFromContext(ctx)
		if !ok {
			return nil, valuerpc.NewError(valuerpc.CodeInternal, "no peer")
		}
		return peer.CallFunction(ctx, "notify", args)
	})

	srv.AddFunction("reverse.pull", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		peer, ok := valueserver.PeerFromContext(ctx)
		if !ok {
			return nil, valuerpc.NewError(valuerpc.CodeInternal, "no peer")
		}
		ch, _, err := peer.GetStream(ctx, "tail", args, 16)
		if err != nil {
			return nil, err
		}
		list := value.EmptyList(true)
		for v := range ch {
			list = list.Append(v)
		}
		return list, nil
	})

	srv.AddFunction("reverse.push", any, any, func(ctx context.Context, args value.Value) (value.Value, error) {
		peer, ok := valueserver.PeerFromContext(ctx)
		if !ok {
			return nil, valuerpc.NewError(valuerpc.CodeInternal, "no peer")
		}
		putCh := make(chan value.Value)
		go func() {
			defer close(putCh)
			for i := int64(1); i <= 3; i++ {
				putCh <- value.Long(i * 100)
			}
		}()
		// The context of this call ends when it returns, and the push is bound
		// to its context: detach it, so that the push outlives the answer.
		if err := peer.PutStream(context.WithoutCancel(ctx), "ingest", args, putCh); err != nil {
			return nil, err
		}
		return value.Utf8("pushed"), nil
	})
}
