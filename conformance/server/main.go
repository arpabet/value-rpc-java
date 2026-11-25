/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Command server is the Go implementation as a peer for the tests of the Java
// one: a real value-rpc server with every pattern, and the calls a server makes
// back to its client. Its functions are those of the conformance server of
// value-rpc-js, so the same scenarios run against it.
//
// It listens on TCP and on WebSocket, prints one line for each when it is
// ready,
//
//	LISTENING tcp 127.0.0.1:53124
//	LISTENING ws 127.0.0.1:53125/rpc
//
// and exits when its standard input ends.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"go.arpabet.com/value"
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

	state := &shared{}
	for _, srv := range []valueserver.Server{tcp, ws} {
		register(srv, state)
		go func(srv valueserver.Server) {
			if err := srv.Run(); err != nil {
				fmt.Fprintln(os.Stderr, "run:", err)
			}
		}(srv)
	}

	fmt.Printf("LISTENING tcp %s\n", tcp.Addr().String())
	fmt.Printf("LISTENING ws %s/rpc\n", ws.Addr().String())
	os.Stdout.Sync()

	// The test that started this closes its standard input when it is done.
	io.Copy(io.Discard, os.Stdin)
	tcp.Close()
	ws.Close()
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
}

func register(srv valueserver.Server, state *shared) {
	// The latest connection, which "kick" closes from the server's side: a
	// real drop, to reconnect and resume the session from.
	var lastConn atomic.Value
	srv.SetConnectAuthorizer(func(conn valuerpc.MsgConn) error {
		lastConn.Store(conn)
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
