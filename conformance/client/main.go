/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Command client is the Go implementation as a client, for the tests of what
// the Java one serves: it connects to the address it is given, over QUIC, over
// TCP or over WebSocket, runs the scenarios it is given, and says how each
// went,
//
//	OK calls
//	OK sequential 50
//
// or, for the first that fails, what failed, and exits with 1.
//
//	client -quic 127.0.0.1:9000 -ca ca.pem calls sequential:50 concurrent:16x25 big:1048576
//	client -tcp 127.0.0.1:9001 chatend:6000
//	client -ws ws://127.0.0.1:9002/rpc -codec json calls streams upload chat
//
// With -cert and -key it shows a QUIC server a certificate; -credential is
// what its handshake carries for the authenticator of the server; -timeout
// and -connect-timeout bound a call and connecting.
//
// The server has to have the functions of the test server of the Java
// implementation: greet, echo and fail; and for the scenarios after "big" the
// functions of the conformance server, the Go one or its Java twin.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.arpabet.com/value"
	valuequic "go.arpabet.com/value-rpc/quic"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
)

func main() {
	addr := flag.String("quic", "", "the address of the QUIC server, host:port")
	tcpAddr := flag.String("tcp", "", "the address of a TCP server, host:port, in place of a QUIC one")
	wsURL := flag.String("ws", "", "the URL of a WebSocket server, ws://host:port/path, in place of a QUIC one")
	codec := flag.String("codec", "msgpack", "the codec of a WebSocket connection: msgpack or json")
	credential := flag.String("credential", "", "what the handshake carries for the authenticator of the server")
	caFile := flag.String("ca", "", "the certificate authority of the QUIC server, PEM")
	certFile := flag.String("cert", "", "a certificate to show the server, PEM")
	keyFile := flag.String("key", "", "the key of the certificate, PEM")
	timeout := flag.Int64("timeout", 10000, "the timeout of a call, in milliseconds")
	connectTimeout := flag.Int64("connect-timeout", 10000, "the timeout of connecting, in milliseconds")
	flag.Parse()
	if (*addr == "" || *caFile == "") && *tcpAddr == "" && *wsURL == "" {
		fmt.Fprintln(os.Stderr, "usage: client (-quic host:port -ca ca.pem [-cert c.pem -key k.pem] | -tcp host:port | -ws url [-codec json]) scenario...")
		os.Exit(2)
	}

	// What travels with a call is what its context carries.
	metadata := valueclient.WithMetadata(func(ctx context.Context) valuerpc.Metadata {
		return valuerpc.MetadataFromContext(ctx)
	})
	var cli valueclient.Client
	var err error
	if *wsURL != "" {
		opts := []valueclient.ClientOption{metadata}
		switch *codec {
		case "json":
			opts = append(opts, valueclient.WithWireCodec(valuerpc.JsonWireCodec))
		case "msgpack":
		default:
			fail("codec", fmt.Errorf("%q is not a codec", *codec))
		}
		cli = valueclient.NewWebSocketClient(*wsURL, opts...)
	} else if *tcpAddr != "" {
		cli = valueclient.NewClient(*tcpAddr, "", metadata)
	} else {
		pem, err := os.ReadFile(*caFile)
		if err != nil {
			fail("read the certificate authority", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			fail("read the certificate authority", fmt.Errorf("no certificate in %s", *caFile))
		}
		config := &tls.Config{RootCAs: pool}
		if *certFile != "" {
			cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
			if err != nil {
				fail("read the certificate of the client", err)
			}
			config.Certificates = []tls.Certificate{cert}
		}
		cli = valuequic.NewClient(*addr, config)
	}
	if *credential != "" {
		cli.SetCredential(value.Utf8(*credential))
	}
	// Connecting is bounded, so that a test of a server that does not answer
	// ends. A server that refuses the client closes the connection on it, which
	// the client learns at once (since value-rpc v1.7.1).
	ctx, cancel := context.WithTimeout(bg, time.Duration(*connectTimeout)*time.Millisecond)
	err = cli.ConnectContext(ctx)
	cancel()
	if err != nil {
		fail("connect", err)
	}
	defer cli.Close()
	cli.SetTimeout(*timeout)

	for _, scenario := range flag.Args() {
		name, arg, _ := strings.Cut(scenario, ":")
		var err error
		switch name {
		case "calls":
			err = calls(cli)
		case "sequential":
			err = sequential(cli, number(arg))
		case "concurrent":
			w, n, _ := strings.Cut(arg, "x")
			err = concurrent(cli, number(w), number(n))
		case "big":
			err = big(cli, number(arg))
		case "chatend":
			err = chatEnd(cli, number(arg))
		case "typed":
			err = typed(cli)
		case "streams":
			err = streams(cli)
		case "upload":
			err = upload(cli, number(arg))
		case "chat":
			err = chat(cli)
		case "metadata":
			err = metadataEcho(cli)
		case "whoami":
			err = whoami(cli, arg)
		case "deadline":
			err = deadline(cli)
		case "cancel":
			err = cancelCall(cli)
		case "resume":
			err = resume(cli)
		default:
			err = fmt.Errorf("unknown scenario")
		}
		if err != nil {
			fail(scenario, err)
		}
		fmt.Println("OK", strings.ReplaceAll(scenario, ":", " "))
	}
}

func fail(what string, err error) {
	fmt.Printf("FAILED %s: %v\n", what, err)
	os.Exit(1)
}

func number(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		fail("argument "+s, err)
	}
	return n
}

var bg = context.Background()

// calls makes one call of each kind of outcome.
func calls(cli valueclient.Client) error {
	res, err := cli.CallFunction(bg, "greet", value.Tuple(value.Utf8("go")))
	if err != nil {
		return fmt.Errorf("greet: %w", err)
	}
	if got := res.String(); got != "Hello, go!" {
		return fmt.Errorf("greet = %q", got)
	}

	sent := value.Tuple(value.Long(7), value.Utf8("seven"), value.Boolean(true))
	res, err = cli.CallFunction(bg, "echo", sent)
	if err != nil {
		return fmt.Errorf("echo: %w", err)
	}
	if !res.Equal(sent) {
		return fmt.Errorf("echo = %s, want %s", res, sent)
	}

	// The code the server chose comes back, and the connection is none the worse.
	_, err = cli.CallFunction(bg, "fail", nil)
	if valuerpc.CodeOf(err) != valuerpc.CodeResourceExhausted {
		return fmt.Errorf("fail: code %v of %v, want ResourceExhausted", valuerpc.CodeOf(err), err)
	}
	_, err = cli.CallFunction(bg, "no.such.function", nil)
	if valuerpc.CodeOf(err) != valuerpc.CodeNotFound {
		return fmt.Errorf("no.such.function: code %v of %v, want NotFound", valuerpc.CodeOf(err), err)
	}
	if _, err = cli.CallFunction(bg, "greet", value.Tuple(value.Utf8("again"))); err != nil {
		return fmt.Errorf("greet after the failures: %w", err)
	}
	return nil
}

// sequential makes calls one after the other: each has a stream of its own,
// and a server that allows few at once is soon stuck if one is not freed.
func sequential(cli valueclient.Client, n int) error {
	for i := 0; i < n; i++ {
		res, err := cli.CallFunction(bg, "echo", value.Long(int64(i)))
		if err != nil {
			return fmt.Errorf("call %d: %w", i, err)
		}
		if got := res.(value.Number).Long(); got != int64(i) {
			return fmt.Errorf("echo(%d) = %d", i, got)
		}
	}
	return nil
}

// concurrent makes calls from many goroutines at once: each has to get its
// own answer.
func concurrent(cli valueclient.Client, workers, callsEach int) error {
	var wg sync.WaitGroup
	errc := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < callsEach; i++ {
				n := int64(base*callsEach + i)
				ctx, cancel := context.WithTimeout(bg, 20*time.Second)
				res, err := cli.CallFunction(ctx, "echo", value.Long(n))
				cancel()
				if err != nil {
					errc <- fmt.Errorf("n=%d: %w", n, err)
					return
				}
				if got := res.(value.Number).Long(); got != n {
					errc <- fmt.Errorf("echo(%d) = %d: the answer of another call", n, got)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errc)
	return <-errc
}

// big sends and takes one value of the given size, which is more than a
// stream may carry before it is granted more.
func big(cli valueclient.Client, size int) error {
	sent := strings.Repeat("0123456789abcdef", size/16)
	res, err := cli.CallFunction(bg, "echo", value.Utf8(sent))
	if err != nil {
		return err
	}
	if got := res.String(); got != sent {
		return fmt.Errorf("echo of %d bytes came back as %d bytes", len(sent), len(got))
	}
	return nil
}

// chatEnd says all it has to say on a chat, ends what it sends, and only then
// reads what came back: every value has to come back, also the ones the
// server sends after the client has ended its side.
func chatEnd(cli valueclient.Client, n int) error {
	sendC := make(chan value.Value)
	readC, _, err := cli.Chat(bg, "chat.echo", nil, 16, sendC)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		select {
		case sendC <- value.Long(int64(i)):
		case <-time.After(10 * time.Second):
			return fmt.Errorf("the server took no more after %d of %d values", i, n)
		}
	}
	close(sendC)
	count := 0
	for {
		select {
		case v, ok := <-readC:
			if !ok {
				if count != n {
					return fmt.Errorf("the chat ended after %d of %d values came back", count, n)
				}
				return nil
			}
			if v != nil && v.Kind() != value.NULL {
				count++
			}
		case <-time.After(5 * time.Second):
			return fmt.Errorf("stalled: %d of %d values came back", count, n)
		}
	}
}

// typed asks for what a server checks before its handler sees it: the
// arguments against what the function was registered with, and the pattern.
func typed(cli valueclient.Client) error {
	res, err := cli.CallFunction(bg, "typed", value.Tuple(value.Utf8("a"), value.Long(1)))
	if err != nil {
		return fmt.Errorf("typed: %w", err)
	}
	if got := res.String(); got != "ok" {
		return fmt.Errorf("typed = %q", got)
	}
	_, err = cli.CallFunction(bg, "typed", value.Tuple(value.Long(1)))
	if valuerpc.CodeOf(err) != valuerpc.CodeInvalidArgument {
		return fmt.Errorf("typed with a number for a string: code %v of %v, want InvalidArgument", valuerpc.CodeOf(err), err)
	}
	if _, _, err = cli.GetStream(bg, "greet", nil, 16); valuerpc.CodeOf(err) != valuerpc.CodeInvalidArgument {
		return fmt.Errorf("a function asked for as a stream: code %v of %v, want InvalidArgument", valuerpc.CodeOf(err), err)
	}
	if _, err = cli.CallFunction(bg, "count", nil); valuerpc.CodeOf(err) != valuerpc.CodeInvalidArgument {
		return fmt.Errorf("a stream asked for as a function: code %v of %v, want InvalidArgument", valuerpc.CodeOf(err), err)
	}
	if res, err = cli.CallFunction(bg, "nothing", nil); err != nil || res != nil {
		return fmt.Errorf("nothing = %v, %v", res, err)
	}
	return nil
}

// streams takes streams from the server: to the end, one that has nothing in
// it, one that is refused, and one that is given up on the way.
func streams(cli valueclient.Client) error {
	ch, _, err := cli.GetStream(bg, "count", value.Tuple(value.Long(10000)), 16)
	if err != nil {
		return fmt.Errorf("count: %w", err)
	}
	want := int64(1)
	for {
		select {
		case v, ok := <-ch:
			if !ok {
				if want != 10001 {
					return fmt.Errorf("count ended at %d of 10000", want-1)
				}
				goto counted
			}
			if got := v.(value.Number).Long(); got != want {
				return fmt.Errorf("count gave %d, want %d", got, want)
			}
			want++
		case <-time.After(10 * time.Second):
			return fmt.Errorf("count stalled at %d", want-1)
		}
	}
counted:
	ch, _, err = cli.GetStream(bg, "count", value.Tuple(value.Long(0)), 16)
	if err != nil {
		return fmt.Errorf("count of none: %w", err)
	}
	select {
	case v, ok := <-ch:
		if ok {
			return fmt.Errorf("count of none gave %v", v)
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("count of none did not end")
	}

	if _, _, err = cli.GetStream(bg, "count", value.Tuple(value.Long(-1)), 16); valuerpc.CodeOf(err) != valuerpc.CodeInvalidArgument {
		return fmt.Errorf("a stream the handler refuses: code %v of %v, want InvalidArgument", valuerpc.CodeOf(err), err)
	}

	ctx, cancel := context.WithCancel(bg)
	ch, _, err = cli.GetStream(ctx, "count", value.Tuple(value.Long(100000000)), 16)
	if err != nil {
		cancel()
		return fmt.Errorf("count without end: %w", err)
	}
	for i := int64(1); i <= 3; i++ {
		if got := (<-ch).(value.Number).Long(); got != i {
			cancel()
			return fmt.Errorf("count gave %d, want %d", got, i)
		}
	}
	cancel()
	gaveUp := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				// The server serves on.
				if _, err = cli.CallFunction(bg, "greet", nil); err != nil {
					return fmt.Errorf("greet after a stream was given up: %w", err)
				}
				return nil
			}
		case <-gaveUp:
			return fmt.Errorf("a stream that was given up did not end")
		}
	}
}

// upload sends the server a stream, and asks what it made of it.
func upload(cli valueclient.Client, n int) error {
	if _, err := cli.CallFunction(bg, "upload.reset", nil); err != nil {
		return fmt.Errorf("upload.reset: %w", err)
	}
	putC := make(chan value.Value)
	done, err := cli.PutStreamWait(bg, "upload", value.Tuple(value.Utf8("batch")), putC)
	if err != nil {
		return err
	}
	sum := int64(0)
	for i := 1; i <= n; i++ {
		select {
		case putC <- value.Long(int64(i)):
			sum += int64(i)
		case <-time.After(10 * time.Second):
			return fmt.Errorf("the server took no more after %d of %d values", i-1, n)
		}
	}
	close(putC)
	select {
	case err = <-done:
		if err != nil {
			return fmt.Errorf("the stream ended with %w", err)
		}
	case <-time.After(10 * time.Second):
		return fmt.Errorf("the stream did not end")
	}
	for until := time.Now().Add(10 * time.Second); ; {
		res, err := cli.CallFunction(bg, "upload.stats", nil)
		if err != nil {
			return fmt.Errorf("upload.stats: %w", err)
		}
		stats := res.(value.Map)
		count, total := stats.GetNumber("count").Long(), stats.GetNumber("sum").Long()
		if count == int64(n) && total == sum {
			return nil
		}
		if time.Now().After(until) {
			return fmt.Errorf("the server counted %d values that sum to %d, want %d and %d", count, total, n, sum)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// chat says three things, ends what it says, and hears each of them back.
func chat(cli valueclient.Client) error {
	sendC := make(chan value.Value)
	readC, _, err := cli.Chat(bg, "chat.echo", value.Tuple(value.Utf8("room")), 16, sendC)
	if err != nil {
		return err
	}
	for _, s := range []string{"one", "two", "three"} {
		sendC <- value.Utf8(s)
	}
	close(sendC)
	for _, want := range []string{"echo:one", "echo:two", "echo:three"} {
		select {
		case v, ok := <-readC:
			if !ok {
				return fmt.Errorf("the chat ended before %q", want)
			}
			if got := v.String(); got != want {
				return fmt.Errorf("the chat said %q, want %q", got, want)
			}
		case <-time.After(5 * time.Second):
			return fmt.Errorf("the chat stalled before %q", want)
		}
	}
	select {
	case v, ok := <-readC:
		if ok {
			return fmt.Errorf("the chat said %v after its last", v)
		}
	case <-time.After(5 * time.Second):
		return fmt.Errorf("the chat did not end")
	}
	return nil
}

// metadataEcho sends what travels with a call, and has the handler say what it was told.
func metadataEcho(cli valueclient.Client) error {
	ctx := valuerpc.ContextWithMetadata(bg, valuerpc.Metadata{"traceparent": "00-t", "extra": "x"})
	res, err := cli.CallFunction(ctx, "md.echo", nil)
	if err != nil {
		return err
	}
	md, ok := res.(value.Map)
	if !ok || md.Len() != 2 || md.GetString("traceparent").String() != "00-t" || md.GetString("extra").String() != "x" {
		return fmt.Errorf("the handler was told %v", res)
	}
	return nil
}

// whoami asks the server who it takes the client for.
func whoami(cli valueclient.Client, want string) error {
	res, err := cli.CallFunction(bg, "whoami", nil)
	if err != nil {
		return err
	}
	if got := res.String(); got != want {
		return fmt.Errorf("whoami = %q, want %q", got, want)
	}
	return nil
}

// deadline has the server say how long it thinks the call may take, and then
// takes longer than a call may.
func deadline(cli valueclient.Client) error {
	ctx, cancel := context.WithTimeout(bg, 3*time.Second)
	res, err := cli.CallFunction(ctx, "deadline", nil)
	cancel()
	if err != nil {
		return fmt.Errorf("deadline: %w", err)
	}
	if left := res.(value.Number).Long(); left <= 0 || left > 3000 {
		return fmt.Errorf("the handler has %d ms, of the 3000 the call may take", left)
	}
	ctx, cancel = context.WithTimeout(bg, 150*time.Millisecond)
	_, err = cli.CallFunction(ctx, "slow", nil)
	cancel()
	if valuerpc.CodeOf(err) != valuerpc.CodeDeadlineExceeded && !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("slow: %v, want the deadline to be exceeded", err)
	}
	if _, err = cli.CallFunction(bg, "greet", nil); err != nil {
		return fmt.Errorf("greet after a call that took too long: %w", err)
	}
	return nil
}

// cancelCall gives a call up while the server is at it.
func cancelCall(cli valueclient.Client) error {
	ctx, cancel := context.WithCancel(bg)
	time.AfterFunc(50*time.Millisecond, cancel)
	_, err := cli.CallFunction(ctx, "slow", nil)
	cancel()
	if valuerpc.CodeOf(err) != valuerpc.CodeCanceled && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("slow: %v, want it to be canceled", err)
	}
	if _, err = cli.CallFunction(bg, "greet", nil); err != nil {
		return fmt.Errorf("greet after a call that was given up: %w", err)
	}
	return nil
}

// resume has the server drop the connection, and goes on as the same client:
// it connects again and shows the next link of its hash chain.
func resume(cli valueclient.Client) error {
	id := cli.ClientId()
	if res, err := cli.CallFunction(bg, "kick", nil); err != nil || res.String() != "kicking" {
		return fmt.Errorf("kick = %v, %v", res, err)
	}
	time.Sleep(200 * time.Millisecond)
	var err error
	for until := time.Now().Add(10 * time.Second); time.Now().Before(until); time.Sleep(50 * time.Millisecond) {
		var res value.Value
		if res, err = cli.CallFunction(bg, "greet", value.Tuple(value.Utf8("again"))); err == nil {
			if got := res.String(); got != "Hello, again!" {
				return fmt.Errorf("greet = %q", got)
			}
			if cli.ClientId() != id {
				return fmt.Errorf("the client came back as another: %d, was %d", cli.ClientId(), id)
			}
			return nil
		}
	}
	return fmt.Errorf("the client did not come back: %w", err)
}
