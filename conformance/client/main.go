/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Command client is the Go implementation as a client, for the tests of what
// the Java one serves: it connects to the address it is given, over QUIC or
// over TCP, runs the scenarios it is given, and says how each went,
//
//	OK calls
//	OK sequential 50
//
// or, for the first that fails, what failed, and exits with 1.
//
//	client -quic 127.0.0.1:9000 -ca ca.pem calls sequential:50 concurrent:16x25 big:1048576
//	client -tcp 127.0.0.1:9001 chatend:6000
//
// With -cert and -key it shows a QUIC server a certificate; -timeout and
// -connect-timeout bound a call and connecting.
//
// The server has to have the functions of the test server of the Java
// implementation: greet, echo and fail; and for chatend the chat.echo of the
// Go conformance server.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
	caFile := flag.String("ca", "", "the certificate authority of the QUIC server, PEM")
	certFile := flag.String("cert", "", "a certificate to show the server, PEM")
	keyFile := flag.String("key", "", "the key of the certificate, PEM")
	timeout := flag.Int64("timeout", 10000, "the timeout of a call, in milliseconds")
	connectTimeout := flag.Int64("connect-timeout", 10000, "the timeout of connecting, in milliseconds")
	flag.Parse()
	if (*addr == "" || *caFile == "") && *tcpAddr == "" {
		fmt.Fprintln(os.Stderr, "usage: client (-quic host:port -ca ca.pem [-cert c.pem -key k.pem] | -tcp host:port) scenario...")
		os.Exit(2)
	}

	var cli valueclient.Client
	var err error
	if *tcpAddr != "" {
		cli = valueclient.NewClient(*tcpAddr, "")
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
