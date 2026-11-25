# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and the project aims to follow
[Semantic Versioning](https://semver.org/). The modules are released together,
in one version.

## Unreleased

Nothing yet.

## v0.1.0 — 2026-11-25

The first release: the client of a vRPC server, for Android and the JVM, in
three modules. It speaks the wire protocol of
[`go.arpabet.com/value-rpc`](https://github.com/arpabet/value-rpc) v1.7, the
reference implementation, and is tested against v1.7.2 of it.

### Added

- **`com.arpabet:value-rpc`**, the core. It depends on `com.arpabet:value`
  0.3.0 and nothing else, is Java 17 bytecode, and calls nothing above Android
  8.0 (API 26), which the build checks.
  - The protocol: the envelopes, byte for byte those of the Go implementation;
    the status codes; argument verification (`TypeDef`); the hash chain of
    session resumption; framing; the MessagePack wire codec and the JSON one;
    a dialect whose field names can be changed.
  - The client, `VrpcClient`: calls, waiting or as a `CompletableFuture`;
    timeouts, which the server is sent as the deadline of its handler;
    cancellation, also by interrupting the caller; metadata; a credential for
    the server's authenticator; interceptors; metrics; a listener of the
    connection. It connects when it is first used and again when its
    connection is lost, as the same session.
  - The three kinds of stream, `getStream`, `putStream` and `chat`, with the
    credit-based flow control of the protocol: nothing is lost, nothing is
    buffered without bound, and a stream nobody reads holds up no other
    request.
  - Transports: TCP, also through a SOCKS5 proxy; in memory; and any stream of
    bytes an application establishes itself, which is how a client uses TLS in
    this release (the README has the dialer).
- **`com.arpabet:value-rpc-resilience`**, the policies of the Go module
  `resilience` as interceptors of a client, with its defaults and its failures:
  retry with backoff and jitter, circuit breaker, timeout, fallback, bulkhead,
  rate limit. No dependency of its own; loads on Android 8.0.
- **`com.arpabet:value-rpc-quic`**, the transport of the Go module `quic`: a
  dialer for a client and a listener, one QUIC stream for each request, TLS 1.3
  with mutual TLS. It is on Netty's QUIC codec 4.2.21, which is native code: for
  a JVM on Linux, macOS or Windows, and not for Android.

### Not in this release

- The server. The QUIC listener is there for the server to come, and for a
  server of one's own on the transport seam.
- A `tls://` transport of the library's own, and WebSocket.
- A client that serves the calls and streams a server opens on it: such a
  request is answered `NotFound`.
- Calls made again by the client itself after a reconnect: a call that was
  waiting when the connection was lost fails with `UNAVAILABLE`, and the retry
  of the resilience module is what makes it again.

### Compatibility

- Checked against the Go implementation at every build: the envelopes, the
  codes, the hash chain, the argument verification and the JSON reader against
  fixtures it writes, and the client against its server, over TCP, over TLS
  and over QUIC; the Go client against the QUIC listener.
- Where the QUIC transport differs from the Go one, a peer does not have to
  know: [MIGRATION_PLAN.md](MIGRATION_PLAN.md) lists it under Progress.
- A Go peer older than value-rpc v1.7.1 does not notice a QUIC connection that
  this side closed until it next writes; from v1.7.1 on it does.
- A chat over QUIC: the side that opened it keeps its half of the stream after
  it has sent its last, to grant the peer credit for what the peer still
  sends, and ends it when the peer ends the chat. The Go transport does the
  same from value-rpc v1.7.2 on. An older one ends the half with the last
  value, and a chat that it opened and ended first stops after one window of
  credit (4096 values by default). A Java client with a Go server is not
  affected.
