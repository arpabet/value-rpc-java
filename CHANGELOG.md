# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and the project aims to follow
[Semantic Versioning](https://semver.org/). The modules are released together,
in one version.

## Unreleased

The server, ahead of its place in the plan, and a WebSocket listener for it,
so that a browser reaches a Java server: the client there is `@arpabet/vrpc`
(value-rpc-js). The version between releases is 0.2.0-SNAPSHOT.

### Added

- **The server**, `com.arpabet.valuerpc.server.VrpcServer`, in the core: it
  depends on nothing new and passes the check of Android 8.0.
  - The four patterns, served: `addFunction`, `addOutgoingStream`,
    `addIncomingStream` and `addChat` (`Registrar`), with handlers that stay
    for as long as their request lasts (`UnaryHandler`,
    `OutgoingStreamHandler`, `IncomingStreamHandler`, `ChatHandler`), the
    credit-based flow control of the protocol in both directions, and the
    arguments and results checked against a `TypeDef`.
  - `RequestContext`: the principal, the metadata, the time the caller still
    waits, and whether it still waits. A request that the client cancels,
    whose time has passed, or whose connection is lost has its handler
    interrupted.
  - The handshake as the Go server answers it: silence for what is not one,
    a coded refusal for what is; an `Authenticator` for the credential and a
    `ConnectAuthorizer` for the connection.
  - Sessions: a client that reconnects is the same session when it is the
    same principal and shows the next link of its hash chain; the session of
    one that does not come back is dropped after two minutes.
  - Limits: connections, requests of a client at once (one too many is
    refused, not queued), streams of a client at once.
  - Closing writes the messages that are queued, the answers of finished
    calls above all, before it closes the connections.
  - `ServerListener` for what happens to the connections; `Metrics` as for a
    client.
- **A WebSocket listener**, in the core, on plain sockets:
  `ws://host:port/path` as the address of a server, or
  `Transports.webSocketListener` with `WebSocketOptions`. MessagePack in
  binary frames and JSON in text frames, agreed on by subprotocol as the Go
  listener agrees on them; the origins a browser may connect from; a ping for
  a silent connection; the closing handshake. No TLS and no compression.
- `conformance/client`, the Go client as a program, also over TCP with every
  scenario and over WebSocket (`-ws`, `-codec`), with scenarios for streams,
  metadata, the credential, deadlines, cancellation and resumption.
- `ConformanceServer`, the Java twin of the Go conformance server, as a test
  fixture and as a program: `./gradlew :value-rpc:conformanceServer`.

### Not yet

- A server that calls its clients, and a client that serves such calls.
- A WebSocket client; `wss://`; a WebSocket listener on Netty.

### Compatibility

- Checked at every build, where there is a Go toolchain: the Go client
  against this server over TCP and over WebSocket in both codecs, with calls,
  the three kinds of stream, deadlines, cancellation, the credential, and a
  dropped connection that is resumed.
- Checked by hand against the conformance suite of value-rpc-js 0.4.0: 15 of
  its 19 scenarios pass in each codec, and the 4 that do not are the ones in
  which the server calls its client.
- Where the server differs from the Go one, a client does not have to know:
  [MIGRATION_PLAN.md](MIGRATION_PLAN.md) lists it under Progress.

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
