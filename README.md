# value-rpc-java

**vRPC for Java and Android**: the Java implementation of
[`go.arpabet.com/value-rpc`](https://github.com/arpabet/value-rpc), on top of
[`com.arpabet:value`](https://github.com/arpabet/value-java). It speaks the
wire protocol of the Go implementation, which is the reference: the same
envelopes, byte for byte, and the same conversation.

The first release, 0.1.0, is the client. The server, and what else is to
come, is in [MIGRATION_PLAN.md](MIGRATION_PLAN.md), with the decisions and the
wire contract; [CHANGELOG.md](CHANGELOG.md) has what each release brought.

## Using it

On Maven Central, in one version:

```kotlin
dependencies {
    implementation("com.arpabet:value-rpc:0.1.0")              // the client: Android 8.0 (API 26) and up, JVM 17 and up
    implementation("com.arpabet:value-rpc-resilience:0.1.0")   // retry, circuit breaker and the other policies
    implementation("com.arpabet:value-rpc-quic:0.1.0")         // the QUIC transport: a JVM on Linux, macOS or Windows
}
```

`value-rpc` brings `com.arpabet:value` and nothing else, and the resilience
module nothing of its own. They are Java 17 bytecode and call nothing above
Android 8.0 (API 26), which the build checks. `value-rpc-quic` brings Netty's
QUIC codec, which is native code, for a JVM and not for Android.

It speaks the protocol of value-rpc v1.7, and every release is tested against
the Go implementation: 0.1.0 against v1.7.1, the Java client with the Go
server over TCP, TLS and QUIC, and the Go client with the Java QUIC listener.

## What is there

| | State |
|---|---|
| The protocol: envelopes, codes, hash chain, argument verification, framing, MessagePack and JSON wire codecs | done; checked against fixtures the Go implementation writes |
| Transports: TCP (with SOCKS5), in-memory, a stream of one's own (which is how to use [TLS](#tls) for now) | done |
| Client: calls, timeouts, cancellation, metadata, credentials, interceptors, metrics, reconnect with session resumption | done; checked against the Go server |
| Client: the three kinds of stream, with credit-based flow control | done; checked against the Go server |
| Resilience: retry, circuit breaker, timeout, fallback, bulkhead, rate limit (`value-rpc-resilience`) | done; the tests of the Go module, and checked against the Go server |
| QUIC transport: a dialer for a client, a listener for a server (`value-rpc-quic`) | done; checked against the Go server and the Go client. For a JVM, not for Android |
| Client: serving the calls and streams a server opens on it; replay of calls after a reconnect | not yet |
| A `tls://` transport of the library's own; WebSocket | not yet |
| Server | not yet |

## A call

```java
VrpcClient client = VrpcClient.builder()
        .address("tcp://example.com:9000")
        .credential(Value.utf8("secret"))             // for the server's authenticator
        .timeoutMillis(5000)
        .build();

Value greeting = client.callFunction("greet", Value.list(Value.utf8("world")));   // "Hello, world!"

try {
    client.callFunction("user.get", Value.list(Value.ofLong(42)), CallOptions.timeoutMillis(250));
} catch (VrpcException e) {
    if (e.code() == Code.NOT_FOUND) { /* ... */ }      // the code the server sent
}

CompletableFuture<Value> later = client.callFunctionAsync("report", null, CallOptions.DEFAULT);
later.cancel(true);                                    // the server is told

client.close();
```

A client connects when it is first used and again when its connection is lost;
it then shows the server the next link of its hash chain, and the server takes
it for the same session.

## Streams

```java
// from the server: taking a value is what lets the server send another
try (ValueStream events = client.getStream("events.tail", Value.list(Value.utf8("orders")), CallOptions.DEFAULT)) {
    for (Value event : events) {
        handle(event);
    }
}                                                      // closing early cancels the stream

// to the server: send() waits while the server has no room
try (ValueSink lines = client.putStream("logs.upload", null, CallOptions.DEFAULT)) {
    lines.send(Value.utf8("first line"));
}                                                      // closing ends the stream

// both at once
try (Chat chat = client.chat("support.chat", null, CallOptions.DEFAULT)) {
    chat.outgoing().send(Value.utf8("hello"));
    chat.outgoing().close();
    for (Value answer : chat.incoming()) {
        show(answer);
    }
}
```

A sender sends only what its receiver has granted it credit for, so nothing is
lost and nothing is buffered without bound, and a stream nobody reads holds up
no other request on the connection.

## TLS

The library has no `tls://` transport yet. Until it has, a client takes a TLS
socket the application makes, as a stream of its own: the protocol is the same
on it, and a Go server on `valueserver.NewTLSServer` is at the other end. The
dialer is asked for a connection whenever the client connects, also after a
connection was lost.

```java
SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();   // or one of a context of one's own

Dialer tls = Transports.dialer(timeoutMillis -> {
    int timeout = (int) Math.max(0, Math.min(timeoutMillis, Integer.MAX_VALUE));
    Socket plain = new Socket();
    try {
        plain.connect(new InetSocketAddress(host, port), timeout);
        plain.setTcpNoDelay(true);
        plain.setSoTimeout(timeout);                               // bounds the handshake
        SSLSocket socket = (SSLSocket) factory.createSocket(plain, host, port, true);
        SSLParameters parameters = socket.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");    // the certificate has to be for the host
        socket.setSSLParameters(parameters);
        socket.startHandshake();
        socket.setSoTimeout(0);                                    // a connection may be silent
        return new Transports.Stream(socket.getInputStream(), socket.getOutputStream(), socket, host + ":" + port);
    } catch (IOException | RuntimeException e) {
        plain.close();
        throw e;
    }
}, TransportOptions.DEFAULT);

VrpcClient client = VrpcClient.builder().dialer(tls).build();
```

Without the line that sets the endpoint identification, a socket checks that
the certificate is one it trusts and not that it is the server's. This dialer
is in the tests as it is here, against the TLS listener of the Go server.

## Resilience

The policies of the Go module `resilience`, with its defaults, as interceptors
of a client. They are a module of their own, `com.arpabet:value-rpc-resilience`.

```java
VrpcClient client = VrpcClient.builder()
        .address("tcp://example.com:9000")
        .interceptors(
                Resilience.rateLimit(100, 20),                 // outermost
                Resilience.circuitBreaker(),
                Resilience.retry().withMaxAttempts(3),
                Resilience.timeoutMillis(2000))                // innermost: each attempt
        .build();
```

The first interceptor is the outermost, so here the breaker sees whole
operations, after their retries, and the timeout bounds each attempt. A retry
tries again only after `UNAVAILABLE` and `RESOURCE_EXHAUSTED`, which say the
server did not process the request; `withRetryable` widens that for methods
that are idempotent. `Resilience.fallback(...)` answers in place of a call
that failed, and `Resilience.bulkhead(n)` lets no more than `n` calls through
at once.

Where the Go policies read a `context.Context`, these read the options of the
call and its thread: a timeout given with the options is for the whole call,
its attempts and the waits between them, and an interrupt stops a policy that
waits.

## QUIC

The transport of the Go module `quic`, and a peer of it:
`com.arpabet:value-rpc-quic`. Each request has a QUIC stream of its own.

```java
// a client; the certificate of the server is checked against the system's authorities
VrpcClient client = QuicTransport.client("example.com:9000", QuicOptions.DEFAULT)
        .timeoutMillis(5000)
        .build();

// with authorities of one's own, and a certificate to show the server
QuicOptions options = QuicOptions.DEFAULT.withSslContext(QuicTransport.clientTls()
        .trustManager(caCertificate)
        .keyManager(clientKey, null, clientCertificate)
        .build());

// a listener, for a server
Listener listener = QuicTransport.listener("0.0.0.0:9000", QuicOptions.DEFAULT
        .withSslContext(QuicTransport.serverTls(keyFile, null, certificateChainFile).build()));
```

QUIC is always encrypted, so a listener needs a certificate. The module brings
Netty and the native library for each platform Netty builds one for; a program
that ships to one platform may exclude the others. There is no server in this
library yet, so the listener serves the tests, and a server of one's own.

## Building

```sh
./gradlew build                  # compile, test, check the Android API level
./gradlew publishToMavenLocal    # the three artifacts, in the version of build.gradle.kts
```

JDK 17 or newer. With a checkout of
[value-java](https://github.com/arpabet/value-java) next to this repository,
`com.arpabet:value` is built from it; without one, or with `-PnoComposite`, it
comes from Maven Central, as it does for a release.

The tests that run against the Go implementation need a Go toolchain (1.25 or
newer) and are skipped without one. They build `conformance/server`, a real
value-rpc server on TCP, TLS, WebSocket and QUIC, and `conformance/client`,
the Go client over QUIC, and talk to them.

```sh
cd conformance
go run ./golden          # writes the fixtures of value-rpc/src/test/resources/golden again
go run ./server          # the Go server the tests talk to, by hand
go run ./client -quic 127.0.0.1:9000 -ca ca.pem calls    # the Go client, against a QUIC listener
```

Two workflows run on GitHub. `build.yaml` builds and tests every push on JDK
17 and 21, checks that the committed fixtures are what the generator writes,
and runs the tests against a larger set written on the spot. `release.yaml`
publishes the three modules to Maven Central, together: push the tag `v` + the
`version` of `build.gradle.kts`, or run it by hand to stage a release in the
Central Portal without publishing it. Between releases the version ends in
`-SNAPSHOT`, which the workflow refuses to release. [RELEASE.md](RELEASE.md)
has the steps and what has to be set up once. The jars are reproducible: the
same sources give the same bytes.

## Layout

```
value-rpc/              the library: com.arpabet.valuerpc (protocol), .client, .transport
value-rpc-resilience/   com.arpabet.valuerpc.resilience: the policies
value-rpc-quic/         com.arpabet.valuerpc.quic: the QUIC transport, on Netty
buildSrc/               what the builds of the modules have in common
conformance/            Go: the fixture writer, and the server and the client the tests talk to
```

## License

[BUSL-1.1](LICENSE), as the Go implementation.
