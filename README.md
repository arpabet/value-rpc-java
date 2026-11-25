# value-rpc-java

**vRPC for Java and Android**: the Java implementation of
[`go.arpabet.com/value-rpc`](https://github.com/arpabet/value-rpc), on top of
[`com.arpabet:value`](https://github.com/arpabet/value-java). It speaks the
wire protocol of the Go implementation, which is the reference: the same
envelopes, byte for byte, and the same conversation.

It is being built; [MIGRATION_PLAN.md](MIGRATION_PLAN.md) has the plan, the
decisions and the wire contract.

## What is there

| | State |
|---|---|
| The protocol: envelopes, codes, hash chain, argument verification, framing, MessagePack and JSON wire codecs | done; checked against fixtures the Go implementation writes |
| Transports: TCP (with SOCKS5), in-memory, a stream of one's own | done |
| Client: calls, timeouts, cancellation, metadata, credentials, interceptors, metrics, reconnect with session resumption | done; checked against the Go server |
| Client: the three kinds of stream, with credit-based flow control | done; checked against the Go server |
| Client: serving the calls and streams a server opens on it; retry with backoff | not yet |
| TLS, WebSocket | not yet |
| Server | not yet |

The core has no dependency but `com.arpabet:value`, is Java 17 bytecode, and
calls nothing above Android 8.0 (API 26), which the build checks.

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

## Building

```sh
./gradlew build
```

JDK 17 or newer. With a checkout of
[value-java](https://github.com/arpabet/value-java) next to this repository,
`com.arpabet:value` is built from it; without one, or with `-PnoComposite`, it
comes from Maven Central.

The tests that run against the Go implementation need a Go toolchain (1.25 or
newer) and are skipped without one. They build `conformance/server`, a real
value-rpc server, and talk to it.

```sh
cd conformance
go run ./golden          # writes the fixtures of value-rpc/src/test/resources/golden again
go run ./server          # the Go server the tests talk to, by hand
```

## Layout

```
value-rpc/       the library: com.arpabet.valuerpc (protocol), .client, .transport
conformance/     Go: the fixture writer and the server the tests talk to
```

## License

[BUSL-1.1](LICENSE), as the Go implementation.
