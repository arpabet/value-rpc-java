# value-rpc-java — migration plan

Port of `go.arpabet.com/value-rpc` (Go) to Java on top of `com.arpabet:value`
(value-java), wire-compatible with the Go implementation and with
`@arpabet/vrpc` (value-rpc-js).

Status: the protocol core, the client and its transports over TCP and in
memory, the resilience policies and the QUIC transport are written and pass
against the Go implementation; the rest is as planned below. Written
2026-11-24; revised the same day after the decisions and after the upstream
fixes of §6 were released, and on 2026-11-25 when resilience and QUIC were
added ahead of their place in the plan, on request.

## Progress

Committed: the first implementation (`3454d36`), the two modules of
2026-11-25 (`f270558`), what prepares the release of 0.1.0 (`98e2540`, the
last row of the table) and the move of the conformance module to value-rpc
v1.7.2 (`9153a24`). In the working tree, for review: what the build on GitHub
showed (below the table), and what is said here, in the README, in the
changelog and in `StreamRules` of the Go implementation as of v1.7.2.

| Phase | State | What is there |
|---|---|---|
| 0 Scaffolding | done; CI is there and has not passed yet (below the table) | Gradle build (core module `value-rpc`, Java 17 bytecode, Android API 26 check, composite build with `../value-java` until 0.3.0 is on Maven Central), BUSL-1.1 headers, `LICENSE` copied from `value-rpc`; `conformance/` Go module pinned to a release of value-rpc (v1.7.2 now) with the fixture writer (`go run ./golden`) |
| 1 Protocol core | done | `MessageType`, `Dialect` (envelopes and readers), `Code`, `VrpcException`, `TypeDef`, `HashChain`, `CreditGate`, `StreamBuffer`, `Metrics`, `Frames`, `WireCodec` (msgpack, JSON), `WireJson`. 36 tests; every envelope, the hash chain, 540 argument checks and 467 JSON inputs (9,167 in a larger run) agree with Go |
| 2 Transports | TCP and in-memory done; TLS not yet | `MsgConn` (push-based), `Dialer`, `Listener`, `StreamConn` (framing, write guard), TCP with SOCKS5 and `TCP_NODELAY`, `mem://`, a stream of one's own (`Transports.dialer`, `single`) |
| 3a Client: calls | done | `VrpcClient` and its builder, lazy connect, handshake, calls (waiting and as a future), timeout and deadline, cancel (also by interrupt), metadata, credential, interceptors, metrics, listener, close. 18 scenarios against the Go server over TCP, 14 more against a server of the test's own over `mem://` and TCP |
| 3c Client: reconnect | in part | a lost connection fails what waited, at once, and is made again as the same session with the next link of the hash chain (checked against the Go server). Not yet: retry with backoff, replay of idempotent calls |
| 3b Client: streams | done | `getStream` / `putStream` / `chat` as `ValueStream`, `ValueSink`, `Chat`; credit granted as the application takes values, sending gated by the server's credit; early close cancels; a refused stream fails to open; a lost connection fails its streams. 12 scenarios against the Go server, among them 100,000 values from it, 50,000 to it, 20,000 both ways in a chat, and a window of 3. No test of its own without Go yet: that comes with the Java server |
| 4 Client as a peer | next | serving the calls and streams the server opens on the client; today such a request is answered `NotFound` |
| 10 Resilience (ahead of its place) | done | module `value-rpc-resilience`, package `com.arpabet.valuerpc.resilience`: `Resilience.retry()`, `circuitBreaker()`, `timeoutMillis`, `fallback`, `bulkhead`, `rateLimit`, with the defaults and the failures of the Go module. No dependency of its own; passes the API 26 check. 65 tests: the 14 of the Go module one for one, what a client adds, and 7 against the Go server |
| QUIC (was left out; added on request) | done, as a transport | module `value-rpc-quic`, package `com.arpabet.valuerpc.quic`, on Netty's QUIC codec 4.2.21: `QuicTransport.client`, `dialer` and `listener`, `QuicOptions`. One QUIC stream for each request, as `quic/transport.go`; ALPN `vrpc`; mutual TLS. 66 tests: the Java client against the Go QUIC server (the tests of `quic/transport_test.go`), the Go client (`conformance/client`) against the Java listener, both ends in Java. JVM only: the codec is native code. The listener waits for the server engine of phase 7 to be of use beyond the tests |
| Release 0.1.0 (prepared 2026-11-25) | tagged once and not released (below the table); to tag again when the build on GitHub is green | version `0.1.0`; the three modules published together (`vrpc.publish` in `buildSrc`, `centralBundle` at the root, `.github/workflows/release.yaml` on a tag `v*`, with the four secrets of value-java); `CHANGELOG.md`, `RELEASE.md`; the licence in every jar. Tested against value-rpc **v1.7.2** (against v1.7.1 when it was prepared) and against `com.arpabet:value` 0.3.0 from Maven Central; signing and the bundle rehearsed with a throwaway key; the jars reproducible; the staged artifacts used from a project of their own against the Go server. It is the client only, earlier than phase 6 had it: without a `tls://` transport (TLS is by a socket of the application's own, which is tested and in the README), WebSocket, and the client as a peer |
| 5 and later | see below | |

How to run: `./gradlew build` (226 tests in three modules). The ones against the Go implementation build
`conformance/server` and `conformance/client` and are skipped without a Go toolchain.
`cd conformance && go run ./golden` writes the fixtures again.

**The build on GitHub, which is the only one on Linux, had not passed once.** That was seen on 2026-11-25,
when the tag `v0.1.0` was pushed on `9153a24`: the Release workflow stopped at its tests, before it signed or
uploaded anything. Nothing is on Maven Central, and the tag is to be moved to a commit whose build is green.
What it showed, the first two mended in the working tree:

- **`GoClientInteropTest` compared all that the Go client wrote, its standard error too.** On Linux quic-go
  logs there that the receive buffer of its UDP socket is smaller than it would like (`failed to sufficiently
  increase receive buffer size`), which it does not on a Mac: six tests failed, in every run. The test now
  looks at the standard output, where the client says how its scenarios went, and leaves the log to the log
  of the test, as it is with the Go server. Reproduced here by building the client with that line written to
  its standard error (`GOFLAGS=-overlay=...`): the same six failures before, none after.
- **A stream to the server said `Canceled: stream N is closed` when it was sent on while its connection was
  being lost**, where the answer is `Unavailable` (`GoStreamInteropTest.aLostConnectionFailsItsStreams`, one
  run in six on GitHub, never here). The loss of a connection fails the requests on it one after the other,
  and took each out of the pending ones before it left with it why; a sender that came in between found no
  connection, or found its stream taken out, and no reason yet. The reason is now left with the request in
  the step that takes it out, and a sender that finds no connection fails its stream itself
  (`ClientImpl.retireFailed`, `StreamCall.send`). `ClientTest.aStreamOfALostConnectionSaysSoFromTheMomentTheConnectionIsLost`
  holds that moment open, over `mem://` and over TCP, and failed before with the answer GitHub had.
- **One job could not download the Kotlin plugin of `buildSrc`** (`429 Too Many Requests` from Maven
  Central). Nothing in this build causes it; running the job again is the remedy.

Not yet run on Linux, since the build stopped before them: the tests of `value-rpc-resilience`, the bundle,
the upload and the GitHub Release.

What the two modules needed elsewhere:

- The core: a call made with `callFunctionAsync` behind interceptors could not be cancelled; cancelling its
  future now interrupts the thread that runs the interceptors, which cancels the call under way and stops a
  policy that waits. `quic://` in an address of the core names the module, as in Go.
- The build: what the modules have in common is in `buildSrc` (`vrpc.library`, `vrpc.android-api`); the
  servers of the tests (`FakeServer`, `GoServer`) are test fixtures of `value-rpc`, for every module.
- `conformance/`: the Go server also listens on QUIC (a listener as it comes, one that allows eight requests
  at once, one with mutual TLS; a certificate authority made when it starts) and has a `flaky` function;
  `conformance/client` is the Go client over QUIC as a program.

Differences from the Go implementation found so far. On input no peer sends:

- A JSON string with the extension prefix and a type the format defines (1, 2, -1): Go keeps it an opaque
  extension whatever its payload; here it is the kind the type names, or text when the payload is not one of
  that kind. With a canonical payload the bytes are the same.

In the QUIC transport, none of which a peer has to know of:

- **When a half of a stream is ended.** Every half Go ends, this ends too. It also ends its half after any
  message that ends the request for both sides, a failure or a cancellation, whoever sent it and whatever the
  request: by Go's rules the half stays open in some of those cases (a stream to the peer that the peer
  fails; a request the peer cancels), and the stream then lasts as long as the connection. The first was
  measured with Go on both ends, with the transport of v1.7.2: on a listener that allows eight streams, the
  ninth stream to the server in a row that the server refuses does not get through. The rest is read from
  `finOnWrite` and `finOnRead`. `StreamRules` has both sets of rules, and `StreamRulesTest` says which is
  which. Up to value-rpc v1.7.1 there was a second difference, which v1.7.2 took away by doing the same (the
  third finding below): **the side that opened a chat does not end its half with the end of what it
  streams**, but when the peer ends the chat, since until then it has to grant the peer credit.
- **A message for a request that is over** (a credit that was on its way) is dropped, and a stream is opened
  only by the message that opens a request. Go does the same from v1.7.2 on, and it is no difference any
  more; before, it opened a new stream for such a message when the id was one it owned, and failed the write
  otherwise.
- **A frame that is too long, or is not a message**, ends the connection, as on TCP. Go stops reading that one
  stream and keeps the connection, on which the caller then waits out its timeout.
- **Keeping a silent connection.** Go sends a PING every 15 s. Netty has no way to send one, so this sends a
  QUIC datagram, to a peer that takes datagrams, which is another one of this implementation. A Go peer does
  not take them and keeps the connection with its own PINGs (checked: 14 s of silence against an idle timeout
  of 6 s). quic-go takes an idle timeout of less than 5 s as 5 s, and its PINGs would come too late for a
  shorter one here.
- **A request that is one too many** for the streams the peer allows waits for one, as in Go, but no longer
  than the write timeout of the transport; then the connection is given up and made again. Go waits for good.

Three things found on the way that are not differences but should be known:

- **In the Go QUIC transport, up to v1.7.0, a connection the peer closed was not noticed by the reader.**
  `quicMsgConn.ReadMessage` waits on its queue, on its own `done` and on the read deadline, and not on the
  QUIC connection. A Go client without a certificate against the Go server that wants one takes the whole
  dial timeout, 30 s, and fails with `handshake: i/o timeout`, where this client is refused in a tenth of a
  second; reproduced with Go on both ends (`conformance/client` against the `quic-mtls` listener of
  `conformance/server`). After the handshake the response loop reads without a deadline, so a client that
  only waits would not learn that its connection is gone until it writes. **Fixed on 2026-11-25 in
  value-rpc, on request, and released as v1.7.1** (`quic/transport.go`, tests in
  `quic/closed_conn_test.go`): `ReadMessage` now ends when the connection has ended and the stream readers
  have handed over what they had read. The refused client fails in milliseconds with the reason, and an
  idle client reconnects. The conformance module here has pinned a release with it since (v1.7.2 now), and
  the test of a Go client that is refused asserts that it is told at once.
- **Netty's QUIC codec (quiche underneath) can lose the end of a half of a stream that is sent by itself.**
  What was seen, in the packet logs of both ends: of 41 ends that were written to the codec, and accepted by
  it, 3 were sent; the 38 others were written while the codec was working through packets that had arrived
  together, and the peer's own end was read before the codec sent anything. What is taken from that, and
  from what made it go away: the library forgets a stream once the peer's end was read and every byte this
  side sent was acknowledged, and an end with no bytes of its own does not hold it back. `QuicConn` therefore
  sends the end with the last message wherever there is one, and otherwise writes it only when the peer's
  own end was read, and between two rounds of reading; with that, 41 of 41 arrive, run after run. What may
  be left is narrow, and was not seen: a peer that ends its half more than a quarter of a second after its
  last message, together with the loss of the packet that carried this side's end, would leave that one
  stream open at the peer until the connection ends. Worth reporting to Netty.
- **In the Go QUIC transport, up to v1.7.1, a chat stopped for good after one window of credit once the
  side that opened it had sent its last.** `finOnWrite` ended the half of whoever made the request with its
  `StreamEnd`, whatever the request (its `reqType` was not looked at). In a chat the peer still streams
  after that, and the side that ended has to grant it credit: with its half ended, `WriteMessage` drops the
  credit ("the write half may already be FIN'd ... swallow it"), and the peer stops after the values it had
  credit for. Measured with Go on both ends: a client that sends 6000 values on `chat.echo`, ends, and then
  reads got all 6000 back over TCP and 4096 over QUIC, the default window, and no more
  (`conformance/client -tcp <addr> chatend:6000` against `-quic <addr> -ca ca.pem chatend:6000`). Here it
  showed as a chat of 10,000 values that hung in two of eight runs on a machine kept busy, since it takes
  a reader that falls behind. This side had the rule that mends it from the start (above), and with that a
  Java client gets all of it from a Go server, 10,000 of 10,000, whether the server is v1.7.1 or v1.7.2.
  What it could not mend: a Go client that ended its side of a chat first dropped its own credit, whatever
  the server. **Fixed on 2026-11-25 in value-rpc, on request, and released as v1.7.2** (`quic/transport.go`,
  tests in `quic/chat_test.go`, `quic/late_frame_test.go` and `quic/transport_internal_test.go`), by the same
  rule: `finOnWrite` does not end the half for a `StreamEnd` of a chat the side opened, and `finOnRead` ends
  it when the message that ends the chat is read, the `StreamEnd` of the peer or its `ErrorResponse`. The
  fix needed a second one, and has it: a message for a stream that was freed is dropped, where a new stream
  was opened for it or the write failed. With v1.7.2 on both ends the same command gets 6000 of 6000 over
  QUIC, and 12,000 of 12,000. It is the side that opens the chat that has to have the fix: a v1.7.2 client
  gets all of it from a v1.7.1 server, and a v1.7.1 client stops at 4096 with a v1.7.2 server too. The
  conformance module here pins v1.7.2.

## 1. Summary

- **Feasible, no blockers.** value-java 0.3.0 mirrors `value` v1.7.0, the
  version value-rpc v1.7.0 pins, and has everything the wire needs: canonical
  MessagePack, `MapValue.get` with Go's "absent is Null" semantics, a
  `jsonify()` that is JSON for every value, `StringValue.parse`, decode
  `Limits`.
- **Scope is about 8,700 lines of Go** (non-test) in three packages plus the
  `resilience` module, and 213 test functions. QUIC (560 lines, its own Go
  module) was to be left out; it was asked for on 2026-11-25 and is a module
  of its own here too, on Netty.
- **There is no normative protocol spec.** `WEB.md` §5 calls for a
  `PROTOCOL.md` and golden frames; neither exists. This plan extracts the
  contract from the code (Appendix A) and makes golden fixtures plus live
  interop the definition of "compatible". **The Go implementation is the
  reference**: where another implementation differs, it is the one that
  changes.
- **Shape, as decided:** Android first, with Android 8.0 (API 26) as the floor.
  Java 17 bytecode; a core with no dependency beyond `value`, on plain sockets,
  so that it loads on Android; a separate Netty module for what a JVM server
  needs (the WebSocket server above all). One symmetric peer engine shared by
  client and server, and a blocking-first API in which a stream is a pull
  iterator or a push sink, so no thread per stream. Licence BUSL-1.1.
- **The client comes first**, since an Android app is a client (and a peer the
  server can call back). The first cross-language proof (Java client calling
  the Go server, unary over TCP) lands at the end of phase 3a; the server
  engine and the Netty module follow.

## 2. Sources researched

| Repo (under `/Users/ashvid/web/arpabet`) | Version | Role | Size |
|---|---|---|---|
| `value-rpc` | v1.7.0 | the original and the reference; porting baseline | `valuerpc` ≈ 3,100, `valueclient` ≈ 2,400, `valueserver` ≈ 2,700, `resilience` ≈ 420, `quic` 560 lines |
| `value` | v1.7.0 | Go serialization library | reference for JSON rendering and limits |
| `value-java` | 0.3.0 | Java serialization library, the dependency | Java 17 bytecode, Android API 24+, no dependencies |
| `value-rpc-js` | 0.4.0 | TypeScript **client** (WebSocket only), second implementation of the protocol | `packages/vrpc` ≈ 3,400 lines; Go conformance server + 19 scenarios × 2 codecs |
| `value-rpc-java` | initial commit | target | empty |

The research was done on value-rpc v1.6.4, value v1.6.1, value-java 0.1.0 and
value-rpc-js 0.3.0. The versions in the table are the ones released on
2026-11-24 with the fixes of §6; nothing else changed in them.
`value-rpc-1` (v1.5.2) and `value-rpc-2` (v1.5.3) are older checkouts and were
ignored. Local toolchain: JDK 17 (Temurin) only, Go 1.27.1, Node 24, Gradle
wrapper 8.14.3 in the cache.

## 3. What "fully compatible" means

The port is done when all four hold:

1. **Bytes.** Every envelope the Java side builds is byte-identical to the Go
   one (MessagePack), checked against fixtures the Go implementation writes.
2. **Live interop.** Every scenario passes in every pairing:
   Java client → Go server, Go client → Java server, JS client → Java server,
   Java client → Java server; over every shared transport.
3. **Behaviour.** The Go test suite, ported, is green (limits, cancellation,
   flow control, reconnect, resumption, shutdown).
4. **Defaults.** Same default values as Go (Appendix D), so a mixed fleet
   behaves the same on both sides.

## 4. Scope

| In | Deferred (seam kept, not built now) | Out |
|---|---|---|
| Wire protocol, all 4 patterns, both call directions (peer symmetry) | QUIC on Android: Netty's codec is native and has no Android build; a pure-Java QUIC would be another module | Obfuscation transports (`obfs`) |
| Credit-based flow control, cancellation, timeouts/SLA | Mounting the server in Jakarta WebSocket / Spring | Code generation of any kind |
| Handshake, authenticator, connect-authorizer, principal | permessage-deflate on WebSocket | `java.net.http`: no Android version has it |
| Hash-chain session resumption, reconnect policy, unary replay | | |
| Core transports, on plain sockets, for Android and the JVM: TCP (+SOCKS5), TLS/mTLS, WebSocket **client**, in-memory, bring-your-own stream | | |
| Netty module, JVM only: WebSocket **server**, and TCP/TLS/Unix listeners on event loops | | |
| Unix sockets on the JVM (JDK 16+) | | |
| Wire codecs: MessagePack everywhere, JSON on WebSocket (optional to use, compatible when used) | | |
| TypeDef/Verify, typed `Codec<T>` helpers, interceptors, metrics, metadata, dialect | | |
| `resilience` (retry, breaker, timeout, fallback, bulkhead, rate limit) | | |
| QUIC transport on a JVM, one stream for each request, in a module of its own on Netty | | |
| Examples, README, `PROTOCOL.md`, CI, Maven Central release | | |

## 5. Design decisions (Go → Java)

### 5.1 Runtime baseline

**Android first, Android 8.0 (API 26) as the floor**, and the JVM from 17.
Android 8.0 came out in August 2017, so the floor covers phones sold since
late 2017 and the older flagships that were updated to it: 98 to 99 percent of
the devices in use by the distribution tables of October 2026.

Android does not run a JDK. A device has the Java library of its Android
version, and API 26 is at about Java 8: lambdas, streams, `CompletableFuture`,
`java.time`. So the core is Java 17 **bytecode** (the language level Android's
build tools take, and what value-java is) that calls **nothing above API 26**,
which the build enforces with animal-sniffer as value-java's does for API 24.
For the core that rules out:

| Not in the core | Why | Instead |
|---|---|---|
| `java.net.http` (JDK 11) | no Android version has it | the WebSocket client is written in the library, over `Socket`/`SSLSocket` |
| virtual threads (JDK 21) | not on Android | threads come from an injectable executor; a client has one connection and needs few |
| `java.util.concurrent.Flow` | API 30 | the blocking stream and sink of §5.5 |
| Unix-socket channels, `jdk.net` socket options | JDK only | in classes of their own, loaded only on a JVM |
| any third-party library | size and conflicts in an app | Netty stays in its own module (§5.2) |

`java.time` is there from API 26 and may appear in the API. The engine is
written in blocking style and takes its threads from injectable factories, so
a JVM application on JDK 21+ can still pass
`Executors.newVirtualThreadPerTaskExecutor()`. Locks are
`ReentrantLock`/`Condition`, not `synchronized`, which pins a virtual thread
on JDK 21 to 23. JDK 17 is the only JDK installed here.

### 5.2 Artifacts, packages, build

```
value-rpc-java/
  settings.gradle.kts, build.gradle.kts, gradle/   copied from value-java, adapted
  value-rpc/                 → com.arpabet:value-rpc          (depends on com.arpabet:value only; loads on Android 8)
    com.arpabet.valuerpc             protocol core   (Go: valuerpc)
    com.arpabet.valuerpc.client      client          (Go: valueclient)
    com.arpabet.valuerpc.server      server engine   (Go: valueserver), over any transport
    com.arpabet.valuerpc.transport   TCP, TLS, WebSocket client, in-memory, bring-your-own
  value-rpc-netty/           → com.arpabet:value-rpc-netty    (depends on Netty; JVM servers)
    com.arpabet.valuerpc.netty       WebSocket server; TCP, TLS and Unix listeners on event loops
  value-rpc-resilience/      → com.arpabet:value-rpc-resilience  (Go: resilience module; loads on Android 8)
    com.arpabet.valuerpc.resilience  the policies, as interceptors of a client
  value-rpc-quic/            → com.arpabet:value-rpc-quic     (Go: quic module; depends on Netty's QUIC codec; JVM only)
    com.arpabet.valuerpc.quic        the dialer and the listener
  buildSrc/                  what the builds of the modules have in common
  examples/                  not published; an Android client among them
  conformance/               Go module: golden/ (fixture writer), server/, client/ (scenario driver)
  PROTOCOL.md  README.md  CHANGELOG.md
```

Every file carries the BUSL-1.1 header of the Go framework. Build conventions
are taken from value-java otherwise unchanged: Gradle Kotlin DSL,
`options.release = 17`, `-Xlint:all -Werror`, JUnit 5, animal-sniffer (here
with the API 26 signature, on the core), reproducible jars,
`Automatic-Module-Name`, R8 rules in the jar, signing and the Central Portal
bundle task, CI on JDK 17 and 21 with a separate conformance job that has Go.

Netty is in a module of its own because an Android app should not carry it:
what an app needs is the client, and the client needs only sockets. A JVM
server takes `value-rpc-netty` and gets the WebSocket server (which the JDK
does not have) with its codecs as handlers in a pipeline of your own, and
listeners that do not cost a thread per connection.

### 5.3 Architecture: one symmetric engine

The Go code implements each role twice: the initiator as `rpcRequestCtx`
(client) and `clientStream` + `pendingCalls` (server), the responder as
`servingRequest` (server) and `clientServingRequest` (client); the comments
say "mirrors" throughout. The Java port should have **one** `Initiator` and
**one** `Responder`, parameterized by the sign of the request ids and by a
policy object, and used by both ends. Client and server then differ only in
what is theirs: dial / reconnect / handshake-initiate on one side, accept /
session registry / handshake-accept on the other.

The two copies in Go are not identical. The differences that are visible on
the wire or to a caller are listed in Appendix A.9 and become policy
parameters; the port must keep them.

### 5.4 Concurrency mapping

| Go | Java |
|---|---|
| reader goroutine per connection | reader thread per connection, blocking `readMessage()`; never runs user code |
| `requestLoop` / `sender` goroutine + bounded channel, coalescing ≤ 64 frames | writer thread per client connection / per server session, `ArrayBlockingQueue` + `drainTo(batch, 64)`, one `write` per batch |
| handler goroutine per request | task on the handler `Executor`; rejected (not queued) over the concurrency limit, as in Go |
| `StreamPump` goroutine per receiving stream | **no thread**: a bounded per-stream buffer; the reader does a non-blocking `offer` (overflow fails the stream with `ResourceExhausted`), the consumer's own `take()` replenishes credit |
| `streamOut` / `outgoingStreamer` goroutine per sending stream | **no thread**: `ValueSink.send()` blocks the caller on the `CreditGate` |
| `time.Timer` per call, `time.AfterFunc` | one shared daemon `ScheduledExecutorService` per client/server (call timeouts, handshake deadline, reconnect backoff, session reap) |
| `context.Context` cancellation | `RequestContext` flag + callbacks, and interruption of the handler thread |
| `sync.Map`, `atomic.*`, `sync.Once` | `ConcurrentHashMap`, `Atomic*`, CAS flags |
| write deadline per write | blocking `OutputStream.write` has none: a watchdog task closes the connection when a write overruns |
| read deadline (handshake timeout) | `Socket.setSoTimeout` where it exists, otherwise the same watchdog |

Threads in use on plain sockets: two per connection, one per running handler,
none extra per stream. That is all an Android client ever has, one connection.

The transport seam is **push-based** so that it serves both kinds of
transport: a connection hands each message it has read to the engine (which
never blocks on it, as in Go), and takes batches to write. A socket transport
does that from its reader thread; a Netty transport from its event loop, with
no thread per connection, which is what lets a JVM server on JDK 17 hold many
connections without virtual threads.

### 5.5 Public API mapping

| Go | Java (proposed) |
|---|---|
| `valuerpc.Peer` (`CallFunction`, `GetStream`, `PutStream`, `PutStreamWait`, `Chat`) | `Peer`: `callFunction`, `callFunctionAsync` (`CompletableFuture`), `getStream`, `putStream`, `chat` |
| `<-chan value.Value` received | `ValueStream`: blocking iterator, `AutoCloseable` (`close()` before the end sends `CancelRequest`), carries the terminal error |
| `putCh <-chan value.Value` sent, `PutStreamWait`'s done channel | `ValueSink`: `send(v)` blocks on credit and throws if the stream was torn down, `close()` sends `StreamEnd` |
| `receiveCap` argument | dropped: the buffer is the credit window (`maxPending`) |
| `valuerpc.Registrar` (`AddFunction`, `AddOutgoingStream`, `AddIncomingStream`, `AddChat`) | `Registrar`, same four methods, on both `VrpcClient` and `VrpcServer` |
| `Function`, `OutgoingStream`, `IncomingStream`, `Chat` handler types | `UnaryHandler (ctx, args) → Value`, `OutgoingStreamHandler (ctx, args, sink)`, `IncomingStreamHandler (ctx, args, stream)`, `ChatHandler (ctx, args, stream, sink)` |
| `context.Context` in a call | `CallOptions` (timeout, metadata, cancellation); thread interruption cancels a blocked call |
| `context.Context` in a handler, `MetadataFromContext`, `PrincipalFromContext`, `PeerFromContext` | `RequestContext`: `metadata()`, `principal()`, `peer()`, `remainingMillis()`, `isCancelled()`, `onCancel(…)` |
| `valuerpc.Error`, `Code`, `CodeOf` | `VrpcException` (unchecked) with `code()`; `Code` enum 0–9 plus the raw wire number for an unknown one |
| `TypeDef`, `Any`, `Void`, `Arg`, `List`, `Map`, `Param`, `Verify` | `TypeDef` sealed hierarchy with the same constants and factories, `TypeDef.verify(value)` |
| `valuerpc.Codec[T]`, `CallUnary`, `GetStreamTyped`, `PutStreamTyped`, `AddUnary`, `AddOutgoingStreamTyped` | `Codec<T>` and a `Typed` helper class; plus `Codec.of(Class<T>)` backed by value-java's `ValueCodec`, which Go has no equivalent of at this layer |
| functional options (`WithTimeout`, `WithMaxConnections`, …) | builders: `VrpcClient.builder()…build()`, `VrpcServer.builder()…build()`; every Go option has a builder method |
| `ClientInterceptor`, `Invoker`, `ChainClientInterceptors` | same names, first interceptor outermost |
| `Metrics`, `NopMetrics` | `Metrics` interface with a no-op default |
| `*zap.Logger` | a small `VrpcLogger` interface, silent by default, with a `System.Logger`/JUL adapter; no SLF4J in core |
| `Dialect`, `DefaultDialect` | `Dialect`, a builder option per client/server, defaulting to `Dialect.standard()` |
| `ErrorHandler`, `ConnectionHandler`, `PerformanceMonitor`, `Stats()` | listener interfaces and `stats()` on the client |
| `ConnectAuthorizer`, `Authenticator`, `PeerCredOf`, `PeerCertificates` | same hooks; `MsgConn.peerCredentials()`, `MsgConn.tlsSession()` |

Go handlers return quickly and leave a goroutine behind; a Java handler would
rather run to the end on its own thread. To keep the wire the same,
`StreamReady` is sent **lazily**: on the handler's first read of the stream or
first `send`, or when it returns, whichever comes first. A handler that throws
before that produces the `ErrorResponse` the Go server would have sent in
place of `StreamReady`, so the caller's `getStream`/`putStream` fails exactly
as it does against Go. The handler slot counted against
`maxConcurrentRequests` is released when the stream is established, since in
Go the handler has returned by then; from there the stream counts against
`maxConcurrentStreams`.

### 5.6 Transports

| Go | Java | Where | Notes |
|---|---|---|---|
| `tcp://`, bare `host:port` | `java.net.Socket` / `ServerSocket` | core | set `TCP_NODELAY` explicitly: Go enables it by default, Java does not. Keepalive via `setKeepAlive` and, on a JDK, `ExtendedSocketOptions.TCP_KEEPIDLE` / `TCP_KEEPINTERVAL` (15 s) |
| SOCKS5 option | `new Socket(new Proxy(Proxy.Type.SOCKS, addr))` | core | TCP only, as in Go |
| `tls://`, `NewTLSServer/Client` | `SSLSocket` / `SSLServerSocket` from a caller's `SSLContext` | core | **set `endpointIdentificationAlgorithm = "HTTPS"`**: an `SSLSocket` does not verify the host name on its own, Go does. mTLS via `setNeedClientAuth`; peer chain from `SSLSession` |
| `ws://`, `wss://` client | the client side of RFC 6455 written in the library, over the TCP and TLS transports above | core | the HTTP upgrade, masked frames, fragments, ping/pong and the close. It offers the subprotocol of its codec and requires the echo, as the Go client does; enforces the frame limit; no extensions. Runs on Android 8 as it does on a JVM |
| `NewWebSocketServer` | Netty's HTTP and WebSocket codecs with the envelope codec as a handler | `value-rpc-netty` | path routing, subprotocol negotiation, same-origin default with origin patterns, ping keepalive, read limit, `wss` with Netty's `SslHandler` |
| `NewWebSocketHandler` (mount on your own HTTP server) | the same handlers, added to a pipeline of your own | `value-rpc-netty` | shares a port with other routes; Jakarta WebSocket / Spring adapters are deferred |
| TCP and TLS for a server with many connections | Netty listeners | `value-rpc-netty` | the same wire, without a thread per connection |
| `unix://` | `SocketChannel` / `ServerSocketChannel` with `StandardProtocolFamily.UNIX` (JDK 16+), in a class loaded only on a JVM; or Netty's native transport | core (JVM only), `value-rpc-netty` | stale-socket cleanup that refuses a non-socket file; peer credentials via `ExtendedSocketOptions.SO_PEERCRED` (uid/gid, no pid; platform support to verify on Linux and macOS) |
| `mem://` | in-JVM registry, rendezvous hand-off of the `MapValue` by reference | core | a mutable map is taken as `toImmutable()` at the boundary |
| `NewFuncDialer`, `NewSingleConnDialer`, `NewAcceptListener`, `NewSingleConnListener` | the same four, over an `InputStream`/`OutputStream` pair | core | the seam for custom transports |
| `quic://` (`valuequic.NewDialer`, `NewListener`) | Netty's QUIC codec (quiche and BoringSSL, native), with the stream of each request as a channel | `value-rpc-quic` | `QuicTransport.dialer` / `listener`; in the core the address gives the same "provided by a separate module" error as in Go core. TLS 1.3 with ALPN `vrpc`, mutual TLS, the handshake handed over before any other stream, streams freed when both halves ended. Not on Android |

Whether an Android app needs WebSocket at all depends on what the servers
expose: `tls://` is the protocol's own transport and needs nothing but a TLS
socket; `wss://` is for a server that is only reachable through an HTTPS port.

### 5.7 JSON wire codec

JSON is optional to use and has to be compatible when it is used. Go encodes
with `value.Jsonify` and decodes with `valuerpc.ParseJson`, and the port does
the same on both sides:

- **Encoding is `Value.jsonify()` of value-java 0.3.0**, which is `Jsonify` of
  value v1.7.0 and JSON for every value (finding F2, fixed upstream). The port
  writes no emitter of its own and requires value-java 0.3.0 or later.
- **Decoding is a port of `ParseJson`.** `Value.parseJson` of value-java is
  the *lossless* reader (a bare number becomes a decimal, `$long` wrappers),
  not the wire one. The wire reader takes integer syntax as a long (a big
  integer on overflow), fraction or exponent as a double, a string through
  `StringValue.parse` (`base64,` → binary), the extension prefix as an
  extension, the last of a repeated key, within the same depth and length
  limits. The base64 payload of an extension can be decoded through
  `StringValue.parse("base64," + rest)`, so no API has to be added to
  value-java.

### 5.8 What value-java could add (optional, not blocking)

- A pack-into-buffer entry point, to frame a message without the intermediate
  `byte[]` that `pack()` returns.

## 6. Findings that affect the port

These came out of reading the code. F1 and F2 were reproduced and are fixed
upstream, in releases of 2026-11-24; F5 is an inference that has not been
reproduced by running anything.

**F1. The Go WebSocket client always offers `vrpc.msgpack` and requires the
echo; the JS client offered nothing.** `msgpackCodec.Subprotocol()` returns
`"vrpc.msgpack"`, and has since the negotiation was added in v1.5.3, although
several comments said msgpack offers nothing. The Go implementation is the
reference, so:

- value-rpc-js 0.4.0 offers `vrpc.msgpack` and requires the echo, as Go does
  (a codec with no subprotocols still offers nothing, for a server older than
  v1.5.3);
- value-rpc v1.7.0 corrects the comments, and the error text of a dial whose
  codec was not negotiated;
- the Java client offers the token of its codec and requires the echo; the
  Java server echoes the codec it is offered, and serves msgpack to a client
  that offers nothing.

**F2. Go's JSON frames were not always JSON.** `Jsonify` quoted text with
`strconv.Quote`, a Go string literal, and wrote a map key with no escape at
all: a control character came out as `\a` or `\x1b`, a quote in a key ended
the key, and an infinite double came out as `+Inf`. Go's own `ParseJson` and a
browser's `JSON.parse` refuse such a frame, so one such argument lost the call.
Reproduced across the wire with the JS client against value-rpc v1.6.3. Fixed
where it was: **value v1.7.0** makes `Jsonify` JSON for every value, keeping
the earlier text wherever that was JSON already; **value-java 0.3.0** mirrors
it (checked against Go on the regenerated corpus); **value-rpc v1.7.0**
requires it and tests the codec on such envelopes. The Java port needs no
emitter of its own (§5.7).

**F3. A Null stream value is dropped by the receiver but costs the sender a
credit.** Both sides skip a `StreamValue` whose `val` is Null, and since it
never reaches the consumer the credit is never replenished. The Java sink
should refuse Null rather than send it.

**F4. `StreamEnd` may carry a final value**, and a receiver must deliver it.
The Go responder sends one when a cancel races a value it had already taken
from the handler's channel.

**F5. On a reconnect the server keeps the forward streams of the session.**
`replaceConn` fails only the reverse requests; the client fails its own
streams locally without a `CancelRequest`. A server-side streamer then appears
to stay blocked on credit until the session ends. The Java client can send
`CancelRequest` for the streams it failed, once resumed; the wire allows it
and the Go server handles it.

**F6. Presence must never be read with `getNumber`/`getString`.** As in Go
(BUG-5 in `FINDINGS.md`), `MapValue.getNumber` gives 0 for a missing key. The
port needs the two helpers `GetNumberField`/`GetStringField` and must use them
everywhere a field is optional or a peer may be hostile.

**F7. The handshake version is a double on the wire, but JSON turns it into
the integer 1.** The check must accept any number ≤ 1.0, as Go's does.

**F8. Reverse streams ignore the configured window.** The server opens them
with `DefaultMaxPending`, not `WithStreamMaxPending`. Harmless; keep it or
fix it on both sides.

## 7. Phases

Sizes: S under about 1,000 lines with tests, M 1,000 to 3,000, L above.
Each phase ends green in CI and is usable on its own.

The order follows the first user: an Android app, which is a client and a
peer. Phases 0 to 6 give it everything and end in a first release; the server
side follows.

| # | Phase | Content | Done when | Size |
|---|---|---|---|---|
| 0 | Scaffolding and conformance tooling | Gradle build with the API 26 check, CI, BUSL-1.1 headers; `conformance/` Go module pinned to value-rpc v1.7.0: fixture writer, Go server (scenario set of value-rpc-js's conformance server, on tcp/tls/ws/unix), Go client driver | `./gradlew build` green; fixtures committed; CI checks they are what the generator writes | S |
| 1 | Protocol core | `MessageType`, `Dialect`, envelope builders and field accessors, `Code`, `VrpcException`, `TypeDef`/verify, metadata, `HashChain`, `CreditGate`, stream buffer, `Metrics`, interceptor chain, `Codec<T>`, length-prefix framing with frame limit and batching, msgpack and JSON wire codecs | every envelope byte-identical to Go; hash-chain and JSON vectors pass; ports of `protocol_test`, `hashchain_test`, `wirecodec_test`, `rpc_test`, `batch_test`, `verify_null_test`, `dialect_test`, `interceptor_test`, pump/credit tests; random bytes never escape the decoder as anything but its own exception | M |
| 2 | Transport seam, TCP, TLS, in-memory | the push-based `MsgConn`, `Listener`, `Dialer`, address parsing; TCP with SOCKS5; TLS and mTLS with host-name verification; `mem://`; bring-your-own stream | ports of `transport_test`, `transport_conn_test`, `transport_mem_test`, `transport_tls_test` | M |
| 3a | Client: unary | lazy connect, handshake (synchronous until acknowledged; a coded rejection surfaces from `connect`), request table, timeout/SLA, cancel, interceptors, metrics, metadata, error handler, close | **Java client ↔ Go server over TCP and TLS**: greet, add, echo of the whole value model, coded error, NotFound, timeout, cancel, metadata, auth | M |
| 3b | Client: streams | get / put / chat with credit flow control, overflow, half-close, cancel | count (with back-pressure), early break, upload, chat.echo against the Go server; 100,000-value lossless stream; slow consumer does not block other requests | M |
| 3c | Client: reconnect | single-flight reconnect, fail-fast, unary replay, backoff with jitter, hash-chain resumption, credential re-sent | kick → reconnect → resume against the Go server; ports of the reconnect tests | M |
| 4 | Client as a peer | the client serves the calls and streams the server opens on it (negative ids, inbound cap, cancel propagation) | reverse.call / pull / push against the Go server, also after a resume; ports of `reverse_id_test` and the client half of the reverse tests | M |
| 5 | WebSocket client, JSON codec | the client side of RFC 6455 over the TCP and TLS transports; subprotocol offered and required; the JSON wire codec | every scenario of phases 3 and 4 against the Go WebSocket server in both codecs, `ws` and `wss` | M |
| 6 | Android, and a first release | an Android example app; the R8 rules; the scenarios run on an API 26 emulator and on a current one; `Typed` helpers and the `ValueCodec` bridge; README | **`com.arpabet:value-rpc:0.1.0`: the client**, on Android 8+ and JVM 17+ | M |
| 7 | Server engine | accept loop with backoff, connection cap, connect-authorizer, handshake, authenticator and principal, session registry, resume/replace, session reaping, sender with coalescing and close-flush, dispatch, request context, the four patterns, request/stream limits, handler-failure isolation, graceful close; over TCP, TLS and `mem://` on sockets | **Go client ↔ Java server**, all scenarios; Java ↔ Java matrix; ports of `integration_test` (39), `session_test`, `session_reap_test`, `server_close_flush_test`, `sender_flush_test`, `client_close_test` | L |
| 8 | Server as a peer | the server-side `Peer` handle: reverse calls and streams toward a client, fail-fast on reconnect, establish timeout | reverse scenarios in every pairing; ports of the `reverse_*` and hardening tests. Mostly wiring, the engine being symmetric since phase 3 | S–M |
| 9 | `value-rpc-netty` | WebSocket server (both codecs, origins, ping, limits, `wss`), mountable in a pipeline of your own; TCP, TLS and Unix listeners on event loops | transport matrix over every transport; Go and JS WebSocket clients against the Java server in both codecs (**the value-rpc-js conformance suite, 19 scenarios × 2 codecs, pointed at Java**) | L |
| 10 | Resilience, examples, docs | `value-rpc-resilience` (**done 2026-11-25**); the 12 examples of the Go repo; `PROTOCOL.md` | examples run against both servers; ports of `resilience` tests (done) | M |
| — | QUIC (added 2026-11-25) | `value-rpc-quic`: client builder, dialer and listener on Netty's QUIC codec (**done**); `valuequic.NewServer` as one call comes with the server engine | the Java client against the Go QUIC server and the Go client against the Java listener (done); the reverse tests of `quic/reverse_test.go` with phases 4 and 8 | M |
| 11 | Hardening and release | fuzz/property tests of frame and JSON decoding; stress (10,000 concurrent unary, many streams, churn of reconnects); thread and memory leak checks after close; JMH numbers next to Go's; a CI run on virtual threads; publish the server and Netty artifacts | all of §3 holds | M |

Phases 3, 4 and 7 are where the risk is. If the engine is built symmetric from
3a, phases 4 and 8 add little code.

## 8. Tests and conformance

Five layers, the first three in CI on every push:

1. **Fixtures written by Go** (`conformance/golden`, as value-java does for
   the codec): each envelope builder with fixed inputs as MessagePack hex and
   JSON text; hash-chain vectors (seed → anchor and links; verify results for
   valid, gapped, replayed and forged links); JSON-codec inputs and what they
   parse to; the `Verify` truth table. CI fails if the committed files are not
   what the generator writes.
2. **The Go tests, ported** (Appendix C). They are the behavioural spec:
   most carry a bug id from `FINDINGS.md` or `REVERSE.md`.
3. **Live interop.** `conformance/server` (Go) and a Java twin with the same
   functions (`greet`, `add`, `echo`, `fail`, `slow`, `md.echo`, `kick`,
   `count`, `upload`, `upload.stats`, `chat.echo`, `reverse.call`,
   `reverse.pull`, `reverse.push`), and a Go client driver that runs the
   scenario list against any address. Java tests start the Go processes and
   skip when Go is absent, as the JS suite does.

   | | tcp | unix | tls | ws msgpack | ws json | mem |
   |---|---|---|---|---|---|---|
   | Java client → Go server | ✓ | ✓ | ✓ | ✓ | ✓ | |
   | Go client → Java server | ✓ | ✓ | ✓ | ✓ | ✓ | |
   | JS client → Java server | | | | ✓ | ✓ | |
   | Java client → Java server | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

   The WebSocket columns of a Java server are the Netty module's. The JS row
   needs one small change in value-rpc-js: let `conformance.test.ts` take the
   server from an environment variable instead of always spawning the Go one.
4. **On Android**: the client scenarios against the Go server from an
   emulator at API 26 and at a current level, before the first release.
5. **Robustness**, before the last release: fuzzing, stress, leak checks, soak.

## 9. Risks

| Risk | Mitigation |
|---|---|
| Concurrency bugs. The Go code reached its current state through 16 numbered bugs (`FINDINGS.md`) and 8 reverse-path findings (`REVERSE.md`), many of them races and lifecycle leaks | port the regression tests with the code; one engine instead of two; a written lock order; stress tests in CI |
| A blocking handler style deadlocks against the establish handshake | lazy `StreamReady` (§5.5), tested against the Go client's 1 s default timeout |
| WebSocket client written in the library | the client side is the small one (no extensions); exercised against two independent servers (Go's, Netty's); the Autobahn test suite before release |
| Thread-per-connection on sockets | not a limit for a client; a JVM server with many connections takes the Netty listeners, or virtual threads on 21+ |
| Something above API 26 slips into the core | animal-sniffer fails the build; the scenarios run on an API 26 emulator before a release |
| Three implementations drift apart | `PROTOCOL.md`, shared fixtures, and each repo's conformance job running against the others; the Go implementation is the reference |
| Platform gaps: Unix sockets, peer credentials, keepalive tuning | JDK-only code isolated in classes loaded only when used, as value-java does with `JavaTime` |

## 10. Decisions

Taken on 2026-11-24:

| # | Question | Decision |
|---|---|---|
| D1 | Android | The main target. Android 8.0 (API 26) is the floor, enforced by the build on the core |
| D2 | Licence | BUSL-1.1, as the Go framework |
| D5 | WebSocket server | Netty, in a module of its own (`value-rpc-netty`), so that the core stays free of dependencies for Android. The WebSocket client is written in the core |
| D6 | JSON | Optional to use, compatible when used: the issue was fixed where it was (value v1.7.0, value-java 0.3.0, value-rpc v1.7.0), and the port encodes with `jsonify()` |
| D7 | Java baseline | 17, which Android requires; JDK 21 features cannot run on Android 8 |
| — | Which implementation decides | Go is the reference. value-rpc-js 0.4.0 was changed to negotiate its codec as Go does |
| — | `java.net.http` | Not used: it came with JDK 11 and no Android version has it |

Still on their defaults, to be confirmed or changed before phase 1:

| # | Question | Default |
|---|---|---|
| D3 | Coordinates and names: `com.arpabet:value-rpc`, package `com.arpabet.valuerpc`, classes `VrpcClient` / `VrpcServer` / `VrpcException` (JS uses the `Vrpc` prefix) | as written |
| D4 | API style: blocking-first with `CompletableFuture` variants, streams as iterator and sink | as written; no reactive types in core |
| D8 | Where `PROTOCOL.md` and the wire fixtures live in the end | start here; move to `value-rpc` and copy out, as `canonical.json` is |
| D9 | Skip QUIC; port `resilience`; keep the customizable dialect | resilience and the dialect: yes. QUIC: asked for on 2026-11-25 and added, on Netty (which D5 had welcomed for the server), as a module of its own that is not for Android |
| D10 | Does an Android app need `wss://`, or do the servers expose `tls://`? | build both; it only orders phases 5 and 6 |

---

## Appendix A — wire contract (as of value-rpc v1.7.0)

### A.1 Framing

- **Stream transports** (TCP, Unix, TLS, custom streams):
  `[4-byte big-endian length N][N bytes]`, the payload one MessagePack map in
  value's canonical encoding. A receiver enforces a frame limit (16 MiB by
  default) and drops the connection beyond it; a payload that is not a map is
  an error.
- **WebSocket:** one envelope per message, no length prefix. MessagePack in a
  binary frame, JSON in a text frame; the other frame type is an error. The
  frame limit is the read limit.
- **In-memory:** the map itself, by reference.

### A.2 Envelope fields (default dialect)

| Field | Meaning | Type |
|---|---|---|
| `t` | message type | integer |
| `rid` | request id | integer |
| `m` | magic, `"vRPC"` | text |
| `v` | version, `1.0` | number (sent as a double) |
| `cid` | client id | integer (Go: random int63) |
| `tok` | hash-chain link | text, 64 hex characters |
| `auth` | credential | any value |
| `fn` | function name | text |
| `args` | arguments | any value |
| `sla` | timeout in milliseconds | integer |
| `md` | metadata | map of text to text |
| `res` | unary result | any value |
| `val` | stream value | any value |
| `cr` | credit granted | integer |
| `code`, `err` | error code and message | integer, text |

A dialect may rename every field and the magic; both peers must share it.

### A.3 Message types

| # | Name | Fields besides `t`, `rid` | Sent by |
|---|---|---|---|
| 0 | HandshakeRequest | `m`, `v`, `cid`, `tok`?, `auth`?; `rid` = 0 | client |
| 1 | HandshakeResponse | `m`, `v`; `rid` = 0 | server |
| 2 | FunctionRequest | `fn`, `args`, `sla`?, `md`? | initiator |
| 3 | FunctionResponse | `res`? (absent or Null = no result) | responder |
| 4 / 5 / 6 | GetStreamRequest / PutStreamRequest / ChatRequest | `fn`, `args`, `sla`?, `md`? | initiator |
| 7 | ErrorResponse | `code`, `err` | either |
| 8 | StreamReady | | responder |
| 9 | StreamValue | `val` | the sending side of a stream |
| 10 | StreamEnd | `val`? | the sending side of a stream |
| 11 | CancelRequest | | either |
| 12, 13 | reserved | | |
| 14 | StreamCredit | `cr` | the receiving side of a stream |

### A.4 Handshake

1. The client sends HandshakeRequest. The server waits at most 10 s for it.
2. Not a map, no `t`, `t` ≠ 0, wrong magic, or a version above 1.0: the server
   closes **without a reply**, so a probe learns nothing.
3. From here a refusal is an ErrorResponse with `rid` 0, then the close:
   `Unauthenticated "authentication failed"` (the authenticator, called with
   `auth` or Null), `InvalidArgument "malformed handshake"` (no `cid`),
   `Unauthenticated "session resume rejected"`.
4. Otherwise the server sends HandshakeResponse. The client reads until it
   arrives, dispatching any other frame normally (a resumed session may send
   some first), and only then counts as connected.

### A.5 Request ids

0 is the handshake. A client numbers its requests 1, 2, 3, …; a server numbers
the ones it initiates −1, −2, −3, …. A server answers a new request whose id
is not positive with `InvalidArgument "client-initiated request id must be
positive"`; a client ignores, as a protocol error, a server request whose id
is not negative. Ids are not reused across a reconnect.

### A.6 The four patterns

`I` is the initiator, `R` the responder.

- **Unary.** `I` FunctionRequest → `R` FunctionResponse or ErrorResponse.
- **Get-stream** (`R` sends). `I` GetStreamRequest → `R` StreamReady → `I`
  StreamCredit(window) → `R` StreamValue… → `R` StreamEnd.
- **Put-stream** (`I` sends). `I` PutStreamRequest → `R` StreamCredit(window)
  → `R` StreamReady → `I` StreamValue… → `I` StreamEnd. The credit comes
  before the ready.
- **Chat.** Both at once. `I`'s StreamEnd only closes `R`'s input; the stream
  ends with `R`'s StreamEnd.
- **Refusal.** `R` answers the opening request with ErrorResponse in place of
  StreamReady. An ErrorResponse may also arrive later and ends the request.
- **Cancel.** CancelRequest from either side, never acknowledged. For a
  stream it tears the stream down; for a unary call it cancels the handler,
  whose late reply the initiator discards.

**Flow control.** A sender sends one StreamValue per credit and waits when it
has none. A receiver grants its whole window (4096 by default) when the stream
is established and grants again, in batches of half the window, as its
application *consumes* values. A receiver whose buffer overflows answers
`ResourceExhausted "… exceeded flow-control credit"` and tears the stream down.

**Timeouts.** `sla` is the lesser of the client's timeout (1000 ms by default)
and what remains of the call's deadline. A server turns it into the handler's
deadline for a unary call only. The client's own timer bounds a unary call,
and for a stream only its establishment; on expiry it sends CancelRequest.

### A.7 Session resumption

`h[0]` is 32 random bytes, `h[i] = SHA-256(h[i−1])`, the anchor is `h[N]`,
`N` = 10,000; a link travels as lower-case hex. The client sends the anchor
until a handshake is first acknowledged, then on **every** reconnect the next
link down (`h[N−1]`, `h[N−2]`, …), whether or not the previous attempt got
through; when the chain is used up the client must start a new session.

The server stores the first value it sees for a `cid`. On a later handshake
with that `cid` it resumes the session only if the principal is the same
**and** hashing the presented link forward, at most 1024 times, gives the
stored value (it then stores the presented one), **or** the link equals the
stored one (a retry; nothing advances). A resumed session takes over the
connection: the old one is closed and its reverse requests fail with
`Unavailable`. A session without a connection is dropped after 2 minutes.

### A.8 Codes and error text

0 OK, 1 Unknown, 2 Canceled, 3 InvalidArgument, 4 DeadlineExceeded,
5 NotFound, 6 ResourceExhausted, 7 Unavailable, 8 Unauthenticated, 9 Internal.

An error prints as `vrpc <CodeName>: <message>`. A handler's failure becomes
its own code if it carries one, DeadlineExceeded or Canceled for a deadline or
a cancellation, Internal otherwise, with the message
`<where>: <message>`, where `<where>` is `function <name>`,
`out stream <name>`, `in stream <name>` or `chat <name>`.

Server texts: `function name field not found` (3), `function not found <name>`
(5), `function '<name>' invalid args` (3), `function wrong type <name>,
expected <n>, actual <n>` (3), `function '<name>' invalid results` (9),
`server busy: too many concurrent requests (max <n>)` (6), `server busy: too
many concurrent streams (max <n>)` (6).

### A.9 Where the two directions differ

| | client → server | server → client |
|---|---|---|
| `args` when there are none | sent as Null | left out |
| `sla` | sent when the timeout is positive | never sent |
| `md` | all four patterns, from the injector | unary only, from the calling context |
| unary bounded by | timeout timer and the caller | the caller only |
| stream establishment bounded by | timeout timer | 10 s |
| receive window | configured `maxPending` | 4096 always |
| interceptors | unary chain | none |
| in flight when the connection is replaced | fails `Unavailable "connection lost during reconnect"`, or is replayed | fails `Unavailable "connection replaced by reconnect; …"` |

| | server as responder | client as responder |
|---|---|---|
| wrong-sign id | ErrorResponse | ignored |
| concurrency limit | 4096 handlers (all patterns), streams unlimited | 1024 handlers (unary only) |
| registering a name twice | error | replaces |
| handler deadline | from `sla`, unary | none |
| handler context | metadata, principal, peer | metadata (unary) |
| checks | arguments, then pattern | pattern, then arguments |
| texts | A.8 | `function '<name>' is not unary`, `function '<name>' wrong stream type`, `stream '<name>' invalid args`, `function '<name>' invalid result`, `client busy: too many concurrent reverse calls (max <n>)` |

### A.10 WebSocket negotiation

Subprotocols `vrpc.msgpack` and `vrpc.json`. A client offers the token of its
codec and requires the server to echo it (Go since v1.5.3, where the
negotiation was added; JS since 0.4.0). A server that enables both (the
default) picks, in its own order of preference, the first one the client
offers and echoes it; a client that offers nothing gets MessagePack. Origin:
same-origin unless patterns are configured. Keepalive: pings every 15 s; a
failed ping closes the connection.

### A.11 JSON mapping

| Kind | JSON | Read back as |
|---|---|---|
| null, bool | `null`, `true`/`false` | the same |
| long | number | long; big integer if beyond 64 bits |
| double | number; NaN → `null` | double (fraction or exponent syntax) |
| big integer, decimal | text in hexadecimal (`0x…`) | **text** |
| timestamp | RFC 3339 text | **text** |
| text | string | text, unless it starts with one of the two prefixes below |
| binary | `"base64,<standard, unpadded>"` | binary |
| extension | `"data:application/x-msgpack-ext;base64,<type byte + data>"` | extension |
| list, map | array, object with sorted keys | list, map (a repeated key: the last wins) |

Text and map keys are JSON strings in which a character that is not printable
is escaped; a byte that is not UTF-8 has no JSON form and travels as U+FFFD;
an infinite double is `null`, as NaN is (value v1.7.0; before it such a frame
was not JSON).

---

## Appendix B — Go file → Java class

| Go | Java |
|---|---|
| `valuerpc/protocol.go`, the builders of `peer.go` | `MessageType`, `Dialect`, `Envelope` |
| `valuerpc/peer.go` | `Peer`, `Registrar`, the four handler interfaces |
| `valuerpc/rpc.go` | `MsgConn`, `FramedMsgConn` |
| `valuerpc/wirecodec.go` | `WireCodec`, `MsgpackWireCodec`, `JsonWireCodec`, `WireJson` |
| `valuerpc/types.go`, `verify.go` | `TypeDef` |
| `valuerpc/codec.go`, `valueclient/typed.go`, `valueserver/typed.go` | `Codec<T>`, `Typed` |
| `valuerpc/error.go` | `Code`, `VrpcException` |
| `valuerpc/credit.go`, `pump.go` | `CreditGate`, `StreamBuffer` |
| `valuerpc/hashchain.go` | `HashChain` |
| `valuerpc/metadata.go`, `principal.go`, `valueserver/peer.go` | `RequestContext`, `CallOptions` |
| `valuerpc/metrics.go`, `interceptor.go` | `Metrics`, `ClientInterceptor`, `Invoker` |
| `valuerpc/peercred*.go` | `PeerCredentials` |
| `valuerpc/transport.go`, `transport_conn.go` | `Listener`, `Dialer`, `Transports`, `TcpTransport` |
| `valuerpc/transport_mem.go`, `transport_tls.go` | `MemTransport`, `TlsTransport` |
| `valuerpc/transport_ws.go` | `WebSocketClientTransport` in the core; the server side in `value-rpc-netty` (`NettyWebSocketListener` and its handlers) |
| `valueclient/api.go`, `options.go`, `error.go` | `VrpcClient` and its builder, `ReconnectPolicy`, `ErrorHandler` |
| `valueclient/client.go`, `connection.go`, `sync_conn.go` | `ClientImpl`, `ClientConnection` |
| `valueclient/request.go`, `valueserver/serving_stream.go` | engine `Initiator` |
| `valueclient/serving.go`, `valueserver/serving_request.go` | engine `Responder` |
| `valueserver/api.go`, `options.go`, `debug.go`, `function.go` | `VrpcServer` and its builder, `ConnectAuthorizer`, `Authenticator`, `HandlerRegistry` |
| `valueserver/server.go`, `serving_client.go` | `ServerImpl`, `Session` |
| `resilience/*.go` | `com.arpabet.valuerpc.resilience`: `Resilience` (the six policies, as the functions of the Go package), `Retry` and `CircuitBreaker` (what `With…` options are in Go are `with…` methods here), `Fallback` |
| `quic/transport.go` | `com.arpabet.valuerpc.quic`: `QuicTransport` (`NewClient`, `NewDialer`, `NewListener`; `NewServer` comes with the server engine), `QuicOptions` (`DialTimeout`, `KeepAlivePeriod`, `MaxStreams`, the `tls.Config`), `QuicConn` (`quicMsgConn`), `StreamRules` (`finOnWrite`, `finOnRead`, who opens a stream) |

## Appendix C — Go tests → phase

| Go tests | Count | Phase |
|---|---|---|
| `valuerpc`: protocol, verify, hashchain, wirecodec, rpc, batch, pump, dialect, interceptor, fuzz | 48 | 1 |
| `valuerpc`: transport, transport_conn, transport_ws_internal | 13 | 2, 5, 9 |
| `valueclient`: client, request_bug, regression_fixes, options, interceptor, typed, reverse_id | 28 | 3, 4, 6 |
| `valueserver`: integration, matrix, session, session_reap, close_flush, sender_flush, client_close, dialect, example | 59 | 7 |
| `valueserver`: reverse_call, reverse_stream, reverse_metadata, reverse_putwait, reverse_reconnect, hardening_reverse | 13 | 4, 8 |
| `valueserver`: transport, transport_conn, transport_mem, transport_tls, transport_ws, transport_ws_json | 24 | 2, 7, 9 |
| `resilience` | 14 | 10: done |
| `quic` | 14 | done but for the 4 of `reverse_test.go`, which need phases 4 and 8 |

## Appendix D — defaults to keep

| Setting | Value |
|---|---|
| frame limit | 16 MiB |
| write coalescing | 64 frames |
| stream window (`maxPending`) | 4096 |
| client call timeout | 1000 ms |
| client send queue | 1024 |
| client dial timeout | 30 s |
| client concurrent reverse calls | 1024 |
| reconnect | fail fast, no retry; when retries are on: 100 ms doubling to 30 s |
| hash chain length / server resync window | 10,000 / 1024 |
| write timeout | 10 s |
| keepalive / WebSocket ping | 15 s |
| server handshake timeout | 10 s |
| server connections | unlimited |
| server concurrent requests per client | 4096 |
| server concurrent streams per client | unlimited |
| server send queue per client | 4096 |
| server session retention | 2 min |
| server close flush | 2 s |
| reverse stream establish timeout | 10 s |
| accept retry backoff | 5 ms doubling to 1 s |
