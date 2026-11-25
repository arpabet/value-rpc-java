// What every module shares. The modules themselves are in their directories,
// and what their builds have in common is in buildSrc:
//   value-rpc              the protocol, the client and the transports; it
//                          depends on com.arpabet:value alone and loads on
//                          Android 8.0 (API 26)
//   value-rpc-resilience   retry, circuit breaker, timeout, rate limit,
//                          bulkhead and fallback as interceptors of a client;
//                          no dependency of its own, loads on Android too
//   value-rpc-quic         the QUIC transport, on Netty's QUIC codec, which
//                          is native code: for a JVM, not for Android

allprojects {
    group = "com.arpabet"
    // Between releases, the next version with -SNAPSHOT.
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}
