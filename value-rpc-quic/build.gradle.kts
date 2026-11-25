plugins {
    id("vrpc.library")
    id("vrpc.publish")
}

description = "QUIC transport for vRPC, on Netty: the Java implementation of go.arpabet.com/value-rpc/quic"

// Netty's QUIC codec: quiche and BoringSSL behind a Java API. It is native
// code, which is why this is a module of its own and not for Android.
val nettyVersion = "4.2.21.Final"

dependencies {
    api(project(":value-rpc"))

    // The TLS context of a transport, and the event loops it may be given, are
    // Netty's: part of the API.
    api("io.netty:netty-codec-classes-quic:$nettyVersion")

    // The native library, for each platform Netty builds one for, so that the
    // transport works wherever it is put. One that ships to a single platform
    // may exclude the others.
    for (platform in listOf("linux-x86_64", "linux-aarch_64", "osx-x86_64", "osx-aarch_64", "windows-x86_64")) {
        runtimeOnly("io.netty:netty-codec-native-quic:$nettyVersion:$platform")
    }

    testImplementation(testFixtures(project(":value-rpc")))
    // Certificates for the tests, made on the spot.
    testImplementation("io.netty:netty-pkitesting:$nettyVersion")
}

tasks.jar {
    manifest {
        attributes("Automatic-Module-Name" to "com.arpabet.valuerpc.quic")
    }
}
