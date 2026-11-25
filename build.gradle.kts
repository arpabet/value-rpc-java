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
    // Between releases, the next version with -SNAPSHOT. A release takes the
    // suffix off and is tagged `v` + the version (.github/workflows/release.yaml).
    // The modules are released together, in one version.
    version = "0.1.0"

    repositories {
        mavenCentral()
    }
}

val cleanStaging by tasks.registering(Delete::class) {
    delete(layout.buildDirectory.dir("staging"))
}

// The bundle the Central Portal takes (central.sonatype.org/publish/publish-portal-upload):
// the files of the release, of every module, in the layout of a Maven
// repository, each with its signature and its checksums. Maven Central writes
// the maven-metadata.xml itself, and a signature needs no checksum.
tasks.register<Zip>("centralBundle") {
    group = "publishing"
    description = "Zips the signed release of all modules for upload to the Central Portal."
    dependsOn(subprojects.map { "${it.path}:publishMavenPublicationToStagingRepository" })
    from(layout.buildDirectory.dir("staging"))
    exclude("**/maven-metadata.xml*")
    exclude("**/*.asc.md5", "**/*.asc.sha1", "**/*.asc.sha256", "**/*.asc.sha512")
    destinationDirectory.set(layout.buildDirectory.dir("central"))
    archiveFileName.set("${rootProject.name}-${project.version}-bundle.zip")
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
    doFirst {
        val staged = layout.buildDirectory.dir("staging").get().asFileTree.files
        check(staged.any { it.name.endsWith(".jar.asc") }) {
            "the release is not signed: pass -PsigningKey and -PsigningPassword"
        }
    }
}
