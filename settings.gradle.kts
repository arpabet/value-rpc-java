rootProject.name = "value-rpc-java"

include("value-rpc")
include("value-rpc-resilience")
include("value-rpc-quic")

// A checkout of value-java next to this repository stands in for
// com.arpabet:value, whatever its version: both are then built from source,
// which is how the two are worked on together. Without one, or with
// -PnoComposite, the dependency comes from Maven Central.
val valueJava = file("../value-java")
if (valueJava.resolve("settings.gradle.kts").isFile && !providers.gradleProperty("noComposite").isPresent) {
    includeBuild(valueJava)
}
