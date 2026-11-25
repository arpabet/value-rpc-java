plugins {
    id("vrpc.library")
    id("vrpc.android-api")
    id("vrpc.publish")
    // FakeServer and GoServer, the peers the tests of every module talk to.
    `java-test-fixtures`
}

description = "vRPC for Java and Android: the Java implementation of go.arpabet.com/value-rpc"

dependencies {
    // Values are the arguments and results of every call: part of the API.
    api("com.arpabet:value:0.3.0")
}

tasks.test {
    // A directory of larger golden files, written by `go run ./golden -out <dir>`
    // in conformance/, to check the implementation against in place of the
    // committed ones.
    providers.gradleProperty("goldenDir").orNull?.let { systemProperty("valuerpc.golden.dir", it) }
}

tasks.jar {
    manifest {
        attributes("Automatic-Module-Name" to "com.arpabet.valuerpc")
    }
}

// The servers of the tests are for the tests of this repository: what is
// published is the library.
val javaComponent = components["java"] as AdhocComponentWithVariants
javaComponent.withVariantsFromConfiguration(configurations["testFixturesApiElements"]) { skip() }
javaComponent.withVariantsFromConfiguration(configurations["testFixturesRuntimeElements"]) { skip() }
