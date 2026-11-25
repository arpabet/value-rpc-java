plugins {
    id("vrpc.library")
    id("vrpc.android-api")
    id("vrpc.publish")
}

description = "Resilience policies for vRPC clients: the Java implementation of go.arpabet.com/value-rpc/resilience"

dependencies {
    api(project(":value-rpc"))

    testImplementation(testFixtures(project(":value-rpc")))
}

tasks.jar {
    manifest {
        attributes("Automatic-Module-Name" to "com.arpabet.valuerpc.resilience")
    }
}
