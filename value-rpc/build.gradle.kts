plugins {
    `java-library`
    id("ru.vyarus.animalsniffer") version "2.0.1"
}

description = "vRPC for Java and Android: the Java implementation of go.arpabet.com/value-rpc"

dependencies {
    // Values are the arguments and results of every call: part of the API.
    api("com.arpabet:value:0.3.0")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // The Android API level this module may call into, checked by the
    // animalsniffer task (part of `check`): Android 8.0.
    signature("com.toasttab.android:gummy-bears-api-26:0.16.0@signature")
}

java {
    withSourcesJar()
    withJavadocJar()
}

// Java 17 bytecode, as com.arpabet:value: the language level of the Android
// build tools and of every maintained server runtime. On Android it calls
// nothing above API 26, which the animalsniffer task enforces; what only a
// JDK has is confined to the classes named below, which load only when an
// application on a JDK uses them.
tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
    options.encoding = "UTF-8"
}

tasks.compileJava {
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

animalsniffer {
    sourceSets = listOf(project.sourceSets.main.get())
}

tasks.javadoc {
    options.encoding = "UTF-8"
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
}

tasks.test {
    useJUnitPlatform()
    // A directory of larger golden files, written by `go run ./golden -out <dir>`
    // in conformance/, to check the implementation against in place of the
    // committed ones.
    providers.gradleProperty("goldenDir").orNull?.let { systemProperty("valuerpc.golden.dir", it) }
    testLogging {
        events("failed")
        showStandardStreams = false
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

// The same sources give the same jars, byte for byte.
tasks.withType<AbstractArchiveTask>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}

tasks.jar {
    manifest {
        attributes(
            "Automatic-Module-Name" to "com.arpabet.valuerpc",
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version,
        )
    }
}
