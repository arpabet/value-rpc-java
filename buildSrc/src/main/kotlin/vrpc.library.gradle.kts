// A module of the library: a jar of Java 17 bytecode with its sources and
// javadoc, compiled without a warning, tested with JUnit.

plugins {
    `java-library`
}

dependencies {
    "testImplementation"(platform("org.junit:junit-bom:5.11.4"))
    "testImplementation"("org.junit.jupiter:junit-jupiter")
    "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
}

java {
    withSourcesJar()
    withJavadocJar()
}

// Java 17 bytecode, as com.arpabet:value: the language level of the Android
// build tools and of every maintained server runtime.
tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
    options.encoding = "UTF-8"
}

tasks.named<JavaCompile>("compileJava") {
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
}

tasks.withType<Javadoc>().configureEach {
    options.encoding = "UTF-8"
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    // The tests against the Go implementation build and run what is in
    // conformance/: when that changes, they are run again.
    inputs.files(rootProject.fileTree("conformance") { include("**/*.go", "go.mod", "go.sum", "go.work") })
        .withPropertyName("conformance")
        .withPathSensitivity(PathSensitivity.RELATIVE)
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

// The licence travels with every copy: in the jar, and in the jars of its
// sources and of its javadoc.
tasks.withType<Jar>().configureEach {
    from(rootProject.file("LICENSE")) {
        into("META-INF")
    }
}

tasks.named<Jar>("jar") {
    manifest {
        attributes(
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version,
        )
    }
}
