// A module that is released to Maven Central: its jar with the sources and
// the javadoc, a POM with what Maven Central asks for, each file signed.

plugins {
    `java-library`
    `maven-publish`
    signing
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set(project.name)
                // The description of a module is set after its plugins: asked for when the POM is written.
                description.set(provider { project.description })
                url.set("https://github.com/arpabet/value-rpc-java")
                licenses {
                    license {
                        name.set("BUSL-1.1")
                        url.set("https://github.com/arpabet/value-rpc-java/blob/main/LICENSE")
                    }
                }
                developers {
                    developer {
                        name.set("Karagatan LLC")
                        organization.set("Karagatan LLC")
                        organizationUrl.set("https://github.com/arpabet")
                    }
                }
                scm {
                    connection.set("scm:git:https://github.com/arpabet/value-rpc-java.git")
                    developerConnection.set("scm:git:ssh://git@github.com/arpabet/value-rpc-java.git")
                    url.set("https://github.com/arpabet/value-rpc-java")
                }
            }
        }
    }
    repositories {
        // Where a release is laid out before it goes to Maven Central: one
        // Maven repository for all the modules, in the build directory of the
        // root, which its `centralBundle` zips.
        maven {
            name = "staging"
            url = uri(rootProject.layout.buildDirectory.dir("staging"))
        }
    }
}

// A release is signed with the key the release workflow passes in
// (ORG_GRADLE_PROJECT_signingKey, an ASCII-armored private key, and
// ORG_GRADLE_PROJECT_signingPassword). Without a key, as on a developer's
// machine, nothing is signed and `publishToMavenLocal` works as it is.
signing {
    val key = providers.gradleProperty("signingKey").orNull
    if (!key.isNullOrBlank()) {
        useInMemoryPgpKeys(key, providers.gradleProperty("signingPassword").orNull)
        sign(publishing.publications["maven"])
    }
}

// The staging repository is emptied before anything is laid out in it, once
// for all the modules.
tasks.named("publishMavenPublicationToStagingRepository") {
    dependsOn(rootProject.tasks.named("cleanStaging"))
}
