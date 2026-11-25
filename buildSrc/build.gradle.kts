// What the modules have in common is written once, as the plugins of
// src/main/kotlin, and each module applies the ones it needs.

plugins {
    `kotlin-dsl`
}

repositories {
    gradlePluginPortal()
}

dependencies {
    // The check of the Android API level, applied by vrpc.android-api.
    implementation("ru.vyarus:gradle-animalsniffer-plugin:2.0.1")
}
