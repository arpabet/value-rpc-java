// A module that has to load on Android 8.0 (API 26): the animalsniffer task,
// part of `check`, fails the build when its code calls anything that level
// does not have. What only a JDK has stays out of such a module.

plugins {
    `java-library`
    id("ru.vyarus.animalsniffer")
}

dependencies {
    "signature"("com.toasttab.android:gummy-bears-api-26:0.16.0@signature")
}

animalsniffer {
    sourceSets = listOf(project.the<SourceSetContainer>()["main"])
}
