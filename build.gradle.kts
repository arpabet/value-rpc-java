// What every module shares. The modules themselves are in their directories:
//   value-rpc   the protocol, the client and the server engine; it depends on
//               com.arpabet:value alone and loads on Android 8.0 (API 26)

allprojects {
    group = "com.arpabet"
    // Between releases, the next version with -SNAPSHOT.
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}
