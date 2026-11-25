# Releasing

The three modules are released together, in one version, to Maven Central:
`com.arpabet:value-rpc`, `com.arpabet:value-rpc-resilience` and
`com.arpabet:value-rpc-quic`. A release is a tag: pushing `v` + the version
runs [`.github/workflows/release.yaml`](.github/workflows/release.yaml), which
builds, tests, signs, uploads, publishes, and makes the GitHub Release. What
is on Maven Central can never be changed or removed, so the steps before the
tag are the ones that count.

## Once, for the repository

The workflow uses four secrets, the ones value-java is released with, by the
same names. The secrets of a repository are its own: they have to be set for
this one too, or for the organization with this repository among those that
may use them.

| Secret | What it is |
|---|---|
| `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD` | the two halves of a user token of the Central Portal account that holds the namespace `com.arpabet` |
| `SIGNING_KEY`, `SIGNING_PASSWORD` | the PGP key the artifacts are signed with, ASCII-armored, and its passphrase |

```sh
gh secret set MAVEN_CENTRAL_USERNAME --repo arpabet/value-rpc-java     # asks for the value
gh secret set MAVEN_CENTRAL_PASSWORD --repo arpabet/value-rpc-java
gh secret set SIGNING_KEY --repo arpabet/value-rpc-java < signing-key.asc
gh secret set SIGNING_PASSWORD --repo arpabet/value-rpc-java
gh secret list --repo arpabet/value-rpc-java                           # the four names
```

The workflow says which are missing before it builds anything.

## Before the tag

1. **What the release depends on is released.** `com.arpabet:value` in the
   version `value-rpc/build.gradle.kts` names is on Maven Central, and
   `conformance/go.mod` pins the release of value-rpc to be compatible with,
   not a checkout.
2. **The build passes as the release will build it**, against Maven Central
   and not against a checkout of value-java next to this one:

   ```sh
   ./gradlew clean build -PnoComposite
   (cd conformance && test -z "$(gofmt -l .)" && go vet ./... && go run ./golden)
   git status --short value-rpc/src/test/resources/golden     # nothing: the fixtures are current
   ```

3. **The version** in `build.gradle.kts` has no `-SNAPSHOT`, `CHANGELOG.md`
   has the release under its version and date, and the README names the
   version in its coordinates.
4. **`build.yaml` is green on GitHub for the commit to be tagged**, on both
   JDKs. It is the only place the build runs on Linux, and on JDK 21.
5. **A rehearsal, when anything about the release changed**: run the Release
   workflow by hand from the Actions page, with "publish" not ticked. It does
   everything a tag does, except that the bundle is only staged: look at it at
   <https://central.sonatype.com/publishing/deployments>, and drop it there.

What the bundle holds can be looked at without GitHub, with any key at all:

```sh
./gradlew centralBundle -PnoComposite -PsigningKey="$(gpg --armor --export-secret-keys <key id>)" -PsigningPassword=<passphrase>
unzip -l build/central/value-rpc-java-<version>-bundle.zip
```

## The tag

```sh
git tag v0.1.0
git push origin v0.1.0
```

The workflow refuses a tag that is not `v` + the version of the build, and a
version that ends in `-SNAPSHOT`. A version that is on Maven Central already
is not uploaded again, so a run that failed after the upload can be run again.

## After

1. The artifacts are at
   <https://repo1.maven.org/maven2/com/arpabet/value-rpc/>; the search of
   Maven Central takes longer to show them.
2. On `main`: the next version with `-SNAPSHOT` in `build.gradle.kts`, and
   "Unreleased" at the top of `CHANGELOG.md`.
