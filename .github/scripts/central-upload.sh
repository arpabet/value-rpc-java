#!/usr/bin/env bash
#
# Uploads a release bundle to the Central Portal, which serves Maven Central,
# and waits to see what becomes of it there.
#
#   central-upload.sh <bundle.zip> [USER_MANAGED|AUTOMATIC]
#
# USER_MANAGED (the default) stages the release: the Portal validates the
# bundle and keeps it until it is published or dropped by hand at
# https://central.sonatype.com/publishing/deployments. AUTOMATIC publishes it
# to Maven Central once it is valid, which cannot be undone.
#
# CENTRAL_USERNAME and CENTRAL_PASSWORD are the two halves of a user token of
# the Portal account (Account, Generate User Token), not the login.
#
# The API: https://central.sonatype.org/publish/publish-portal-api/

set -euo pipefail

bundle="${1:?usage: central-upload.sh <bundle.zip> [USER_MANAGED|AUTOMATIC]}"
mode="${2:-USER_MANAGED}"
: "${CENTRAL_USERNAME:?the Central Portal token is not set (secret MAVEN_CENTRAL_USERNAME)}"
: "${CENTRAL_PASSWORD:?the Central Portal token is not set (secret MAVEN_CENTRAL_PASSWORD)}"

case "$mode" in
  USER_MANAGED | AUTOMATIC) ;;
  *) echo "unknown publishing type: $mode" >&2; exit 2 ;;
esac
[ -f "$bundle" ] || { echo "no bundle at $bundle" >&2; exit 2; }

url="${CENTRAL_URL:-https://central.sonatype.com}"
auth="Authorization: Bearer $(printf '%s:%s' "$CENTRAL_USERNAME" "$CENTRAL_PASSWORD" | base64 | tr -d '\n')"
name="$(basename "$bundle" .zip)"

echo "Uploading $name ($mode)"
if ! id="$(curl --silent --show-error --fail-with-body --request POST \
  --header "$auth" \
  --form "bundle=@${bundle}" \
  "${url}/api/v1/publisher/upload?name=${name}&publishingType=${mode}")"; then
  echo "The Portal did not take the upload (401 is a wrong or expired user token): $id" >&2
  exit 1
fi
echo "Deployment $id"

# Validation takes a minute or two; reaching Maven Central after that, a few more.
tries="${CENTRAL_TRIES:-180}"
pause="${CENTRAL_PAUSE:-10}"
state=""
for _ in $(seq 1 "$tries"); do
  # A status that cannot be had this time is asked for again: the upload is done.
  if ! status="$(curl --silent --show-error --fail-with-body --request POST \
    --header "$auth" \
    "${url}/api/v1/publisher/status?id=${id}")"; then
    echo "  (no answer: $status)"
    sleep "$pause"
    continue
  fi
  next="$(printf '%s' "$status" | sed -n 's/.*"deploymentState"[[:space:]]*:[[:space:]]*"\([A-Z_]*\)".*/\1/p')"
  if [ "$next" != "$state" ]; then
    state="$next"
    echo "  ${state:-(no state in the answer)}"
  fi
  case "$state" in
    PUBLISHED)
      echo "Published to Maven Central."
      exit 0
      ;;
    VALIDATED)
      if [ "$mode" = USER_MANAGED ]; then
        echo "Valid, and staged: publish or drop it at ${url}/publishing/deployments"
        exit 0
      fi
      ;;
    FAILED)
      echo "The Portal refused the bundle:" >&2
      printf '%s\n' "$status" >&2
      exit 1
      ;;
  esac
  sleep "$pause"
done

if [ "$state" = PUBLISHING ]; then
  # Accepted and on its way; only the copy to Maven Central is slow today.
  echo "Still publishing after $((tries * pause)) seconds: see ${url}/publishing/deployments"
  exit 0
fi
echo "No verdict after $((tries * pause)) seconds (last state: ${state:-none}): see ${url}/publishing/deployments" >&2
exit 1
