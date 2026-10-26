#!/bin/sh
# Run the same Go API contract against three pinned, isolated emulators.
set -eu
TMPDIR=/tmp
export TMPDIR
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
run_dir=$(mktemp -d /tmp/rhiza-embedded-providers.XXXXXX)
run_name="rhiza-embedded-$(date +%s)-$$"
image="rhiza-embedded-provider-test:$run_name"
containers=""
networks=""
image_built=0
result=1
cleanup() {
 trap - EXIT HUP INT TERM
 cleanup_failed=0
 for id in $containers; do
  if docker inspect "$id" >/dev/null 2>&1; then
   docker logs "$id" >"$run_dir/$id.log" 2>&1 || :
   if ! docker rm -f "$id" >/dev/null; then cleanup_failed=1; fi
  fi
  if docker inspect "$id" >/dev/null 2>&1; then echo "Owned container remains: $id" >&2; cleanup_failed=1; fi
 done
 for network in $networks; do
  if ! docker network rm "$network" >/dev/null; then cleanup_failed=1; fi
  if docker network inspect "$network" >/dev/null 2>&1; then echo "Owned network remains: $network" >&2; cleanup_failed=1; fi
 done
 if [ "$image_built" -eq 1 ]; then
  if ! docker image rm "$image" >"$run_dir/image-cleanup.log" 2>&1; then cleanup_failed=1; fi
  if docker image inspect "$image" >/dev/null 2>&1; then echo "Owned test image remains: $image" >&2; cleanup_failed=1; fi
 fi
 if [ "$result" -eq 0 ] && [ "$cleanup_failed" -eq 0 ]; then
  rm -rf "$run_dir"
  if [ -d "$run_dir" ]; then echo "Owned run directory remains: $run_dir" >&2; cleanup_failed=1; fi
 else echo "Provider diagnostics preserved: $run_dir" >&2; fi
 if [ "$cleanup_failed" -eq 0 ]; then echo "Cleanup verified: owned containers, networks, and test image absent ($run_name)"; fi
 if [ "$cleanup_failed" -ne 0 ]; then exit 1; fi
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
case "$(docker info --format '{{.Architecture}}')" in aarch64|arm64) arch=arm64 ;; x86_64|amd64) arch=amd64 ;; *) echo "Unsupported Docker architecture" >&2; exit 1 ;; esac
CGO_ENABLED=0 GOOS=linux GOARCH="$arch" TMPDIR=/tmp go test -c -o "$run_dir/embedded.test" .
cat >"$run_dir/Dockerfile" <<'DOCKERFILE'
FROM mirror.gcr.io/library/alpine:3.19@sha256:6baf43584bcb78f2e5847d1de515f23499913ac9f12bdf834811a3145eb11ca1
COPY embedded.test /embedded.test
ENTRYPOINT ["/embedded.test"]
DOCKERFILE
docker build --platform "linux/$arch" -t "$image" "$run_dir"
image_built=1
# Includes TestEmbeddedObjectStorageGC* after the real GC engine is integrated.
pattern=${RHIZA_EMBEDDED_TEST_PATTERN:-'^TestEmbeddedObjectStorage'}
list_id=$(docker create --label rhiza.embedded-test="$run_name" --network none "$image" -test.list "$pattern")
containers="$containers $list_id"
docker start "$list_id" >/dev/null
if [ "$(docker wait "$list_id")" -ne 0 ]; then echo "Could not list selected tests" >&2; exit 1; fi
docker logs "$list_id" >"$run_dir/selected-tests.txt" 2>&1
if ! grep -E '^Test[[:alnum:]_]+$' "$run_dir/selected-tests.txt" >/dev/null; then
 echo "Test pattern selected no tests: $pattern" >&2
 exit 1
fi
cat "$run_dir/selected-tests.txt"
failed=0
for provider in s3 gcs azure; do
 case "$provider" in
  s3) emulator_image=floci/floci:2.3.0; port=4566 ;;
  gcs) emulator_image=floci/floci-gcp:0.11.0; port=4588 ;;
  azure) emulator_image=floci/floci-az:0.15.0; port=4577 ;;
 esac
 docker pull "$emulator_image"
 docker image inspect "$emulator_image" --format '{{json .RepoDigests}}' >"$run_dir/$provider-image-digests.json"
 cat "$run_dir/$provider-image-digests.json"
 network=$(docker network create --label rhiza.embedded-test="$run_name" "$run_name-$provider")
 networks="$networks $network"
 id=$(docker create --label rhiza.embedded-test="$run_name" --network "$network" --network-alias floci "$emulator_image")
 containers="$containers $id"
 docker start "$id" >/dev/null
 # No published host ports, mounted credentials, or host cloud configuration.
 ready_id=$(docker create --label rhiza.embedded-test="$run_name" --network "$network" --entrypoint /bin/sh "$image" -c "attempt=0; until nc -z -w 1 floci $port; do attempt=\$((attempt+1)); [ \"\$attempt\" -lt 60 ] || exit 1; sleep 1; done")
 containers="$containers $ready_id"
 docker start "$ready_id" >/dev/null
 if [ "$(docker wait "$ready_id")" -ne 0 ]; then echo "$provider emulator did not start" >&2; failed=1; continue; fi
 echo "Testing $provider at http://floci:$port ($emulator_image)"
 test_id=$(docker create --label rhiza.embedded-test="$run_name" --network "$network" \
  -e RHIZA_EMBEDDED_CONTAINER=1 -e "RHIZA_EMBEDDED_PROVIDER=$provider" -e "RHIZA_EMBEDDED_ENDPOINT=http://floci:$port" \
  "$image" -test.run "$pattern" -test.v -test.count=1 -test.timeout=8m)
 containers="$containers $test_id"
 docker start "$test_id" >/dev/null
 if [ "$(docker wait "$test_id")" -ne 0 ]; then failed=1; fi
 docker logs "$test_id" >"$run_dir/$provider-tests.log" 2>&1
 cat "$run_dir/$provider-tests.log"
 awk -v provider="$provider" '/^--- PASS:/{p++} /^--- FAIL:/{f++} /^--- SKIP:/{s++} END {printf "%s top-level tests: pass=%d fail=%d skip=%d\n", provider,p,f,s}' "$run_dir/$provider-tests.log"
done
if [ "$failed" -ne 0 ]; then exit 1; fi
result=0
