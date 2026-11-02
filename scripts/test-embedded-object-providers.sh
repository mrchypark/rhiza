#!/bin/sh
# Run the same Go API contract against three pinned, isolated emulators.
set -eu

ffi_verify_log() {
 awk '
 /^--- PASS: TestNotificationFFIThreeNode \(/ {s3++}
 /^--- PASS: TestManagedStartupBudgetKeepsRealCorePeerUntilClose \(/ {core++}
 /^--- (SKIP|FAIL):/ {bad=1}
 END {exit !(s3==1 && core==1 && !bad)}' "$1"
}

ffi_owned_endpoint() {
 jq -er --arg id "$2" --arg network "$3" --arg name "$4" --arg owner "$5" '
 .[0] | select(.Id==$id and .State.Running==true and .Config.Labels["rhiza.embedded-test"]==$owner)
 | .NetworkSettings.Networks[$name] | select(.NetworkID==$network) | .IPAddress
 | select(test("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$"))
 | . as $ip | split(".") | map(tonumber)
 | select(all(.[]; .>=0 and .<=255))
 | select(.[0]==10 or (.[0]==172 and .[1]>=16 and .[1]<=31) or (.[0]==192 and .[1]==168))
 | $ip+":4566"' "$1"
}

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
ffi_required=${RHIZA_EMBEDDED_FFI_REQUIRED:-0}
ffi_child=''
ffi_evidence=''
ffi_wait() {
 if wait "$ffi_child"; then ffi_child=''; return 0; else status=$?; ffi_child=''; return "$status"; fi
}
cleanup() {
 original_exit=$?
 trap - EXIT HUP INT TERM
 cleanup_failed=0
 if [ -n "$ffi_child" ]; then
  kill "$ffi_child" 2>/dev/null || :
  wait "$ffi_child" 2>/dev/null || :
  if kill -0 "$ffi_child" 2>/dev/null; then echo 'Owned FFI process remains' >&2; cleanup_failed=1; fi
 fi
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
  if ! rm -rf "$run_dir"; then cleanup_failed=1; fi
  if [ -d "$run_dir" ]; then echo "Owned run directory remains: $run_dir" >&2; cleanup_failed=1; fi
 else echo "Provider diagnostics preserved: $run_dir" >&2; fi
 if [ "$cleanup_failed" -eq 0 ]; then echo "Cleanup verified: owned containers, networks, and test image absent ($run_name)"; fi
 final_exit=$original_exit
 if [ "$cleanup_failed" -ne 0 ] && [ "$final_exit" -eq 0 ]; then final_exit=1; fi
 if [ -n "$ffi_evidence" ]; then
  printf 'original_exit=%s\ncleanup_failed=%s\nfinal_exit=%s\n' "$original_exit" "$cleanup_failed" "$final_exit" > "$ffi_evidence/cleanup.txt" || final_exit=1
 fi
 exit "$final_exit"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
case "$ffi_required" in 0|1) ;; *) echo 'invalid FFI qualification mode' >&2; exit 1 ;; esac
case "$(docker info --format '{{.Architecture}}')" in aarch64|arm64) arch=arm64 ;; x86_64|amd64) arch=amd64 ;; *) echo "Unsupported Docker architecture" >&2; exit 1 ;; esac
ffi_pattern='^(TestNotificationFFIThreeNode|TestManagedStartupBudgetKeepsRealCorePeerUntilClose)$'
if [ "$ffi_required" -eq 1 ]; then
 [ "${GITHUB_ACTIONS:-}" = true ] && [ "$(uname -s)" = Linux ] || { echo 'required S3 FFI qualification needs Linux CI' >&2; exit 1; }
 : "${RUNNER_TEMP:?}"
 ffi_evidence=$(mktemp -d "$RUNNER_TEMP/rhiza-embedded-ffi.XXXXXX")
 CGO_ENABLED=1 go test -c -o "$run_dir/ffi.test" ./cmd/rhiza-ffi > "$ffi_evidence/build.log" 2>&1 &
 ffi_child=$!
 ffi_wait || { status=$?; cat "$ffi_evidence/build.log" >&2; exit "$status"; }
 "$run_dir/ffi.test" -test.list "$ffi_pattern" > "$ffi_evidence/selected-tests.txt"
 awk '/^TestNotificationFFIThreeNode$/ {s3++} /^TestManagedStartupBudgetKeepsRealCorePeerUntilClose$/ {core++} END {exit !(s3==1 && core==1)}' "$ffi_evidence/selected-tests.txt"
 { git rev-parse HEAD; go version; printf 'CGO_ENABLED=1\n'; shasum -a 256 "$run_dir/ffi.test"; } > "$ffi_evidence/source.txt"
fi
CGO_ENABLED=0 GOOS=linux GOARCH="$arch" TMPDIR=/tmp go test -c -o "$run_dir/embedded.test" .
cat >"$run_dir/Dockerfile" <<'DOCKERFILE'
FROM mirror.gcr.io/library/alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
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
 if [ "$provider" = s3 ] && [ "$ffi_required" -eq 1 ]; then
  docker inspect "$id" > "$ffi_evidence/owned-s3.json"
  ffi_endpoint=$(ffi_owned_endpoint "$ffi_evidence/owned-s3.json" "$id" "$network" "$run_name-s3" "$run_name") || { echo 'owned Floci endpoint could not be verified' >&2; exit 1; }
  cp "$run_dir/s3-image-digests.json" "$ffi_evidence/s3-image-digests.json"
  env -i PATH="$PATH" TMPDIR="$run_dir" GITHUB_ACTIONS=true RHIZA_EMBEDDED_FFI_REQUIRED=1 \
   RHIZA_E2E_S3_ENDPOINT="$ffi_endpoint" RHIZA_E2E_S3_BUCKET="rhiza-ffi-$(date +%s)-$$" \
   RHIZA_E2E_S3_ACCESS_KEY=rhiza-local-test RHIZA_E2E_S3_SECRET_KEY=rhiza-local-test \
   AWS_EC2_METADATA_DISABLED=true \
   "$run_dir/ffi.test" -test.run "$ffi_pattern" -test.v -test.count=1 -test.timeout=60s > "$ffi_evidence/tests.log" 2>&1 &
  ffi_child=$!
  if ffi_wait; then ffi_status=0; else ffi_status=$?; fi
  printf '%s\n' "$ffi_status" > "$ffi_evidence/test-exit.txt"
  cat "$ffi_evidence/tests.log"
  [ "$ffi_status" -eq 0 ] || exit "$ffi_status"
  ffi_verify_log "$ffi_evidence/tests.log" || { echo 'required FFI tests did not both PASS without SKIP' >&2; exit 1; }
  printf 'PASS TestNotificationFFIThreeNode\nPASS TestManagedStartupBudgetKeepsRealCorePeerUntilClose\n' > "$ffi_evidence/qualification.txt"
 fi
done
if [ "$failed" -ne 0 ]; then exit 1; fi
result=0
