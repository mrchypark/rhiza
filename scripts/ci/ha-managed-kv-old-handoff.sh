#!/bin/sh
set -eu
umask 077

# Actual nested baseline compilation belongs to GitHub Actions, not local tests.
[ "${GITHUB_ACTIONS:-}" = true ] || { echo 'old-loader qualification requires GitHub Actions' >&2; exit 1; }
: "${RUNNER_TEMP:?}"
repo=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
src="$repo/scripts/ha-managed-kv-old-handoff-src"
baseline=04a6006e9c29cf9a652397f209896e79984ce344
logs="$RUNNER_TEMP/ha-v1alpha2-contract/old-managed-handoff"
mkdir -p "$logs"
scratch=$(mktemp -d "$RUNNER_TEMP/ha-managed-old-handoff.XXXXXXXX")
child=''
cleanup() {
  result=$?
  trap - EXIT HUP INT TERM
  printf '%s\n' "$result" > "$logs/original-exit.txt"
  if [ -n "$child" ]; then
    kill "$child" 2>/dev/null || :
    wait "$child" 2>/dev/null || :
    if kill -0 "$child" 2>/dev/null; then echo 'owned qualification process remains' >&2; result=1; fi
  fi
  case "$scratch" in "$RUNNER_TEMP"/ha-managed-old-handoff.*) rm -rf -- "$scratch";; *) echo 'refusing unowned cleanup' >&2; result=1;; esac
  if [ -e "$scratch" ]; then echo 'old-loader private scratch remains' >&2; result=1; fi
  printf '%s\n' "$result" > "$logs/final-exit.txt"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
wait_owned() {
  if wait "$child"; then child=''; return 0; else child_status=$?; child=''; return "$child_status"; fi
}
git -C "$repo" cat-file -e "$baseline^{commit}"
{
  printf 'candidate=%s\n' "$(git -C "$repo" rev-parse HEAD)"
  printf 'baseline=%s\n' "$baseline"
  printf 'baseline_tree=%s\n' "$(git -C "$repo" rev-parse "$baseline^{tree}")"
  go version
} > "$logs/source.txt"
mkdir "$scratch/baseline" "$scratch/helper"
git -C "$repo" archive "$baseline" > "$scratch/baseline.tar"
tar -xf "$scratch/baseline.tar" -C "$scratch/baseline"
cp "$src/go.mod" "$src/go.sum" "$src/main.go" "$scratch/helper/"
digest=$(cd "$src" && shasum -a 256 go.mod go.sum main.go | shasum -a 256 | awk '{print $1}')
binary="$scratch/helper/old-handoff-$digest"
(
  cd "$scratch/helper"
  go mod edit -replace "github.com/mrchypark/rhiza=$scratch/baseline"
  exec go build -mod=readonly -o "$binary" .
) > "$logs/build.txt" 2>&1 &
child=$!
wait_owned || { result=$?; cat "$logs/build.txt" >&2; exit "$result"; }
go version -m "$binary" > "$logs/build-info.txt"
grep -F "$scratch/baseline" "$logs/build-info.txt" >/dev/null
shasum -a 256 "$binary" > "$logs/binary-sha256.txt"
(
  cd "$repo"
  export RHIZA_MANAGED_KV_OLD_HANDOFF_FIXTURE="$scratch/fixture"
  exec go test ./pkg/recovery -run '^TestManagedRecoveryOldHandoffFixtureExport$' -count=1 -timeout=60s -v
) > "$logs/fixture.txt" 2>&1 &
child=$!
wait_owned || { result=$?; cat "$logs/fixture.txt" >&2; exit "$result"; }
"$binary" "$scratch/fixture" > "$logs/result.json" 2> "$logs/loader.txt" &
child=$!
wait_owned || { result=$?; cat "$logs/loader.txt" >&2; exit "$result"; }
cat "$logs/result.json"
