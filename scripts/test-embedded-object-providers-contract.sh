#!/bin/sh
# Exercise the real pure evidence predicates without Docker, Go or cloud.
# The extracted real cleanup consumes these fixture variables and rm override.
# shellcheck disable=SC2034,SC2329
set -eu
repo=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
driver="$repo/scripts/test-embedded-object-providers.sh"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
eval "$(sed -n '/^ffi_verify_log() {/,/^}/p' "$driver")"
eval "$(sed -n '/^ffi_owned_endpoint() {/,/^}/p' "$driver")"
eval "$(sed -n '/^ffi_wait() {/,/^}/p' "$driver")"
eval "$(sed -n '/^cleanup() {/,/^}/p' "$driver")"
printf '%s\n' '--- PASS: TestNotificationFFIThreeNode (1.00s)' '--- PASS: TestManagedStartupBudgetKeepsRealCorePeerUntilClose (0.10s)' > "$scratch/log"
ffi_verify_log "$scratch/log"
for bad in missing skip fail duplicate; do
 case "$bad" in
 missing) printf '%s\n' '--- PASS: TestNotificationFFIThreeNode (1.00s)' > "$scratch/log" ;;
 skip) printf '%s\n' '--- SKIP: TestNotificationFFIThreeNode (0.00s)' > "$scratch/log" ;;
 fail) printf '%s\n' '--- FAIL: TestNotificationFFIThreeNode (0.00s)' > "$scratch/log" ;;
 duplicate) printf '%s\n' '--- PASS: TestNotificationFFIThreeNode (1.00s)' '--- PASS: TestNotificationFFIThreeNode (1.00s)' '--- PASS: TestManagedStartupBudgetKeepsRealCorePeerUntilClose (0.10s)' > "$scratch/log" ;;
 esac
 if ffi_verify_log "$scratch/log"; then echo "accepted $bad evidence" >&2; exit 1; fi
done
printf '%s\n' '[{"Id":"container","State":{"Running":true},"Config":{"Labels":{"rhiza.embedded-test":"owner"}},"NetworkSettings":{"Networks":{"owned-s3":{"NetworkID":"network","IPAddress":"172.18.0.2"}}}}]' > "$scratch/inspect.json"
[ "$(ffi_owned_endpoint "$scratch/inspect.json" container network owned-s3 owner)" = 172.18.0.2:4566 ]
for mutation in '.Id="foreign"' '.State.Running=false' '.Config.Labels["rhiza.embedded-test"]="foreign"' '.NetworkSettings.Networks["owned-s3"].NetworkID="foreign"' '.NetworkSettings.Networks["owned-s3"].IPAddress="8.8.8.8"' '.NetworkSettings.Networks["owned-s3"].IPAddress="172.999.0.2"'; do
 jq ".[0] |= ($mutation)" "$scratch/inspect.json" > "$scratch/bad.json"
 if ffi_owned_endpoint "$scratch/bad.json" container network owned-s3 owner >/dev/null; then echo 'accepted unowned endpoint' >&2; exit 1; fi
done
grep -F 'CGO_ENABLED=1 go test -c' "$driver" >/dev/null
grep -F 'env -i PATH=' "$driver" >/dev/null
# shellcheck disable=SC2016
grep -F 'printf '\''%s\n'\'' "$ffi_status"' "$driver" >/dev/null
for child_code in 0 17; do
 for cleanup_error in 0 1; do
  case_dir="$scratch/child-$child_code-cleanup-$cleanup_error"
  mkdir -p "$case_dir/owned" "$case_dir/evidence"
  if (
   containers=''; networks=''; image_built=0; result=0
   run_dir="$case_dir/owned"; ffi_evidence="$case_dir/evidence"; run_name=mock
   ffi_child=''
   if [ "$cleanup_error" -eq 1 ]; then rm() { return 1; }; fi
   trap cleanup EXIT
   sh -c 'exit "$1"' sh "$child_code" &
   ffi_child=$!
   if ffi_wait; then exit 0; else status=$?; exit "$status"; fi
  ); then actual=0; else actual=$?; fi
  expected=$child_code
  if [ "$expected" -eq 0 ] && [ "$cleanup_error" -eq 1 ]; then expected=1; fi
  [ "$actual" -eq "$expected" ]
  grep -Fx "original_exit=$child_code" "$case_dir/evidence/cleanup.txt" >/dev/null
  grep -Fx "cleanup_failed=$cleanup_error" "$case_dir/evidence/cleanup.txt" >/dev/null
  grep -Fx "final_exit=$expected" "$case_dir/evidence/cleanup.txt" >/dev/null
 done
done
echo 'PASS: exact FFI evidence and owned Floci endpoint predicates'
