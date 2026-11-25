#!/bin/sh
# Exercise the real pure evidence predicates without Docker, Go or cloud.
# The extracted real cleanup consumes these fixture variables and rm override.
# shellcheck disable=SC2034,SC2329
set -eu
repo=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
driver="$repo/scripts/test-embedded-object-providers.sh"
fixture="$repo/scripts/ha-managed-bootstrap-fixture.sh"
bucket="$repo/scripts/ha-managed-bucket-setup"
k8s="$repo/scripts/test-embedded-object-k8s.sh"
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
check_floci_ref() {
 if ! grep -F "$2" "$1" >/dev/null; then
  echo "missing pinned Floci reference in $1: $2" >&2
  exit 1
 fi
}
s3_ref='floci/floci:2.3.0@sha256:e0b292df4dc9559b7b1bf4f51bdda4bb8e269276236438cd03cae661260ea8a9'
gcs_ref='floci/floci-gcp:0.11.0@sha256:eae9683d806ceb643bcb36aee97cdb8cad13c3e2959cd053bf40e481994fde12'
azure_ref='floci/floci-az:0.15.0@sha256:530635d603223e120adbf9221a97560cffc6991d65fa0c72d0f79d1097974c89'
# Exact literal presence only; this does not exhaustively parse image references.
for ref in "$s3_ref" "$gcs_ref" "$azure_ref"; do
 check_floci_ref "$driver" "$ref"
 check_floci_ref "$fixture" "$ref"
 check_floci_ref "$bucket" "$ref"
done
check_floci_ref "$k8s" "$s3_ref"
eval "$(sed -n '/^ffi_verify_log() {/,/^}/p' "$driver")"
eval "$(sed -n '/^retention_verify_log() {/,/^}/p' "$driver")"
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
printf '%s\n' '--- PASS: TestManagedRetentionThreeProcessesRestart (1.00s)' > "$scratch/retention-log"
retention_verify_log "$scratch/retention-log"
for bad in missing skip fail duplicate; do
 case "$bad" in
 missing) : > "$scratch/retention-log" ;;
 skip) printf '%s\n' '--- SKIP: TestManagedRetentionThreeProcessesRestart (0.00s)' > "$scratch/retention-log" ;;
 fail) printf '%s\n' '--- FAIL: TestManagedRetentionThreeProcessesRestart (0.00s)' > "$scratch/retention-log" ;;
 duplicate) printf '%s\n' '--- PASS: TestManagedRetentionThreeProcessesRestart (1.00s)' '--- PASS: TestManagedRetentionThreeProcessesRestart (1.00s)' > "$scratch/retention-log" ;;
 esac
 if retention_verify_log "$scratch/retention-log"; then echo "accepted $bad retention evidence" >&2; exit 1; fi
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
# A failed or interrupted qualification still removes only its own anonymous
# volume after saving logs; failed evidence capture must preserve the container.
for scenario in failure interruption logfail mountfail; do
 case_dir="$scratch/retention-cleanup-$scenario"
 mkdir -p "$case_dir/owned"
 touch "$case_dir/container" "$case_dir/volume"
 if (
  containers=owned; networks=''; image_built=0; result=1
  retention_required=1; run_dir="$case_dir/owned"; ffi_evidence=''; ffi_child=''; run_name=mock
  docker() {
   case "$1 $2" in
   'inspect owned')
    [ -f "$case_dir/container" ] || return 1
    if [ "$#" -gt 2 ]; then [ "$scenario" != mountfail ] || return 1; printf '%s\n' anonymous; fi ;;
   'logs owned') [ "$scenario" != logfail ] || return 1; printf '%s\n' 'saved logs' ;;
   'rm -f') [ "$3" = -v ] && [ "$4" = owned ] || return 1; rm "$case_dir/container" "$case_dir/volume" ;;
   'volume inspect') [ -f "$case_dir/volume" ] ;;
   *) return 1 ;;
   esac
  }
  trap cleanup EXIT
  [ "$scenario" != interruption ] || exit 130
  exit 17
 ); then actual=0; else actual=$?; fi
 case "$scenario" in
 failure|interruption)
  [ ! -e "$case_dir/container" ] && [ ! -e "$case_dir/volume" ]
  grep -F 'saved logs' "$case_dir/owned/owned.log" >/dev/null
  grep -F 'anonymous removed; data not recoverable' "$case_dir/owned/owned-volume-cleanup.txt" >/dev/null ;;
 logfail|mountfail) [ -e "$case_dir/container" ] && [ -e "$case_dir/volume" ] ;;
 esac
 case "$scenario:$actual" in interruption:130|*:17) ;; *) exit 1 ;; esac
done
echo 'PASS: exact FFI evidence, owned Floci endpoint predicates, and expected Floci references present'
