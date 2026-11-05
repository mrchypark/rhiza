#!/bin/sh
# Manual experiment only. Cargo/release CI remains authoritative.
set -eu

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d ' ' -f 1
  else shasum -a 256 "$1" | cut -d ' ' -f 1; fi
}
short_home_valid() {
  case "$1" in /tmp/rhiza-blaze.*) [ "${#1}" -le 60 ] ;; *) return 1 ;; esac
}
owned_root_valid() {
  if [ -f "$1/.rhiza-blaze-owner" ]; then [ "$(cat "$1/.rhiza-blaze-owner")" = "$2" ]
  else [ ! -e "$1/state" ]; fi
}
prebuilt_valid() {
  [ -f "$1/librhiza_ffi.a" ] && [ -f "$1/source-commit" ] && [ -f "$1/archive-sha256" ] &&
    [ "$(cat "$1/source-commit")" = "$2" ] && [ "$(cat "$1/archive-sha256")" = "$(sha256 "$1/librhiza_ffi.a")" ]
}
release_paths_valid() {
  awk -v prefix="$1/" 'index($0,prefix)!=1 || $0 ~ /(^|\/)\.\.(\/|$)/ {bad=1} END {exit bad}'
}
preserve_daemon_logs() {
  daemon_home=$1
  log_dir=$2
  capture=$3
  for daemon_log in v2/serve.log daemon.log; do
    if [ -f "$daemon_home/$daemon_log" ]; then
      log_name=$(printf '%s' "$daemon_log" | tr / -)
      tail -c 262144 "$daemon_home/$daemon_log" > "$log_dir/$capture-$log_name" || return 1
    fi
  done
}
record_invocation() {
  invocation_log=$1
  shift
  {
    printf 'cwd=%s\n' "$PWD"
    argument=0
    for value do
      printf 'client_argv[%s]=%s\n' "$argument" "$value"
      argument=$((argument + 1))
    done
    printf '%s\n' 'daemon_spawn_argv=not_observed; inspect captured v2-serve.log (no environment dump)'
  } > "$invocation_log"
}
if [ "${1-}" = --self-test ]; then
  test_root=$(mktemp -d /tmp/rhiza-blaze-check.XXXXXX)
  trap 'rm -rf "$test_root"' EXIT HUP INT TERM
  short_home_valid /tmp/rhiza-blaze.123456
  if short_home_valid /tmp/rhiza-blaze.abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz; then exit 1; fi
  mkdir "$test_root/state"
  if owned_root_valid "$test_root" source; then exit 1; fi
  printf '%s\n' source > "$test_root/.rhiza-blaze-owner"
  owned_root_valid "$test_root" source
  if owned_root_valid "$test_root" other; then exit 1; fi
  printf '%s\n' archive > "$test_root/librhiza_ffi.a"
  printf '%s\n' source > "$test_root/source-commit"
  sha256 "$test_root/librhiza_ffi.a" > "$test_root/archive-sha256"
  prebuilt_valid "$test_root" source
  if prebuilt_valid "$test_root" other; then exit 1; fi
  printf '%s\n' changed >> "$test_root/librhiza_ffi.a"
  if prebuilt_valid "$test_root" source; then exit 1; fi
  printf '%s\n' release/bin | release_paths_valid release
  if printf '%s\n' other/bin | release_paths_valid release; then exit 1; fi
  if printf '%s\n' release/../bin | release_paths_valid release; then exit 1; fi
  mkdir "$test_root/deny-go"
  ln -s /usr/bin/false "$test_root/deny-go/go"
  if PATH="$test_root/deny-go:$PATH" go version; then exit 1; fi
  mkdir -p "$test_root/daemon/v2" "$test_root/logs"
  printf '%s\n' 'known-daemon-startup-failure' > "$test_root/daemon/v2/serve.log"
  preserve_daemon_logs "$test_root/daemon" "$test_root/logs" before-stop
  rm -r "$test_root/daemon"
  grep -Fx 'known-daemon-startup-failure' "$test_root/logs/before-stop-v2-serve.log" >/dev/null
  record_invocation "$test_root/logs/invocation.log" blaze build --locked --example embedded
  grep -Fx 'client_argv[4]=embedded' "$test_root/logs/invocation.log" >/dev/null
  echo 'Blaze script safeguards self-test passed (no Rust/Go build).'
  exit 0
fi

mode=${1:-probe}
case "$mode" in probe|verify) ;; *) echo 'usage: sh scripts/test-blaze-rust-compat.sh probe|verify' >&2; exit 2 ;; esac
: "${BLAZE_COMPAT_ROOT:?set an absolute, dedicated experiment directory}"
case "$BLAZE_COMPAT_ROOT" in /*) ;; *) echo 'BLAZE_COMPAT_ROOT must be absolute' >&2; exit 2 ;; esac
[ "$BLAZE_COMPAT_ROOT" != / ] || exit 2
mkdir -p "$BLAZE_COMPAT_ROOT"
task_root=$(CDPATH= cd -- "$BLAZE_COMPAT_ROOT" && pwd -P)
repo=$(CDPATH= cd -- "${RHIZA_BLAZE_SOURCE_DIR:-$(dirname -- "$0")/..}" && pwd -P)
case "$task_root" in /|/tmp|/private/tmp|/Users|"$repo"|"$HOME") echo 'experiment directory is too broad' >&2; exit 2 ;; esac
owner="$task_root/.rhiza-blaze-owner"
owned_root_valid "$task_root" "$repo" || { echo 'experiment state ownership differs; choose a fresh directory' >&2; exit 2; }
if [ ! -f "$owner" ]; then
  printf '%s\n' "$repo" > "$owner"
fi
mkdir -p "$task_root/logs"
cargo_command=$(command -v cargo)
release=v0.20261105.1
nightly=nightly-2026-10-27
case "$(uname -s):$(uname -m)" in
  Darwin:arm64)
    target=aarch64-apple-darwin
    archive_sha=2c6fb64b14668cb73c1a4531bd4f8b77665d2abe0e365d5ad76aaa77a3001f3e
    ;;
  Linux:x86_64)
    target=x86_64-unknown-linux-gnu
    archive_sha=aaf09e38f7cd9e873c84d4fa6afe8c26f713cb558e2e9dfc1a599af31b15ad8b
    ;;
  *) echo 'experiment supports only macOS arm64 and Linux x86_64 GNU hosts' >&2; exit 2 ;;
esac
archive="$task_root/blaze-$release-$target.tar.gz"
if [ ! -f "$archive" ]; then
  curl --proto '=https' --tlsv1.2 -fsSL --max-time 180 \
    "https://github.com/indexable-inc/blaze/releases/download/$release/blaze-$release-$target.tar.gz" -o "$archive"
fi
[ "$(sha256 "$archive")" = "$archive_sha" ] || { echo 'Blaze archive digest mismatch' >&2; exit 1; }
# Never run the installer: it can replace global Cargo launchers/configuration.
# A pinned archive is inspected before extraction into this owned directory.
tar -tzf "$archive" | release_paths_valid "blaze-$release-$target"
tar -xzf "$archive" -C "$task_root"
bin="$task_root/blaze-$release-$target"
# The release archive pins these extracted bytes; record the member digest too.
binary_sha=$(sha256 "$bin/blaze")
export BLAZE_HOME
BLAZE_HOME=$(mktemp -d /tmp/rhiza-blaze.XXXXXX)
short_home_valid "$BLAZE_HOME" || exit 2
printf '%s\n' "$repo" > "$BLAZE_HOME/.rhiza-blaze-owner"
blaze_started=0
cleanup() {
  result=$?
  trap - EXIT HUP INT TERM
  cleanup_exit=0
  evidence_saved=1
  if ! preserve_daemon_logs "$BLAZE_HOME" "$task_root/logs" before-stop; then
    evidence_saved=0
    echo 'daemon evidence capture failed; preserving owned home' >&2
    [ "$result" -ne 0 ] || result=1
  fi
  if [ "$blaze_started" = 1 ]; then
    if "$bin/blaze" stop > "$task_root/logs/cleanup.log" 2>&1; then :; else cleanup_exit=$?; fi
  fi
  printf 'cleanup_exit=%s\n' "$cleanup_exit" >> "$task_root/results.txt"
  if [ "$cleanup_exit" -ne 0 ]; then
    echo "owned Blaze cleanup failed; preserved $BLAZE_HOME" >&2
    [ "$result" -ne 0 ] || result=1
  elif [ "$evidence_saved" = 0 ]; then
    echo "preserved daemon evidence at $BLAZE_HOME" >&2
  elif short_home_valid "$BLAZE_HOME" && [ "$(cat "$BLAZE_HOME/.rhiza-blaze-owner")" = "$repo" ]; then
    rm -rf "$BLAZE_HOME"
    if [ -e "$BLAZE_HOME" ]; then [ "$result" -ne 0 ] || result=1; fi
  else
    echo 'Blaze home ownership changed; cleanup refused' >&2
    [ "$result" -ne 0 ] || result=1
  fi
  if [ -n "${lock_before-}" ] && [ "$(sha256 "$repo/sdk/rust/Cargo.lock")" != "$lock_before" ]; then
    echo 'Cargo.lock changed' >&2
    [ "$result" -ne 0 ] || result=1
  fi
  if [ -n "${native_before-}" ] && [ "$(sha256 "$native/librhiza_ffi.a")" != "$native_before" ]; then
    echo 'native archive changed' >&2
    [ "$result" -ne 0 ] || result=1
  fi
  printf 'final_exit=%s\n' "$result" >> "$task_root/results.txt"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
export CARGO_TARGET_DIR="$task_root/target-blaze"
# No enable/disable, rustup default, Cargo alias, or shared daemon manipulation.
PATH="$bin:$PATH"
export PATH
"$bin/blaze" --version
"$bin/blaze" toolchain --json > "$task_root/logs/toolchain.json"
jq -e --arg nightly "$nightly" --arg target "$target" '.nightly==$nightly and .host==$target and (.sysroot|type=="string")' "$task_root/logs/toolchain.json" >/dev/null
toolchain=$(jq -r '.sysroot' "$task_root/logs/toolchain.json")
case "$toolchain" in /*) ;; *) echo 'unexpected Blaze sysroot' >&2; exit 1 ;; esac
[ -d "$toolchain" ] || { echo 'Blaze sysroot does not exist' >&2; exit 1; }
[ "$mode" = verify ] || exit 0

: "${RHIZA_NATIVE_LIB_DIR:?build an exact-source archive first}"
native=$(CDPATH= cd -- "$RHIZA_NATIVE_LIB_DIR" && pwd -P)
prebuilt_valid "$native" "$(git -C "$repo" rev-parse HEAD)" || { echo 'exact-source prebuilt native archive validation failed' >&2; exit 1; }
export RHIZA_NATIVE_LIB_DIR="$native"
git -C "$repo" diff --quiet HEAD -- sdk/rust '*.go' cmd/rhiza-ffi pkg internal go.mod go.sum || {
  echo 'native/Rust inputs are dirty; build and verify a pinned source checkout' >&2; exit 1;
}
untracked_inputs=$(git -C "$repo" ls-files --others --exclude-standard -- sdk/rust '*.go' cmd/rhiza-ffi pkg internal)
[ -z "$untracked_inputs" ] || { echo 'native/Rust inputs contain untracked files' >&2; exit 1; }
lock_before=$(sha256 "$repo/sdk/rust/Cargo.lock")
native_before=$(sha256 "$native/librhiza_ffi.a")
printf 'source=%s\nrelease=%s\ntoolchain=%s\ntarget=%s\nnative_archive=%s\nblaze_binary=%s\n' \
  "$(git -C "$repo" rev-parse HEAD)" "$release" "$nightly" "$target" \
  "$(sha256 "$native/librhiza_ffi.a")" "$binary_sha" > "$task_root/results.txt"
run_phase() {
  phase=$1
  shift
  record_invocation "$task_root/logs/$phase-invocation.log" "$@"
  if "$@" > "$task_root/logs/$phase.log" 2>&1; then phase_exit=0; else phase_exit=$?; fi
  printf '%s_exit=%s\n' "$phase" "$phase_exit" >> "$task_root/results.txt"
  if [ "$phase_exit" -ne 0 ]; then
    if ! preserve_daemon_logs "$BLAZE_HOME" "$task_root/logs" "$phase-failed"; then
      echo 'failed to preserve daemon log; cleanup will retain owned home if capture remains unavailable' >&2
    fi
  fi
  tail -n 80 "$task_root/logs/$phase.log"
  return "$phase_exit"
}
cd "$repo/sdk/rust"
# Reuse the exact same native archive; these commands must not invoke Go.
[ "$("$cargo_command" --version | awk '{print $2}')" = 1.98.0 ] || { echo 'Cargo baseline must be 1.98.0' >&2; exit 1; }
run_phase cargo-baseline env CARGO_TARGET_DIR="$task_root/target-cargo" "$cargo_command" test --locked --lib
mkdir "$BLAZE_HOME/deny-go"
ln -s /usr/bin/false "$BLAZE_HOME/deny-go/go"
PATH="$BLAZE_HOME/deny-go:$PATH"
export PATH
blaze_started=1
run_phase blaze-build "$bin/blaze" build --locked --example embedded
run_phase blaze-test "$bin/blaze" test --locked --lib
run_phase blaze-status "$bin/blaze" status
echo 'Cargo baseline and actual Blaze CLI checks passed; backend/fallback status is recorded separately. No performance or provider qualification claimed.'
