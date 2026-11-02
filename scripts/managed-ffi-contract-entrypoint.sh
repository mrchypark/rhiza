#!/bin/sh
# CI image only. Do not run beside another process owning this Pod's WAL.
set -eu
umask 077
diag_dir=/var/lib/rhiza/contract-diagnostics
emit_first_contract() {
  if [ -f "$diag_dir/first.log" ]; then
    printf '%s\n' 'FIRST_CONTRACT_DIAGNOSTIC_BEGIN' >&2
    sed -n '1,200p' "$diag_dir/first.log" >&2
    for first_stage in go-initial rust-initial; do
      if [ -f "$diag_dir/$first_stage.exit" ]; then
        first_code=$(cat "$diag_dir/$first_stage.exit")
        case "$first_code" in ''|*[!0-9]*) first_code=unknown ;; esac
        printf 'FIRST_CONTRACT_EXIT stage=%s code=%.3s\n' "$first_stage" "$first_code" >&2
      fi
    done
    printf '%s\n' 'FIRST_CONTRACT_DIAGNOSTIC_END' >&2
  else
    printf '%s\n' 'FIRST_CONTRACT_DIAGNOSTIC_UNAVAILABLE' >&2
  fi
}
run_contract() {
  stage=$1
  shift
  case "$stage" in
    go-initial|rust-initial)
      [ ! -e "$diag_dir/$stage.exit" ] || { emit_first_contract; return 125; }
      printf 'FIRST_CONTRACT_STAGE %s started\n' "$stage" >> "$diag_dir/first.log"
      sync
      ;;
    go-restart|rust-restart) rm -f "$diag_dir/$stage.exit" ;;
    *) echo 'FAIL: invalid contract diagnostic stage' >&2; return 125 ;;
  esac
  # Drain all child output without SIGPIPE or limits on its WAL/SQLite files.
  # Persist only fixed markers, error classes and source line numbers—not raw
  # messages, profile data, SQL values, endpoints or credentials.
  (
    set +e
    "$@"
    code=$?
    printf '%s\n' "$code" > "$diag_dir/$stage.exit"
  ) 2>&1 | awk '
    function emit(s) { if (used+length(s)+1<=65536) { print s; used+=length(s)+1 } }
    /^=== RUN   TestManagedFFIInCluster(SQL|Mixed)Lifecycle$/ { emit($0); next }
    /^--- (PASS|FAIL): TestManagedFFIInCluster(SQL|Mixed)Lifecycle / { emit(substr($0,1,160)); next }
    /^[[:space:]]*managed_incluster_test.go:[0-9]+:/ {
      emit("GO_CONTRACT_SOURCE " $1)
      if (match($0,/code=[a-z_]+/)) emit(substr($0,RSTART,RLENGTH))
      if (index($0,"actual in-cluster Go FFI -> managed Host -> SQL ACK readback -> close passed")) emit("actual in-cluster Go FFI -> managed Host -> SQL ACK readback -> close passed")
      if (index($0,"actual in-cluster Go FFI -> mixed managed Host -> SQL KV ACK readback -> close passed")) emit("actual in-cluster Go FFI -> mixed managed Host -> SQL KV ACK readback -> close passed")
      next
    }
    /^FAIL: managed-ha-contract [a-z_]+$/ { emit(substr($0,1,160)); next }
    /^PASS: managed Rust FFI SQL contract$/ { emit($0); next }
    /^PASS: prior managed Rust ACK verified$/ { emit($0); next }
    /^PASS: managed Rust FFI mixed (initial|verify) contract$/ { emit($0); next }
    /^RUST_MIXED_ROW_ACK_SLOT=[0-9]+$/ { emit($0); next }
    /^RUST_MIXED_CURRENT_UID=[0-9a-fA-F-]+$/ { emit($0); next }
    /^RUST_MIXED_TUPLE=/ { if (length($0)<=16384) emit($0); next }
    /^panic: test timed out after [0-9hms.]+$/ { emit($0); next }
    /^(PASS|FAIL)$/ { emit($0) }
  ' > "$diag_dir/active.log"
  code=$(cat "$diag_dir/$stage.exit")
  case "$code" in ''|*[!0-9]*) echo 'FAIL: contract exit outcome unavailable' >&2; return 125 ;; esac
  case "$stage" in
    go-initial|rust-initial)
      remaining=32600
      awk -v remaining="$remaining" '{ if (remaining>0) { text=substr($0,1,remaining-1); print text; remaining-=length(text)+1 } }' "$diag_dir/active.log" >> "$diag_dir/first.log"
      ;;
  esac
  sync
  cat "$diag_dir/active.log"
  if [ "$code" != 0 ]; then emit_first_contract; return "$code"; fi
}
if [ "${1-}" = --diagnostic-self-test ]; then
  diag_test_dir=$(mktemp -d)
  trap 'rm -f "$diag_test_dir/first.log" "$diag_test_dir/active.log" "$diag_test_dir/go-initial.exit" "$diag_test_dir/rust-initial.exit" "$diag_test_dir/rust-restart.exit"; rmdir "$diag_test_dir"' EXIT HUP INT TERM
  diag_dir=$diag_test_dir
  : > "$diag_dir/first.log"
  if run_contract go-initial sh -c 'printf "%s\n" "private-profile-DO-NOT-RETAIN" "    managed_incluster_test.go:123: private-profile-DO-NOT-RETAIN"; exit 7'; then exit 1; else [ "$?" = 7 ]; fi
  grep -Fx 'GO_CONTRACT_SOURCE managed_incluster_test.go:123:' "$diag_dir/first.log" >/dev/null
  if grep -F 'DO-NOT-RETAIN' "$diag_dir/first.log"; then exit 1; fi
  before=$(cksum "$diag_dir/first.log")
  if run_contract rust-restart sh -c 'echo "FAIL: managed-ha-contract prior_ack_marker_missing"; exit 1'; then exit 1; fi
  [ "$before" = "$(cksum "$diag_dir/first.log")" ]
  [ "$(cat "$diag_dir/go-initial.exit")" = 7 ]
  if run_contract rust-initial sh -c 'i=0; while [ "$i" -lt 4000 ]; do echo "FAIL: managed-ha-contract prior_ack_marker_missing"; i=$((i+1)); done; exit 9' >/dev/null 2>&1; then exit 1; else [ "$?" = 9 ]; fi
  [ "$(wc -c < "$diag_dir/first.log")" -le 65536 ]
  [ "$(cat "$diag_dir/rust-initial.exit")" = 9 ]
  grep -Fx 'GO_CONTRACT_SOURCE managed_incluster_test.go:123:' "$diag_dir/first.log" >/dev/null
  exit 0
fi
if [ "${1-}" = serve-managed-sql ]; then
  marker_output=$(RHIZA_RUST_CONTRACT_MARKER=1 /usr/local/libexec/rhiza-managed-ffi-contract \
    -test.run='^TestRustContractRunStartedMarker$' -test.count=1 -test.v -test.timeout=10s)
  if printf '%s\n' "$marker_output" | grep -Fx 'RHIZA_RUST_FIXTURE_PROFILE=mixed' >/dev/null; then
    mixed_mode=$(printf '%s\n' "$marker_output" | sed -n 's/^RHIZA_RUST_CONTRACT_MODE=//p')
    producer_uid=$(printf '%s\n' "$marker_output" | sed -n 's/^RHIZA_RUST_CONTRACT_PRODUCER_UID=//p')
    printf '%s\n' "$producer_uid" | grep -Eq '^[0-9a-fA-F-]{16,64}$' || { echo 'FAIL: invalid mixed producer UID' >&2; exit 1; }
    export RHIZA_RUST_PRODUCER_UID="$producer_uid"
    case "$mixed_mode" in
      skip) ;;
      initial|verify)
        if [ -d "$diag_dir" ]; then
          [ "$mixed_mode" = verify ] || { emit_first_contract; exit 125; }
          go_stage=go-restart rust_stage=rust-restart
        else
          mkdir -m 700 "$diag_dir"
          (set -C; : > "$diag_dir/first.log")
          sync
          go_stage=go-initial rust_stage=rust-initial
        fi
        if [ -e /var/lib/rhiza/repair.json ]; then
          [ "$mixed_mode" = verify ] || { echo 'FAIL: replacement cannot run initial mixed writes' >&2; exit 1; }
          RHIZA_FFI_IN_CLUSTER_CONTRACT=1 run_contract "$go_stage" /usr/local/libexec/rhiza-managed-ffi-contract \
            -test.run='^TestManagedFFIInClusterMixedLifecycle$' -test.count=1 -test.v -test.timeout=120s
        fi
        run_contract "$rust_stage" /usr/local/libexec/rhiza-rust-managed-contract "--mixed-$mixed_mode"
        ;;
      *) echo 'FAIL: mixed contract phase unavailable' >&2; exit 1 ;;
    esac
  elif [ -e /var/lib/rhiza/repair.json ]; then
  case "$marker_output" in
    *RHIZA_RUST_CONTRACT_MODE=initial*) rust_initial=1 ;;
    *RHIZA_RUST_CONTRACT_MODE=restart*) rust_initial=0 ;;
    *) echo 'FAIL: Rust run-start marker result missing' >&2; exit 1 ;;
  esac
  if [ "$rust_initial" = 1 ]; then
    mkdir -m 700 "$diag_dir"
    (set -C; : > "$diag_dir/first.log")
    sync
  elif [ ! -d "$diag_dir" ]; then
    # Older runs may lack diagnostics; this is not initial-run permission.
    mkdir -m 700 "$diag_dir"
  fi
  if [ "$rust_initial" = 0 ]; then
    # A prior start with a lost ACK never authorizes another initial mutation.
    run_contract rust-restart /usr/local/libexec/rhiza-rust-managed-contract
  fi
  RHIZA_FFI_IN_CLUSTER_CONTRACT=1 run_contract "go-$([ "$rust_initial" = 1 ] && printf initial || printf restart)" /usr/local/libexec/rhiza-managed-ffi-contract \
    -test.run='^TestManagedFFIInClusterSQLLifecycle$' -test.count=1 -test.v -test.timeout=120s
  if [ "$rust_initial" = 1 ]; then
    run_contract rust-initial /usr/local/libexec/rhiza-rust-managed-contract --initial-run
  fi
  fi
fi
# The contract closes its real FFI Host before normal same-Pod adopted startup.
exec /usr/local/libexec/rhiza-ha-init "$@"
