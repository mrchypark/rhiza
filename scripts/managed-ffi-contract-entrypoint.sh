#!/bin/sh
# CI image only. Do not run beside another process owning this Pod's WAL.
set -eu
if [ "${1-}" = serve-managed-sql ] && [ -e /var/lib/rhiza/repair.json ]; then
  marker_output=$(RHIZA_RUST_CONTRACT_MARKER=1 /usr/local/libexec/rhiza-managed-ffi-contract \
    -test.run='^TestRustContractRunStartedMarker$' -test.count=1 -test.v -test.timeout=10s)
  case "$marker_output" in
    *RHIZA_RUST_CONTRACT_MODE=initial*) rust_initial=1 ;;
    *RHIZA_RUST_CONTRACT_MODE=restart*) rust_initial=0 ;;
    *) echo 'FAIL: Rust run-start marker result missing' >&2; exit 1 ;;
  esac
  if [ "$rust_initial" = 0 ]; then
    # A prior start with a lost ACK never authorizes another initial mutation.
    /usr/local/libexec/rhiza-rust-managed-contract
  fi
  RHIZA_FFI_IN_CLUSTER_CONTRACT=1 /usr/local/libexec/rhiza-managed-ffi-contract \
    -test.run='^TestManagedFFIInClusterSQLLifecycle$' -test.count=1 -test.v -test.timeout=120s
  if [ "$rust_initial" = 1 ]; then
    /usr/local/libexec/rhiza-rust-managed-contract --initial-run
  fi
fi
# The contract closes its real FFI Host before normal same-Pod adopted startup.
exec /usr/local/libexec/rhiza-ha-init "$@"
