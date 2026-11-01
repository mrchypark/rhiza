#!/bin/sh
# CI image only. Do not run beside another process owning this Pod's WAL.
set -eu
if [ "${1-}" = serve-managed-sql ] && [ -e /var/lib/rhiza/repair.json ]; then
  RHIZA_FFI_IN_CLUSTER_CONTRACT=1 /usr/local/libexec/rhiza-managed-ffi-contract \
    -test.run='^TestManagedFFIInClusterSQLLifecycle$' -test.count=1 -test.v -test.timeout=120s
  /usr/local/libexec/rhiza-rust-managed-contract
fi
# The contract closes its real FFI Host before normal same-Pod adopted startup.
exec /usr/local/libexec/rhiza-ha-init "$@"
