#!/bin/sh
# Offline allowlist check for sanitized managed-cluster log capture.
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
sh scripts/test-ha-managed-client-job-test.sh
sh -n scripts/test-ha-managed-cluster-chaos.sh
capture=$(sed -n '/^capture_managed_logs() {/,/^}/p' scripts/test-ha-managed-cluster-chaos.sh)
[ -n "$capture" ]
scratch=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-chaos-log-capture.XXXXXX")
mkdir "$scratch/logs"
cleanup() {
  rm -f "$scratch/calls.txt" \
    "$scratch/logs/operator-operator-pod-operator-current.log" \
    "$scratch/logs/operator-operator-pod-operator-previous.log" \
    "$scratch/logs/managed-sql-pod-sql-current.log" \
    "$scratch/logs/managed-sql-pod-sql-previous.log" \
    "$scratch/logs/managed-sql-pod-rhiza-prepare-identity-current.log" \
    "$scratch/logs/managed-sql-pod-rhiza-prepare-identity-previous.log" \
    "$scratch/logs/managed-sql-pod-rhiza-await-bootstrap-plan-current.log" \
    "$scratch/logs/managed-sql-pod-rhiza-await-bootstrap-plan-previous.log"
  rmdir "$scratch/logs" "$scratch"
}
trap cleanup EXIT HUP INT TERM

sh -c '
  set -eu
  logs=$1
  namespace=managed-bootstrap
  uid=cluster-uid
  calls="$logs/../calls.txt"
  k() {
    [ "$1" = -n ] && [ "$2" = "$namespace" ] || { echo "bad namespace selector" >&2; return 2; }
    case "$3" in
      get)
        [ "$4" = pods ] && [ "$5" = -l ] && [ "$7" = -o ] && [ "$8" = json ] && [ "$#" -eq 8 ] || { echo "unexpected Pod query" >&2; return 2; }
        printf "get %s\n" "$6" >> "$calls"
        case "$6" in
          app.kubernetes.io/name=rhiza-operator)
            printf "%s\n" '\''{"items":[{"metadata":{"name":"operator-pod"},"spec":{"containers":[{"name":"operator","env":[{"name":"PROFILE","value":"NO_PRIVATE_PROFILE_LOG"}]},{"name":"sidecar-secret"}]}}]}'\''
            ;;
          rhiza.mrchypark.dev/cluster-uid=cluster-uid)
            printf "%s\n" '\''{"items":[{"metadata":{"name":"sql-pod"},"spec":{"initContainers":[{"name":"rhiza-prepare-identity"},{"name":"rhiza-await-bootstrap-plan"},{"name":"secret-init"}],"containers":[{"name":"sql"},{"name":"secret-sidecar"}],"env":[{"name":"TOKEN","value":"NO_PRIVATE_TOKEN_LOG"}]}}]}'\''
            ;;
          *) echo "unapproved selector: $6" >&2; return 2 ;;
        esac
        ;;
      logs)
        [ "$5" = -c ] && [ "$7" = --tail=200 ] && [ "$8" = --limit-bytes=65536 ] || { echo "unbounded logs request" >&2; return 2; }
        case "$#:${9-}" in
          8:) version=current ;;
          9:--previous) version=previous ;;
          *) echo "unexpected logs flags" >&2; return 2 ;;
        esac
        case "$4/$6" in
          operator-pod/operator|sql-pod/sql|sql-pod/rhiza-prepare-identity|sql-pod/rhiza-await-bootstrap-plan) ;;
          *) echo "unapproved container log request: $4/$6" >&2; return 2 ;;
        esac
        printf "logs %s %s %s\n" "$4" "$6" "$version" >> "$calls"
        printf "CAPTURED %s %s %s\n" "$4" "$6" "$version"
        ;;
      *) echo "unapproved kubectl operation: $3" >&2; return 2 ;;
    esac
  }
  eval "$2"
  capture_managed_logs
' fixture "$scratch/logs" "$capture"

[ "$(wc -l < "$scratch/calls.txt" | tr -d ' ')" -eq 10 ]
[ "$(grep -c '^get ' "$scratch/calls.txt")" -eq 2 ]
[ "$(grep -c '^logs ' "$scratch/calls.txt")" -eq 8 ]
grep -Fx 'get app.kubernetes.io/name=rhiza-operator' "$scratch/calls.txt" >/dev/null
grep -Fx 'get rhiza.mrchypark.dev/cluster-uid=cluster-uid' "$scratch/calls.txt" >/dev/null
grep -F 'CAPTURED operator-pod operator current' "$scratch/logs/operator-operator-pod-operator-current.log" >/dev/null
grep -F 'CAPTURED sql-pod rhiza-prepare-identity previous' "$scratch/logs/managed-sql-pod-rhiza-prepare-identity-previous.log" >/dev/null
grep -F 'CAPTURED sql-pod rhiza-await-bootstrap-plan current' "$scratch/logs/managed-sql-pod-rhiza-await-bootstrap-plan-current.log" >/dev/null
grep -F 'CAPTURED sql-pod sql previous' "$scratch/logs/managed-sql-pod-sql-previous.log" >/dev/null
if grep -R -E 'NO_PRIVATE_(PROFILE|TOKEN)_LOG|sidecar-secret|secret-init' "$scratch/logs"; then
  echo 'private Pod spec or non-allowlisted container leaked into captured logs' >&2
  exit 1
fi
printf '%s\n' 'PASS: only selected operator/init/sql current+previous logs are captured with bounded flags; Pod specs and secret containers are not emitted'
