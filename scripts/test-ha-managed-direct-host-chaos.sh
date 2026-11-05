#!/bin/sh
# Direct-host managed HA qualification, restricted to a pre-created namespace.
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
execution_mode=${RHIZA_CHAOS_EXECUTION_MODE:-github-actions}
case "$execution_mode" in
  github-actions) [ "${GITHUB_ACTIONS-}" = true ] || { echo 'GitHub Actions required for github-actions mode' >&2; exit 1; } ;;
  local)
    [ "${GITHUB_ACTIONS-}" != true ] || { echo 'local mode cannot run in GitHub Actions' >&2; exit 1; }
    : "${RHIZA_LOCAL_RUN_ID:?}"
    case "$RHIZA_LOCAL_RUN_ID" in ''|*[!0-9]*) echo 'local run ID must contain only digits' >&2; exit 1 ;; esac
    [ "${#RHIZA_LOCAL_RUN_ID}" -le 34 ]
    # The shared fixture uses these names for unique bucket naming, not CI authorization.
    GITHUB_RUN_ID=$RHIZA_LOCAL_RUN_ID
    GITHUB_RUN_ATTEMPT=1
    GITHUB_SHA=$(git rev-parse HEAD)
    ;;
  *) echo 'unsupported Chaos execution mode' >&2; exit 1 ;;
esac
: "${RUNNER_TEMP:?}" "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}" "${GITHUB_SHA:?}"
: "${RHIZA_HOST_KUBECONFIG:?}" "${RHIZA_HOST_CONTEXT:?}" "${RHIZA_HOST_NAMESPACE:?}"
: "${RHIZA_HOST_NAMESPACE_UID:?}" "${RHIZA_HOST_SERVICEACCOUNT_UID:?}" "${RHIZA_HOST_NODE:?}"
: "${RHIZA_RELEASE_IMAGE:?}" "${RHIZA_MANAGED_TEST_DOCKERFILE:?}"
[ "$RHIZA_MANAGED_TEST_DOCKERFILE" = Dockerfile.managed-ffi-contract ] || { echo 'direct-host requires managed-ffi-contract image' >&2; exit 1; }
printf '%s\n' "$RHIZA_RELEASE_IMAGE" | grep -Eq '^ghcr\.io/mrchypark/rhiza-operator@sha256:[0-9a-f]{64}$'
printf '%s\n' "$RHIZA_HOST_NAMESPACE_UID" | grep -Eq '^[0-9a-fA-F-]{16,64}$'
printf '%s\n' "$RHIZA_HOST_NODE" | grep -Eq '^[a-z0-9][a-z0-9.-]{0,252}$'
case "$RHIZA_HOST_NAMESPACE" in rhiza-ha-chaos-*) ;; *) echo 'isolated test namespace required' >&2; exit 1;; esac
logs="$RUNNER_TEMP/ha-managed-cluster-chaos"
mkdir -p "$logs"
printf '%s\n' "$execution_mode" "$GITHUB_SHA" "$GITHUB_RUN_ID" "$GITHUB_RUN_ATTEMPT" > "$logs/execution-source.txt"
private=$(mktemp -d "$RUNNER_TEMP/ha-managed-direct-host-private.XXXXXX")
chmod 700 "$private"
nonce=$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')
namespace=$RHIZA_HOST_NAMESPACE
release="rhiza-direct-chaos-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
image=$RHIZA_RELEASE_IMAGE
bucket_pid=
fault_name=
owned=0
h() { kubectl --kubeconfig "$RHIZA_HOST_KUBECONFIG" --context "$RHIZA_HOST_CONTEXT" --request-timeout=20s "$@"; }
hn() { h -n "$namespace" "$@"; }
k() { hn "$@"; }
host_guard() { [ "$(hn get serviceaccount rhiza-chaos-runner -o jsonpath='{.metadata.uid}')" = "$RHIZA_HOST_SERVICEACCOUNT_UID" ]; }
assert_names_absent() {
  resource=$1
  shift
  case "$resource" in rhizaclusters|deployments|services|serviceaccounts|secrets|roles|rolebindings|jobs|podchaos) ;; *) return 2 ;; esac
  [ "$#" -gt 0 ] || return 2
  list_json="$private/preflight-list.json"
  if ! hn get "$resource" -o json > "$list_json"; then
    echo "failed to list namespaced $resource during preflight" >&2
    return 1
  fi
  names_json=$(printf '%s\n' "$@" | jq -R . | jq -s .)
  jq -e --argjson names "$names_json" '.items|all(.[]; .metadata.name as $name | ($names|index($name))==null)' "$list_json" >/dev/null || {
    echo "preflight found an existing fixture name in $resource" >&2
    return 1
  }
}
capture_managed_logs() {
  pods="$private/pods.json"
  hn get pods -o json > "$pods" 2>/dev/null || return 0
  jq -r '.items[] as $pod | ($pod.spec.containers[]? | select(.name=="sql") | [$pod.metadata.name,.name]), ($pod.spec.initContainers[]? | select(.name=="rhiza-prepare-identity" or .name=="rhiza-await-bootstrap-plan") | [$pod.metadata.name,.name]) | @tsv' "$pods" 2>/dev/null |
  while IFS="$(printf '\t')" read -r pod container; do
    case "$pod" in *[!a-z0-9.-]*|'') continue ;; esac
    case "$container" in sql|rhiza-prepare-identity|rhiza-await-bootstrap-plan) ;; *) continue ;; esac
    for version in current previous; do
      if [ "$version" = previous ]; then
        hn logs "$pod" -c "$container" --previous --tail=200 --limit-bytes=65536 > "$logs/$container-$pod-previous.log" 2>/dev/null || :
      else
        hn logs "$pod" -c "$container" --tail=200 --limit-bytes=65536 > "$logs/$container-$pod-current.log" 2>/dev/null || :
      fi
    done
  done
  hn logs deployment/operator -c operator --tail=200 --limit-bytes=65536 > "$logs/operator-current.log" 2>/dev/null || :
  hn logs deployment/floci -c floci --tail=100 --limit-bytes=32768 > "$logs/floci-current.log" 2>/dev/null || :
  rm -f "$pods"
}
cleanup() {
  result=$?
  original_result=$result
  printf '%s\n' "$original_result" > "$logs/driver-original.exit"
  trap - EXIT HUP INT TERM
  if [ "$owned" = 1 ] && host_guard; then
    capture_managed_logs || :
    if [ -n "$fault_name" ]; then
      if [ "$result" -ne 0 ]; then capture_chaos_failure "$fault_name" cleanup || :; fi
      hn delete podchaos "$fault_name" --wait=true --timeout=45s > "$logs/fault-cleanup.txt" 2>&1 || result=1
    fi
    hn delete rhizacluster sql --wait=true --timeout=45s --ignore-not-found > "$logs/cluster-cleanup.txt" 2>&1 || result=1
    if [ -n "${uid-}" ]; then hn delete pods,deployments -l "rhiza.mrchypark.dev/cluster-uid=$uid" --wait=true --timeout=45s --ignore-not-found > "$logs/sql-workload-cleanup.txt" 2>&1 || result=1; fi
    hn delete deployment/operator deployment/floci service/floci service/bootstrap-client pod/floci-service-links-probe --wait=true --timeout=45s --ignore-not-found > "$logs/workload-cleanup.txt" 2>&1 || result=1
    hn delete jobs -l rhiza-ci-owner=harness --wait=true --timeout=45s --ignore-not-found > "$logs/job-cleanup.txt" 2>&1 || result=1
    hn delete secret/runtime secret/request-create-sql secret/request-insert-sql secret/request-query secret/request-query-after-replacement secret/request-query-after-restart secret/request-insert-after-chaos secret/request-query-after-chaos --ignore-not-found > "$logs/secret-cleanup.txt" 2>&1 || result=1
    hn delete rolebinding/runtime-read rolebinding/runtime-pod-discovery rolebinding/runtime-status rolebinding/managed-host-account-read rolebinding/bootstrap rolebinding/create-deployment rolebinding/status role/runtime-read role/runtime-pod-discovery role/runtime-status role/managed-host-account-read role/bootstrap role/create-deployment role/status serviceaccount/operator serviceaccount/rhiza-managed-host --ignore-not-found > "$logs/rbac-cleanup.txt" 2>&1 || result=1
    hn get pods -o json > "$logs/host-pods-after-cleanup.json" 2>&1 || result=1
    jq -e '.items|length==0' "$logs/host-pods-after-cleanup.json" >/dev/null || result=1
  elif [ "$owned" = 1 ]; then
    echo 'namespace identity changed; cleanup refused' > "$logs/cleanup-blocked.txt"
    result=1
  fi
  if [ -n "$bucket_pid" ]; then kill "$bucket_pid" 2>/dev/null || :; wait "$bucket_pid" 2>/dev/null || :; kill -0 "$bucket_pid" 2>/dev/null && result=1 || :; fi
  rm -f "$private/runtime.json" "$private/admin.token" "$private/curl.conf" "$private/request.body" "$private/forward.log" "$private/port-forward.log" "$private/preflight-list.json" "$RUNNER_TEMP/ha-managed-direct-host-bucket-setup"
  rmdir "$private" || result=1
  [ "$original_result" -eq 0 ] || result=$original_result
  printf '%s\n' "$result" > "$logs/driver.exit"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
host_guard
printf '%s\n' "$RHIZA_HOST_NAMESPACE" "$RHIZA_HOST_NAMESPACE_UID" "$RHIZA_HOST_SERVICEACCOUNT_UID" > "$logs/host-scope.txt"
# API discovery proves that the preinstalled RhizaCluster v1alpha2 endpoint is
# served. This test never creates/patches CRDs and never asks for Recovery API.
h get --raw=/apis/rhiza.mrchypark.dev/v1alpha2 > "$logs/rhizacluster-api.json"
jq -e '.resources|any(.[]; .name=="rhizaclusters" and .kind=="RhizaCluster")' "$logs/rhizacluster-api.json" >/dev/null
[ "$(h auth can-i create customresourcedefinitions.apiextensions.k8s.io)" = no ]
[ "$(h auth can-i create namespaces)" = no ]
[ "$(hn auth can-i create podchaos.chaos-mesh.org)" = yes ]
hn get pods -o json > "$logs/host-pods-before.json"
jq -e '.items|length==0' "$logs/host-pods-before.json" >/dev/null
assert_names_absent rhizaclusters sql
assert_names_absent deployments operator floci
assert_names_absent services floci bootstrap-client
assert_names_absent serviceaccounts operator rhiza-managed-host
assert_names_absent secrets runtime request-create-sql request-insert-sql request-query request-query-after-replacement request-query-after-restart request-insert-after-chaos request-query-after-chaos
assert_names_absent roles runtime-read runtime-pod-discovery runtime-status managed-host-account-read bootstrap create-deployment status
assert_names_absent rolebindings runtime-read runtime-pod-discovery runtime-status managed-host-account-read bootstrap create-deployment status
assert_names_absent jobs create-sql insert-sql query query-after-replacement query-after-restart insert-after-chaos query-after-chaos
assert_names_absent podchaos "$release-pod-kill" "$release-container-kill"
owned=1
export RHIZA_MANAGED_FIXTURE_NODE=$RHIZA_HOST_NODE
managed_bucket_setup() {
  hn port-forward --address=127.0.0.1 service/floci :4566 > "$private/port-forward.log" 2>&1 &
  bucket_pid=$!
  tries=0
  while :; do
    port=$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9]*\) -> .*$/\1/p' "$private/port-forward.log" | head -1)
    [ -z "$port" ] || break
    kill -0 "$bucket_pid" 2>/dev/null || return 1
    tries=$((tries+1)); [ "$tries" -lt 30 ] || return 1; sleep 1
  done
  (cd scripts/ha-managed-bucket-setup-src && go build -mod=readonly -o "$RUNNER_TEMP/ha-managed-direct-host-bucket-setup" .)
  "$RUNNER_TEMP/ha-managed-direct-host-bucket-setup" "http://127.0.0.1:$port" "$bucket"
  kill "$bucket_pid"; wait "$bucket_pid" 2>/dev/null || :; bucket_pid=
}
capture_floci() { hn logs deployment/floci --tail=80 > "$logs/floci.log" 2>&1 || :; }
chaos_k() { hn "$@"; }
chaos_scope_guard() { host_guard; }
. scripts/ha-managed-podchaos.sh
managed_pod_fault() {
  hn exec "$1" -c sql -- sha256sum /var/lib/rhiza/data/qlog/voter-identity.json > "$logs/old-voter-identity.txt"
  direct_chaos_fault pod-kill pod-kill "$1" "$2"
}
managed_container_fault() {
  hn exec "$1" -c sql -- sha256sum /var/lib/rhiza/data/qlog/voter-identity.json > "$logs/replacement-voter-identity.txt"
  managed_replacement_identity_oracle "$logs/old-voter-identity.txt" "$logs/replacement-voter-identity.txt"
  direct_chaos_fault container-kill container-kill "$1" "$2"
  managed_chaos_verified=1
}
. scripts/ha-managed-bootstrap-fixture.sh
managed_fixture
run_client insert-after-chaos execute '{"request_id":"chaos-final-insert","sql":"INSERT INTO bootstrap_ci VALUES (2, '\''after-chaos'\'')"}'
run_client query-after-chaos query '{"sql":"SELECT id, value FROM bootstrap_ci ORDER BY id"}'
jq -e '.rows==[[1,"real-kubelet"],[2,"after-chaos"]]' "$logs/query-after-chaos.json" >/dev/null
printf '%s\n' 'PASS: direct-host managed Go/Rust FFI contract, exact-UID Chaos Mesh Pod replacement and same-Pod restart, certified replacement, NoPVC, ACK readback and new write' \
  'SCOPE: namespace-scoped preinstalled RhizaCluster v1alpha2 API and pinned in-namespace Floci S3; not operator CRD installation, live GCS, network partition, physical fencing or performance qualification' > "$logs/result.txt"
