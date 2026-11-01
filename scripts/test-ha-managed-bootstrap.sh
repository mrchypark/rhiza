#!/bin/sh
# Actual Kind bootstrap/replacement/restart driver; Chaos Mesh has a separate driver.
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
if [ "${1-}" = --oracle-self-test ]; then
  (cd scripts/ha-managed-bucket-setup-src && go test -mod=readonly -timeout=45s -run 'Test(GatedOracle|CommittedPlanOracle)$' .)
  exit 0
fi
[ "${GITHUB_ACTIONS-}" = true ] || { echo 'GitHub Actions only' >&2; exit 1; }
: "${RUNNER_TEMP:?}" "${GITHUB_SHA:?}" "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}"
logs="$RUNNER_TEMP/ha-managed-bootstrap"
mkdir -p "$logs"
private=$(mktemp -d "$RUNNER_TEMP/ha-managed-private.XXXXXX")
nonce=$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')
cluster="ha-managed-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}-$nonce"
namespace=managed-bootstrap
image="rhiza-managed:$GITHUB_SHA-$nonce"
kubeconfig="$private/kubeconfig"
owned=0
built=0
k() { kubectl --kubeconfig "$kubeconfig" --context "kind-$cluster" --request-timeout=30s "$@"; }
capture_floci() {
  k -n "$namespace" get pods -l rhiza-ci-component=floci -o json > "$logs/floci-pods.json" 2> "$logs/floci-pods-errors.txt" || return 0
  # Restrict metadata to emulator Pods: never inspect Secret data or runtime profile contents.
  jq '[.items[]|{metadata:{name:.metadata.name,uid:.metadata.uid,labels:.metadata.labels},spec:{enableServiceLinks:.spec.enableServiceLinks,containers:[.spec.containers[]|{name,image}]},status:.status}]' "$logs/floci-pods.json" > "$logs/floci-status.json" || :
  for floci_pod in $(jq -r '.items[].metadata.name' "$logs/floci-pods.json"); do
    k -n "$namespace" logs "$floci_pod" -c floci > "$logs/$floci_pod-current.log" 2> "$logs/$floci_pod-current-errors.txt" || :
    k -n "$namespace" logs "$floci_pod" -c floci --previous > "$logs/$floci_pod-previous.log" 2> "$logs/$floci_pod-previous-errors.txt" || :
  done
}
capture_bootstrap_logs() {
  for component in operator managed; do
    if [ "$component" = operator ]; then log_selector=app=operator
    elif [ -n "${uid-}" ]; then log_selector="rhiza.mrchypark.dev/cluster-uid=$uid"
    else continue
    fi
    k -n "$namespace" get pods -l "$log_selector" -o json > "$logs/$component-log-pods.json" 2> "$logs/$component-log-pods-errors.txt" || continue
    # Read only public Pod/container names. Never exec or read identity/profile/Secret files.
    jq -r '.items[]|.metadata.name as $pod|(.spec.initContainers[]?,.spec.containers[]?)|[$pod,.name]|@tsv' "$logs/$component-log-pods.json" |
    while IFS="$(printf '\t')" read -r log_pod log_container; do
      k -n "$namespace" logs "$log_pod" -c "$log_container" --tail=200 --limit-bytes=65536 > "$logs/$log_pod-$log_container-current.log" 2> "$logs/$log_pod-$log_container-current-errors.txt" || :
      k -n "$namespace" logs "$log_pod" -c "$log_container" --previous --tail=200 --limit-bytes=65536 > "$logs/$log_pod-$log_container-previous.log" 2> "$logs/$log_pod-$log_container-previous-errors.txt" || :
    done
  done
}
cleanup() {
  result=$?
  trap - EXIT HUP INT TERM
  if [ "$owned" = 1 ]; then
    capture_floci
    capture_bootstrap_logs
    k -n "$namespace" get pods,deployments,replicasets,rhizaclusters,jobs,services,endpoints -o json > "$logs/final-state.json" 2>&1 || :
    for job in create-sql insert-sql query; do
      k -n "$namespace" logs "job/$job" --tail=200 --limit-bytes=65536 > "$logs/$job-cleanup.log" 2> "$logs/$job-log-errors.txt" || :
    done
    k -n "$namespace" get events > "$logs/events.txt" 2>&1 || :
    kind delete cluster --name "$cluster" > "$logs/delete.txt" 2>&1 || result=1
    kind get clusters > "$logs/clusters-after.txt" 2>&1 || result=1
    if grep -Fx "$cluster" "$logs/clusters-after.txt" >/dev/null; then result=1; fi
  fi
  if [ "$built" = 1 ]; then
    docker image rm "$image" > "$logs/image-cleanup.txt" 2>&1 || result=1
    if docker image inspect "$image" >/dev/null 2>&1; then result=1; fi
  fi
  rm -f "$private/runtime.json" "$private/kubeconfig" "$private/curl.conf" "$private/request.body" "$private/admin.token"
  rmdir "$private" || result=1
  exit "$result"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
kind get clusters > "$logs/clusters-before.txt"
if grep -Fx "$cluster" "$logs/clusters-before.txt" >/dev/null; then exit 1; fi
owned=1
kind create cluster --name "$cluster" --kubeconfig "$kubeconfig" --wait 120s > "$logs/create.txt" 2>&1
built=1
docker build -f "${RHIZA_MANAGED_TEST_DOCKERFILE:-Dockerfile.operator}" -t "$image" . > "$logs/build.txt" 2>&1
kind load docker-image --name "$cluster" "$image" > "$logs/load.txt" 2>&1
k create namespace "$namespace"
k apply -f deploy/operator/cluster-crd.yaml -f deploy/operator/crd.yaml
k wait --for=condition=Established --timeout=90s crd/rhizaclusters.rhiza.mrchypark.dev crd/rhizarecoveries.rhiza.mrchypark.dev
managed_bucket_setup() { "$RHIZA_MANAGED_BUCKET_SETUP" "$kubeconfig" "kind-$cluster" "$namespace" "$bucket"; }
managed_pod_fault() { k -n "$namespace" delete pod "$1" --wait=true --timeout=90s; }
managed_container_fault() { k -n "$namespace" exec "$1" -c sql -- sh -c 'kill -TERM 1'; }
. scripts/ha-managed-bootstrap-fixture.sh
managed_fixture
