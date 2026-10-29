#!/bin/sh
set -eu
umask 077

admission_error() {
  file=$1 expected=$2
  if grep -Ei 'connection refused|timed out|timeout|unable to connect|no such host|i/o timeout|EOF|Error from server \((Forbidden|NotFound)\)' "$file" >/dev/null; then return 1; fi
  grep -F "$expected" "$file" >/dev/null || return 1
  grep -E 'Error from server \((Invalid|BadRequest)\)|strict decoding error|^The (RhizaCluster|RhizaRecovery) "[^"]+" is invalid:' "$file" >/dev/null
}

if [ "${1:-}" = --self-test-oracle ]; then
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/ha-oracle.XXXXXXXX")
  trap 'rm -f "$tmp/case"; rmdir "$tmp"' EXIT HUP INT TERM
  printf '%s\n' 'The RhizaCluster "cluster" is invalid: spec.logicalID: Invalid value: "other": logicalID is immutable' > "$tmp/case"
  admission_error "$tmp/case" 'logicalID is immutable' || exit 1
  if admission_error "$tmp/case" 'target is immutable'; then exit 1; fi
  for line in \
    'Error from server (Invalid): target is immutable' \
    'Error from server (BadRequest): strict decoding error: unknown field "spec.target.typo"'; do
    printf '%s\n' "$line" > "$tmp/case"
    case "$line" in *Invalid*) expected='target is immutable';; *) expected='spec.target.typo';; esac
    admission_error "$tmp/case" "$expected" || exit 1
  done
  for line in \
    'Error from server (NotFound): target is immutable' \
    'Error from server (Forbidden): target is immutable' \
    'connection refused: target is immutable' \
    'timeout: target is immutable'; do
    printf '%s\n' "$line" > "$tmp/case"
    if admission_error "$tmp/case" 'target is immutable'; then exit 1; fi
  done
  exit 0
fi

: "${RUNNER_TEMP:?RUNNER_TEMP is required}"
: "${GITHUB_RUN_ID:?GITHUB_RUN_ID is required}"
: "${GITHUB_RUN_ATTEMPT:?GITHUB_RUN_ATTEMPT is required}"
logs="$RUNNER_TEMP/ha-v1alpha2-contract"
mkdir -p "$logs"
nonce=$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')
[ "${#nonce}" -eq 16 ] || { echo 'random nonce unavailable' >&2; exit 1; }
cluster="ha-contract-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}-${nonce}"
context="kind-$cluster"
namespace="ha-contract-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
kubeconfig="$RUNNER_TEMP/ha-contract-$nonce.kubeconfig"
tokenfile="$RUNNER_TEMP/ha-contract-$nonce.token"
cafile="$RUNNER_TEMP/ha-contract-$nonce.ca"
owned=0

cleanup() {
  result=$?
  trap - EXIT HUP INT TERM
  if [ "$owned" -eq 1 ]; then
    kubectl --kubeconfig "$kubeconfig" --context "$context" get crd -o wide > "$logs/crds.txt" 2>&1 || :
    kubectl --kubeconfig "$kubeconfig" --context "$context" -n "$namespace" get rhizaclusters,rhizarecoveries -o yaml > "$logs/resources.yaml" 2>&1 || :
    if ! kind delete cluster --name "$cluster" > "$logs/delete.txt" 2>&1; then
      echo "failed to delete owned cluster $cluster; see delete.txt" >&2
      result=1
    fi
    if ! kind get clusters > "$logs/clusters-after.txt" 2>&1; then
      echo "could not verify cleanup of owned cluster $cluster; see clusters-after.txt" >&2
      result=1
    elif grep -Fx "$cluster" "$logs/clusters-after.txt" >/dev/null; then
      echo "owned cluster remains: $cluster" >&2
      result=1
    fi
  fi
  # The kubeconfig contains credentials and must not be uploaded.
  rm -f "$kubeconfig" "$tokenfile" "$cafile"
  if [ -e "$kubeconfig" ] || [ -e "$tokenfile" ] || [ -e "$cafile" ]; then
    echo "credential cleanup incomplete for contract $nonce" >&2
    result=1
  fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
kind version > "$logs/versions.txt"
kubectl version --client >> "$logs/versions.txt"
kind get clusters > "$logs/clusters-before.txt"
if grep -Fx "$cluster" "$logs/clusters-before.txt" >/dev/null; then
  echo "refusing existing cluster $cluster" >&2
  exit 1
fi
owned=1
if ! kind create cluster --name "$cluster" --kubeconfig "$kubeconfig" --wait 120s > "$logs/create-cluster.txt" 2>&1; then
  echo "cluster creation failed; cleaning up the exact preflight-absent cluster $cluster" | tee "$logs/partial-cluster.txt" >&2
  exit 1
fi
k() { kubectl --kubeconfig "$kubeconfig" --context "$context" --request-timeout=30s "$@"; }
k version >> "$logs/versions.txt"
k create namespace "$namespace" > "$logs/namespace.txt" 2>&1
k apply --validate=strict -f deploy/operator/cluster-crd.yaml -f deploy/operator/crd.yaml > "$logs/apply-crds.txt" 2>&1
k wait --for=condition=Established --timeout=90s crd/rhizaclusters.rhiza.mrchypark.dev crd/rhizarecoveries.rhiza.mrchypark.dev > "$logs/established.txt" 2>&1

ok() {
  label=$1; shift
  if ! k "$@" > "$logs/$label.txt" 2>&1; then cat "$logs/$label.txt" >&2; return 1; fi
}
reject() {
  label=$1; expected=$2; shift 2
  if k "$@" > "$logs/$label.txt" 2>&1; then echo "unexpected acceptance: $label" >&2; return 1; fi
  if ! admission_error "$logs/$label.txt" "$expected"; then
    echo "not an admission rejection: $label" >&2; cat "$logs/$label.txt" >&2; return 1
  fi
}
cluster_json() {
  printf '{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","metadata":{"name":"%s","namespace":"%s"},"spec":{"logicalID":"test","voterCount":%s,"workload":{"podTemplate":{"spec":{"containers":[{"name":"app","image":"example.invalid/app"}]}}}%s}}' "$1" "$namespace" "$2" "$3"
}
recovery_json() {
  printf '{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaRecovery","metadata":{"name":"%s","namespace":"%s"},"spec":{"target":{"name":"cluster","uid":"uid"}%s}}' "$1" "$namespace" "$2"
}
cluster_json cluster 3 '' > "$logs/cluster.json"
ok cluster-create create --validate=strict -f "$logs/cluster.json"
reject cluster-logical 'logicalID is immutable' patch rhizacluster cluster -n "$namespace" --type=merge -p '{"spec":{"logicalID":"other"}}'
reject cluster-voters 'voterCount updates are unsupported' patch rhizacluster cluster -n "$namespace" --type=merge -p '{"spec":{"voterCount":4}}'
reject cluster-container-name 'workload updates are unsupported' patch rhizacluster cluster -n "$namespace" --type=merge -p '{"spec":{"workload":{"podTemplate":{"spec":{"containers":[{"name":"other","image":"example.invalid/app"}]}}}}}'
reject cluster-container-image 'workload updates are unsupported' patch rhizacluster cluster -n "$namespace" --type=merge -p '{"spec":{"workload":{"podTemplate":{"spec":{"containers":[{"name":"app","image":"other"}]}}}}}'
reject cluster-container-list 'workload updates are unsupported' patch rhizacluster cluster -n "$namespace" --type=merge -p '{"spec":{"workload":{"podTemplate":{"spec":{"containers":[{"name":"app","image":"example.invalid/app"},{"name":"extra","image":"example.invalid/app"}]}}}}}'
ok cluster-status patch rhizacluster cluster -n "$namespace" --subresource=status --type=merge -p '{"status":{"authority":{"opaque":"keep"}}}'
ok cluster-metadata patch rhizacluster cluster -n "$namespace" --type=merge -p '{"metadata":{"labels":{"test":"updated"}}}'
ok cluster-authority get rhizacluster cluster -n "$namespace" -o jsonpath='{.status.authority.opaque}'
[ "$(cat "$logs/cluster-authority.txt")" = keep ] || { echo 'cluster status authority lost' >&2; exit 1; }
ok cluster-defaults get rhizacluster cluster -n "$namespace" -o jsonpath='{.spec.automaticRecovery}{" "}{.spec.provisionalRetentionSeconds}{" "}{.spec.deletionPolicy}'
if [ "$(cat "$logs/cluster-defaults.txt")" != 'OFF 172800 Orphan' ]; then echo 'cluster defaults mismatch' >&2; exit 1; fi
for voters in 2 64 65; do
  cluster_json "voters-$voters" "$voters" '' > "$logs/voters-$voters.json"
  if [ "$voters" = 64 ]; then ok voters-64 create --validate=strict --dry-run=server -f "$logs/voters-$voters.json"; else reject "voters-$voters" 'spec.voterCount' create --validate=strict --dry-run=server -f "$logs/voters-$voters.json"; fi
done
recovery_json request ',"expectedGeneration":0' > "$logs/recovery.json"
ok recovery-create create --validate=strict -f "$logs/recovery.json"
ok recovery-metadata patch rhizarecovery request -n "$namespace" --type=merge -p '{"metadata":{"labels":{"test":"updated"}}}'
ok backoff-default get rhizarecovery request -n "$namespace" -o jsonpath='{.spec.backoffLimit}'
[ "$(cat "$logs/backoff-default.txt")" = 6 ] || { echo 'backoff default mismatch' >&2; exit 1; }
recovery_json missing-generation '' > "$logs/missing-generation.json"
reject missing-generation 'expectedGeneration' create --validate=strict --dry-run=server -f "$logs/missing-generation.json"
for value in 0 -1; do
  recovery_json "backoff-${value#-}" ",\"expectedGeneration\":0,\"backoffLimit\":$value" > "$logs/backoff-$value.json"
  if [ "$value" = 0 ]; then ok backoff-zero create --validate=strict --dry-run=server -f "$logs/backoff-$value.json"; else reject backoff-negative 'spec.backoffLimit' create --validate=strict --dry-run=server -f "$logs/backoff-$value.json"; fi
done
reject immutable-target 'target is immutable' patch rhizarecovery request -n "$namespace" --type=merge -p '{"spec":{"target":{"name":"other"}}}'
reject immutable-generation 'expectedGeneration is immutable' patch rhizarecovery request -n "$namespace" --type=merge -p '{"spec":{"expectedGeneration":1}}'
reject immutable-backoff 'effective backoffLimit is immutable' patch rhizarecovery request -n "$namespace" --type=merge -p '{"spec":{"backoffLimit":0}}'
reject immutable-deadline 'activeDeadlineSeconds is immutable' patch rhizarecovery request -n "$namespace" --type=merge -p '{"spec":{"activeDeadlineSeconds":10}}'
reject immutable-uid 'metadata.uid' patch rhizarecovery request -n "$namespace" --type=merge -p '{"metadata":{"uid":"other"}}'
recovery_json finite ',"expectedGeneration":0,"activeDeadlineSeconds":10' > "$logs/finite.json"
ok finite-create create --validate=strict -f "$logs/finite.json"
reject finite-remove 'activeDeadlineSeconds is immutable' patch rhizarecovery finite -n "$namespace" --type=merge -p '{"spec":{"activeDeadlineSeconds":null}}'
ok ttl-before patch rhizarecovery request -n "$namespace" --type=merge -p '{"spec":{"ttlSecondsAfterFinished":0}}'
for phase in Complete Failed; do
  if [ "$phase" = Complete ]; then ttl=10; else ttl=20; fi
  ok "status-$phase" patch rhizarecovery request -n "$namespace" --subresource=status --type=merge -p "{\"status\":{\"phase\":\"$phase\",\"authority\":{\"opaque\":\"keep\"}}}"
  ok "ttl-$phase" patch rhizarecovery request -n "$namespace" --type=merge -p "{\"spec\":{\"ttlSecondsAfterFinished\":$ttl}}"
  ok "ttl-read-$phase" get rhizarecovery request -n "$namespace" -o jsonpath='{.spec.ttlSecondsAfterFinished}'
  [ "$(cat "$logs/ttl-read-$phase.txt")" = "$ttl" ] || { echo "TTL mismatch for $phase" >&2; exit 1; }
  ok "authority-$phase" get rhizarecovery request -n "$namespace" -o jsonpath='{.status.authority.opaque}'
  [ "$(cat "$logs/authority-$phase.txt")" = keep ] || { echo 'status authority lost' >&2; exit 1; }
done
ok ttl-remove patch rhizarecovery request -n "$namespace" --type=merge -p '{"spec":{"ttlSecondsAfterFinished":null}}'
recovery_json legacy ',"expectedGeneration":0,"statefulSet":"old"' > "$logs/legacy.json"
reject legacy 'legacy recovery fields are unsupported' create --validate=strict --dry-run=server -f "$logs/legacy.json"
recovery_json target-extra ',"expectedGeneration":0' | sed 's/"uid":"uid"/"uid":"uid","namespace":"other"/' > "$logs/target-namespace.json"
reject target-namespace 'target must be in the request namespace' create --validate=strict --dry-run=server -f "$logs/target-namespace.json"
recovery_json target-typo ',"expectedGeneration":0' | sed 's/"uid":"uid"/"uid":"uid","typo":"x"/' > "$logs/target-typo.json"
reject target-typo 'spec.target.typo' create --validate=strict --dry-run=server -f "$logs/target-typo.json"
cluster_json unknown 3 '' | sed 's/"containers":/"securityContext":{},"containers":/' > "$logs/security-context.json"
reject security-context 'spec.workload.podTemplate.spec.securityContext' create --validate=strict --dry-run=server -f "$logs/security-context.json"
cluster_json command 3 '' | sed 's/"image":"example.invalid\/app"/"image":"example.invalid\/app","command":["x"]/' > "$logs/command.json"
reject command 'spec.workload.podTemplate.spec.containers[0].command' create --validate=strict --dry-run=server -f "$logs/command.json"
if k get --raw "/apis/rhiza.mrchypark.dev/v1alpha1" > "$logs/v1alpha1.txt" 2>&1; then
  echo 'unexpected v1alpha1 endpoint' >&2; exit 1
fi
if ! grep -F 'Error from server (NotFound)' "$logs/v1alpha1.txt" >/dev/null; then
  echo 'v1alpha1 check did not return NotFound' >&2; cat "$logs/v1alpha1.txt" >&2; exit 1
fi

# Administrative test fixtures only; this is not an authority genesis protocol.
for name in status-contract queue-contract finalizer-contract lifecycle-cancel lifecycle-prepare cancel-complete cancel-retained cancel-waiting missing-authority; do
  cluster_json "$name" 3 '' > "$logs/$name.json"
  ok "$name-create" create --validate=strict -f "$logs/$name.json"
done
for name in status-contract queue-contract finalizer-contract lifecycle-cancel lifecycle-prepare cancel-complete cancel-retained cancel-waiting; do
  uid=$(k get rhizacluster "$name" -n "$namespace" -o jsonpath='{.metadata.uid}')
  [ -n "$uid" ] || exit 1
  ok "$name-seed" patch rhizacluster "$name" -n "$namespace" --subresource=status --type=merge -p "{\"status\":{\"authority\":{\"owner\":{\"namespace\":\"$namespace\",\"name\":\"$name\",\"uid\":\"$uid\",\"logicalID\":\"test\"},\"generation\":2,\"binding\":{\"deploymentUID\":\"deployment\",\"storageID\":\"storage\"},\"pendingWrite\":false,\"transition\":null,\"receipt\":{\"opaque\":\"keep\"}},\"other\":{\"opaque\":\"keep\"}}}"
done
for target in cancel-complete cancel-retained cancel-waiting; do
  target_uid=$(k get rhizacluster "$target" -n "$namespace" -o jsonpath='{.metadata.uid}')
  [ -n "$target_uid" ] || exit 1
  for suffix in first next; do
    finalizers='"rhiza.mrchypark.dev/recovery-record"'
    if [ "$target-$suffix" = cancel-retained-first ]; then finalizers="$finalizers,\"example.invalid/keep\""; fi
    printf '{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaRecovery","metadata":{"name":"%s-%s","namespace":"%s","finalizers":[%s]},"spec":{"target":{"name":"%s","uid":"%s"},"expectedGeneration":2}}' "$target" "$suffix" "$namespace" "$finalizers" "$target" "$target_uid" > "$logs/$target-$suffix.json"
    ok "$target-$suffix-create" create --validate=strict -f "$logs/$target-$suffix.json"
    if [ "$suffix" = first ]; then sleep 2; fi
  done
done
for name in lifecycle-cancel lifecycle-prepare; do
  target_uid=$(k get rhizacluster "$name" -n "$namespace" -o jsonpath='{.metadata.uid}')
  [ -n "$target_uid" ] || exit 1
  printf '{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaRecovery","metadata":{"name":"%s","namespace":"%s","finalizers":["rhiza.mrchypark.dev/recovery-record","example.invalid/keep"]},"spec":{"target":{"name":"%s","uid":"%s"},"expectedGeneration":2}}' "$name" "$namespace" "$name" "$target_uid" > "$logs/$name-recovery.json"
  ok "$name-recovery-create" create --validate=strict -f "$logs/$name-recovery.json"
done
finalizer_uid=$(k get rhizacluster finalizer-contract -n "$namespace" -o jsonpath='{.metadata.uid}')
[ -n "$finalizer_uid" ] || exit 1
for name in finalizer-deleting finalizer-install; do
  target=finalizer-contract
  target_uid=$finalizer_uid
  if [ "$name" = finalizer-install ]; then
    target=missing-authority
    target_uid=$(k get rhizacluster "$target" -n "$namespace" -o jsonpath='{.metadata.uid}')
  fi
  [ -n "$target_uid" ] || exit 1
  printf '{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaRecovery","metadata":{"name":"%s","namespace":"%s","finalizers":["example.invalid/keep"]},"spec":{"target":{"name":"%s","uid":"%s"},"expectedGeneration":2}}' "$name" "$namespace" "$target" "$target_uid" > "$logs/$name.json"
  ok "$name-create" create --validate=strict -f "$logs/$name.json"
done
ok finalizer-deleting-delete delete rhizarecovery finalizer-deleting -n "$namespace" --wait=false
queue_uid=$(k get rhizacluster queue-contract -n "$namespace" -o jsonpath='{.metadata.uid}')
for name in queue-first queue-second; do
  printf '{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaRecovery","metadata":{"name":"%s","namespace":"%s","finalizers":["rhiza.mrchypark.dev/recovery-record"]},"spec":{"target":{"name":"queue-contract","uid":"%s"},"expectedGeneration":2}}' "$name" "$namespace" "$queue_uid" > "$logs/$name.json"
  ok "$name-create" create --validate=strict -f "$logs/$name.json"
done
k -n "$namespace" create serviceaccount phase2-contract > "$logs/serviceaccount.txt" 2>&1
k -n "$namespace" apply -f - > "$logs/role.txt" 2>&1 <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: phase2-contract
  namespace: $namespace
rules:
- apiGroups: ["rhiza.mrchypark.dev"]
  resources: ["rhizaclusters"]
  verbs: ["get"]
- apiGroups: ["rhiza.mrchypark.dev"]
  resources: ["rhizaclusters/status"]
  verbs: ["update"]
- apiGroups: ["rhiza.mrchypark.dev"]
  resources: ["rhizarecoveries"]
  verbs: ["get", "list", "update", "delete"]
- apiGroups: ["rhiza.mrchypark.dev"]
  resources: ["rhizarecoveries/status"]
  verbs: ["update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: phase2-contract
  namespace: $namespace
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: phase2-contract
subjects:
- kind: ServiceAccount
  name: phase2-contract
  namespace: $namespace
EOF
k -n "$namespace" create token phase2-contract --duration=10m > "$tokenfile"
k config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d > "$cafile"
server=$(k config view --raw --minify -o jsonpath='{.clusters[0].cluster.server}')
status_uid=$(k get rhizacluster status-contract -n "$namespace" -o jsonpath='{.metadata.uid}')
missing_uid=$(k get rhizacluster missing-authority -n "$namespace" -o jsonpath='{.metadata.uid}')
cancel_uid=$(k get rhizacluster lifecycle-cancel -n "$namespace" -o jsonpath='{.metadata.uid}')
prepare_uid=$(k get rhizacluster lifecycle-prepare -n "$namespace" -o jsonpath='{.metadata.uid}')
complete_uid=$(k get rhizacluster cancel-complete -n "$namespace" -o jsonpath='{.metadata.uid}')
retained_uid=$(k get rhizacluster cancel-retained -n "$namespace" -o jsonpath='{.metadata.uid}')
waiting_uid=$(k get rhizacluster cancel-waiting -n "$namespace" -o jsonpath='{.metadata.uid}')
: "${server:?}" "${status_uid:?}" "${missing_uid:?}" "${queue_uid:?}" "${cancel_uid:?}" "${prepare_uid:?}" "${complete_uid:?}" "${retained_uid:?}" "${waiting_uid:?}"
[ -s "$tokenfile" ] && [ -s "$cafile" ] || exit 1
export RHIZA_CONTRACT_REQUIRED=1 RHIZA_CONTRACT_SERVER="$server" RHIZA_CONTRACT_CA="$cafile" RHIZA_CONTRACT_TOKEN="$tokenfile" RHIZA_CONTRACT_NAMESPACE="$namespace" RHIZA_CONTRACT_STATUS_UID="$status_uid" RHIZA_CONTRACT_QUEUE_UID="$queue_uid" RHIZA_CONTRACT_MISSING_UID="$missing_uid" RHIZA_CONTRACT_FINALIZER_UID="$finalizer_uid" RHIZA_CONTRACT_CANCEL_UID="$cancel_uid" RHIZA_CONTRACT_PREPARE_UID="$prepare_uid"
export RHIZA_CONTRACT_COMPLETE_UID="$complete_uid" RHIZA_CONTRACT_RETAINED_UID="$retained_uid" RHIZA_CONTRACT_WAITING_UID="$waiting_uid"
go test ./pkg/operator -run '^TestV1Alpha2(Status|Lifecycle|Cancellation)APIServerContract$' -count=1 -timeout=60s
