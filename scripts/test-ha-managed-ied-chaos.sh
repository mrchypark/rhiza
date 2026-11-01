#!/bin/sh
# CI-only isolated virtual API; never apply Rhiza CRDs to the shared host API.
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
[ "${GITHUB_ACTIONS-}" = true ] || { echo 'GitHub Actions only' >&2; exit 1; }
: "${RUNNER_TEMP:?}" "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}" "${GITHUB_SHA:?}"
: "${RHIZA_HOST_KUBECONFIG:?}" "${RHIZA_HOST_CONTEXT:?}" "${RHIZA_HOST_NAMESPACE:?}" "${RHIZA_HOST_NAMESPACE_UID:?}" "${RHIZA_HOST_SERVICEACCOUNT_UID:?}" "${RHIZA_RELEASE_IMAGE:?}"
: "${RHIZA_HOST_NODE:?existing Ready node hostname label, verified by administrator}"
printf '%s\n' "$RHIZA_HOST_NODE" | grep -Eq '^[a-z0-9][a-z0-9.-]{0,252}$'
case "$RHIZA_HOST_NAMESPACE" in rhiza-ha-chaos-*) ;; *) echo 'isolated test namespace required' >&2; exit 1;; esac
printf '%s\n' "$RHIZA_RELEASE_IMAGE" | grep -Eq '^ghcr\.io/mrchypark/rhiza-operator@sha256:[0-9a-f]{64}$'
logs="$RUNNER_TEMP/ha-managed-ied-chaos"
mkdir -p "$logs"
private=$(mktemp -d "$RUNNER_TEMP/ha-managed-ied-private.XXXXXX")
chmod 700 "$private"
nonce=$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')
namespace=managed-bootstrap
image=$RHIZA_RELEASE_IMAGE
release="rhiza-chaos-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
kubeconfig="$private/virtual.config"
installed=0
forward_pid=
bucket_pid=
h() { kubectl --kubeconfig "$RHIZA_HOST_KUBECONFIG" --context "$RHIZA_HOST_CONTEXT" --request-timeout=30s "$@"; }
hn() { h -n "$RHIZA_HOST_NAMESPACE" "$@"; }
k() { kubectl --kubeconfig "$kubeconfig" --request-timeout=30s "$@"; }
host_guard() {
  # A recreated namespace cannot recreate the token's original ServiceAccount UID.
  # This avoids granting the namespace-only CI account cluster-scoped Namespace GET.
  [ "$(hn get serviceaccount rhiza-chaos-runner -o jsonpath='{.metadata.uid}')" = "$RHIZA_HOST_SERVICEACCOUNT_UID" ]
}
cleanup() {
  result=$?
  trap - EXIT HUP INT TERM
  if [ "$installed" = 1 ]; then
    if host_guard; then
      hn get podchaos -o json > "$logs/faults-final.json" 2>&1 || result=1
      hn delete podchaos -l "rhiza-ci-run=$release" --wait=true --timeout=60s > "$logs/fault-cleanup.txt" 2>&1 || result=1
      if [ -f "$kubeconfig" ]; then
        k -n "$namespace" get pods,rhizaclusters,jobs,deployments -o json > "$logs/virtual-final.json" 2>&1 || :
        k -n "$namespace" get events > "$logs/virtual-events.txt" 2>&1 || :
      fi
      helm uninstall "$release" --kubeconfig "$RHIZA_HOST_KUBECONFIG" --kube-context "$RHIZA_HOST_CONTEXT" --namespace "$RHIZA_HOST_NAMESPACE" --wait --timeout=3m > "$logs/helm-cleanup.txt" 2>&1 || result=1
      hn get pods -o json > "$logs/host-pods-after.json" 2>&1 || result=1
      jq -e '.items|length==0' "$logs/host-pods-after.json" >/dev/null || result=1
      hn get podchaos -l "rhiza-ci-run=$release" -o json > "$logs/faults-after.json" 2>&1 || result=1
      jq -e '.items|length==0' "$logs/faults-after.json" >/dev/null || result=1
    else
      echo 'namespace identity changed; cleanup refused' > "$logs/cleanup-blocked.txt"
      result=1
    fi
  fi
  for pid in "$bucket_pid" "$forward_pid"; do
    [ -n "$pid" ] || continue
    kill "$pid" 2>/dev/null || :
    wait "$pid" 2>/dev/null || :
    if kill -0 "$pid" 2>/dev/null; then result=1; fi
  done
  # Only this private run directory: credentials are never uploaded as artifacts.
  rm -f "$private/virtual.config" "$private/runtime.json" "$private/admin.token" "$private/curl.conf" "$private/request.body" "$private/chart.tgz" "$private/values.yaml" "$private/rendered.yaml" "$private/forward.log" "$private/bucket-forward.log" "$RUNNER_TEMP/ha-managed-ied-bucket-setup"
  rmdir "$private" || result=1
  printf '%s\n' "$result" > "$logs/driver.exit"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
host_guard
printf '%s\n' "$RHIZA_HOST_NAMESPACE" "$RHIZA_HOST_NAMESPACE_UID" "$RHIZA_HOST_SERVICEACCOUNT_UID" > "$logs/host-scope.txt"
# The credential may administer only this disposable host namespace, never CRDs.
[ "$(h auth can-i create customresourcedefinitions.apiextensions.k8s.io)" = no ]
[ "$(h auth can-i create namespaces)" = no ]
hn get pods -o json > "$logs/host-pods-before.json"
jq -e '.items|length==0' "$logs/host-pods-before.json" >/dev/null
curl --fail --silent --show-error --max-time 60 https://charts.loft.sh/charts/vcluster-0.28.0.tgz -o "$private/chart.tgz"
printf '%s  %s\n' d65fa9a49dc423e62377fde56e5cbec727d3260d2dd17acfdca3bcc6d58110e4 "$private/chart.tgz" | sha256sum -c -
cat > "$private/values.yaml" <<'VALUES'
sync:
  fromHost:
    nodes: {enabled: false, syncBackChanges: false}
  toHost:
    persistentVolumeClaims: {enabled: false}
    pods:
      useSecretsForSATokens: true
      rewriteHosts: {enabled: false}
controlPlane:
  statefulSet:
    image: {registry: ghcr.io, repository: loft-sh/vcluster-oss, tag: 0.28.0}
    persistence:
      volumeClaim: {enabled: false}
    resources:
      requests: {cpu: 200m, memory: 256Mi, ephemeral-storage: 1Gi}
      limits: {cpu: '1', memory: 2Gi, ephemeral-storage: 4Gi}
rbac:
  clusterRole: {enabled: false}
VALUES
node_selector=$(jq -cn --arg node "$RHIZA_HOST_NODE" '{"kubernetes.io/hostname":$node}')
# OSS 0.28 podSyncer applies Nodes.Selector.Labels to EVERY physical Pod even
# when real-node synchronization is disabled. Do not use paid translate patches
# or an unsupported RhizaCluster podTemplate field to pretend this is enforced.
helm template "$release" "$private/chart.tgz" --namespace "$RHIZA_HOST_NAMESPACE" -f "$private/values.yaml" --set-json "sync.fromHost.nodes.selector.labels=$node_selector" --set-json "controlPlane.statefulSet.scheduling.nodeSelector=$node_selector" --set-json "controlPlane.coredns.deployment.nodeSelector=$node_selector" > "$private/rendered.yaml"
if grep -Eq '^kind: (ClusterRole|ClusterRoleBinding|CustomResourceDefinition|Namespace|PersistentVolume|PersistentVolumeClaim)$|privileged: true|hostPID: true|hostNetwork: true|hostPath:' "$private/rendered.yaml"; then
  echo 'forbidden host scope or privileges in pinned chart' >&2; exit 1
fi
installed=1
helm install "$release" "$private/chart.tgz" --kubeconfig "$RHIZA_HOST_KUBECONFIG" --kube-context "$RHIZA_HOST_CONTEXT" --namespace "$RHIZA_HOST_NAMESPACE" -f "$private/values.yaml" --set-json "sync.fromHost.nodes.selector.labels=$node_selector" --set-json "controlPlane.statefulSet.scheduling.nodeSelector=$node_selector" --set-json "controlPlane.coredns.deployment.nodeSelector=$node_selector" --wait --timeout=5m > "$logs/helm-install.txt" 2>&1
hn get secret "vc-$release" -o jsonpath='{.data.config}' | base64 --decode > "$kubeconfig"
hn port-forward --address=127.0.0.1 "service/$release" :443 > "$private/forward.log" 2>&1 &
forward_pid=$!
forward_port() {
  tries=0
  while :; do
    port=$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9]*\) -> .*$/\1/p' "$1" | head -1)
    [ -z "$port" ] || break
    kill -0 "$2" 2>/dev/null || return 1
    tries=$((tries+1)); [ "$tries" -lt 30 ] || return 1
    sleep 1
  done
}
forward_port "$private/forward.log" "$forward_pid"
config_cluster=$(kubectl --kubeconfig "$kubeconfig" config view -o jsonpath='{.contexts[0].context.cluster}')
kubectl --kubeconfig "$kubeconfig" config set-cluster "$config_cluster" --server="https://127.0.0.1:$port" >/dev/null
k get --raw=/readyz > "$logs/virtual-ready.txt"
k create namespace "$namespace"
k apply -f deploy/operator/cluster-crd.yaml -f deploy/operator/crd.yaml
k wait --for=condition=Established --timeout=90s crd/rhizaclusters.rhiza.mrchypark.dev crd/rhizarecoveries.rhiza.mrchypark.dev
managed_bucket_setup() {
  k -n "$namespace" port-forward --address=127.0.0.1 service/floci :4566 > "$private/bucket-forward.log" 2>&1 &
  bucket_pid=$!
  forward_port "$private/bucket-forward.log" "$bucket_pid"
  # Reuse the exact actual SDK helper; no ambient provider credentials or cloud.
  (cd scripts/ha-managed-bucket-setup-src && go build -mod=readonly -o "$RUNNER_TEMP/ha-managed-ied-bucket-setup" .)
  "$RUNNER_TEMP/ha-managed-ied-bucket-setup" "http://127.0.0.1:$port" "$bucket"
  kill "$bucket_pid"
  wait "$bucket_pid" 2>/dev/null || :
  bucket_pid=
}
capture_floci() { k -n "$namespace" logs deployment/floci --tail=80 > "$logs/floci.log" 2>&1 || :; }
mapped_host_pod() {
  virtual_pod=$1 virtual_uid=$2
  host_guard
  k -n "$namespace" get pod "$virtual_pod" -o json > "$logs/$fault-virtual-before.json"
  jq -e --arg uid "$virtual_uid" '.metadata.uid==$uid and .metadata.deletionTimestamp==null' "$logs/$fault-virtual-before.json" >/dev/null
  hn get pods -o json | jq --arg name "$virtual_pod" --arg ns "$namespace" --arg uid "$virtual_uid" '[.items[]|select(.metadata.annotations["vcluster.loft.sh/object-name"]==$name and .metadata.annotations["vcluster.loft.sh/object-namespace"]==$ns and .metadata.annotations["vcluster.loft.sh/object-uid"]==$uid)]' > "$logs/$fault-host-before.json"
  jq -e --arg node "$RHIZA_HOST_NODE" 'length==1 and .[0].metadata.deletionTimestamp==null and .[0].spec.nodeName==$node and .[0].spec.nodeSelector["kubernetes.io/hostname"]==$node and any(.[0].status.containerStatuses[]; .name=="sql" and .ready)' "$logs/$fault-host-before.json" >/dev/null
  host_pod=$(jq -r '.[0].metadata.name' "$logs/$fault-host-before.json")
  host_uid=$(jq -r '.[0].metadata.uid' "$logs/$fault-host-before.json")
}
chaos_record_oracle() {
  jq -e --arg id "$2" 'any(.status.conditions[]?; .type=="AllInjected" and .status=="True") and (.status.experiment.containerRecords|length)==1 and all(.status.experiment.containerRecords[]; .id==$id and .injectedCount>0)' "$1" >/dev/null
}
chaos_fault() {
  action=$1 fault=$2
  mapped_host_pod "$3" "$4"
  old_container=$(jq -r '.[0].status.containerStatuses[]|select(.name=="sql")|.containerID' "$logs/$fault-host-before.json")
  [ -n "$old_container" ] && [ "$old_container" != null ]
  expected_record="$RHIZA_HOST_NAMESPACE/$host_pod"
  if [ "$action" = container-kill ]; then expected_record="$expected_record/sql"; fi
  jq -n --arg fault "$fault" --arg ns "$RHIZA_HOST_NAMESPACE" --arg run "$release" --arg action "$action" --arg pod "$host_pod" --arg uid "$4" '{apiVersion:"chaos-mesh.org/v1alpha1",kind:"PodChaos",metadata:{name:$fault,namespace:$ns,labels:{"rhiza-ci-run":$run}},spec:{action:$action,mode:"one",duration:"10s",containerNames:["sql"],selector:{namespaces:[$ns],pods:{($ns):[$pod]},annotationSelectors:{"vcluster.loft.sh/object-uid":$uid}}}}' > "$logs/$fault-request.json"
  hn create -f "$logs/$fault-request.json"
  hn wait --for=condition=AllInjected "podchaos/$fault" --timeout=90s
  hn get "podchaos/$fault" -o json > "$logs/$fault-injected.json"
  chaos_record_oracle "$logs/$fault-injected.json" "$expected_record"
  hn wait --for=condition=AllRecovered "podchaos/$fault" --timeout=90s
  hn get "podchaos/$fault" -o json > "$logs/$fault-recovered.json"
  chaos_record_oracle "$logs/$fault-recovered.json" "$expected_record"
  if [ "$action" = container-kill ]; then
    tries=0
    until hn get pod "$host_pod" -o json > "$logs/$fault-host-after.json" &&
      jq -e --arg uid "$host_uid" --arg old "$old_container" '.metadata.uid==$uid and any(.status.containerStatuses[]; .name=="sql" and .containerID!=$old and .lastState.terminated.containerID==$old)' "$logs/$fault-host-after.json" >/dev/null; do
      tries=$((tries+1)); [ "$tries" -lt 60 ] || exit 1; sleep 1
    done
  else
    hn get pods --field-selector "metadata.name=$host_pod" -o json > "$logs/$fault-host-after.json"
    jq -e '.items|length==0' "$logs/$fault-host-after.json" >/dev/null
  fi
  hn delete "podchaos/$fault" --wait=true --timeout=60s
}
managed_pod_fault() {
  k -n "$namespace" exec "$1" -c sql -- sha256sum /var/lib/rhiza/data/qlog/voter-identity.json > "$logs/old-voter-identity.txt"
  chaos_fault pod-kill pod-kill "$1" "$2"
}
managed_container_fault() {
  k -n "$namespace" exec "$1" -c sql -- sha256sum /var/lib/rhiza/data/qlog/voter-identity.json > "$logs/replacement-voter-identity.txt"
  if cmp -s "$logs/old-voter-identity.txt" "$logs/replacement-voter-identity.txt"; then
    echo 'emptyDir replacement incorrectly reused old voter identity' >&2; exit 1
  fi
  chaos_fault container-kill container-kill "$1" "$2"
}
. scripts/ha-managed-bootstrap-fixture.sh
managed_fixture
# No synthetic virtual delete fallback: common oracle requires new virtual UID
# and a Completed certified replacement after the host Chaos Mesh pod-kill.
run_client insert-after-chaos execute '{"request_id":"chaos-final-insert","sql":"INSERT INTO bootstrap_ci VALUES (2, '\''after-chaos'\'')"}'
run_client query-after-chaos query '{"sql":"SELECT id, value FROM bootstrap_ci ORDER BY id"}'
jq -e '.rows==[[1,"real-kubelet"],[2,"after-chaos"]]' "$logs/query-after-chaos.json" >/dev/null
printf '%s\n' 'PASS: released-image managed HA in isolated IED vcluster; actual host Chaos Mesh pod-kill/container-kill, certified replacement, same-Pod identity retention, NoPVC, ACK readback and new write' 'SCOPE: pinned in-namespace Floci S3; not live GCS, network partition, physical fencing, whole-quorum disaster recovery or performance qualification' > "$logs/result.txt"
