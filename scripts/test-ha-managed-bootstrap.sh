#!/bin/sh
# Actual owned Kind bootstrap/recovery with two scoped Chaos Mesh kill actions.
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
managed_install_mode_valid() {
  case "$1" in
    false) return 0 ;;
    true) [ "$2" = internal ] || { echo 'published install mode requires internal fixture' >&2; return 1; } ;;
    *) echo 'invalid published install mode' >&2; return 1 ;;
  esac
}
if [ "${1-}" = --install-mode-self-test ]; then
  managed_install_mode_valid true internal || exit 1
  managed_install_mode_valid false graph || exit 1
  for invalid_case in graph graph-append-only internal-append-only legacy ''; do
    if managed_install_mode_valid true "$invalid_case" 2>/dev/null; then exit 1; fi
  done
  if managed_install_mode_valid invalid internal 2>/dev/null; then exit 1; fi
  exit 0
fi
cleanup_managed_private() {
  rm -f "$private/runtime.json" "$private/kubeconfig" "$private/curl.conf" "$private/request.body" "$private/admin.token" "$private/runtime-before.json" "$private/runtime-after.json" "$private/chaos-chart.yaml" "$private/chaos-render.jsonl" "$private/chaos-failure.json" "$private/gcs-emulator.json" "$private/managed-install.jsonl" "$private/functional-load.sh" "$private/functional-load-job.json" "$private/functional-stage" "$private/functional-probe.body" "$private/functional-readback.body" || return 1
  rmdir "$private"
}
managed_private_cleanup_self_test() (
  scratch=$(mktemp -d)
  private=$scratch/private
  trap 'rm -f "$private/managed-install.jsonl" "$private/unlisted" "$scratch/error"; rmdir "$private" 2>/dev/null || :; rmdir "$scratch"' EXIT HUP INT TERM
  mkdir "$private"
  # Seed the actual generated fixture filename, not a parallel mock spelling.
  generated=$(sed -n 's/.*> "\$private\/\(managed-install.jsonl\)".*/\1/p' scripts/ha-managed-bootstrap-fixture.sh)
  [ "$generated" = managed-install.jsonl ] || exit 1
  : > "$private/$generated"
  . scripts/ha-managed-scale-qualification.sh
  managed_functional_write_load_script "$private/functional-load.sh"
  managed_functional_load_job test-image > "$private/functional-load-job.json"
  for functional_private in functional-stage functional-probe.body functional-readback.body; do
    : > "$private/$functional_private"
  done
  cleanup_managed_private || exit 1
  [ ! -e "$private" ] || exit 1
  mkdir "$private"
  : > "$private/unlisted"
  if cleanup_managed_private 2> "$scratch/error"; then exit 1; fi
  [ -f "$private/unlisted" ] || exit 1
)
if [ "${1-}" = --private-cleanup-self-test ]; then
  managed_private_cleanup_self_test
  exit 0
fi
managed_first_pod_fault_self_test() (
  scratch=$(mktemp -d)
  trap 'rm -f "$scratch"/*.json "$scratch"/*.txt "$scratch"/*.exit; rmdir "$scratch"' EXIT HUP INT TERM
  namespace=test-ns release=test-run
  . scripts/ha-managed-podchaos.sh
  chaos_scope_guard() { [ "$scope_ok" = 1 ]; }
  chaos_k() {
    [ "$*" = 'get podchaos/pod-kill --ignore-not-found -o json' ] || return 97
    [ "$get_present" = 0 ] || printf '%s\n' '{"kind":"PodChaos"}'
    return "$get_code"
  }
  for mutation in valid empty-other extra-empty missing-proof nonempty-absence get-error get-still-present wrong-scope old-uid-running wrong-uid; do
    rm -f "$scratch"/first-*.json "$scratch"/pod-kill-unexpected.json "$scratch"/pod-kill-after-delete-recheck.txt "$scratch"/pod-kill-after-delete-recheck.exit
    scope_ok=1 get_code=0 get_present=0 proof_uid=old-uid
    managed_chaos_request pod-kill sql-pod > "$scratch/pod-kill-request.json"
    jq -n '{metadata:{namespace:"test-ns",name:"sql-pod",uid:"old-uid"},status:{containerStatuses:[{name:"sql",containerID:"containerd://old-container"}]}}' > "$scratch/pod-kill-pod-before.json"
    cp "$scratch/pod-kill-pod-before.json" "$scratch/pod-kill-runtime-before.json"
    jq -n '{containers:[{id:"old-container",state:"CONTAINER_RUNNING",labels:{"io.kubernetes.pod.uid":"old-uid"}}]}' > "$scratch/pod-kill-cri-before.json"
    printf '%s\n' '{"containers":[]}' > "$scratch/pod-kill-cri-after.json"
    jq -n '{status:{conditions:[{type:"AllInjected",status:"True"}],experiment:{containerRecords:[{id:"test-ns/sql-pod",phase:"Injected",injectedCount:1,recoveredCount:0}]}}}' > "$scratch/pod-kill-injected.json"
    cp "$scratch/pod-kill-injected.json" "$scratch/pod-kill-before-delete.json"
    printf '%s\n' '{"items":[{"metadata":{"uid":"new-uid"}}]}' > "$scratch/pod-kill-pods-after.json"
    : > "$scratch/pod-kill-after-delete.json"
    case "$mutation" in
      valid) ;;
      empty-other) : > "$scratch/pod-kill-injected.json" ;;
      extra-empty) : > "$scratch/pod-kill-unexpected.json" ;;
      missing-proof) rm "$scratch/pod-kill-cri-after.json" ;;
      nonempty-absence) printf '{}\n' > "$scratch/pod-kill-after-delete.json" ;;
      get-error) get_code=41 ;;
      get-still-present) get_present=1 ;;
      wrong-scope) scope_ok=0 ;;
      old-uid-running) cp "$scratch/pod-kill-cri-before.json" "$scratch/pod-kill-cri-after.json" ;;
      wrong-uid) proof_uid=other-uid ;;
    esac
    if managed_preserve_first_pod_fault "$scratch" sql-pod "$proof_uid"; then
      [ "$mutation" = valid ] || { echo "accepted invalid first fault proof: $mutation" >&2; return 1; }
      [ -f "$scratch/first-pod-kill-after-delete.json" ] && [ ! -s "$scratch/first-pod-kill-after-delete.json" ] || return 1
      [ "$(cat "$scratch/pod-kill-after-delete-recheck.exit")" = 0 ] || return 1
      for suffix in request pod-before runtime-before cri-before cri-after injected pods-after before-delete; do
        [ -s "$scratch/first-pod-kill-$suffix.json" ] || return 1
      done
    else
      code=$?
      [ "$mutation" != valid ] || return "$code"
      [ ! -e "$scratch/first-pod-kill-request.json" ] || return 1
      if [ "$mutation" = get-error ]; then
        [ "$code" = 41 ] && [ "$(cat "$scratch/pod-kill-after-delete-recheck.exit")" = 41 ] || return 1
      fi
    fi
  done
)

managed_completed_repair_oracle_self_test() {
  scratch=$(mktemp -d)
  trap 'rm -f "$scratch"/repair.json "$scratch"/mismatch.json; rmdir "$scratch"' EXIT HUP INT TERM
  jq -n '{status:{applicationInit:{plan:{applicationProtocol:"rhiza-managed-request-append-only/v1",evidenceFormat:"rhiza-managed-request-append-only/v1"}},repair:{state:"Completed",plan:{sequence:"addThenRemove",operationID:"op-2",expectedConfigID:3,expectedAbortSlot:7,oldNodeID:"old-2",replacement:{podUID:"pod-2",nodeID:"node-2"},finalVoterIDs:["node-2","voter-a","voter-b"]},preparedHandoff:{planDigest:("cd"*32),preparedConfig:{config_id:3,members:[{node_id:"old-2"},{node_id:"voter-a"},{node_id:"voter-b"}]},preparedSlot:8,preparedHash:("ef"*32)},addCompletion:{kind:"Terminal",operationID:"op-2",expectedConfigID:3,expectedAbortSlot:7,configID:4,slot:12,hash:("01"*32),planDigest:("cd"*32),finalVoterIDs:["node-2","old-2","voter-a","voter-b"]},completion:{kind:"Terminal",operationID:"op-2",expectedConfigID:3,expectedAbortSlot:7,configID:5,slot:17,hash:("ab"*32),planDigest:("cd"*32),finalVoterIDs:["node-2","voter-a","voter-b"]}}}}' > "$scratch/repair.json"
  managed_completed_repair_oracle "$scratch/repair.json" old-2 old-2 op-1
  jq '.status.applicationInit.plan.applicationProtocol="rhiza-managed-request-graph-append-only/v1" | .status.applicationInit.plan.evidenceFormat="rhiza-managed-request-graph-append-only/v1"' "$scratch/repair.json" > "$scratch/mismatch.json"
  managed_completed_repair_oracle "$scratch/mismatch.json" old-2 old-2 op-1
  if managed_completed_repair_oracle "$scratch/repair.json" old-2 old-1 op-1; then return 1; fi
  if managed_completed_repair_oracle "$scratch/repair.json" old-2 old-2 op-2; then return 1; fi
  jq '.status.repair.completion.finalVoterIDs=["old-2","voter-a","voter-b"]' "$scratch/repair.json" > "$scratch/mismatch.json"
  if managed_completed_repair_oracle "$scratch/mismatch.json" old-2 old-2 op-1; then return 1; fi
  for mutation in duplicate missing-proof wrong-config stale-slot retired-profile remove-first missing-add malformed-add malformed-add-object wrong-add-operation wrong-add-digest wrong-add-config add-before-source remove-before-add wrong-add-roster wrong-prepared-config missing-prepared; do
    case "$mutation" in
      duplicate) filter='.status.repair.plan.finalVoterIDs=["node-2","node-2","voter-a"] | .status.repair.completion.finalVoterIDs=.status.repair.plan.finalVoterIDs' ;;
      missing-proof) filter='del(.status.repair.completion.hash)' ;;
      wrong-config) filter='.status.repair.completion.configID=4' ;;
      stale-slot) filter='.status.repair.completion.slot=7' ;;
      retired-profile) filter='.status.applicationInit.plan.applicationProtocol="rhiza-managed-sql-kv/v1" | .status.applicationInit.plan.evidenceFormat="rhiza-managed-sql-kv/v1"' ;;
      remove-first) filter='.status.repair.plan.sequence="removeThenAdd"' ;;
      missing-add) filter='del(.status.repair.addCompletion)' ;;
      malformed-add) filter='.status.repair.addCompletion.hash=null' ;;
      malformed-add-object) filter='.status.repair.addCompletion="invalid"' ;;
      wrong-add-operation) filter='.status.repair.addCompletion.operationID="other"' ;;
      wrong-add-digest) filter='.status.repair.addCompletion.planDigest=("00"*32)' ;;
      wrong-add-config) filter='.status.repair.addCompletion.configID=5' ;;
      add-before-source) filter='.status.repair.addCompletion.slot=8' ;;
      remove-before-add) filter='.status.repair.completion.slot=12' ;;
      wrong-add-roster) filter='.status.repair.addCompletion.finalVoterIDs=["node-2","old-2","old-2","voter-a"]' ;;
      wrong-prepared-config) filter='.status.repair.preparedHandoff.preparedConfig.config_id=4' ;;
      missing-prepared) filter='del(.status.repair.preparedHandoff)' ;;
    esac
    jq "$filter" "$scratch/repair.json" > "$scratch/mismatch.json"
    if managed_completed_repair_oracle "$scratch/mismatch.json" old-2 old-2 op-1; then return 1; fi
  done
  rm -f "$scratch/repair.json" "$scratch/mismatch.json"
  rmdir "$scratch"
  trap - EXIT HUP INT TERM
}
if [ "${1-}" = --oracle-self-test ]; then
  . scripts/ha-managed-bootstrap-fixture.sh
  managed_first_pod_fault_self_test
  managed_completed_repair_oracle_self_test
  (cd scripts/ha-managed-bucket-setup-src && go test -mod=readonly -timeout=45s ./...)
  exit 0
fi
if [ "${1-}" = --pod-fault-proof-self-test ]; then
  . scripts/ha-managed-bootstrap-fixture.sh
  managed_first_pod_fault_self_test
  exit 0
fi
published_install_only=${RHIZA_MANAGED_PUBLISHED_INSTALL_ONLY-false}
functional_scenario=${RHIZA_MANAGED_FUNCTIONAL_SCENARIO-}
case "$functional_scenario" in
  ''|scale-up|quorum) ;;
  *) echo 'invalid RHIZA_MANAGED_FUNCTIONAL_SCENARIO' >&2; exit 1 ;;
esac
managed_install_mode_valid "$published_install_only" "${RHIZA_MANAGED_FIXTURE_CASE-internal}"
[ "${GITHUB_ACTIONS-}" = true ] || { echo 'GitHub Actions only' >&2; exit 1; }
: "${RUNNER_TEMP:?}" "${GITHUB_SHA:?}" "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}"
fixture_provider=${RHIZA_MANAGED_FIXTURE_PROVIDER-s3}
case "$fixture_provider" in s3|gcs|azure) ;; *) echo 'unsupported explicit emulator provider' >&2; exit 1 ;; esac
logs="$RUNNER_TEMP/ha-managed-bootstrap"
if [ "$published_install_only" = true ]; then logs="$RUNNER_TEMP/ha-managed-published-install"; fi
mkdir -p "$logs"
private=$(mktemp -d "$RUNNER_TEMP/ha-managed-private.XXXXXX")
nonce=$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')
cluster="ha-managed-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}-$nonce"
namespace=managed-bootstrap
image="rhiza-managed:$GITHUB_SHA-$nonce"
if [ "$published_install_only" = true ]; then
  image=ghcr.io/mrchypark/rhiza-operator@sha256:2f7752353aa8796a074cf8cf3d91102f206d30cc0628cfd5fff4fcd37e555f40
fi
kubeconfig="$private/kubeconfig"
owned=0
built=0
fault_name=
release="rhiza-kind-chaos-$nonce"
control=managed-chaos-control
k() { kubectl --kubeconfig "$kubeconfig" --context "kind-$cluster" --request-timeout=30s "$@"; }
chaos_k() { k -n "$namespace" "$@"; }
chaos_scope_guard() {
  [ "$(k get namespace "$namespace" -o jsonpath='{.metadata.uid}')" = "$chaos_namespace_uid" ]
}
. scripts/ha-managed-podchaos.sh
# Unlike API deletion, these CRI observations cover the old runtime identity.
kind_runtime_oracle() {
  jq -e --arg uid "$2" --arg id "$3" --arg action "$4" --arg replacement "${5-}" '
    (.containers|type)=="array" and
    all(.containers[]; (.id|type)=="string" and (.id|length)>0 and
      (.state=="CONTAINER_RUNNING" or .state=="CONTAINER_EXITED" or .state=="CONTAINER_CREATED" or .state=="CONTAINER_UNKNOWN") and
      (.state!="CONTAINER_RUNNING" or ((.labels["io.kubernetes.pod.uid"]|type)=="string" and (.labels["io.kubernetes.pod.uid"]|length)>0))) and
    all(.containers[]; .id!=$id or .state=="CONTAINER_EXITED") and
    all(.containers[];
      .state!="CONTAINER_RUNNING" or .labels["io.kubernetes.pod.uid"]!=$uid or
      ($action=="container-kill" and $replacement!="" and .labels["io.kubernetes.container.name"]=="sql" and .id==$replacement and .id!=$id)) and
    ($action!="container-kill" or ([.containers[]|select(.state=="CONTAINER_RUNNING" and .labels["io.kubernetes.pod.uid"]==$uid and .id==$replacement)]|length)==1)
  ' "$1" >/dev/null
}
kind_node_guard() {
  [ "$runtime_node" = "$cluster-control-plane" ] &&
  [ "$(docker inspect --format '{{index .Config.Labels "io.x-k8s.kind.cluster"}}' "$runtime_node")" = "$cluster" ] &&
  [ "$(docker inspect --format '{{index .Config.Labels "io.x-k8s.kind.role"}}' "$runtime_node")" = control-plane ]
}
kind_runtime_before() {
  k -n "$namespace" get pod "$1" -o json > "$logs/$3-runtime-before.json"
  jq -e --arg uid "$2" '.metadata.uid==$uid and .metadata.deletionTimestamp==null' "$logs/$3-runtime-before.json" >/dev/null
  runtime_node=$(jq -er '.spec.nodeName' "$logs/$3-runtime-before.json")
  runtime_old=$(jq -er --arg container "${4-sql}" '.status.containerStatuses[]|select(.name==$container)|.containerID|select(test("^containerd://[a-f0-9]{64}$"))|sub("^containerd://";"")' "$logs/$3-runtime-before.json")
  kind_node_guard
  docker exec "$runtime_node" crictl --runtime-endpoint unix:///run/containerd/containerd.sock ps -a -o json > "$private/runtime-before.json"
  jq -e --arg id "$runtime_old" --arg uid "$2" --arg container "${4-sql}" 'any(.containers[]; .id==$id and .state=="CONTAINER_RUNNING" and .labels["io.kubernetes.pod.uid"]==$uid and .labels["io.kubernetes.container.name"]==$container)' "$private/runtime-before.json" >/dev/null
  jq --arg uid "$2" '{containers:[.containers[]|select(.labels["io.kubernetes.pod.uid"]==$uid)|{id,state,labels:{"io.kubernetes.pod.uid":.labels["io.kubernetes.pod.uid"],"io.kubernetes.container.name":.labels["io.kubernetes.container.name"]}}]}' "$private/runtime-before.json" > "$logs/$3-cri-before.json"
}
kind_runtime_after() {
  runtime_tries=0
  runtime_new=
  if [ "$1" = container-kill ]; then
    runtime_new=$(jq -er '.status.containerStatuses[]|select(.name=="sql")|.containerID|select(test("^containerd://[a-f0-9]{64}$"))|sub("^containerd://";"")' "$logs/container-kill-pod-after.json")
  fi
  while :; do
    kind_node_guard
    docker exec "$runtime_node" crictl --runtime-endpoint unix:///run/containerd/containerd.sock ps -a -o json > "$private/runtime-after.json"
    if kind_runtime_oracle "$private/runtime-after.json" "$2" "$runtime_old" "$1" "$runtime_new"; then break; fi
    runtime_tries=$((runtime_tries+1)); [ "$runtime_tries" -lt 60 ] || return 1; sleep 1
  done
  jq --arg uid "$2" '{containers:[.containers[]|select(.labels["io.kubernetes.pod.uid"]==$uid)|{id,state,labels:{"io.kubernetes.pod.uid":.labels["io.kubernetes.pod.uid"],"io.kubernetes.container.name":.labels["io.kubernetes.container.name"]}}]}' "$private/runtime-after.json" > "$logs/$1-cri-after.json"
}
kind_chaos_profile_valid() {
  jq -s -e --arg ns "$namespace" --argjson controllers "$controllers" '
    [.[]|select(.kind=="Deployment" and .metadata.name=="chaos-controller-manager")|.spec.template.spec.containers[]|select(.name=="chaos-mesh")|.env|map(select(has("value"))|{key:.name,value:.value})|from_entries] as $envs |
    ($envs|length)==1 and $envs[0].ENABLED_CONTROLLERS==($controllers|join(",")) and
    $envs[0].CLUSTER_SCOPED=="false" and $envs[0].TARGET_NAMESPACE==$ns and ($ns|length)>0 and
    $envs[0].ENABLE_FILTER_NAMESPACE=="false" and $envs[0].ENABLED_WEBHOOKS=="podchaos" and
    ([.[]|select(.kind=="RoleBinding" and (.metadata.name|endswith("-chaos-controller-manager-target-namespace")))]|length==1 and .[0].metadata.namespace==$ns and .[0].roleRef.kind=="ClusterRole" and .[0].roleRef.name==.[0].metadata.name) and
    all(.[]|select(.kind=="ClusterRoleBinding");(.roleRef.name|endswith("-chaos-controller-manager-target-namespace"))|not) and
    any(.[];.kind=="DaemonSet" and .metadata.name=="chaos-daemon" and .spec.template.spec.hostPID==false and
      .spec.template.spec.containers[0].securityContext=={privileged:false,allowPrivilegeEscalation:false,capabilities:{drop:["ALL"]}} and
      any(.spec.template.spec.volumes[]; .name=="socket-path" and .hostPath.path=="/run/containerd") and
      any(.spec.template.spec.containers[0].volumeMounts[]; .name=="socket-path" and .mountPath=="/host-run") and
      ((.spec.template.spec.containers[0].command|index("--runtime-socket-path")) as $socketArg |
        $socketArg!=null and .spec.template.spec.containers[0].command[$socketArg+1]=="/host-run/containerd.sock") and
      all(.spec.template.spec.volumes[]; .hostPath.path!="/sys" and .hostPath.path!="/lib/modules")) and
    all(.[];.metadata.labels["app.kubernetes.io/component"]!="chaos-dashboard" and .metadata.labels["app.kubernetes.io/component"]!="dns-server")
  ' "$1" >/dev/null
}
install_kind_chaos() {
  command -v helm >/dev/null && command -v yq >/dev/null
  helm version --short > "$logs/helm-version.txt"
  yq --version > "$logs/yq-version.txt"
  controllers='["awschaos-records","azurechaos-records","blockchaos-records","dnschaos-records","gcpchaos-records","httpchaos-records","iochaos-records","jvmchaos-records","kernelchaos-records","networkchaos-records","physicalmachinechaos-records","podchaos-records","stresschaos-records","timechaos-records","podchaos-initFinalizers","podchaos-desiredphase","podchaos-condition","podchaos-cleanFinalizers"]'
  docker exec "$cluster-control-plane" test -S /run/containerd/containerd.sock
  k create namespace "$control"
  helm template "$release" chaos-mesh --repo https://charts.chaos-mesh.org --version 2.8.7 --include-crds --namespace "$control" \
    --set clusterScoped=false --set controllerManager.targetNamespace="$namespace" \
    --set controllerManager.enableFilterNamespace=false --set controllerManager.replicaCount=1 \
    --set-json "controllerManager.enabledControllers=$controllers" --set-json 'controllerManager.enabledWebhooks=["podchaos"]' \
    --set dashboard.create=false --set dnsServer.create=false --set enableProfiling=false --set bpfki.create=false \
    --set chaosDaemon.runtime=containerd --set chaosDaemon.socketPath=/run/containerd/containerd.sock \
    --set chaosDaemon.privileged=false --set-json 'chaosDaemon.capabilities={"drop":["ALL"]}' > "$private/chaos-chart.yaml"
  yq -o=json -I=0 '
    select(.kind != null) |
    select(.metadata.labels."app.kubernetes.io/component" != "chaos-dashboard" and .metadata.labels."app.kubernetes.io/component" != "dns-server") |
    (select(.kind == "DaemonSet" and .metadata.name == "chaos-daemon") | .spec.template.spec.hostPID) = false |
    (select(.kind == "DaemonSet" and .metadata.name == "chaos-daemon") | .spec.template.spec.containers[0].securityContext) = {"privileged":false,"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]}} |
    (select(.kind == "DaemonSet" and .metadata.name == "chaos-daemon") | .spec.template.spec.containers[0].volumeMounts) |= map(select(.name != "sys-path" and .name != "lib-modules")) |
    (select(.kind == "DaemonSet" and .metadata.name == "chaos-daemon") | .spec.template.spec.volumes) |= map(select(.name != "sys-path" and .name != "lib-modules"))
  ' "$private/chaos-chart.yaml" > "$private/chaos-render.jsonl"
  kind_chaos_profile_valid "$private/chaos-render.jsonl"
  sha256sum "$private/chaos-render.jsonl" > "$logs/chaos-manifest.sha256"
  k create -f "$private/chaos-render.jsonl" > "$logs/chaos-create.txt"
  k wait --for=condition=Established crd/podchaos.chaos-mesh.org --timeout=90s
  k -n "$control" rollout status deployment/chaos-controller-manager --timeout=180s
  k -n "$control" rollout status daemonset/chaos-daemon --timeout=180s
}
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
    if [ "$component" = operator ]; then log_selector=app.kubernetes.io/name=rhiza-operator
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
  printf '%s\n' "$result" > "$logs/driver-original.exit"
  if [ "$owned" = 1 ]; then
    if [ -n "$fault_name" ]; then
      capture_chaos_failure "$fault_name" cleanup || :
      chaos_k delete "podchaos/$fault_name" --wait=true --timeout=45s > "$logs/fault-cleanup.txt" 2>&1 || result=1
    fi
    if [ "$published_install_only" = false ]; then
    for chaos_component in controller-manager chaos-daemon; do
      k -n "$control" logs -l "app.kubernetes.io/component=$chaos_component" --all-containers --tail=200 --limit-bytes=65536 > "$logs/$chaos_component-current.log" 2>&1 || :
      k -n "$control" logs -l "app.kubernetes.io/component=$chaos_component" --all-containers --previous --tail=200 --limit-bytes=65536 > "$logs/$chaos_component-previous.log" 2>&1 || :
    done
    fi
    if [ -n "$functional_scenario" ] && command -v managed_functional_capture >/dev/null 2>&1; then
      managed_functional_capture || :
    fi
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
    docker ps -a --filter "label=io.x-k8s.kind.cluster=$cluster" --format '{{.ID}}' > "$logs/kind-containers-after.txt" 2>&1 || result=1
    [ ! -s "$logs/kind-containers-after.txt" ] || result=1
  fi
  if [ "$built" = 1 ]; then
    docker image rm "$image" > "$logs/image-cleanup.txt" 2>&1 || result=1
    if docker image inspect "$image" >/dev/null 2>&1; then result=1; fi
  fi
  cleanup_managed_private || result=1
  printf '%s\n' "$result" > "$logs/driver.exit"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
kind get clusters > "$logs/clusters-before.txt" 2>&1 || true
if grep -Fx "$cluster" "$logs/clusters-before.txt" >/dev/null; then exit 1; fi
owned=1
kind create cluster --name "$cluster" --kubeconfig "$kubeconfig" --wait 120s > "$logs/create.txt" 2>&1
if [ "$published_install_only" = false ]; then
  built=1
  docker build --network host -f "${RHIZA_MANAGED_TEST_DOCKERFILE:-Dockerfile.operator}" -t "$image" . > "$logs/build.txt" 2>&1
  kind load docker-image --name "$cluster" "$image" > "$logs/load.txt" 2>&1
fi
k create namespace "$namespace"
chaos_namespace_uid=$(k get namespace "$namespace" -o jsonpath='{.metadata.uid}')
if [ "$published_install_only" = false ]; then install_kind_chaos; fi
k apply -f deploy/operator/cluster-crd.yaml -f deploy/operator/crd.yaml
k wait --for=condition=Established --timeout=90s crd/rhizaclusters.rhiza.mrchypark.dev crd/rhizarecoveries.rhiza.mrchypark.dev
managed_bucket_setup() {
  "$RHIZA_MANAGED_BUCKET_SETUP" "$kubeconfig" "kind-$cluster" "$namespace" "$bucket" "$fixture_provider"
  if [ "$fixture_provider" = gcs ]; then
    "$RHIZA_MANAGED_BUCKET_SETUP" emulator-service-account "$private/gcs-emulator.json"
  fi
}
managed_pod_fault() {
  kind_runtime_before "$1" "$2" pod-kill
  direct_chaos_fault pod-kill pod-kill "$1" "$2"
  kind_runtime_after pod-kill "$2"
}
managed_container_fault() {
  kind_runtime_before "$1" "$2" container-kill
  direct_chaos_fault container-kill container-kill "$1" "$2"
  kind_runtime_after container-kill "$2"
  managed_chaos_verified=1
}
. scripts/ha-managed-bootstrap-fixture.sh
if [ -n "$functional_scenario" ]; then
  . scripts/ha-managed-scale-qualification.sh
fi
managed_fixture
