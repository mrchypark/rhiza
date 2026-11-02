#!/bin/sh
# Actual owned Kind bootstrap/recovery with two scoped Chaos Mesh kill actions.
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
if [ "${1-}" = --oracle-self-test ]; then
  (cd scripts/ha-managed-bucket-setup-src && go test -mod=readonly -timeout=45s -run 'Test(GatedOracle|CommittedPlanOracle)$' .)
  exit 0
fi
[ "${GITHUB_ACTIONS-}" = true ] || { echo 'GitHub Actions only' >&2; exit 1; }
: "${RUNNER_TEMP:?}" "${GITHUB_SHA:?}" "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}"
fixture_provider=${RHIZA_MANAGED_FIXTURE_PROVIDER-s3}
case "$fixture_provider" in s3|gcs|azure) ;; *) echo 'unsupported explicit emulator provider' >&2; exit 1 ;; esac
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
  runtime_old=$(jq -er '.status.containerStatuses[]|select(.name=="sql")|.containerID|select(test("^containerd://[a-f0-9]{64}$"))|sub("^containerd://";"")' "$logs/$3-runtime-before.json")
  kind_node_guard
  docker exec "$runtime_node" crictl --runtime-endpoint unix:///run/containerd/containerd.sock ps -a -o json > "$private/runtime-before.json"
  jq -e --arg id "$runtime_old" --arg uid "$2" 'any(.containers[]; .id==$id and .state=="CONTAINER_RUNNING" and .labels["io.kubernetes.pod.uid"]==$uid and .labels["io.kubernetes.container.name"]=="sql")' "$private/runtime-before.json" >/dev/null
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
  printf '%s\n' "$result" > "$logs/driver-original.exit"
  if [ "$owned" = 1 ]; then
    if [ -n "$fault_name" ]; then
      capture_chaos_failure "$fault_name" cleanup || :
      chaos_k delete "podchaos/$fault_name" --wait=true --timeout=45s > "$logs/fault-cleanup.txt" 2>&1 || result=1
    fi
    for chaos_component in controller-manager chaos-daemon; do
      k -n "$control" logs -l "app.kubernetes.io/component=$chaos_component" --all-containers --tail=200 --limit-bytes=65536 > "$logs/$chaos_component-current.log" 2>&1 || :
      k -n "$control" logs -l "app.kubernetes.io/component=$chaos_component" --all-containers --previous --tail=200 --limit-bytes=65536 > "$logs/$chaos_component-previous.log" 2>&1 || :
    done
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
  rm -f "$private/runtime.json" "$private/kubeconfig" "$private/curl.conf" "$private/request.body" "$private/admin.token" "$private/runtime-before.json" "$private/runtime-after.json" "$private/chaos-chart.yaml" "$private/chaos-render.jsonl" "$private/chaos-failure.json" "$private/gcs-emulator.json"
  rmdir "$private" || result=1
  printf '%s\n' "$result" > "$logs/driver.exit"
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
chaos_namespace_uid=$(k get namespace "$namespace" -o jsonpath='{.metadata.uid}')
install_kind_chaos
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
managed_fixture
