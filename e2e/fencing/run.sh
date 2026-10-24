#!/bin/sh
# Disposable Kind only. Images and the Go driver are built by GitHub Actions.
set -eu

profile_field_valid() {
  case "${1:-}" in
    ''|*[!A-Za-z0-9._+-]*) return 1 ;;
    *) return 0 ;;
  esac
}

crictl_version_field() {
  printf '%s\n' "$2" | awk -F: -v field="$1" '
    $1 == field {
      value = substr($0, index($0, ":") + 1)
      sub(/^[[:space:]]+/, "", value)
      sub(/[[:space:]]+$/, "", value)
      count++
      result = value
    }
    END {
      if (count != 1 || result == "") exit 1
      print result
    }'
}

if [ "${FENCING_PROFILE_SELFTEST:-}" = 1 ]; then
  profile_field_valid containerd
  profile_field_valid 1.7.27
  profile_field_valid io.containerd.runc.v2
  profile_field_valid 6.12.0
  if profile_field_valid '' || profile_field_valid 'runtime:type'; then
    echo 'runtime profile validator accepted an invalid field' >&2
    exit 1
  fi
  fencing_crictl_fixture='Version:  0.1.0
RuntimeName:  containerd
RuntimeVersion:  1.7.27
RuntimeApiVersion:  v1'
  [ "$(crictl_version_field RuntimeName "$fencing_crictl_fixture")" = containerd ]
  [ "$(crictl_version_field RuntimeVersion "$fencing_crictl_fixture")" = 1.7.27 ]
  [ "$(crictl_version_field RuntimeApiVersion "$fencing_crictl_fixture")" = v1 ]
  if crictl_version_field RuntimeName 'RuntimeName: containerd
RuntimeName: containerd' >/dev/null; then
    echo 'runtime profile parser accepted duplicate fields' >&2
    exit 1
  fi
  exit 0
fi

test -n "${FENCING_E2E_BIN:-}" || { echo 'FENCING_E2E_BIN is required' >&2; exit 2; }
test -x "$FENCING_E2E_BIN"
fencing_passive_kernel=${RHIZA_RUNTIME_AGENT_PASSIVE_KERNEL:-no}
fencing_sandbox_executable_sha256=${RHIZA_QUALIFIED_SANDBOX_EXECUTABLE_SHA256:-}
case "$fencing_passive_kernel" in
  no) ;;
  yes)
    [ -n "$fencing_sandbox_executable_sha256" ] || {
      echo 'passive kernel proof requires an independently approved sandbox executable digest' >&2
      exit 2
    }
    ;;
  *) echo 'RHIZA_RUNTIME_AGENT_PASSIVE_KERNEL must be yes or no' >&2; exit 2 ;;
esac
if [ -n "$fencing_sandbox_executable_sha256" ]; then
  case "$fencing_sandbox_executable_sha256" in *[!0-9a-f]*) echo 'sandbox executable approval must be lowercase SHA-256 hex' >&2; exit 2 ;; esac
  [ "${#fencing_sandbox_executable_sha256}" -eq 64 ] || { echo 'sandbox executable approval must be 64 lowercase hex characters' >&2; exit 2; }
fi
fencing_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$fencing_root"
fencing_cluster=rhiza-generic-fencing
fencing_context=kind-rhiza-generic-fencing
fencing_namespace=rhiza-fencing-e2e
fencing_node_image='kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5'
FENCING_E2E_OUT=${FENCING_E2E_OUT:-generic-fencing-results}
export FENCING_E2E_OUT
mkdir -p "$FENCING_E2E_OUT"
git rev-parse HEAD >"$FENCING_E2E_OUT/source-head.txt"
docker image inspect rhiza-fencing-host:local rhiza-fencing-operator:local --format '{{.Id}}' >"$FENCING_E2E_OUT/image-ids.txt"
if kind get clusters | grep -Fx "$fencing_cluster" >/dev/null; then
  echo 'Refusing to replace an existing qualification cluster' >&2
  exit 2
fi
fencing_tmp=$(mktemp -d)
fencing_owned=false
fencing_source_sts_uid=''
k() { kubectl --context "$fencing_context" -n "$fencing_namespace" "$@"; }
capture_voter_startup_diagnostics() (
  umask 077
  voter_diagnostics_dir="$FENCING_E2E_OUT/private-voter-diagnostics"
  (umask 077 && mkdir -m 700 "$voter_diagnostics_dir") || return 1
  pinned_uid=$fencing_source_sts_uid
  if [ -z "$pinned_uid" ]; then
    printf '%s\n' '{"statefulSetIdentity":"unavailable","voters":[]}' >"$FENCING_E2E_OUT/voter-startup-private.json"
    chmod 600 "$FENCING_E2E_OUT/voter-startup-private.json"
    return 0
  fi
  for ordinal in 0 1 2; do
    pod="rhiza-$ordinal"
    pod_dir="$voter_diagnostics_dir/$pod"
    (umask 077 && mkdir -m 700 "$pod_dir") || return 1
    printf '%s\n' '{}' >"$pod_dir/status.json"
    before_sts=$(k --request-timeout=10s get statefulset rhiza -o json 2>"$pod_dir/sts-before.stderr" || true)
    before_pod=$(k --request-timeout=10s get pod "$pod" -o json 2>"$pod_dir/pod-before.stderr" || true)
    identity_status=unavailable
    pod_uid=''
    container_id=''
    restart_count=''
    if [ -n "$before_sts" ] && [ -n "$before_pod" ] &&
      printf '%s\n' "$before_sts" | jq -e --arg uid "$pinned_uid" '.metadata.uid == $uid' >/dev/null 2>&1 &&
      pod_uid=$(printf '%s\n' "$before_pod" | jq -er --arg name "$pod" --arg ns "$fencing_namespace" --arg owner "$pinned_uid" \
        'select(.metadata.name == $name and .metadata.namespace == $ns) | select(any(.metadata.ownerReferences[]?; .controller == true and .kind == "StatefulSet" and .name == "rhiza" and .uid == $owner)) | .metadata.uid | select(type == "string" and length > 0)' 2>/dev/null) &&
      container_id=$(printf '%s\n' "$before_pod" | jq -er '[.status.containerStatuses[]? | select(.name == "rhiza")] | if length == 1 then .[0].containerID // "" else "" end' 2>/dev/null) &&
      restart_count=$(printf '%s\n' "$before_pod" | jq -er '[.status.containerStatuses[]? | select(.name == "rhiza")] | if length == 1 then (.[0].restartCount | tostring) else "" end' 2>/dev/null); then
      identity_status=verified
      printf '%s\n' "$before_pod" | jq -ce --arg name "$pod" --arg ns "$fencing_namespace" --arg owner "$pinned_uid" \
        '[select(.metadata.name == $name and .metadata.namespace == $ns) | select(any(.metadata.ownerReferences[]?; .controller == true and .kind == "StatefulSet" and .name == "rhiza" and .uid == $owner)) | {
          phase:(.status.phase // "Unknown"),
          conditions:[.status.conditions[]? | {type,status,reason}],
          containers:[.status.containerStatuses[]? | select(.name == "rhiza") | {ready,restartCount,
            state:{waiting:{reason:.state.waiting.reason},terminated:{reason:.state.terminated.reason,exitCode:.state.terminated.exitCode,signal:.state.terminated.signal}},
            lastState:{terminated:{reason:.lastState.terminated.reason,exitCode:.lastState.terminated.exitCode,signal:.lastState.terminated.signal}}}]
        }] | if length == 1 then .[0] else error("identity mismatch") end' >"$pod_dir/status.json" 2>"$pod_dir/status.stderr" || identity_status=unavailable
    fi
    logs_ok=false
    if [ "$identity_status" = verified ] && [ -n "$container_id" ]; then
      if k --request-timeout=10s logs "$pod" -c rhiza --tail=80 --limit-bytes=16384 >"$pod_dir/current.log" 2>"$pod_dir/current.stderr"; then current_ok=true; else current_ok=false; fi
      if k --request-timeout=10s logs "$pod" -c rhiza --previous --tail=80 --limit-bytes=16384 >"$pod_dir/previous.log" 2>"$pod_dir/previous.stderr"; then previous_ok=true; else previous_ok=false; fi
      after_sts=$(k --request-timeout=10s get statefulset rhiza -o json 2>"$pod_dir/sts-after.stderr" || true)
      after_pod=$(k --request-timeout=10s get pod "$pod" -o json 2>"$pod_dir/pod-after.stderr" || true)
      after_container=''
      after_restarts=''
      if [ -n "$after_pod" ]; then
        after_container=$(printf '%s\n' "$after_pod" | jq -er '[.status.containerStatuses[]? | select(.name == "rhiza")] | if length == 1 then .[0].containerID // "" else "" end' 2>/dev/null || true)
        after_restarts=$(printf '%s\n' "$after_pod" | jq -er '[.status.containerStatuses[]? | select(.name == "rhiza")] | if length == 1 then (.[0].restartCount | tostring) else "" end' 2>/dev/null || true)
      fi
      if [ "$after_container" = "$container_id" ] && [ "$after_restarts" = "$restart_count" ] &&
        printf '%s\n' "$after_sts" | jq -e --arg uid "$pinned_uid" '.metadata.uid == $uid' >/dev/null 2>&1 &&
        printf '%s\n' "$after_pod" | jq -e --arg poduid "$pod_uid" --arg name "$pod" --arg ns "$fencing_namespace" --arg owner "$pinned_uid" \
          '.metadata.uid == $poduid and .metadata.name == $name and .metadata.namespace == $ns and any(.metadata.ownerReferences[]?; .controller == true and .kind == "StatefulSet" and .name == "rhiza" and .uid == $owner)' >/dev/null 2>&1; then
        logs_ok=true
        printf '%s\n' "$current_ok" >"$pod_dir/current.captured"
        printf '%s\n' "$previous_ok" >"$pod_dir/previous.captured"
      else
        identity_status=identity_drift
        : >"$pod_dir/current.log"
        : >"$pod_dir/previous.log"
      fi
    else
      : >"$pod_dir/current.log"
      : >"$pod_dir/previous.log"
    fi
    if [ "$identity_status" = unavailable ]; then
      printf '%s\n' '{}' >"$pod_dir/status.json"
    fi
    jq -cn --arg ordinal "$ordinal" --arg uid "$pod_uid" --arg identity "$identity_status" --argjson logs "$logs_ok" \
      --slurpfile status "$pod_dir/status.json" \
      '{ordinal:$ordinal,podUID:$uid,identity:$identity,logsCaptured:$logs,status:($status[0] // {})}' >"$pod_dir/record.json"
    chmod 600 "$pod_dir"/* 2>/dev/null || true
    unset before_sts before_pod after_sts after_pod
  done
  jq -s --arg pinned "$pinned_uid" '{statefulSetIdentity:"pinned",statefulSetUID:$pinned,voters:.}' \
    "$voter_diagnostics_dir"/rhiza-*/record.json >"$FENCING_E2E_OUT/voter-startup-private.json" 2>"$voter_diagnostics_dir/records.stderr" || \
    printf '%s\n' '{"statefulSetIdentity":"unavailable","voters":[]}' >"$FENCING_E2E_OUT/voter-startup-private.json"
  chmod 600 "$FENCING_E2E_OUT/voter-startup-private.json"
  find "$voter_diagnostics_dir" -type f -exec chmod 600 {} +
)
capture_fencer_diagnostics() {
  diagnostics_dir="$FENCING_E2E_OUT/private-fencer-diagnostics"
  (umask 077 && mkdir -m 700 "$diagnostics_dir") || return 1
  capture_voter_startup_diagnostics || true

  # Never persist a Pod spec (which may contain literal env values) or kube auth.
  # Project only runtime status and Warning event identity/reason into the public packet.
  if pods_json=$(k --request-timeout=10s get pods -l app.kubernetes.io/name=rhiza-fencer -o json 2>"$diagnostics_dir/pods.stderr") &&
    printf '%s\n' "$pods_json" | jq -ce '{pods: [.items[:5][] | {name:.metadata.name, uid:.metadata.uid, phase:.status.phase,
      conditions:[.status.conditions[]? | {type,status,reason}],
      containers:[.status.containerStatuses[]? | {name,ready,restartCount,
        state:{waiting:{reason:.state.waiting.reason},terminated:{reason:.state.terminated.reason,exitCode:.state.terminated.exitCode,finishedAt:.state.terminated.finishedAt}},
        lastState:{terminated:{reason:.lastState.terminated.reason,exitCode:.lastState.terminated.exitCode,finishedAt:.lastState.terminated.finishedAt}}}]}]}' \
      >"$FENCING_E2E_OUT/fencer-pods.json" 2>"$diagnostics_dir/pods-jq.stderr"; then
    pods_captured=true
  else
    pods_captured=false
    printf '%s\n' '{"pods":[]}' >"$FENCING_E2E_OUT/fencer-pods.json"
  fi
  unset pods_json

  if events_json=$(k --request-timeout=10s get events --field-selector=type=Warning --sort-by=.lastTimestamp -o json 2>"$diagnostics_dir/events.stderr") &&
    printf '%s\n' "$events_json" | jq -ce '{warnings: [.items[-100:][] | {type,reason,count,lastTimestamp,
      involvedObject:{kind:.involvedObject.kind,name:.involvedObject.name},
      source:{component:.source.component}}]}' \
      >"$FENCING_E2E_OUT/fencer-events.json" 2>"$diagnostics_dir/events-jq.stderr"; then
    events_captured=true
  else
    events_captured=false
    printf '%s\n' '{"warnings":[]}' >"$FENCING_E2E_OUT/fencer-events.json"
  fi
  unset events_json

  if deployment_json=$(k --request-timeout=10s get deployment rhiza-fencer -o json 2>"$diagnostics_dir/deployment.stderr") &&
    printf '%s\n' "$deployment_json" | jq -ce '{name:.metadata.name,uid:.metadata.uid,generation:.metadata.generation,
      observedGeneration:.status.observedGeneration,replicas:.status.replicas,updatedReplicas:.status.updatedReplicas,
      readyReplicas:.status.readyReplicas,availableReplicas:.status.availableReplicas,unavailableReplicas:.status.unavailableReplicas,
      conditions:[.status.conditions[]? | {type,status,reason,lastUpdateTime,lastTransitionTime}]}' \
      >"$FENCING_E2E_OUT/fencer-deployment.json" 2>"$diagnostics_dir/deployment-jq.stderr"; then
    deployment_captured=true
  else
    deployment_captured=false
    printf '%s\n' '{}' >"$FENCING_E2E_OUT/fencer-deployment.json"
  fi
  unset deployment_json

  # Keep exact bounded container output only in the runner-private directory.
  # It is hashed into the OSS-safe packet, never uploaded verbatim.
  logs_file="$diagnostics_dir/authority-current.log"
  if k --request-timeout=10s logs -l app.kubernetes.io/name=rhiza-fencer --all-containers --prefix \
    --tail=200 --limit-bytes=65536 --max-log-requests=5 >"$logs_file" 2>"$diagnostics_dir/logs.stderr"; then
    logs_captured=true
  else
    logs_captured=false
  fi
  chmod 600 "$logs_file"
  logs_bytes=$(wc -c <"$logs_file" | tr -d ' ')
  logs_lines=$(wc -l <"$logs_file" | tr -d ' ')
  logs_sha256=$(sha256sum "$logs_file" | awk '{print $1}')
  chmod 400 "$logs_file"
  chmod 600 "$FENCING_E2E_OUT/fencer-pods.json" "$FENCING_E2E_OUT/fencer-events.json" "$FENCING_E2E_OUT/fencer-deployment.json"

  runtime_agent_logs="$diagnostics_dir/runtime-agent-current.log"
  runtime_agent_ds_uid=$(k --request-timeout=10s get daemonset rhiza-runtime-agent -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  runtime_agent_pod_json=''
  runtime_agent_pod_name=''
  runtime_agent_pod_uid=''
  if [ -n "$runtime_agent_ds_uid" ] && runtime_agent_pod_json=$(k --request-timeout=10s get pods -l app.kubernetes.io/name=rhiza-runtime-agent -o json 2>/dev/null); then
    runtime_agent_pod_identity=$(printf '%s\n' "$runtime_agent_pod_json" | jq -cer --arg node "$fencing_node" --arg owner "$runtime_agent_ds_uid" \
      '[.items[] | select(.spec.nodeName == $node) | select(any(.metadata.ownerReferences[]?; .kind == "DaemonSet" and .name == "rhiza-runtime-agent" and .uid == $owner)) | {name:.metadata.name,uid:.metadata.uid}] | if length == 1 then .[0] else error("runtime-agent Pod identity is ambiguous") end' 2>/dev/null || true)
    runtime_agent_pod_name=$(printf '%s\n' "$runtime_agent_pod_identity" | jq -r '.name // empty' 2>/dev/null || true)
    runtime_agent_pod_uid=$(printf '%s\n' "$runtime_agent_pod_identity" | jq -r '.uid // empty' 2>/dev/null || true)
  fi
  unset runtime_agent_pod_json runtime_agent_pod_identity
  runtime_agent_logs_captured=false
  if [ -n "$runtime_agent_pod_name" ] && [ -n "$runtime_agent_pod_uid" ] &&
    k --request-timeout=10s logs "$runtime_agent_pod_name" -c runtime-agent --prefix --tail=200 --limit-bytes=65536 >"$runtime_agent_logs" 2>"$diagnostics_dir/runtime-agent-logs.stderr"; then
    runtime_agent_after_ds_uid=$(k --request-timeout=10s get daemonset rhiza-runtime-agent -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
    runtime_agent_after_json=$(k --request-timeout=10s get pod "$runtime_agent_pod_name" -o json 2>/dev/null || true)
    if [ "$runtime_agent_after_ds_uid" = "$runtime_agent_ds_uid" ] && printf '%s\n' "$runtime_agent_after_json" | jq -e --arg pod "$runtime_agent_pod_uid" --arg node "$fencing_node" --arg owner "$runtime_agent_ds_uid" \
      '.metadata.uid == $pod and .spec.nodeName == $node and any(.metadata.ownerReferences[]?; .kind == "DaemonSet" and .name == "rhiza-runtime-agent" and .uid == $owner)' >/dev/null 2>&1; then
      runtime_agent_logs_captured=true
    else
      : >"$runtime_agent_logs"
      runtime_agent_pod_name=''
      runtime_agent_pod_uid=''
    fi
    unset runtime_agent_after_ds_uid runtime_agent_after_json
  else
    : >"$runtime_agent_logs"
  fi
  chmod 600 "$runtime_agent_logs"
  runtime_agent_logs_bytes=$(wc -c <"$runtime_agent_logs" | tr -d ' ')
  runtime_agent_logs_lines=$(wc -l <"$runtime_agent_logs" | tr -d ' ')
  runtime_agent_logs_sha256=$(sha256sum "$runtime_agent_logs" | awk '{print $1}')
  chmod 400 "$runtime_agent_logs"
  jq -cn --arg context "$fencing_context" --arg namespace "$fencing_namespace" \
    --arg logsSHA256 "$logs_sha256" --argjson logsBytes "$logs_bytes" --argjson logsLines "$logs_lines" \
    --argjson podsCaptured "$pods_captured" --argjson eventsCaptured "$events_captured" --argjson deploymentCaptured "$deployment_captured" --argjson logsCaptured "$logs_captured" \
    --arg runtimeAgentPodName "$runtime_agent_pod_name" --arg runtimeAgentPodUID "$runtime_agent_pod_uid" \
    --arg runtimeAgentLogsSHA256 "$runtime_agent_logs_sha256" --argjson runtimeAgentLogsBytes "$runtime_agent_logs_bytes" --argjson runtimeAgentLogsLines "$runtime_agent_logs_lines" --argjson runtimeAgentLogsCaptured "$runtime_agent_logs_captured" \
    '{context:$context,namespace:$namespace,podsCaptured:$podsCaptured,eventsCaptured:$eventsCaptured,deploymentCaptured:$deploymentCaptured,
      logs:{captured:$logsCaptured,sha256:$logsSHA256,bytes:$logsBytes,lines:$logsLines},
      runtimeAgent:{podName:$runtimeAgentPodName,podUID:$runtimeAgentPodUID,logs:{captured:$runtimeAgentLogsCaptured,sha256:$runtimeAgentLogsSHA256,bytes:$runtimeAgentLogsBytes,lines:$runtimeAgentLogsLines}}}' \
    >"$FENCING_E2E_OUT/fencer-diagnostics.json"
  chmod 600 "$FENCING_E2E_OUT/fencer-diagnostics.json"
}
cleanup() {
  fencing_exit=$?
  trap - EXIT INT TERM
  if [ "$fencing_owned" = true ]; then
    if [ "$fencing_exit" -ne 0 ]; then
      capture_fencer_diagnostics || true
    fi
    kind export logs "$FENCING_E2E_OUT/kind" --name "$fencing_cluster" >"$FENCING_E2E_OUT/export.log" 2>&1 || true
    k logs deployment/rhiza-fencer >"$FENCING_E2E_OUT/authority.log" 2>&1 || true
    k logs deployment/rhiza-operator >"$FENCING_E2E_OUT/operator.log" 2>&1 || true
    k get pods,rhizafences,rhizarecoveries,rhizaclusters,configmaps -o json >"$FENCING_E2E_OUT/terminal.json" 2>"$FENCING_E2E_OUT/terminal.err" || true
    if ! kind delete cluster --name "$fencing_cluster" >"$FENCING_E2E_OUT/cleanup.log" 2>&1; then fencing_exit=1; fi
    if kind get clusters | grep -Fx "$fencing_cluster" >/dev/null; then
      echo 'Owned qualification cluster remains' >&2
      fencing_exit=1
    fi
  fi
  rm -f "$fencing_tmp/tls.key" "$fencing_tmp/tls.crt" "$fencing_tmp/openssl.cnf" "$fencing_tmp/authority.yaml" "$fencing_tmp/supervised.patch.yaml" "$fencing_tmp/sa.yaml" "$fencing_tmp/rbac.yaml" "$fencing_tmp/manifests.json" "$fencing_tmp/local-cooperative-store-secret.json" "$fencing_tmp/crictl-version.txt" "$fencing_tmp/crictl-info.json"
  rmdir "$fencing_tmp" || fencing_exit=1
  printf '%s\n' "$fencing_exit" >"$FENCING_E2E_OUT/actual-exit.txt"
  exit "$fencing_exit"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
kind create cluster --name "$fencing_cluster" --image "$fencing_node_image" --wait 120s
fencing_owned=true
printf '%s\n' "$fencing_cluster" >"$FENCING_E2E_OUT/owned-cluster.txt"
fencing_node=$fencing_cluster-control-plane
docker exec "$fencing_node" crictl version >"$fencing_tmp/crictl-version.txt"
docker exec "$fencing_node" crictl info -o json >"$fencing_tmp/crictl-info.json"
fencing_runtime_name=$(crictl_version_field RuntimeName "$(cat "$fencing_tmp/crictl-version.txt")")
fencing_runtime_version=$(crictl_version_field RuntimeVersion "$(cat "$fencing_tmp/crictl-version.txt")")
fencing_runtime_api=$(crictl_version_field RuntimeApiVersion "$(cat "$fencing_tmp/crictl-version.txt")")
fencing_runtime_type=$(jq -er '.config.containerd as $c | $c.runtimes[$c.defaultRuntimeName].runtimeType' "$fencing_tmp/crictl-info.json")
fencing_kernel=$(docker exec "$fencing_node" uname -r)
fencing_cgroup_fs=$(docker exec "$fencing_node" stat -fc %T /sys/fs/cgroup)
case "$fencing_runtime_name:$fencing_runtime_api:$fencing_cgroup_fs" in containerd:v1:cgroup2fs) ;; *) echo 'runtime probe requires containerd CRI v1 on cgroup v2' >&2; exit 1 ;; esac
profile_field_valid "$fencing_runtime_version" && profile_field_valid "$fencing_runtime_type" && profile_field_valid "$fencing_kernel" || { echo 'unsafe or missing runtime profile value' >&2; exit 1; }
fencing_profile_id=kind-ci-cgroup-v2-component-v1
fencing_sandbox_args_json='["/pause"]'
printf '%s' "$fencing_sandbox_args_json" | jq -e 'type == "array" and length > 0 and all(.[]; type == "string" and length > 0 and (contains("\u0000") | not))' >/dev/null || {
  echo 'sandbox argv must be a non-empty JSON string array without NUL arguments' >&2
  exit 1
}
jq -cn --arg kindNodeImage "$fencing_node_image" --arg runtimeName "$fencing_runtime_name" --arg runtimeVersion "$fencing_runtime_version" --arg runtimeAPIVersion "$fencing_runtime_api" --arg runtimeType "$fencing_runtime_type" --arg kernelRelease "$fencing_kernel" --arg cgroupFilesystem "$fencing_cgroup_fs" --arg cgroupProfileID "$fencing_profile_id" --argjson passiveSandboxProfileConfigured "$([ -n "$fencing_sandbox_executable_sha256" ] && printf true || printf false)" '{qualification:"runtime-agent-component-only",kindNodeImage:$kindNodeImage,runtimeName:$runtimeName,runtimeVersion:$runtimeVersion,runtimeAPIVersion:$runtimeAPIVersion,runtimeType:$runtimeType,kernelRelease:$kernelRelease,cgroupFilesystem:$cgroupFilesystem,cgroupProfileID:$cgroupProfileID,passiveSandboxProfileConfigured:$passiveSandboxProfileConfigured}' >"$FENCING_E2E_OUT/runtime-profile.json"
kind load docker-image rhiza-fencing-host:local rhiza-fencing-operator:local --name "$fencing_cluster"
kubectl --context "$fencing_context" create namespace "$fencing_namespace"
for fencing_crd in crd cluster-crd fence-crd; do k apply -f "deploy/operator/$fencing_crd.yaml"; done
k wait --for=condition=Established crd/rhizarecoveries.rhiza.mrchypark.dev crd/rhizaclusters.rhiza.mrchypark.dev crd/rhizafences.rhiza.mrchypark.dev --timeout=60s
sed "s/namespace: rhiza$/namespace: $fencing_namespace/" deploy/operator/rbac.yaml >"$fencing_tmp/rbac.yaml"
k apply -f "$fencing_tmp/rbac.yaml"
printf '%s\n' '[req]' 'distinguished_name=dn' 'x509_extensions=ext' 'prompt=no' '[dn]' 'CN=rhiza-fencer' '[ext]' 'basicConstraints=critical,CA:TRUE' 'keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign' 'extendedKeyUsage=serverAuth' "subjectAltName=DNS:rhiza-fencer,DNS:rhiza-fencer.$fencing_namespace.svc,DNS:runtime-agent" >"$fencing_tmp/openssl.cnf"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -config "$fencing_tmp/openssl.cnf" -keyout "$fencing_tmp/tls.key" -out "$fencing_tmp/tls.crt" 2>"$FENCING_E2E_OUT/tls-generation.log"
k create secret tls rhiza-fencer-tls --cert="$fencing_tmp/tls.crt" --key="$fencing_tmp/tls.key"
k create configmap rhiza-fencer-ca --from-file="ca.crt=$fencing_tmp/tls.crt"
k create secret tls rhiza-runtime-agent-tls --cert="$fencing_tmp/tls.crt" --key="$fencing_tmp/tls.key"
k create secret generic rhiza-runtime-agent-ca --from-file="ca.crt=$fencing_tmp/tls.crt"
sed -e "s/namespace: rhiza$/namespace: $fencing_namespace/" \
  -e "s/resourceNames: \[rhiza\]$/resourceNames: [$fencing_namespace]/" \
  -e 's/image: rhiza-operator:latest$/image: rhiza-fencing-operator:local/' \
  -e "s/QUALIFIED_CONTAINERD_VERSION/$fencing_runtime_version/" -e "s/QUALIFIED_CONTAINERD_RUNTIME_TYPE/$fencing_runtime_type/" -e "s/QUALIFIED_PRIVATE_CGROUP_V2_PROFILE_ID/$fencing_profile_id/" \
  -e "s/QUALIFIED_LINUX_KERNEL_RELEASE/$fencing_kernel/" -e 's/PINNED_RHIZA_FENCER_DEPLOYMENT_UID/pending-authority-uid/' \
  -e "s/QUALIFIED_SANDBOX_EXECUTABLE_SHA256/$fencing_sandbox_executable_sha256/g" \
  deploy/operator/fencing/authority.yaml >"$fencing_tmp/authority.yaml"
k apply -f "$fencing_tmp/authority.yaml"
fencing_authority_uid=$(k get deployment rhiza-fencer -o jsonpath='{.metadata.uid}')
test -n "$fencing_authority_uid"
k patch configmap rhiza-runtime-agent-profile --type=merge -p "$(jq -cn --arg uid "$fencing_authority_uid" --arg sandboxExecutableSHA256 "$fencing_sandbox_executable_sha256" --arg sandboxArgsJSON "$fencing_sandbox_args_json" '{data:{"authority-deployment-uid":$uid,"sandbox-executable-sha256":$sandboxExecutableSHA256,"sandbox-args-json":$sandboxArgsJSON}}')"
if [ "$fencing_passive_kernel" = yes ]; then
  sed "s/namespace: rhiza$/namespace: $fencing_namespace/" \
    deploy/operator/fencing/runtime-agent-passive-kernel-patch.yaml >"$fencing_tmp/runtime-agent-passive-kernel-patch.yaml"
  k patch daemonset rhiza-runtime-agent --type=strategic --patch-file "$fencing_tmp/runtime-agent-passive-kernel-patch.yaml"
fi
k rollout restart daemonset rhiza-runtime-agent
k rollout status daemonset/rhiza-runtime-agent --timeout=180s
# The second YAML document is the application's unprivileged ServiceAccount.
sed -n '/^---$/,$p' deploy/operator/fencing/supervised-statefulset.patch.yaml | sed "s/namespace: rhiza$/namespace: $fencing_namespace/" >"$fencing_tmp/sa.yaml"
k apply -f "$fencing_tmp/sa.yaml"
sed '/^---$/,$d' deploy/operator/fencing/supervised-statefulset.patch.yaml | sed "s/namespace: rhiza$/namespace: $fencing_namespace/" >"$fencing_tmp/supervised.patch.yaml"
"$FENCING_E2E_BIN" manifests >"$fencing_tmp/manifests.json"
k apply -f "$fencing_tmp/manifests.json"
k patch statefulset rhiza --type=strategic --patch-file "$fencing_tmp/supervised.patch.yaml"
# Local cooperative fixture overlay only: credentials are never part of the
# shipped supervised patch or the base fixture manifests.
"$FENCING_E2E_BIN" local-cooperative-store-secret >"$fencing_tmp/local-cooperative-store-secret.json"
k apply -f "$fencing_tmp/local-cooperative-store-secret.json"
local_store_ref='[{"op":"add","path":"/spec/template/spec/containers/0/envFrom/-","value":{"secretRef":{"name":"rhiza-object-store"}}}]'
k patch statefulset rhiza --type=json -p "$local_store_ref"
k patch deployment rhiza-operator --type=json -p "$local_store_ref"
local_fencer_store_ref='[{"op":"add","path":"/spec/template/spec/containers/0/envFrom","value":[{"configMapRef":{"name":"rhiza-config"}},{"secretRef":{"name":"rhiza-object-store"}}]}]'
k patch deployment rhiza-fencer --type=json -p "$local_fencer_store_ref"
k rollout status deployment/rhiza-minio --timeout=180s
k patch job rhiza-create-bucket --type=merge -p '{"spec":{"suspend":false}}'
k wait --for=condition=Complete job/rhiza-create-bucket --timeout=180s
k rollout status deployment/rhiza-fencer --timeout=180s
"$FENCING_E2E_BIN" runtime-probe
fencing_source_sts_uid=$(k --request-timeout=10s get statefulset rhiza -o jsonpath='{.metadata.uid}')
case "$fencing_source_sts_uid" in ''|*[!A-Za-z0-9_-]*) echo 'source StatefulSet identity is unavailable' >&2; exit 2 ;; esac
# The automatic controller must pin the API-assigned RhizaCluster UID onto the
# supervised source template before any voter Pod can enroll. Start it while
# the StatefulSet is still empty, then require that exact binding before scale.
fencing_source_root_uid=$(k --request-timeout=10s get rhizaclusters rhiza -o jsonpath='{.metadata.uid}')
case "$fencing_source_root_uid" in ''|*[!A-Za-z0-9_-]*) echo 'source RhizaCluster identity is unavailable' >&2; exit 2 ;; esac
k scale deployment rhiza-operator --replicas=1
k rollout status deployment/rhiza-operator --timeout=120s
fencing_root_pin_attempt=0
while [ "$fencing_root_pin_attempt" -lt 120 ]; do
  fencing_root_pin_state=$(k --request-timeout=10s get statefulset rhiza -o json 2>/dev/null || true)
  if printf '%s\n' "$fencing_root_pin_state" | jq -e --arg uid "$fencing_source_root_uid" --arg sts_uid "$fencing_source_sts_uid" \
    '.metadata.uid == $sts_uid and .spec.replicas == 0 and .spec.template.metadata.annotations["rhiza.mrchypark.dev/fencing-root-uid"] == $uid' >/dev/null 2>&1; then
    break
  fi
  sleep 1
  fencing_root_pin_attempt=$((fencing_root_pin_attempt + 1))
done
if [ "$fencing_root_pin_attempt" -ge 120 ]; then
  echo 'automatic controller did not pin the verified source root before initial scale' >&2
  exit 2
fi
fencing_current_root_uid=$(k --request-timeout=10s get rhizaclusters rhiza -o jsonpath='{.metadata.uid}')
[ "$fencing_current_root_uid" = "$fencing_source_root_uid" ] || { echo 'source RhizaCluster identity changed before initial scale' >&2; exit 2; }
k scale statefulset rhiza --replicas=3
"$FENCING_E2E_BIN" wait-source
"$FENCING_E2E_BIN" run
