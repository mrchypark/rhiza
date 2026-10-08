#!/bin/sh
# Exact release engine + reviewed test-host overlay. Never a generation fencer.
set -eu
# Never trace credential references or rejected kubeconfig contents.
set +x
umask 077
mode=${1:-}
case "$mode" in render|run|run-local) ;; *) printf '%s\n' 'usage: run-gcs-postrelease.sh render|run|run-local' >&2; exit 1 ;; esac
release=f4d4cfee6d0928d813e84fe6e7d8c2b01c69f32c
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd -P)
die() { printf '%s\n' "$*" >&2; exit 1; }
safe_name() { printf '%s' "$1" | LC_ALL=C grep -Eq '^[a-f0-9]{8}$'; }
: "${RHIZA_RUN_ID:?}" "${RHIZA_NAMESPACE:?}" "${RHIZA_HOST_IMAGE:?}" "${RHIZA_METADATA_IMAGE:?}"
: "${RHIZA_NODE_A:?}" "${RHIZA_NODE_B:?}" "${RHIZA_NODE_C:?}" "${RHIZA_OUTPUT:?}"
: "${RHIZA_AUTH_CREATION_SHA:?}"
printf '%s' "$RHIZA_AUTH_CREATION_SHA" | LC_ALL=C grep -Eq '^[a-f0-9]{40}$' || die 'exact auth creation SHA required'
safe_name "$RHIZA_RUN_ID" || die 'unsafe run id'
[ "$RHIZA_NAMESPACE" = "rhiza-v0191-20261008-$RHIZA_RUN_ID" ] || die 'namespace mismatch'
for node in "$RHIZA_NODE_A" "$RHIZA_NODE_B" "$RHIZA_NODE_C"; do
  printf '%s' "$node" | LC_ALL=C grep -Eq '^gke-ied-cluster-[a-z0-9-]+$' || die 'existing-node pin required'
done
[ "$RHIZA_NODE_A" != "$RHIZA_NODE_B" ] && [ "$RHIZA_NODE_A" != "$RHIZA_NODE_C" ] && [ "$RHIZA_NODE_B" != "$RHIZA_NODE_C" ] || die 'three distinct nodes required'
printf '%s' "$RHIZA_HOST_IMAGE" | grep -Eq '^ghcr.io/mrchypark/rhiza-sql@sha256:[a-f0-9]{64}$' || die 'host digest required'
printf '%s' "$RHIZA_METADATA_IMAGE" | grep -Eq '^gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:[a-f0-9]{64}$' || die 'reviewed metadata image digest required'
[ ! -e "$RHIZA_OUTPUT" ] || die 'output already exists; no rerun'
mkdir -p "$RHIZA_OUTPUT"
out=$(CDPATH='' cd -- "$RHIZA_OUTPUT" && pwd -P)
ns=$RHIZA_NAMESPACE
prefix="postrelease/v0.19.1/$RHIZA_RUN_ID/"
cluster="gcs-$RHIZA_RUN_ID"
context=gke_patch2-the-new-era_asia-northeast3_ied-cluster
remaining() {
  seconds=$((local_deadline - $(date +%s)))
  [ "$seconds" -gt 0 ] || return 124
  printf '%s\n' "$seconds"
}
k() {
  if [ "$mode" = run-local ]; then
    seconds=$(remaining) || return 124
    limit=240
    case "$1" in logs) limit=10 ;; get|auth|scale) limit=30 ;; exec) limit=60 ;; esac
    [ "$seconds" -le "$limit" ] || seconds=$limit
    timeout --kill-after=5s "${seconds}s" kubectl --kubeconfig="$RHIZA_LOCAL_RUNTIME_KUBECONFIG" \
      --context="$context" --namespace="$ns" --request-timeout=30s "$@"
  else
    kubectl --context="$context" --namespace="$ns" "$@"
  fi
}
private_file() {
  case "$1" in /*) ;; *) die 'private absolute credential path required' ;; esac
  [ -f "$1" ] && [ ! -L "$1" ] || die 'regular non-symlink credential file required'
  parent=$(dirname -- "$1")
  [ ! -L "$parent" ] || die 'credential directory must not be a symlink'
  if [ "$(uname -s)" = Darwin ]; then
    permissions=$(stat -f '%Lp' "$1"); file_owner=$(stat -f '%u' "$1")
    directory_permissions=$(stat -f '%Lp' "$parent")
  else
    permissions=$(stat -c '%a' "$1"); file_owner=$(stat -c '%u' "$1")
    directory_permissions=$(stat -c '%a' "$parent")
  fi
  [ "$permissions" = 600 ] && [ "$file_owner" = "$(id -u)" ] && [ "$directory_permissions" = 700 ] || die 'private credential ownership/permissions required'
}
sed -e "s|__NAMESPACE__|$ns|g" -e "s|__RUN_ID__|$RHIZA_RUN_ID|g" \
  -e "s|__CLUSTER_ID__|$cluster|g" -e "s|__HOST_IMAGE__|$RHIZA_HOST_IMAGE|g" \
  -e "s|__METADATA_IMAGE__|$RHIZA_METADATA_IMAGE|g" \
  -e "s|__NODE_A__|$RHIZA_NODE_A|g" -e "s|__NODE_B__|$RHIZA_NODE_B|g" -e "s|__NODE_C__|$RHIZA_NODE_C|g" \
  "$script_dir/gcs-postrelease.yaml.in" > "$out/rendered.yaml"
# Template split is explicit: the learner cannot start before cold authority.
awk '/^# BEGIN COLD LEARNER/{cold=1;next} /^# END COLD LEARNER/{cold=0;next} !cold' "$out/rendered.yaml" > "$out/initial.yaml"
awk '/^# BEGIN COLD LEARNER/{cold=1;next} /^# END COLD LEARNER/{cold=0;next} cold' "$out/rendered.yaml" > "$out/learner.yaml"
awk 'BEGIN{RS="---\n"} /name: rhiza-metadata/{print}' "$out/initial.yaml" > "$out/metadata.yaml"
awk 'BEGIN{RS="---\n"} !/name: rhiza-metadata/ && !/name: rhiza-quic-probe/ && !/kind: NetworkPolicy/{print $0 "---"}' "$out/initial.yaml" > "$out/voters.yaml"
awk 'BEGIN{RS="---\n"} /name: rhiza-quic-probe/{print}' "$out/initial.yaml" > "$out/probe.yaml"
awk 'BEGIN{RS="---\n"} /kind: NetworkPolicy/{print}' "$out/initial.yaml" > "$out/cold-policy.yaml"
if [ "$mode" = render ]; then exit 0; fi
if [ "$mode" = run ]; then
  [ "${GITHUB_ACTIONS:-}" = true ] && [ "${GITHUB_EVENT_NAME:-}" = workflow_dispatch ] && [ "${GITHUB_REF:-}" = refs/heads/main ] || die 'qualification must run in approved CI'
  [ "${RHIZA_EXECUTION_GO:-}" = "$GITHUB_SHA:$RHIZA_RUN_ID" ] || die 'exact harness/run GO required'
else
  [ -z "${GITHUB_ACTIONS:-}${GITHUB_EVENT_NAME:-}${GITHUB_REF:-}${GITHUB_SHA:-}" ] || die 'local entry must not impersonate CI'
  : "${RHIZA_HARNESS_SHA:?}" "${RHIZA_LOCAL_RUNTIME_KUBECONFIG:?}"
  : "${RHIZA_LOCAL_API_SERVER:?}" "${RHIZA_LOCAL_CA_DATA:?}"
  : "${RHIZA_AUTH_STARTED_EPOCH:?}" "${RHIZA_AUTH_EXPIRES:?}" "${RHIZA_LOCAL_TOKEN_EXPIRES:?}"
  printf '%s' "$RHIZA_HARNESS_SHA" | grep -Eq '^[a-f0-9]{40}$' || die 'exact local harness SHA required'
  [ "$(git -C "$script_dir/../.." rev-parse HEAD)" = "$RHIZA_HARNESS_SHA" ] || die 'local harness HEAD mismatch'
  [ "${RHIZA_EXECUTION_GO:-}" = "$RHIZA_HARNESS_SHA:$RHIZA_RUN_ID" ] || die 'exact local harness/run GO required'
  command -v timeout >/dev/null || die 'native GNU timeout required'
  timeout --version | grep -Fq 'GNU coreutils' || die 'native GNU timeout required'
  private_file "$RHIZA_LOCAL_RUNTIME_KUBECONFIG"
  # Native parsing stays in memory, never artifacts/logs. A rejected config may
  # contain credentials, so suppress parser diagnostics and disclose only cause.
  config=$(timeout --kill-after=5s 5s kubectl --kubeconfig="$RHIZA_LOCAL_RUNTIME_KUBECONFIG" \
    --context="$context" --namespace="$ns" config view --raw -o json 2>/dev/null) || die 'local kubeconfig parsing failed'
  printf '%s' "$config" | jq -e --arg context "$context" --arg ns "$ns" \
    --arg server "$RHIZA_LOCAL_API_SERVER" --arg ca "$RHIZA_LOCAL_CA_DATA" '
    .kind=="Config" and .apiVersion=="v1" and .["current-context"]==$context and
    (.contexts|length)==1 and (.clusters|length)==1 and (.users|length)==1 and
    .contexts[0].name==$context and .contexts[0].context.namespace==$ns and
    (.contexts[0].context|keys)==["cluster","namespace","user"] and
    .contexts[0].context.cluster==.clusters[0].name and .contexts[0].context.user==.users[0].name and
    (.clusters[0].cluster|keys)==["certificate-authority-data","server"] and
    (.clusters[0].cluster.server==$server and ($server|startswith("https://"))) and
    .clusters[0].cluster["certificate-authority-data"]==$ca and ($ca|length)>0 and
    (.users[0].user|keys)==["tokenFile"] and (.users[0].user.tokenFile|type)=="string"
  ' >/dev/null || die 'local kubeconfig must be one pinned tokenFile-only identity'
  token_file=$(printf '%s' "$config" | jq -r '.users[0].user.tokenFile')
  unset config
  private_file "$token_file"
  [ -s "$token_file" ] || die 'local token file is empty'
  printf '%s' "$RHIZA_AUTH_STARTED_EPOCH" | grep -Eq '^[1-9][0-9]{0,11}$' || die 'fixed auth start required'
  auth_expiry=$(jq -ner --arg value "$RHIZA_AUTH_EXPIRES" '$value|fromdateiso8601') || die 'fixed auth expiry required'
  token_expiry=$(jq -ner --arg value "$RHIZA_LOCAL_TOKEN_EXPIRES" '$value|fromdateiso8601') || die 'actual token expiry required'
  now=$(date +%s)
  [ "$((auth_expiry - RHIZA_AUTH_STARTED_EPOCH))" -eq 3300 ] && \
    [ "$now" -ge "$RHIZA_AUTH_STARTED_EPOCH" ] && [ "$((now - RHIZA_AUTH_STARTED_EPOCH))" -le 900 ] && \
    [ "$token_expiry" -le "$auth_expiry" ] && [ "$((token_expiry - now))" -ge 2400 ] || die 'fixed 55-minute scope/preparation/runtime/cleanup reserve invalid'
  local_deadline=$((now + 1200))
fi
[ "${RHIZA_APPLICATION_SHA:-}" = "$release" ] || die 'application release mismatch'
: "${RHIZA_APPROVED_CAP_KRW:?}" "${RHIZA_BOOTSTRAP_UID:?}"
[ "$RHIZA_APPROVED_CAP_KRW" = 10000 ] || die 'this run requires the approved 10000 KRW cap'
seq=0
run() {
  seq=$((seq + 1)); label=$1; shift
  set +e
  if [ "$mode" = run-local ] && [ "$1" = curl ]; then
    seconds=$(remaining)
    code=$?
    if [ "$code" = 0 ]; then timeout --kill-after=5s "${seconds}s" "$@" > "$out/$seq-$label.stdout" 2> "$out/$seq-$label.stderr"; code=$?; fi
  else
    "$@" > "$out/$seq-$label.stdout" 2> "$out/$seq-$label.stderr"
    code=$?
  fi
  set -e
  printf '%s\n' "$code" > "$out/$seq-$label.exit"
  if [ "$code" != 0 ]; then
    if [ "$mode" = run-local ]; then printf 'first failure: %s exit %s\n' "$label" "$code" >&2; exit "$code"; fi
    die "first failure: $label exit $code"
  fi
}
started=${RHIZA_JOB_STARTED:-$(date +%s)}
[ "$mode" != run-local ] || started=$now
watchdog=''
stop_watchdog() {
  if [ -n "$watchdog" ]; then kill "$watchdog" 2>/dev/null || true; wait "$watchdog" 2>/dev/null || true; watchdog=''; fi
}
pf_pids=''
active_fault=''
owned_created=false
cleanup() {
  code=$?
  trap '' USR1
  trap - EXIT HUP INT TERM
  set +e
  stop_watchdog
  if [ "$mode" = run-local ]; then
    local_deadline=$(( $(date +%s) + 1200 ))
    [ "$local_deadline" -le "$token_expiry" ] || local_deadline=$token_expiry
    [ "$local_deadline" -le "$auth_expiry" ] || local_deadline=$auth_expiry
  fi
  clean=true
  # Never replace the first failure with a diagnostics/cleanup result.
  printf '%s\n' "$code" > "$out/workload.exit"
  if [ "$owned_created" = true ]; then
  for pod in rhiza-voter-0 rhiza-voter-1 rhiza-voter-2 rhiza-learner; do
    k logs "$pod" --tail=2000 > "$out/$pod.log" 2> "$out/$pod-log.stderr" || true
  done
  if [ -n "$active_fault" ]; then
    k delete "$active_fault" --ignore-not-found --wait=true --timeout=60s > "$out/fault-cleanup.stdout" 2> "$out/fault-cleanup.stderr"
    cleanup_code=$?
    printf '%s\n' "$cleanup_code" > "$out/fault-cleanup.exit"
    [ "$cleanup_code" = 0 ] || clean=false
    if [ "$code" = 0 ] && [ "$cleanup_code" != 0 ]; then code=$cleanup_code; fi
  fi
  # Run-owned writers stop normally; this is NOT fencing evidence.
  k scale statefulset/rhiza-voter --replicas=0 > "$out/stop-voters.stdout" 2> "$out/stop-voters.stderr"
  cleanup_code=$?
  printf '%s\n' "$cleanup_code" > "$out/stop-voters.exit"
  [ "$cleanup_code" = 0 ] || clean=false
  if [ "$code" = 0 ] && [ "$cleanup_code" != 0 ]; then code=$cleanup_code; fi
  k wait pod --selector="app=rhiza-voter,chaos.rhiza.io/run=$RHIZA_RUN_ID" --for=delete --timeout=90s > "$out/voters-gone.stdout" 2> "$out/voters-gone.stderr"
  cleanup_code=$?
  printf '%s\n' "$cleanup_code" > "$out/voters-gone.exit"
  [ "$cleanup_code" = 0 ] || clean=false
  if [ "$code" = 0 ] && [ "$cleanup_code" != 0 ]; then code=$cleanup_code; fi
  k get pods --selector="app=rhiza-voter,chaos.rhiza.io/run=$RHIZA_RUN_ID" -o json > "$out/voters-absent.json" 2> "$out/voters-absent.stderr"
  cleanup_code=$?
  if [ "$cleanup_code" = 0 ]; then jq -e '.items|length==0' "$out/voters-absent.json" >/dev/null; cleanup_code=$?; fi
  printf '%s\n' "$cleanup_code" > "$out/voters-absent.exit"
  [ "$cleanup_code" = 0 ] || clean=false
  if [ "$code" = 0 ] && [ "$cleanup_code" != 0 ]; then code=$cleanup_code; fi
  k delete pod/rhiza-learner --ignore-not-found --wait=true --timeout=60s > "$out/stop-learner.stdout" 2> "$out/stop-learner.stderr"
  cleanup_code=$?
  printf '%s\n' "$cleanup_code" > "$out/stop-learner.exit"
  [ "$cleanup_code" = 0 ] || clean=false
  if [ "$code" = 0 ] && [ "$cleanup_code" != 0 ]; then code=$cleanup_code; fi
  k delete pod/rhiza-quic-probe --ignore-not-found --wait=true --timeout=60s > "$out/stop-probe.stdout" 2> "$out/stop-probe.stderr"
  cleanup_code=$?
  printf '%s\n' "$cleanup_code" > "$out/stop-probe.exit"
  [ "$cleanup_code" = 0 ] || clean=false
  if [ "$code" = 0 ] && [ "$cleanup_code" != 0 ]; then code=$cleanup_code; fi
  fi
  if [ "$clean" = true ]; then
    printf '%s\n' 'Run-owned fault/writers stopped; Root storage/IAM cleanup still pending.' > "$out/cleanup-status.txt"
  else
    printf '%s\n' 'NOT CLEAN: bounded run-owned shutdown failed; Root must reconcile before storage/IAM cleanup.' > "$out/cleanup-status.txt"
    printf '%s\n' 'NOT CLEAN (see workload.exit for the preserved original result).' >> "$out/result.txt"
  fi
  for pid in $pf_pids; do kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; done
  printf '%s\n' 'Managed-folder objects, soft-delete retention and namespace/IAM teardown remain Root-owned; no recursive cleanup here.' > "$out/cleanup-owner.txt"
  printf '%s\n' "$code" > "$out/root.exit"
  (cd "$out" && find . -type f ! -name SHA256SUMS -exec sha256sum {} \;) > "$out/SHA256SUMS"
  exit "$code"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
if [ "$mode" = run-local ]; then
  trap 'exit 124' USR1
  runtime_pid=$$
  watchdog_seconds=$(remaining) || exit 124
  (
    sleep_pid=''
    trap 'if [ -n "$sleep_pid" ]; then kill "$sleep_pid" 2>/dev/null || true; wait "$sleep_pid" 2>/dev/null || true; fi; exit 0' HUP INT TERM
    sleep "$watchdog_seconds" & sleep_pid=$!
    wait "$sleep_pid"
    kill -USR1 "$runtime_pid"
  ) & watchdog=$!
  printf '%s\n' "$watchdog" > "$out/watchdog.pid"
  run local-identity k auth whoami -o json
  jq -e --arg user "system:serviceaccount:$ns:rhiza-runtime" --arg group "system:serviceaccounts:$ns" \
    '.status.userInfo.username==$user and (.status.userInfo.groups|sort)==(["system:authenticated","system:serviceaccounts",$group]|sort)' \
    "$out/$seq-local-identity.stdout" >/dev/null || die 'local runtime server identity mismatch'
  for resource in secrets serviceaccounts/token roles validatingadmissionpolicies; do
    verb='create'
    [ "$resource" != secrets ] || verb='get'
    case "$resource" in
      serviceaccounts/token) set -- create serviceaccounts --subresource=token ;;
      validatingadmissionpolicies) set -- create validatingadmissionpolicies.admissionregistration.k8s.io --all-namespaces ;;
      *) set -- "$verb" "$resource" ;;
    esac
    seq=$((seq + 1))
    set +e
    k auth can-i "$@" > "$out/$seq-permission-denied.stdout" 2> "$out/$seq-permission-denied.stderr"
    denied_code=$?
    set -e
    printf '%s\n' "$denied_code" > "$out/$seq-permission-denied.exit"
    if [ "$denied_code" = 124 ] || [ "$denied_code" = 137 ]; then exit "$denied_code"; fi
    [ "$denied_code" = 1 ] && grep -qx no "$out/$seq-permission-denied.stdout" && [ ! -s "$out/$seq-permission-denied.stderr" ] || die 'local runtime forbidden permission/authorization check failed'
  done
fi
run namespace k get namespace "$ns" -o json
jq -e --arg uid "$RHIZA_BOOTSTRAP_UID" --arg run "$RHIZA_RUN_ID" --arg owner "rhiza-postrelease-$RHIZA_RUN_ID-$RHIZA_AUTH_CREATION_SHA" '.metadata.uid==$uid and .metadata.labels["chaos.rhiza.io/run"]==$run and .metadata.annotations["rhiza.dev/auth-owner"]==$owner and .metadata.annotations["chaos-mesh.org/inject"]=="enabled"' "$out/$seq-namespace.stdout" >/dev/null || die 'bootstrap namespace/workflow identity mismatch'
run collision k get pods,statefulsets,services,configmaps,networkpolicies,podchaos,networkchaos -l "chaos.rhiza.io/run=$RHIZA_RUN_ID" -o json
jq -e '.items|length==0' "$out/$seq-collision.stdout" >/dev/null || die 'run resources already exist'
owned_created=true
run create-metadata k create -f "$out/metadata.yaml"
run metadata-ready k wait pod/rhiza-metadata --for=condition=Ready --timeout=180s
storage="gs://rhiza-v070-chaos-ied-20260811/$prefix"
seq=$((seq + 1))
set +e
k exec rhiza-metadata -- gcloud storage cat "gs://rhiza-v070-chaos-ied-20260811/postrelease/v0.19.1/denied-$RHIZA_RUN_ID/no-object" > "$out/$seq-outside-scope.stdout" 2> "$out/$seq-outside-scope.stderr"
denied_code=$?
set -e
printf '%s\n' "$denied_code" > "$out/$seq-outside-scope.exit"
[ "$denied_code" != 0 ] && grep -Eq '403|PERMISSION_DENIED|AccessDenied' "$out/$seq-outside-scope.stderr" && [ ! -s "$out/$seq-outside-scope.stdout" ] || die 'outside-folder access did not fail closed'
# A single exact folder list, before any database writer exists. gcloud's
# documented empty-match error is an expected probe, never an auth fallback.
seq=$((seq + 1))
set +e
k exec rhiza-metadata -- gcloud storage ls --json "${storage}**" > "$out/$seq-prefix-empty.stdout" 2> "$out/$seq-prefix-empty.stderr"
list_code=$?
set -e
printf '%s\n' "$list_code" > "$out/$seq-prefix-empty.exit"
if [ "$list_code" = 0 ]; then
  jq -e 'length==0' "$out/$seq-prefix-empty.stdout" >/dev/null || die 'prefix not empty'
else
  [ "$list_code" = 1 ] && grep -Fq 'matched no objects' "$out/$seq-prefix-empty.stderr" || die 'prefix/list permission gate failed'
fi
run create-voters k create -f "$out/voters.yaml"
run voters-ready k rollout status statefulset/rhiza-voter --timeout=180s
forward() {
  pod=$1; port=$2
  if [ "$mode" = run-local ]; then
    seconds=$(remaining) || exit 124
    timeout --kill-after=5s "${seconds}s" kubectl --kubeconfig="$RHIZA_LOCAL_RUNTIME_KUBECONFIG" \
      --context="$context" --namespace="$ns" --request-timeout=30s port-forward "pod/$pod" "$port:8080" "$((port + 100)):8081" > "$out/$pod-portforward.log" 2>&1 &
  else
    k port-forward "pod/$pod" "$port:8080" "$((port + 100)):8081" > "$out/$pod-portforward.log" 2>&1 &
  fi
  pf_pids="$pf_pids $!"
  tries=0
  until curl -fsS --max-time 2 "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; do
    tries=$((tries + 1)); [ "$tries" -lt 30 ] || die 'port-forward timeout'; sleep 1
  done
}
forward rhiza-voter-0 18080
forward rhiza-voter-1 18081
forward rhiza-voter-2 18082
pod_snapshot() { k get pods -l "chaos.rhiza.io/run=$RHIZA_RUN_ID" -o json | jq '[.items[]|{name:.metadata.name,uid:.metadata.uid,node:.spec.nodeName,volumes:.spec.volumes,containers:.status.containerStatuses}]'; }
run before pod_snapshot
cp "$out/$seq-before.stdout" "$out/voters-before.json"
jq -e 'all(.[]|select(.name|startswith("rhiza-voter-"));all(.containers[]; .restartCount==0))' "$out/voters-before.json" >/dev/null || die 'unexpected startup restart; unseen incarnation not budgeted'
printf '%s\n' '{}' > "$out/counter-segments.json"
voter0_restarts=0
learner_started=false
# Sampled high-water sums, NOT a strict billing cap. Five expected process
# incarnations: three voters, one voter-0 restart, one fresh learner. Stop at
# 40k observed attempts/640MiB upload/body/download, leaving 10k/384MiB planned
# reserve for unseen startup/SIGKILL tail, metadata probes and shutdown. Those
# reserves are conservative planning allowances, not measured upper bounds.
metrics() {
  [ "$(( $(date +%s) - started ))" -lt 1200 ] || die '20-minute workload ceiling; cleanup reserved'
  ports='18080 18081 18082'
  [ "$learner_started" = false ] || ports="$ports 18083"
  for port in $ports; do
    # Preserve the pre-kill sample; SIGKILL tail is a disclosed reserve, not
    # falsely reconstructed from the restarted process's zeroed counters.
    if [ "$active_fault" = PodChaos/process-one ] && [ "$port" = 18080 ]; then continue; fi
    case "$port" in
      18080) pod=rhiza-voter-0; expected=$voter0_restarts;;
      18081) pod=rhiza-voter-1; expected=0;;
      18082) pod=rhiza-voter-2; expected=0;;
      18083) pod=rhiza-learner; expected=0;;
    esac
    run counter-identity k get pod "$pod" -o json
    identity="$out/$seq-counter-identity.stdout"
    jq -e --argjson expected "$expected" '.status.containerStatuses|length==1 and .[0].restartCount==$expected and (.[0].containerID|length)>0' "$identity" >/dev/null || die 'unexpected incarnation/restart; budget cannot be reconstructed'
    if [ "$pod" != rhiza-learner ]; then
      jq -e --slurpfile before "$out/voters-before.json" '. as $p|any($before[0][];.name==$p.metadata.name and .uid==$p.metadata.uid)' "$identity" >/dev/null || die 'unplanned voter Pod replacement'
    fi
    run counters curl -fsS --max-time 5 "http://127.0.0.1:$port/qualification/object-store"
    counters="$out/$seq-counters.stdout"
    run counter-identity-after k get pod "$pod" -o json
    jq -e --slurpfile before "$identity" '.metadata.uid==$before[0].metadata.uid and .status.containerStatuses[0].containerID==$before[0].status.containerStatuses[0].containerID and .status.containerStatuses[0].restartCount==$before[0].status.containerStatuses[0].restartCount' "$out/$seq-counter-identity-after.stdout" >/dev/null || die 'incarnation changed during counter sample'
    key=$(jq -r '.metadata.uid+"/"+.status.containerStatuses[0].containerID' "$identity")
    jq -e --arg key "$key" --slurpfile sample "$counters" '
      .[$key] as $old|$sample[0] as $new|
      if $old!=null and any($old|keys[]; . as $field|$new[$field]<$old[$field]) then error("counter reset within incarnation")
      else .[$key]=$new end' "$out/counter-segments.json" > "$out/counter-segments.next.json" || die 'counter reset within UID/container segment'
    mv "$out/counter-segments.next.json" "$out/counter-segments.json"
    jq '[.[]]|reduce .[] as $s ({};reduce ($s|keys[]) as $k (. ;.[$k]=((.[$k]//0)+$s[$k])))' "$out/counter-segments.json" > "$out/counter-totals.json"
    jq -e 'length<=5 and all(.[];.http_requests<8000 and .bytes_uploaded<134217728 and .http_request_body_bytes<134217728 and .bytes_downloaded<134217728)' "$out/counter-segments.json" >/dev/null || die 'incarnation bound reached'
    jq -e '.http_requests<40000 and .bytes_uploaded<671088640 and .http_request_body_bytes<671088640 and .bytes_downloaded<671088640' "$out/counter-totals.json" >/dev/null || die 'sampled aggregate reserve threshold reached'
  done
}
execute() {
  id=$1; sql=$2
  payload=$(jq -nc --arg id "$id" --arg sql "$sql" '{request_id:$id,sql:$sql}')
  run ack curl -fsS --max-time 30 -H 'Content-Type: application/json' -d "$payload" http://127.0.0.1:18081/sql/execute
  jq -e '.slot>0 and .status=="committed" and .applied==true and (.error_code==null or .error_code=="")' "$out/$seq-ack.stdout" >/dev/null || die 'not a durable successful ACK'
  printf '%s\n' "$id" >> "$out/ack-ids.txt"
  metrics
}
readback() {
  port=$1; consistency=$2; expected=$3
  payload=$(jq -nc --arg c "$consistency" '{sql:"SELECT id,value FROM qualification ORDER BY id",consistency:$c}')
  run readback curl -fsS --max-time 30 -H 'Content-Type: application/json' -d "$payload" "http://127.0.0.1:$port/sql/query"
  jq -e --argjson expected "$expected" '.rows==$expected and .applied_slot>0 and .consensus_tip>=.applied_slot' "$out/$seq-readback.stdout" >/dev/null || die 'ACK/read-barrier mismatch'
}
execute "$RHIZA_RUN_ID-schema" 'CREATE TABLE qualification (id INTEGER PRIMARY KEY,value TEXT NOT NULL)'
execute "$RHIZA_RUN_ID-seed" "INSERT INTO qualification VALUES (1,'before')"
for port in 18080 18081 18082; do readback "$port" linearizable '[[1,"before"]]'; done
fault() {
  kind=$1; action=$2; name=$3
  case "$kind" in
    NetworkChaos) expected_policy="$ns-network-faults" ;;
    PodChaos) expected_policy="$ns-faults" ;;
    *) die 'unsupported qualification fault kind' ;;
  esac
  jq -n --arg kind "$kind" --arg action "$action" --arg ns "$ns" --arg run "$RHIZA_RUN_ID" --arg name "$name" \
    '{apiVersion:"chaos-mesh.org/v1alpha1",kind:$kind,metadata:{name:$name,namespace:$ns,labels:{"chaos.rhiza.io/run":$run}},spec:{action:$action,mode:"one",duration:"10s",selector:{namespaces:[$ns],labelSelectors:{"app.kubernetes.io/part-of":"rhiza-chaos","chaos.rhiza.io/run":$run,"statefulset.kubernetes.io/pod-name":"rhiza-voter-0"}}}}' > "$out/$name.json"
  if [ "$kind" = NetworkChaos ]; then
    jq --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '.spec.direction="both"|.spec.target={mode:"all",selector:{namespaces:[$ns],labelSelectors:{app:"rhiza-voter","chaos.rhiza.io/run":$run}}}' "$out/$name.json" > "$out/$name-network.json"
    cp "$out/$name-network.json" "$out/$name.json"
  else
    jq '.spec.containerNames=["rhiza"]' "$out/$name.json" > "$out/$name-container.json"
    cp "$out/$name-container.json" "$out/$name.json"
  fi
  # Prove server admission rejects an out-of-namespace selector without
  # actually creating a fault or reading the foreign namespace.
  jq '.metadata.name="denied-scope"|.spec.selector.namespaces=["default"]' "$out/$name.json" > "$out/denied-scope.json"
  seq=$((seq + 1))
  set +e
  k create --dry-run=server -f "$out/denied-scope.json" > "$out/$seq-selector-denial.stdout" 2> "$out/$seq-selector-denial.stderr"
  denied_code=$?
  set -e
  printf '%s\n' "$denied_code" > "$out/$seq-selector-denial.exit"
  [ "$denied_code" != 0 ] && grep -Fq "ValidatingAdmissionPolicy '$expected_policy' " "$out/$seq-selector-denial.stderr" || die 'server-side selector confinement not proven by the expected policy'
  run fault-validate k create --dry-run=server -f "$out/$name.json"
  # Registration precedes the write: an accepted create with lost response
  # still has one exact run-owned target for safe absent-aware cleanup.
  active_fault="$kind/$name"
  run fault-create k create -f "$out/$name.json"
  run injected k wait "$active_fault" --for=condition=AllInjected=True --timeout=60s
  run injection k get "$active_fault" -o json
}
fault NetworkChaos partition network-one
execute "$RHIZA_RUN_ID-network" "INSERT INTO qualification VALUES (2,'network')"
run recovered k wait "$active_fault" --for=condition=AllRecovered=True --timeout=90s
run fault-delete k delete "$active_fault" --wait=true --timeout=60s
active_fault=''
for port in 18080 18081 18082; do readback "$port" linearizable '[[1,"before"],[2,"network"]]'; done
metrics
voter0_restarts=1
fault PodChaos container-kill process-one
execute "$RHIZA_RUN_ID-process" "INSERT INTO qualification VALUES (3,'process')"
run recovered k wait "$active_fault" --for=condition=AllRecovered=True --timeout=90s
run fault-delete k delete "$active_fault" --wait=true --timeout=60s
active_fault=''
run voters-ready k rollout status statefulset/rhiza-voter --timeout=120s
# Existing port-forward connection may exit when its container is killed.
forward rhiza-voter-0 18080
run after pod_snapshot
jq -e --slurpfile before "$out/voters-before.json" '[.[]|select(.name|startswith("rhiza-voter-"))] as $a|[$before[0][]|select(.name|startswith("rhiza-voter-"))] as $b|all($b[];. as $old|any($a[];.name==$old.name and .uid==$old.uid and .volumes==$old.volumes)) and any($a[];.name=="rhiza-voter-0" and .containers[0].restartCount>0)' "$out/$seq-after.stdout" >/dev/null || die 'voter UID/WAL identity changed or no container restart'
metrics
for port in 18080 18081 18082; do readback "$port" linearizable '[[1,"before"],[2,"network"],[3,"process"]]'; done
sleep 15
run current k exec rhiza-metadata -- gcloud storage cat "${storage}${cluster}/checkpoint/CURRENT"
cp "$out/$seq-current.stdout" "$out/CURRENT.json"
run head k exec rhiza-metadata -- gcloud storage cat "${storage}${cluster}/archive/HEAD"
cp "$out/$seq-head.stdout" "$out/HEAD.json"
jq -e --slurpfile current "$out/CURRENT.json" 'def hex: "0123456789abcdef" as $h|map(. as $n|$h[($n/16|floor):($n/16|floor)+1]+$h[($n%16):($n%16)+1])|join(""); .base>0 and .base_seal!=null and .base_decision!=null and .base==$current[0].index and (.base_seal.root_hash|hex)==$current[0].root_hash' "$out/HEAD.json" >/dev/null || die 'certified cold checkpoint base absent'
run probe-create k create -f "$out/probe.yaml"
run probe-ready k wait pod/rhiza-quic-probe --for=condition=Ready --timeout=120s
run probe-identity k get pod rhiza-quic-probe -o json
probe_identity="$out/$seq-probe-identity.stdout"
for voter in 0 1 2; do
  run probe-target k get pod "rhiza-voter-$voter" -o json
  jq -r '.status.podIP' "$out/$seq-probe-target.stdout" > "$out/probe-voter-$voter.ip"
  target=$(cat "$out/probe-voter-$voter.ip")
  run probe-connected k exec rhiza-quic-probe -- /usr/local/bin/rhiza-entrypoint qualification-quic-probe connected "rhiza-voter-$voter" "$target:9090"
done
run cold-policy-create k create -f "$out/cold-policy.yaml"
sleep 5
for voter in 0 1 2; do
  target=$(cat "$out/probe-voter-$voter.ip")
  run probe-blocked k exec rhiza-quic-probe -- /usr/local/bin/rhiza-entrypoint qualification-quic-probe blocked "rhiza-voter-$voter" "$target:9090"
done
run probe-identity-after k get pod rhiza-quic-probe -o json
jq -e --slurpfile before "$probe_identity" '.metadata.uid==$before[0].metadata.uid and .status.podIP==$before[0].status.podIP and .spec.nodeName==$before[0].spec.nodeName and .metadata.labels==$before[0].metadata.labels and .status.containerStatuses[0].restartCount==0' "$out/$seq-probe-identity-after.stdout" >/dev/null || die 'probe identity changed across policy verification'
run probe-delete k delete pod/rhiza-quic-probe --wait=true --timeout=60s
run cold-create k create -f "$out/learner.yaml"
run learner-running k wait pod/rhiza-learner --for=jsonpath='{.status.phase}'=Running --timeout=180s
forward rhiza-learner 18083
learner_started=true
metrics
jq -e '.bytes_downloaded>0' "$counters" >/dev/null || die 'fresh learner GCS download evidence absent'
readback 18083 local '[[1,"before"],[2,"network"],[3,"process"]]'
run blocked-quorum curl -fsS --max-time 10 http://127.0.0.1:18183/recovery/status
jq -e '.ready==false and .quorum==false' "$out/$seq-blocked-quorum.stdout" >/dev/null || die 'learner completed peer catch-up despite deny policy'
run unblock k delete networkpolicy/learner-cold --wait=true
tries=0
until curl -fsS --max-time 2 http://127.0.0.1:18083/ready >/dev/null 2>&1; do
  tries=$((tries + 1)); [ "$tries" -lt 30 ] || die 'learner catch-up did not resume'; sleep 1
done
run learner-quorum curl -fsS --max-time 10 http://127.0.0.1:18183/recovery/status
jq -e '.ready==true and .quorum==false' "$out/$seq-learner-quorum.stdout" >/dev/null || die 'unpromoted learner contract mismatch'
execute "$RHIZA_RUN_ID-after-cold" "INSERT INTO qualification VALUES (4,'after-cold')"
readback 18081 linearizable '[[1,"before"],[2,"network"],[3,"process"],[4,"after-cold"]]'
barrier=$(jq -r '.applied_slot' "$out/$seq-readback.stdout")
# Unpromoted learners cannot originate ReadIndex. Observe their certified
# catch-up against the real surviving-voter barrier, not an invented quorum.
tries=0
payload='{"sql":"SELECT id,value FROM qualification ORDER BY id","consistency":"local"}'
while :; do
  run learner-local curl -fsS --max-time 5 -H 'Content-Type: application/json' -d "$payload" http://127.0.0.1:18083/sql/query
  if jq -e --argjson barrier "$barrier" '.rows==[[1,"before"],[2,"network"],[3,"process"],[4,"after-cold"]] and .applied_slot>=$barrier' "$out/$seq-learner-local.stdout" >/dev/null; then break; fi
  tries=$((tries + 1)); [ "$tries" -lt 30 ] || die 'learner did not reach voter read barrier'; sleep 1
done
run learner-topology pod_snapshot
jq -e 'any(.[];.name=="rhiza-learner" and (.volumes|length)==1 and .volumes[0].emptyDir!=null)' "$out/$seq-learner-topology.stdout" >/dev/null || die 'fresh learner volume proof missing'
metrics
printf '%s\n' 'Sampled UID/container high-water totals exclude unseen startup/kill/shutdown tails and metadata SDK calls; 10k attempts/384MiB are planned reserves, NOT hard billing guarantees. Root must reconcile actual object generations/transfer and cleanup.' > "$out/budget-limitations.txt"
printf '%s\n' 'PASS selected GCS/process/network/fresh-learner layer; no whole-generation, promotion, fencing, IO/power-loss or published Rust package claim.' > "$out/result.txt"
