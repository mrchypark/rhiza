#!/bin/sh
# Exact release engine + reviewed test-host overlay. Never a generation fencer.
set -eu
# Never trace credential references or rejected kubeconfig contents.
set +x
umask 077
mode=${1:-}
case "$mode" in render|run|run-local|check-shutdown) ;; *) printf '%s\n' 'usage: run-gcs-postrelease.sh render|run|run-local|check-shutdown' >&2; exit 1 ;; esac
release=315a5ee6bc6635b4ff4f85b9534afa4abe537c79
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
  if [ -n "${metadata_wait_deadline:-}" ]; then
    metadata_seconds=$((metadata_wait_deadline - $(date +%s)))
    [ "$metadata_seconds" -gt 0 ] || return 124
  fi
  if [ -n "${voter_wait_deadline:-}" ]; then
    k_deadline_seconds=$((voter_wait_deadline - $(date +%s)))
    [ "$k_deadline_seconds" -gt 0 ] || return 124
  fi
  if [ "$mode" = run-local ]; then
    seconds=$(remaining) || return 124
    limit=240
    case "$1" in logs) limit=10 ;; get|auth|scale) limit=30 ;; exec) limit=60 ;; esac
    [ "$seconds" -le "$limit" ] || seconds=$limit
    if [ -n "${metadata_wait_deadline:-}" ] && [ "$seconds" -gt "$metadata_seconds" ]; then seconds=$metadata_seconds; fi
    if [ -n "${voter_wait_deadline:-}" ] && [ "$seconds" -gt "$k_deadline_seconds" ]; then seconds=$k_deadline_seconds; fi
    timeout --kill-after=5s "${seconds}s" kubectl --kubeconfig="$RHIZA_LOCAL_RUNTIME_KUBECONFIG" \
      --context="$context" --namespace="$ns" --request-timeout=30s "$@"
  else
    if [ -n "${metadata_wait_deadline:-}" ]; then
      timeout --kill-after=5s "${metadata_seconds}s" kubectl --context="$context" --namespace="$ns" "$@"
    elif [ -n "${voter_wait_deadline:-}" ]; then
      timeout --kill-after=5s "${k_deadline_seconds}s" kubectl --context="$context" --namespace="$ns" "$@"
    else
      kubectl --context="$context" --namespace="$ns" "$@"
    fi
  fi
}
metadata_credentials_ready() (
  # 55 seconds of work plus the existing five-second timeout reap reserve.
  metadata_started=$(date +%s)
  metadata_wait_deadline=$((metadata_started + 55))
  if [ "$mode" = run-local ] && [ "$metadata_wait_deadline" -gt "$local_deadline" ]; then metadata_wait_deadline=$local_deadline; fi
  metadata_attempt=0
  while [ "$metadata_attempt" -lt 6 ]; do
    [ "$(date +%s)" -lt "$metadata_wait_deadline" ] || return 124
    metadata_attempt=$((metadata_attempt + 1))
    metadata_code=0
    # Keep the token response in memory only; neither curl nor kubectl stderr
    # is public evidence. Never pass the response as an argument to a process.
    metadata_response=$(k exec rhiza-metadata -- curl --silent --max-time 5 --max-filesize 16384 \
      --noproxy '*' -H 'Metadata-Flavor: Google' --write-out '\n%{http_code}' \
      http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token 2>/dev/null) || metadata_code=$?
    metadata_http=$(printf '%s\n' "$metadata_response" | tail -n 1)
    case "$metadata_http" in [1-5][0-9][0-9]) ;; *) metadata_http=000 ;; esac
    metadata_category=unknown
    if [ "$metadata_code" = 0 ]; then
      case "$metadata_http" in
        200)
          if printf '%s\n' "$metadata_response" | sed '$d' | jq -s -e '
            length==1 and (.[0] | type=="object" and (.access_token|type)=="string" and (.access_token|length)>0 and
            .token_type=="Bearer" and (.expires_in|type)=="number" and .expires_in>0)
          ' >/dev/null 2>&1; then metadata_category=ready; else metadata_category=invalid-response; fi ;;
        403) metadata_category=forbidden ;;
        5[0-9][0-9]) metadata_category=transient-server ;;
      esac
    else
      case "$metadata_code" in
        6|7|28|52|56) metadata_category=transient-connection ;;
        124|137) metadata_category=deadline ;;
      esac
    fi
    # A known denial must not become retryable because its body read timed out.
    case "$metadata_http" in
      403) metadata_category=forbidden ;;
      [1-4][0-9][0-9]) [ "$metadata_http" = 200 ] || metadata_category=unknown ;;
    esac
    unset metadata_response
    printf 'stage=metadata-credential-readiness attempt=%s native=%s http=%s elapsed=%s category=%s\n' \
      "$metadata_attempt" "$metadata_code" "$metadata_http" "$(( $(date +%s) - metadata_started ))" "$metadata_category"
    case "$metadata_category" in
      ready) [ "$(date +%s)" -lt "$metadata_wait_deadline" ] || return 124; return 0 ;;
      transient-connection|transient-server) ;;
      deadline) return "$metadata_code" ;;
      *) return 1 ;;
    esac
    [ "$metadata_attempt" -lt 6 ] || return 1
    [ "$((metadata_wait_deadline - $(date +%s)))" -gt 1 ] || return 124
    sleep 1
  done
)
wait_voters() (
  # One deadline covers creation, Ready and identity checks for all three Pods.
  # Subshell scope prevents this deadline from constraining later cleanup.
  voter_wait_deadline=$(( $(date +%s) + $1 ))
  for voter_wait_index in 0 1 2; do
    voter_wait_seconds=$((voter_wait_deadline - $(date +%s)))
    [ "$voter_wait_seconds" -gt 0 ] || exit 124
    k wait "pod/rhiza-voter-$voter_wait_index" --for=create --timeout="${voter_wait_seconds}s" || exit $?
  done
  voter_wait_before=$(k get pods rhiza-voter-0 rhiza-voter-1 rhiza-voter-2 -o json) || exit $?
  for voter_wait_index in 0 1 2; do
    voter_wait_seconds=$((voter_wait_deadline - $(date +%s)))
    [ "$voter_wait_seconds" -gt 0 ] || exit 124
    k wait "pod/rhiza-voter-$voter_wait_index" --for=condition=Ready --timeout="${voter_wait_seconds}s" || exit $?
  done
  voter_wait_after=$(k get pods rhiza-voter-0 rhiza-voter-1 rhiza-voter-2 -o json) || exit $?
  jq -n -e --argjson before "$voter_wait_before" --argjson after "$voter_wait_after" '
      ($after.items|sort_by(.metadata.name)) as $pods |
      ($pods|map(.metadata.name))==["rhiza-voter-0","rhiza-voter-1","rhiza-voter-2"] and
      ($pods|map({name:.metadata.name,uid:.metadata.uid}))==($before.items|map({name:.metadata.name,uid:.metadata.uid})|sort_by(.name)) and
      ($pods|map(.metadata.uid)|unique|length)==3 and
      all($pods[]; (.metadata.uid|type)=="string" and (.metadata.uid|length)>0 and
        .metadata.deletionTimestamp==null and any(.status.conditions[]?; .type=="Ready" and .status=="True"))
    ' || exit 1
  [ "$(date +%s)" -lt "$voter_wait_deadline" ] || exit 124
)
validate_archive_head_metadata() {
  jq -e --arg bucket rhiza-v070-chaos-ied-20260811 --arg name "${prefix}${cluster}/archive/head.bin" '
    keys==["bucket","generation","name","size"] and .bucket==$bucket and .name==$name and
    (.generation|type)=="string" and (.generation|test("^[1-9][0-9]*$")) and
    (.size|type)=="string" and (.size|test("^(0|[1-9][0-9]*)$")) and
    ((.size|tonumber)>=132) and ((.size|tonumber)<=8388632)
  ' "$1" >/dev/null
}
same_archive_head_metadata() {
  jq -e --slurpfile before "$1" '.generation==$before[0].generation and .size==$before[0].size' "$2" >/dev/null
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
shutdown_events() {
  jq -s -e --slurpfile before "$1/shutdown-before.json" '
    . as $events |
    if any($events[]; .type=="ERROR") then error("watch ERROR event")
    elif any($before[0][]; . as $pod | any($events[];
      .object.metadata.name==$pod.name and (.object.metadata.uid!=$pod.uid or
        any(.object.status.containerStatuses[]?; .name=="rhiza" and
          (.containerID!=$pod.container_id or .imageID!=$pod.image_id or .restartCount!=$pod.restart_count or
            (.state.terminated!=null and (.state.terminated.exitCode!=0 or (.state.terminated.signal // 0)!=0)))))))
      then error("watch writer incarnation/termination changed")
    elif all($before[0][]; . as $pod | any($events[]; .object.metadata.uid==$pod.uid and
      any(.object.status.containerStatuses[]?; .name=="rhiza" and .containerID==$pod.container_id and
        .imageID==$pod.image_id and .restartCount==$pod.restart_count and
        .state.terminated.exitCode==0 and (.state.terminated.signal // 0)==0)))
      then $events else empty end
  ' "$1/shutdown-watch.json"
}
wait_shutdown_watch() {
  # Partial JSON while this stream is still writing is pending, never proof.
  # Preserve the first parser error; EOF/error/deadline without full events fails.
  watch_finished=false
  while :; do
    shutdown_events "$1" > "$1/shutdown-watch-proof.next.json" 2> "$1/shutdown-watch-parse.stderr"
    watch_parse_code=$?
    if [ -s "$1/shutdown-watch-parse.stderr" ] && [ ! -e "$1/shutdown-watch-first-parse.stderr" ]; then
      cp "$1/shutdown-watch-parse.stderr" "$1/shutdown-watch-first-parse.stderr"
      printf '%s\n' "$watch_parse_code" > "$1/shutdown-watch-first-parse.exit"
    fi
    if [ "$watch_parse_code" = 0 ]; then
      mv "$1/shutdown-watch-proof.next.json" "$1/shutdown-watch-proof.json"
      return 0
    fi
    case "$watch_parse_code" in
      4) ;; # Complete JSON, terminal events not yet present.
      5) grep -q '^jq: parse error:' "$1/shutdown-watch-parse.stderr" || return 1 ;;
      *) return 1 ;;
    esac
    [ ! -s "$1/shutdown-watch.stderr" ] || return 1
    [ "$watch_finished" = false ] || return 1
    [ "$(date +%s)" -lt "$3" ] || return 124
    # A stream can finish between parsing and observing EOF. Re-read ONCE at
    # EOF so complete final bytes are not discarded; invalid bytes still fail.
    if [ -n "$2" ]; then
      if ! kill -0 "$2" 2>/dev/null; then watch_finished=true; continue; fi
    elif [ -e "$1/shutdown-watch.exit" ]; then watch_finished=true; continue; fi
    sleep 1
  done
}
verify_shutdown() {
  proof_dir=$1
  case "$(cat "$proof_dir/shutdown-watch.exit")" in 0|143) ;; *) return 1 ;; esac
  jq -e '.items|length==0' "$proof_dir/shutdown-writers-after.json" >/dev/null || return 1
  jq -e '.spec.replicas==0 and (.status.replicas // 0)==0' "$proof_dir/shutdown-controller.json" >/dev/null || return 1
  jq -e --slurpfile expected "$proof_dir/shutdown-expected.json" --arg ns "$ns" --arg run "$RHIZA_RUN_ID" --arg image "$RHIZA_HOST_IMAGE" '
    ($expected[0]|sort_by(.name)) as $expected |
    ($expected|map(.name)) as $names |
    ($names==["rhiza-voter-0","rhiza-voter-1","rhiza-voter-2"] or
      $names==["rhiza-learner","rhiza-voter-0","rhiza-voter-1","rhiza-voter-2"]) and
    all($expected[]; .namespace==$ns and .run==$run and (.uid|type)=="string" and (.uid|length)>0) and
    (map({name,namespace,run,uid})|sort_by(.name))==$expected and
    type=="array" and all(.[]; .namespace==$ns and .run==$run and
      (.uid|type)=="string" and (.uid|length)>0 and (.container_id|type)=="string" and (.container_id|length)>0 and
      .image==$image and (.image_id|endswith($image|split("@")[1])) and
      (.name|test("^rhiza-voter-[012]$|^rhiza-learner$")) and .restart_count>=0) and
    ([.[].uid]|unique|length)==length
  ' "$proof_dir/shutdown-before.json" >/dev/null || return 1
  # Use the complete parsed stream captured before intentionally stopping it.
  [ -s "$proof_dir/shutdown-watch-proof.json" ] || return 1
  for proof_pod in $(jq -r '.[].name' "$proof_dir/shutdown-before.json"); do
    [ "$(cat "$proof_dir/$proof_pod-shutdown-log.exit")" = 0 ] || return 1
    jq -R -s -e --arg name "$proof_pod" --slurpfile before "$proof_dir/shutdown-before.json" '
      ($before[0][]|select(.name==$name)) as $pod |
      [split("\n")[]|fromjson?|select(.namespace==$pod.namespace and .run==$pod.run and
        .pod_uid==$pod.uid and .node_id==$pod.name)] as $markers |
      [$markers[]|select(.event=="qualification-host-start" and (.process|test("^[a-f0-9]{32}$")))] as $starts |
      ($starts|length)==1 and
      ([$markers[]|select(.event=="qualification-host-shutdown-failed")]|length)==0 and
      ([$markers[]|select(.event=="qualification-host-shutdown-complete" and .process==$starts[0].process and
        .http_drained==true and .db_closed==true)]|length)==1
    ' "$proof_dir/$proof_pod-shutdown.log" >/dev/null || return 1
  done
}
if [ "$mode" = check-shutdown ]; then
  : "${RHIZA_SHUTDOWN_FIXTURE:?offline proof directory required}"
  if [ "${RHIZA_SHUTDOWN_WAIT:-}" = yes ]; then
    set +e
    wait_shutdown_watch "$RHIZA_SHUTDOWN_FIXTURE" '' "$(( $(date +%s) + 5 ))"
    wait_code=$?
    [ "$wait_code" = 0 ] || exit "$wait_code"
  else
    shutdown_events "$RHIZA_SHUTDOWN_FIXTURE" > "$RHIZA_SHUTDOWN_FIXTURE/shutdown-watch-proof.json"
  fi
  verify_shutdown "$RHIZA_SHUTDOWN_FIXTURE"
  exit $?
fi
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
  if config=$(timeout --kill-after=5s 5s kubectl --kubeconfig="$RHIZA_LOCAL_RUNTIME_KUBECONFIG" \
    --context="$context" --namespace="$ns" config view --raw -o json 2>/dev/null); then
    :
  else
    parser_exit=$?
    die "local kubeconfig parsing failed (exit $parser_exit)"
  fi
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
stop_forward() {
  forward_stop_pid=$1
  kill -TERM "-$forward_stop_pid" 2>/dev/null || kill -TERM "$forward_stop_pid" 2>/dev/null || true
  forward_stop_tries=0
  while kill -0 "-$forward_stop_pid" 2>/dev/null; do
    forward_stop_tries=$((forward_stop_tries + 1))
    [ "$forward_stop_tries" -lt 5 ] || break
    sleep 1
  done
  if kill -0 "-$forward_stop_pid" 2>/dev/null; then
    kill -KILL "-$forward_stop_pid" 2>/dev/null || kill -KILL "$forward_stop_pid" 2>/dev/null || true
    forward_stop_tries=0
    while kill -0 "-$forward_stop_pid" 2>/dev/null; do
      forward_stop_tries=$((forward_stop_tries + 1))
      [ "$forward_stop_tries" -lt 5 ] || return 1
      sleep 1
    done
  fi
  wait "$forward_stop_pid" 2>/dev/null || true
}
pf_pids=''
active_fault=''
owned_created=false
voters_attempted=false
learner_attempted=false
shutdown_watch_pid=''
shutdown_log_pids=''
shutdown_capture=false
# A watch started from the pre-stop list version replays termination events even
# if the request connects after scale0. No privileged node/container API is used.
shutdown_stream() {
  if [ "$mode" = run-local ]; then
    stream_limit=$(remaining) || return 124
    [ "$stream_limit" -le 240 ] || stream_limit=240
    exec timeout --kill-after=5s "${stream_limit}s" kubectl --kubeconfig="$RHIZA_LOCAL_RUNTIME_KUBECONFIG" \
      --context="$context" --namespace="$ns" --request-timeout=0 "$@"
  fi
  exec timeout --kill-after=5s 240s kubectl --context="$context" --namespace="$ns" --request-timeout=0 "$@"
}
shutdown_watch() {
  # get has no --resource-version flag. The raw namespace URI retains the
  # authentic list version and selector, using the same scoped transport.
  watch_version=$(jq -ner --arg version "$1" '$version|select(length>0)|@uri') || return 1
  watch_selector=$(jq -nr --arg run "$RHIZA_RUN_ID" '"chaos.rhiza.io/run="+$run|@uri') || return 1
  watch_seconds=$(( $2 - $(date +%s) ))
  [ "$watch_seconds" -gt 0 ] || return 124
  shutdown_stream get --raw="/api/v1/namespaces/$ns/pods?watch=true&resourceVersion=$watch_version&labelSelector=$watch_selector&timeoutSeconds=$watch_seconds"
}
capture_shutdown() {
  # Original PodUIDs cover all created writers. Planned container-kill history
  # is not a clean Close: only the current incarnation is pinned below.
  jq -e '[.[]|select(.name|test("^rhiza-voter-[012]$"))] as $v |
    ($v|map(.name)|sort)==["rhiza-voter-0","rhiza-voter-1","rhiza-voter-2"] and
    all($v[]; (.uid|type)=="string" and (.uid|length)>0)
  ' "$out/voters-before.json" >/dev/null || return 1
  jq --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '[.[]|select(.name|test("^rhiza-voter-[012]$"))|{name,uid,namespace:$ns,run:$run}]' \
    "$out/voters-before.json" > "$out/shutdown-expected.json" || return $?
  if [ "$learner_attempted" = true ]; then
    jq -e --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '.metadata.name=="rhiza-learner" and
      .metadata.namespace==$ns and .metadata.labels["chaos.rhiza.io/run"]==$run and
      (.metadata.uid|type)=="string" and (.metadata.uid|length)>0' "$out/learner-created.json" >/dev/null || return 1
    jq --slurpfile learner "$out/learner-created.json" '.+[$learner[0].metadata|{name,uid,namespace,run:.labels["chaos.rhiza.io/run"]}]' \
      "$out/shutdown-expected.json" > "$out/shutdown-expected.next.json" || return $?
    mv "$out/shutdown-expected.next.json" "$out/shutdown-expected.json"
  fi
  snapshot_selector=$(jq -nr --arg run "$RHIZA_RUN_ID" '"chaos.rhiza.io/run="+$run|@uri') || return 1
  # Generic get output rebuilds a client-side List with an empty ListMeta.
  # Keep the server's opaque collection RV, using exactly the watch selector.
  k get --raw="/api/v1/namespaces/$ns/pods?labelSelector=$snapshot_selector" > "$out/shutdown-pods-before.json" 2> "$out/shutdown-pods-before.stderr" || return $?
  jq -e --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '.apiVersion=="v1" and .kind=="PodList" and
    (.metadata.resourceVersion|type)=="string" and (.metadata.resourceVersion|length)>0 and
    (.metadata.continue==null or .metadata.continue=="") and (.items|type)=="array" and
    all(.items[]; .metadata.namespace==$ns and .metadata.labels["chaos.rhiza.io/run"]==$run)
  ' "$out/shutdown-pods-before.json" >/dev/null || return 1
  # Never silently omit a writer whose status/log incarnation is unavailable.
  jq -e 'all(.items[]|select(.metadata.labels.app=="rhiza-voter" or .metadata.name=="rhiza-learner");
    [.status.containerStatuses[]?|select(.name=="rhiza" and (.containerID|type)=="string" and (.containerID|length)>0)]|length==1)
  ' "$out/shutdown-pods-before.json" >/dev/null || return 1
  jq '[.items[]|select(.metadata.labels.app=="rhiza-voter" or .metadata.name=="rhiza-learner")|
    . as $pod | .status.containerStatuses[]?|select(.name=="rhiza")|
    {name:$pod.metadata.name,namespace:$pod.metadata.namespace,run:$pod.metadata.labels["chaos.rhiza.io/run"],uid:$pod.metadata.uid,
      image:([ $pod.spec.containers[]|select(.name=="rhiza")|.image ][0]),image_id:.imageID,
      container_id:.containerID,restart_count:.restartCount}]' "$out/shutdown-pods-before.json" > "$out/shutdown-before.json" || return $?
  jq -e --slurpfile expected "$out/shutdown-expected.json" \
    '(map({name,uid,namespace,run})|sort_by(.name))==($expected[0]|sort_by(.name))' "$out/shutdown-before.json" >/dev/null || return 1
  shutdown_watch_deadline=$(( $(date +%s) + 240 ))
  if [ "$mode" = run-local ] && [ "$shutdown_watch_deadline" -gt "$local_deadline" ]; then shutdown_watch_deadline=$local_deadline; fi
  resource_version=$(jq -er '.metadata.resourceVersion|select(type=="string" and length>0)' "$out/shutdown-pods-before.json") || return 1
  shutdown_watch "$resource_version" "$shutdown_watch_deadline" > "$out/shutdown-watch.json" 2> "$out/shutdown-watch.stderr" & shutdown_watch_pid=$!
  for shutdown_pod in $(jq -r '.[].name' "$out/shutdown-before.json"); do
    shutdown_stream logs "$shutdown_pod" --container=rhiza --follow > "$out/$shutdown_pod-shutdown.log" 2> "$out/$shutdown_pod-shutdown.stderr" &
    shutdown_log_pids="$shutdown_log_pids $shutdown_pod:$!"
  done
  shutdown_capture=true
}
finish_shutdown_capture() {
  for log_owner in $shutdown_log_pids; do
    wait "${log_owner#*:}"
    printf '%s\n' "$?" > "$out/${log_owner%:*}-shutdown-log.exit"
  done
  wait_shutdown_watch "$out" "$shutdown_watch_pid" "$shutdown_watch_deadline"
  watch_proof_code=$?
  printf '%s\n' "$watch_proof_code" > "$out/shutdown-watch-proof.exit"
  if [ -n "$shutdown_watch_pid" ]; then
    kill "$shutdown_watch_pid" 2>/dev/null
    wait "$shutdown_watch_pid"
    printf '%s\n' "$?" > "$out/shutdown-watch.exit"
    shutdown_watch_pid=''
  fi
  [ ! -s "$out/shutdown-watch.stderr" ] || return 1
  [ "$watch_proof_code" = 0 ] || return "$watch_proof_code"
  k get statefulset/rhiza-voter -o json > "$out/shutdown-controller.json" 2> "$out/shutdown-controller.stderr" || return $?
  k get pods -l "chaos.rhiza.io/run=$RHIZA_RUN_ID" -o json > "$out/shutdown-pods-after.json" 2> "$out/shutdown-pods-after.stderr" || return $?
  jq '{items:[.items[]|select(.metadata.labels.app=="rhiza-voter" or .metadata.name=="rhiza-learner")]}' \
    "$out/shutdown-pods-after.json" > "$out/shutdown-writers-after.json" || return $?
  verify_shutdown "$out"
}
cleanup_metadata_only() (
  # Reuse the scoped transport deadline for all reads, delete and wait together.
  voter_wait_deadline=$(( $(date +%s) + 180 ))
  [ "$learner_attempted" = false ] && [ -z "$active_fault$pf_pids" ] && [ "$shutdown_capture" = false ] || return 1
  # An absent or ambiguous create ACK cannot authorize a name-only delete.
  jq -e --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '.kind=="Pod" and .metadata.name=="rhiza-metadata" and
    .metadata.namespace==$ns and .metadata.labels["chaos.rhiza.io/run"]==$run and
    (.metadata.uid|type)=="string" and (.metadata.uid|length)>0' "$out/metadata-created.json" >/dev/null || return 1
  k get namespace "$ns" -o json > "$out/metadata-cleanup-namespace.json" 2> "$out/metadata-cleanup-namespace.stderr" || return $?
  jq -e --arg uid "$RHIZA_BOOTSTRAP_UID" --arg run "$RHIZA_RUN_ID" \
    --arg owner "rhiza-postrelease-$RHIZA_RUN_ID-$RHIZA_AUTH_CREATION_SHA" '
    .metadata.uid==$uid and .metadata.labels["chaos.rhiza.io/run"]==$run and
    .metadata.annotations["rhiza.dev/auth-owner"]==$owner
  ' "$out/metadata-cleanup-namespace.json" >/dev/null || return 1
  # Do not filter by a label: an unexpected namespace actor must fail closed.
  k get pods,statefulsets,deployments,replicasets,daemonsets,jobs,cronjobs,podchaos,networkchaos -o json > "$out/metadata-cleanup-before.json" 2> "$out/metadata-cleanup-before.stderr" || return $?
  jq -e --slurpfile ack "$out/metadata-created.json" --arg ns "$ns" --arg run "$RHIZA_RUN_ID" \
    --arg image "$RHIZA_METADATA_IMAGE" --arg node "$RHIZA_NODE_A" '
    (.items|type)=="array" and (.items|length)==1 and (.metadata.continue==null or .metadata.continue=="") and
    (.items[0] | .kind=="Pod" and .metadata.name=="rhiza-metadata" and .metadata.namespace==$ns and
      .metadata.uid==$ack[0].metadata.uid and .metadata.labels["chaos.rhiza.io/run"]==$run and
      (.metadata.resourceVersion|type)=="string" and (.metadata.resourceVersion|length)>0 and
      (.metadata.ownerReferences // []|length)==0 and .spec.serviceAccountName=="rhiza-gcs" and
      .spec.nodeName==$node and (.spec.containers|length)==1 and
      .spec.containers[0].name=="metadata" and .spec.containers[0].image==$image)
  ' "$out/metadata-cleanup-before.json" >/dev/null || return 1
  jq '.items[0] | {apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:.metadata.uid,resourceVersion:.metadata.resourceVersion}}' \
    "$out/metadata-cleanup-before.json" > "$out/metadata-delete-options.json" || return $?
  k delete --raw "/api/v1/namespaces/$ns/pods/rhiza-metadata" -f "$out/metadata-delete-options.json" \
    > "$out/metadata-delete.stdout" 2> "$out/metadata-delete.stderr" || return $?
  k wait pod/rhiza-metadata --for=delete --timeout=90s > "$out/metadata-gone.stdout" 2> "$out/metadata-gone.stderr" || return $?
  k get pods,statefulsets,deployments,replicasets,daemonsets,jobs,cronjobs,podchaos,networkchaos -o json > "$out/metadata-cleanup-after.json" 2> "$out/metadata-cleanup-after.stderr" || return $?
  jq -e '(.items|type)=="array" and (.items|length)==0 and (.metadata.continue==null or .metadata.continue=="")' \
    "$out/metadata-cleanup-after.json" >/dev/null
)
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
  if [ "$owned_created" = true ] && [ "$voters_attempted" = false ]; then
    cleanup_metadata_only
    cleanup_code=$?
    printf '%s\n' "$cleanup_code" > "$out/metadata-cleanup.exit"
    [ "$cleanup_code" = 0 ] || clean=false
    if [ "$code" = 0 ] && [ "$cleanup_code" != 0 ]; then code=$cleanup_code; fi
  elif [ "$owned_created" = true ]; then
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
  capture_shutdown
  capture_code=$?
  printf '%s\n' "$capture_code" > "$out/shutdown-capture.exit"
  [ "$capture_code" = 0 ] || clean=false
  if [ "$code" = 0 ] && [ "$capture_code" != 0 ]; then code=$capture_code; fi
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
  if [ "$shutdown_capture" = true ]; then
    finish_shutdown_capture
    proof_code=$?
    printf '%s\n' "$proof_code" > "$out/owned-close-proof.exit"
    [ "$proof_code" = 0 ] || clean=false
    if [ "$code" = 0 ] && [ "$proof_code" != 0 ]; then code=$proof_code; fi
  fi
  fi
  forward_cleanup_code=0
  for pid in $pf_pids; do stop_forward "$pid" || forward_cleanup_code=1; done
  printf '%s\n' "$forward_cleanup_code" > "$out/port-forward-cleanup.exit"
  [ "$forward_cleanup_code" = 0 ] || clean=false
  if [ "$code" = 0 ] && [ "$forward_cleanup_code" != 0 ]; then code=$forward_cleanup_code; fi
  if [ "$owned_created" = false ]; then
    printf '%s\n' 'No owned runtime resources created; Root auth cleanup still pending.' > "$out/cleanup-status.txt"
  elif [ "$clean" = true ] && [ "$voters_attempted" = false ]; then
    printf '%s\n' 'Metadata-only Pod cleanup verified; database writer creation was not attempted. No HTTP/DB Close proof. Root storage/IAM reconciliation still pending.' > "$out/cleanup-status.txt"
  elif [ "$clean" = true ]; then
    printf '%s\n' 'Owned HTTP/DB close and pinned container termination verified; NOT universal remote-request quiescence. Root storage/IAM reconciliation still pending.' > "$out/cleanup-status.txt"
  else
    printf '%s\n' 'NOT CLEAN: bounded run-owned shutdown failed; Root must reconcile before storage/IAM cleanup.' > "$out/cleanup-status.txt"
    printf '%s\n' 'NOT CLEAN (see workload.exit for the preserved original result).' >> "$out/result.txt"
  fi
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
run create-metadata k create -f "$out/metadata.yaml" -o json
cp "$out/$seq-create-metadata.stdout" "$out/metadata-created.json"
run metadata-ready k wait pod/rhiza-metadata --for=condition=Ready --timeout=180s
run metadata-credentials metadata_credentials_ready
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
voters_attempted=true
run create-voters k create -f "$out/voters.yaml"
run voters-ready wait_voters 180
forward() {
  pod=$1; port=$2
  case "$port" in
    18080) old_pid=${pf_18080:-} ;;
    18081) old_pid=${pf_18081:-} ;;
    18082) old_pid=${pf_18082:-} ;;
    18083) old_pid=${pf_18083:-} ;;
    *) die 'unsupported port-forward port' ;;
  esac
  if [ -n "$old_pid" ]; then
    stop_forward "$old_pid" || die 'old port-forward process group did not stop'
    current_pids=''
    for pid in $pf_pids; do [ "$pid" = "$old_pid" ] || current_pids="$current_pids $pid"; done
    pf_pids=$current_pids
  fi
  pf_generation=$(( ${pf_generation:-0} + 1 ))
  pf_before="$out/port-forward-$port-generation-$pf_generation-before.json"
  host_digest=${RHIZA_HOST_IMAGE#*@}
  k get pod "$pod" -o json | jq --argjson pid 0 --arg image "$RHIZA_HOST_IMAGE" --arg digest "$host_digest" '
    (.status.containerStatuses|length) as $status_count | (.spec.containers|length) as $spec_count |
    [.status.containerStatuses[]?|select(.name=="rhiza")] as $status |
    [.spec.containers[]?|select(.name=="rhiza")] as $spec |
    {pid:$pid,pod:.metadata.name,uid:.metadata.uid,deleting:.metadata.deletionTimestamp,
      ready:any(.status.conditions[]?;.type=="Ready" and .status=="True"),
      container_id:$status[0].containerID,image:$spec[0].image,image_id:$status[0].imageID} |
    select($status_count==1 and $spec_count==1 and
      ($status|length)==1 and ($spec|length)==1 and .image==$image and (.image_id|endswith($digest)) and
      (.uid|type)=="string" and (.uid|length)>0 and
      .deleting==null and .ready==true and (.container_id|type)=="string" and (.container_id|length)>0 and
      (.image|type)=="string" and (.image|length)>0 and (.image_id|type)=="string" and (.image_id|length)>0)
  ' > "$pf_before" || die 'port-forward target identity invalid'
  forward_deadline=$((started + 1200))
  if [ "$mode" = run-local ] && [ "$local_deadline" -lt "$forward_deadline" ]; then forward_deadline=$local_deadline; fi
  forward_seconds=$((forward_deadline - $(date +%s)))
  [ "$forward_seconds" -gt 0 ] || exit 124
  if [ "$mode" = run-local ]; then
    timeout --kill-after=5s "${forward_seconds}s" kubectl --kubeconfig="$RHIZA_LOCAL_RUNTIME_KUBECONFIG" \
      --context="$context" --namespace="$ns" --request-timeout=30s port-forward "pod/$pod" "$port:8080" "$((port + 100)):8081" > "$out/$pod-portforward.log" 2>&1 &
  else
    timeout --kill-after=5s "${forward_seconds}s" kubectl --context="$context" --namespace="$ns" \
      port-forward "pod/$pod" "$port:8080" "$((port + 100)):8081" > "$out/$pod-portforward.log" 2>&1 &
  fi
  forward_pid=$!
  case "$port" in
    18080) pf_18080=$forward_pid ;;
    18081) pf_18081=$forward_pid ;;
    18082) pf_18082=$forward_pid ;;
    18083) pf_18083=$forward_pid ;;
  esac
  pf_pids="$pf_pids $forward_pid"
  jq --argjson pid "$forward_pid" '.pid=$pid' "$pf_before" > "$pf_before.next"
  mv "$pf_before.next" "$pf_before"
  tries=0
  until curl -fsS --max-time 2 "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; do
    kill -0 "$forward_pid" 2>/dev/null || die 'port-forward exited before readiness'
    [ "$(date +%s)" -lt "$forward_deadline" ] || exit 124
    tries=$((tries + 1)); [ "$tries" -lt 30 ] || die 'port-forward timeout'; sleep 1
  done
  kill -0 "$forward_pid" 2>/dev/null || die 'port-forward exited after readiness'
  pf_after="$out/port-forward-$port-generation-$pf_generation-after.json"
  k get pod "$pod" -o json | jq --argjson pid "$forward_pid" --arg image "$RHIZA_HOST_IMAGE" --arg digest "$host_digest" '
    (.status.containerStatuses|length) as $status_count | (.spec.containers|length) as $spec_count |
    [.status.containerStatuses[]?|select(.name=="rhiza")] as $status |
    [.spec.containers[]?|select(.name=="rhiza")] as $spec |
    {pid:$pid,pod:.metadata.name,uid:.metadata.uid,deleting:.metadata.deletionTimestamp,
      ready:any(.status.conditions[]?;.type=="Ready" and .status=="True"),
      container_id:$status[0].containerID,image:$spec[0].image,image_id:$status[0].imageID} |
    select($status_count==1 and $spec_count==1 and
      ($status|length)==1 and ($spec|length)==1 and .image==$image and (.image_id|endswith($digest)) and
      (.uid|type)=="string" and (.uid|length)>0 and
      .deleting==null and .ready==true and (.container_id|type)=="string" and (.container_id|length)>0 and
      (.image|type)=="string" and (.image|length)>0 and (.image_id|type)=="string" and (.image_id|length)>0)
  ' > "$pf_after" || die 'port-forward target identity invalid after readiness'
  jq -e --slurpfile before "$pf_before" '.==$before[0]' "$pf_after" >/dev/null || die 'port-forward target changed during startup'
}
forward rhiza-voter-0 18080
forward rhiza-voter-1 18081
forward rhiza-voter-2 18082
pod_snapshot() { k get pods -l "chaos.rhiza.io/run=$RHIZA_RUN_ID" -o json | jq '[.items[]|{name:.metadata.name,uid:.metadata.uid,node:.spec.nodeName,volumes:.spec.volumes,containers:.status.containerStatuses,deleting:.metadata.deletionTimestamp,ready:any(.status.conditions[]?;.type=="Ready" and .status=="True")}]'; }
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
  # SQL receipts omit Applied; committed plus no execution error is their
  # success contract. The configured before-ack barrier remains engine-owned.
  jq -e '(.slot|type)=="number" and .slot>0 and (.slot|floor)==.slot and .status=="committed" and (.error_code==null or .error_code=="")' "$out/$seq-ack.stdout" >/dev/null || die 'not a durable successful ACK'
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
container_kill() {
  run process-before pod_snapshot
  cp "$out/$seq-process-before.stdout" "$out/process-before.json"
  jq -e --slurpfile original "$out/voters-before.json" '
    def voters: [.[]|select(.name|test("^rhiza-voter-[012]$"))]|sort_by(.name);
    voters as $b | ($original[0]|voters) as $o |
    ($b|map(.name))==["rhiza-voter-0","rhiza-voter-1","rhiza-voter-2"] and
    ($b|map({name,uid,node,volumes,containers}))==($o|map({name,uid,node,volumes,containers})) and
    all($b[]; (.uid|type)=="string" and (.uid|length)>0 and .deleting==null and .ready==true and
      (.volumes|type)=="array" and any(.volumes[]; .name=="data" and (.emptyDir|type)=="object") and
      ([.containers[]?|select(.name=="rhiza" and .restartCount==0 and
        (.containerID|type)=="string" and (.containerID|length)>0)]|length)==1)
  ' "$out/process-before.json" >/dev/null || die 'original intact-WAL voters not present before container-kill'
  voter0_restarts=1
  fault PodChaos container-kill process-one
  jq -e --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '.kind=="PodChaos" and
    .metadata.name=="process-one" and .metadata.namespace==$ns and .metadata.labels["chaos.rhiza.io/run"]==$run and
    .spec.action=="container-kill" and .spec.mode=="one" and .spec.duration=="10s" and .spec.containerNames==["rhiza"] and
    .spec.selector.namespaces==[$ns] and .spec.selector.labelSelectors["chaos.rhiza.io/run"]==$run and
    .spec.selector.labelSelectors["statefulset.kubernetes.io/pod-name"]=="rhiza-voter-0" and
    ([.status.conditions[]?|select(.type=="AllInjected" and .status=="True")]|length)==1 and
    (.status.experiment.containerRecords|length)==1 and
    (.status.experiment.containerRecords[0] | .id==($ns+"/rhiza-voter-0/rhiza") and
      .phase=="Injected" and .injectedCount==1 and .recoveredCount==0 and
      ([.events[]?|select(.operation=="Apply" and .type=="Succeeded")]|length)==1)
  ' "$out/$seq-injection.stdout" >/dev/null || die 'exact single container-kill injection not proven'
  execute "$RHIZA_RUN_ID-process" "INSERT INTO qualification VALUES (3,'process')"
  # ContainerKill is one-shot: duration does not recover it. Delete the exact
  # fault and wait for its finalizer before proving application recovery.
  run fault-delete k delete "$active_fault" --wait=true --timeout=60s
  active_fault=''
  run voters-ready wait_voters 120
  forward rhiza-voter-0 18080
  run after pod_snapshot
  jq -e --slurpfile before "$out/process-before.json" --slurpfile original "$out/voters-before.json" '
    def voters: [.[]|select(.name|test("^rhiza-voter-[012]$"))]|sort_by(.name);
    def container: [.containers[]?|select(.name=="rhiza")];
    voters as $a | ($before[0]|voters) as $b | ($original[0]|voters) as $o |
    ($a|map(.name))==["rhiza-voter-0","rhiza-voter-1","rhiza-voter-2"] and
    ($b|map(.name))==($a|map(.name)) and ($o|map(.name))==($a|map(.name)) and
    all(range(0;3); . as $i | $a[$i] as $new | $b[$i] as $old |
      ($new|container[0]) as $nc | ($old|container[0]) as $oc |
      $new.name==$old.name and $new.uid==$old.uid and $new.uid==$o[$i].uid and
      ($new.uid|type)=="string" and ($new.uid|length)>0 and
      $new.node==$old.node and $new.volumes==$old.volumes and $new.volumes==$o[$i].volumes and
      $new.deleting==null and $new.ready==true and
      ($new|container|length)==1 and ($old|container|length)==1 and
      ($nc.containerID|type)=="string" and ($nc.containerID|length)>0 and
      ($oc.containerID|type)=="string" and ($oc.containerID|length)>0 and
      ($nc.imageID|type)=="string" and ($nc.imageID|length)>0 and $nc.imageID==$oc.imageID and
      if $i==0 then $oc.restartCount==0 and $nc.restartCount==1 and $nc.containerID!=$oc.containerID
      else $nc.restartCount==0 and $oc.restartCount==0 and $nc.containerID==$oc.containerID end)
  ' "$out/$seq-after.stdout" >/dev/null || die 'voter UID/WAL identity changed or exact container restart not proven'
  metrics
  for port in 18080 18081 18082; do readback "$port" linearizable '[[1,"before"],[2,"network"],[3,"process"]]'; done
}
fault NetworkChaos partition network-one
execute "$RHIZA_RUN_ID-network" "INSERT INTO qualification VALUES (2,'network')"
run recovered k wait "$active_fault" --for=condition=AllRecovered=True --timeout=90s
run fault-delete k delete "$active_fault" --wait=true --timeout=60s
active_fault=''
for port in 18080 18081 18082; do readback "$port" linearizable '[[1,"before"],[2,"network"]]'; done
metrics
container_kill
sleep 15
run current k exec rhiza-metadata -- gcloud storage cat "${storage}${cluster}/checkpoint/CURRENT"
cp "$out/$seq-current.stdout" "$out/CURRENT.json"
run head-metadata-before k exec rhiza-metadata -- gcloud storage objects describe "${storage}${cluster}/archive/head.bin" '--format=json(bucket,name,generation,size)'
cp "$out/$seq-head-metadata-before.stdout" "$out/archive-head-metadata-before.json"
validate_archive_head_metadata "$out/archive-head-metadata-before.json" || die 'published archive head metadata invalid'
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
learner_attempted=true
run cold-create k create -f "$out/learner.yaml" -o json
# This manifest creates a Service and Pod; retain the authentic List ACK and
# extract exactly one learner Pod, not a later name-only lookup.
cp "$out/$seq-cold-create.stdout" "$out/learner-create-ack.json"
jq '[if .kind=="List" then .items[] else . end|select(.kind=="Pod" and .metadata.name=="rhiza-learner")] |
  if length==1 then .[0] else error("learner create UID unresolved") end' \
  "$out/learner-create-ack.json" > "$out/learner-created.json" || die 'learner create UID unresolved'
jq -e --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '.metadata.name=="rhiza-learner" and
  .metadata.namespace==$ns and .metadata.labels["chaos.rhiza.io/run"]==$run and
  (.metadata.uid|type)=="string" and (.metadata.uid|length)>0' "$out/learner-created.json" >/dev/null || die 'learner create UID unresolved'
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
run head-metadata-after k exec rhiza-metadata -- gcloud storage objects describe "${storage}${cluster}/archive/head.bin" '--format=json(bucket,name,generation,size)'
cp "$out/$seq-head-metadata-after.stdout" "$out/archive-head-metadata-after.json"
validate_archive_head_metadata "$out/archive-head-metadata-after.json" || die 'published archive head metadata changed to invalid'
same_archive_head_metadata "$out/archive-head-metadata-before.json" "$out/archive-head-metadata-after.json" || \
  die 'archive head generation or size changed during fresh learner proof'
metrics
printf '%s\n' 'Sampled UID/container high-water totals exclude unseen startup/kill/shutdown tails and metadata SDK calls; 10k attempts/384MiB are planned reserves, NOT hard billing guarantees. Root must reconcile actual object generations/transfer and cleanup.' > "$out/budget-limitations.txt"
printf '%s\n' 'PASS selected GCS/process/network/fresh-learner layer; no whole-generation, promotion, fencing, IO/power-loss or published Rust package claim.' > "$out/result.txt"
