#!/bin/sh
# Shared actual managed bootstrap/replacement checks. Drivers own API, image and cleanup.
managed_bootstrap_check() {
  if jq -e "$4" "$3" >/dev/null 2>&1; then return 0; fi
  printf 'MANAGED_BOOTSTRAP_ORACLE_FAILURE phase=%s check=%s artifact=%s\n' "$1" "$2" "${3##*/}" >&2
  return 1
}

managed_initial_ack_oracle() {
  managed_bootstrap_check initial "$2" "$1" '
    def slot: type=="number" and .>0 and .==floor;
    .status=="committed" and (.slot|slot) and
    (.retry_through_slot|slot) and .retry_through_slot>=.slot' || return 1
  if [ "$2" = insert_ack ]; then
    managed_bootstrap_check initial insert_rows "$1" '.rows_affected==1' || return 1
  fi
}

managed_initial_oracle_self_test() (
  set -eu
  oracle_dir=$(mktemp -d)
  trap 'rm -f "$oracle_dir/ack.json" "$oracle_dir/query.json" "$oracle_dir/pods.json" "$oracle_dir/error"; rmdir "$oracle_dir"' EXIT HUP INT TERM
  printf '%s\n' '{"status":"committed","slot":7,"retry_through_slot":99,"rows_affected":1}' > "$oracle_dir/ack.json"
  managed_initial_ack_oracle "$oracle_dir/ack.json" insert_ack || exit 1
  printf '%s\n' '{"status":"committed","slot":7,"retry_through_slot":99,"applied":false}' > "$oracle_dir/ack.json"
  managed_initial_ack_oracle "$oracle_dir/ack.json" create_ack || exit 1
  for value in '{"status":"rejected","slot":7,"retry_through_slot":99}' '{"status":"committed","slot":0,"retry_through_slot":99}' '{"status":"committed","slot":7}' '{"status":"committed","slot":7,"retry_through_slot":6}' '{"status":"committed","slot":7,"retry_through_slot":"99"}' '{"status":"committed","slot":7.5,"retry_through_slot":99}' '{}' 'not-json' '{"status":"committed","slot":7,"retry_through_slot":99,"rows_affected":0}'; do
    printf '%s\n' "$value" > "$oracle_dir/ack.json"
    if managed_initial_ack_oracle "$oracle_dir/ack.json" insert_ack 2> "$oracle_dir/error"; then exit 1; fi
    grep -Eq '^MANAGED_BOOTSTRAP_ORACLE_FAILURE phase=initial check=(insert_ack|insert_rows) artifact=ack.json$' "$oracle_dir/error" || exit 1
  done
  printf '%s\n' '{"rows":[[1,"real-kubelet"]]}' > "$oracle_dir/query.json"
  managed_bootstrap_check initial query_rows "$oracle_dir/query.json" '.rows==[[1,"real-kubelet"]]' || exit 1
  printf '%s\n' '{"rows":[[2,"wrong"]]}' > "$oracle_dir/query.json"
  if managed_bootstrap_check initial query_rows "$oracle_dir/query.json" '.rows==[[1,"real-kubelet"]]' 2> "$oracle_dir/error"; then exit 1; fi
  grep -Fx 'MANAGED_BOOTSTRAP_ORACLE_FAILURE phase=initial check=query_rows artifact=query.json' "$oracle_dir/error" || exit 1
  printf '%s\n' '{"items":[{"status":{"containerStatuses":[{"restartCount":0}]}}]}' > "$oracle_dir/pods.json"
  managed_bootstrap_check initial restart_zero "$oracle_dir/pods.json" 'all(.items[]; all(.status.containerStatuses[]; .restartCount==0))' || exit 1
  printf '%s\n' '{"items":[{"status":{"containerStatuses":[{"restartCount":1}]}}]}' > "$oracle_dir/pods.json"
  if managed_bootstrap_check initial restart_zero "$oracle_dir/pods.json" 'all(.items[]; all(.status.containerStatuses[]; .restartCount==0))' 2> "$oracle_dir/error"; then exit 1; fi
  grep -Fx 'MANAGED_BOOTSTRAP_ORACLE_FAILURE phase=initial check=restart_zero artifact=pods.json' "$oracle_dir/error" || exit 1
)

managed_preserve_first_pod_fault() (
  proof_dir=$1 proof_pod=$2 proof_uid=$3
  # Absence is represented by one intentionally empty JSON file, never by a
  # missing file. Every other JSON proof still has to be present and nonempty.
  for suffix in request pod-before runtime-before cri-before cri-after injected pods-after before-delete after-delete; do
    [ -f "$proof_dir/pod-kill-$suffix.json" ] || return 1
  done
  for evidence in "$proof_dir"/pod-kill-*.json; do
    [ -f "$evidence" ] || return 1
    case "${evidence##*/}" in
      pod-kill-after-delete.json) [ ! -s "$evidence" ] || return 1 ;;
      *) [ -s "$evidence" ] && jq -e . "$evidence" >/dev/null || return 1 ;;
    esac
  done
  jq -e --arg ns "$namespace" --arg pod "$proof_pod" --arg run "$release" '.metadata.namespace==$ns and .metadata.labels["rhiza-ci-run"]==$run and .spec.action=="pod-kill" and .spec.mode=="one" and .spec.selector.namespaces==[$ns] and .spec.selector.pods=={($ns):[$pod]}' "$proof_dir/pod-kill-request.json" >/dev/null || return 1
  jq -e --arg ns "$namespace" --arg pod "$proof_pod" --arg uid "$proof_uid" '.metadata.namespace==$ns and .metadata.name==$pod and .metadata.uid==$uid' "$proof_dir/pod-kill-pod-before.json" >/dev/null || return 1
  jq -e --arg uid "$proof_uid" 'all(.items[]; .metadata.uid!=$uid)' "$proof_dir/pod-kill-pods-after.json" >/dev/null || return 1
  chaos_record_oracle "$proof_dir/pod-kill-injected.json" "$namespace/$proof_pod" || return 1
  old_container=$(jq -er --arg uid "$proof_uid" 'select(.metadata.uid==$uid)|.status.containerStatuses[]|select(.name=="sql")|.containerID|select(length>0)|sub("^containerd://";"")' "$proof_dir/pod-kill-runtime-before.json") || return 1
  jq -e --arg uid "$proof_uid" --arg id "$old_container" 'any(.containers[]; .id==$id and .state=="CONTAINER_RUNNING" and .labels["io.kubernetes.pod.uid"]==$uid)' "$proof_dir/pod-kill-cri-before.json" >/dev/null || return 1
  jq -e --arg uid "$proof_uid" --arg id "$old_container" '(.containers|type)=="array" and all(.containers[]; (.id!=$id or .state=="CONTAINER_EXITED") and (.state!="CONTAINER_RUNNING" or .labels["io.kubernetes.pod.uid"]!=$uid))' "$proof_dir/pod-kill-cri-after.json" >/dev/null || return 1
  # Keep the producer's empty success proof and independently record a scoped
  # successful GET. An empty stdout from a failed GET is not absence evidence.
  chaos_scope_guard || return 1
  if chaos_k get podchaos/pod-kill --ignore-not-found -o json > "$proof_dir/pod-kill-after-delete-recheck.txt"; then
    printf '0\n' > "$proof_dir/pod-kill-after-delete-recheck.exit"
  else
    code=$?
    printf '%s\n' "$code" > "$proof_dir/pod-kill-after-delete-recheck.exit"
    return "$code"
  fi
  [ ! -s "$proof_dir/pod-kill-after-delete-recheck.txt" ] || return 1
  for evidence in "$proof_dir"/pod-kill-*.json; do
    cp "$evidence" "$proof_dir/first-${evidence##*/}" || return 1
  done
)

managed_completed_repair_oracle() {
  jq -e --arg old_uid "$2" --arg old_node "$3" --arg prior_operation "${4-}" '
    .status.repair as $r | .status.applicationInit.plan as $application | try (
    ($application.applicationProtocol=="rhiza-managed-request-append-only/v1" or $application.applicationProtocol=="rhiza-managed-request-graph-append-only/v1") and
    $application.evidenceFormat==$application.applicationProtocol and
    $r.state=="Completed" and $r.plan.oldNodeID==$old_node and
    $r.plan.sequence=="addThenRemove" and
    $r.plan.expectedConfigID>0 and
    $r.plan.replacement.podUID!=$old_uid and
    ($prior_operation=="" or $r.plan.operationID!=$prior_operation) and
    $r.completion.kind=="Terminal" and
    $r.completion.operationID==$r.plan.operationID and
    $r.completion.expectedConfigID==$r.plan.expectedConfigID and
    $r.completion.expectedAbortSlot==$r.plan.expectedAbortSlot and
    $r.completion.configID==($r.plan.expectedConfigID+2) and
    $r.completion.slot>$r.plan.expectedAbortSlot and
    ($r.completion.hash|if type=="string" then test("^[0-9a-f]{64}$") else false end) and
    ($r.completion.planDigest|if type=="string" then test("^[0-9a-f]{64}$") else false end) and
    $r.preparedHandoff.planDigest==$r.completion.planDigest and
    $r.addCompletion.planDigest==$r.completion.planDigest and
    $r.preparedHandoff.preparedConfig.config_id==$r.plan.expectedConfigID and
    ($r.preparedHandoff.preparedConfig.members|length)==3 and
    ([$r.preparedHandoff.preparedConfig.members[].node_id]|unique|length)==3 and
    ([$r.preparedHandoff.preparedConfig.members[].node_id]|index($old_node))!=null and
    ([$r.preparedHandoff.preparedConfig.members[].node_id]|index($r.plan.replacement.nodeID))==null and
    $r.preparedHandoff.preparedSlot>$r.plan.expectedAbortSlot and
    ($r.preparedHandoff.preparedHash|if type=="string" then test("^[0-9a-f]{64}$") else false end) and
    ($r.preparedHandoff.removeTerminalSlot // 0)==0 and
    ($r.preparedHandoff.removeTerminalHash // "")=="" and
    ($r.preparedHandoff.survivors.config_id // 0)==0 and
    ($r.preparedHandoff.survivors.members // []|length)==0 and
    $r.addCompletion.kind=="Terminal" and
    $r.addCompletion.operationID==$r.plan.operationID and
    $r.addCompletion.expectedConfigID==$r.plan.expectedConfigID and
    $r.addCompletion.expectedAbortSlot==$r.plan.expectedAbortSlot and
    $r.addCompletion.configID==($r.plan.expectedConfigID+1) and
    $r.addCompletion.slot>$r.preparedHandoff.preparedSlot and
    $r.completion.slot>$r.addCompletion.slot and
    ($r.addCompletion.hash|if type=="string" then test("^[0-9a-f]{64}$") else false end) and
    ($r.addCompletion.finalVoterIDs|length)==4 and
    ($r.addCompletion.finalVoterIDs|unique|length)==4 and
    ($r.addCompletion.finalVoterIDs|sort)==(([$r.preparedHandoff.preparedConfig.members[].node_id]+[$r.plan.replacement.nodeID])|sort) and
    ($r.plan.finalVoterIDs|sort)==($r.addCompletion.finalVoterIDs|map(select(.!=$old_node))|sort) and
    all([$r.plan.expectedConfigID,$r.plan.expectedAbortSlot,$r.preparedHandoff.preparedSlot,$r.addCompletion.configID,$r.addCompletion.slot,$r.completion.configID,$r.completion.slot][]; type=="number" and floor==. and .>=0) and
    $r.completion.finalVoterIDs==$r.plan.finalVoterIDs and
    ($r.plan.finalVoterIDs|length)==3 and
    ($r.plan.finalVoterIDs|unique|length)==3 and
    all($r.plan.finalVoterIDs[]; type=="string" and length>0) and
    ($r.plan.finalVoterIDs|index($old_node))==null and
    ($r.plan.finalVoterIDs|index($r.plan.replacement.nodeID))!=null
    ) catch false
  ' "$1" >/dev/null
}
capture_client_job() {
  client_capture_name=$1 client_capture_uid=$2
  client_capture_gap=0
  if client_pods=$(k -n "$namespace" get pods -l "job-name=$client_capture_name" -o json 2>/dev/null); then
    printf '%s\n' "$client_pods" | jq --arg uid "$client_capture_uid" --arg ns "$namespace" '
      def safeStatus: {name,ready,restartCount,state:{waiting:(.state.waiting | if .==null then null else {reason} end),running:.state.running,terminated:(.state.terminated | if .==null then null else {reason,exitCode,signal,startedAt,finishedAt} end)},lastState:{terminated:(.lastState.terminated | if .==null then null else {reason,exitCode,signal,startedAt,finishedAt} end)}};
      {items:[.items[] | select(.metadata.namespace==$ns and any(.metadata.ownerReferences[]?; .kind=="Job" and .uid==$uid and .controller==true)) | {metadata:{name:.metadata.name,namespace:.metadata.namespace,uid:.metadata.uid,ownerReferences:.metadata.ownerReferences},status:{phase:.status.phase,conditions:[.status.conditions[]? | {type,status,reason}],initContainerStatuses:[.status.initContainerStatuses[]? | select(.name=="ready") | safeStatus],containerStatuses:[.status.containerStatuses[]? | select(.name=="client") | safeStatus]}}]}' > "$logs/$client_capture_name-pods.json" || client_capture_gap=1
  else
    printf '%s\n' 'client Pod discovery unavailable' > "$logs/$client_capture_name-evidence-gap.txt"
    return 1
  fi
  if ! jq -e '.items|length==1' "$logs/$client_capture_name-pods.json" >/dev/null; then
    printf '%s\n' 'exact single Job-owned client Pod unavailable' > "$logs/$client_capture_name-evidence-gap.txt"
    return 1
  fi
  client_capture_pod=$(jq -r '.items[0].metadata.name' "$logs/$client_capture_name-pods.json")
  jq -e '.items[0].status.initContainerStatuses|length==1' "$logs/$client_capture_name-pods.json" >/dev/null || client_capture_gap=1
  # The prerequisite and SQL run in the same Pod; retain init failure before cleanup.
  for client_stream in current previous; do
    if [ "$client_stream" = previous ]; then client_previous=--previous; else client_previous=; fi
    client_log_exit=0
    client_log=$(k -n "$namespace" logs "$client_capture_pod" -c ready --tail=200 --limit-bytes=65536 $client_previous 2>&1) || client_log_exit=$?
    printf '%s\n' "$client_log" | head -c 65536 > "$logs/$client_capture_name-ready-$client_stream.log"
    printf '%s\n' "$client_log_exit" > "$logs/$client_capture_name-ready-$client_stream.exit"
    if [ "$client_stream" = current ] && [ "$client_log_exit" -ne 0 ]; then client_capture_gap=1; fi
  done
  for client_stream in current previous; do
    if [ "$client_stream" = previous ]; then client_previous=--previous; else client_previous=; fi
    client_log_exit=0
    client_log=$(k -n "$namespace" logs "$client_capture_pod" -c client --tail=200 --limit-bytes=65536 $client_previous 2>&1) || client_log_exit=$?
    printf '%s\n' "$client_log" | head -c 65536 > "$logs/$client_capture_name-client-$client_stream.log"
    printf '%s\n' "$client_log_exit" > "$logs/$client_capture_name-client-$client_stream.exit"
    if [ "$client_stream" = current ] && [ "$client_log_exit" -ne 0 ]; then client_capture_gap=1; fi
  done
  if [ "$client_capture_gap" -ne 0 ]; then
    printf '%s\n' 'current client logs unavailable' > "$logs/$client_capture_name-evidence-gap.txt"
  fi
  return "$client_capture_gap"
}
wait_client_job() {
  client_wait_name=$1 client_wait_uid=$2
  client_deadline=$(( $(date +%s) + 120 ))
  while :; do
    client_remaining=$(( client_deadline - $(date +%s) ))
    [ "$client_remaining" -gt 0 ] || { printf '%s\n' 'client Job terminal outcome unavailable within 120s' > "$logs/$client_wait_name-evidence-gap.txt"; return 124; }
    client_budget=$client_remaining
    [ "$client_budget" -le 20 ] || client_budget=20
    if client_status=$(k -n "$namespace" get "job/$client_wait_name" --request-timeout="${client_budget}s" -o json 2> "$logs/$client_wait_name-job-api.stderr"); then
      printf '%s\n' "$client_status" | jq --arg uid "$client_wait_uid" -e '.metadata.uid==$uid' >/dev/null || { printf '%s\n' 'created client Job identity changed' > "$logs/$client_wait_name-evidence-gap.txt"; return 1; }
      printf '%s\n' "$client_status" | jq '{apiVersion,kind,metadata:{name:.metadata.name,namespace:.metadata.namespace,uid:.metadata.uid},spec:{backoffLimit:.spec.backoffLimit,activeDeadlineSeconds:.spec.activeDeadlineSeconds},status}' > "$logs/$client_wait_name-job.json"
      if printf '%s\n' "$client_status" | jq -e 'any(.status.conditions[]?; (.type=="Failed" or .type=="FailureTarget") and .status=="True")' >/dev/null; then return 1; fi
      if printf '%s\n' "$client_status" | jq -e 'any(.status.conditions[]?; .type=="Complete" and .status=="True")' >/dev/null; then return 0; fi
    else
      client_api_exit=$?
      printf '%s\n' 'client Job status API unavailable' > "$logs/$client_wait_name-evidence-gap.txt"
      return "$client_api_exit"
    fi
    sleep 1
  done
}
managed_replacement_identity_oracle() {
  if cmp -s "$1" "$2"; then
    echo 'replacement Pod incorrectly reused the deleted voter identity' >&2
    return 1
  fi
}
managed_same_pod_identity_oracle() {
  cmp -s "$1" "$2" || { echo 'same-Pod restart changed voter identity' >&2; return 1; }
}
managed_mixed_tuple_oracle() {
  tuple_log=$1 tuple_phase=$2 tuple_uid=$3 tuple_output=$4
  [ "$(grep -Fxc "PASS: managed Rust FFI mixed $tuple_phase contract" "$tuple_log")" = 1 ] || return 1
  [ "$(grep -Fxc "RUST_MIXED_CURRENT_UID=$tuple_uid" "$tuple_log")" = 1 ] || return 1
  [ "$(grep -c '^RUST_MIXED_TUPLE=' "$tuple_log")" = 1 ] || return 1
  sed -n 's/^RUST_MIXED_TUPLE=//p' "$tuple_log" > "$tuple_output.raw"
  [ "$(wc -c < "$tuple_output.raw")" -lt 16384 ] || return 1
  jq -e --arg producer "$producer_uid" '
    keys==["cases","producer_uid","version"] and .version==1 and .producer_uid==$producer and (.cases|length)==5 and
    all(.cases[]; keys==["expected","expected_exists","key_b64","operation","receipt","recovered","request_id_b64","value","wanted_value"] and (.recovered|type)=="boolean" and (.expected_exists|type)=="boolean" and
      (.receipt|keys)==["applied","retry_through_slot","slot","status"]) and
    ([.cases[].request_id_b64]|unique|length)==5 and
    ([.cases[0:3][].request_id_b64]==["/w==","/g==","77+9"]) and
    ([.cases[].operation]==["put","put","put","cas","delete"]) and
    ([.cases[].key_b64]==["/w==","/g==","77+9","/w==","/gA="]) and
    ([.cases[].value]==["ZmYtdmFsdWU=","ZmUtdmFsdWU=",null,"",""]) and
    ([.cases[].expected]==[null,"","/w==",null,null]) and
    ([.cases[].expected_exists]==[true,false,true,false,false]) and
    ([.cases[].wanted_value]==["ZmYtdmFsdWU=","ZmUtdmFsdWU=","","ZmYtdmFsdWU=",null]) and
    ([.cases[].receipt.applied]==[true,true,true,false,false]) and
    all(.cases[]; .receipt.status=="committed" and .receipt.slot>0 and .receipt.retry_through_slot>=.receipt.slot) and
    ([.cases[].receipt.slot]|unique|length)==5' "$tuple_output.raw" >/dev/null || return 1
  if [ "$tuple_phase" = initial ]; then
    [ "$(grep -c '^RUST_MIXED_ROW_ACK_SLOT=[1-9][0-9]*$' "$tuple_log")" = 1 ] || return 1
    tuple_sql_slot=$(sed -n 's/^RUST_MIXED_ROW_ACK_SLOT=//p' "$tuple_log")
    jq -e --argjson slot "$tuple_sql_slot" 'all(.cases[]; .receipt.slot<$slot)' "$tuple_output.raw" >/dev/null || return 1
  else
    [ "$(grep -c '^RUST_MIXED_ROW_ACK_SLOT=' "$tuple_log" || :)" = 0 ] || return 1
  fi
  jq -S . "$tuple_output.raw" > "$tuple_output"
}

managed_mixed_tuple_self_test() (
  managed_initial_oracle_self_test || exit 1
  set -eu
  tuple_test_dir=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-mixed-oracle.XXXXXX")
  # shellcheck disable=SC2329 # Invoked by the owned subshell EXIT/signal trap.
  tuple_test_cleanup() {
    tuple_code=$?
    trap - EXIT HUP INT TERM
    rm -rf "$tuple_test_dir" || { [ "$tuple_code" -ne 0 ] || tuple_code=1; }
    [ ! -e "$tuple_test_dir" ] || { [ "$tuple_code" -ne 0 ] || tuple_code=1; }
    exit "$tuple_code"
  }
  trap tuple_test_cleanup EXIT HUP INT TERM
  producer_uid=00000000-0000-0000-0000-000000000001
  jq -cn --arg producer "$producer_uid" '{version:1,producer_uid:$producer,cases:[range(0;5)|. as $i|{request_id_b64:(["/w==","/g==","77+9","/wE=","/gE="][$i]),key_b64:(["/w==","/g==","77+9","/w==","/gA="][$i]),operation:(["put","put","put","cas","delete"][$i]),value:(["ZmYtdmFsdWU=","ZmUtdmFsdWU=",null,"",""][$i]),expected:([null,"","/w==",null,null][$i]),expected_exists:([true,false,true,false,false][$i]),wanted_value:(["ZmYtdmFsdWU=","ZmUtdmFsdWU=","","ZmYtdmFsdWU=",null][$i]),receipt:{slot:($i+1),status:"committed",applied:($i<3),retry_through_slot:100},recovered:false}]}' > "$tuple_test_dir/tuple"
  { printf 'PASS: managed Rust FFI mixed initial contract\nRUST_MIXED_CURRENT_UID=%s\nRUST_MIXED_ROW_ACK_SLOT=7\nRUST_MIXED_TUPLE=' "$producer_uid"; cat "$tuple_test_dir/tuple"; } > "$tuple_test_dir/good"
  managed_mixed_tuple_oracle "$tuple_test_dir/good" initial "$producer_uid" "$tuple_test_dir/actual"
  for tuple_fault in wrong-uid duplicate-pass missing-tuple missing-row-ack false-cas; do
    case "$tuple_fault" in
      wrong-uid) sed "s/$producer_uid/00000000-0000-0000-0000-000000000099/g" "$tuple_test_dir/good" ;;
      duplicate-pass) cat "$tuple_test_dir/good"; printf 'PASS: managed Rust FFI mixed initial contract\n' ;;
      missing-tuple) sed '/^RUST_MIXED_TUPLE=/d' "$tuple_test_dir/good" ;;
      missing-row-ack) sed '/^RUST_MIXED_ROW_ACK_SLOT=/d' "$tuple_test_dir/good" ;;
      false-cas) sed 's/"applied":false/"applied":true/g' "$tuple_test_dir/good" ;;
    esac > "$tuple_test_dir/bad"
    if managed_mixed_tuple_oracle "$tuple_test_dir/bad" initial "$producer_uid" "$tuple_test_dir/rejected" >/dev/null 2>&1; then echo 'mixed oracle accepted invalid evidence' >&2; exit 1; fi
  done
  replacement_uid=00000000-0000-0000-0000-000000000002
  { printf 'PASS: managed Rust FFI mixed verify contract\nRUST_MIXED_CURRENT_UID=%s\nRUST_MIXED_TUPLE=' "$replacement_uid"; cat "$tuple_test_dir/tuple"; } > "$tuple_test_dir/verify"
  managed_mixed_tuple_oracle "$tuple_test_dir/verify" verify "$replacement_uid" "$tuple_test_dir/verified"
  cmp "$tuple_test_dir/actual" "$tuple_test_dir/verified"
  printf '%s\n' 'PASS: mixed tuple initial/verify oracle and failure controls'
)
managed_graph_tuple_oracle() {
  graph_log=$1 graph_phase=$2 graph_uid=$3 graph_output=$4
  case "$graph_phase" in initial|verify) ;; *) return 2 ;; esac
  [ "$(grep -Fxc "PASS: managed Rust FFI Graph $graph_phase state contract" "$graph_log")" = 1 ] || return 1
  [ "$(grep -Fxc "RUST_GRAPH_CURRENT_UID=$graph_uid" "$graph_log")" = 1 ] || return 1
  [ "$(grep -c '^RUST_GRAPH_TUPLE=' "$graph_log")" = 1 ] || return 1
  sed -n 's/^RUST_GRAPH_TUPLE=//p' "$graph_log" > "$graph_output.raw"
  [ "$(wc -c < "$graph_output.raw")" -le 32768 ] || return 1
  jq -e '.version==1 and .trim_before_count==2 and (.producer_uid|type)=="string" and (.cases|length)==5 and
    ([.cases[].request_id_b64]==["/wE=","/gI=","77+9","Z3JhcGgtb2Zmc2V0LW1heA==","Z3JhcGgtdHJpbS1tYXg="]) and
    all(.cases[]; (.expected_original_b64|type)=="string" and (.expected_original_b64|length)>0 and (.frame_b64|type)=="string" and (.frame_b64|length)>0 and
      (.recovered|type)=="boolean" and .receipt.status=="committed" and .receipt.applied==true and
      (.receipt.slot|type)=="number" and .receipt.slot>0 and .receipt.retry_through_slot>=.receipt.slot) and
    .state.nodes==[["a","alpha",0.1],["b","beta",2]] and .state.edges==[["a","b"]] and
    ([.state.events[].payload]==[1,2,3]) and all(.state.events[]; .kind=="ordered" and .sequence>0) and
    .state.events[0].sequence<.state.events[1].sequence and .state.events[1].sequence<.state.events[2].sequence and
    .state.offset=="18446744073709551615" and .state.trim_empty==true' "$graph_output.raw" >/dev/null || return 1
  jq -S . "$graph_output.raw" > "$graph_output"
}

managed_graph_tuple_self_test() (
  graph_test_dir=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-graph-oracle.XXXXXX")
  trap 'rm -f "$graph_test_dir/tuple" "$graph_test_dir/good" "$graph_test_dir/bad" "$graph_test_dir/out.raw" "$graph_test_dir/out"; rmdir "$graph_test_dir"' EXIT HUP INT TERM
  jq -cn '{version:1,producer_uid:"producer",trim_before_count:2,cases:["/wE=","/gI=","77+9","Z3JhcGgtb2Zmc2V0LW1heA==","Z3JhcGgtdHJpbS1tYXg="]|map({request_id_b64:.,expected_original_b64:"TQ==",frame_b64:"UQ==",receipt:{slot:7,status:"committed",applied:true,retry_through_slot:99},recovered:false}),state:{nodes:[["a","alpha",0.1],["b","beta",2]],edges:[["a","b"]],events:[range(1;4)|{sequence:.,kind:"ordered",payload:.}],offset:"18446744073709551615",trim_empty:true}}' > "$graph_test_dir/tuple"
  { printf '%s\n' 'PASS: managed Rust FFI Graph initial state contract' 'RUST_GRAPH_CURRENT_UID=producer'; printf 'RUST_GRAPH_TUPLE='; cat "$graph_test_dir/tuple"; } > "$graph_test_dir/good"
  managed_graph_tuple_oracle "$graph_test_dir/good" initial producer "$graph_test_dir/out"
  for graph_bad in missing-pass wrong-uid duplicate-pass wrong-edge missing-event wrong-u64 unacked; do
    case "$graph_bad" in
      missing-pass) sed '/^PASS:/d' "$graph_test_dir/good" > "$graph_test_dir/bad" ;;
      wrong-uid) sed 's/CURRENT_UID=producer/CURRENT_UID=other/' "$graph_test_dir/good" > "$graph_test_dir/bad" ;;
      duplicate-pass) cat "$graph_test_dir/good" > "$graph_test_dir/bad"; printf '%s\n' 'PASS: managed Rust FFI Graph initial state contract' >> "$graph_test_dir/bad" ;;
      *)
        case "$graph_bad" in wrong-edge) graph_filter='.state.edges=[]' ;; missing-event) graph_filter='.state.events|=.[0:2]' ;; wrong-u64) graph_filter='.state.offset="18446744073709551614"' ;; unacked) graph_filter='.cases[0].receipt.slot=0' ;; esac
        { printf '%s\n' 'PASS: managed Rust FFI Graph initial state contract' 'RUST_GRAPH_CURRENT_UID=producer'; printf 'RUST_GRAPH_TUPLE='; jq -c "$graph_filter" "$graph_test_dir/tuple"; } > "$graph_test_dir/bad" ;;
    esac
    if managed_graph_tuple_oracle "$graph_test_dir/bad" initial producer "$graph_test_dir/out" >/dev/null 2>&1; then echo 'Graph oracle accepted invalid evidence' >&2; exit 1; fi
  done
)

managed_graph_cabi_oracle() {
  cabi_log=$1 cabi_uid=$2 cabi_out=$3
  [ "$(grep -Fxc 'PASS: actual Go CABI Graph native/root/counter qualification' "$cabi_log")" = 1 ] || return 1
  [ "$(grep -c '^GRAPH_CABI_QUALIFICATION=' "$cabi_log")" = 1 ] || return 1
  sed -n 's/^GRAPH_CABI_QUALIFICATION=//p' "$cabi_log" > "$cabi_out.raw"
  [ "$(wc -c < "$cabi_out.raw")" -le 2048 ] || return 1
  jq -e --arg uid "$cabi_uid" '.version==1 and .scope=="go-cabi-incluster" and .pod_uid==$uid and
    ([.base,.seal_slot,.tip,.graph_suffix,.native_originals,.normal_kube_requests,.recovery_application_proposal_entries,.positive_graph_proposal_entries,.positive_kv_proposal_entries,.positive_sql_proposal_entries,.logical_deletes,.http_deletes,.sdk_http_requests,.startup_kube_requests]|all(type=="number" and .>=0 and floor==.)) and
    .base>0 and .base<.seal_slot and .seal_slot<=.tip and .graph_suffix>0 and .native_originals==5 and
    (.root_hash|test("^[0-9a-f]{64}$")) and (.seal_hash|test("^[0-9a-f]{64}$")) and
    .normal_kube_requests==0 and .recovery_application_proposal_entries==0 and .positive_graph_proposal_entries>0 and .positive_kv_proposal_entries>0 and .positive_sql_proposal_entries>0 and
    .logical_deletes==0 and .http_deletes==0 and .sdk_http_requests>0 and .startup_kube_requests>0' "$cabi_out.raw" >/dev/null || return 1
  jq -S . "$cabi_out.raw" > "$cabi_out"
}

managed_graph_cabi_self_test() (
  cabi_dir=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-graph-cabi-oracle.XXXXXX")
  trap 'rm -f "$cabi_dir/data" "$cabi_dir/log" "$cabi_dir/out.raw" "$cabi_dir/out"; rmdir "$cabi_dir"' EXIT HUP INT TERM
  jq -cn '{version:1,scope:"go-cabi-incluster",pod_uid:"owned",base:2,seal_slot:3,tip:5,root_hash:("a"*64),seal_hash:("b"*64),graph_suffix:1,native_originals:5,normal_kube_requests:0,recovery_application_proposal_entries:0,positive_graph_proposal_entries:1,positive_kv_proposal_entries:1,positive_sql_proposal_entries:1,logical_deletes:0,http_deletes:0,sdk_http_requests:4,startup_kube_requests:2}' > "$cabi_dir/data"
  for cabi_filter in '.' '.scope="rust"' '.pod_uid="other"' '.base=0' '.graph_suffix=0' '.normal_kube_requests=1' '.recovery_application_proposal_entries=1' '.positive_graph_proposal_entries=0' '.positive_kv_proposal_entries=0' '.positive_sql_proposal_entries=0' '.positive_sql_proposal_entries="1"' '.http_deletes=1' '.native_originals=4'; do
    { printf '%s\n' 'PASS: actual Go CABI Graph native/root/counter qualification'; printf 'GRAPH_CABI_QUALIFICATION='; jq -c "$cabi_filter" "$cabi_dir/data"; } > "$cabi_dir/log"
    if managed_graph_cabi_oracle "$cabi_dir/log" owned "$cabi_dir/out"; then
      [ "$cabi_filter" = . ] || exit 1
    else
      [ "$cabi_filter" != . ] || exit 1
    fi
  done
)

managed_fixture_profile_family() {
  case "$1" in
    internal|internal-append-only) printf '%s\n' internal ;;
    graph|graph-append-only) printf '%s\n' graph ;;
    *) return 1 ;;
  esac
}

managed_fixture_profile_protocol() {
  case "$1" in
    internal|internal-append-only) printf '%s\n' rhiza-managed-request-append-only/v1 ;;
    graph|graph-append-only) printf '%s\n' rhiza-managed-request-graph-append-only/v1 ;;
    *) return 1 ;;
  esac
}

managed_replacement_ffi_contract() (
  [ "${RHIZA_MANAGED_TEST_DOCKERFILE-}" = Dockerfile.managed-ffi-contract ] || return 0
  ffi_pod=$1 ffi_uid=$2 ffi_phase=$3
  ffi_log="$logs/go-ffi-contract-$ffi_phase.log"
  k -n "$namespace" logs "$ffi_pod" -c sql > "$ffi_log"
  if [ "$fixture_family" = internal ]; then
    [ "$(grep -Fxc 'RHIZA_ACTUAL_MATERIALIZATION_PROFILE=9' "$ffi_log")" = 1 ]
  fi
  if [ "$fixture_case" != legacy ]; then
    if [ "$fixture_family" = graph ]; then
      [ "$(grep -Fxc 'RHIZA_ACTUAL_MATERIALIZATION_PROFILE=10' "$ffi_log")" = 1 ]
      managed_graph_tuple_oracle "$ffi_log" verify "$ffi_uid" "$logs/rust-graph-$ffi_phase-tuple.json"
      managed_graph_cabi_oracle "$ffi_log" "$ffi_uid" "$logs/graph-cabi-$ffi_phase.json"
      cmp "$logs/rust-graph-prefault-tuple.json" "$logs/rust-graph-$ffi_phase-tuple.json"
    else
      grep -F 'actual in-cluster Go FFI -> mixed managed Host -> SQL KV ACK readback -> close passed' "$ffi_log" >/dev/null
    fi
    managed_mixed_tuple_oracle "$ffi_log" verify "$ffi_uid" "$logs/rust-mixed-$ffi_phase-tuple.json"
    cmp "$logs/rust-mixed-prefault-tuple.json" "$logs/rust-mixed-$ffi_phase-tuple.json"
    run_client "mixed-$ffi_phase" query '{"sql":"SELECT tuple FROM rust_mixed_contract WHERE id=1","consistency":"linearizable"}'
    jq -S '.rows[0][0]|fromjson' "$logs/mixed-$ffi_phase.json" > "$logs/rust-mixed-$ffi_phase-row.json"
    cmp "$logs/rust-mixed-prefault-tuple.json" "$logs/rust-mixed-$ffi_phase-row.json"
  else
    grep -F 'actual in-cluster Go FFI -> managed Host -> SQL ACK readback -> close passed' "$ffi_log" >/dev/null
    grep -Fx 'PASS: managed Rust FFI SQL contract' "$ffi_log" >/dev/null
  fi
)

managed_fixture() {
fixture_case=${RHIZA_MANAGED_FIXTURE_CASE:-internal}
case "$fixture_case" in
  internal|internal-append-only|graph|graph-append-only)
    fixture_protocol=$(managed_fixture_profile_protocol "$fixture_case") || return 1
    fixture_evidence=$fixture_protocol
    ;;
  *) echo 'invalid managed fixture case' >&2; return 1 ;;
esac
fixture_family=$(managed_fixture_profile_family "$fixture_case")
floci_node_selector='{}'
fixture_provider=${fixture_provider-s3}
case "$fixture_provider" in
  s3) floci_image=floci/floci:2.3.0; floci_port=4566 ;;
  gcs) floci_image=floci/floci-gcp:0.11.0; floci_port=4588 ;;
  azure) floci_image=floci/floci-az:0.15.0; floci_port=4577 ;;
  *) echo 'unsupported explicit emulator provider' >&2; return 1 ;;
esac
printf 'case=%s\nprovider=%s\nemulator=%s\nport=%s\nprotocol=%s\n' "$fixture_case" "$fixture_provider" "$floci_image" "$floci_port" "$fixture_protocol" > "$logs/fixture-provider.txt"
if [ -n "${RHIZA_MANAGED_FIXTURE_NODE-}" ]; then
  printf '%s\n' "$RHIZA_MANAGED_FIXTURE_NODE" | grep -Eq '^[a-z0-9][a-z0-9.-]{0,252}$' || {
    echo 'invalid optional Floci fixture node hostname' >&2; return 1;
  }
  floci_node_selector=$(jq -cn --arg node "$RHIZA_MANAGED_FIXTURE_NODE" '{"kubernetes.io/hostname":$node}')
fi
# Create the Service first so both variants see the same existing Service set.
jq -n --argjson port "$floci_port" '{apiVersion:"v1",kind:"Service",metadata:{name:"floci"},spec:{selector:{app:"floci"},ports:[{port:$port,targetPort:$port}]}}' | k -n "$namespace" create -f -
# Bounded diagnostic only: retain actual failure logs, never infer the Spring cause.
jq -n --argjson nodeSelector "$floci_node_selector" --arg image "$floci_image" --argjson port "$floci_port" '{apiVersion:"v1",kind:"Pod",metadata:{name:"floci-service-links-probe",labels:{"rhiza-ci-component":"floci","rhiza-ci-probe":"service-links"}},spec:{nodeSelector:$nodeSelector,activeDeadlineSeconds:45,enableServiceLinks:true,automountServiceAccountToken:false,containers:[{name:"floci",image:$image,readinessProbe:{tcpSocket:{port:$port},periodSeconds:1}}]}}' | k -n "$namespace" create -f -
if k -n "$namespace" wait --for=condition=Ready pod/floci-service-links-probe --timeout=45s > "$logs/floci-service-links-wait.txt" 2>&1; then
  printf '%s\n' 'NOT REPRODUCED: serviceLinks=true diagnostic Pod reached TCP readiness; old startup cause remains unknown' > "$logs/floci-service-links-result.txt"
else
  k -n "$namespace" get pod floci-service-links-probe -o json > "$logs/floci-service-links-probe.json"
  if jq -e 'any(.status.containerStatuses[]?; .restartCount>0 or (.state.terminated.exitCode//0)!=0)' "$logs/floci-service-links-probe.json" >/dev/null; then
    printf '%s\n' 'OBSERVED: serviceLinks=true diagnostic Pod exited/restarted; inspect captured actual logs for cause (not assumed)' > "$logs/floci-service-links-result.txt"
  else
    printf '%s\n' 'INCONCLUSIVE: serviceLinks=true diagnostic Pod did not reach TCP readiness within 45s; old startup cause remains unknown' > "$logs/floci-service-links-result.txt"
  fi
fi
capture_floci
# The actual SDK fixture excludes injected Service environment and must be TCP-ready.
jq -n --argjson nodeSelector "$floci_node_selector" --arg image "$floci_image" --argjson port "$floci_port" '{apiVersion:"apps/v1",kind:"Deployment",metadata:{name:"floci"},spec:{replicas:1,selector:{matchLabels:{app:"floci"}},template:{metadata:{labels:{app:"floci","rhiza-ci-component":"floci"}},spec:{nodeSelector:$nodeSelector,enableServiceLinks:false,automountServiceAccountToken:false,containers:[{name:"floci",image:$image,ports:[{containerPort:$port}],readinessProbe:{tcpSocket:{port:$port},periodSeconds:1}}]}}}}' | k -n "$namespace" create -f -
k -n "$namespace" rollout status deployment/floci --timeout=120s
k -n "$namespace" get pods -l app=floci -o json > "$logs/floci-sdk-ready.json"
jq -e '.items|length==1 and all(.[]; .spec.enableServiceLinks==false and any(.status.conditions[]?; .type=="Ready" and .status=="True"))' "$logs/floci-sdk-ready.json" >/dev/null
if [ -n "${RHIZA_MANAGED_FIXTURE_NODE-}" ]; then
  jq -e --arg node "$RHIZA_MANAGED_FIXTURE_NODE" '.items|length==1 and all(.[]; .spec.nodeSelector["kubernetes.io/hostname"]==$node and .spec.nodeName==$node)' "$logs/floci-sdk-ready.json" >/dev/null
fi
bucket="rhiza-managed-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT-$nonce"
managed_bucket_setup
admin=$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')
umask 077
printf '%s' "$admin" > "$private/admin.token"
fixture_gcs_credentials=/dev/null
if [ "$fixture_provider" = gcs ]; then fixture_gcs_credentials=$private/gcs-emulator.json; fi
jq -n --arg image "$image" --rawfile admin "$private/admin.token" --rawfile gcs "$fixture_gcs_credentials" --arg provider "$fixture_provider" --arg bucket "$bucket" --arg root "ci-$nonce" --arg protocol "$fixture_protocol" --arg evidence "$fixture_evidence" '{InitImage:$image,PeerPort:9090,EvidenceFormat:$evidence,ProfileSecretName:"runtime",Storage:{enable_reconfiguration:true,admin_token:$admin,objstore_provider:$provider,objstore_bucket:$bucket,objstore_prefix:$root,objstore_durability:"before-ack"}} | .Storage += (if $provider=="s3" then {objstore_endpoint:"floci:4566",objstore_region:"us-east-1",objstore_insecure:true,objstore_access_key:"rhiza-local-test",objstore_secret_key:"rhiza-local-test"} elif $provider=="gcs" then {objstore_insecure:false,objstore_service_account:$gcs} else {objstore_insecure:false,objstore_azure_storage_account:"rhizatest",objstore_azure_connection_string:("DefaultEndpointsProtocol=http;AccountName=rhizatest;AccountKey="+("rhiza-local-test-key-only-32bytes!"|@base64)+";BlobEndpoint=http://floci:4577/rhizatest;")} end) | if $protocol=="" then . else .ApplicationProtocol=$protocol end' > "$private/runtime.json"
if [ "$fixture_family" = graph ]; then
  # This exercises the real policy10 product worker, not a test-only publisher.
  jq '.Storage.checkpoint_interval=1000000000 | .Storage.checkpoint_tail_bytes=1' "$private/runtime.json" > "$private/runtime.graph.json"
  mv "$private/runtime.graph.json" "$private/runtime.json"
fi
printf 'header = "Authorization: Bearer %s"\n' "$admin" > "$private/curl.conf"
unset admin
k -n "$namespace" create secret generic runtime --from-file="runtime.json=$private/runtime.json"
# Consume the public managed overlay, not a test-owned operator Deployment.
# CRDs and the isolated namespace were already installed by the driver.
kubectl kustomize deploy/operator-managed > "$logs/managed-install.yaml"
yq -o=json -I=0 '.' "$logs/managed-install.yaml" > "$private/managed-install.jsonl"
jq -s --arg namespace "$namespace" --arg image "$image" '
  map(select(.kind!="CustomResourceDefinition" and .kind!="Namespace") |
    .metadata.namespace=$namespace |
    if .kind=="RoleBinding" then .subjects |= map(if .kind=="ServiceAccount" then .namespace=$namespace else . end) else . end |
    if .kind=="Deployment" and .metadata.name=="rhiza-operator" then
      .spec.template.spec.containers |= map(if .name=="operator" then .image=$image else . end)
    else . end) | {apiVersion:"v1",kind:"List",items:.}' "$private/managed-install.jsonl" > "$logs/managed-install.json"
jq -e '[.items[]|select(.kind=="Deployment")]|length==1 and .[0].metadata.name=="rhiza-operator"' "$logs/managed-install.json" >/dev/null
jq -e '[.items[]|select(.kind=="Role" and .metadata.name=="rhiza-operator")|.rules[]|select((.resources|index("rhizaclusters/status"))!=null and (.verbs|index("update"))!=null)]|length==1' "$logs/managed-install.json" >/dev/null
# Withhold ONLY operator status-update until actual kubelet gate observations.
jq '(.items[]|select(.kind=="Role" and .metadata.name=="rhiza-operator")|.rules[]|select(.resources|index("rhizaclusters/status"))|.verbs) -= ["update"]' "$logs/managed-install.json" > "$logs/managed-install-gated.json"
jq -e 'all(.items[]|select(.kind=="Role" and .metadata.name=="rhiza-operator")|.rules[]|select(.resources|index("rhizaclusters/status")); (.verbs|index("update"))==null)' "$logs/managed-install-gated.json" >/dev/null
k -n "$namespace" apply --validate=strict -f "$logs/managed-install-gated.json"
k -n "$namespace" get deployments -o json > "$logs/before-cr-deployments.json"
jq -e 'all(.items[]; .metadata.name!="" and .metadata.name!="sql" and (.metadata.labels["rhiza.mrchypark.dev/cluster-uid"] // "")=="")' "$logs/before-cr-deployments.json" >/dev/null
jq -n --arg image "$image" --arg protocol "$fixture_protocol" '{apiVersion:"rhiza.mrchypark.dev/v1alpha2",kind:"RhizaCluster",metadata:{name:"sql"},spec:{logicalID:"managed-ci",genesisIntent:"Create",voterCount:3,workload:{podTemplate:{spec:{containers:[{name:"sql",image:$image}]}}}}} | if $protocol=="" then . else .spec.applicationProtocol=$protocol end' | k -n "$namespace" apply -f -
uid=$(k -n "$namespace" get rhizacluster sql -o jsonpath='{.metadata.uid}')
selector="rhiza.mrchypark.dev/cluster-uid=$uid"
attempt=0
until k -n "$namespace" get pods -l "$selector" -o json > "$logs/gated-pods.json" && sh scripts/ha-managed-bucket-setup oracle-gated "$logs/gated-pods.json"; do
  attempt=$((attempt+1)); [ "$attempt" -lt 120 ] || exit 1; sleep 1
done
jq '.items[]|select(.kind=="Role" and .metadata.name=="rhiza-operator")' "$logs/managed-install.json" | k -n "$namespace" apply --validate=strict -f -
k -n "$namespace" get serviceaccounts,roles,rolebindings -o jsonpath='{range .items[*]}{.kind}{" "}{.metadata.name}{"\n"}{end}' > "$logs/public-rbac-names.txt"
k -n "$namespace" wait --for=condition=Ready pod -l "$selector" --timeout=180s
k -n "$namespace" get rhizacluster sql -o json > "$logs/committed.json"
jq -e '.status.bootstrapPlan.reconfiguration==true' "$logs/committed.json" >/dev/null
jq -e --arg protocol "$fixture_protocol" --arg evidence "$fixture_evidence" '(.status.applicationInit.plan.applicationProtocol // "")==$protocol and .status.applicationInit.plan.evidenceFormat==$evidence and .status.bootstrapPlan.evidenceFormat==$evidence' "$logs/committed.json" >/dev/null
sh scripts/ha-managed-bucket-setup oracle-plan "$logs/gated-pods.json" "$logs/committed.json"
k -n "$namespace" get pods -l "$selector" -o json > "$logs/ready-pods.json"
# Test-owned route only: this does not qualify operator-managed Service ownership.
jq -n --arg uid "$uid" '{apiVersion:"v1",kind:"Service",metadata:{name:"bootstrap-client",labels:{"rhiza-ci-owner":"harness"}},spec:{selector:{"rhiza.mrchypark.dev/cluster-uid":$uid},ports:[{port:8080,targetPort:8080}]}}' | k -n "$namespace" create -f -
attempt=0
until k -n "$namespace" get endpoints bootstrap-client -o json > "$logs/client-endpoints.json" && jq -e '[.subsets[]?.addresses[]?]|length==3' "$logs/client-endpoints.json" >/dev/null; do
  attempt=$((attempt+1)); [ "$attempt" -lt 60 ] || exit 1; sleep 1
done
k -n "$namespace" get service bootstrap-client -o json > "$logs/test-owned-client-service.json"
jq -e --slurpfile pods "$logs/ready-pods.json" '([.subsets[]?.addresses[]?.targetRef.uid]|sort)==([$pods[0].items[].metadata.uid]|sort)' "$logs/client-endpoints.json" >/dev/null
jq -e '.metadata.ownerReferences==null and .metadata.labels["rhiza-ci-owner"]=="harness" and .spec.type=="ClusterIP" and .spec.clusterIP!="None"' "$logs/test-owned-client-service.json" >/dev/null
# Separate in-cluster clients resolve Service DNS and use the ClusterIP dataplane.
# No automatic POST retry: an uncertain mutation must not become a new execution.
run_client() {
  job=$1 route=$2 body=$3
  printf '%s\n' "$body" > "$private/request.body"
  k -n "$namespace" create secret generic "request-$job" --from-file="config=$private/curl.conf" --from-file="body=$private/request.body" || return $?
  client_created=$(jq -n --arg job "$job" --arg route "http://bootstrap-client.$namespace.svc.cluster.local:8080/$route" --arg ready "http://bootstrap-client.$namespace.svc.cluster.local:8080/readyz" '{apiVersion:"batch/v1",kind:"Job",metadata:{name:$job,labels:{"rhiza-ci-owner":"harness"}},spec:{backoffLimit:0,activeDeadlineSeconds:90,template:{spec:{restartPolicy:"Never",automountServiceAccountToken:false,securityContext:{runAsUser:65532,runAsGroup:65532,fsGroup:65532,runAsNonRoot:true},containers:[{name:"client",image:"curlimages/curl:8.12.1@sha256:94e9e444bcba979c2ea12e27ae39bee4cd10bc7041a472c4727a558e213744e6",command:["curl"],args:["--silent","--show-error","--fail-with-body","--write-out","\nRHIZA_HTTP_CODE=%{http_code}\n","--max-time","60","--noproxy","*","--config","/request/config","--header","Content-Type: application/json","--data-binary","@/request/body",$route],securityContext:{allowPrivilegeEscalation:false,readOnlyRootFilesystem:true,capabilities:{drop:["ALL"]}},volumeMounts:[{name:"request",mountPath:"/request",readOnly:true}]}],volumes:[{name:"request",secret:{secretName:("request-"+$job),defaultMode:288}}]}}}}
    | .spec.template.spec.initContainers=[(.spec.template.spec.containers[0] | .name="ready" | .args=["--silent","--show-error","--fail","--output","/dev/null","--write-out","RHIZA_READY_HTTP_CODE=%{http_code}\n","--connect-timeout","2","--max-time","2","--retry","15","--retry-delay","1","--retry-max-time","15","--retry-connrefused","--noproxy","*","--config","/request/config",$ready])]' | k -n "$namespace" create -f - -o json) || return $?
  client_job_uid=$(printf '%s\n' "$client_created" | jq -er '.metadata.uid | select(type=="string" and length>0)') || return 1
  printf '%s\n' "$client_created" | jq '{apiVersion,kind,metadata:{name:.metadata.name,namespace:.metadata.namespace,uid:.metadata.uid},spec:{backoffLimit:.spec.backoffLimit,activeDeadlineSeconds:.spec.activeDeadlineSeconds},status}' > "$logs/$job-job.json"
  if wait_client_job "$job" "$client_job_uid"; then client_wait_exit=0; else client_wait_exit=$?; fi
  printf '%s\n' "$client_wait_exit" > "$logs/$job-wait.exit"
  client_capture_exit=0
  capture_client_job "$job" "$client_job_uid" || client_capture_exit=$?
  [ "$client_wait_exit" -eq 0 ] || return "$client_wait_exit"
  [ "$client_capture_exit" -eq 0 ] || return "$client_capture_exit"
  client_raw="$logs/$job-client-current.log"
  [ "$(wc -c < "$client_raw")" -lt 65536 ] || return 1
  client_http_code=$(sed -n 's/^RHIZA_HTTP_CODE=\([0-9][0-9][0-9]\)$/\1/p' "$client_raw")
  case "$client_http_code" in 2[0-9][0-9]) ;; *) return 1 ;; esac
  sed '/^RHIZA_HTTP_CODE=[0-9][0-9][0-9]$/d' "$client_raw" > "$logs/$job.json"
  jq -e . "$logs/$job.json" >/dev/null
}

run_client create-sql execute '{"request_id":"bootstrap-create","sql":"CREATE TABLE bootstrap_ci (id INTEGER PRIMARY KEY, value TEXT)"}'
managed_initial_ack_oracle "$logs/create-sql.json" create_ack
run_client insert-sql execute '{"request_id":"bootstrap-insert","sql":"INSERT INTO bootstrap_ci VALUES (1, '\''real-kubelet'\'')"}'
managed_initial_ack_oracle "$logs/insert-sql.json" insert_ack
run_client query query '{"sql":"SELECT id, value FROM bootstrap_ci"}'
managed_bootstrap_check initial query_rows "$logs/query.json" '.rows==[[1,"real-kubelet"]]'
if [ "$fixture_case" != legacy ]; then
  producer_uid=$(jq -er '.status.bootstrapPlan.members|sort_by(.member.node_id)|.[0].podUID' "$logs/committed.json")
  producer_pod=$(jq -er --arg uid "$producer_uid" '.items[]|select(.metadata.uid==$uid)|.metadata.name' "$logs/ready-pods.json")
  managed_bootstrap_check initial restart_zero "$logs/ready-pods.json" 'all(.items[]; all(.status.containerStatuses[]; .restartCount==0))'
  k -n "$namespace" logs "$producer_pod" -c sql > "$logs/rust-mixed-prefault.log"
  if [ "$fixture_family" = internal ]; then
    [ "$(grep -Fxc 'RHIZA_ACTUAL_MATERIALIZATION_PROFILE=9' "$logs/rust-mixed-prefault.log")" = 1 ]
  fi
  managed_mixed_tuple_oracle "$logs/rust-mixed-prefault.log" initial "$producer_uid" "$logs/rust-mixed-prefault-tuple.json"
  [ "$(grep -c '^RUST_MIXED_ROW_ACK_SLOT=[1-9][0-9]*$' "$logs/rust-mixed-prefault.log")" = 1 ]
  run_client mixed-prefault query '{"sql":"SELECT tuple FROM rust_mixed_contract WHERE id=1","consistency":"linearizable"}'
  jq -e '.rows|length==1 and (.[0]|length)==1' "$logs/mixed-prefault.json" >/dev/null
  jq -S '.rows[0][0]|fromjson' "$logs/mixed-prefault.json" > "$logs/rust-mixed-prefault-row.json"
  cmp "$logs/rust-mixed-prefault-tuple.json" "$logs/rust-mixed-prefault-row.json"
  if [ "$fixture_family" = graph ]; then
    [ "$(grep -Fxc 'RHIZA_ACTUAL_MATERIALIZATION_PROFILE=10' "$logs/rust-mixed-prefault.log")" = 1 ]
    managed_graph_tuple_oracle "$logs/rust-mixed-prefault.log" initial "$producer_uid" "$logs/rust-graph-prefault-tuple.json"
    managed_graph_cabi_oracle "$logs/rust-mixed-prefault.log" "$producer_uid" "$logs/graph-cabi-prefault.json"
    run_client graph-prefault query '{"sql":"SELECT tuple FROM rust_graph_contract WHERE id=1","consistency":"linearizable"}'
    jq -S '.rows[0][0]|fromjson' "$logs/graph-prefault.json" > "$logs/rust-graph-prefault-row.json"
    cmp "$logs/rust-graph-prefault-tuple.json" "$logs/rust-graph-prefault-row.json"
  fi
fi
k -n "$namespace" get rhizacluster sql -o json > "$logs/after-sql.json"
jq -S '.status|{bootstrapPlan,genesis,plan:.applicationInit.plan}' "$logs/committed.json" > "$logs/immutable-before.json"
jq -S '.status|{bootstrapPlan,genesis,plan:.applicationInit.plan}' "$logs/after-sql.json" > "$logs/immutable-after.json"
cmp "$logs/immutable-before.json" "$logs/immutable-after.json"
# Graceful deletion exercises Deployment replacement, not proof that an isolated
# process or outstanding object-store request has physically terminated.
old_pod=$(jq -r '.items|sort_by(.metadata.name)|.[0].metadata.name' "$logs/ready-pods.json")
old_uid=$(jq -r --arg name "$old_pod" '.items[]|select(.metadata.name==$name)|.metadata.uid' "$logs/ready-pods.json")
if [ "$fixture_case" != legacy ]; then
  old_pod=$producer_pod old_uid=$producer_uid
  k -n "$namespace" get pod "$old_pod" -o json > "$logs/rust-mixed-producer-before.json"
  jq -e --arg uid "$producer_uid" '.metadata.uid==$uid and any(.status.containerStatuses[]; .name=="sql" and .ready==true and .restartCount==0)' "$logs/rust-mixed-producer-before.json" >/dev/null
fi
old_node=$(jq -er --arg uid "$old_uid" '.status.bootstrapPlan.members[]|select(.podUID==$uid)|.member.node_id' "$logs/committed.json")
managed_pod_fault "$old_pod" "$old_uid"
managed_preserve_first_pod_fault "$logs" "$old_pod" "$old_uid"
attempt=0
until k -n "$namespace" get rhizacluster sql -o json > "$logs/replacement-status.json" &&
  managed_completed_repair_oracle "$logs/replacement-status.json" "$old_uid" "$old_node"; do
  attempt=$((attempt+1)); [ "$attempt" -lt 180 ] || exit 1; sleep 1
done
k -n "$namespace" wait --for=condition=Ready pod -l "$selector" --timeout=180s
k -n "$namespace" get pods -l "$selector" -o json > "$logs/replacement-pods.json"
jq -e --arg old "$old_uid" '.items|length==3 and all(.[]; .metadata.uid!=$old and ([.status.initContainerStatuses[]?|select(.state.terminated.exitCode==0)]|length)==2 and all(.spec.volumes[]?; has("persistentVolumeClaim")|not))' "$logs/replacement-pods.json" >/dev/null
run_client query-after-replacement query '{"sql":"SELECT id, value FROM bootstrap_ci"}'
managed_bootstrap_check first_replacement query_rows "$logs/query-after-replacement.json" '.rows==[[1,"real-kubelet"]]'
jq -S '.status|{bootstrapPlan,genesis,plan:.applicationInit.plan}' "$logs/replacement-status.json" > "$logs/immutable-replacement.json"
cmp "$logs/immutable-before.json" "$logs/immutable-replacement.json"
replacement_uid=$(jq -er '.status.repair.plan.replacement.podUID' "$logs/replacement-status.json")
replacement_pod=$(jq -er --arg uid "$replacement_uid" '.items[]|select(.metadata.uid==$uid)|.metadata.name' "$logs/replacement-pods.json")
jq -e --slurpfile initial "$logs/committed.json" '.status.repair.plan as $p | ([$initial[0].status.bootstrapPlan.members[].member.node_id]|index($p.replacement.nodeID))==null and ($p.finalVoterIDs|sort)==(([$initial[0].status.bootstrapPlan.members[].member.node_id]|map(select(.!=$p.oldNodeID)))+[$p.replacement.nodeID]|sort)' "$logs/replacement-status.json" >/dev/null
jq -e --arg uid "$replacement_uid" '[.items[]|select(.metadata.uid==$uid)]|length==1 and any(.[0].status.containerStatuses[]; .name=="sql" and .ready==true and .restartCount==0)' "$logs/replacement-pods.json" >/dev/null || { printf '%s\n' 'MANAGED_BOOTSTRAP_ORACLE_FAILURE phase=first_replacement check=restart_zero artifact=replacement-pods.json' >&2; return 1; }
managed_replacement_ffi_contract "$replacement_pod" "$replacement_uid" first-replacement

# Replacing a second fresh Pod proves sequential voter repair, not only a
# process restart of the first replacement.
first_replacement_uid=$replacement_uid
first_replacement_pod=$replacement_pod
first_replacement_node=$(jq -er '.status.repair.plan.replacement.nodeID' "$logs/replacement-status.json")
first_operation=$(jq -er '.status.repair.plan.operationID' "$logs/replacement-status.json")
managed_pod_fault "$first_replacement_pod" "$first_replacement_uid"
attempt=0
until k -n "$namespace" get rhizacluster sql -o json > "$logs/replacement-second-status.json" &&
  managed_completed_repair_oracle "$logs/replacement-second-status.json" "$first_replacement_uid" "$first_replacement_node" "$first_operation"; do
  attempt=$((attempt+1)); [ "$attempt" -lt 180 ] || exit 1; sleep 1
done
k -n "$namespace" wait --for=condition=Ready pod -l "$selector" --timeout=180s
k -n "$namespace" get pods -l "$selector" -o json > "$logs/replacement-second-pods.json"
jq -e --arg old "$first_replacement_uid" '.items|length==3 and all(.[]; .metadata.uid!=$old and ([.status.initContainerStatuses[]?|select(.state.terminated.exitCode==0)]|length)==2 and all(.spec.volumes[]?; has("persistentVolumeClaim")|not))' "$logs/replacement-second-pods.json" >/dev/null
second_replacement_uid=$(jq -er '.status.repair.plan.replacement.podUID' "$logs/replacement-second-status.json")
[ "$second_replacement_uid" != "$old_uid" ]
jq -e --arg original_old "$old_node" --slurpfile previous "$logs/replacement-status.json" '.status.repair.plan as $p | ($p.finalVoterIDs|index($original_old))==null and ($p.finalVoterIDs|sort)==(($previous[0].status.repair.plan.finalVoterIDs|map(select(.!=$p.oldNodeID)))+[$p.replacement.nodeID]|sort)' "$logs/replacement-second-status.json" >/dev/null
second_replacement_pod=$(jq -er --arg uid "$second_replacement_uid" '.items[]|select(.metadata.uid==$uid)|.metadata.name' "$logs/replacement-second-pods.json")
jq -e --arg uid "$second_replacement_uid" '[.items[]|select(.metadata.uid==$uid)]|length==1 and any(.[0].status.containerStatuses[]; .name=="sql" and .ready==true and .restartCount==0)' "$logs/replacement-second-pods.json" >/dev/null || { printf '%s\n' 'MANAGED_BOOTSTRAP_ORACLE_FAILURE phase=second_replacement check=restart_zero artifact=replacement-second-pods.json' >&2; return 1; }
managed_replacement_ffi_contract "$second_replacement_pod" "$second_replacement_uid" second-replacement
run_client query-after-second-replacement query '{"sql":"SELECT id, value FROM bootstrap_ci"}'
managed_bootstrap_check second_replacement query_rows "$logs/query-after-second-replacement.json" '.rows==[[1,"real-kubelet"]]'
jq -S '.status|{bootstrapPlan,genesis,plan:.applicationInit.plan}' "$logs/replacement-second-status.json" > "$logs/immutable-second-replacement.json"
cmp "$logs/immutable-before.json" "$logs/immutable-second-replacement.json"
jq -S '.status.repair' "$logs/replacement-second-status.json" > "$logs/repair-proof-before-restart.json"

# Keep the main-container restart as a separate, same-Pod recovery assertion.
replacement_uid=$second_replacement_uid
replacement_pod=$second_replacement_pod
restart_before=$(jq -r --arg uid "$replacement_uid" '.items[]|select(.metadata.uid==$uid)|.status.containerStatuses[]|select(.name=="sql")|.restartCount' "$logs/replacement-second-pods.json")
if [ "${RHIZA_MANAGED_TEST_DOCKERFILE-}" = Dockerfile.managed-ffi-contract ]; then
  # A failed test must not become a pass through kubelet retries/new request IDs.
  [ "$restart_before" = 0 ] || { echo 'FFI contract initial main restarted; retain first failure' >&2; return 1; }
fi
k -n "$namespace" exec "$replacement_pod" -c sql -- sha256sum /var/lib/rhiza/repair.json /var/lib/rhiza/data/prepared-adoption.json /var/lib/rhiza/data/qlog/voter-identity.json > "$logs/restart-identity-before.txt"
# The application handles TERM; kubelet restarts main without replacing the
# Pod or rerunning its completed initContainers and keeps the same emptyDir.
managed_container_fault "$replacement_pod" "$replacement_uid"
attempt=0
until k -n "$namespace" get pod "$replacement_pod" -o json > "$logs/restarted-pod.json" &&
  jq -e --arg uid "$replacement_uid" --argjson before "$restart_before" '.metadata.uid==$uid and any(.status.containerStatuses[]; .name=="sql" and .restartCount>$before and .ready==true) and ([.status.initContainerStatuses[]|select(.state.terminated.exitCode==0)]|length)==2' "$logs/restarted-pod.json" >/dev/null; do
  attempt=$((attempt+1)); [ "$attempt" -lt 120 ] || exit 1; sleep 1
done
if [ "${RHIZA_MANAGED_TEST_DOCKERFILE-}" = Dockerfile.managed-ffi-contract ]; then
  jq -e --argjson before "$restart_before" 'any(.status.containerStatuses[]; .name=="sql" and .restartCount==($before+1))' "$logs/restarted-pod.json" >/dev/null
  k -n "$namespace" logs "$replacement_pod" -c sql > "$logs/go-ffi-contract-restart.log"
  if [ "$fixture_family" = internal ]; then
    [ "$(grep -Fxc 'RHIZA_ACTUAL_MATERIALIZATION_PROFILE=9' "$logs/go-ffi-contract-restart.log")" = 1 ]
  fi
  if [ "$fixture_case" != legacy ]; then
    if [ "$fixture_family" = graph ]; then
      [ "$(grep -Fxc 'RHIZA_ACTUAL_MATERIALIZATION_PROFILE=10' "$logs/go-ffi-contract-restart.log")" = 1 ]
      managed_graph_tuple_oracle "$logs/go-ffi-contract-restart.log" verify "$replacement_uid" "$logs/rust-graph-restart-tuple.json"
      managed_graph_cabi_oracle "$logs/go-ffi-contract-restart.log" "$replacement_uid" "$logs/graph-cabi-restart.json"
      cmp "$logs/rust-graph-prefault-tuple.json" "$logs/rust-graph-restart-tuple.json"
    else
      grep -F 'actual in-cluster Go FFI -> mixed managed Host -> SQL KV ACK readback -> close passed' "$logs/go-ffi-contract-restart.log" >/dev/null
    fi
    managed_mixed_tuple_oracle "$logs/go-ffi-contract-restart.log" verify "$replacement_uid" "$logs/rust-mixed-restart-tuple.json"
    cmp "$logs/rust-mixed-prefault-tuple.json" "$logs/rust-mixed-restart-tuple.json"
    run_client mixed-restart query '{"sql":"SELECT tuple FROM rust_mixed_contract WHERE id=1","consistency":"linearizable"}'
    jq -S '.rows[0][0]|fromjson' "$logs/mixed-restart.json" > "$logs/rust-mixed-restart-row.json"
    cmp "$logs/rust-mixed-prefault-tuple.json" "$logs/rust-mixed-restart-row.json"
  else
    grep -F 'actual in-cluster Go FFI -> managed Host -> SQL ACK readback -> close passed' "$logs/go-ffi-contract-restart.log" >/dev/null
    grep -Fx 'PASS: managed Rust FFI SQL contract' "$logs/go-ffi-contract-restart.log" >/dev/null
  fi
fi
k -n "$namespace" exec "$replacement_pod" -c sql -- sha256sum /var/lib/rhiza/repair.json /var/lib/rhiza/data/prepared-adoption.json /var/lib/rhiza/data/qlog/voter-identity.json > "$logs/restart-identity-after.txt"
managed_same_pod_identity_oracle "$logs/restart-identity-before.txt" "$logs/restart-identity-after.txt"
run_client query-after-restart query '{"sql":"SELECT id, value FROM bootstrap_ci"}'
managed_bootstrap_check same_pod_restart query_rows "$logs/query-after-restart.json" '.rows==[[1,"real-kubelet"]]'
k -n "$namespace" get rhizacluster sql -o json > "$logs/restart-final-status.json"
managed_completed_repair_oracle "$logs/restart-final-status.json" "$first_replacement_uid" "$first_replacement_node" "$first_operation"
jq -S '.status.repair' "$logs/restart-final-status.json" > "$logs/repair-proof-after-restart.json"
cmp "$logs/repair-proof-before-restart.json" "$logs/repair-proof-after-restart.json"
printf '%s\n' 'PASS: actual kubelet bootstrap, two sequential Deployment Pod replacements, separate same-Pod main restart preserving origin/adoption/voter identity, NoPVC and ACK readback' > "$logs/result.txt"
if [ "${managed_chaos_verified-0}" = 1 ]; then
  printf '%s\n' 'PASS: scoped Chaos Mesh pod-kill and container-kill injection, runtime effects and one-shot deletion verified' >> "$logs/result.txt"
else
  printf '%s\n' 'GAP: Chaos Mesh qualification remains pending' >> "$logs/result.txt"
fi
printf '%s\n' 'GAP: later-operation rollover main restart, isolated-process physical fencing and operator-managed Service ownership remain pending; no NetworkPolicy claim' >> "$logs/result.txt"
}
