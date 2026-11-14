#!/bin/sh
# Two one-shot kill callbacks. The driver owns chaos_k, scope and cleanup.
chaos_record_oracle() {
  jq -e --arg id "$2" '
    any(.status.conditions[]?; .type=="AllInjected" and .status=="True") and
    (.status.experiment.containerRecords|length)==1 and
    all(.status.experiment.containerRecords[];
      .id==$id and .phase=="Injected" and
      (.injectedCount|type)=="number" and (.recoveredCount|type)=="number" and
      .injectedCount>0 and .recoveredCount>=0 and .injectedCount>.recoveredCount)
  ' "$1" >/dev/null
}
capture_chaos_failure() {
  case "$1" in pod-kill|container-kill) ;; *) return 2 ;; esac
  case "$2" in AllInjected|cleanup) ;; *) return 2 ;; esac
  chaos_scope_guard || return 1
  failure_json="$private/chaos-failure.json"
  if chaos_k get "podchaos/$1" -o json > "$failure_json" 2>/dev/null; then
    if jq -e --arg ns "$namespace" --arg run "$release" '.metadata.namespace==$ns and .metadata.labels["rhiza-ci-run"]==$run' "$failure_json" >/dev/null; then
      jq '{apiVersion,kind,metadata:{name:.metadata.name,namespace:.metadata.namespace,uid:.metadata.uid,generation:.metadata.generation},spec,status}' "$failure_json" | head -c 65536 > "$logs/$1-$2-failure.json"
      failure_uid=$(jq -r '.metadata.uid' "$failure_json")
      chaos_k get events --field-selector "involvedObject.kind=PodChaos,involvedObject.uid=$failure_uid" -o json 2>/dev/null |
        jq '{items:[.items[]? | {reason,message,count,firstTimestamp,lastTimestamp,involvedObject}] | .[-20:]}' |
        head -c 65536 > "$logs/$1-$2-failure-events.json" || :
    fi
  fi
  rm -f "$failure_json"
}
wait_chaos_injected() {
  if chaos_k wait --for=condition=AllInjected "podchaos/$1" --timeout=90s; then
    return 0
  else
    wait_failure_exit=$?
    printf '%s\n' "$wait_failure_exit" > "$logs/$1-AllInjected-wait.exit"
    capture_chaos_failure "$1" AllInjected || :
    return "$wait_failure_exit"
  fi
}
managed_chaos_request() {
  case "$1" in pod-kill|container-kill) ;; *) return 2 ;; esac
  [ -n "$namespace" ] && [ -n "$2" ] && [ -n "$release" ] || return 2
  jq -n --arg name "$1" --arg ns "$namespace" --arg run "$release" --arg action "$1" --arg pod "$2" \
    '{apiVersion:"chaos-mesh.org/v1alpha1",kind:"PodChaos",metadata:{name:$name,namespace:$ns,labels:{"rhiza-ci-run":$run}},spec:{action:$action,mode:"one",duration:"10s",containerNames:["sql"],selector:{namespaces:[$ns],pods:{($ns):[$pod]}}}}'
}
direct_chaos_fault() {
  action=$1 fault_name=$2 pod=$3 pod_uid=$4
  [ "$action" = "$fault_name" ] || return 2
  chaos_scope_guard || return $?
  chaos_k get pod "$pod" -o json > "$logs/$fault_name-pod-before.json" || return $?
  jq -e --arg pod_uid "$pod_uid" --arg cluster_uid "$uid" '.metadata.uid==$pod_uid and .metadata.deletionTimestamp==null and .metadata.labels["rhiza.mrchypark.dev/cluster-uid"]==$cluster_uid and any(.status.containerStatuses[]?; .name=="sql" and .ready)' "$logs/$fault_name-pod-before.json" >/dev/null || return $?
  old_container=$(jq -er '.status.containerStatuses[]|select(.name=="sql")|.containerID|select(length>0)' "$logs/$fault_name-pod-before.json") || return $?
  expected_record="$namespace/$pod"
  [ "$action" != container-kill ] || expected_record="$expected_record/sql"
  managed_chaos_request "$action" "$pod" > "$logs/$fault_name-request.json" || return $?
  chaos_k create -f "$logs/$fault_name-request.json" || return $?
  wait_chaos_injected "$fault_name" || return $?
  chaos_k get "podchaos/$fault_name" -o json > "$logs/$fault_name-injected.json" || return $?
  chaos_record_oracle "$logs/$fault_name-injected.json" "$expected_record" || return $?
  if [ "$action" = container-kill ]; then
    tries=0
    until chaos_k get pod "$pod" -o json > "$logs/$fault_name-pod-after.json" &&
      jq -e --arg pod_uid "$pod_uid" --arg old "$old_container" '.metadata.uid==$pod_uid and any(.status.containerStatuses[]; .name=="sql" and .containerID!=$old and .lastState.terminated.containerID==$old)' "$logs/$fault_name-pod-after.json" >/dev/null; do
      tries=$((tries+1)); [ "$tries" -lt 60 ] || return 1; sleep 1
    done
  else
    chaos_k get pods -o json > "$logs/$fault_name-pods-after.json" || return $?
    jq -e --arg old_uid "$pod_uid" 'all(.items[]; .metadata.uid!=$old_uid)' "$logs/$fault_name-pods-after.json" >/dev/null || return $?
  fi
  chaos_k get "podchaos/$fault_name" -o json > "$logs/$fault_name-before-delete.json" || return $?
  chaos_k delete "podchaos/$fault_name" --wait=true --timeout=45s || return $?
  chaos_k get "podchaos/$fault_name" --ignore-not-found -o json > "$logs/$fault_name-after-delete.json" || return $?
  [ ! -s "$logs/$fault_name-after-delete.json" ] || return 1
  fault_name=
}
