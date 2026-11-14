#!/bin/sh
# Source from test-ha-managed-bootstrap.sh after its external-scale setup.
# Requires k, namespace, logs, selector and the managed_functional_* helpers.

managed_autoscale_install() {
  command -v helm >/dev/null
  helm template qualification-metrics metrics-server --repo https://kubernetes-sigs.github.io/metrics-server/ --version 3.12.2 --namespace "$namespace" --set 'args[0]=--kubelet-insecure-tls' > "$logs/autoscale-metrics-manifest.yaml" || return 1
  k apply -f "$logs/autoscale-metrics-manifest.yaml" > "$logs/autoscale-metrics-install.txt" || return 1
  k -n "$namespace" rollout status deployment/qualification-metrics-metrics-server --timeout=180s || return 1
  k wait --for=condition=Available apiservice/v1beta1.metrics.k8s.io --timeout=180s || return 1
  helm template qualification-keda keda --repo https://kedacore.github.io/charts --version 2.17.2 --include-crds --namespace "$namespace" > "$logs/autoscale-keda-manifest.yaml" || return 1
  k apply --server-side -f "$logs/autoscale-keda-manifest.yaml" > "$logs/autoscale-keda-install.txt" || return 1
  k wait --for=condition=Established crd/scaledobjects.keda.sh --timeout=180s || return 1
  for mas_deployment in keda-operator keda-operator-metrics-apiserver keda-admission-webhooks; do
    k -n "$namespace" rollout status "deployment/$mas_deployment" --timeout=180s || return 1
  done
  k wait --for=condition=Available apiservice/v1beta1.external.metrics.k8s.io --timeout=180s
}

managed_autoscale_delete() {
  mas_kind=$1 mas_name=$2 mas_uid=$3 mas_api=$4
  [ -n "$mas_uid" ] || return 0
  jq -n --arg uid "$mas_uid" '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid}}' > "$logs/autoscale-delete.json" || return 1
  k delete --raw "/apis/$mas_api/namespaces/$namespace/$mas_kind/$mas_name" -f "$logs/autoscale-delete.json" > "$logs/autoscale-delete-$mas_name.json" || return 1
  mas_group=${mas_api%/*}
  mas_delete_deadline=$(( $(date +%s) + 90 ))
  while :; do
    k -n "$namespace" get "$mas_kind.$mas_group" "$mas_name" --ignore-not-found -o json > "$logs/autoscale-deleted-$mas_name.json" || return 1
    [ -s "$logs/autoscale-deleted-$mas_name.json" ] || break
    jq -e --arg uid "$mas_uid" '.metadata.uid==$uid' "$logs/autoscale-deleted-$mas_name.json" >/dev/null || { echo "replacement resource observed during cleanup: $mas_name" >&2; return 1; }
    [ "$(date +%s)" -lt "$mas_delete_deadline" ] || { echo "resource not deleted: $mas_name" >&2; return 1; }
    sleep 2
  done
}

managed_autoscale_wait() {
  mas_target=$1 mas_tag=$2 mas_kind=$3
  mas_deadline=$(( $(date +%s) + 600 ))
  while :; do
    k -n "$namespace" get rhizacluster sql -o json > "$logs/autoscale-$mas_tag-cr.json" || return 1
    if jq -e --argjson target "$mas_target" '.spec.replicas==$target' "$logs/autoscale-$mas_tag-cr.json" >/dev/null; then break; fi
    [ "$(date +%s)" -lt "$mas_deadline" ] || { echo "$mas_kind did not propose $mas_target" >&2; return 1; }
    sleep 2
  done
  managed_functional_wait_external_scale "$mas_target" "$mas_tag" "$mas_kind" "$mas_operator_pod_uid" "$mas_operator_deployment_uid" "$mas_operator_containers"
}

managed_autoscale_stop_load() {
  mas_stop_rc=0
  for mas_pod in ${mas_load_pods-}; do
    k -n "$namespace" exec "$mas_pod" -c sql -- touch /tmp/rhiza-autoscale-stop > /dev/null 2>&1 || mas_stop_rc=1
  done
  for mas_pid in ${mas_load_pids-}; do
    mas_stop_deadline=$(( $(date +%s) + 15 ))
    while kill -0 "$mas_pid" 2>/dev/null; do
      if [ "$(date +%s)" -ge "$mas_stop_deadline" ]; then kill "$mas_pid" 2>/dev/null || :; mas_stop_rc=1; break; fi
      sleep 1
    done
    wait "$mas_pid" 2>/dev/null || mas_stop_rc=1
  done
  mas_load_pids= mas_load_pods=
  return "$mas_stop_rc"
}

managed_autoscale_cleanup() {
  mas_result=$1
  trap - EXIT HUP INT TERM
  managed_autoscale_stop_load || mas_result=1
  managed_autoscale_delete horizontalpodautoscalers rhiza-qualification-hpa "$mas_hpa_uid" autoscaling/v2 || mas_result=1
  managed_autoscale_delete scaledobjects rhiza-qualification-keda "$mas_keda_uid" keda.sh/v1alpha1 || mas_result=1
  managed_autoscale_generated_gone || mas_result=1
  exit "$mas_result"
}

managed_autoscale_generated_gone() {
  [ -n "${mas_generated_uid-}" ] || return 0
  mas_generated_deadline=$(( $(date +%s) + 90 ))
  while :; do
    k -n "$namespace" get hpa keda-hpa-rhiza-qualification-keda --ignore-not-found -o json > "$logs/autoscale-generated-hpa-cleanup.json" || return 1
    [ -s "$logs/autoscale-generated-hpa-cleanup.json" ] || return 0
    jq -e --arg uid "$mas_generated_uid" '.metadata.uid==$uid' "$logs/autoscale-generated-hpa-cleanup.json" >/dev/null || return 1
    [ "$(date +%s)" -lt "$mas_generated_deadline" ] || return 1
    sleep 2
  done
}

# Run only against the harness-created disposable namespace, never a shared CR.
managed_embedded_autoscale_qualification() (
  set -eu
  : "${namespace:?}" "${logs:?}" "${selector:?}"
  [ "${RHIZA_AUTOSCALE_DISPOSABLE_NAMESPACE:-}" = "$namespace" ] || { echo 'Explicit disposable namespace confirmation required' >&2; exit 1; }
  command -v jq >/dev/null
  k -n "$namespace" get rhizacluster sql -o json > "$logs/autoscale-initial-cr.json"
  jq -e '.spec.replicas==3 and (.status.appliedVoterCount // .spec.voterCount)==3' "$logs/autoscale-initial-cr.json" >/dev/null
  # Do not create competing controllers in the test namespace.
  k -n "$namespace" get hpa -o json | jq -e '[.items[] | select(.spec.scaleTargetRef.kind=="RhizaCluster" and .spec.scaleTargetRef.name=="sql")]|length==0' >/dev/null
  k -n "$namespace" get scaledobjects.keda.sh -o json | jq -e '[.items[] | select(.spec.scaleTargetRef.kind=="RhizaCluster" and .spec.scaleTargetRef.name=="sql")]|length==0' >/dev/null
  managed_functional_external_observe 3 autoscale-baseline bootstrap
  for mas_pod in $(k -n "$namespace" get pods -l "$selector" -o json | jq -r '.items[].metadata.name'); do
    k -n "$namespace" exec "$mas_pod" -c sql -- wget -q -O - http://127.0.0.1:8081/query > "$logs/autoscale-embedded-$mas_pod.json"
    jq -e '.rows==[[1]]' "$logs/autoscale-embedded-$mas_pod.json" >/dev/null
  done
  k -n "$namespace" get deployment rhiza-operator -o json > "$logs/functional-operator-autoscale-baseline-deployment.json"
  k -n "$namespace" get pods -l app.kubernetes.io/name=rhiza-operator -o json > "$logs/functional-operator-autoscale-baseline-pods.json"
  managed_functional_operator_running autoscale-baseline
  mas_operator_pod_uid=$(jq -er '.items[0].metadata.uid' "$logs/functional-operator-autoscale-baseline-pods.json")
  mas_operator_deployment_uid=$(jq -er '.metadata.uid' "$logs/functional-operator-autoscale-baseline-deployment.json")
  mas_operator_containers=$(managed_functional_operator_container_identity "$logs/functional-operator-autoscale-baseline-pods.json")
  k -n "$namespace" get pods -l "$selector" -o json > "$logs/autoscale-cpu-prerequisites.json"
  jq -e '(.items|length)==3 and all(.items[]; any(.spec.containers[]; .name=="sql" and (.resources.requests.cpu // "")!=""))' "$logs/autoscale-cpu-prerequisites.json" >/dev/null || { echo 'CPU requests are required for HPA utilization' >&2; exit 1; }
  k get --raw /apis/metrics.k8s.io/v1beta1 > "$logs/autoscale-metrics-api.json"
  mas_hpa_uid= mas_keda_uid= mas_generated_uid= mas_load_pids= mas_load_pods=
  trap 'managed_autoscale_cleanup $?' EXIT
  trap 'exit 1' HUP INT TERM
  jq -n '{apiVersion:"autoscaling/v2",kind:"HorizontalPodAutoscaler",metadata:{name:"rhiza-qualification-hpa"},spec:{scaleTargetRef:{apiVersion:"rhiza.mrchypark.dev/v1alpha2",kind:"RhizaCluster",name:"sql"},minReplicas:3,maxReplicas:5,behavior:{scaleDown:{stabilizationWindowSeconds:0}},metrics:[{type:"Resource",resource:{name:"cpu",target:{type:"Utilization",averageUtilization:70}}}]}}' | k -n "$namespace" create -f - -o json > "$logs/autoscale-hpa-created.json"
  mas_hpa_uid=$(jq -er '.metadata.uid' "$logs/autoscale-hpa-created.json")
  mas_deadline=$(( $(date +%s) + 300 ))
  while :; do
    k -n "$namespace" get hpa rhiza-qualification-hpa -o json > "$logs/autoscale-hpa-idle.json"
    if jq -e '.status.desiredReplicas==3 and any(.status.currentMetrics[]?; .type=="Resource" and .resource.name=="cpu" and .resource.current.averageUtilization<70)' "$logs/autoscale-hpa-idle.json" >/dev/null; then break; fi
    [ "$(date +%s)" -lt "$mas_deadline" ] || { echo 'HPA did not observe idle CPU metrics' >&2; exit 1; }
    sleep 5
  done
  mas_load_pods=$(jq -r '.items[].metadata.name' "$logs/autoscale-cpu-prerequisites.json")
  for mas_pod in $mas_load_pods; do
    k -n "$namespace" exec "$mas_pod" -c sql -- sh -c 'while [ ! -e /tmp/rhiza-autoscale-stop ]; do :; done' > "$logs/autoscale-load-$mas_pod.log" 2>&1 &
    mas_load_pids="$mas_load_pids $!"
  done
  managed_autoscale_wait 5 hpa-up add
  k -n "$namespace" get hpa rhiza-qualification-hpa -o json > "$logs/autoscale-hpa-up.json"
  jq -e '.status.desiredReplicas==5 and any(.status.conditions[]?; .type=="ScalingActive" and .status=="True") and any(.status.currentMetrics[]?; .type=="Resource" and .resource.name=="cpu" and .resource.current.averageUtilization>70)' "$logs/autoscale-hpa-up.json" >/dev/null
  managed_functional_commit autoscale-hpa-write autoscale-hpa "INSERT INTO bootstrap_ci VALUES (8101,'autoscale-hpa')" autoscale-hpa
  managed_autoscale_stop_load
  managed_autoscale_wait 3 hpa-down leave
  managed_autoscale_delete horizontalpodautoscalers rhiza-qualification-hpa "$mas_hpa_uid" autoscaling/v2
  mas_hpa_uid=
  # Place the current minute within a long active window, including across UTC midnight.
  mas_minute=$(date -u +%M | sed 's/^0//')
  [ -n "$mas_minute" ] || mas_minute=0
  mas_start="$(( (mas_minute + 59) % 60 )) * * * *"
  mas_end="$(( (mas_minute + 50) % 60 )) * * * *"
  jq -n --arg start "$mas_start" --arg end "$mas_end" '{apiVersion:"keda.sh/v1alpha1",kind:"ScaledObject",metadata:{name:"rhiza-qualification-keda"},spec:{scaleTargetRef:{apiVersion:"rhiza.mrchypark.dev/v1alpha2",kind:"RhizaCluster",name:"sql"},pollingInterval:5,minReplicaCount:3,maxReplicaCount:5,advanced:{horizontalPodAutoscalerConfig:{behavior:{scaleDown:{stabilizationWindowSeconds:0}}}},triggers:[{type:"cron",metadata:{timezone:"Etc/UTC",start:$start,end:$end,desiredReplicas:"3"}}]}}' | k -n "$namespace" create -f - -o json > "$logs/autoscale-keda-created.json"
  mas_keda_uid=$(jq -er '.metadata.uid' "$logs/autoscale-keda-created.json")
  mas_deadline=$(( $(date +%s) + 300 ))
  while :; do
    if k -n "$namespace" get hpa keda-hpa-rhiza-qualification-keda -o json > "$logs/autoscale-keda-hpa-idle.json" 2>/dev/null && jq -e '.status.desiredReplicas==3 and any(.status.currentMetrics[]?; .type=="External" and ((.external.current.value // .external.current.averageValue) != null))' "$logs/autoscale-keda-hpa-idle.json" >/dev/null; then break; fi
    [ "$(date +%s)" -lt "$mas_deadline" ] || { echo 'KEDA HPA did not observe its external metric' >&2; exit 1; }
    sleep 5
  done
  mas_generated_uid=$(jq -er --arg uid "$mas_keda_uid" 'select(any(.metadata.ownerReferences[]?; .uid==$uid and .kind=="ScaledObject")) | .metadata.uid' "$logs/autoscale-keda-hpa-idle.json")
  k -n "$namespace" patch scaledobject rhiza-qualification-keda --type json -p '[{"op":"replace","path":"/spec/triggers/0/metadata/desiredReplicas","value":"5"}]' > "$logs/autoscale-keda-up-patch.json"
  managed_autoscale_wait 5 keda-up add
  k -n "$namespace" get scaledobject rhiza-qualification-keda -o json > "$logs/autoscale-keda-up.json"
  jq -e 'any(.status.conditions[]?; .type=="Ready" and .status=="True")' "$logs/autoscale-keda-up.json" >/dev/null
  k -n "$namespace" get hpa keda-hpa-rhiza-qualification-keda -o json > "$logs/autoscale-keda-hpa-up.json"
  jq -e '.status.desiredReplicas==5 and any(.status.currentMetrics[]?; .type=="External" and ((.external.current.value // .external.current.averageValue) != null))' "$logs/autoscale-keda-hpa-up.json" >/dev/null
  mas_idle_metric=$(jq -cer '[.status.currentMetrics[] | select(.type=="External") | .external.current]' "$logs/autoscale-keda-hpa-idle.json")
  mas_up_metric=$(jq -cer '[.status.currentMetrics[] | select(.type=="External") | .external.current]' "$logs/autoscale-keda-hpa-up.json")
  [ "$mas_idle_metric" != "$mas_up_metric" ] || { echo 'KEDA external metric did not change' >&2; exit 1; }
  managed_functional_commit autoscale-keda-write autoscale-keda "INSERT INTO bootstrap_ci VALUES (8102,'autoscale-keda')" autoscale-keda
  k -n "$namespace" patch scaledobject rhiza-qualification-keda --type json -p '[{"op":"replace","path":"/spec/triggers/0/metadata/desiredReplicas","value":"3"}]' > "$logs/autoscale-keda-down-patch.json"
  managed_autoscale_wait 3 keda-down leave
  run_client autoscale-readback query '{"sql":"SELECT id,value FROM bootstrap_ci WHERE id IN (8101,8102) ORDER BY id","consistency":"linearizable"}'
  jq -e '.rows==[[8101,"autoscale-hpa"],[8102,"autoscale-keda"]]' "$logs/autoscale-readback.json" >/dev/null
  managed_autoscale_delete scaledobjects rhiza-qualification-keda "$mas_keda_uid" keda.sh/v1alpha1
  mas_keda_uid=
  managed_autoscale_generated_gone
  trap - EXIT HUP INT TERM
  printf '%s\n' 'PASS: HPA and KEDA each drove 3->5->3 with certified roster and readback' > "$logs/autoscale-result.txt"
)
