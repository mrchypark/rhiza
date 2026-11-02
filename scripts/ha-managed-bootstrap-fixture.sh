#!/bin/sh
# Shared actual managed bootstrap/replacement checks. Drivers own API, image and cleanup.
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
managed_fixture() {
fixture_case=${RHIZA_MANAGED_FIXTURE_CASE:-legacy}
case "$fixture_case" in
  legacy) fixture_protocol='' fixture_evidence=rhiza-managed-sql/v1 ;;
  mixed) fixture_protocol=rhiza-managed-sql-kv/v1 fixture_evidence=rhiza-managed-sql-kv/v1 ;;
  *) echo 'invalid managed fixture case' >&2; return 1 ;;
esac
floci_node_selector='{}'
fixture_provider=${fixture_provider-s3}
case "$fixture_provider" in
  s3) floci_image=floci/floci:2.3.0; floci_port=4566 ;;
  gcs) floci_image=floci/floci-gcp:0.11.0; floci_port=4588 ;;
  azure) floci_image=floci/floci-az:0.15.0; floci_port=4577 ;;
  *) echo 'unsupported explicit emulator provider' >&2; return 1 ;;
esac
if [ "$fixture_case" = legacy ] && [ "$fixture_provider" != s3 ]; then
  echo 'legacy runtime qualification is S3-only' >&2; return 1
fi
printf 'case=%s\nprovider=%s\nemulator=%s\nport=%s\n' "$fixture_case" "$fixture_provider" "$floci_image" "$floci_port" > "$logs/fixture-provider.txt"
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
printf 'header = "Authorization: Bearer %s"\n' "$admin" > "$private/curl.conf"
unset admin
k -n "$namespace" create secret generic runtime --from-file="runtime.json=$private/runtime.json"
k -n "$namespace" create serviceaccount operator
k -n "$namespace" create serviceaccount rhiza-managed-host
k -n "$namespace" create role runtime-read --verb=get --resource=rhizaclusters,pods,replicasets,deployments
k -n "$namespace" create rolebinding runtime-read --role=runtime-read --serviceaccount="$namespace:rhiza-managed-host"
# Pod LIST is namespace-scoped discovery, not field/label-scoped RBAC.
k -n "$namespace" create role runtime-pod-discovery --verb=list --resource=pods
k -n "$namespace" create rolebinding runtime-pod-discovery --role=runtime-pod-discovery --serviceaccount="$namespace:rhiza-managed-host"
k -n "$namespace" create role runtime-status --verb=get,update --resource=rhizaclusters/status --resource-name=sql
k -n "$namespace" create rolebinding runtime-status --role=runtime-status --serviceaccount="$namespace:rhiza-managed-host"
k -n "$namespace" create role managed-host-account-read --verb=get --resource=serviceaccounts --resource-name=rhiza-managed-host
k -n "$namespace" create rolebinding managed-host-account-read --role=managed-host-account-read --serviceaccount="$namespace:operator"
k -n "$namespace" create role bootstrap --verb=get,list --resource=rhizaclusters,pods,replicasets,deployments,secrets
k -n "$namespace" create role create-deployment --verb=create --resource=deployments
k -n "$namespace" create rolebinding bootstrap --role=bootstrap --serviceaccount="$namespace:operator"
k -n "$namespace" create rolebinding create-deployment --role=create-deployment --serviceaccount="$namespace:operator"
# Withhold status-update RBAC until actual kubelet termination and gate observations.
jq -n --arg image "$image" --arg namespace "$namespace" '{apiVersion:"apps/v1",kind:"Deployment",metadata:{name:"operator"},spec:{replicas:1,selector:{matchLabels:{app:"operator"}},template:{metadata:{labels:{app:"operator"}},spec:{serviceAccountName:"operator",securityContext:{runAsUser:65532,runAsGroup:65532,fsGroup:65532,runAsNonRoot:true},containers:[{name:"operator",image:$image,args:[("--namespace="+$namespace),"--poll-interval=1s","--managed-runtime-profile=/etc/rhiza/managed/runtime.json"],volumeMounts:[{name:"runtime",mountPath:"/etc/rhiza/managed",readOnly:true}]}],volumes:[{name:"runtime",secret:{secretName:"runtime",defaultMode:288}}]}}}}' | k -n "$namespace" apply -f -
jq -n --arg image "$image" --arg protocol "$fixture_protocol" '{apiVersion:"rhiza.mrchypark.dev/v1alpha2",kind:"RhizaCluster",metadata:{name:"sql"},spec:{logicalID:"managed-ci",genesisIntent:"Create",voterCount:3,workload:{podTemplate:{spec:{containers:[{name:"sql",image:$image}]}}}}} | if $protocol=="" then . else .spec.applicationProtocol=$protocol end' | k -n "$namespace" apply -f -
uid=$(k -n "$namespace" get rhizacluster sql -o jsonpath='{.metadata.uid}')
selector="rhiza.mrchypark.dev/cluster-uid=$uid"
attempt=0
until k -n "$namespace" get pods -l "$selector" -o json > "$logs/gated-pods.json" && sh scripts/ha-managed-bucket-setup oracle-gated "$logs/gated-pods.json"; do
  attempt=$((attempt+1)); [ "$attempt" -lt 120 ] || exit 1; sleep 1
done
k -n "$namespace" create role status --verb=update --resource=rhizaclusters/status
k -n "$namespace" create rolebinding status --role=status --serviceaccount="$namespace:operator"
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
run_client insert-sql execute '{"request_id":"bootstrap-insert","sql":"INSERT INTO bootstrap_ci VALUES (1, '\''real-kubelet'\'')"}'
run_client query query '{"sql":"SELECT id, value FROM bootstrap_ci"}'
jq -e '.rows==[[1,"real-kubelet"]]' "$logs/query.json" >/dev/null
if [ "$fixture_case" = mixed ]; then
  producer_uid=$(jq -er '.status.bootstrapPlan.members|sort_by(.member.node_id)|.[0].podUID' "$logs/committed.json")
  producer_pod=$(jq -er --arg uid "$producer_uid" '.items[]|select(.metadata.uid==$uid)|.metadata.name' "$logs/ready-pods.json")
  jq -e 'all(.items[]; all(.status.containerStatuses[]; .restartCount==0))' "$logs/ready-pods.json" >/dev/null
  k -n "$namespace" logs "$producer_pod" -c sql > "$logs/rust-mixed-prefault.log"
  managed_mixed_tuple_oracle "$logs/rust-mixed-prefault.log" initial "$producer_uid" "$logs/rust-mixed-prefault-tuple.json"
  [ "$(grep -c '^RUST_MIXED_ROW_ACK_SLOT=[1-9][0-9]*$' "$logs/rust-mixed-prefault.log")" = 1 ]
  run_client mixed-prefault query '{"sql":"SELECT tuple FROM rust_mixed_contract WHERE id=1","consistency":"linearizable"}'
  jq -e '.rows|length==1 and (.[0]|length)==1' "$logs/mixed-prefault.json" >/dev/null
  jq -S '.rows[0][0]|fromjson' "$logs/mixed-prefault.json" > "$logs/rust-mixed-prefault-row.json"
  cmp "$logs/rust-mixed-prefault-tuple.json" "$logs/rust-mixed-prefault-row.json"
fi
k -n "$namespace" get rhizacluster sql -o json > "$logs/after-sql.json"
jq -S '.status|{bootstrapPlan,genesis,plan:.applicationInit.plan}' "$logs/committed.json" > "$logs/immutable-before.json"
jq -S '.status|{bootstrapPlan,genesis,plan:.applicationInit.plan}' "$logs/after-sql.json" > "$logs/immutable-after.json"
cmp "$logs/immutable-before.json" "$logs/immutable-after.json"
# Graceful deletion exercises Deployment replacement, not proof that an isolated
# process or outstanding object-store request has physically terminated.
old_pod=$(jq -r '.items|sort_by(.metadata.name)|.[0].metadata.name' "$logs/ready-pods.json")
old_uid=$(jq -r --arg name "$old_pod" '.items[]|select(.metadata.name==$name)|.metadata.uid' "$logs/ready-pods.json")
if [ "$fixture_case" = mixed ]; then
  old_pod=$producer_pod old_uid=$producer_uid
  k -n "$namespace" get pod "$old_pod" -o json > "$logs/rust-mixed-producer-before.json"
  jq -e --arg uid "$producer_uid" '.metadata.uid==$uid and any(.status.containerStatuses[]; .name=="sql" and .ready==true and .restartCount==0)' "$logs/rust-mixed-producer-before.json" >/dev/null
fi
managed_pod_fault "$old_pod" "$old_uid"
attempt=0
until k -n "$namespace" get rhizacluster sql -o json > "$logs/replacement-status.json" &&
  jq -e --arg old "$old_uid" '.status.repair.state=="Completed" and .status.repair.plan.replacement.podUID!=$old' "$logs/replacement-status.json" >/dev/null; do
  attempt=$((attempt+1)); [ "$attempt" -lt 180 ] || exit 1; sleep 1
done
k -n "$namespace" wait --for=condition=Ready pod -l "$selector" --timeout=180s
k -n "$namespace" get pods -l "$selector" -o json > "$logs/replacement-pods.json"
jq -e --arg old "$old_uid" '.items|length==3 and all(.[]; .metadata.uid!=$old and ([.status.initContainerStatuses[]?|select(.state.terminated.exitCode==0)]|length)==2 and all(.spec.volumes[]?; has("persistentVolumeClaim")|not))' "$logs/replacement-pods.json" >/dev/null
run_client query-after-replacement query '{"sql":"SELECT id, value FROM bootstrap_ci"}'
jq -e '.rows==[[1,"real-kubelet"]]' "$logs/query-after-replacement.json" >/dev/null
jq -S '.status|{bootstrapPlan,genesis,plan:.applicationInit.plan}' "$logs/replacement-status.json" > "$logs/immutable-replacement.json"
cmp "$logs/immutable-before.json" "$logs/immutable-replacement.json"
replacement_uid=$(jq -r '.status.repair.plan.replacement.podUID' "$logs/replacement-status.json")
replacement_pod=$(jq -r --arg uid "$replacement_uid" '.items[]|select(.metadata.uid==$uid)|.metadata.name' "$logs/replacement-pods.json")
[ -n "$replacement_pod" ]
restart_before=$(jq -r --arg uid "$replacement_uid" '.items[]|select(.metadata.uid==$uid)|.status.containerStatuses[]|select(.name=="sql")|.restartCount' "$logs/replacement-pods.json")
if [ "${RHIZA_MANAGED_TEST_DOCKERFILE-}" = Dockerfile.managed-ffi-contract ]; then
  # A failed test must not become a pass through kubelet retries/new request IDs.
  [ "$restart_before" = 0 ] || { echo 'FFI contract initial main restarted; retain first failure' >&2; return 1; }
  k -n "$namespace" logs "$replacement_pod" -c sql > "$logs/go-ffi-contract-first.log"
  if [ "$fixture_case" = mixed ]; then
    grep -F 'actual in-cluster Go FFI -> mixed managed Host -> SQL KV ACK readback -> close passed' "$logs/go-ffi-contract-first.log" >/dev/null
    managed_mixed_tuple_oracle "$logs/go-ffi-contract-first.log" verify "$replacement_uid" "$logs/rust-mixed-replacement-tuple.json"
    cmp "$logs/rust-mixed-prefault-tuple.json" "$logs/rust-mixed-replacement-tuple.json"
    run_client mixed-replacement query '{"sql":"SELECT tuple FROM rust_mixed_contract WHERE id=1","consistency":"linearizable"}'
    jq -S '.rows[0][0]|fromjson' "$logs/mixed-replacement.json" > "$logs/rust-mixed-replacement-row.json"
    cmp "$logs/rust-mixed-prefault-tuple.json" "$logs/rust-mixed-replacement-row.json"
  else
    grep -F 'actual in-cluster Go FFI -> managed Host -> SQL ACK readback -> close passed' "$logs/go-ffi-contract-first.log" >/dev/null
    grep -Fx 'PASS: managed Rust FFI SQL contract' "$logs/go-ffi-contract-first.log" >/dev/null
  fi
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
  if [ "$fixture_case" = mixed ]; then
    grep -F 'actual in-cluster Go FFI -> mixed managed Host -> SQL KV ACK readback -> close passed' "$logs/go-ffi-contract-restart.log" >/dev/null
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
jq -e '.rows==[[1,"real-kubelet"]]' "$logs/query-after-restart.json" >/dev/null
printf '%s\n' 'PASS: actual kubelet bootstrap, Deployment Pod replacement, same-Pod main restart preserving origin/adoption/voter identity, NoPVC and ACK readback' > "$logs/result.txt"
if [ "${managed_chaos_verified-0}" = 1 ]; then
  printf '%s\n' 'PASS: scoped Chaos Mesh pod-kill and container-kill injection, runtime effects and one-shot deletion verified' >> "$logs/result.txt"
else
  printf '%s\n' 'GAP: Chaos Mesh qualification remains pending' >> "$logs/result.txt"
fi
printf '%s\n' 'GAP: later-operation rollover main restart, isolated-process physical fencing and operator-managed Service ownership remain pending; no NetworkPolicy claim' >> "$logs/result.txt"
}
