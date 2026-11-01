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
managed_fixture() {
floci_node_selector='{}'
if [ -n "${RHIZA_MANAGED_FIXTURE_NODE-}" ]; then
  printf '%s\n' "$RHIZA_MANAGED_FIXTURE_NODE" | grep -Eq '^[a-z0-9][a-z0-9.-]{0,252}$' || {
    echo 'invalid optional Floci fixture node hostname' >&2; return 1;
  }
  floci_node_selector=$(jq -cn --arg node "$RHIZA_MANAGED_FIXTURE_NODE" '{"kubernetes.io/hostname":$node}')
fi
# Create the Service first so both variants see the same existing Service set.
jq -n '{apiVersion:"v1",kind:"Service",metadata:{name:"floci"},spec:{selector:{app:"floci"},ports:[{port:4566,targetPort:4566}]}}' | k -n "$namespace" create -f -
# Bounded diagnostic only: retain actual failure logs, never infer the Spring cause.
jq -n --argjson nodeSelector "$floci_node_selector" '{apiVersion:"v1",kind:"Pod",metadata:{name:"floci-service-links-probe",labels:{"rhiza-ci-component":"floci","rhiza-ci-probe":"service-links"}},spec:{nodeSelector:$nodeSelector,activeDeadlineSeconds:45,enableServiceLinks:true,automountServiceAccountToken:false,containers:[{name:"floci",image:"floci/floci:2.3.0",readinessProbe:{tcpSocket:{port:4566},periodSeconds:1}}]}}' | k -n "$namespace" create -f -
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
jq -n --argjson nodeSelector "$floci_node_selector" '{apiVersion:"apps/v1",kind:"Deployment",metadata:{name:"floci"},spec:{replicas:1,selector:{matchLabels:{app:"floci"}},template:{metadata:{labels:{app:"floci","rhiza-ci-component":"floci"}},spec:{nodeSelector:$nodeSelector,enableServiceLinks:false,automountServiceAccountToken:false,containers:[{name:"floci",image:"floci/floci:2.3.0",ports:[{containerPort:4566}],readinessProbe:{tcpSocket:{port:4566},periodSeconds:1}}]}}}}' | k -n "$namespace" create -f -
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
jq -n --arg image "$image" --rawfile admin "$private/admin.token" --arg bucket "$bucket" --arg root "ci-$nonce" '{InitImage:$image,PeerPort:9090,EvidenceFormat:"rhiza-managed-sql/v1",ProfileSecretName:"runtime",Storage:{enable_reconfiguration:true,admin_token:$admin,objstore_provider:"s3",objstore_endpoint:"floci:4566",objstore_bucket:$bucket,objstore_prefix:$root,objstore_region:"us-east-1",objstore_insecure:true,objstore_access_key:"rhiza-local-test",objstore_secret_key:"rhiza-local-test",objstore_durability:"before-ack"}}' > "$private/runtime.json"
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
jq -n --arg image "$image" '{apiVersion:"rhiza.mrchypark.dev/v1alpha2",kind:"RhizaCluster",metadata:{name:"sql"},spec:{logicalID:"managed-ci",genesisIntent:"Create",voterCount:3,workload:{podTemplate:{spec:{containers:[{name:"sql",image:$image}]}}}}}' | k -n "$namespace" apply -f -
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
k -n "$namespace" get rhizacluster sql -o json > "$logs/after-sql.json"
jq -S '.status|{bootstrapPlan,genesis,plan:.applicationInit.plan}' "$logs/committed.json" > "$logs/immutable-before.json"
jq -S '.status|{bootstrapPlan,genesis,plan:.applicationInit.plan}' "$logs/after-sql.json" > "$logs/immutable-after.json"
cmp "$logs/immutable-before.json" "$logs/immutable-after.json"
# Graceful deletion exercises Deployment replacement, not proof that an isolated
# process or outstanding object-store request has physically terminated.
old_pod=$(jq -r '.items|sort_by(.metadata.name)|.[0].metadata.name' "$logs/ready-pods.json")
old_uid=$(jq -r --arg name "$old_pod" '.items[]|select(.metadata.name==$name)|.metadata.uid' "$logs/ready-pods.json")
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
  grep -F 'actual in-cluster Go FFI -> managed Host -> SQL ACK readback -> close passed' "$logs/go-ffi-contract-first.log" >/dev/null
  grep -Fx 'PASS: managed Rust FFI SQL contract' "$logs/go-ffi-contract-first.log" >/dev/null
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
  grep -F 'actual in-cluster Go FFI -> managed Host -> SQL ACK readback -> close passed' "$logs/go-ffi-contract-restart.log" >/dev/null
  grep -Fx 'PASS: managed Rust FFI SQL contract' "$logs/go-ffi-contract-restart.log" >/dev/null
fi
k -n "$namespace" exec "$replacement_pod" -c sql -- sha256sum /var/lib/rhiza/repair.json /var/lib/rhiza/data/prepared-adoption.json /var/lib/rhiza/data/qlog/voter-identity.json > "$logs/restart-identity-after.txt"
managed_same_pod_identity_oracle "$logs/restart-identity-before.txt" "$logs/restart-identity-after.txt"
run_client query-after-restart query '{"sql":"SELECT id, value FROM bootstrap_ci"}'
jq -e '.rows==[[1,"real-kubelet"]]' "$logs/query-after-restart.json" >/dev/null
printf '%s\n' 'PASS: actual kubelet bootstrap, Deployment Pod replacement, same-Pod main restart preserving origin/adoption/voter identity, NoPVC and ACK readback' 'GAP: later-operation rollover main restart, isolated-process physical fencing, operator-managed Service ownership and Chaos Mesh qualification remain pending; no NetworkPolicy claim' > "$logs/result.txt"
}
