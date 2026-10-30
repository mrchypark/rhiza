#!/bin/sh
# Initial bootstrap only: replacement/learner recovery and Chaos Mesh remain unqualified.
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
if [ "${1-}" = --oracle-self-test ]; then
  (cd scripts/ha-managed-bucket-setup-src && go test -mod=readonly -timeout=45s -run 'Test(GatedOracle|CommittedPlanOracle)$' .)
  exit 0
fi
[ "${GITHUB_ACTIONS-}" = true ] || { echo 'GitHub Actions only' >&2; exit 1; }
: "${RUNNER_TEMP:?}" "${GITHUB_SHA:?}" "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}"
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
k() { kubectl --kubeconfig "$kubeconfig" --context "kind-$cluster" --request-timeout=30s "$@"; }
cleanup() {
  result=$?
  trap - EXIT HUP INT TERM
  if [ "$owned" = 1 ]; then
    k -n "$namespace" get pods,deployments,replicasets,rhizaclusters,jobs,services,endpoints -o json > "$logs/final-state.json" 2>&1 || :
    for job in create-sql insert-sql query; do
      k -n "$namespace" logs "job/$job" > "$logs/$job.json" 2> "$logs/$job-log-errors.txt" || :
    done
    k -n "$namespace" get events > "$logs/events.txt" 2>&1 || :
    kind delete cluster --name "$cluster" > "$logs/delete.txt" 2>&1 || result=1
    kind get clusters > "$logs/clusters-after.txt" 2>&1 || result=1
    if grep -Fx "$cluster" "$logs/clusters-after.txt" >/dev/null; then result=1; fi
  fi
  if [ "$built" = 1 ]; then
    docker image rm "$image" > "$logs/image-cleanup.txt" 2>&1 || result=1
    if docker image inspect "$image" >/dev/null 2>&1; then result=1; fi
  fi
  rm -f "$private/runtime.json" "$private/kubeconfig" "$private/curl.conf" "$private/request.body" "$private/admin.token"
  rmdir "$private" || result=1
  exit "$result"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
kind get clusters > "$logs/clusters-before.txt"
if grep -Fx "$cluster" "$logs/clusters-before.txt" >/dev/null; then exit 1; fi
owned=1
kind create cluster --name "$cluster" --kubeconfig "$kubeconfig" --wait 120s > "$logs/create.txt" 2>&1
built=1
docker build -f Dockerfile.operator -t "$image" . > "$logs/build.txt" 2>&1
kind load docker-image --name "$cluster" "$image" > "$logs/load.txt" 2>&1
k create namespace "$namespace"
k apply -f deploy/operator/cluster-crd.yaml -f deploy/operator/crd.yaml
k wait --for=condition=Established --timeout=90s crd/rhizaclusters.rhiza.mrchypark.dev crd/rhizarecoveries.rhiza.mrchypark.dev
k -n "$namespace" create deployment floci --image=floci/floci:2.3.0
k -n "$namespace" expose deployment floci --port=4566
k -n "$namespace" rollout status deployment/floci --timeout=120s
bucket="rhiza-managed-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT-$nonce"
"$RHIZA_MANAGED_BUCKET_SETUP" "$kubeconfig" "kind-$cluster" "$namespace" "$bucket"
admin=$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')
umask 077
printf '%s' "$admin" > "$private/admin.token"
jq -n --arg image "$image" --rawfile admin "$private/admin.token" --arg bucket "$bucket" --arg root "ci-$nonce" '{InitImage:$image,PeerPort:9090,EvidenceFormat:"rhiza-managed-sql/v1",ProfileSecretName:"runtime",Storage:{admin_token:$admin,objstore_provider:"s3",objstore_endpoint:"floci:4566",objstore_bucket:$bucket,objstore_prefix:$root,objstore_region:"us-east-1",objstore_insecure:true,objstore_access_key:"rhiza-local-test",objstore_secret_key:"rhiza-local-test",objstore_durability:"before-ack"}}' > "$private/runtime.json"
printf 'header = "Authorization: Bearer %s"\n' "$admin" > "$private/curl.conf"
unset admin
k -n "$namespace" create secret generic runtime --from-file="runtime.json=$private/runtime.json"
k -n "$namespace" create serviceaccount operator
k -n "$namespace" create role bootstrap --verb=get,list --resource=rhizaclusters,pods,replicasets,deployments,secrets
k -n "$namespace" create role create-deployment --verb=create --resource=deployments
k -n "$namespace" create rolebinding bootstrap --role=bootstrap --serviceaccount="$namespace:operator"
k -n "$namespace" create rolebinding create-deployment --role=create-deployment --serviceaccount="$namespace:operator"
# Withhold status-update RBAC until actual kubelet termination and gate observations.
jq -n --arg image "$image" '{apiVersion:"apps/v1",kind:"Deployment",metadata:{name:"operator"},spec:{replicas:1,selector:{matchLabels:{app:"operator"}},template:{metadata:{labels:{app:"operator"}},spec:{serviceAccountName:"operator",securityContext:{runAsUser:65532,runAsGroup:65532,fsGroup:65532,runAsNonRoot:true},containers:[{name:"operator",image:$image,args:["--namespace=managed-bootstrap","--poll-interval=1s","--managed-runtime-profile=/etc/rhiza/managed/runtime.json"],volumeMounts:[{name:"runtime",mountPath:"/etc/rhiza/managed",readOnly:true}]}],volumes:[{name:"runtime",secret:{secretName:"runtime",defaultMode:288}}]}}}}' | k -n "$namespace" apply -f -
jq -n --arg image "$image" '{apiVersion:"rhiza.mrchypark.dev/v1alpha2",kind:"RhizaCluster",metadata:{name:"sql"},spec:{logicalID:"managed-ci",genesisIntent:"Create",voterCount:3,workload:{podTemplate:{spec:{containers:[{name:"sql",image:$image}]}}}}}' | k -n "$namespace" apply -f -
uid=$(k -n "$namespace" get rhizacluster sql -o jsonpath='{.metadata.uid}')
selector="rhiza.mrchypark.dev/cluster-uid=$uid"
attempt=0
until k -n "$namespace" get pods -l "$selector" -o json > "$logs/gated-pods.json" && sh scripts/ha-managed-bucket-setup oracle-gated "$logs/gated-pods.json"; do
  attempt=$((attempt+1)); [ "$attempt" -lt 120 ] || exit 1; sleep 1
done
k -n "$namespace" create role status --verb=update --resource=rhizaclusters/status
k -n "$namespace" create rolebinding status --role=status --serviceaccount="$namespace:operator"
k -n "$namespace" create role runtime-read --verb=get --resource=rhizaclusters,pods,replicasets,deployments
k -n "$namespace" create rolebinding runtime-read --role=runtime-read --serviceaccount="$namespace:default"
k -n "$namespace" create rolebinding runtime-status --role=status --serviceaccount="$namespace:default"
k -n "$namespace" wait --for=condition=Ready pod -l "$selector" --timeout=180s
k -n "$namespace" get rhizacluster sql -o json > "$logs/committed.json"
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
  k -n "$namespace" create secret generic "request-$job" --from-file="config=$private/curl.conf" --from-file="body=$private/request.body"
  jq -n --arg job "$job" --arg route "http://bootstrap-client.$namespace.svc.cluster.local:8080/$route" '{apiVersion:"batch/v1",kind:"Job",metadata:{name:$job,labels:{"rhiza-ci-owner":"harness"}},spec:{backoffLimit:0,activeDeadlineSeconds:90,template:{spec:{restartPolicy:"Never",automountServiceAccountToken:false,securityContext:{runAsUser:65532,runAsGroup:65532,fsGroup:65532,runAsNonRoot:true},containers:[{name:"client",image:"curlimages/curl:8.12.1@sha256:94e9e444bcba979c2ea12e27ae39bee4cd10bc7041a472c4727a558e213744e6",command:["curl"],args:["--silent","--show-error","--fail","--max-time","60","--noproxy","*","--config","/request/config","--header","Content-Type: application/json","--data-binary","@/request/body",$route],securityContext:{allowPrivilegeEscalation:false,readOnlyRootFilesystem:true,capabilities:{drop:["ALL"]}},volumeMounts:[{name:"request",mountPath:"/request",readOnly:true}]}],volumes:[{name:"request",secret:{secretName:("request-"+$job),defaultMode:288}}]}}}}' | k -n "$namespace" create -f -
  k -n "$namespace" wait --for=condition=Complete "job/$job" --timeout=120s
  k -n "$namespace" logs "job/$job" > "$logs/$job.json"
}
run_client create-sql execute '{"request_id":"bootstrap-create","sql":"CREATE TABLE bootstrap_ci (id INTEGER PRIMARY KEY, value TEXT)"}'
run_client insert-sql execute '{"request_id":"bootstrap-insert","sql":"INSERT INTO bootstrap_ci VALUES (1, '\''real-kubelet'\'')"}'
run_client query query '{"sql":"SELECT id, value FROM bootstrap_ci"}'
jq -e '.rows==[[1,"real-kubelet"]]' "$logs/query.json" >/dev/null
k -n "$namespace" get rhizacluster sql -o json > "$logs/after-sql.json"
jq -S '.status|{bootstrapPlan,genesis,plan:.applicationInit.plan}' "$logs/committed.json" > "$logs/immutable-before.json"
jq -S '.status|{bootstrapPlan,genesis,plan:.applicationInit.plan}' "$logs/after-sql.json" > "$logs/immutable-after.json"
cmp "$logs/immutable-before.json" "$logs/immutable-after.json"
printf '%s\n' 'PASS: actual kubelet three-Pod initial bootstrap and authenticated in-cluster SQL via test-owned Service DNS/ClusterIP' 'GAP: operator-managed Service ownership, replacement learner pipeline and Chaos Mesh recovery qualification remain pending; no NetworkPolicy claim' > "$logs/result.txt"
