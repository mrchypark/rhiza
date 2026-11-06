#!/bin/sh
# Offline guardrails for direct-host mode. No kubectl/API calls.
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
if [ "${1-}" = --podchaos-self-test ]; then
  scratch=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-kind-chaos-oracles.XXXXXX")
  trap 'rm -f "$scratch"/*.json "$scratch"/*.jsonl "$scratch"/*.txt "$scratch"/*.exit; rmdir "$scratch"' EXIT HUP INT TERM
  namespace=test-ns release=test-run
  . scripts/ha-managed-podchaos.sh
  managed_chaos_request pod-kill sql-pod > "$scratch/request.json"
  jq -e '.spec=={action:"pod-kill",mode:"one",duration:"10s",containerNames:["sql"],selector:{namespaces:["test-ns"],pods:{"test-ns":["sql-pod"]}}}' "$scratch/request.json" >/dev/null
  managed_chaos_request container-kill sql-pod > "$scratch/request.json"
  jq -e '.spec.action=="container-kill" and .spec.duration=="10s"' "$scratch/request.json" >/dev/null
  if managed_chaos_request network-delay sql-pod >/dev/null 2>&1; then exit 1; fi
  if managed_chaos_request pod-kill '' >/dev/null 2>&1; then exit 1; fi
  jq -n '{status:{conditions:[{type:"AllInjected",status:"True"}],experiment:{containerRecords:[{id:"test-ns/sql-pod",phase:"Injected",injectedCount:1,recoveredCount:0}]}}}' > "$scratch/record.json"
  chaos_record_oracle "$scratch/record.json" test-ns/sql-pod
  for expression in '.status.conditions[0].status="False"' '.status.experiment.containerRecords[0].phase="NotInjected"' '.status.experiment.containerRecords[0].injectedCount=0' '.status.experiment.containerRecords[0].recoveredCount=1' '.status.experiment.containerRecords+=.status.experiment.containerRecords'; do
    jq "$expression" "$scratch/record.json" > "$scratch/bad.json"
    if chaos_record_oracle "$scratch/bad.json" test-ns/sql-pod; then exit 1; fi
  done
  if chaos_record_oracle "$scratch/record.json" wrong-target; then exit 1; fi
  runtime_helper=$(sed -n '/^kind_runtime_oracle() {/,/^}/p' scripts/test-ha-managed-bootstrap.sh)
  [ -n "$runtime_helper" ]; eval "$runtime_helper"
  jq -n '{containers:[{id:"old",state:"CONTAINER_EXITED",labels:{"io.kubernetes.pod.uid":"old-uid","io.kubernetes.container.name":"sql"}}]}' > "$scratch/cri.json"
  kind_runtime_oracle "$scratch/cri.json" old-uid old pod-kill
  jq '.containers[0].state="CONTAINER_RUNNING"' "$scratch/cri.json" > "$scratch/bad.json"
  if kind_runtime_oracle "$scratch/bad.json" old-uid old pod-kill; then exit 1; fi
  eval "$(sed -n '/^  controllers=/p' scripts/test-ha-managed-bootstrap.sh)"
  eval "$(sed -n '/^kind_chaos_profile_valid() {/,/^}/p' scripts/test-ha-managed-bootstrap.sh)"
  jq -cn --arg ns "$namespace" --arg enabled "$(printf '%s' "$controllers" | jq -r 'join(",")')" '
    {kind:"Deployment",metadata:{name:"chaos-controller-manager"},spec:{template:{spec:{containers:[{name:"chaos-mesh",env:[{name:"ENABLED_CONTROLLERS",value:$enabled},{name:"CLUSTER_SCOPED",value:"false"},{name:"TARGET_NAMESPACE",value:$ns},{name:"ENABLE_FILTER_NAMESPACE",value:"false"},{name:"ENABLED_WEBHOOKS",value:"podchaos"}]}]}}}},
    {kind:"DaemonSet",metadata:{name:"chaos-daemon"},spec:{template:{spec:{hostPID:false,containers:[{securityContext:{privileged:false,allowPrivilegeEscalation:false,capabilities:{drop:["ALL"]}},command:["--runtime-socket-path","/host-run/containerd.sock"],volumeMounts:[{name:"socket-path",mountPath:"/host-run"}]}],volumes:[{name:"socket-path",hostPath:{path:"/run/containerd"}}]}}}}
    , {kind:"RoleBinding",metadata:{name:"test-chaos-controller-manager-target-namespace",namespace:$ns},roleRef:{kind:"ClusterRole",name:"test-chaos-controller-manager-target-namespace"}}
  ' > "$scratch/profile.jsonl"
  kind_chaos_profile_valid "$scratch/profile.jsonl"
  for expression in '(.spec.template.spec.containers[]?.env[]?|select(.name=="TARGET_NAMESPACE").value)=""' '(.spec.template.spec.containers[]?.env[]?|select(.name=="CLUSTER_SCOPED").value)="true"' '(.spec.template.spec.containers[]?.env[]?|select(.name=="ENABLE_FILTER_NAMESPACE").value)="true"' '(.spec.template.spec.containers[]?.env[]?|select(.name=="ENABLED_CONTROLLERS").value)+=",remotecluster"' '(select(.kind=="DaemonSet")|.spec.template.spec.hostPID)=true' '(select(.kind=="DaemonSet")|.spec.template.spec.containers[0].securityContext.privileged)=true' '(select(.kind=="DaemonSet")|.spec.template.spec.volumes)+=[{hostPath:{path:"/sys"}}]' '(select(.kind=="DaemonSet")|.spec.template.spec.containers[0].command[1])="/wrong/socket"'; do
    jq -c "$expression" "$scratch/profile.jsonl" > "$scratch/bad-profile.jsonl"
    if kind_chaos_profile_valid "$scratch/bad-profile.jsonl"; then exit 1; fi
  done
  if kind_runtime_oracle "$scratch/bad.json" old-uid old container-kill; then exit 1; fi
  jq '.containers+=[{id:"new",state:"CONTAINER_RUNNING",labels:{"io.kubernetes.pod.uid":"old-uid","io.kubernetes.container.name":"sql"}}]' "$scratch/cri.json" > "$scratch/new.json"
  kind_runtime_oracle "$scratch/new.json" old-uid old container-kill new
  if kind_runtime_oracle "$scratch/new.json" old-uid old container-kill wrong-new-id; then exit 1; fi
  jq '.containers+=[{id:"unexpected-running",state:"CONTAINER_RUNNING",labels:{"io.kubernetes.pod.uid":"old-uid","io.kubernetes.container.name":"sql"}}]' "$scratch/new.json" > "$scratch/bad.json"
  if kind_runtime_oracle "$scratch/bad.json" old-uid old container-kill new; then exit 1; fi
  if kind_runtime_oracle "$scratch/new.json" old-uid old pod-kill; then exit 1; fi
  printf '%s\n' '{"containers":[]}' > "$scratch/absent.json"
  kind_runtime_oracle "$scratch/absent.json" old-uid old pod-kill
  printf '%s\n' '{}' > "$scratch/bad.json"
  if kind_runtime_oracle "$scratch/bad.json" old-uid old pod-kill; then exit 1; fi
  # Run the actual callback in an errexit child, not in a conditional function.
  eval "$(sed -n '/^kind_node_guard() {/,/^}/p' scripts/test-ha-managed-bootstrap.sh)"
  # shellcheck disable=SC2034 # Read by the actual source-extracted node guard.
  cluster=owned-kind runtime_node=foreign-node node_queries=0
  # shellcheck disable=SC2329 # Called by the actual source-extracted node guard.
  docker() { node_queries=$((node_queries+1)); printf '%s\n' wrong-cluster; }
  if kind_node_guard; then exit 1; fi
  [ "$node_queries" = 0 ]
  # shellcheck disable=SC2034 # Read by the actual source-extracted node guard.
  runtime_node=owned-kind-control-plane
  if kind_node_guard; then exit 1; fi
  # shellcheck disable=SC2329 # Called by the actual source-extracted node guard.
  docker() {
    case "$3" in
      *io.x-k8s.kind.cluster*) printf '%s\n' owned-kind ;;
      *io.x-k8s.kind.role*) printf '%s\n' control-plane ;;
      *) return 2 ;;
    esac
  }
  kind_node_guard
  for callback_mode in pass wait-fail delete-fail stale-fault; do
    callback_exit=0
    sh -c 'set -eu
      . scripts/ha-managed-podchaos.sh
      private=$1 logs=$1 namespace=test-ns release=test-run uid=cluster-uid mode=$2
      chaos_scope_guard() { :; }
      chaos_k() {
        printf "%s\n" "$*" >> "$logs/calls.txt"
        case "$1:$2" in
          get:pod) printf "%s\n" '\''{"metadata":{"uid":"pod-uid","labels":{"rhiza.mrchypark.dev/cluster-uid":"cluster-uid"}},"status":{"containerStatuses":[{"name":"sql","ready":true,"containerID":"containerd://old"}]}}'\'' ;;
          create:*) : ;;
          wait:*) [ "$mode" != wait-fail ] || return 41 ;;
          get:pods) printf "%s\n" '\''{"items":[]}'\'' ;;
          get:podchaos/pod-kill)
            if [ "${deleted-0}" = 1 ] && [ "$mode" != stale-fault ]; then return 0; fi
            printf "%s\n" '\''{"metadata":{"namespace":"test-ns","uid":"fault-uid","labels":{"rhiza-ci-run":"test-run"}},"status":{"conditions":[{"type":"AllInjected","status":"True"}],"experiment":{"containerRecords":[{"id":"test-ns/sql-pod","phase":"Injected","injectedCount":1,"recoveredCount":0}]}}}'\'' ;;
          get:events) printf "%s\n" '\''{"items":[]}'\'' ;;
          delete:*) [ "$mode" != delete-fail ] || return 43; deleted=1 ;;
          *) return 49 ;;
        esac
      }
      direct_chaos_fault pod-kill pod-kill sql-pod pod-uid
      printf "%s\n" callback-completed >> "$logs/calls.txt"
    ' fixture "$scratch" "$callback_mode" > "$scratch/child-output.txt" 2>&1 || callback_exit=$?
    case "$callback_mode" in
      pass) [ "$callback_exit" = 0 ]; grep -Fx callback-completed "$scratch/calls.txt" >/dev/null ;;
      wait-fail) [ "$callback_exit" = 41 ]; [ "$(cat "$scratch/pod-kill-AllInjected-wait.exit")" = 41 ]; [ -s "$scratch/pod-kill-AllInjected-failure.json" ] ;;
      delete-fail) [ "$callback_exit" = 43 ] ;;
      stale-fault) [ "$callback_exit" != 0 ] ;;
    esac
    if [ "$callback_mode" != pass ] && grep -Fx callback-completed "$scratch/calls.txt" >/dev/null; then exit 1; fi
    rm -f "$scratch/calls.txt" "$scratch/pod-kill-AllInjected-wait.exit"
  done
  cleanup_helper=$(sed -n '/^cleanup() {/,/^}/p' scripts/test-ha-managed-bootstrap.sh)
  private_cleanup_helper=$(sed -n '/^cleanup_managed_private() {/,/^}/p' scripts/test-ha-managed-bootstrap.sh)
  [ -n "$private_cleanup_helper" ]
  mkdir "$scratch/cleanup-private"
  : > "$scratch/cleanup-private/managed-install.jsonl"
  cleanup_exit=0
  sh -c 'set +e; logs=$1 private=$1/cleanup-private owned=0 built=0; eval "$3"; eval "$2"; (exit 17); cleanup' fixture "$scratch" "$cleanup_helper" "$private_cleanup_helper" || cleanup_exit=$?
  [ "$cleanup_exit" = 17 ] && [ "$(cat "$scratch/driver-original.exit")" = 17 ] && [ "$(cat "$scratch/driver.exit")" = 17 ]
  [ ! -e "$scratch/cleanup-private" ]
  printf '%s\n' 'PASS: actual PodChaos request/record/CRI callbacks and failure exit controls'
  exit 0
fi
sh "$0" --podchaos-self-test
direct=scripts/test-ha-managed-direct-host-chaos.sh
shared=scripts/ha-managed-podchaos.sh
bootstrap=scripts/test-ha-managed-bootstrap.sh
fixture=scripts/ha-managed-bootstrap-fixture.sh
candidate=.github/workflows/ha-managed-candidate-image.yml
chaos=.github/workflows/ha-managed-cluster-chaos.yml
operator_builder=pkg/operator/bootstrap_members_v1alpha2.go
operator_initializer=pkg/operator/initializer_v1alpha2.go
for source in "$direct" "$shared" "$bootstrap" "$fixture" scripts/test-ha-managed-direct-host-chaos-test.sh; do sh -n "$source"; done
scratch=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-direct-chaos-oracles.XXXXXX")
cleanup_test() {
  rm -f "$scratch/old-id" "$scratch/new-id" "$scratch/same-a" "$scratch/same-b" "$scratch/preflight-list.json" "$scratch/bucket-contract_test.go" "$scratch/bucket-overlay.json"
  rmdir "$scratch"
}
trap cleanup_test EXIT HUP INT TERM
identity_oracles=$(sed -n '/^managed_replacement_identity_oracle() {/,/^}/p' "$fixture")
[ -n "$identity_oracles" ]
printf '%s\n' 'old-voter-identity' > "$scratch/old-id"
printf '%s\n' 'new-voter-identity' > "$scratch/new-id"
sh -c 'set -eu; eval "$1"; managed_replacement_identity_oracle "$2" "$3"' fixture "$identity_oracles" "$scratch/old-id" "$scratch/new-id"
if sh -c 'set -eu; eval "$1"; managed_replacement_identity_oracle "$2" "$3"' fixture "$identity_oracles" "$scratch/old-id" "$scratch/old-id" 2>/dev/null; then
  echo 'replacement identity oracle accepted reused identity' >&2; exit 1
fi
same_pod_oracle=$(sed -n '/^managed_same_pod_identity_oracle() {/,/^}/p' "$fixture")
printf '%s\n' 'same-voter-identity' > "$scratch/same-a"
cp "$scratch/same-a" "$scratch/same-b"
sh -c 'set -eu; eval "$1"; managed_same_pod_identity_oracle "$2" "$3"' fixture "$same_pod_oracle" "$scratch/same-a" "$scratch/same-b"
printf '%s\n' 'changed-voter-identity' > "$scratch/same-b"
if sh -c 'set -eu; eval "$1"; managed_same_pod_identity_oracle "$2" "$3"' fixture "$same_pod_oracle" "$scratch/same-a" "$scratch/same-b" 2>/dev/null; then
  echo 'same-Pod oracle accepted changed identity' >&2; exit 1
fi
preflight_helper=$(sed -n '/^assert_names_absent() {/,/^}/p' "$direct")
[ -n "$preflight_helper" ]
run_preflight_case() {
  mode=$1 list_json=$2 expected=$3
  code=0
  sh -c 'set -eu; private=$1; mode=$2; test_list_json=$3; helper=$4
    hn() { [ "$mode" != error ] || return 41; printf "%s\n" "$test_list_json"; }
    eval "$helper"
    assert_names_absent services wanted
  ' fixture "$scratch" "$mode" "$list_json" "$preflight_helper" >/dev/null 2>&1 || code=$?
  if [ "$expected" = pass ]; then [ "$code" -eq 0 ]; else [ "$code" -ne 0 ]; fi
}
run_preflight_case ok '{"items":[{"metadata":{"name":"another"}}]}' pass
run_preflight_case collision '{"items":[{"metadata":{"name":"wanted"}}]}' fail
run_preflight_case error '{}' fail
grep -Fq 'operator) dockerfile=Dockerfile.operator' "$candidate"
grep -Fq 'managed-ffi-contract) dockerfile=Dockerfile.managed-ffi-contract' "$candidate"
grep -Fq 'rhiza-candidate-image/v2' "$candidate"
grep -Fq 'options: [amd64, arm64]' "$candidate"
grep -Fq "'ubuntu-24.04-arm'" "$candidate"
grep -Fq 'platforms: ${{ steps.variant.outputs.platform }}' "$candidate"
grep -Fq '"platform":"%s"' "$candidate"
mode_guard=$(sed -n '/^execution_mode=/,/^esac/p' "$direct")
[ -n "$mode_guard" ]
sh -c 'set -eu; git() { printf "%s\n" test-source; }; RHIZA_CHAOS_EXECUTION_MODE=local RHIZA_LOCAL_RUN_ID=123; unset GITHUB_ACTIONS; eval "$1"; [ "$execution_mode" = local ]; [ "$GITHUB_RUN_ID" = 123 ]; [ "$GITHUB_SHA" = test-source ]; [ "${GITHUB_ACTIONS-}" != true ]' guard "$mode_guard"
local_id_34=1234567890123456789012345678901234
bucket_assignment=$(sed -n '/^bucket=/p' "$fixture")
[ -n "$bucket_assignment" ]
generated_buckets=
for local_id in 123 "$local_id_34"; do
  generated_bucket=$(sh -c 'set -eu; git() { printf "%s\n" test-source; }; RHIZA_CHAOS_EXECUTION_MODE=local RHIZA_LOCAL_RUN_ID=$2; unset GITHUB_ACTIONS; eval "$1"; nonce=012345abcdef; eval "$3"; [ "${#bucket}" -le 63 ]; printf "%s\n" "$bucket"' guard "$mode_guard" "$local_id" "$bucket_assignment")
  generated_buckets="$generated_buckets $generated_bucket"
done
cat > "$scratch/bucket-contract_test.go" <<'GO'
package main

import (
  "os"
  "strings"
  "testing"
)

func TestGeneratedLocalChaosBucketContract(t *testing.T) {
  buckets := strings.Fields(os.Getenv("RHIZA_GENERATED_TEST_BUCKETS"))
  if len(buckets) != 2 {
    t.Fatal("expected short and maximum-length generated buckets")
  }
  for _, bucket := range buckets {
    if err := validate("http://127.0.0.1:1234", bucket); err != nil {
      t.Fatalf("driver-generated bucket %q rejected by real setup validator: %v", bucket, err)
    }
  }
}
GO
jq -n --arg source "$(pwd)/scripts/ha-managed-bucket-setup-src/main_test.go" --arg replacement "$scratch/bucket-contract_test.go" '{Replace:{($source):$replacement}}' > "$scratch/bucket-overlay.json"
(cd scripts/ha-managed-bucket-setup-src && RHIZA_GENERATED_TEST_BUCKETS="$generated_buckets" go test -mod=readonly -overlay "$scratch/bucket-overlay.json" -run '^TestGeneratedLocalChaosBucketContract$' -count=1 -timeout=30s)
for invalid_id in local-test "$(printf '123\nbad')" "$(printf '123\n456')" "$(printf '123\t456')"; do
  if sh -c 'set -eu; git() { printf "%s\n" test-source; }; RHIZA_CHAOS_EXECUTION_MODE=local RHIZA_LOCAL_RUN_ID=$2; unset GITHUB_ACTIONS; eval "$1"' guard "$mode_guard" "$invalid_id" 2>/dev/null; then
    echo 'local mode accepted a nonnumeric or multiline ID rejected by the bucket helper' >&2; exit 1
  fi
done
if sh -c 'set -eu; git() { printf "%s\n" test-source; }; RHIZA_CHAOS_EXECUTION_MODE=local RHIZA_LOCAL_RUN_ID=$2; unset GITHUB_ACTIONS; eval "$1"' guard "$mode_guard" "${local_id_34}5" 2>/dev/null; then
  echo 'local mode accepted an ID exceeding the bucket naming budget' >&2; exit 1
fi
if sh -c 'set -eu; RHIZA_CHAOS_EXECUTION_MODE=local RHIZA_LOCAL_RUN_ID=123 GITHUB_ACTIONS=true; eval "$1"' guard "$mode_guard" 2>/dev/null; then
  echo 'local mode accepted a GitHub Actions execution' >&2; exit 1
fi
if sh -c 'set -eu; unset GITHUB_ACTIONS RHIZA_CHAOS_EXECUTION_MODE; eval "$1"' guard "$mode_guard" 2>/dev/null; then
  echo 'default mode accepted a non-CI execution' >&2; exit 1
fi
grep -Fq 'io.rhiza.candidate.variant=' "$candidate"
grep -Fq 'options: [release, candidate, candidate-direct-host]' "$chaos"
grep -Fq 'candidate-direct-host) IMAGE_VARIANT=managed-ffi-contract' "$chaos"
dispatch_inputs=$(sed -n '/^    inputs:/,/^permissions:/p' "$chaos" | grep -Ec '^      [a-z_]+:')
[ "$dispatch_inputs" -le 10 ]
grep -Fq 'direct-host requires managed-ffi-contract image' "$direct"
grep -Fq '[ "$RHIZA_MANAGED_TEST_DOCKERFILE" = Dockerfile.managed-ffi-contract ]' "$direct"
grep -Fq 'export RHIZA_MANAGED_FIXTURE_NODE=$RHIZA_HOST_NODE' "$direct"
grep -Fq 'nodeSelector:$nodeSelector' "$fixture"
[ "$(grep -Fc 'nodeSelector:$nodeSelector' "$fixture")" -eq 2 ]
grep -Fq 'containers:[{name:"sql",image:$image}]' "$fixture"
grep -Fq 'ENTRYPOINT ["rhiza-operator"]' Dockerfile.managed-ffi-contract
grep -Fq 'COPY --chmod=0755 scripts/managed-ffi-contract-entrypoint.sh /usr/local/bin/rhiza-ha-init' Dockerfile.managed-ffi-contract
grep -Fq 'exec /usr/local/libexec/rhiza-ha-init "$@"' scripts/managed-ffi-contract-entrypoint.sh
grep -Fq 'main["command"] = []string{"/usr/local/bin/rhiza-ha-init"}' "$operator_builder"
grep -Fq 'podSpec, err := V1Alpha2ManagedSQLPodSpec' "$operator_initializer"
grep -Fq 'name=="rhizaclusters" and .kind=="RhizaCluster"' "$direct"
grep -Fq 'create podchaos.chaos-mesh.org' "$direct"
grep -Fq '. scripts/ha-managed-podchaos.sh' "$direct"
grep -Fq 'AllInjected' "$shared"
if grep -Fq 'AllRecovered' "$shared"; then echo 'one-shot kill waits for unsupported pre-delete recovery condition' >&2; exit 1; fi
grep -Fq 'metadata.uid!=$old_uid' "$shared"
grep -Fq 'containerID!=$old' "$shared"
grep -Fq 'voter-identity.json' "$direct"
grep -Fq 'rhiza-prepare-identity' "$direct"
grep -Fq 'rhiza-await-bootstrap-plan' "$direct"
if grep -Eq 'helm |kubectl apply .*cluster-crd|kubectl apply .*crd\.yaml' "$direct"; then
  echo 'direct-host runner contains forbidden vcluster/CRD installation path' >&2; exit 1
fi
if grep -Eq 'kind:(Namespace|ClusterRole|ClusterRoleBinding|CustomResourceDefinition)|persistentVolumeClaim' "$direct"; then
  echo 'direct-host fixture includes cluster-scoped or persistent storage resource' >&2; exit 1
fi
printf '%s\n' 'PASS: direct-host mode requires the provenance-bound FFI image, uses namespace-local exact-UID Chaos Mesh checks, and does not install CRDs or vcluster'
