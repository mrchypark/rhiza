#!/bin/sh
# Offline guardrails for direct-host mode. No kubectl/API calls.
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
if [ "${1-}" = --overlay-self-test ]; then
  scratch=$(mktemp -d)
  trap 'rm -f "$scratch"/overlay.json "$scratch"/direct-overlay-created.tsv "$scratch"/direct-cluster-create-attempted* "$scratch"/overlay-delete.json "$scratch"/source-cluster.json "$scratch"/source-*.json "$scratch"/status-*.json "$scratch"/test-owned-*.json "$scratch"/test-owned-*.err "$scratch"/residual-* "$scratch"/readme-deletes.txt "$scratch"/finalizer-patch.json "$scratch"/access-delete-order.txt; rmdir "$scratch"' EXIT HUP INT TERM
  private=$scratch logs=$scratch namespace=test-ns
  release=test-release
  attempt_id=current-attempt
  cluster_create_attempt="$scratch/direct-cluster-create-attempted.$attempt_id"
  direct=scripts/test-ha-managed-direct-host-chaos.sh
  for helper in project_direct_overlay direct_overlay_path delete_direct_overlay create_direct_cluster report_unrecorded_cluster managed_cleanup_status_safe force_release_test_owned_managed_finalizer delete_test_owned_managed_access; do
    definition=$(sed -n "/^$helper() {/,/^}/p" "$direct")
    [ -n "$definition" ]
    eval "$definition"
  done
  jq -n '{items:(["Role","RoleBinding","ServiceAccount"] | map(. as $k | {kind:$k,metadata:{name:"rhiza-operator"},rules:[{resources:["pods"],verbs:["get"]}]+(if $k=="Role" then [{resources:["rhizarecoveries"],verbs:["create"]},{resources:["rhizarecoveries/status"],verbs:["update"]}] else [] end)}) + [{kind:"Deployment",metadata:{name:"rhiza-operator"}}])}' > "$scratch/overlay.json"
  project_direct_overlay "$scratch/overlay.json" | jq -e '[.items[]|select(.kind=="Role")|.rules[]]==[{resources:["pods"],verbs:["get"]}]' >/dev/null
  calls=0
  hn() {
    calls=$((calls+1))
    [ "$*" = "delete --raw /apis/rbac.authorization.k8s.io/v1/namespaces/test-ns/roles/rhiza-operator -f $scratch/overlay-delete.json" ] || return 1
    jq -e '.preconditions.uid=="created-uid"' "$scratch/overlay-delete.json" >/dev/null
  }
  delete_direct_overlay
  [ "$calls" = 0 ]
  printf 'Role\trhiza-operator\tcreated-uid\n' > "$scratch/direct-overlay-created.tsv"
  delete_direct_overlay
  [ "$calls" = 1 ]
  printf 'Role\tunowned\tother-uid\n' > "$scratch/direct-overlay-created.tsv"
  if delete_direct_overlay; then exit 1; fi
  [ "$calls" = 1 ]
  jq -n '{apiVersion:"rhiza.mrchypark.dev/v1alpha2",kind:"RhizaCluster",metadata:{name:"sql",namespace:"test-ns",uid:"owned-uid",resourceVersion:"7",deletionTimestamp:"2026-11-17T00:00:00Z",finalizers:["example.invalid/keep","rhiza.mrchypark.dev/managed-host-cleanup"]},status:{authority:{pendingWrite:false},repair:{state:"Completed",completion:{kind:"Terminal"}}}}' > "$scratch/source-cluster.json"
  managed_cleanup_status_safe "$scratch/source-cluster.json"
  jq '.status.repair={state:"Aborted",completion:{kind:"Abort"}}' "$scratch/source-cluster.json" > "$scratch/status-aborted.json"
  managed_cleanup_status_safe "$scratch/status-aborted.json"
  jq 'del(.status.repair)' "$scratch/source-cluster.json" > "$scratch/status-no-repair.json"
  managed_cleanup_status_safe "$scratch/status-no-repair.json"
  for expression in \
    '.status={}' \
    '.status.authority.pendingWrite=true' \
    '.status.authority.transition={}' \
    '.status.authority.pendingRequest={}' \
    '.status.replicaStep={}' \
    '.status.recoveryQueue={entries:[1]}' \
    '.status.repair.state="Preparing"' \
    '.status.repair.state="Closed"' \
    '.status.repair.state="unknown"' \
    '.status.repair.completion=null'; do
    jq -c "$expression" "$scratch/source-cluster.json" > "$scratch/status-case.json"
    if managed_cleanup_status_safe "$scratch/status-case.json"; then exit 1; fi
  done
  patches=0
  hn() {
    case "$1 $2 $3" in
      'get rhizacluster sql') cat "$scratch/source-cluster.json" ;;
      'patch rhizacluster sql')
        [ "$4 $5" = '--type=json -p' ] || return 1
        printf '%s\n' "$6" > "$scratch/finalizer-patch.json"
        patches=$((patches+1))
        ;;
      *) return 1 ;;
    esac
  }
  force_release_test_owned_managed_finalizer RhizaCluster sql owned-uid "$scratch/source-cluster.json"
  [ "$patches" = 1 ]
  jq -e '
    length==7 and
    .[0]=={op:"test",path:"/metadata/uid",value:"owned-uid"} and
    .[1]=={op:"test",path:"/metadata/resourceVersion",value:"7"} and
    .[2]=={op:"test",path:"/metadata/deletionTimestamp",value:"2026-11-17T00:00:00Z"} and
    .[3]=={op:"test",path:"/status",value:{authority:{pendingWrite:false},repair:{state:"Completed",completion:{kind:"Terminal"}}}} and
    .[4].op=="test" and .[4].path=="/metadata/finalizers" and
    .[5]=={op:"test",path:"/metadata/finalizers/1",value:"rhiza.mrchypark.dev/managed-host-cleanup"} and
    .[6]=={op:"replace",path:"/metadata/finalizers",value:["example.invalid/keep"]}
  ' "$scratch/finalizer-patch.json" >/dev/null
  jq '.metadata.uid="other"' "$scratch/source-cluster.json" > "$scratch/source-cluster.json.tmp"
  mv "$scratch/source-cluster.json.tmp" "$scratch/source-cluster.json"
  if force_release_test_owned_managed_finalizer RhizaCluster sql owned-uid "$scratch/source-cluster.json"; then exit 1; fi
  [ "$patches" = 1 ]
  jq '.metadata.uid="owned-uid"' "$scratch/source-cluster.json" > "$scratch/source-cluster.json.tmp"
  mv "$scratch/source-cluster.json.tmp" "$scratch/source-cluster.json"
  uid=owned-uid
  sa="rhiza-managed-host-$(printf '%s\000%s\000%s' test-ns sql "$uid" | sha256sum | cut -c1-16)"
  deployment="rhiza-$(printf '%s' "$uid" | sha256sum | cut -c1-20)-g0"
  jq -n --arg sa "$sa" --arg uid "$uid" '{kind:"ServiceAccount",metadata:{name:$sa,namespace:"test-ns",uid:"sa-uid",labels:{"app.kubernetes.io/managed-by":"rhiza-operator","rhiza.mrchypark.dev/cluster-uid":$uid,"rhiza.mrchypark.dev/cluster-name":"sql","rhiza.mrchypark.dev/access-contract":"v1alpha2-host-v1","rhiza.mrchypark.dev/access-lifecycle":"retain-until-manual-cleanup"}}}' > "$scratch/source-sa.json"
  jq -n --arg sa "$sa" --arg uid "$uid" --arg deployment "$deployment" '{kind:"Role",metadata:{name:$sa,namespace:"test-ns",uid:"role-uid",labels:{"app.kubernetes.io/managed-by":"rhiza-operator","rhiza.mrchypark.dev/cluster-uid":$uid,"rhiza.mrchypark.dev/cluster-name":"sql","rhiza.mrchypark.dev/access-contract":"v1alpha2-host-v1","rhiza.mrchypark.dev/access-lifecycle":"retain-until-manual-cleanup"}},rules:[{apiGroups:["rhiza.mrchypark.dev"],resources:["rhizaclusters"],resourceNames:["sql"],verbs:["get"]},{apiGroups:["rhiza.mrchypark.dev"],resources:["rhizaclusters/status"],resourceNames:["sql"],verbs:["get","update"]},{apiGroups:[""],resources:["pods"],verbs:["get","list"]},{apiGroups:["apps"],resources:["deployments"],resourceNames:[$deployment],verbs:["get"]},{apiGroups:["apps"],resources:["replicasets"],verbs:["get"]}]}' > "$scratch/source-role.json"
  jq -n --arg sa "$sa" --arg uid "$uid" '{kind:"RoleBinding",metadata:{name:($sa+"-binding"),namespace:"test-ns",uid:"binding-uid",labels:{"app.kubernetes.io/managed-by":"rhiza-operator","rhiza.mrchypark.dev/cluster-uid":$uid,"rhiza.mrchypark.dev/cluster-name":"sql","rhiza.mrchypark.dev/access-contract":"v1alpha2-host-v1","rhiza.mrchypark.dev/access-lifecycle":"retain-until-manual-cleanup"}},roleRef:{apiGroup:"rbac.authorization.k8s.io",kind:"Role",name:$sa},subjects:[{kind:"ServiceAccount",name:$sa,namespace:"test-ns"}]}' > "$scratch/source-binding.json"
  : > "$scratch/access-delete-order.txt"
  hn() {
    case "$1 $2 $3" in
      "get serviceaccount $sa") cat "$scratch/source-sa.json" ;;
      "get role $sa") cat "$scratch/source-role.json" ;;
      "get rolebinding $sa-binding") cat "$scratch/source-binding.json" ;;
      'delete --raw '*)
        printf '%s\n' "$3" >> "$scratch/access-delete-order.txt"
        case "$3" in
          *rolebindings/*) expected_uid=binding-uid ;;
          *roles/*) expected_uid=role-uid ;;
          *serviceaccounts/*) expected_uid=sa-uid ;;
        esac
        jq -e --arg uid "$expected_uid" '.preconditions.uid==$uid' "$5" >/dev/null
        ;;
      *) return 1 ;;
    esac
  }
  delete_test_owned_managed_access "$uid"
  [ "$(sed -n '1p' "$scratch/access-delete-order.txt")" = "/apis/rbac.authorization.k8s.io/v1/namespaces/test-ns/rolebindings/$sa-binding" ]
  [ "$(sed -n '2p' "$scratch/access-delete-order.txt")" = "/apis/rbac.authorization.k8s.io/v1/namespaces/test-ns/roles/$sa" ]
  [ "$(sed -n '3p' "$scratch/access-delete-order.txt")" = "/api/v1/namespaces/test-ns/serviceaccounts/$sa" ]
  cp "$scratch/source-sa.json" "$scratch/source-sa-good.json"
  cp "$scratch/source-role.json" "$scratch/source-role-good.json"
  cp "$scratch/source-binding.json" "$scratch/source-binding-good.json"
  for mutation in binding-role-kind binding-subject binding-empty-uid role-api-group role-empty-uid sa-cluster-uid sa-managed-by sa-cluster-name sa-access-contract sa-access-lifecycle sa-empty-uid; do
    case "$mutation" in
      binding-role-kind) jq '.roleRef.kind="ClusterRole"' "$scratch/source-binding-good.json" > "$scratch/source-binding.json" ;;
      binding-subject) jq '.subjects[0].name="other"' "$scratch/source-binding-good.json" > "$scratch/source-binding.json" ;;
      binding-empty-uid) jq '.metadata.uid=""' "$scratch/source-binding-good.json" > "$scratch/source-binding.json" ;;
      role-api-group) jq '.rules[0].apiGroups=[""]' "$scratch/source-role-good.json" > "$scratch/source-role.json" ;;
      role-empty-uid) jq '.metadata.uid=""' "$scratch/source-role-good.json" > "$scratch/source-role.json" ;;
      sa-cluster-uid) jq '.metadata.labels["rhiza.mrchypark.dev/cluster-uid"]="other"' "$scratch/source-sa-good.json" > "$scratch/source-sa.json" ;;
      sa-managed-by) jq '.metadata.labels["app.kubernetes.io/managed-by"]="other"' "$scratch/source-sa-good.json" > "$scratch/source-sa.json" ;;
      sa-cluster-name) jq '.metadata.labels["rhiza.mrchypark.dev/cluster-name"]="other"' "$scratch/source-sa-good.json" > "$scratch/source-sa.json" ;;
      sa-access-contract) jq '.metadata.labels["rhiza.mrchypark.dev/access-contract"]="other"' "$scratch/source-sa-good.json" > "$scratch/source-sa.json" ;;
      sa-access-lifecycle) jq '.metadata.labels["rhiza.mrchypark.dev/access-lifecycle"]="other"' "$scratch/source-sa-good.json" > "$scratch/source-sa.json" ;;
      sa-empty-uid) jq '.metadata.uid=""' "$scratch/source-sa-good.json" > "$scratch/source-sa.json" ;;
    esac
    : > "$scratch/access-delete-order.txt"
    if delete_test_owned_managed_access "$uid"; then exit 1; fi
    [ ! -s "$scratch/access-delete-order.txt" ]
    cp "$scratch/source-sa-good.json" "$scratch/source-sa.json"
    cp "$scratch/source-role-good.json" "$scratch/source-role.json"
    cp "$scratch/source-binding-good.json" "$scratch/source-binding.json"
  done
  jq -n '{kind:"RhizaCluster",metadata:{name:"sql",namespace:"test-ns",uid:"forged-uid",labels:{"rhiza-ci-owner":"harness","rhiza-ci-run":"forged"}}}' > "$scratch/residual-candidate.json"
  hn() {
    case "$1 $2" in
      'get rhizacluster') cat "$scratch/residual-candidate.json" ;;
      *) return 1 ;;
    esac
  }
  : > "$scratch/direct-overlay-created.tsv"
  printf 'sql\n' > "$scratch/direct-cluster-create-attempted.previous-attempt"
  if report_unrecorded_cluster; then exit 1; fi
  grep -F 'unrecorded RhizaCluster sql' "$scratch/residual-resources.txt" >/dev/null
  printf 'sql\n' > "$cluster_create_attempt"
  if report_unrecorded_cluster; then exit 1; fi
  grep -F 'create was attempted without a recorded UID' "$scratch/residual-resources.txt" >/dev/null
  printf 'RhizaCluster\tsql\tforged-other-uid\n' > "$scratch/direct-overlay-created.tsv"
  if report_unrecorded_cluster; then exit 1; fi
  grep -F 'recorded_uid=forged-other-uid' "$scratch/residual-resources.txt" >/dev/null
  : > "$scratch/direct-overlay-created.tsv"
  hn() { printf '%s\n' 'Error from server (NotFound)' >&2; return 1; }
  if report_unrecorded_cluster; then exit 1; fi
  grep -F 'CR is now absent' "$scratch/residual-resources.txt" >/dev/null
  rm -f "$cluster_create_attempt"
  report_unrecorded_cluster
  [ ! -e "$cluster_create_attempt" ]
  [ -f "$scratch/direct-cluster-create-attempted.previous-attempt" ]
  uid=owned-uid
  order=
  delete_test_owned_managed_access() { order="$order access"; }
  force_release_test_owned_managed_finalizer() { order="$order finalizer"; }
  hn() {
    case "$1 $2" in
      'get rhizacluster') cat "$scratch/source-cluster.json" ;;
      "delete --raw") order="$order delete" ;;
      'wait --for=delete') order="$order wait" ;;
      *) return 1 ;;
    esac
  }
  printf 'RhizaCluster\tsql\t%s\n' "$uid" > "$scratch/direct-overlay-created.tsv"
  delete_direct_overlay
  [ "$order" = ' delete access finalizer wait' ]
  jq '.metadata.resourceVersion="8" | .status.authority.pendingWrite=true' "$scratch/source-cluster.json" > "$scratch/status-after-access.json"
  : > "$scratch/direct-overlay-created.tsv"
  printf 'RhizaCluster\tsql\t%s\n' "$uid" >> "$scratch/direct-overlay-created.tsv"
  order=
  gets=0
  hn() {
    case "$1 $2" in
      'get rhizacluster')
        gets=$((gets+1))
        if [ "$gets" = 1 ]; then cat "$scratch/source-cluster.json"; else cat "$scratch/status-after-access.json"; fi
        ;;
      "delete --raw") order="$order delete" ;;
      'wait --for=delete') order="$order wait" ;;
      *) return 1 ;;
    esac
  }
  if delete_direct_overlay; then exit 1; fi
  [ "$order" = ' delete access' ]
  jq '.status.repair.state="Preparing"' "$scratch/source-cluster.json" > "$scratch/status-preparing.json"
  : > "$scratch/direct-overlay-created.tsv"
  printf 'RhizaCluster\tsql\t%s\n' "$uid" >> "$scratch/direct-overlay-created.tsv"
  order=
  hn() {
    case "$1 $2" in
      'get rhizacluster') cat "$scratch/status-preparing.json" ;;
      "delete --raw") order="$order delete" ;;
      'wait --for=delete') order="$order wait" ;;
      *) return 1 ;;
    esac
  }
  if delete_direct_overlay; then exit 1; fi
  [ "$order" = ' delete' ]
  order=
  delete_test_owned_managed_access() { return 1; }
  if delete_direct_overlay; then exit 1; fi
  [ "$order" = ' delete' ]
  namespace=test-ns cr=sql uid=owned-uid
  release=test-release
  attempt_id=current-attempt
  cluster_name_label=sql
  sa="rhiza-managed-host-$(printf '%s\000%s\000%s' "$namespace" "$cr" "$uid" | sha256sum | cut -c1-16)"
  binding="$sa-binding"
  deployment="rhiza-$(printf '%s' "$uid" | sha256sum | cut -c1-20)-g0"
  readme_function=$(sed -n '/^delete_owned_access() {/,/^}/p' deploy/operator/README.md)
  [ -n "$readme_function" ]
  eval "$readme_function"
  kubectl() {
    case "$readme_mode $1" in
      'absent-before get') printf '%s\n' 'Error from server (NotFound)' >&2; return 1 ;;
      'forbidden-before get') printf '%s\n' 'Error from server (Forbidden)' >&2; return 1 ;;
      'positive get')
        readme_gets=$((readme_gets+1))
        if [ "$readme_gets" = 1 ]; then cat "$scratch/source-binding-good.json"; else printf '%s\n' 'Error from server (NotFound)' >&2; return 1; fi
        ;;
      'post-network get')
        readme_gets=$((readme_gets+1))
        if [ "$readme_gets" = 1 ]; then cat "$scratch/source-binding-good.json"; else printf '%s\n' 'connection refused' >&2; return 1; fi
        ;;
      positive\ delete|post-network\ delete) printf 'delete\n' >> "$scratch/readme-deletes.txt" ;;
      *) return 1 ;;
    esac
  }
  : > "$scratch/readme-deletes.txt"
  readme_mode=absent-before readme_gets=0
  delete_owned_access rolebinding "$binding"
  [ ! -s "$scratch/readme-deletes.txt" ]
  readme_mode=forbidden-before readme_gets=0
  if delete_owned_access rolebinding "$binding"; then exit 1; fi
  [ ! -s "$scratch/readme-deletes.txt" ]
  readme_mode=positive readme_gets=0
  delete_owned_access rolebinding "$binding"
  [ "$readme_gets" = 2 ] && [ "$(wc -l < "$scratch/readme-deletes.txt" | tr -d ' ')" = 1 ]
  readme_mode=post-network readme_gets=0
  if delete_owned_access rolebinding "$binding"; then exit 1; fi
  [ "$readme_gets" = 2 ] && [ "$(wc -l < "$scratch/readme-deletes.txt" | tr -d ' ')" = 2 ]
  printf '%s\n' 'PASS: exact overlay projection, UID-conditioned deletion, atomic finalizer release and owned access cleanup'
  exit 0
fi
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
sh "$0" --overlay-self-test
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
grep -Fq 'COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt' Dockerfile.managed-ffi-contract
if grep -Eq '^RUN .*apt-get' Dockerfile.managed-ffi-contract; then echo 'managed contract image requires package-repository DNS' >&2; exit 1; fi
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
