#!/bin/sh
# Direct-host managed HA qualification, restricted to a pre-created namespace.
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
execution_mode=${RHIZA_CHAOS_EXECUTION_MODE:-github-actions}
case "$execution_mode" in
  github-actions) [ "${GITHUB_ACTIONS-}" = true ] || { echo 'GitHub Actions required for github-actions mode' >&2; exit 1; } ;;
  local)
    [ "${GITHUB_ACTIONS-}" != true ] || { echo 'local mode cannot run in GitHub Actions' >&2; exit 1; }
    : "${RHIZA_LOCAL_RUN_ID:?}"
    case "$RHIZA_LOCAL_RUN_ID" in ''|*[!0-9]*) echo 'local run ID must contain only digits' >&2; exit 1 ;; esac
    [ "${#RHIZA_LOCAL_RUN_ID}" -le 34 ]
    # The shared fixture uses these names for unique bucket naming, not CI authorization.
    GITHUB_RUN_ID=$RHIZA_LOCAL_RUN_ID
    GITHUB_RUN_ATTEMPT=1
    GITHUB_SHA=$(git rev-parse HEAD)
    ;;
  *) echo 'unsupported Chaos execution mode' >&2; exit 1 ;;
esac
: "${RUNNER_TEMP:?}" "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}" "${GITHUB_SHA:?}"
: "${RHIZA_HOST_KUBECONFIG:?}" "${RHIZA_HOST_CONTEXT:?}" "${RHIZA_HOST_NAMESPACE:?}"
: "${RHIZA_HOST_NAMESPACE_UID:?}" "${RHIZA_HOST_SERVICEACCOUNT_UID:?}" "${RHIZA_HOST_NODE:?}"
: "${RHIZA_RELEASE_IMAGE:?}" "${RHIZA_MANAGED_TEST_DOCKERFILE:?}"
[ "$RHIZA_MANAGED_TEST_DOCKERFILE" = Dockerfile.managed-ffi-contract ] || { echo 'direct-host requires managed-ffi-contract image' >&2; exit 1; }
[ "${RHIZA_MANAGED_FIXTURE_CASE:-internal}" = internal ] || { echo 'direct-host qualifies only the internal profile' >&2; exit 1; }
command -v yq >/dev/null || { echo 'direct-host requires the pinned YAML renderer' >&2; exit 1; }
printf '%s\n' "$RHIZA_RELEASE_IMAGE" | grep -Eq '^ghcr\.io/mrchypark/rhiza-operator@sha256:[0-9a-f]{64}$'
printf '%s\n' "$RHIZA_HOST_NAMESPACE_UID" | grep -Eq '^[0-9a-fA-F-]{16,64}$'
printf '%s\n' "$RHIZA_HOST_NODE" | grep -Eq '^[a-z0-9][a-z0-9.-]{0,252}$'
case "$RHIZA_HOST_NAMESPACE" in rhiza-ha-chaos-*) ;; *) echo 'isolated test namespace required' >&2; exit 1;; esac
logs="$RUNNER_TEMP/ha-managed-cluster-chaos"
mkdir -p "$logs"
printf '%s\n' "$execution_mode" "$GITHUB_SHA" "$GITHUB_RUN_ID" "$GITHUB_RUN_ATTEMPT" > "$logs/execution-source.txt"
private=$(mktemp -d "$RUNNER_TEMP/ha-managed-direct-host-private.XXXXXX")
chmod 700 "$private"
nonce=$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')
attempt_id=$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')
case "$attempt_id" in ''|*[!0-9a-f]*) echo 'attempt identity unavailable' >&2; exit 1;; esac
[ "${#attempt_id}" -eq 24 ] || { echo 'attempt identity must contain 24 hex characters' >&2; exit 1; }
namespace=$RHIZA_HOST_NAMESPACE
release="rhiza-direct-chaos-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
image=$RHIZA_RELEASE_IMAGE
bucket_pid=
fault_name=
owned=0
cluster_create_attempt="$logs/direct-cluster-create-attempted.$attempt_id"
printf '%s\t%s\n' "$release" "$attempt_id" > "$logs/direct-cluster-attempt-id.txt"
# Exact names from the internal fixture, including both replacement phases.
fixture_jobs='create-sql insert-sql query mixed-prefault query-after-replacement mixed-first-replacement query-after-second-replacement mixed-second-replacement mixed-restart query-after-restart insert-after-chaos query-after-chaos'
h() { kubectl --kubeconfig "$RHIZA_HOST_KUBECONFIG" --context "$RHIZA_HOST_CONTEXT" --request-timeout=20s "$@"; }
hn() { h -n "$namespace" "$@"; }
k() { hn "$@"; }
host_guard() {
  [ "$(hn get namespace "$namespace" -o jsonpath='{.metadata.uid}')" = "$RHIZA_HOST_NAMESPACE_UID" ] &&
    [ "$(hn get serviceaccount rhiza-chaos-runner -o jsonpath='{.metadata.uid}')" = "$RHIZA_HOST_SERVICEACCOUNT_UID" ]
}
assert_names_absent() {
  resource=$1
  shift
  case "$resource" in rhizaclusters|deployments|services|serviceaccounts|secrets|roles|rolebindings|jobs|podchaos) ;; *) return 2 ;; esac
  [ "$#" -gt 0 ] || return 2
  list_json="$private/preflight-list.json"
  if ! hn get "$resource" -o json > "$list_json"; then
    echo "failed to list namespaced $resource during preflight" >&2
    return 1
  fi
  names_json=$(printf '%s\n' "$@" | jq -R . | jq -s .)
  jq -e --argjson names "$names_json" '.items|all(.[]; .metadata.name as $name | ($names|index($name))==null)' "$list_json" >/dev/null || {
    echo "preflight found an existing fixture name in $resource" >&2
    return 1
  }
}
project_direct_overlay() {
  jq -e '
    ([.items[] | [.kind,.metadata.name] | join("/")] | sort) as $names |
    if $names != (["Deployment/rhiza-operator","Role/rhiza-operator","RoleBinding/rhiza-operator","ServiceAccount/rhiza-operator"]|sort) then error("unexpected direct-host overlay scope") else . end |
    (.items[] | select(.kind=="Role" and .metadata.name=="rhiza-operator") | .rules) |= map(select(.resources!=["rhizarecoveries"] and .resources!=["rhizarecoveries/status"])) |
    if any(.items[] | select(.kind=="Role") | .rules[] | .resources[]; startswith("rhizarecoveries")) then error("unexpected recovery rule") else . end
  ' "$1"
}
direct_overlay_path() {
  case "$1/$2" in
    RhizaCluster/sql) api=apis/rhiza.mrchypark.dev/v1alpha2 resource=rhizaclusters ;;
    Deployment/rhiza-operator) api=apis/apps/v1 resource=deployments ;;
    Role/rhiza-operator) api=apis/rbac.authorization.k8s.io/v1 resource=roles ;;
    RoleBinding/rhiza-operator) api=apis/rbac.authorization.k8s.io/v1 resource=rolebindings ;;
    ServiceAccount/rhiza-operator) api=api/v1 resource=serviceaccounts ;;
    *) return 1 ;;
  esac
  printf '/%s/namespaces/%s/%s/%s\n' "$api" "$namespace" "$resource" "$2"
}
create_direct_cluster() {
  host_guard || return 1
  printf 'sql\n' > "$cluster_create_attempt" || return 1
  hn create --validate=strict -f "$1" -o json > "$private/overlay-create.json" || return 1
  uid=$(jq -er --arg ns "$namespace" 'select(.kind=="RhizaCluster" and .metadata.namespace==$ns and .metadata.name=="sql") | .metadata.uid | select(type=="string" and length>0)' "$private/overlay-create.json") || return 1
  printf 'RhizaCluster\tsql\t%s\n' "$uid" >> "$logs/direct-overlay-created.tsv"
}
report_unrecorded_cluster() {
  recorded_uid=$(awk -F '\t' '$1=="RhizaCluster" && $2=="sql" {print $3}' "$logs/direct-overlay-created.tsv" 2>/dev/null || true)
  if ! hn get rhizacluster sql -o json > "$private/residual-cluster.json" 2>"$private/residual-cluster.err"; then
    if grep -F NotFound "$private/residual-cluster.err" >/dev/null; then
      if [ -n "$recorded_uid" ] || [ ! -f "$cluster_create_attempt" ]; then return 0; fi
      printf 'FAIL: CR create was attempted without a recorded UID and the CR is now absent; per-CR access remnants cannot be located safely. automatic cleanup skipped\n' > "$logs/residual-resources.txt"
      return 1
    fi
    return 1
  fi
  live_uid=$(jq -er '.metadata.uid | select(type=="string" and length>0)' "$private/residual-cluster.json") || return 1
  if [ -z "$recorded_uid" ] && [ ! -f "$cluster_create_attempt" ]; then
    printf 'FAIL: unrecorded RhizaCluster sql exists without a create-attempt record live_uid=%s; automatic CR/access deletion skipped\n' "$live_uid" > "$logs/residual-resources.txt"
    return 1
  fi
  if [ -z "$recorded_uid" ]; then
    printf 'FAIL: CR create was attempted without a recorded UID live_uid=%s; automatic CR/access deletion skipped\n' "$live_uid" > "$logs/residual-resources.txt"
    return 1
  fi
  note="unrecorded RhizaCluster sql requires manual cleanup"
  [ "$live_uid" = "$recorded_uid" ] && note="recorded RhizaCluster sql cleanup failed; CR and access may remain"
  printf 'FAIL: %s live_uid=%s recorded_uid=%s; automatic CR/access deletion skipped\n' "$note" "$live_uid" "${recorded_uid:-missing}" > "$logs/residual-resources.txt"
  return 1
}
managed_cleanup_status_safe() {
  jq -e '
    (has("status") and (.status|type)=="object" and (.status|length)>0) and
    (.status.authority|type)=="object" and
    .status.authority.pendingWrite==false and
    ((.status.authority.transition // null)==null) and
    ((.status.authority.pendingRequest // null)==null) and
    ((.status.replicaStep // null)==null) and
    ((.status.replicaCheck // null)==null) and
    (((.status.recoveryQueue // null)==null) or
      ((.status.recoveryQueue|type)=="object" and (.status.recoveryQueue|length)==0)) and
    ((.status.repair // null)==null or
      ((.status.repair|type)=="object" and
       (.status.repair.state=="Completed" or .status.repair.state=="Aborted") and
       (.status.repair.completion|type)=="object")) and
    ((.status|has("pendingWrite")|not) or .status.pendingWrite==false)
  ' "$1" >/dev/null
}
force_release_test_owned_managed_finalizer() {
  kind=$1 name=$2 created_uid=$3 snapshot=$4
  [ "$kind" = RhizaCluster ] || return 0
  [ -n "$created_uid" ] && [ -f "$snapshot" ] || return 1
  [ "$(jq -er '.metadata.uid' "$snapshot")" = "$created_uid" ] || return 1
  [ "$(jq -er '.metadata.resourceVersion | select(type=="string" and length>0)' "$snapshot")" ] || return 1
  [ "$(jq -er '.metadata.deletionTimestamp | select(type=="string" and length>0)' "$snapshot")" ] || return 1
  jq -e --arg f rhiza.mrchypark.dev/managed-host-cleanup '((.metadata.finalizers // [])|index($f))!=null' "$snapshot" >/dev/null || return 1
  managed_cleanup_status_safe "$snapshot" || return 1
  finalizer_patch=$(jq -c --arg uid "$created_uid" --arg rv "$(jq -er .metadata.resourceVersion "$snapshot")" --arg f rhiza.mrchypark.dev/managed-host-cleanup '
    (.metadata.deletionTimestamp | select(type=="string" and length>0)) as $deletion |
    (.metadata.finalizers | index($f)) as $i |
    if $i == null then error("cleanup finalizer absent") else
    [
      {op:"test",path:"/metadata/uid",value:$uid},
      {op:"test",path:"/metadata/resourceVersion",value:$rv},
      {op:"test",path:"/metadata/deletionTimestamp",value:$deletion},
      {op:"test",path:"/status",value:.status},
      {op:"test",path:"/metadata/finalizers",value:.metadata.finalizers},
      {op:"test",path:"/metadata/finalizers/\($i)",value:$f},
      {op:"replace",path:"/metadata/finalizers",value:[.metadata.finalizers[]|select(.!=$f)]}
    ] end
  ' "$snapshot") || return 1
  hn patch rhizacluster "$name" --type=json -p "$finalizer_patch"
}
delete_test_owned_managed_access() {
  created_uid=${1-}
  [ -n "${uid-}" ] && [ "$created_uid" = "$uid" ] || return 1
  sa="rhiza-managed-host-$(printf '%s\000%s\000%s' "$namespace" sql "$uid" | sha256sum | cut -c1-16)"
  deployment="rhiza-$(printf '%s' "$uid" | sha256sum | cut -c1-20)-g0"
  present_sa=0 present_role=0 present_binding=0
  if hn get serviceaccount "$sa" -o json > "$private/test-owned-sa.json" 2>"$private/test-owned-sa.err"; then present_sa=1; elif ! grep -F NotFound "$private/test-owned-sa.err" >/dev/null; then return 1; fi
  if hn get role "$sa" -o json > "$private/test-owned-role.json" 2>"$private/test-owned-role.err"; then present_role=1; elif ! grep -F NotFound "$private/test-owned-role.err" >/dev/null; then return 1; fi
  if hn get rolebinding "$sa-binding" -o json > "$private/test-owned-binding.json" 2>"$private/test-owned-binding.err"; then present_binding=1; elif ! grep -F NotFound "$private/test-owned-binding.err" >/dev/null; then return 1; fi
  for object in sa role binding; do
    eval "present=\$present_$object"
    [ "$present" = 1 ] || continue
    file="$private/test-owned-$object.json"
    jq -e --arg uid "$uid" --arg namespace "$namespace" --arg name "$sa" --arg binding "$sa-binding" --arg deployment "$deployment" --arg kind "$object" '
      .metadata.namespace==$namespace and
      .metadata.labels["app.kubernetes.io/managed-by"]=="rhiza-operator" and
      .metadata.labels["rhiza.mrchypark.dev/cluster-uid"]==$uid and
      .metadata.labels["rhiza.mrchypark.dev/cluster-name"]=="sql" and
      .metadata.labels["rhiza.mrchypark.dev/access-contract"]=="v1alpha2-host-v1" and
      .metadata.labels["rhiza.mrchypark.dev/access-lifecycle"]=="retain-until-manual-cleanup" and
      (.metadata.uid|type=="string" and length>0) and
      if $kind=="sa" then .kind=="ServiceAccount" and .metadata.name==$name
      elif $kind=="role" then .kind=="Role" and .metadata.name==$name and
        .rules==[
          {apiGroups:["rhiza.mrchypark.dev"],resources:["rhizaclusters"],resourceNames:["sql"],verbs:["get"]},
          {apiGroups:["rhiza.mrchypark.dev"],resources:["rhizaclusters/status"],resourceNames:["sql"],verbs:["get","update"]},
          {apiGroups:[""],resources:["pods"],verbs:["get","list"]},
          {apiGroups:["apps"],resources:["deployments"],resourceNames:[$deployment],verbs:["get"]},
          {apiGroups:["apps"],resources:["replicasets"],verbs:["get"]}
        ]
      else .kind=="RoleBinding" and .metadata.name==$binding and
        .roleRef=={apiGroup:"rbac.authorization.k8s.io",kind:"Role",name:$name} and
        .subjects==[{kind:"ServiceAccount",name:$name,namespace:$namespace}]
      end
    ' "$file" >/dev/null || return 1
  done
  for spec in "rolebindings/$sa-binding" "roles/$sa" "serviceaccounts/$sa"; do
    resource=${spec%%/*}
    name=${spec#*/}
    file="$private/test-owned-binding.json"
    case "$resource" in roles) file="$private/test-owned-role.json" ;; serviceaccounts) file="$private/test-owned-sa.json" ;; esac
    case "$resource" in rolebindings) [ "$present_binding" = 1 ] || continue ;; roles) [ "$present_role" = 1 ] || continue ;; serviceaccounts) [ "$present_sa" = 1 ] || continue ;; esac
    object_uid=$(jq -er '.metadata.uid' "$file") || return 1
    api=apis/rbac.authorization.k8s.io/v1
    [ "$resource" != serviceaccounts ] || api=api/v1
    jq -nc --arg uid "$object_uid" '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid}}' > "$private/test-owned-delete.json" || return 1
    hn delete --raw "/$api/namespaces/$namespace/$resource/$name" -f "$private/test-owned-delete.json" || return 1
  done
}
create_direct_overlay() {
  host_guard || return 1
  jq -ce '.items[]' "$1" > "$private/direct-overlay.jsonl" || return 1
  while IFS= read -r item; do
    kind=$(printf '%s\n' "$item" | jq -er .kind) || return 1
    name=$(printf '%s\n' "$item" | jq -er .metadata.name) || return 1
    direct_overlay_path "$kind" "$name" >/dev/null || return 1
    printf '%s\n' "$item" | hn create --validate=strict -f - -o json > "$private/overlay-create.json" || return 1
    jq -er --arg kind "$kind" --arg name "$name" --arg ns "$namespace" '
      select(.kind==$kind and .metadata.name==$name and .metadata.namespace==$ns) |
      [.kind,.metadata.name,(.metadata.uid|select(type=="string" and length>0))] | select(length==3) | @tsv
    ' "$private/overlay-create.json" >> "$logs/direct-overlay-created.tsv" || return 1
  done < "$private/direct-overlay.jsonl"
}
restore_direct_overlay_status() {
  host_guard || return 1
  role_uid=$(awk -F '\t' '$1=="Role" && $2=="rhiza-operator" {print $3}' "$logs/direct-overlay-created.tsv") || return 1
  [ -n "$role_uid" ] || return 1
  hn get role rhiza-operator -o json > "$private/overlay-create.json" || return 1
  jq -ce --arg uid "$role_uid" --slurpfile overlay "$1" '
    select(.metadata.uid==$uid and (.metadata.resourceVersion|type=="string" and length>0)) |
    .rules=($overlay[0].items[] | select(.kind=="Role" and .metadata.name=="rhiza-operator") | .rules)
  ' "$private/overlay-create.json" > "$private/direct-status-update.json" || return 1
  hn replace --validate=strict -f "$private/direct-status-update.json"
}
delete_direct_overlay() {
  [ -f "$logs/direct-overlay-created.tsv" ] || return 0
  delete_result=0
  while IFS="$(printf '\t')" read -r kind name created_uid; do
    if [ -z "$created_uid" ] || ! url=$(direct_overlay_path "$kind" "$name"); then delete_result=1; continue; fi
    jq -nc --arg uid "$created_uid" '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid},propagationPolicy:"Foreground"}' > "$private/overlay-delete.json" || return 1
    if [ "$kind" = RhizaCluster ]; then
      if ! hn delete --raw "$url" -f "$private/overlay-delete.json"; then delete_result=1; continue; fi
      if ! hn get rhizacluster "$name" -o json > "$private/test-owned-cluster.json"; then delete_result=1; continue; fi
      [ "$(jq -er '.metadata.uid' "$private/test-owned-cluster.json")" = "$created_uid" ] || { delete_result=1; continue; }
      managed_cleanup_status_safe "$private/test-owned-cluster.json" || { delete_result=1; continue; }
      if ! delete_test_owned_managed_access "$created_uid"; then delete_result=1; continue; fi
      if ! hn get rhizacluster "$name" -o json > "$private/test-owned-cluster-after-access.json"; then delete_result=1; continue; fi
      if [ "$(jq -S -c '{uid:.metadata.uid,rv:.metadata.resourceVersion,status:(.status // null)}' "$private/test-owned-cluster.json")" != "$(jq -S -c '{uid:.metadata.uid,rv:.metadata.resourceVersion,status:(.status // null)}' "$private/test-owned-cluster-after-access.json")" ]; then delete_result=1; continue; fi
      if ! force_release_test_owned_managed_finalizer "$kind" "$name" "$created_uid" "$private/test-owned-cluster.json"; then delete_result=1; continue; fi
    else
      if ! hn delete --raw "$url" -f "$private/overlay-delete.json"; then delete_result=1; continue; fi
    fi
    case "$kind" in
      Deployment|RhizaCluster) hn wait --for=delete "$kind/$name" --timeout=45s || delete_result=1 ;;
    esac
  done < "$logs/direct-overlay-created.tsv"
  return "$delete_result"
}
capture_managed_logs() {
  pods="$private/pods.json"
  hn get pods -o json > "$pods" 2>/dev/null || return 0
  jq -r '.items[] as $pod | ($pod.spec.containers[]? | select(.name=="sql") | [$pod.metadata.name,.name]), ($pod.spec.initContainers[]? | select(.name=="rhiza-prepare-identity" or .name=="rhiza-await-bootstrap-plan") | [$pod.metadata.name,.name]) | @tsv' "$pods" 2>/dev/null |
  while IFS="$(printf '\t')" read -r pod container; do
    case "$pod" in *[!a-z0-9.-]*|'') continue ;; esac
    case "$container" in sql|rhiza-prepare-identity|rhiza-await-bootstrap-plan) ;; *) continue ;; esac
    for version in current previous; do
      if [ "$version" = previous ]; then
        hn logs "$pod" -c "$container" --previous --tail=200 --limit-bytes=65536 > "$logs/$container-$pod-previous.log" 2>/dev/null || :
      else
        hn logs "$pod" -c "$container" --tail=200 --limit-bytes=65536 > "$logs/$container-$pod-current.log" 2>/dev/null || :
      fi
    done
  done
  hn logs deployment/rhiza-operator -c operator --tail=200 --limit-bytes=65536 > "$logs/operator-current.log" 2>/dev/null || :
  hn logs deployment/floci -c floci --tail=100 --limit-bytes=32768 > "$logs/floci-current.log" 2>/dev/null || :
  rm -f "$pods"
}
cleanup() {
  result=$?
  original_result=$result
  printf '%s\n' "$original_result" > "$logs/driver-original.exit"
  trap - EXIT HUP INT TERM
  if [ "$owned" = 1 ] && host_guard; then
    capture_managed_logs || :
    if [ -n "$fault_name" ]; then
      if [ "$result" -ne 0 ]; then capture_chaos_failure "$fault_name" cleanup || :; fi
      hn delete podchaos --field-selector "metadata.name=$fault_name" -l "rhiza-ci-run=$release" --wait=true --timeout=45s > "$logs/fault-cleanup.txt" 2>&1 || result=1
    fi
    delete_direct_overlay > "$logs/overlay-cleanup.txt" 2>&1 || result=1
    report_unrecorded_cluster > "$logs/residual-cluster-check.txt" 2>&1 || result=1
    if [ -n "${uid-}" ]; then hn delete pods,deployments -l "rhiza.mrchypark.dev/cluster-uid=$uid" --wait=true --timeout=45s --ignore-not-found > "$logs/sql-workload-cleanup.txt" 2>&1 || result=1; fi
    hn delete deployment/floci service/floci service/bootstrap-client pod/floci-service-links-probe --wait=true --timeout=45s --ignore-not-found > "$logs/workload-cleanup.txt" 2>&1 || result=1
    # Keep deletion restricted to names preflighted in the exclusive namespace.
    for fixture_job in $fixture_jobs; do
      hn delete jobs --field-selector "metadata.name=$fixture_job" -l rhiza-ci-owner=harness --wait=true --timeout=45s --ignore-not-found >> "$logs/job-cleanup.txt" 2>&1 || result=1
    done
    # shellcheck disable=SC2086
    hn delete secret runtime $fixture_secrets --ignore-not-found > "$logs/secret-cleanup.txt" 2>&1 || result=1
    assert_names_absent deployments rhiza-operator || result=1
    assert_names_absent rhizaclusters sql || result=1
    for fixture_resource in roles rolebindings serviceaccounts; do
      assert_names_absent "$fixture_resource" rhiza-operator rhiza-managed-host || result=1
    done
    hn get pods -o json > "$logs/host-pods-after-cleanup.json" 2>&1 || result=1
    jq -e '.items|length==0' "$logs/host-pods-after-cleanup.json" >/dev/null || result=1
  elif [ "$owned" = 1 ]; then
    echo 'namespace identity changed; cleanup refused' > "$logs/cleanup-blocked.txt"
    result=1
  fi
  if [ -n "$bucket_pid" ]; then kill "$bucket_pid" 2>/dev/null || :; wait "$bucket_pid" 2>/dev/null || :; kill -0 "$bucket_pid" 2>/dev/null && result=1 || :; fi
  rm -f "$private/runtime.json" "$private/admin.token" "$private/curl.conf" "$private/request.body" "$private/forward.log" "$private/port-forward.log" "$private/preflight-list.json" "$RUNNER_TEMP/ha-managed-direct-host-bucket-setup"
  rm -f "$private/managed-install.jsonl" "$private/managed-install-direct.json" "$private/direct-overlay.jsonl" "$private/residual-cluster.json" "$private/residual-cluster.err" "$private/overlay-create.json" "$private/direct-status-update.json" "$private/overlay-delete.json" || result=1
  rm -f "$private/test-owned-cluster.json" "$private/test-owned-cluster-after-access.json" "$private/test-owned-cluster.err" "$private/test-owned-sa.json" "$private/test-owned-role.json" "$private/test-owned-binding.json" "$private/test-owned-delete.json" "$private/test-owned-sa.err" "$private/test-owned-role.err" "$private/test-owned-binding.err" || result=1
  rmdir "$private" || result=1
  [ "$original_result" -eq 0 ] || result=$original_result
  printf '%s\n' "$result" > "$logs/driver.exit"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
host_guard
printf '%s\n' "$RHIZA_HOST_NAMESPACE" "$RHIZA_HOST_NAMESPACE_UID" "$RHIZA_HOST_SERVICEACCOUNT_UID" > "$logs/host-scope.txt"
# API discovery proves that the preinstalled RhizaCluster v1alpha2 endpoint is
# served. This test never creates/patches CRDs and never asks for Recovery API.
h get --raw=/apis/rhiza.mrchypark.dev/v1alpha2 > "$logs/rhizacluster-api.json"
jq -e '.resources|any(.[]; .name=="rhizaclusters" and .kind=="RhizaCluster")' "$logs/rhizacluster-api.json" >/dev/null
[ "$(h auth can-i create customresourcedefinitions.apiextensions.k8s.io)" = no ]
[ "$(h auth can-i create namespaces)" = no ]
[ "$(hn auth can-i create podchaos.chaos-mesh.org)" = yes ]
hn get pods -o json > "$logs/host-pods-before.json"
jq -e '.items|length==0' "$logs/host-pods-before.json" >/dev/null
assert_names_absent rhizaclusters sql
assert_names_absent deployments rhiza-operator floci
assert_names_absent services floci bootstrap-client
assert_names_absent serviceaccounts rhiza-operator rhiza-managed-host
set --
for fixture_job in $fixture_jobs; do set -- "$@" "request-$fixture_job"; done
fixture_secrets="$*"
# shellcheck disable=SC2086
assert_names_absent secrets runtime $fixture_secrets
assert_names_absent roles rhiza-operator rhiza-managed-host
assert_names_absent rolebindings rhiza-operator rhiza-managed-host
# shellcheck disable=SC2086
assert_names_absent jobs $fixture_jobs
# Both pod kills reuse the exact name only after the first is confirmed absent.
assert_names_absent podchaos pod-kill container-kill
: > "$logs/direct-overlay-created.tsv"
owned=1
export RHIZA_MANAGED_FIXTURE_NODE=$RHIZA_HOST_NODE
managed_bucket_setup() {
  hn port-forward --address=127.0.0.1 service/floci :4566 > "$private/port-forward.log" 2>&1 &
  bucket_pid=$!
  tries=0
  while :; do
    port=$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9]*\) -> .*$/\1/p' "$private/port-forward.log" | head -1)
    [ -z "$port" ] || break
    kill -0 "$bucket_pid" 2>/dev/null || return 1
    tries=$((tries+1)); [ "$tries" -lt 30 ] || return 1; sleep 1
  done
  (cd scripts/ha-managed-bucket-setup-src && go build -mod=readonly -o "$RUNNER_TEMP/ha-managed-direct-host-bucket-setup" .)
  "$RUNNER_TEMP/ha-managed-direct-host-bucket-setup" "http://127.0.0.1:$port" "$bucket"
  kill "$bucket_pid"; wait "$bucket_pid" 2>/dev/null || :; bucket_pid=
}
capture_floci() { hn logs deployment/floci --tail=80 > "$logs/floci.log" 2>&1 || :; }
chaos_k() { hn "$@"; }
chaos_scope_guard() { host_guard; }
. scripts/ha-managed-podchaos.sh
managed_pod_fault() {
  hn exec "$1" -c sql -- sha256sum /var/lib/rhiza/data/qlog/voter-identity.json > "$logs/prior-voter-identity.txt"
  if [ -f "$logs/old-voter-identity.txt" ]; then
    managed_replacement_identity_oracle "$logs/old-voter-identity.txt" "$logs/prior-voter-identity.txt"
  fi
  cp "$logs/prior-voter-identity.txt" "$logs/old-voter-identity.txt"
  direct_chaos_fault pod-kill pod-kill "$1" "$2"
  if [ ! -f "$logs/first-pod-kill-request.json" ]; then
    for suffix in pod-before request injected pods-after before-delete after-delete; do
      cp "$logs/pod-kill-$suffix.json" "$logs/first-pod-kill-$suffix.json"
    done
    cp "$logs/old-voter-identity.txt" "$logs/first-old-voter-identity.txt"
  fi
}
managed_container_fault() {
  hn exec "$1" -c sql -- sha256sum /var/lib/rhiza/data/qlog/voter-identity.json > "$logs/replacement-voter-identity.txt"
  managed_replacement_identity_oracle "$logs/old-voter-identity.txt" "$logs/replacement-voter-identity.txt"
  direct_chaos_fault container-kill container-kill "$1" "$2"
  managed_chaos_verified=1
}
managed_verify_first_manual_pod_fault() {
  [ -f "$logs/first-pod-kill-after-delete.json" ] && [ ! -s "$logs/first-pod-kill-after-delete.json" ] &&
    jq -e --arg pod "$1" --arg uid "$2" '.metadata.name==$pod and .metadata.uid==$uid' "$logs/first-pod-kill-pod-before.json" >/dev/null &&
    jq -e --arg uid "$2" 'all(.items[]; .metadata.uid!=$uid)' "$logs/first-pod-kill-pods-after.json" >/dev/null || return 1
  printf '%s\n' 'First Pod kill: exact host Pod UID, scoped injection and Pod API absence; no host CRI proof.' > "$logs/manual-first-pod-fault.txt"
}
. scripts/ha-managed-bootstrap-fixture.sh
managed_manual_proof=direct-host
managed_fixture
run_client insert-after-chaos execute '{"request_id":"chaos-final-insert","sql":"INSERT INTO bootstrap_ci VALUES (2, '\''after-chaos'\'')"}'
run_client query-after-chaos query '{"sql":"SELECT id, value FROM bootstrap_ci ORDER BY id"}'
jq -e '.rows==[[1,"real-kubelet"],[2,"after-chaos"]]' "$logs/query-after-chaos.json" >/dev/null
printf '%s\n' 'PASS: direct-host managed Go/Rust FFI contract, exact-UID Chaos Mesh Pod replacement and same-Pod restart, certified replacement, NoPVC, ACK readback and new write' \
  'SCOPE: namespace-scoped preinstalled RhizaCluster v1alpha2 API, pinned in-namespace Floci S3 and Pod API/Chaos evidence; not operator CRD installation, host CRI, live GCS, network partition, physical fencing or performance qualification' > "$logs/result.txt"
