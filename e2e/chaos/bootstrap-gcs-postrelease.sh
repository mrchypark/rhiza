#!/bin/sh
# Administrative, one-run bootstrap. Default render is offline; never run in CI.
set -eu
# Never inherit shell tracing while processing freshly generated fixture bytes.
set +x
umask 077
die() { printf '%s\n' "$*" >&2; exit 1; }
mode=${1:-render}
case "$mode" in render|check-fixture|negative-node-fixture|apply|rollback) ;; *) die 'Usage: sh bootstrap-gcs-postrelease.sh render|check-fixture|negative-node-fixture|apply|rollback' ;; esac
: "${RHIZA_RUN_ID:?8 lowercase hexadecimal characters required}"
: "${RHIZA_WORKFLOW_SHA:?protected-merge workflow SHA required}"
: "${RHIZA_AUTH_EXPIRES:?UTC YYYY-MM-DDTHH:MM:SSZ required}"
printf '%s\n' "$RHIZA_RUN_ID" | LC_ALL=C grep -Eq '^[a-f0-9]{8}$' || die 'Invalid run ID'
printf '%s\n' "$RHIZA_WORKFLOW_SHA" | LC_ALL=C grep -Eq '^[a-f0-9]{40}$' || die 'Invalid workflow SHA'
printf '%s\n' "$RHIZA_AUTH_EXPIRES" | LC_ALL=C grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$' || die 'Invalid expiry'
project=patch2-the-new-era
number=602454948273
context=gke_patch2-the-new-era_asia-northeast3_ied-cluster
bucket=rhiza-v070-chaos-ied-20260811
ns=rhiza-v0190-20261008-$RHIZA_RUN_ID
prefix=postrelease/v0.19.0/$RHIZA_RUN_ID/
folder=gs://$bucket/$prefix
pool=rhiza-v0190-gcs-1008
ci=rhiza-v0190-ci-1008@$project.iam.gserviceaccount.com
data=rhiza-v0190-gcs-1008@$project.iam.gserviceaccount.com
cluster_role=rhiza19_${RHIZA_RUN_ID}_cluster
folder_role=rhiza19_${RHIZA_RUN_ID}_objects
owner=rhiza-postrelease-$RHIZA_RUN_ID-$RHIZA_WORKFLOW_SHA
expiry="title=rhiza-$RHIZA_RUN_ID-expiry,expression=request.time < timestamp('$RHIZA_AUTH_EXPIRES')"
cluster_condition="title=rhiza-$RHIZA_RUN_ID-cluster,expression=resource.type == 'container.googleapis.com/Cluster' && resource.name == 'projects/$project/locations/asia-northeast3/clusters/ied-cluster' && request.time < timestamp('$RHIZA_AUTH_EXPIRES')"
ci_member=principalSet://iam.googleapis.com/projects/$number/locations/global/workloadIdentityPools/$pool/attribute.repository_id/1295475703
data_member=serviceAccount:$project.svc.id.goog[$ns/rhiza-gcs]
oidc="assertion.repository_id == '1295475703' && assertion.repository_owner_id == '6179259' && assertion.ref == 'refs/heads/main' && assertion.event_name == 'workflow_dispatch' && assertion.workflow_ref == 'mrchypark/rhiza/.github/workflows/gcs-postrelease.yml@refs/heads/main' && assertion.workflow_sha == '$RHIZA_WORKFLOW_SHA' && assertion.environment == 'rhiza-gcs-postrelease'"
mapping='google.subject=assertion.sub,attribute.repository_id=assertion.repository_id,attribute.repository_owner_id=assertion.repository_owner_id,attribute.ref=assertion.ref,attribute.event_name=assertion.event_name,attribute.workflow_ref=assertion.workflow_ref,attribute.workflow_sha=assertion.workflow_sha,attribute.environment=assertion.environment'
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
render() {
    sed -e "s/@NAMESPACE@/$ns/g" -e "s/@RUN_ID@/$RHIZA_RUN_ID/g" -e "s/@CI_GSA@/$ci/g" -e "s/@POD_GSA@/$data/g" -e "s/@OWNER@/$owner/g" "$script_dir/gcs-postrelease-auth.yaml.in"
}
validate_fixture() {
    # Input is private stdin; output is only a boolean discarded locally.
    # No payload text is allowed into diagnostics on parse/schema failure.
    jq -e --arg ns "$ns" --arg run "$RHIZA_RUN_ID" --arg owner "$owner" '
      (.stringData.RHIZA_PEER_TOKENS|fromjson) as $tokens |
      (.stringData.RHIZA_CLUSTER_MEMBERS|fromjson) as $members |
      (.stringData.learner_member|fromjson) as $learner |
      .apiVersion=="v1" and .kind=="Secret" and .type=="Opaque" and .immutable==true and
      .metadata.name=="rhiza-qualification-auth" and .metadata.namespace==$ns and
      .metadata.labels["chaos.rhiza.io/run"]==$run and .metadata.annotations["rhiza.dev/auth-owner"]==$owner and
      (.stringData|keys)==["RHIZA_ADMIN_TOKEN","RHIZA_CLUSTER_MEMBERS","RHIZA_PEER_TOKENS","learner_member","learner_token"] and
      (.stringData.RHIZA_ADMIN_TOKEN|test("^[a-f0-9]{64}$")) and
      (.stringData.learner_token|test("^[a-f0-9]{64}$")) and
      ($tokens|keys)==["rhiza-voter-0","rhiza-voter-1","rhiza-voter-2"] and
      all($tokens[]; test("^[a-f0-9]{64}$")) and
      ([$tokens[],.stringData.RHIZA_ADMIN_TOKEN,.stringData.learner_token]|unique|length)==5 and
      ($members|length)==3 and
      all(range(0;3); . as $i |
        $members[$i].node_id==("rhiza-voter-"+($i|tostring)) and
        $members[$i].url==("http://rhiza-voter-"+($i|tostring)+".rhiza-peers:8080") and
        $members[$i].peer_url==("quic://rhiza-voter-"+($i|tostring)+".rhiza-peers:9090") and
        ($members[$i].public_key|test("^[A-Za-z0-9+/]{43}=$"))) and
      $learner.node_id=="rhiza-learner" and $learner.url=="http://rhiza-learner:8080" and
      $learner.peer_url=="quic://rhiza-learner:9090" and ($learner.public_key|test("^[A-Za-z0-9+/]{43}=$"))
    ' >/dev/null 2>/dev/null
}
if [ "$mode" = render ]; then
    render
    exit 0
fi
if [ "$mode" = check-fixture ]; then
    command -v jq >/dev/null || die 'Missing jq'
    validate_fixture || die 'Fixture schema/identity rejected (payload omitted)'
    printf '%s\n' 'Fixture schema and public identity fields accepted; KDF consistency needs generator tests.'
    exit 0
fi
if [ "$mode" = negative-node-fixture ]; then
    # Offline adversarial fixture. ONLY pipe to an authorized server DRY RUN;
    # never create or execute it. Approved affinity cannot redeem nodeName.
    command -v jq >/dev/null || die 'Missing jq'
    jq -n --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '{
      apiVersion:"v1",kind:"Pod",metadata:{name:"negative-unapproved-node",namespace:$ns,labels:{"chaos.rhiza.io/run":$run,app:"rhiza-voter"}},
      spec:{serviceAccountName:"rhiza-gcs",automountServiceAccountToken:false,
        nodeName:"gke-ied-cluster-unapproved-node",
        affinity:{nodeAffinity:{requiredDuringSchedulingIgnoredDuringExecution:{nodeSelectorTerms:[{matchExpressions:[{key:"kubernetes.io/hostname",operator:"In",values:["gke-ied-cluster-ied-data-nodepool-72484396-ra2i"]}]}]}}},
        securityContext:{runAsNonRoot:true,runAsUser:65532,seccompProfile:{type:"RuntimeDefault"}},
        restartPolicy:"Never",containers:[{name:"rhiza",image:("ghcr.io/mrchypark/rhiza-sql@sha256:"+([range(0;64)|"0"]|join(""))),
          securityContext:{allowPrivilegeEscalation:false,capabilities:{drop:["ALL"]}},
          resources:{requests:{cpu:"250m",memory:"256Mi","ephemeral-storage":"128Mi"},limits:{cpu:"1",memory:"512Mi","ephemeral-storage":"1Gi"}}}]
      }}'
    exit 0
fi
[ "${RHIZA_BOOTSTRAP_GO:-}" = I_APPROVE_NEW_SCOPE ] || die 'Administrative GO absent; no cloud calls made'
case "${RHIZA_COST_CAP_KRW:-}" in 10000) ;; *) die 'Explicit approved monetary cap of KRW 10000 required; no cloud calls made' ;; esac
: "${RHIZA_EXPECTED_ADMIN_ACCOUNT:?explicit nonempty administrative account required}"
: "${RHIZA_AUTH_STATE:?absolute private receipt directory required}"
case "$RHIZA_AUTH_STATE" in /*) ;; *) die 'State directory must be absolute' ;; esac
for cmd in gcloud kubectl jq gh shasum; do command -v "$cmd" >/dev/null || die "Missing $cmd"; done
k() { kubectl --context="$context" "$@"; }
g() { gcloud --project="$project" --quiet "$@"; }
identity=$(jq -nc --arg run "$RHIZA_RUN_ID" --arg sha "$RHIZA_WORKFLOW_SHA" --arg expiry "$RHIZA_AUTH_EXPIRES" --arg cap "$RHIZA_COST_CAP_KRW" --arg owner "$owner" '{run:$run,workflow_sha:$sha,expiry:$expiry,cost_cap_krw:$cap,owner:$owner}')
if [ "$mode" = apply ]; then
    # Refuse stale/long-lived grants before the first remote call.
    expires=$(date -u -j -f '%Y-%m-%dT%H:%M:%SZ' "$RHIZA_AUTH_EXPIRES" '+%s' 2>/dev/null || date -u -d "$RHIZA_AUTH_EXPIRES" '+%s')
    now=$(date -u '+%s')
    [ "$expires" -gt "$now" ] && [ "$expires" -le "$((now + 14400))" ] || die 'Expiry must be future and at most four hours away'
    : "${RHIZA_FIXTURE_GENERATOR:?reviewed local embedded-host binary required}"
    : "${RHIZA_FIXTURE_GENERATOR_SHA256:?reviewed binary SHA256 required}"
    case "$RHIZA_FIXTURE_GENERATOR" in /*) ;; *) die 'Generator must be an absolute binary path' ;; esac
    [ -f "$RHIZA_FIXTURE_GENERATOR" ] && [ -x "$RHIZA_FIXTURE_GENERATOR" ] || die 'Generator binary unavailable'
    printf '%s\n' "$RHIZA_FIXTURE_GENERATOR_SHA256" | LC_ALL=C grep -Eq '^[a-f0-9]{64}$' || die 'Invalid generator hash'
    hash=$(shasum -a 256 "$RHIZA_FIXTURE_GENERATOR" | awk '{print $1}')
    [ "$hash" = "$RHIZA_FIXTURE_GENERATOR_SHA256" ] || die 'Generator binary differs from reviewed hash'
    [ ! -e "$RHIZA_AUTH_STATE" ] || die 'Receipt directory exists; no resume or adoption'
    mkdir -m 700 "$RHIZA_AUTH_STATE"
    printf '%s\n' "$identity" > "$RHIZA_AUTH_STATE/identity.json"
    render > "$RHIZA_AUTH_STATE/auth.yaml"
else
    [ -f "$RHIZA_AUTH_STATE/identity.json" ] || die 'Missing positive ownership receipt'
    [ "$(jq -cS . "$RHIZA_AUTH_STATE/identity.json")" = "$(printf '%s\n' "$identity" | jq -cS .)" ] || die 'Receipt identity mismatch'
fi
# Receipts are local and private, not public artifacts. No shell tracing/tokens.
record() {
    receipt=$1; shift
    printf '%s\n' "$receipt" > "$RHIZA_AUTH_STATE/inflight"
    if "$@" > "$RHIZA_AUTH_STATE/$receipt.json" 2> "$RHIZA_AUTH_STATE/$receipt.stderr"; then
        printf '0\n' > "$RHIZA_AUTH_STATE/$receipt.exit"
    else
        rc=$?; printf '%s\n' "$rc" > "$RHIZA_AUTH_STATE/$receipt.exit"
        die "Stopped at $receipt (exit $rc); preserve receipts, resolve unknown outcome before rollback"
    fi
}
absent() {
    item=$1; shift
    if "$@" > "$RHIZA_AUTH_STATE/absent-$item.json" 2> "$RHIZA_AUTH_STATE/absent-$item.stderr"; then
        die "Collision: $item already exists; never adopt"
    else
        rc=$?
        printf '%s\n' "$rc" > "$RHIZA_AUTH_STATE/absent-$item.exit"
        grep -Eq 'NOT_FOUND|NotFound|not found|does not exist|notFound|One or more URLs matched no objects' "$RHIZA_AUTH_STATE/absent-$item.stderr" || die "Cannot prove $item absent; preserve diagnostic"
    fi
}
mark() { printf '%s\n' "$owner" > "$RHIZA_AUTH_STATE/$1.created"; }
owned() { [ -f "$RHIZA_AUTH_STATE/$1.created" ] && [ "$(cat "$RHIZA_AUTH_STATE/$1.created")" = "$owner" ]; }
if [ "$mode" = apply ]; then
    [ "$(g config get-value account 2>/dev/null)" = "$RHIZA_EXPECTED_ADMIN_ACCOUNT" ] || die 'Unexpected administrative account'
    record github-main gh api repos/mrchypark/rhiza/branches/main
    jq -e --arg sha "$RHIZA_WORKFLOW_SHA" '.protected == true and .commit.sha == $sha' "$RHIZA_AUTH_STATE/github-main.json" >/dev/null || die 'Workflow SHA is not the current protected main SHA'
    record github-workflow gh api "repos/mrchypark/rhiza/contents/.github/workflows/gcs-postrelease.yml?ref=$RHIZA_WORKFLOW_SHA"
    jq -e '.type == "file" and .sha != null' "$RHIZA_AUTH_STATE/github-workflow.json" >/dev/null || die 'Canonical workflow absent at frozen protected merge'
    record github-environment gh api repos/mrchypark/rhiza/environments/rhiza-gcs-postrelease
    jq -e '.deployment_branch_policy.protected_branches == true and any(.protection_rules[]; .type == "required_reviewers" and (.reviewers|length)>0)' "$RHIZA_AUTH_STATE/github-environment.json" >/dev/null || die 'Protected-branch environment with required reviewers must exist before bootstrap'
    record project-before g projects get-iam-policy "$project" --format=json
    record bucket-before g storage buckets get-iam-policy "gs://$bucket" --format=json
    absent namespace k get namespace "$ns" -o json
    absent fault-policy k get validatingadmissionpolicy "$ns-faults" -o json
    absent fault-binding k get validatingadmissionpolicybinding "$ns-faults" -o json
    absent runtime-policy k get validatingadmissionpolicy "$ns-runtime" -o json
    absent runtime-binding k get validatingadmissionpolicybinding "$ns-runtime" -o json
    absent ns-reader k get clusterrole "$ns-reader" -o json
    absent ns-reader-binding k get clusterrolebinding "$ns-reader" -o json
    absent pool g iam workload-identity-pools describe "$pool" --location=global --format=json
    absent ci g iam service-accounts describe "$ci" --format=json
    absent data g iam service-accounts describe "$data" --format=json
    absent cluster-role g iam roles describe "$cluster_role" --format=json
    absent folder-role g iam roles describe "$folder_role" --format=json
    absent folder g storage managed-folders describe "$folder" --raw --format=json
    # Exact new prefix only; authentication errors cannot masquerade as empty.
    absent prefix g storage ls "$folder**"
    record ci-create g iam service-accounts create rhiza-v0190-ci-1008 --description="$owner" --format=json
    mark ci
    record data-create g iam service-accounts create rhiza-v0190-gcs-1008 --description="$owner" --format=json
    mark data
    record cluster-role-create g iam roles create "$cluster_role" --title='Rhiza temporary cluster metadata' --description="$owner" --permissions=container.clusters.get --stage=GA --format=json
    mark cluster-role
    record folder-role-create g iam roles create "$folder_role" --title='Rhiza temporary folder objects' --description="$owner" --permissions=storage.objects.get,storage.objects.list,storage.objects.create,storage.objects.update,storage.objects.delete --stage=GA --format=json
    mark folder-role
    record pool-create g iam workload-identity-pools create "$pool" --location=global --description="$owner" --format=json
    mark pool
    record provider-create g iam workload-identity-pools providers create-oidc github --location=global --workload-identity-pool="$pool" --issuer-uri=https://token.actions.githubusercontent.com --attribute-mapping="$mapping" --attribute-condition="$oidc" --description="$owner" --format=json
    mark provider
    record folder-create g storage managed-folders create "$folder" --format=json
    mark folder
    record folder-identity g storage managed-folders describe "$folder" --raw --format=json
    jq -e '.id != null' "$RHIZA_AUTH_STATE/folder-identity.json" >/dev/null || die 'Managed folder immutable identity missing; stop before grants'
    record folder-before g storage managed-folders get-iam-policy "$folder" --format=json
    record ci-wi-before g iam service-accounts get-iam-policy "$ci" --format=json
    record data-wi-before g iam service-accounts get-iam-policy "$data" --format=json
    # Bootstrap namespace and admission BEFORE any CI runtime access.
    # No apply/adoption: a collision fails. Partial kubectl creation needs audit.
    record kubernetes-create k create -f "$RHIZA_AUTH_STATE/auth.yaml" -o json
    mark kubernetes
    record namespace-identity k get namespace "$ns" -o json
    for kind in validatingadmissionpolicy validatingadmissionpolicybinding; do
        for suffix in faults runtime; do
            record "$kind-$suffix-identity" k get "$kind" "$ns-$suffix" -o json
        done
    done
    for kind in clusterrole clusterrolebinding; do
        record "$kind-reader-identity" k get "$kind" "$ns-reader" -o json
    done
    # Existing host's fixture mode uses crypto/rand and rhiza.PeerPublicKey.
    # Only public namespace/run/owner are argv; no token bytes in CLI arguments.
    fixture_dir=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-fixture.XXXXXXXX")
    chmod 700 "$fixture_dir"
    clear_fixture() {
        rm -f "$fixture_dir/secret.json"
        rmdir "$fixture_dir"
    }
    trap clear_fixture 0
    trap 'exit 130' HUP INT TERM
    if ! "$RHIZA_FIXTURE_GENERATOR" qualification-fixture "$ns" "$RHIZA_RUN_ID" "$owner" > "$fixture_dir/secret.json" 2>/dev/null; then
        die 'Trusted fixture generator failed (payload/diagnostics omitted)'
    fi
    chmod 600 "$fixture_dir/secret.json"
    validate_fixture < "$fixture_dir/secret.json" || die 'Generated fixture rejected (payload omitted)'
    # Invoked by the receipt wrapper, which accepts a function command.
    # shellcheck disable=SC2329
    create_fixture_secret() {
        # Never serialize the returned Secret or log API errors containing it.
        k create -f - -o 'jsonpath={.metadata.uid}' < "$fixture_dir/secret.json" 2>/dev/null
    }
    record fixture-secret-create create_fixture_secret
    [ -s "$RHIZA_AUTH_STATE/fixture-secret-create.json" ] || die 'Secret UID acknowledgement missing'
    mark fixture-secret
    clear_fixture
    trap - 0 HUP INT TERM
    record ci-project-grant g projects add-iam-policy-binding "$project" --member="serviceAccount:$ci" --role="projects/$project/roles/$cluster_role" --condition="$cluster_condition" --format=json
    mark ci-project-grant
    record ci-wi-grant g iam service-accounts add-iam-policy-binding "$ci" --member="$ci_member" --role=roles/iam.workloadIdentityUser --condition="$expiry" --format=json
    mark ci-wi-grant
    record data-wi-grant g iam service-accounts add-iam-policy-binding "$data" --member="$data_member" --role=roles/iam.workloadIdentityUser --condition="$expiry" --format=json
    mark data-wi-grant
    record folder-grant g storage managed-folders add-iam-policy-binding "$folder" --member="serviceAccount:$data" --role="projects/$project/roles/$folder_role" --condition="$expiry" --format=json
    mark folder-grant
    record project-after g projects get-iam-policy "$project" --format=json
    record bucket-after g storage buckets get-iam-policy "gs://$bucket" --format=json
    record folder-after g storage managed-folders get-iam-policy "$folder" --format=json
    printf '%s\n' "Prepared $ns; CI/GCS negative authorization and admission checks still required before faults."
    exit 0
fi
# Rollback never deletes GCS objects. Executor must first prove fault/workload
# quiescence and complete exact-prefix cleanup; unknown in-flight outcomes stop.
[ "${RHIZA_ROLLBACK_QUIESCENT:-}" = YES_VERIFIED ] || die 'Fault/process and storage quiescence attestation required'
if [ -f "$RHIZA_AUTH_STATE/inflight" ]; then
    last=$(cat "$RHIZA_AUTH_STATE/inflight")
    [ -f "$RHIZA_AUTH_STATE/$last.exit" ] && [ "$(cat "$RHIZA_AUTH_STATE/$last.exit")" = 0 ] || die 'Unresolved operation outcome; manual exact-target audit required'
fi
check_sa() {
    which=$1; account=$2
    record "$which-current" g iam service-accounts describe "$account" --format=json
    [ "$(jq -r .uniqueId "$RHIZA_AUTH_STATE/$which-current.json")" = "$(jq -r .uniqueId "$RHIZA_AUTH_STATE/$which-create.json")" ] || die 'GSA identity changed'
    [ "$(jq -r .description "$RHIZA_AUTH_STATE/$which-current.json")" = "$owner" ] || die 'GSA ownership changed'
}
owned ci && check_sa ci "$ci"
owned data && check_sa data "$data"
if owned pool; then
    record pool-current g iam workload-identity-pools describe "$pool" --location=global --format=json
    [ "$(jq -r .description "$RHIZA_AUTH_STATE/pool-current.json")" = "$owner" ] || die 'Pool ownership changed'
fi
if owned provider; then
    record provider-current g iam workload-identity-pools providers describe github --location=global --workload-identity-pool="$pool" --format=json
    [ "$(jq -r .description "$RHIZA_AUTH_STATE/provider-current.json")" = "$owner" ] || die 'Provider ownership changed'
    record provider-disable g iam workload-identity-pools providers update-oidc github --location=global --workload-identity-pool="$pool" --disabled --format=json
fi
if owned folder; then
    record folder-current g storage managed-folders describe "$folder" --raw --format=json
    [ "$(jq -r .id "$RHIZA_AUTH_STATE/folder-current.json")" = "$(jq -r .id "$RHIZA_AUTH_STATE/folder-identity.json")" ] || die 'Folder identity changed'
fi
# SDK add/remove binding operations preserve policy etags; never set old policy.
# Any concurrent-modification error stops for review, not an unconditional retry.
if owned folder-grant; then record revoke-folder g storage managed-folders remove-iam-policy-binding "$folder" --member="serviceAccount:$data" --role="projects/$project/roles/$folder_role" --condition="$expiry" --format=json; fi
if owned ci-wi-grant; then record revoke-ci-wi g iam service-accounts remove-iam-policy-binding "$ci" --member="$ci_member" --role=roles/iam.workloadIdentityUser --condition="$expiry" --format=json; fi
if owned data-wi-grant; then record revoke-data-wi g iam service-accounts remove-iam-policy-binding "$data" --member="$data_member" --role=roles/iam.workloadIdentityUser --condition="$expiry" --format=json; fi
if owned ci-project-grant; then record revoke-project g projects remove-iam-policy-binding "$project" --member="serviceAccount:$ci" --role="projects/$project/roles/$cluster_role" --condition="$cluster_condition" --format=json; fi
if owned kubernetes; then
    record namespace-current k get namespace "$ns" -o json
    [ "$(jq -r .metadata.uid "$RHIZA_AUTH_STATE/namespace-current.json")" = "$(jq -r .metadata.uid "$RHIZA_AUTH_STATE/namespace-identity.json")" ] || die 'Namespace identity changed'
    [ "$(jq -r '.metadata.annotations["rhiza.dev/auth-owner"]' "$RHIZA_AUTH_STATE/namespace-current.json")" = "$owner" ] || die 'Namespace ownership changed'
    delete_kube() {
        tag=$1; kind=$2; name=$3; uri=$4
        record "$tag-current" k get "$kind" "$name" -o json
        [ "$(jq -r .metadata.uid "$RHIZA_AUTH_STATE/$tag-current.json")" = "$(jq -r .metadata.uid "$RHIZA_AUTH_STATE/$tag-identity.json")" ] || die 'Kubernetes object identity changed'
        [ "$(jq -r '.metadata.annotations["rhiza.dev/auth-owner"]' "$RHIZA_AUTH_STATE/$tag-current.json")" = "$owner" ] || die 'Kubernetes object ownership changed'
        jq '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:.metadata.uid,resourceVersion:.metadata.resourceVersion}}' "$RHIZA_AUTH_STATE/$tag-current.json" > "$RHIZA_AUTH_STATE/$tag-delete-options.json"
        record "$tag-delete" k delete --raw "$uri" -f "$RHIZA_AUTH_STATE/$tag-delete-options.json"
    }
    # Keep admission guards until the namespace is fully gone; UID/version
    # preconditions protect against delete/recreate races, never force deletion.
    delete_kube namespace namespace "$ns" "/api/v1/namespaces/$ns"
    record namespace-gone k wait --for=delete "namespace/$ns" --timeout=180s
    for kind in validatingadmissionpolicybinding validatingadmissionpolicy; do
        for suffix in faults runtime; do
            delete_kube "$kind-$suffix" "$kind" "$ns-$suffix" "/apis/admissionregistration.k8s.io/v1/${kind}s/$ns-$suffix"
        done
    done
    for kind in clusterrolebinding clusterrole; do
        delete_kube "$kind-reader" "$kind" "$ns-reader" "/apis/rbac.authorization.k8s.io/v1/${kind}s/$ns-reader"
    done
fi
# No bucket/object recursive deletion or --all IAM removal is permitted here.
if owned folder; then
    record folder-predelete g storage managed-folders describe "$folder" --raw --format=json
    [ "$(jq -r .id "$RHIZA_AUTH_STATE/folder-predelete.json")" = "$(jq -r .id "$RHIZA_AUTH_STATE/folder-identity.json")" ] || die 'Folder identity changed before deletion'
    record folder-delete g storage managed-folders delete "$folder"
fi
if owned provider; then record provider-delete g iam workload-identity-pools providers delete github --location=global --workload-identity-pool="$pool"; fi
if owned pool; then record pool-delete g iam workload-identity-pools delete "$pool" --location=global; fi
if owned ci; then record ci-delete g iam service-accounts delete "$(jq -r .uniqueId "$RHIZA_AUTH_STATE/ci-create.json")"; fi
if owned data; then record data-delete g iam service-accounts delete "$(jq -r .uniqueId "$RHIZA_AUTH_STATE/data-create.json")"; fi
for role in "$cluster_role" "$folder_role"; do
    case "$role" in "$cluster_role") tag='cluster-role' ;; *) tag='folder-role' ;; esac
    if owned "$tag"; then
        record "$tag-current" g iam roles describe "$role" --format=json
        [ "$(jq -r .description "$RHIZA_AUTH_STATE/$tag-current.json")" = "$owner" ] || die 'Role ownership changed'
        jq -e --slurpfile original "$RHIZA_AUTH_STATE/$tag-create.json" '.name == $original[0].name and .etag == $original[0].etag and .includedPermissions == $original[0].includedPermissions and .stage == $original[0].stage' "$RHIZA_AUTH_STATE/$tag-current.json" >/dev/null || die 'Custom role changed since creation; preserve and request exact rollback review'
        record "$tag-delete" g iam roles delete "$role"
    fi
done
record project-rollback g projects get-iam-policy "$project" --format=json
record bucket-rollback g storage buckets get-iam-policy "gs://$bucket" --format=json
printf '%s\n' 'Rollback completed; soft-deleted storage remains billable for retention period.'
