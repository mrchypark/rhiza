#!/bin/sh
# Administrative, one-run bootstrap. Default render is offline; never run in CI.
set -eu
# Never inherit shell tracing while processing freshly generated fixture bytes.
set +x
umask 077
die() { printf '%s\n' "$*" >&2; exit 1; }
mode=${1:-render}
auth_mode=ci
case "$mode" in
    render-local|apply-local|mint-local|rollback-local|check-local-auth)
        auth_mode=local
        case "$mode" in render-local) mode=render ;; apply-local) mode=apply ;; rollback-local) mode=rollback ;; esac
        : "${RHIZA_HARNESS_SHA:?reviewed local harness SHA required}"
        RHIZA_WORKFLOW_SHA=$RHIZA_HARNESS_SHA ;;
    render|check-fixture|check-folder-iam|check-cleanup-uris|negative-node-fixture|apply|rollback) ;;
    *) die 'Usage: bootstrap-gcs-postrelease.sh render[-local]|apply[-local]|mint-local|rollback[-local]|check-local-auth|check-fixture|check-folder-iam|check-cleanup-uris|negative-node-fixture' ;;
esac
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
ns=rhiza-v0191-20261008-$RHIZA_RUN_ID
prefix=postrelease/v0.19.1/$RHIZA_RUN_ID/
folder=gs://$bucket/$prefix
pool=rhiza-v0191-gcs-$RHIZA_RUN_ID
ci_id=rhiza-v0191-ci-$RHIZA_RUN_ID
data_id=rhiza-v0191-gcs-$RHIZA_RUN_ID
ci=$ci_id@$project.iam.gserviceaccount.com
data=$data_id@$project.iam.gserviceaccount.com
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
    runtime_kind=User; runtime_name=$ci
    runtime_api_group='    apiGroup: rbac.authorization.k8s.io'; runtime_namespace=
    if [ "$auth_mode" = local ]; then
        runtime_kind=ServiceAccount; runtime_name=rhiza-runtime
        runtime_api_group=; runtime_namespace="    namespace: $ns"
    fi
    sed -e "s/@NAMESPACE@/$ns/g" -e "s/@RUN_ID@/$RHIZA_RUN_ID/g" -e "s/@POD_GSA@/$data/g" -e "s/@OWNER@/$owner/g" \
      -e "s/@RUNTIME_KIND@/$runtime_kind/g" -e "s/@RUNTIME_NAME@/$runtime_name/g" \
      -e "s/@RUNTIME_API_GROUP@/$runtime_api_group/g" -e "s/@RUNTIME_NAMESPACE@/$runtime_namespace/g" \
      "$script_dir/gcs-postrelease-auth.yaml.in" | awk -v mode="$auth_mode" '
        /^# BEGIN LOCAL RUNTIME ACCOUNT/ {skip=(mode!="local"); next}
        /^# END LOCAL RUNTIME ACCOUNT/ {skip=0; next}
        !skip {print}'
}
utc_epoch() { date -u -j -f '%Y-%m-%dT%H:%M:%SZ' "$1" '+%s' 2>/dev/null || date -u -d "$1" '+%s'; }
local_token_files() {
    # This consumes the native TokenRequest response without printing credentials.
    jq -e '.apiVersion=="authentication.k8s.io/v1" and .kind=="TokenRequest" and
      (.status.token|type)=="string" and (.status.token|test("^[A-Za-z0-9_-]+[.][A-Za-z0-9_-]+[.][A-Za-z0-9_-]+$")) and
      (.status.expirationTimestamp|type)=="string"' "$1" >/dev/null 2>/dev/null || die 'TokenRequest response rejected (payload omitted)'
    token_expiry=$(jq -r .status.expirationTimestamp "$1")
    token_expires=$(utc_epoch "$token_expiry") || die 'TokenRequest expiry rejected'
    scope_expires=$(utc_epoch "$RHIZA_AUTH_EXPIRES") || die 'Scope expiry rejected'
    token_now=$(date -u '+%s')
    [ "$token_expires" -le "$scope_expires" ] && [ "$token_expires" -ge "$((token_now + 2400))" ] || die 'Token expiry exceeds scope or leaves insufficient runtime/cleanup time'
    jq -e '.name=="ied-cluster" and .location=="asia-northeast3" and
      (.endpoint|type)=="string" and (.endpoint|test("^[a-zA-Z0-9.-]+$")) and
      (.masterAuth.clusterCaCertificate|type)=="string" and (.masterAuth.clusterCaCertificate|length)>0' "$2" >/dev/null || die 'Pinned cluster endpoint/CA metadata rejected'
    jq -r .status.token "$1" > "$RHIZA_AUTH_STATE/runtime.token"
    chmod 600 "$RHIZA_AUTH_STATE/runtime.token"
    jq -n --arg context "$context" --arg ns "$ns" --arg file "$RHIZA_AUTH_STATE/runtime.token" --slurpfile cluster "$2" '
      {apiVersion:"v1",kind:"Config",clusters:[{name:$context,cluster:{server:("https://"+$cluster[0].endpoint),"certificate-authority-data":$cluster[0].masterAuth.clusterCaCertificate}}],
       users:[{name:"rhiza-runtime",user:{tokenFile:$file}}],contexts:[{name:$context,context:{cluster:$context,user:"rhiza-runtime",namespace:$ns}}],"current-context":$context}
    ' > "$RHIZA_AUTH_STATE/runtime.kubeconfig"
    chmod 600 "$RHIZA_AUTH_STATE/runtime.kubeconfig"
    jq -n --arg expiry "$token_expiry" --arg subject "system:serviceaccount:$ns:rhiza-runtime" '{expirationTimestamp:$expiry,subject:$subject}' > "$RHIZA_AUTH_STATE/runtime-token-ack.json"
}
mint_local() (
    [ ! -e "$RHIZA_AUTH_STATE/runtime-token-request.attempted" ] || die 'TokenRequest already attempted; no renewal/retry'
    [ ! -e "$RHIZA_AUTH_STATE/runtime.token" ] && [ ! -L "$RHIZA_AUTH_STATE/runtime.token" ] &&
      [ ! -e "$RHIZA_AUTH_STATE/runtime.kubeconfig" ] && [ ! -L "$RHIZA_AUTH_STATE/runtime.kubeconfig" ] || die 'Runtime credential collision'
    mint_expires=$(utc_epoch "$RHIZA_AUTH_EXPIRES")
    mint_now=$(date -u '+%s')
    mint_remaining=$((mint_expires - mint_now))
    [ "$mint_remaining" -ge 2430 ] && [ "$mint_remaining" -le 3300 ] || die 'Local preparation window exhausted or scope exceeds 55 minutes'
    # Native API responses containing tokens never enter record() artifacts.
    # shellcheck disable=SC2329
    clear_local_mint() {
        mint_exit=$?
        rm -f "$RHIZA_AUTH_STATE/runtime-token-response.private" "$RHIZA_AUTH_STATE/runtime-token-request.private"
        if [ "$mint_exit" != 0 ]; then rm -f "$RHIZA_AUTH_STATE/runtime.token" "$RHIZA_AUTH_STATE/runtime.kubeconfig"; fi
        printf '%s\n' "$mint_exit" > "$RHIZA_AUTH_STATE/mint-local.exit"
    }
    trap clear_local_mint 0
    trap 'exit 130' HUP INT TERM
    record mint-cluster g container clusters describe ied-cluster --region=asia-northeast3 --format=json
    jq -n --argjson duration "$((mint_remaining - 30))" '{apiVersion:"authentication.k8s.io/v1",kind:"TokenRequest",spec:{expirationSeconds:$duration}}' > "$RHIZA_AUTH_STATE/runtime-token-request.private"
    printf '%s\n' "$owner" > "$RHIZA_AUTH_STATE/runtime-token-request.attempted"
    printf '%s\n' runtime-token-request > "$RHIZA_AUTH_STATE/inflight"
    mint_code=0
    k create --raw "/api/v1/namespaces/$ns/serviceaccounts/rhiza-runtime/token" -f "$RHIZA_AUTH_STATE/runtime-token-request.private" > "$RHIZA_AUTH_STATE/runtime-token-response.private" 2>/dev/null || mint_code=$?
    printf '%s\n' "$mint_code" > "$RHIZA_AUTH_STATE/runtime-token-request.exit"
    [ "$mint_code" = 0 ] || die 'TokenRequest failed; native exit retained, payload/diagnostics omitted; no retry'
    local_token_files "$RHIZA_AUTH_STATE/runtime-token-response.private" "$RHIZA_AUTH_STATE/mint-cluster.json"
    printf '%s\n' "$owner" > "$RHIZA_AUTH_STATE/runtime-token.created"
    printf '%s\n' 'Single local TokenRequest accepted; private tokenFile kubeconfig ready. Scoped actor checks remain required.'
)
admission_uri() {
    case "$1" in
        validatingadmissionpolicy) collection=validatingadmissionpolicies ;;
        validatingadmissionpolicybinding) collection=validatingadmissionpolicybindings ;;
        *) die 'Unsupported admission cleanup kind' ;;
    esac
    printf '/apis/admissionregistration.k8s.io/v1/%s/%s\n' "$collection" "$2"
}
create_account() {
    case "$1" in "$ci_id"|"$data_id") ;; *) die 'Account ID does not match fresh run' ;; esac
    gcloud --project="$project" --quiet iam service-accounts create "$1" --description="$owner" --format=json
}
check_folder_identity() {
    # Path/id alone cannot distinguish a replacement folder. IAM may legitimately
    # change metageneration, so preserve createTime and validate current metadata.
    jq -e --arg bucket "$bucket" --arg prefix "$prefix" --slurpfile original "$RHIZA_AUTH_STATE/folder-identity.json" '
      ($original[0]) as $o |
      $o.bucket==$bucket and $o.name==$prefix and $o.id==($bucket+"/"+$prefix) and
      ($o.createTime|type)=="string" and ($o.createTime|length)>0 and
      ($o.metageneration|type)=="string" and ($o.metageneration|test("^[1-9][0-9]*$")) and
      ($o.updateTime|type)=="string" and ($o.updateTime|length)>0 and
      .bucket==$o.bucket and .name==$o.name and .id==$o.id and .createTime==$o.createTime and
      (.metageneration|type)=="string" and (.metageneration|test("^[1-9][0-9]*$")) and
      (.updateTime|type)=="string" and (.updateTime|length)>0
    ' "$1" >/dev/null || die 'Managed folder incarnation/metadata changed or missing'
}
folder_iam() (
    # Subshell keeps private credential traps separate from fixture-secret traps.
    # No SDK policy projection, automatic folder creation, retries or old-policy restore.
    action=$1
    case "$action" in get|add|remove) ;; *) die 'Unknown folder IAM action' ;; esac
    iam_dir=$(mktemp -d "$RHIZA_AUTH_STATE/.folder-iam.XXXXXXXX") || exit 1
    chmod 700 "$iam_dir" || exit 1
    # Invoked by the EXIT/signal trap, not as an ordinary shell call.
    # shellcheck disable=SC2329
    clear_iam() {
        iam_exit=$?
        rm -f "$iam_dir/token" "$iam_dir/header"
        if [ "$iam_exit" = 0 ]; then
            rm -f "$iam_dir"/metadata-before-get.* "$iam_dir"/metadata-before-put.* "$iam_dir"/current.* "$iam_dir"/desired.* "$iam_dir"/put-ack.* "$iam_dir"/readback.* "$iam_dir"/readback-match.* "$iam_dir/auth.exit"
            rmdir "$iam_dir"
        else
            # Private noncredential bodies/statuses are first-outcome evidence;
            # keep separate current/desired/PUT ACK/readback, never overwrite.
            printf '%s\n' "$iam_exit" > "$iam_dir/terminal.exit"
            printf 'Folder IAM stopped; private phase receipts retained at %s\n' "$iam_dir" >&2
        fi
    }
    trap clear_iam 0
    trap 'exit 130' HUP INT TERM
    [ "$(gcloud --project="$project" --quiet config get-value account 2>/dev/null)" = "$RHIZA_EXPECTED_ADMIN_ACCOUNT" ] || die 'Unexpected IAM administrative account'
    folder_metadata() {
        metadata_phase=$1; metadata_exit=0
        gcloud --project="$project" --quiet storage managed-folders describe "$folder" --raw --format=json > "$iam_dir/$metadata_phase.json" 2> "$iam_dir/$metadata_phase.stderr" || metadata_exit=$?
        printf '%s\n' "$metadata_exit" > "$iam_dir/$metadata_phase.exit"
        if [ "$metadata_exit" != 0 ]; then cat "$iam_dir/$metadata_phase.stderr" >&2; return "$metadata_exit"; fi
        check_folder_identity "$iam_dir/$metadata_phase.json"
    }
    folder_metadata metadata-before-get || exit $?
    auth_exit=0
    gcloud --project="$project" --quiet auth print-access-token > "$iam_dir/token" 2>/dev/null || auth_exit=$?
    printf '%s\n' "$auth_exit" > "$iam_dir/auth.exit"
    [ "$auth_exit" = 0 ] || die 'Folder IAM token acquisition failed (diagnostics omitted)'
    chmod 600 "$iam_dir/token" || exit 1
    [ -s "$iam_dir/token" ] || die 'Empty folder IAM token'
    awk '{printf "Authorization: Bearer %s\n",$0}' "$iam_dir/token" > "$iam_dir/header" || exit 1
    chmod 600 "$iam_dir/header" || exit 1
    encoded_prefix=$(printf '%s' "$prefix" | jq -sRr @uri) || exit 1
    iam_url="https://storage.googleapis.com/storage/v1/b/$bucket/managedFolders/$encoded_prefix/iam"
    folder_request() {
        method=$1; request_phase=$2; request_exit=0
        if [ "$method" = GET ]; then
            curl --silent --show-error --fail-with-body --max-time 30 --header "@$iam_dir/header" --output "$iam_dir/$request_phase.json" --write-out '%{http_code}' "$iam_url?optionsRequestedPolicyVersion=3" > "$iam_dir/$request_phase.http-status" 2> "$iam_dir/$request_phase.stderr" || request_exit=$?
        else
            curl --silent --show-error --fail-with-body --max-time 30 --request PUT --header "@$iam_dir/header" --header 'Content-Type: application/json' --data-binary "@$iam_dir/desired.json" --output "$iam_dir/$request_phase.json" --write-out '%{http_code}' "$iam_url" > "$iam_dir/$request_phase.http-status" 2> "$iam_dir/$request_phase.stderr" || request_exit=$?
        fi
        printf '%s\n' "$request_exit" > "$iam_dir/$request_phase.exit"
        if [ "$request_exit" != 0 ]; then cat "$iam_dir/$request_phase.stderr" >&2; return "$request_exit"; fi
        [ "$(cat "$iam_dir/$request_phase.http-status")" = 200 ] || die 'Folder IAM HTTP acknowledgement mismatch; no retry'
    }
    validate_policy() {
        policy_exit=0
        jq -e --arg resource "projects/_/buckets/$bucket/managedFolders/$prefix" '
          type=="object" and .kind=="storage#policy" and .resourceId==$resource and
          (.etag|type)=="string" and (.etag|length)>0 and
          ((has("bindings")|not) or (.bindings|type)=="array") and
          (.version==1 or .version==3 or
            (((has("version")|not) or .version==0) and ((.bindings//[])|length)==0)) and
          all((.bindings//[])[];
            (.role|type)=="string" and (.role|contains("_withcond_")|not) and
            (.members|type)=="array" and all(.members[]; type=="string") and
            (.condition==null or ((.condition|type)=="object" and
              (.condition.title|type)=="string" and (.condition.expression|type)=="string"))) and
          (.version==3 or all((.bindings//[])[]; .condition==null))
        ' "$1" >/dev/null 2> "$1.validation.stderr" || policy_exit=$?
        printf '%s\n' "$policy_exit" > "$1.validation.exit"
        if [ "$policy_exit" != 0 ]; then cat "$1.validation.stderr" >&2; die 'Folder IAM full policy/version/identity rejected'; fi
    }
    folder_request GET current || exit $?
    validate_policy "$iam_dir/current.json"
    if [ "$action" = get ]; then cat "$iam_dir/current.json"; exit 0; fi
    desired_exit=0
    jq --arg action "$action" --arg role "projects/$project/roles/$folder_role" --arg member "serviceAccount:$data" \
      --arg title "rhiza-$RHIZA_RUN_ID-expiry" --arg expression "request.time < timestamp('$RHIZA_AUTH_EXPIRES')" '
      {title:$title,expression:$expression} as $condition |
      (.bindings//[]) as $bindings |
      [$bindings[]|select(.role==$role and .condition==$condition)] as $matches |
      if ($matches|length)>1 or any($bindings[]; .role==$role and .condition!=$condition) or
         ($action=="remove" and (($matches|length)!=1 or ($matches[0].members|index($member))==null)) or
         ($action=="add" and ($matches|length)==1 and ($matches[0].members|index($member))!=null)
      then error("Missing/ambiguous/mismatched owned folder IAM binding; stop")
      else .version=3 | .bindings=$bindings |
        if $action=="add" then
          if ($matches|length)==0 then .bindings += [{role:$role,members:[$member],condition:$condition}]
          else .bindings |= map(if .role==$role and .condition==$condition then .members += [$member] else . end) end
        else .bindings |= map(if .role==$role and .condition==$condition and (.members|index($member))!=null
          then .members-=[$member] | select((.members|length)>0) else . end) end
      end
    ' "$iam_dir/current.json" > "$iam_dir/desired.json" 2> "$iam_dir/desired.stderr" || desired_exit=$?
    printf '%s\n' "$desired_exit" > "$iam_dir/desired.exit"
    if [ "$desired_exit" != 0 ]; then cat "$iam_dir/desired.stderr" >&2; die 'Folder IAM target refused; no SET'; fi
    # Fresh incarnation check immediately before the sole etag-protected PUT.
    folder_metadata metadata-before-put || exit $?
    folder_request PUT put-ack || exit $?
    folder_request GET readback || exit $?
    validate_policy "$iam_dir/readback.json"
    match_exit=0
    jq -e --slurpfile desired "$iam_dir/desired.json" '(.bindings//[])==$desired[0].bindings' "$iam_dir/readback.json" > "$iam_dir/readback-match.json" 2> "$iam_dir/readback-match.stderr" || match_exit=$?
    printf '%s\n' "$match_exit" > "$iam_dir/readback-match.exit"
    [ "$match_exit" = 0 ] || die 'Folder IAM SET/readback mismatch; outcome needs review, no retry'
    cat "$iam_dir/readback.json"
)
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
if [ "$mode" = check-local-auth ]; then
    # Offline boundary exercises the real render/mint functions, not cloud APIs.
    local_fixture=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-local-auth.XXXXXXXX")
    RHIZA_AUTH_STATE=$local_fixture
    # shellcheck disable=SC2329
    clear_local_fixture() { rm -f "$local_fixture"/*; rmdir "$local_fixture"; }
    trap clear_local_fixture 0
    trap 'exit 130' HUP INT TERM
    fixture_now=$(date -u '+%s')
    stamp() { date -u -r "$1" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -d "@$1" '+%Y-%m-%dT%H:%M:%SZ'; }
    RHIZA_AUTH_EXPIRES=$(stamp "$((fixture_now + 3300))")
    render > "$local_fixture/local.yaml"
    auth_mode=ci; render > "$local_fixture/ci.yaml"; auth_mode=local
    subjects() {
        awk '/^---/ {binding=0;subject=0} /^kind: (RoleBinding|ClusterRoleBinding)$/ {binding=1}
          binding && /^subjects:/ {subject=1} /^roleRef:/ {subject=0} subject {print}' "$1"
    }
    subjects "$local_fixture/local.yaml" > "$local_fixture/local-subjects"
    [ "$(grep -c 'kind: ServiceAccount' "$local_fixture/local-subjects")" = 2 ] || die 'Local bindings must select two KSA subjects'
    [ "$(grep -c "namespace: $ns" "$local_fixture/local-subjects")" = 2 ] || die 'Local binding namespace missing'
    if grep -Eq 'apiGroup:|kind: User' "$local_fixture/local-subjects"; then die 'Local subject has User/apiGroup'; fi
    grep -q 'name: rhiza-runtime' "$local_fixture/local.yaml" || die 'Local runtime KSA missing'
    subjects "$local_fixture/ci.yaml" > "$local_fixture/ci-subjects"
    [ "$(grep -c 'kind: User' "$local_fixture/ci-subjects")" = 2 ] || die 'CI User bindings changed'
    if grep -q 'name: rhiza-runtime' "$local_fixture/ci.yaml"; then die 'Unused local KSA created in CI'; fi
    jq -n '{name:"ied-cluster",location:"asia-northeast3",endpoint:"127.0.0.1",masterAuth:{clusterCaCertificate:"FAKE-OFFLINE-CA"}}' > "$local_fixture/cluster.json"
    jq -n --arg expiry "$(stamp "$((fixture_now + 2700))")" '{apiVersion:"authentication.k8s.io/v1",kind:"TokenRequest",status:{token:"FAKE.OFFLINE.TOKEN",expirationTimestamp:$expiry}}' > "$local_fixture/response.json"
    # Invoked indirectly through the synthetic record() wrapper.
    # shellcheck disable=SC2329
    g() { [ "$*" = 'container clusters describe ied-cluster --region=asia-northeast3 --format=json' ] || die 'Unexpected offline metadata command'; cat "$local_fixture/cluster.json"; }
    k() {
        [ "$*" = "create --raw /api/v1/namespaces/$ns/serviceaccounts/rhiza-runtime/token -f $RHIZA_AUTH_STATE/runtime-token-request.private" ] || die 'Unexpected offline token command'
        printf 'request\n' >> "$local_fixture/api-calls"
        [ "$fixture_case" != native-failure ] || return 56
        cat "$local_fixture/response.json"
    }
    record() { fixture_receipt=$1; shift; "$@" > "$local_fixture/$fixture_receipt.json"; }
    fixture_case=positive
    mint_local > "$local_fixture/result" 2> "$local_fixture/stderr"
    jq -e --arg context "$context" --arg ns "$ns" --arg token "$local_fixture/runtime.token" '
      .apiVersion=="v1" and .kind=="Config" and ."current-context"==$context and
      (.users|length)==1 and .users[0].user=={tokenFile:$token} and
      (.clusters|length)==1 and (.contexts|length)==1 and .contexts[0].context.namespace==$ns
    ' "$local_fixture/runtime.kubeconfig" >/dev/null || die 'Static private kubeconfig mismatch'
    [ "$(find "$local_fixture/runtime.token" "$local_fixture/runtime.kubeconfig" -type f -user "$(id -u)" -perm 0600 -print | wc -l | tr -d ' ')" = 2 ] || die 'Credential file mode regression'
    if mint_local > "$local_fixture/result" 2> "$local_fixture/stderr"; then die 'Token renewal accepted'; fi
    [ "$(wc -l < "$local_fixture/api-calls" | tr -d ' ')" = 1 ] || die 'TokenRequest repeated'
    for fixture_case in overlong short missing-token wrong-cluster native-failure; do
        rm -f "$local_fixture/runtime.token" "$local_fixture/runtime.kubeconfig" "$local_fixture/runtime-token-request.attempted" "$local_fixture/api-calls"
        jq -n --arg expiry "$(stamp "$((fixture_now + 2700))")" '{apiVersion:"authentication.k8s.io/v1",kind:"TokenRequest",status:{token:"FAKE.OFFLINE.TOKEN",expirationTimestamp:$expiry}}' > "$local_fixture/response.json"
        jq -n '{name:"ied-cluster",location:"asia-northeast3",endpoint:"127.0.0.1",masterAuth:{clusterCaCertificate:"FAKE-OFFLINE-CA"}}' > "$local_fixture/cluster.json"
        case "$fixture_case" in
            overlong) jq --arg expiry "$(stamp "$((fixture_now + 3600))")" '.status.expirationTimestamp=$expiry' "$local_fixture/response.json" > "$local_fixture/change.json"; mv "$local_fixture/change.json" "$local_fixture/response.json" ;;
            short) jq --arg expiry "$(stamp "$((fixture_now + 1800))")" '.status.expirationTimestamp=$expiry' "$local_fixture/response.json" > "$local_fixture/change.json"; mv "$local_fixture/change.json" "$local_fixture/response.json" ;;
            missing-token) jq 'del(.status.token)' "$local_fixture/response.json" > "$local_fixture/change.json"; mv "$local_fixture/change.json" "$local_fixture/response.json" ;;
            wrong-cluster) jq '.name="other-cluster"' "$local_fixture/cluster.json" > "$local_fixture/change.json"; mv "$local_fixture/change.json" "$local_fixture/cluster.json" ;;
        esac
        if mint_local > "$local_fixture/result" 2> "$local_fixture/stderr"; then die "Local negative accepted: $fixture_case"; fi
        [ ! -e "$local_fixture/runtime.token" ] && [ ! -e "$local_fixture/runtime.kubeconfig" ] && [ ! -e "$local_fixture/runtime-token-response.private" ] && [ ! -e "$local_fixture/runtime-token-request.private" ] || die 'Failure retained credential bytes'
        [ "$(wc -l < "$local_fixture/api-calls" | tr -d ' ')" = 1 ] || die 'Negative request count regression'
        [ "$fixture_case" != native-failure ] || [ "$(cat "$local_fixture/runtime-token-request.exit")" = 56 ] || die 'Native failure lost'
        printf 'Local auth boundary %s refused; one request, no credentials retained.\n' "$fixture_case"
    done
    # One symlink regression fixture checks each output before any TokenRequest.
    rm -f "$local_fixture/runtime-token-request.attempted" "$local_fixture/api-calls"
    for credential in runtime.token runtime.kubeconfig; do
        ln -s "$local_fixture/missing-target" "$local_fixture/$credential"
        if mint_local > "$local_fixture/result" 2> "$local_fixture/stderr"; then die 'Dangling credential symlink accepted'; fi
        [ ! -e "$local_fixture/api-calls" ] && [ ! -e "$local_fixture/runtime-token-request.attempted" ] &&
          [ -L "$local_fixture/$credential" ] && [ ! -e "$local_fixture/missing-target" ] || die 'Symlink fixture issued a request or followed the link'
        rm -f "$local_fixture/$credential"
    done
    printf '%s\n' 'Both dangling credential symlinks refused before TokenRequest; no target writes.'
    printf '%s\n' 'Local/CI render, bounded token, static kubeconfig and no-renewal checks passed; no cloud calls.'
    exit 0
fi
if [ "$mode" = check-cleanup-uris ]; then
    # Offline fixture exercises the exact URI builder used by guarded rollback.
    [ "$(admission_uri validatingadmissionpolicy "$ns-runtime")" = "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/$ns-runtime" ] || die 'Policy cleanup URI regression'
    [ "$(admission_uri validatingadmissionpolicybinding "$ns-runtime")" = "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/$ns-runtime" ] || die 'Binding cleanup URI regression'
    if (admission_uri unsupported "$ns-runtime") >/dev/null 2>&1; then die 'Unknown cleanup kind accepted'; fi
    printf '%s\n' 'Two exact admission cleanup URIs and unknown-kind refusal verified; no cloud calls.'
    exit 0
fi
if [ "$mode" = check-folder-iam ]; then
    # Native command doubles exercise folder_iam itself, never external commands.
    fixture_dir=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-folder-iam.XXXXXXXX")
    RHIZA_AUTH_STATE=$fixture_dir
    RHIZA_EXPECTED_ADMIN_ACCOUNT=fixture-account.invalid
    # Invoked by the EXIT/signal trap.
    # shellcheck disable=SC2329
    clear_fixture_iam() {
        rm -f "$fixture_dir/folder-identity.json" "$fixture_dir/policy.json" "$fixture_dir/applied.json" "$fixture_dir/result.json" "$fixture_dir/stderr" "$fixture_dir/get.log" "$fixture_dir/put.log"
        rmdir "$fixture_dir"
    }
    trap clear_fixture_iam 0
    trap 'exit 130' HUP INT TERM
    jq -n --arg bucket "$bucket" --arg prefix "$prefix" '{bucket:$bucket,name:$prefix,id:($bucket+"/"+$prefix),createTime:"fixture-created",updateTime:"fixture-updated",metageneration:"1"}' > "$fixture_dir/folder-identity.json"
    gcloud() {
        case "$*" in
            "--project=$project --quiet config get-value account") printf '%s\n' "$RHIZA_EXPECTED_ADMIN_ACCOUNT" ;;
            "--project=$project --quiet auth print-access-token") printf '%s\n' 'FAKE-OFFLINE-NOT-A-CREDENTIAL' ;;
            "--project=$project --quiet iam service-accounts create $ci_id --description=$owner --format=json") printf '{"fixture_created":"%s"}\n' "$ci_id" ;;
            "--project=$project --quiet iam service-accounts create $data_id --description=$owner --format=json") printf '{"fixture_created":"%s"}\n' "$data_id" ;;
            "--project=$project --quiet storage managed-folders describe $folder --raw --format=json")
                if [ "$fixture_case" = replaced-folder ]; then jq '.createTime="replacement"' "$fixture_dir/folder-identity.json"
                else cat "$fixture_dir/folder-identity.json"; fi ;;
            *) die 'Unexpected synthetic gcloud command' ;;
        esac
    }
    [ "$pool" = "rhiza-v0191-gcs-$RHIZA_RUN_ID" ] && [ "$ci" = "rhiza-v0191-ci-$RHIZA_RUN_ID@$project.iam.gserviceaccount.com" ] && [ "$data" = "rhiza-v0191-gcs-$RHIZA_RUN_ID@$project.iam.gserviceaccount.com" ] || die 'Fresh identity derivation regression'
    create_account "$ci_id" | jq -e --arg id "$ci_id" '.fixture_created==$id' >/dev/null
    create_account "$data_id" | jq -e --arg id "$data_id" '.fixture_created==$id' >/dev/null
    if (create_account rhiza-v0190-ci-1008) >/dev/null 2>&1; then die 'Old account accepted'; fi
    other_run=ffffffff
    [ "$RHIZA_RUN_ID" != "$other_run" ] || other_run=00000000
    if (create_account "rhiza-v0191-gcs-$other_run") >/dev/null 2>&1; then die 'Cross-run account accepted'; fi
    printf 'Fresh run %s: exact pool, two GSA create arguments, old/cross-run refusal verified.\n' "$RHIZA_RUN_ID"
    curl() {
        fixture_method=GET; fixture_output=; fixture_body=; fixture_header=; fixture_url=
        while [ "$#" -gt 0 ]; do
            case "$1" in
                --request) fixture_method=$2; shift ;;
                --output) fixture_output=$2; shift ;;
                --data-binary) fixture_body=${2#@}; shift ;;
                --header) case "$2" in @*) fixture_header=${2#@} ;; 'Content-Type: application/json') ;; *) die 'Synthetic header rejected' ;; esac; shift ;;
                --max-time|--write-out) shift ;;
                --silent|--show-error|--fail-with-body) ;;
                https://*) fixture_url=$1 ;;
                *) die 'Unexpected synthetic curl argument' ;;
            esac
            shift
        done
        [ -f "$fixture_header" ] && [ -s "$fixture_header" ] || die 'Private header absent'
        # Failed stat stdout must not contaminate the other platform's result.
        if fixture_permissions=$(stat -c '%a' "$fixture_header" 2>/dev/null); then :
        else fixture_permissions=$(stat -f '%Lp' "$fixture_header"); fi
        [ "$fixture_permissions" = 600 ] || die 'Header permissions not private'
        grep -Fxq 'Authorization: Bearer FAKE-OFFLINE-NOT-A-CREDENTIAL' "$fixture_header" || die 'Synthetic token mismatch'
        fixture_base="https://storage.googleapis.com/storage/v1/b/$bucket/managedFolders/$(printf '%s' "$prefix" | jq -sRr @uri)/iam"
        if [ "$fixture_method" = GET ]; then
            [ "$fixture_url" = "$fixture_base?optionsRequestedPolicyVersion=3" ] || die 'GET policy version or fields regression'
            printf 'GET\n' >> "$fixture_dir/get.log"
            case "$fixture_case" in
                get-404) printf '{"error":"GET404"}' > "$fixture_output"; printf '404'; return 22 ;;
                unknown-get) printf '{"partial":"GETunknown"}' > "$fixture_output"; printf '000'; return 56 ;;
            esac
            if [ -s "$fixture_dir/put.log" ]; then
                [ "$fixture_case" != readback-404 ] || { printf '{"error":"readback404"}' > "$fixture_output"; printf '404'; return 22; }
                if [ "$fixture_case" = readback-mismatch ]; then jq '.bindings[1].members=["user:unexpected.invalid"]' "$fixture_dir/applied.json" > "$fixture_output"
                elif [ "$fixture_case" = remove-empty-readback ]; then jq 'del(.version,.bindings)' "$fixture_dir/applied.json" > "$fixture_output"
                else cp "$fixture_dir/applied.json" "$fixture_output"; fi
            else cp "$fixture_dir/policy.json" "$fixture_output"; fi
        else
            [ "$fixture_method" = PUT ] && [ "$fixture_url" = "$fixture_base" ] || die 'PUT URL regression'
            printf 'PUT\n' >> "$fixture_dir/put.log"
            jq -e --slurpfile original "$fixture_dir/policy.json" '.version==3 and .etag=="fresh-current-etag" and has("extra")==($original[0]|has("extra")) and .extra==$original[0].extra' "$fixture_body" >/dev/null || die 'Fresh etag/full policy missing'
            case "$fixture_case" in
                put-409|put-412|put-404) printf '{"error":"%s"}' "$fixture_case" > "$fixture_output"; printf '%s' "${fixture_case#put-}"; return 22 ;;
                unknown-put) printf '{"partial":"PUTunknown"}' > "$fixture_output"; printf '000'; return 56 ;;
            esac
            cp "$fixture_body" "$fixture_dir/applied.json"
            jq '.synthetic_ack="PUT-ACK-not-readback"' "$fixture_body" > "$fixture_output"
        fi
        printf '200'
    }
    for fixture_case in get-empty get-empty-omitted get-empty-array-omitted get-empty-zero-omitted get-empty-zero-array get-unconditional add add-empty-omitted add-empty-zero remove remove-single remove-empty-readback changed-condition duplicate-target hashed-role missing-target missing-etag version1-condition version-omitted-populated version-zero-populated version-null version-string bindings-null bindings-object bindings-false policy-null wrong-resource malformed-json replaced-folder get-404 unknown-get put-409 put-412 put-404 unknown-put readback-404 readback-mismatch; do
        : > "$fixture_dir/get.log"; : > "$fixture_dir/put.log"
        jq -n --arg resource "projects/_/buckets/$bucket/managedFolders/$prefix" --arg role "projects/$project/roles/$folder_role" --arg member "serviceAccount:$data" --arg title "rhiza-$RHIZA_RUN_ID-expiry" --arg expression "request.time < timestamp('$RHIZA_AUTH_EXPIRES')" '{kind:"storage#policy",resourceId:$resource,version:3,etag:"fresh-current-etag",extra:"preserve-root",bindings:[{role:$role,members:[$member,"user:co-member.invalid"],condition:{title:$title,expression:$expression}},{role:"roles/storage.objectViewer",members:[],extra:"preserve-empty"},{role:"roles/storage.objectViewer",members:["user:unrelated.invalid"]}]}' > "$fixture_dir/policy.json"
        fixture_action=remove; expected_code=1; expected_gets=1; expected_puts=0
        case "$fixture_case" in
            get-empty|add) jq '.version=1 | .bindings=[]' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            get-empty-omitted|add-empty-omitted) jq 'del(.version,.bindings,.extra)' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            get-empty-array-omitted) jq 'del(.version) | .bindings=[]' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            get-empty-zero-omitted) jq '.version=0 | del(.bindings)' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            get-empty-zero-array|add-empty-zero) jq '.version=0 | .bindings=[]' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            get-unconditional) jq '.version=1 | .bindings=.bindings[1:]' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            changed-condition) jq '.bindings[0].condition.description="different-whole-condition"' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            duplicate-target) jq '.bindings += [.bindings[0]]' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            hashed-role) jq '.bindings[0].role += "_withcond_hidden"' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            missing-target) jq '.bindings[0].members=["user:co-member.invalid"]' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            remove-single) jq '.bindings[0].members=[.bindings[0].members[0]]' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            remove-empty-readback) jq '.bindings=[.bindings[0]] | .bindings[0].members=[.bindings[0].members[0]]' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            missing-etag) jq 'del(.etag)' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            version1-condition) jq '.version=1' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            version-omitted-populated) jq 'del(.version) | .bindings=.bindings[1:]' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            version-zero-populated) jq '.version=0 | .bindings=.bindings[1:]' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            version-null) jq '.version=null | .bindings=[]' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            version-string) jq '.version="0" | .bindings=[]' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            bindings-null) jq '.bindings=null' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            bindings-object) jq '.bindings={}' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            bindings-false) jq '.bindings=false' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            policy-null) printf 'null\n' > "$fixture_dir/applied.json" ;;
            wrong-resource) jq '.resourceId="projects/_/buckets/wrong/managedFolders/wrong/"' "$fixture_dir/policy.json" > "$fixture_dir/applied.json" ;;
            malformed-json) printf '{\n' > "$fixture_dir/applied.json" ;;
            *) cp "$fixture_dir/policy.json" "$fixture_dir/applied.json" ;;
        esac
        cp "$fixture_dir/applied.json" "$fixture_dir/policy.json"
        case "$fixture_case" in
            get-empty|get-empty-omitted|get-empty-array-omitted|get-empty-zero-omitted|get-empty-zero-array|get-unconditional) fixture_action='get'; expected_code=0 ;;
            add|add-empty-omitted|add-empty-zero) fixture_action=add; expected_code=0; expected_gets=2; expected_puts=1 ;;
            remove|remove-single|remove-empty-readback) expected_code=0; expected_gets=2; expected_puts=1 ;;
            replaced-folder) expected_gets=0 ;;
            get-404) expected_code=22 ;;
            unknown-get) expected_code=56 ;;
            put-409|put-412|put-404) expected_code=22; expected_puts=1 ;;
            unknown-put) expected_code=56; expected_puts=1 ;;
            readback-404) expected_code=22; expected_gets=2; expected_puts=1 ;;
            readback-mismatch) expected_code=1; expected_gets=2; expected_puts=1 ;;
        esac
        actual_code=0
        (folder_iam "$fixture_action") > "$fixture_dir/result.json" 2> "$fixture_dir/stderr" || actual_code=$?
        [ "$actual_code" = "$expected_code" ] || die "Folder IAM fixture $fixture_case exit $actual_code expected $expected_code"
        [ "$(wc -l < "$fixture_dir/get.log" | tr -d ' ')" = "$expected_gets" ] && [ "$(wc -l < "$fixture_dir/put.log" | tr -d ' ')" = "$expected_puts" ] || die "Folder IAM fixture $fixture_case request count regression"
        retained=0
        for leftover in "$fixture_dir"/.folder-iam.*; do
            [ -e "$leftover" ] || continue
            [ ! -e "$leftover/token" ] && [ ! -e "$leftover/header" ] || die 'Private IAM credentials leaked'
            [ "$actual_code" != 0 ] || die 'Successful private IAM artifacts leaked'
            retained=$((retained+1))
            [ "$(cat "$leftover/terminal.exit")" = "$actual_code" ] || die 'Original terminal failure lost'
            if [ "$expected_gets" -ge 1 ]; then [ -s "$leftover/current.json" ] && [ -s "$leftover/current.exit" ] && [ -s "$leftover/current.http-status" ] || die 'First GET evidence lost'; fi
            if [ "$expected_puts" = 1 ]; then
                [ -s "$leftover/desired.json" ] && [ -s "$leftover/put-ack.json" ] && [ -s "$leftover/put-ack.exit" ] && [ -s "$leftover/put-ack.http-status" ] || die 'PUT outcome evidence lost'
            fi
            case "$fixture_case" in
                readback-404|readback-mismatch)
                    [ "$(cat "$leftover/put-ack.exit")" = 0 ] && [ "$(cat "$leftover/put-ack.http-status")" = 200 ] || die 'PUT ACK overwritten'
                    jq -e '.synthetic_ack=="PUT-ACK-not-readback"' "$leftover/put-ack.json" >/dev/null || die 'Original PUT body overwritten'
                    [ -s "$leftover/readback.json" ] && [ -s "$leftover/readback.exit" ] && [ -s "$leftover/readback.http-status" ] || die 'Readback failure evidence lost'
                    ;;
            esac
            # Only synthetic artifacts in this fresh fixture directory, after checks.
            rm -f "$leftover"/*
            rmdir "$leftover"
        done
        if [ "$actual_code" = 0 ]; then [ "$retained" = 0 ] || die 'Unexpected retained success'
        else [ "$retained" = 1 ] || die 'Private first failure receipt missing'; fi
        if [ "$fixture_case" = remove ]; then
            jq -e --slurpfile original "$fixture_dir/policy.json" '.bindings[1:]==$original[0].bindings[1:] and .bindings[0].members==["user:co-member.invalid"] and .bindings[0].condition==$original[0].bindings[0].condition' "$fixture_dir/result.json" >/dev/null || die 'Non-target/condition preservation regression'
        fi
        if [ "$fixture_case" = remove-single ]; then
            jq -e --slurpfile original "$fixture_dir/policy.json" '.bindings==$original[0].bindings[1:]' "$fixture_dir/result.json" >/dev/null || die 'Only emptied target must be removed'
        fi
        printf 'Folder IAM boundary %s: expected exit %s, GET %s, PUT %s; first-outcome evidence checked, credentials removed.\n' "$fixture_case" "$actual_code" "$expected_gets" "$expected_puts"
    done
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
for cmd in gcloud kubectl jq shasum curl; do command -v "$cmd" >/dev/null || die "Missing $cmd"; done
[ "$auth_mode" = local ] || command -v gh >/dev/null || die 'Missing gh'
k() { kubectl --context="$context" "$@"; }
g() { gcloud --project="$project" --quiet "$@"; }
identity=$(jq -nc --arg run "$RHIZA_RUN_ID" --arg sha "$RHIZA_WORKFLOW_SHA" --arg expiry "$RHIZA_AUTH_EXPIRES" --arg cap "$RHIZA_COST_CAP_KRW" --arg owner "$owner" '{run:$run,workflow_sha:$sha,expiry:$expiry,cost_cap_krw:$cap,owner:$owner}')
if [ "$auth_mode" = local ]; then identity=$(printf '%s\n' "$identity" | jq -c '.auth_mode="local"'); fi
if [ "$mode" = apply ]; then
    # Refuse stale/long-lived grants before the first remote call.
    expires=$(utc_epoch "$RHIZA_AUTH_EXPIRES")
    now=$(date -u '+%s')
    horizon=14400
    [ "$auth_mode" != local ] || horizon=3300
    [ "$expires" -gt "$now" ] && [ "$expires" -le "$((now + horizon))" ] || die 'Expiry exceeds the selected bounded scope'
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
[ ! -L "$RHIZA_AUTH_STATE" ] && [ "$(CDPATH='' cd -- "$RHIZA_AUTH_STATE" && pwd -P)" = "$RHIZA_AUTH_STATE" ] || die 'State path must be a private physical directory'
[ -n "$(find "$RHIZA_AUTH_STATE" -prune -type d -user "$(id -u)" -perm 0700 -print)" ] || die 'State directory owner/mode rejected'
[ "$(g config get-value account 2>/dev/null)" = "$RHIZA_EXPECTED_ADMIN_ACCOUNT" ] || die 'Unexpected administrative account'
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
check_folder_role() {
    record folder-role-current g iam roles describe "$folder_role" --format=json
    jq -e --arg owner "$owner" --slurpfile original "$RHIZA_AUTH_STATE/folder-role-create.json" '
      .description==$owner and .name==$original[0].name and .etag==$original[0].etag and
      .includedPermissions==$original[0].includedPermissions and .stage==$original[0].stage
    ' "$RHIZA_AUTH_STATE/folder-role-current.json" >/dev/null || die 'Folder role ownership/metadata changed'
}
if [ "$mode" = mint-local ]; then
    [ "$auth_mode" = local ] || die 'Local mode required'
    for required in local-prepared kubernetes data-wi-grant folder-grant fixture-secret; do
        owned "$required" || die 'Local bootstrap did not complete; no mint from partial/failed apply'
    done
    if [ -f "$RHIZA_AUTH_STATE/inflight" ]; then
        last=$(cat "$RHIZA_AUTH_STATE/inflight")
        [ -f "$RHIZA_AUTH_STATE/$last.exit" ] && [ "$(cat "$RHIZA_AUTH_STATE/$last.exit")" = 0 ] || die 'Unresolved operation outcome; no token mint'
    fi
    [ -s "$RHIZA_AUTH_STATE/runtime-ksa-identity.json" ] && [ -s "$RHIZA_AUTH_STATE/runtime-binding-identity.json" ] || die 'Runtime KSA/binding receipts missing'
    record mint-namespace k get namespace "$ns" -o json
    jq -e --arg owner "$owner" --slurpfile original "$RHIZA_AUTH_STATE/namespace-identity.json" '.metadata.uid==$original[0].metadata.uid and .metadata.annotations["rhiza.dev/auth-owner"]==$owner' "$RHIZA_AUTH_STATE/mint-namespace.json" >/dev/null || die 'Namespace identity changed'
    record mint-runtime-ksa k get serviceaccount rhiza-runtime --namespace="$ns" -o json
    jq -e --arg owner "$owner" --slurpfile original "$RHIZA_AUTH_STATE/runtime-ksa-identity.json" '.metadata.uid==$original[0].metadata.uid and .metadata.annotations["rhiza.dev/auth-owner"]==$owner and .automountServiceAccountToken==false' "$RHIZA_AUTH_STATE/mint-runtime-ksa.json" >/dev/null || die 'Runtime KSA identity changed'
    record mint-runtime-binding k get rolebinding rhiza-ci-runtime --namespace="$ns" -o json
    record mint-reader-binding k get clusterrolebinding "$ns-reader" -o json
    check_runtime_binding() {
        jq -e --slurpfile original "$RHIZA_AUTH_STATE/$2.json" '.metadata.uid==$original[0].metadata.uid and .subjects==$original[0].subjects and .roleRef==$original[0].roleRef' "$RHIZA_AUTH_STATE/$1.json" >/dev/null || die 'Runtime binding identity/spec changed'
    }
    check_runtime_binding mint-runtime-binding runtime-binding-identity
    check_runtime_binding mint-reader-binding clusterrolebinding-reader-identity
    mint_local
    exit 0
fi
if [ "$mode" = apply ]; then
    if [ "$auth_mode" = ci ]; then
    record github-main gh api repos/mrchypark/rhiza/branches/main
    jq -e --arg sha "$RHIZA_WORKFLOW_SHA" '.protected == true and .commit.sha == $sha' "$RHIZA_AUTH_STATE/github-main.json" >/dev/null || die 'Workflow SHA is not the current protected main SHA'
    record github-workflow gh api "repos/mrchypark/rhiza/contents/.github/workflows/gcs-postrelease.yml?ref=$RHIZA_WORKFLOW_SHA"
    jq -e '.type == "file" and .sha != null' "$RHIZA_AUTH_STATE/github-workflow.json" >/dev/null || die 'Canonical workflow absent at frozen protected merge'
    record github-environment gh api repos/mrchypark/rhiza/environments/rhiza-gcs-postrelease
    jq -e '.deployment_branch_policy.protected_branches == true and any(.protection_rules[]; .type == "required_reviewers" and (.reviewers|length)>0)' "$RHIZA_AUTH_STATE/github-environment.json" >/dev/null || die 'Protected-branch environment with required reviewers must exist before bootstrap'
    fi
    record project-before g projects get-iam-policy "$project" --format=json
    record bucket-before g storage buckets get-iam-policy "gs://$bucket" --format=json
    absent namespace k get namespace "$ns" -o json
    for suffix in faults network-faults; do
        absent "$suffix-policy" k get validatingadmissionpolicy "$ns-$suffix" -o json
        absent "$suffix-binding" k get validatingadmissionpolicybinding "$ns-$suffix" -o json
    done
    absent runtime-policy k get validatingadmissionpolicy "$ns-runtime" -o json
    absent runtime-binding k get validatingadmissionpolicybinding "$ns-runtime" -o json
    absent ns-reader k get clusterrole "$ns-reader" -o json
    absent ns-reader-binding k get clusterrolebinding "$ns-reader" -o json
    absent data g iam service-accounts describe "$data" --format=json
    if [ "$auth_mode" = ci ]; then
        absent pool g iam workload-identity-pools describe "$pool" --location=global --format=json
        absent ci g iam service-accounts describe "$ci" --format=json
        absent cluster-role g iam roles describe "$cluster_role" --format=json
    fi
    absent folder-role g iam roles describe "$folder_role" --format=json
    absent folder g storage managed-folders describe "$folder" --raw --format=json
    # Exact new prefix only; authentication errors cannot masquerade as empty.
    absent prefix g storage ls "$folder**"
    if [ "$auth_mode" = ci ]; then record ci-create create_account "$ci_id"; mark ci; fi
    record data-create create_account "$data_id"
    mark data
    if [ "$auth_mode" = ci ]; then
        record cluster-role-create g iam roles create "$cluster_role" --title='Rhiza temporary cluster metadata' --description="$owner" --permissions=container.clusters.get --stage=GA --format=json
        mark cluster-role
    fi
    record folder-role-create g iam roles create "$folder_role" --title='Rhiza temporary folder objects' --description="$owner" --permissions=storage.objects.get,storage.objects.list,storage.objects.create,storage.objects.update,storage.objects.delete --stage=GA --format=json
    mark folder-role
    if [ "$auth_mode" = ci ]; then
    record pool-create g iam workload-identity-pools create "$pool" --location=global --description="$owner" --format=json
    mark pool
    record provider-create g iam workload-identity-pools providers create-oidc github --location=global --workload-identity-pool="$pool" --issuer-uri=https://token.actions.githubusercontent.com --attribute-mapping="$mapping" --attribute-condition="$oidc" --description="$owner" --format=json
    mark provider
    fi
    record folder-create g storage managed-folders create "$folder" --format=json
    mark folder
    record folder-identity g storage managed-folders describe "$folder" --raw --format=json
    check_folder_identity "$RHIZA_AUTH_STATE/folder-identity.json"
    record folder-before folder_iam get
    if [ "$auth_mode" = ci ]; then record ci-wi-before g iam service-accounts get-iam-policy "$ci" --format=json; fi
    record data-wi-before g iam service-accounts get-iam-policy "$data" --format=json
    # Bootstrap namespace and admission BEFORE any CI runtime access.
    # No apply/adoption: a collision fails. Partial kubectl creation needs audit.
    record kubernetes-create k create -f "$RHIZA_AUTH_STATE/auth.yaml" -o json
    mark kubernetes
    record namespace-identity k get namespace "$ns" -o json
    for kind in validatingadmissionpolicy validatingadmissionpolicybinding; do
        for suffix in faults network-faults runtime; do
            record "$kind-$suffix-identity" k get "$kind" "$ns-$suffix" -o json
        done
    done
    for kind in clusterrole clusterrolebinding; do
        record "$kind-reader-identity" k get "$kind" "$ns-reader" -o json
    done
    if [ "$auth_mode" = local ]; then
        record runtime-ksa-identity k get serviceaccount rhiza-runtime --namespace="$ns" -o json
        record runtime-binding-identity k get rolebinding rhiza-ci-runtime --namespace="$ns" -o json
    fi
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
    if [ "$auth_mode" = ci ]; then
    record ci-project-grant g projects add-iam-policy-binding "$project" --member="serviceAccount:$ci" --role="projects/$project/roles/$cluster_role" --condition="$cluster_condition" --format=json
    mark ci-project-grant
    record ci-wi-grant g iam service-accounts add-iam-policy-binding "$ci" --member="$ci_member" --role=roles/iam.workloadIdentityUser --condition="$expiry" --format=json
    mark ci-wi-grant
    fi
    record data-wi-grant g iam service-accounts add-iam-policy-binding "$data" --member="$data_member" --role=roles/iam.workloadIdentityUser --condition="$expiry" --format=json
    mark data-wi-grant
    record folder-grant folder_iam add
    mark folder-grant
    record project-after g projects get-iam-policy "$project" --format=json
    record bucket-after g storage buckets get-iam-policy "gs://$bucket" --format=json
    record folder-after folder_iam get
    if [ "$auth_mode" = local ]; then mark local-prepared; fi
    printf '%s\n' "Prepared $ns ($auth_mode); scoped actor/GCS/admission checks still required before faults."
    exit 0
fi
# Rollback never deletes GCS objects. Executor must first prove fault/workload
# quiescence and complete exact-prefix cleanup; unknown in-flight outcomes stop.
[ "${RHIZA_ROLLBACK_QUIESCENT:-}" = YES_VERIFIED ] || die 'Fault/process and storage quiescence attestation required'
if [ -f "$RHIZA_AUTH_STATE/inflight" ]; then
    last=$(cat "$RHIZA_AUTH_STATE/inflight")
    [ -f "$RHIZA_AUTH_STATE/$last.exit" ] && [ "$(cat "$RHIZA_AUTH_STATE/$last.exit")" = 0 ] || die 'Unresolved operation outcome; manual exact-target audit required'
fi
if [ "$auth_mode" = local ]; then
    for credential in runtime.token runtime.kubeconfig; do
        [ ! -L "$RHIZA_AUTH_STATE/$credential" ] || die 'Local credential path replaced by symlink'
        rm -f "$RHIZA_AUTH_STATE/$credential"
    done
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
    check_folder_identity "$RHIZA_AUTH_STATE/folder-current.json"
    check_folder_role
    record folder-policy-current folder_iam get
    expected_folder_policy=$RHIZA_AUTH_STATE/folder-before.json
    if owned folder-grant; then expected_folder_policy=$RHIZA_AUTH_STATE/folder-after.json; fi
    jq -e --slurpfile expected "$expected_folder_policy" '(.bindings // [] | sort_by(.role,.condition.title)) == ($expected[0].bindings // [] | sort_by(.role,.condition.title))' "$RHIZA_AUTH_STATE/folder-policy-current.json" >/dev/null || die 'Folder policy changed; preserve concurrent bindings for review'
fi
# Only the exact live folder binding is changed with its current etag. No retries.
if owned folder-grant; then record revoke-folder folder_iam remove; fi
if owned ci-wi-grant; then record revoke-ci-wi g iam service-accounts remove-iam-policy-binding "$ci" --member="$ci_member" --role=roles/iam.workloadIdentityUser --condition="$expiry" --format=json; fi
if owned data-wi-grant; then record revoke-data-wi g iam service-accounts remove-iam-policy-binding "$data" --member="$data_member" --role=roles/iam.workloadIdentityUser --condition="$expiry" --format=json; fi
if owned ci-project-grant; then record revoke-project g projects remove-iam-policy-binding "$project" --member="serviceAccount:$ci" --role="projects/$project/roles/$cluster_role" --condition="$cluster_condition" --format=json; fi
if owned kubernetes; then
    # The preserved original bootstrap matched both fault kinds in -faults.
    # A reviewed transition records the added pair in a separate working
    # receipt copy. Never invent an identity for an absent/unrecorded pair.
    policy_suffixes='faults runtime'
    if [ -f "$RHIZA_AUTH_STATE/validatingadmissionpolicy-network-faults-identity.json" ] ||
       [ -f "$RHIZA_AUTH_STATE/validatingadmissionpolicybinding-network-faults-identity.json" ] ||
       grep -Fq "$ns-network-faults" "$RHIZA_AUTH_STATE/auth.yaml"; then
        policy_suffixes='faults network-faults runtime'
    fi
    for kind in validatingadmissionpolicybinding validatingadmissionpolicy; do
        for suffix in $policy_suffixes; do
            [ -s "$RHIZA_AUTH_STATE/$kind-$suffix-identity.json" ] || die 'Missing policy identity receipt; stop before namespace teardown'
        done
    done
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
        for suffix in $policy_suffixes; do
            uri=$(admission_uri "$kind" "$ns-$suffix")
            delete_kube "$kind-$suffix" "$kind" "$ns-$suffix" "$uri"
        done
    done
    for kind in clusterrolebinding clusterrole; do
        delete_kube "$kind-reader" "$kind" "$ns-reader" "/apis/rbac.authorization.k8s.io/v1/${kind}s/$ns-reader"
    done
fi
# No bucket/object recursive deletion or --all IAM removal is permitted here.
if owned folder; then
    record folder-predelete g storage managed-folders describe "$folder" --raw --format=json
    check_folder_identity "$RHIZA_AUTH_STATE/folder-predelete.json"
    check_folder_role
    record folder-policy-predelete folder_iam get
    jq -e --slurpfile expected "$RHIZA_AUTH_STATE/folder-before.json" '(.bindings // [] | sort_by(.role,.condition.title)) == ($expected[0].bindings // [] | sort_by(.role,.condition.title))' "$RHIZA_AUTH_STATE/folder-policy-predelete.json" >/dev/null || die 'Folder policy has unrelated changes; do not delete'
    # The installed CLI exposes no metageneration precondition. These immediate
    # metadata/IAM checks are not an atomic compare-and-delete; never claim CAS.
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
