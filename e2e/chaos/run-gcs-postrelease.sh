#!/bin/sh
# Exact candidate application + reviewed test-host overlay. Never a generation fencer.
set -eu
# Never trace credential references or rejected kubeconfig contents.
set +x
umask 077
requested_mode=${1:-}
case "$requested_mode" in render|run|run-local|check-shutdown) mode=$requested_mode ;;
  metadata-readiness-worker)
    mode=${RHIZA_METADATA_PARENT_MODE:-}
    case "$mode" in run|run-local) ;; *) printf '%s\n' 'trusted metadata readiness parent mode required' >&2; exit 1 ;; esac ;;
  *) printf '%s\n' 'usage: run-gcs-postrelease.sh render|run|run-local|check-shutdown' >&2; exit 1 ;;
esac
application_sha=19a326b64e30c841c3dac731a2bcef0633e30afb
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd -P)
die() { printf '%s\n' "$*" >&2; exit 1; }
sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256_output=$(sha256sum "$1") || return
  elif command -v shasum >/dev/null 2>&1; then
    sha256_output=$(shasum -a 256 "$1") || return
  else
    die 'Missing SHA-256 utility (sha256sum or shasum)'
  fi
  set -- $sha256_output
  [ "$#" -ge 1 ] && [ "${#1}" -eq 64 ] || return 1
  case "$1" in *[!a-f0-9]*) return 1 ;; esac
  printf '%s\n' "$1"
}
safe_name() { printf '%s' "$1" | LC_ALL=C grep -Eq '^[a-f0-9]{8}$'; }
: "${RHIZA_RUN_ID:?}" "${RHIZA_NAMESPACE:?}" "${RHIZA_HOST_IMAGE:?}" "${RHIZA_METADATA_IMAGE:?}"
: "${RHIZA_NODE_A:?}" "${RHIZA_NODE_B:?}" "${RHIZA_NODE_C:?}" "${RHIZA_OUTPUT:?}"
: "${RHIZA_AUTH_CREATION_SHA:?}"
: "${RHIZA_GCP_PROJECT_ID:?GCP project ID required}"
: "${RHIZA_GCP_PROJECT_NUMBER:?GCP project number required}"
: "${RHIZA_GKE_CLUSTER:?existing GKE cluster name required}"
: "${RHIZA_GKE_LOCATION:?existing GKE cluster location required}"
: "${RHIZA_KUBE_CONTEXT:?isolated kube context required}"
: "${RHIZA_GCS_BUCKET:?existing GCS bucket required}"
: "${RHIZA_GCS_PREFIX:?fresh run-specific GCS prefix required}"
: "${RHIZA_DATA_GSA_ID:?fresh data GSA account ID required}"
printf '%s' "$RHIZA_AUTH_CREATION_SHA" | LC_ALL=C grep -Eq '^[a-f0-9]{40}$' || die 'exact auth creation SHA required'
safe_name "$RHIZA_RUN_ID" || die 'unsafe run id'
printf '%s\n' "$RHIZA_GCP_PROJECT_ID" | LC_ALL=C grep -Eq '^[a-z][a-z0-9-]{4,28}[a-z0-9]$' || die 'invalid GCP project ID'
printf '%s\n' "$RHIZA_GCP_PROJECT_NUMBER" | LC_ALL=C grep -Eq '^[1-9][0-9]{5,19}$' || die 'invalid GCP project number'
printf '%s\n' "$RHIZA_GKE_CLUSTER" | LC_ALL=C grep -Eq '^[a-z][a-z0-9-]{0,38}[a-z0-9]$' || die 'invalid GKE cluster name'
printf '%s\n' "$RHIZA_GKE_LOCATION" | LC_ALL=C grep -Eq '^[a-z0-9]+(-[a-z0-9]+)+$' || die 'invalid GKE location'
printf '%s\n' "$RHIZA_KUBE_CONTEXT" | LC_ALL=C grep -Eq '^[A-Za-z0-9_.:@/-]+$' || die 'invalid kube context'
printf '%s\n' "$RHIZA_GCS_BUCKET" | LC_ALL=C grep -Eq '^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$' || die 'invalid GCS bucket'
printf '%s\n' "$RHIZA_GCS_PREFIX" | LC_ALL=C grep -Eq '^[A-Za-z0-9][A-Za-z0-9._/-]*/$' || die 'invalid GCS prefix'
case "$RHIZA_GCS_PREFIX" in *"/$RHIZA_RUN_ID/") ;; *) die 'GCS prefix must end in the fresh run ID' ;; esac
printf '%s\n' "$RHIZA_NAMESPACE" | LC_ALL=C grep -Eq '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$' || die 'invalid namespace'
printf '%s\n' "$RHIZA_DATA_GSA_ID" | LC_ALL=C grep -Eq '^[a-z][a-z0-9-]{4,28}[a-z0-9]$' || die 'invalid data GSA account ID'
case "$RHIZA_NAMESPACE" in *"$RHIZA_RUN_ID"*) ;; *) die 'namespace must contain the fresh run ID' ;; esac
case "$RHIZA_DATA_GSA_ID" in *"$RHIZA_RUN_ID"*) ;; *) die 'data GSA account ID must contain the fresh run ID' ;; esac
data_gsa_email="$RHIZA_DATA_GSA_ID@$RHIZA_GCP_PROJECT_ID.iam.gserviceaccount.com"
for node in "$RHIZA_NODE_A" "$RHIZA_NODE_B" "$RHIZA_NODE_C"; do
  printf '%s' "$node" | LC_ALL=C grep -Eq '^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$' || die 'existing-node pin required'
done
[ "$RHIZA_NODE_A" != "$RHIZA_NODE_B" ] && [ "$RHIZA_NODE_A" != "$RHIZA_NODE_C" ] && [ "$RHIZA_NODE_B" != "$RHIZA_NODE_C" ] || die 'three distinct nodes required'
printf '%s' "$RHIZA_HOST_IMAGE" | grep -Eq '^[a-z0-9][a-z0-9.-]+(/[a-z0-9._-]+)+@sha256:[a-f0-9]{64}$' || die 'host digest required'
printf '%s' "$RHIZA_METADATA_IMAGE" | grep -Eq '^gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:[a-f0-9]{64}$' || die 'reviewed metadata image digest required'
if [ "$requested_mode" = metadata-readiness-worker ]; then
  [ -d "$RHIZA_OUTPUT" ] && [ ! -L "$RHIZA_OUTPUT" ] || die 'worker output directory required'
else
  [ ! -e "$RHIZA_OUTPUT" ] || die 'output already exists; no rerun'
  mkdir -p "$RHIZA_OUTPUT"
fi
out=$(CDPATH='' cd -- "$RHIZA_OUTPUT" && pwd -P)
ns=$RHIZA_NAMESPACE
prefix=$RHIZA_GCS_PREFIX
cluster="gcs-$RHIZA_RUN_ID"
context=$RHIZA_KUBE_CONTEXT
remaining() {
  seconds=$((local_deadline - $(date +%s)))
  [ "$seconds" -gt 0 ] || return 124
  printf '%s\n' "$seconds"
}
k() {
  if [ -n "${metadata_wait_deadline:-}" ]; then
    metadata_seconds=$((metadata_wait_deadline - $(date +%s)))
    [ "$metadata_seconds" -gt 0 ] || return 124
  fi
  if [ -n "${voter_wait_deadline:-}" ]; then
    k_deadline_seconds=$((voter_wait_deadline - $(date +%s)))
    [ "$k_deadline_seconds" -gt 0 ] || return 124
  fi
  if [ "$mode" = run-local ]; then
    seconds=$(remaining) || return 124
    limit=240
    case "$1" in logs) limit=10 ;; get|auth|scale) limit=30 ;; exec) limit=60 ;; esac
    [ "$seconds" -le "$limit" ] || seconds=$limit
    if [ -n "${metadata_wait_deadline:-}" ] && [ "$seconds" -gt "$metadata_seconds" ]; then seconds=$metadata_seconds; fi
    if [ -n "${voter_wait_deadline:-}" ] && [ "$seconds" -gt "$k_deadline_seconds" ]; then seconds=$k_deadline_seconds; fi
    timeout --kill-after=5s "${seconds}s" kubectl --kubeconfig="$RHIZA_LOCAL_RUNTIME_KUBECONFIG" \
      --context="$context" --namespace="$ns" --request-timeout=30s "$@"
  else
    if [ -n "${metadata_wait_deadline:-}" ]; then
      timeout --kill-after=5s "${metadata_seconds}s" kubectl --context="$context" --namespace="$ns" "$@"
    elif [ -n "${voter_wait_deadline:-}" ]; then
      timeout --kill-after=5s "${k_deadline_seconds}s" kubectl --context="$context" --namespace="$ns" "$@"
    else
      kubectl --context="$context" --namespace="$ns" "$@"
    fi
  fi
}
metadata_credentials_ready_inner() (
  # IAM policy grants normally propagate in about two minutes, but can take
  # seven minutes or longer. Bound readiness to that documented seven-minute
  # window and never consume the existing 20-minute auth cleanup reserve.
  metadata_started=$(date +%s)
  metadata_wait_deadline=$((metadata_started + 420))
  if [ "$mode" = run-local ]; then
    metadata_scope_deadline=$((auth_expiry - 1200))
    [ "$metadata_wait_deadline" -le "$local_deadline" ] || metadata_wait_deadline=$local_deadline
    [ "$metadata_wait_deadline" -le "$metadata_scope_deadline" ] || metadata_wait_deadline=$metadata_scope_deadline
  fi
  if [ "$metadata_wait_deadline" -le "$metadata_started" ]; then
    printf '%s\n' 'stage=metadata-credential-readiness category=budget-exhausted'
    return 124
  fi
  jq -e --arg a "$RHIZA_NODE_A" --arg b "$RHIZA_NODE_B" --arg c "$RHIZA_NODE_C" --arg cluster_name "$RHIZA_GKE_CLUSTER" '
    . as $cluster | [$a,$b,$c] as $nodes | ($nodes|unique|length)==3 and
    all($nodes[]; . as $node |
      [$cluster.nodePools[]? | . as $pool | select($node | startswith("gke-" + $cluster_name + "-" + $pool.name + "-"))] as $selected |
      ($selected|length)==1 and $selected[0].workloadMetadataMode=="GKE_METADATA")
  ' "$RHIZA_CLUSTER_METADATA" >/dev/null || return 1
  printf '%s\n' 'stage=metadata-credential-readiness category=node-pool-gke-metadata'
  metadata_identity_code=0
  metadata_identity=$(k exec rhiza-metadata -- curl --silent --max-time 5 --max-filesize 1024 \
    --noproxy '*' -H 'Metadata-Flavor: Google' --write-out '\n%{http_code}' \
    http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email 2>/dev/null) || metadata_identity_code=$?
  metadata_identity_http=$(printf '%s\n' "$metadata_identity" | tail -n 1)
  metadata_identity_value=$(printf '%s\n' "$metadata_identity" | sed '$d')
  case "$metadata_identity_http" in [1-5][0-9][0-9]) ;; *) metadata_identity_http=000 ;; esac
  metadata_principal_identity="principal://iam.googleapis.com/projects/$RHIZA_GCP_PROJECT_NUMBER/locations/global/workloadIdentityPools/$RHIZA_GCP_PROJECT_ID.svc.id.goog/subject/ns/$ns/sa/rhiza-gcs"
  metadata_legacy_identity="rhiza-v0191-gcs-$RHIZA_RUN_ID.svc.id.goog"
  metadata_default_node_identity="$RHIZA_GCP_PROJECT_NUMBER-compute@developer.gserviceaccount.com"
  metadata_identity_category=unknown
  if [ "$metadata_identity_code" != 0 ]; then
    metadata_identity_category='native-error'
  elif [ "$metadata_identity_http" != 200 ]; then
    metadata_identity_category='http-error'
  else
    case "$metadata_identity_value" in
      "$data_gsa_email") metadata_identity_category='linked-gsa-email' ;;
      "$metadata_principal_identity") metadata_identity_category='principal-uri' ;;
      "$metadata_legacy_identity") metadata_identity_category='legacy-ksa-alias' ;;
      "$metadata_default_node_identity") metadata_identity_category='node-default' ;;
      *@*.iam.gserviceaccount.com) metadata_identity_category='other-gsa-email' ;;
      '') metadata_identity_category=empty ;;
    esac
  fi
  unset metadata_identity metadata_identity_value metadata_principal_identity metadata_legacy_identity metadata_default_node_identity
  printf 'stage=metadata-credential-readiness native=%s http=%s category=%s\n' \
    "$metadata_identity_code" "$metadata_identity_http" "$metadata_identity_category"
  [ "$metadata_identity_category" = linked-gsa-email ] || return 1
  metadata_attempt=0
  metadata_delay=5
  while :; do
    [ "$(date +%s)" -lt "$metadata_wait_deadline" ] || return 124
    metadata_attempt=$((metadata_attempt + 1))
    metadata_code=0
    # Keep the token response in memory only; neither curl nor kubectl stderr
    # is public evidence. Never pass the response as an argument to a process.
    metadata_response=$(k exec rhiza-metadata -- curl --silent --max-time 5 --max-filesize 16384 \
      --noproxy '*' -H 'Metadata-Flavor: Google' --write-out '\n%{http_code}' \
      http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token 2>/dev/null) || metadata_code=$?
    metadata_http=$(printf '%s\n' "$metadata_response" | tail -n 1 | tr -d '\015')
    case "$metadata_http" in [1-5][0-9][0-9]) ;; *) metadata_http=000 ;; esac
    metadata_category=unknown
    if [ "$metadata_code" = 0 ]; then
      case "$metadata_http" in
        200)
          if printf '%s\n' "$metadata_response" | sed '$d' | jq -s -e '
            length==1 and (.[0] | type=="object" and (.access_token|type)=="string" and (.access_token|length)>0 and
            .token_type=="Bearer" and (.expires_in|type)=="number" and .expires_in>0)
          ' >/dev/null 2>&1; then metadata_category=token-valid; else metadata_category=invalid-response; fi ;;
        403) metadata_category=propagation-pending ;;
      esac
    else
      case "$metadata_code" in
        124|137) metadata_category=deadline ;;
        *) metadata_category=native-error ;;
      esac
    fi
    unset metadata_response
    printf 'stage=metadata-credential-readiness attempt=%s native=%s http=%s elapsed=%s category=%s\n' \
      "$metadata_attempt" "$metadata_code" "$metadata_http" "$(( $(date +%s) - metadata_started ))" \
      "$metadata_category"
    case "$metadata_category" in
      token-valid)
        [ "$(date +%s)" -lt "$metadata_wait_deadline" ] || return 124
        metadata_post_code=0
        metadata_post=$(k exec rhiza-metadata -- curl --silent --max-time 5 --max-filesize 1024 \
          --noproxy '*' -H 'Metadata-Flavor: Google' --write-out '\n%{http_code}' \
          http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email 2>/dev/null) || metadata_post_code=$?
        metadata_post_http=$(printf '%s\n' "$metadata_post" | tail -n 1 | tr -d '\015')
        metadata_post_value=$(printf '%s\n' "$metadata_post" | sed '$d')
        case "$metadata_post_http" in [1-5][0-9][0-9]) ;; *) metadata_post_http=000 ;; esac
        if [ "$metadata_post_code" = 0 ] && [ "$metadata_post_http" = 200 ] && [ "$metadata_post_value" = "$data_gsa_email" ]; then
          metadata_post_category=linked-gsa-email
        else
          metadata_post_category='identity-drift'
        fi
        unset metadata_post metadata_post_value
        printf 'stage=metadata-credential-readiness post-token-native=%s post-token-http=%s category=%s\n' \
          "$metadata_post_code" "$metadata_post_http" "$metadata_post_category"
        [ "$metadata_post_category" = linked-gsa-email ] || return 1
        return 0 ;;
      propagation-pending) ;;
      deadline) return "$metadata_code" ;;
      *) return 1 ;;
    esac
    [ "$((metadata_wait_deadline - $(date +%s)))" -gt "$metadata_delay" ] || return 124
    sleep "$metadata_delay"
    [ "$metadata_delay" = 20 ] || metadata_delay=$((metadata_delay * 2))
  done
)
metadata_credentials_ready() {
  metadata_total_budget=420
  metadata_kill_grace=5
  metadata_budget=$((metadata_total_budget - metadata_kill_grace))
  if [ "$mode" = run-local ]; then
    metadata_budget_now=$(date +%s)
    metadata_runtime_budget=$((local_deadline - metadata_budget_now - metadata_kill_grace))
    metadata_auth_budget=$((auth_expiry - 1200 - metadata_budget_now - metadata_kill_grace))
    [ "$metadata_budget" -le "$metadata_runtime_budget" ] || metadata_budget=$metadata_runtime_budget
    [ "$metadata_budget" -le "$metadata_auth_budget" ] || metadata_budget=$metadata_auth_budget
  fi
  if [ "$metadata_budget" -le 0 ]; then
    printf '%s\n' 'stage=metadata-credential-readiness category=budget-exhausted'
    return 124
  fi
  RHIZA_METADATA_PARENT_MODE=$mode \
    RHIZA_CLUSTER_METADATA=$RHIZA_CLUSTER_METADATA \
    RHIZA_LOCAL_RUNTIME_KUBECONFIG=${RHIZA_LOCAL_RUNTIME_KUBECONFIG:-} \
    timeout --kill-after="${metadata_kill_grace}s" "${metadata_budget}s" /bin/sh "$script_dir/run-gcs-postrelease.sh" metadata-readiness-worker
}
validate_cluster_metadata() {
  jq -e --arg cluster "$RHIZA_GKE_CLUSTER" --arg location "$RHIZA_GKE_LOCATION" '
    type=="object" and (keys==["clusterCaSHA256","endpoint","location","name","nodePools"]) and
    .name==$cluster and .location==$location and
    (.endpoint|type)=="string" and (.endpoint|test("^[a-zA-Z0-9.-]+$")) and
    (.clusterCaSHA256|test("^[a-f0-9]{64}$"))' "$1" >/dev/null &&
    validate_node_pool_metadata "$1"
}
validate_node_pool_metadata() {
  jq -e --arg a "$RHIZA_NODE_A" --arg b "$RHIZA_NODE_B" --arg c "$RHIZA_NODE_C" --arg cluster_name "$RHIZA_GKE_CLUSTER" '
    . as $cluster |
    ([$a,$b,$c] as $nodes | ($nodes|unique|length)==3 and
      all($nodes[]; . as $node |
        [$cluster.nodePools[]? | . as $pool | select($node | startswith("gke-" + $cluster_name + "-" + $pool.name + "-"))] as $selected |
          ($selected|length)==1 and $selected[0].workloadMetadataMode=="GKE_METADATA"))
  ' "$1" >/dev/null
}
cluster_metadata_format='json(name,location,endpoint,masterAuth.clusterCaCertificate,nodePools.name,nodePools.config.workloadMetadataConfig.mode)'
sanitize_cluster_metadata() (
  projected=$1; output=$2
  ca_file=$projected.ca; safe_tmp=$output.tmp
  cleanup_cluster_projection() { rm -f "$ca_file" "$safe_tmp"; }
  trap cleanup_cluster_projection 0
  trap 'exit 130' HUP INT TERM
  [ ! -e "$output" ] && [ ! -L "$output" ] || return 1
  jq -e --arg cluster "$RHIZA_GKE_CLUSTER" --arg location "$RHIZA_GKE_LOCATION" '
    type=="object" and .name==$cluster and .location==$location and
    (.endpoint|type)=="string" and (.endpoint|test("^[a-zA-Z0-9.-]+$")) and
    (.masterAuth|keys)==["clusterCaCertificate"] and
    (.masterAuth.clusterCaCertificate|type)=="string" and (.masterAuth.clusterCaCertificate|length)>0 and
    (.nodePools|type)=="array" and all(.nodePools[]?;
      (keys==["config","name"]) and (.name|type)=="string" and
      (.config|keys)==["workloadMetadataConfig"] and
      (.config.workloadMetadataConfig|keys)==["mode"] and
      (.config.workloadMetadataConfig.mode|type)=="string")
  ' "$projected" >/dev/null || return 1
  jq -r .masterAuth.clusterCaCertificate "$projected" > "$ca_file" || return 1
  ca_hash=$(sha256_file "$ca_file") || return 1
  jq --arg ca_hash "$ca_hash" '
    {name,location,endpoint,clusterCaSHA256:$ca_hash,
     nodePools:[.nodePools[]? | {name,workloadMetadataMode:.config.workloadMetadataConfig.mode}]}
  ' "$projected" > "$safe_tmp" || return 1
  chmod 600 "$safe_tmp"
  mv "$safe_tmp" "$output"
)
prepare_cluster_metadata() {
  if [ "$mode" = run ]; then
    command -v gcloud >/dev/null || return 1
    cluster_metadata_tmp=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-cluster-metadata.XXXXXXXX") || return 1
    chmod 700 "$cluster_metadata_tmp"
    cluster_metadata_projected="$cluster_metadata_tmp/cluster-projected.json"
    cleanup_cluster_metadata_tmp() {
      rm -f "$cluster_metadata_projected"
      rmdir "$cluster_metadata_tmp"
    }
    trap cleanup_cluster_metadata_tmp 0 HUP INT TERM
    gcloud --project="$RHIZA_GCP_PROJECT_ID" --quiet container clusters describe "$RHIZA_GKE_CLUSTER" \
      --location="$RHIZA_GKE_LOCATION" --format="$cluster_metadata_format" > "$cluster_metadata_projected" 2>/dev/null || return $?
    chmod 600 "$cluster_metadata_projected"
    RHIZA_CLUSTER_METADATA="$out/cluster-metadata-validation.json"
    sanitize_cluster_metadata "$cluster_metadata_projected" "$RHIZA_CLUSTER_METADATA" || return 1
    validate_cluster_metadata "$RHIZA_CLUSTER_METADATA" || return 1
    sha256_file "$RHIZA_CLUSTER_METADATA" > "$out/cluster-metadata-validation.sha256"
    cleanup_cluster_metadata_tmp
    trap - 0 HUP INT TERM
  fi
  private_file "$RHIZA_CLUSTER_METADATA"
  cluster_metadata_checksum=${RHIZA_CLUSTER_METADATA%.json}.sha256
  private_file "$cluster_metadata_checksum"
  expected_cluster_metadata_checksum=$(cat "$cluster_metadata_checksum")
  printf '%s' "$expected_cluster_metadata_checksum" | grep -Eq '^[a-f0-9]{64}$' || return 1
  [ "$(sha256_file "$RHIZA_CLUSTER_METADATA")" = "$expected_cluster_metadata_checksum" ] || return 1
  validate_node_pool_metadata "$RHIZA_CLUSTER_METADATA"
}
wait_voters() (
  # One deadline covers creation, Ready and identity checks for all three Pods.
  # Subshell scope prevents this deadline from constraining later cleanup.
  voter_wait_deadline=$(( $(date +%s) + $1 ))
  for voter_wait_index in 0 1 2; do
    voter_wait_seconds=$((voter_wait_deadline - $(date +%s)))
    [ "$voter_wait_seconds" -gt 0 ] || exit 124
    k wait "pod/rhiza-voter-$voter_wait_index" --for=create --timeout="${voter_wait_seconds}s" || exit $?
  done
  voter_wait_before=$(k get pods rhiza-voter-0 rhiza-voter-1 rhiza-voter-2 -o json) || exit $?
  for voter_wait_index in 0 1 2; do
    voter_wait_seconds=$((voter_wait_deadline - $(date +%s)))
    [ "$voter_wait_seconds" -gt 0 ] || exit 124
    k wait "pod/rhiza-voter-$voter_wait_index" --for=condition=Ready --timeout="${voter_wait_seconds}s" || exit $?
  done
  voter_wait_after=$(k get pods rhiza-voter-0 rhiza-voter-1 rhiza-voter-2 -o json) || exit $?
  jq -n -e --argjson before "$voter_wait_before" --argjson after "$voter_wait_after" '
      ($after.items|sort_by(.metadata.name)) as $pods |
      ($pods|map(.metadata.name))==["rhiza-voter-0","rhiza-voter-1","rhiza-voter-2"] and
      ($pods|map({name:.metadata.name,uid:.metadata.uid}))==($before.items|map({name:.metadata.name,uid:.metadata.uid})|sort_by(.name)) and
      ($pods|map(.metadata.uid)|unique|length)==3 and
      all($pods[]; (.metadata.uid|type)=="string" and (.metadata.uid|length)>0 and
        .metadata.deletionTimestamp==null and any(.status.conditions[]?; .type=="Ready" and .status=="True"))
    ' || exit 1
  [ "$(date +%s)" -lt "$voter_wait_deadline" ] || exit 124
)
validate_archive_head_metadata() {
  # gcloud may emit size as a number; jq preserves its decoded value, not its JSON lexeme.
  jq -e --arg bucket "$RHIZA_GCS_BUCKET" --arg name "${prefix}${cluster}/archive/head.bin" '
    keys==["bucket","generation","name","size"] and .bucket==$bucket and .name==$name and
    (.generation|type)=="string" and (.generation|test("^[1-9][0-9]*$")) and
    (.size as $size |
      ((($size|type)=="string" and ($size|test("^(0|[1-9][0-9]*)$"))) or
       (($size|type)=="number" and $size == ($size|floor))) and
      (($size|tonumber)>=132) and (($size|tonumber)<=8388632))
  ' "$1" >/dev/null
}
validate_checkpoint_current_metadata() {
  jq -e --arg bucket "$RHIZA_GCS_BUCKET" --arg name "${prefix}${cluster}/checkpoint/CURRENT" '
    keys==["bucket","generation","name","size"] and .bucket==$bucket and .name==$name and
    (.generation|type)=="string" and (.generation|test("^[1-9][0-9]*$")) and
    (.size|tonumber)>0 and (.size|tonumber)==(.size|tonumber|floor)
  ' "$1" >/dev/null
}
same_archive_head_metadata() {
  jq -e --slurpfile before "$1" '.generation==$before[0].generation and (.size|tonumber)==($before[0].size|tonumber)' "$2" >/dev/null
}
same_object_evidence() {
  same_archive_head_metadata "$1" "$2" && [ "$(cat "$3")" = "$(cat "$4")" ]
}
validate_checkpoint_root_against_seal() {
  root=$1; head=$2
  jq -e --slurpfile head "$head" '
    def bytehex:
      if type!="array" or length!=32 or any(.[]; type!="number" or .<0 or .>255 or .!=floor) then error("invalid hash")
      else map(. as $b | "0123456789abcdef"[($b/16|floor):($b/16|floor)+1] + "0123456789abcdef"[($b%16):($b%16)+1]) | join("") end;
    type=="object" and keys==["config_id","files","generation","hash","index","size","sql_execution_policy"] and
    .config_id==$head[0].config_id and .index==$head[0].base and
    (.generation|type)=="number" and .generation>0 and .generation==(.generation|floor) and
    (.size|type)=="number" and .size>0 and .size==(.size|floor) and
    (.sql_execution_policy|type)=="number" and .sql_execution_policy>0 and .sql_execution_policy==(.sql_execution_policy|floor) and
    (.files|type)=="array" and (.files|length)>=1 and (.files|length)<=2 and
    all(.files[]; keys==["blocks","hash","role","size"] and
      (.role|type)=="string" and (.role|length)>0 and (.hash|type)=="string" and (.hash|test("^[a-f0-9]{64}$")) and
      (.size|type)=="number" and .size>0 and .size==(.size|floor) and (.blocks|type)=="array" and (.blocks|length)>0 and
      all(.blocks[]; (keys==["hash","size"] or keys==["generation","hash","size"]) and
        (.hash|type)=="string" and (.hash|test("^[a-f0-9]{64}$")) and
        (.size|type)=="number" and .size>0 and .size==(.size|floor) and
        ((.generation // 1)|type)=="number" and (.generation // 1)>0 and
        (.generation // 1)==((.generation // 1)|floor))) and
    (.hash|bytehex)==$head[0].base_state_hash
  ' "$root" >/dev/null
}
validate_current_against_archive() {
  current=$1; head=$2
  jq -e --slurpfile head "$head" '
    keys==["index","root_hash"] and (.index|type)=="number" and .index>0 and .index==(.index|floor) and
    (.root_hash|type)=="string" and (.root_hash|test("^[a-f0-9]{64}$")) and
    .index<=$head[0].base and (.index!=$head[0].base or .root_hash==$head[0].base_root_hash)
  ' "$current" >/dev/null
}
normalize_object_inventory() {
  jq -e '
    if type!="array" or any(.[]; .type!="cloud_object" or (.metadata|type)!="object") then
      error("invalid gcloud object inventory")
    else
      [ .[] | {name:.metadata.name,generation:.metadata.generation,size:(.metadata.size|tonumber)} ] |
      if any(.[]; (.name|type)!="string" or (.generation|type)!="string" or
          (.generation|test("^[1-9][0-9]*$")|not) or .size<0 or .size!=(.size|floor)) then
        error("invalid object identity")
      else sort_by(.name) end
    end
  ' "$1"
}
redacted_storage_json() {
  k exec rhiza-metadata -- gcloud storage cat "$1" | jq -e 'if type=="object" then del(.token) else error("stored coordination record is not an object") end'
}
decode_archive_head() {
  k exec -i rhiza-inspector -- rhiza qualification-archive-head < "$1"
}
preserve_run_output() {
  [ -f "$out/$seq-$1.stdout" ] || die "missing run output for $1"
  cp "$out/$seq-$1.stdout" "$2"
}
validate_cold_inventory() {
  before=$1; after=$2; pin_id=$3; status=$4; archive_head=$5; checkpoint_root=$6; gc_lock=$7; publisher=$8
  archive_pin_record=$9; checkpoint_pin_record=${10}; archive_head=${11}; learner_clock=${12}
  printf '%s' "$pin_id" | LC_ALL=C grep -Eq '^[a-f0-9]{64}$' || return 1
  archive_pin="${prefix}${cluster}/archive/recovery-pins/$pin_id"
  checkpoint_pin="${prefix}${cluster}/checkpoint/recovery-pins/$pin_id"
  gc_lock_name="${prefix}${cluster}/archive/GC_LOCK"
  publisher_name="${prefix}${cluster}/checkpoint/PUBLISHER"
  scope="${prefix}${cluster}/"
  jq -e --slurpfile after "$after" --arg archive_pin "$archive_pin" --arg checkpoint_pin "$checkpoint_pin" \
    --arg gc_lock "$gc_lock_name" --arg publisher "$publisher_name" --arg scope "$scope" '
    . as $before | $after[0] as $after |
    ($before|map(.name)|unique|length)==($before|length) and
    ($after|map(.name)|unique|length)==($after|length) and
    all(($before[],$after[]); .name|startswith($scope)) and
    ([$before[]|select(.name==$gc_lock)]|length)==1 and ([$after[]|select(.name==$gc_lock)]|length)==1 and
    ([$before[]|select(.name==$publisher)]|length)==1 and ([$after[]|select(.name==$publisher)]|length)==1 and
    ([$before[]|select(.name==$archive_pin or .name==$checkpoint_pin)]|length)==0 and
    ([$after[]|select(.name==$archive_pin)]|length)==1 and ([$after[]|select(.name==$checkpoint_pin)]|length)==1 and
    (([$after[]|select(.name==$gc_lock)][0].generation|tonumber) > ([$before[]|select(.name==$gc_lock)][0].generation|tonumber)) and
    (([$after[]|select(.name==$publisher)][0].generation|tonumber) > ([$before[]|select(.name==$publisher)][0].generation|tonumber)) and
    ([$before[]|select(.name!=$gc_lock and .name!=$publisher and .name!=$archive_pin and .name!=$checkpoint_pin)] ==
     [$after[]|select(.name!=$gc_lock and .name!=$publisher and .name!=$archive_pin and .name!=$checkpoint_pin)]) and
    ($after|length)==($before|length)+2
  ' "$before" >/dev/null || return 1
  owner=$(jq -er '.owner_id|select(test("^replica-rhiza-learner-[0-9a-f]{16}$"))' "$archive_pin_record") || return 1
  if command -v sha256sum >/dev/null 2>&1; then
    owner_hash_output=$(printf '%s' "$owner" | sha256sum) || return 1
  elif command -v shasum >/dev/null 2>&1; then
    owner_hash_output=$(printf '%s' "$owner" | shasum -a 256) || return 1
  else
    return 1
  fi
  set -- $owner_hash_output
  owner_hash=$1
  [ "$owner_hash" = "$pin_id" ] || return 1
  checkpoint_root_hash=$(sha256_file "$checkpoint_root") || return 1
  [ "$checkpoint_root_hash" = "$(jq -er '.base_root_hash|select(test("^[a-f0-9]{64}$"))' "$archive_head")" ] || return 1
  jq -n -e --slurpfile status "$status" --slurpfile root "$checkpoint_root" --slurpfile head "$archive_head" --slurpfile gc "$gc_lock" \
    --slurpfile publisher "$publisher" --slurpfile archive_pin "$archive_pin_record" \
    --slurpfile checkpoint_pin "$checkpoint_pin_record" --arg pin_id "$pin_id" --arg owner "$owner" \
    --argjson clock_ms "$learner_clock" '
    def positive_integer: type=="number" and .>0 and .==floor;
    ($status[0].recovery_pin_id==$pin_id and $status[0].recovery_pins_created==2 and $status[0].recovery_pins_released==2) and
    ($gc[0]|keys|sort)==["generation","lease_until_unix_ms","owner_id"] and
    $gc[0].owner_id==("snapshot:"+$owner) and ($gc[0].generation|positive_integer) and
    ($gc[0].lease_until_unix_ms|positive_integer) and $gc[0].lease_until_unix_ms<=$clock_ms and
    ($publisher[0]|keys|sort)==["config_id","generation","lease_until_unix_ms","owner_id","purpose","reserved_index"] and
    $publisher[0].owner_id=="checkpoint-gc" and $publisher[0].purpose=="maintenance" and
    ($publisher[0].config_id|positive_integer) and ($publisher[0].generation|positive_integer) and
    $publisher[0].reserved_index==0 and ($publisher[0].lease_until_unix_ms|positive_integer) and
    $publisher[0].lease_until_unix_ms<=$clock_ms and
    ($archive_pin[0]|keys|sort)==["base","lease_until_unix_ms","owner_id","tail_hash","tail_object","tip"] and
    $archive_pin[0].owner_id==$owner and $archive_pin[0].base==$head[0].base and
    $archive_pin[0].tip==$head[0].tip and $archive_pin[0].tail_hash==$head[0].tail_hash and
    $archive_pin[0].tail_object==$head[0].tail_object and
    ($archive_pin[0].tail_hash|type)=="array" and ($archive_pin[0].tail_hash|length)==32 and
    all($archive_pin[0].tail_hash[]; type=="number" and .>=0 and .<=255 and .==floor) and
    ($archive_pin[0].lease_until_unix_ms|positive_integer) and $archive_pin[0].lease_until_unix_ms<=$clock_ms and
    ($checkpoint_pin[0]|keys|sort)==["config_id","index","lease_until_unix_ms","owner_id","root","root_hash"] and
    $checkpoint_pin[0].owner_id==$owner and $checkpoint_pin[0].config_id==$root[0].config_id and
    $checkpoint_pin[0].index==$head[0].base and
    $checkpoint_pin[0].root_hash==$head[0].base_root_hash and
    ($root[0]|type)=="object" and $root[0].config_id==$head[0].config_id and $root[0].index==$head[0].base and
    $checkpoint_pin[0].root==$root[0] and
    ($checkpoint_pin[0].lease_until_unix_ms|positive_integer) and $checkpoint_pin[0].lease_until_unix_ms<=$clock_ms
  ' >/dev/null
}
validate_cold_restore_status() {
  status=$1; archive_head=$2; archive_metadata=$3; counters=$4
  jq -e --slurpfile head "$archive_head" --slurpfile archive "$archive_metadata" --slurpfile counters "$counters" '
    .mode=="object-store" and .source=="object-store" and .lag_slots==0 and
    .applied_slot==.source_tip and .source_tip==$head[0].tip and
    .checkpoint_base_slot==$head[0].base and .checkpoint_root_hash==$head[0].base_root_hash and
    .checkpoint_state_hash==$head[0].base_state_hash and .checkpoint_prefix_hash==$head[0].base_prefix_hash and
    .archive_head_version_type=="generation" and .archive_head_version==$archive[0].generation and
    (.recovery_pin_id|test("^[a-f0-9]{64}$")) and
    .recovery_pins_created==2 and .recovery_pins_released==2 and
    $counters[0].uploads>=(.recovery_pins_created+.recovery_pins_released) and
    $counters[0].deletes==0
  ' "$status" >/dev/null
}
validate_probe_pod() {
  probe_file=$1; probe_receipt=$2; probe_name=$3; probe_app=$4; probe_container=$5
  jq -e --slurpfile created "$probe_receipt" --arg ns "$ns" --arg run "$RHIZA_RUN_ID" --arg image "$RHIZA_HOST_IMAGE" --arg digest "${RHIZA_HOST_IMAGE##*@}" --arg node "$RHIZA_NODE_A" --arg name "$probe_name" --arg app "$probe_app" --arg container "$probe_container" '
    .metadata.name==$name and .metadata.namespace==$ns and .metadata.uid==$created[0].metadata.uid and
    .metadata.labels=={"app":$app,"chaos.rhiza.io/run":$run} and
    (.metadata.ownerReferences // [])==[] and .spec.serviceAccountName=="rhiza-inspector" and
    .spec.automountServiceAccountToken==false and .spec.hostNetwork==false and .spec.hostPID==false and .spec.hostIPC==false and
    .spec.nodeName==$node and .spec.restartPolicy=="Never" and (.spec.volumes // [])==[] and
    .spec.securityContext.runAsNonRoot==true and .spec.securityContext.runAsUser==65532 and
    .spec.securityContext.runAsGroup==65532 and .spec.securityContext.fsGroup==65532 and
    .spec.securityContext.seccompProfile.type=="RuntimeDefault" and
    (.spec.initContainers // [])==[] and (.spec.ephemeralContainers // [])==[] and
    (.spec.containers|length)==1 and (.spec.containers[0] as $c |
      $c.name==$container and $c.image==$image and $c.command==["sh","-c","sleep 1500"] and
      ($c.args // [])==[] and ($c.env // [])==[] and ($c.envFrom // [])==[] and ($c.ports // [])==[] and ($c.volumeMounts // [])==[] and
      ($c.readinessProbe // null)==null and ($c.livenessProbe // null)==null and
      ($c.startupProbe // null)==null and ($c.lifecycle // null)==null and ($c.workingDir // "")=="" and
      ($c.stdin // false)==false and ($c.stdinOnce // false)==false and ($c.tty // false)==false and
      $c.securityContext.allowPrivilegeEscalation==false and $c.securityContext.readOnlyRootFilesystem==true and
      $c.securityContext.runAsNonRoot==true and $c.securityContext.capabilities.drop==["ALL"]) and
    .status.phase=="Running" and (.status.containerStatuses|length)==1 and
    (.status.containerStatuses[0] as $s | $s.name==$container and $s.ready==true and $s.restartCount==0 and
      ($s.state.running|type)=="object" and ($s.imageID|endswith($digest)))
  ' "$probe_file" >/dev/null
}
validate_inspector_pod() {
  validate_probe_pod "$1" "$out/inspector-created.json" rhiza-inspector rhiza-inspector inspector
}
validate_inspector_control_pod() {
  validate_probe_pod "$1" "$out/inspector-control-created.json" rhiza-inspector-control rhiza-inspector-control control
}
validate_inspector_policy() {
  policy=$1
  jq -e --slurpfile created "$out/inspector-networkpolicy-bootstrap.json" --arg ns "$ns" --arg run "$RHIZA_RUN_ID" --arg owner "rhiza-postrelease-$RHIZA_RUN_ID-$RHIZA_AUTH_CREATION_SHA" '
    .metadata.name=="rhiza-inspector-deny-all" and .metadata.namespace==$ns and
    .metadata.uid==$created[0].metadata.uid and .metadata.labels=={"app":"rhiza-inspector","chaos.rhiza.io/run":$run} and
    .metadata.annotations=={"rhiza.dev/auth-owner":$owner} and
    (.metadata.ownerReferences // [])==[] and
    .spec.podSelector.matchLabels=={"app":"rhiza-inspector","chaos.rhiza.io/run":$run} and
    (.spec.policyTypes|sort)==["Egress","Ingress"] and .spec.ingress==[] and .spec.egress==[]
  ' "$policy" >/dev/null
}
validate_inspector_access() {
  ksa=$1; rolebindings=$2; clusterrolebindings=$3; policy=$4
  jq -e --arg ns "$ns" '
    .metadata.name=="rhiza-inspector" and .metadata.namespace==$ns and
    ((.metadata.annotations // {})|length)==0 and (.metadata.ownerReferences // [])==[] and
    .automountServiceAccountToken==false and (.secrets // [])==[] and (.imagePullSecrets // [])==[]
  ' "$ksa" >/dev/null || return 1
  jq -e --arg ns "$ns" '
    def forbidden:
      (.kind=="ServiceAccount" and .name=="rhiza-inspector" and ((.namespace // $ns)==$ns)) or
      (.kind=="User" and .name==("system:serviceaccount:"+$ns+":rhiza-inspector")) or
      (.kind=="Group" and (.name==("system:serviceaccounts:"+$ns) or .name=="system:serviceaccounts" or .name=="system:authenticated"));
    all(.items[]; all(.subjects[]?; forbidden|not))
  ' "$rolebindings" >/dev/null || return 1
  jq -e --arg ns "$ns" '
    def forbidden:
      (.kind=="ServiceAccount" and .name=="rhiza-inspector" and .namespace==$ns) or
      (.kind=="User" and .name==("system:serviceaccount:"+$ns+":rhiza-inspector")) or
      (.kind=="Group" and (.name==("system:serviceaccounts:"+$ns) or .name=="system:serviceaccounts" or .name=="system:authenticated"));
    all(.items[]; all(.subjects[]?; forbidden|not))
  ' "$clusterrolebindings" >/dev/null || return 1
  validate_inspector_policy "$policy"
}
validate_learner_pod() {
  jq -e --slurpfile created "$out/learner-created.json" --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '
    .metadata.name=="rhiza-learner" and .metadata.namespace==$ns and
    .metadata.labels["chaos.rhiza.io/run"]==$run and .metadata.uid==$created[0].metadata.uid and
    .metadata.labels["chaos.rhiza.io/role"]=="object-store" and
    (.metadata.uid|type)=="string" and (.metadata.uid|length)>0 and
    (.spec.volumes|length)==1 and .spec.volumes[0].name=="data" and
    (.spec.volumes[0]|keys)==["emptyDir","name"] and (.spec.volumes[0].emptyDir|type)=="object" and
    (.spec.containers|length)==1 and (.spec.containers[0] as $container |
      $container.name=="rhiza" and $container.readinessProbe.httpGet.path=="/ready" and
      $container.readinessProbe.httpGet.port=="http" and
      $container.envFrom==[{"configMapRef":{"name":"rhiza-config"}}] and
      ([$container.env[].name]|sort)==["RHIZA_CLUSTER_MEMBERS","RHIZA_ENABLE_RECONFIGURATION","RHIZA_NAMESPACE","RHIZA_NODE_ID","RHIZA_POD_UID","RHIZA_QUALIFICATION_RUN_ID","RHIZA_ROLE"] and
      ([$container.env[]|select(.name=="RHIZA_NAMESPACE" and .valueFrom.fieldRef.fieldPath=="metadata.namespace")]|length)==1 and
      ([$container.env[]|select(.name=="RHIZA_QUALIFICATION_RUN_ID" and .valueFrom.fieldRef.fieldPath=="metadata.labels['"'"'chaos.rhiza.io/run'"'"']")]|length)==1 and
      ([$container.env[]|select(.name=="RHIZA_POD_UID" and .valueFrom.fieldRef.fieldPath=="metadata.uid")]|length)==1 and
      ([$container.env[]|select(.name=="RHIZA_NODE_ID" and .value=="rhiza-learner")]|length)==1 and
      ([$container.env[]|select(.name=="RHIZA_ROLE" and .value=="object-store")]|length)==1 and
      ([$container.env[]|select(.name=="RHIZA_ENABLE_RECONFIGURATION" and .value=="true")]|length)==1 and
      ([$container.env[]|select(.name=="RHIZA_CLUSTER_MEMBERS" and .valueFrom.secretKeyRef.name=="rhiza-qualification-auth" and .valueFrom.secretKeyRef.key=="RHIZA_CLUSTER_MEMBERS")]|length)==1)
  ' "$1" >/dev/null
}
normalize_learner_create_ack() {
  jq -s -e --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '
    [.[] | if .kind=="List" then .items[] else . end] as $items |
    if ($items|length)==2 and
      ($items|map(.kind+"/"+.metadata.name)|sort)==["Pod/rhiza-learner","Service/rhiza-learner"] and
      all($items[]; .apiVersion=="v1" and .metadata.namespace==$ns and
        .metadata.labels["chaos.rhiza.io/run"]==$run and
        (.metadata.uid|type)=="string" and (.metadata.uid|length)>0)
    then {apiVersion:"v1",kind:"List",items:$items}
    else error("learner create identities unresolved") end
  ' "$1"
}
private_file() {
  case "$1" in /*) ;; *) die 'private absolute credential path required' ;; esac
  [ -f "$1" ] && [ ! -L "$1" ] || die 'regular non-symlink credential file required'
  parent=$(dirname -- "$1")
  [ ! -L "$parent" ] || die 'credential directory must not be a symlink'
  if [ "$(uname -s)" = Darwin ]; then
    permissions=$(stat -f '%Lp' "$1"); file_owner=$(stat -f '%u' "$1")
    directory_permissions=$(stat -f '%Lp' "$parent")
  else
    permissions=$(stat -c '%a' "$1"); file_owner=$(stat -c '%u' "$1")
    directory_permissions=$(stat -c '%a' "$parent")
  fi
  [ "$permissions" = 600 ] && [ "$file_owner" = "$(id -u)" ] && [ "$directory_permissions" = 700 ] || die 'private credential ownership/permissions required'
}
prepare_workload_identity_receipt() {
  wi_receipt=$runtime_receipt_dir/workload-identity.json
  wi_checksum=$runtime_receipt_dir/workload-identity.sha256
  data_create_receipt=$runtime_receipt_dir/data-create.json
  data_pre_policy_receipt=$runtime_receipt_dir/data-wi-before.json
  data_policy_receipt=$runtime_receipt_dir/data-wi-grant.json
  private_file "$wi_receipt"
  private_file "$wi_checksum"
  private_file "$data_create_receipt"
  private_file "$data_pre_policy_receipt"
  private_file "$data_policy_receipt"
  expected_wi_checksum=$(cat "$wi_checksum")
  printf '%s' "$expected_wi_checksum" | grep -Eq '^[a-f0-9]{64}$' || return 1
  [ "$(sha256_file "$wi_receipt")" = "$expected_wi_checksum" ] || return 1
  wi_data=$data_gsa_email
  wi_owner="rhiza-postrelease-$RHIZA_RUN_ID-$RHIZA_AUTH_CREATION_SHA"
  wi_member="serviceAccount:$RHIZA_GCP_PROJECT_ID.svc.id.goog[$ns/rhiza-gcs]"
  wi_title="rhiza-$RHIZA_RUN_ID-expiry"
  wi_expression="request.time < timestamp('$RHIZA_AUTH_EXPIRES')"
  jq -e --arg data "$wi_data" --arg owner "$wi_owner" --arg name "projects/$RHIZA_GCP_PROJECT_ID/serviceAccounts/$wi_data" '
    .email==$data and .name==$name and .description==$owner and
    (.uniqueId|type)=="string" and (.uniqueId|length)>0
  ' "$data_create_receipt" >/dev/null || return 1
  jq -e '((has("bindings")|not) or .bindings==[])' "$data_pre_policy_receipt" >/dev/null || return 1
  jq -e --arg member "$wi_member" --arg title "$wi_title" --arg expression "$wi_expression" '
    (.bindings|type)=="array" and (.bindings|length)==1 and
    (.bindings[0]|keys)==["condition","members","role"] and
    .bindings[0].role=="roles/iam.workloadIdentityUser" and .bindings[0].members==[$member] and
    (.bindings[0].condition|keys)==["expression","title"] and
    .bindings[0].condition=={title:$title,expression:$expression}
  ' "$data_policy_receipt" >/dev/null || return 1
  jq -e --arg run "$RHIZA_RUN_ID" --arg ns "$ns" \
    --arg data "$wi_data" --arg owner "$wi_owner" --arg member "$wi_member" \
    --arg title "$wi_title" --arg expression "$wi_expression" --slurpfile created "$data_create_receipt" '
    keys==["gsa","ksa","namespace","run"] and .run==$run and .namespace==$ns and
    (.ksa|keys)==["gcp_service_account","name","return_principal_id_as_email","uid"] and
    .ksa.name=="rhiza-gcs" and (.ksa.uid|type)=="string" and (.ksa.uid|length)>0 and
    .ksa.gcp_service_account==$data and .ksa.return_principal_id_as_email==true and
    (.gsa|keys)==["condition","email","member","owner","role","unique_id"] and .gsa.email==$data and
    .gsa.unique_id==$created[0].uniqueId and .gsa.owner==$owner and
    .gsa.member==$member and .gsa.role=="roles/iam.workloadIdentityUser" and
    (.gsa.condition|keys)==["expression","title"] and
    .gsa.condition.title==$title and .gsa.condition.expression==$expression
  ' "$wi_receipt" >/dev/null
}
sed -e "s|__NAMESPACE__|$ns|g" -e "s|__RUN_ID__|$RHIZA_RUN_ID|g" \
  -e "s|__CLUSTER_ID__|$cluster|g" -e "s|__HOST_IMAGE__|$RHIZA_HOST_IMAGE|g" \
  -e "s|__GCS_BUCKET__|$RHIZA_GCS_BUCKET|g" -e "s|__GCS_PREFIX__|$RHIZA_GCS_PREFIX|g" \
  -e "s|__METADATA_IMAGE__|$RHIZA_METADATA_IMAGE|g" \
  -e "s|__NODE_A__|$RHIZA_NODE_A|g" -e "s|__NODE_B__|$RHIZA_NODE_B|g" -e "s|__NODE_C__|$RHIZA_NODE_C|g" \
  "$script_dir/gcs-postrelease.yaml.in" > "$out/rendered.yaml"
# Template split is explicit: neither cold helper can start before cold authority.
awk '/^# BEGIN COLD (LEARNER|INSPECTOR)/{cold=1;next} /^# END COLD (LEARNER|INSPECTOR)/{cold=0;next} !cold' "$out/rendered.yaml" > "$out/initial.yaml"
awk '/^# BEGIN COLD LEARNER/{cold=1;next} /^# END COLD LEARNER/{cold=0;next} cold' "$out/rendered.yaml" > "$out/learner.yaml"
awk '/^# BEGIN COLD INSPECTOR/{cold=1;next} /^# END COLD INSPECTOR/{cold=0;next} cold' "$out/rendered.yaml" > "$out/inspector.yaml"
awk 'BEGIN{RS="---\n"} /kind: NetworkPolicy/ && /name: rhiza-inspector-deny-all/{print $0 "---"}' "$out/inspector.yaml" > "$out/inspector-networkpolicy.yaml"
awk 'BEGIN{RS="---\n"} /kind: Pod/ && /name: rhiza-inspector/{print $0 "---"}' "$out/inspector.yaml" > "$out/inspector-pod.yaml"
sed -e 's/^  name: rhiza-inspector$/  name: rhiza-inspector-control/' \
  -e "s/app: 'rhiza-inspector'/app: 'rhiza-inspector-control'/" \
  -e 's/^    - name: inspector$/    - name: control/' \
  "$out/inspector-pod.yaml" > "$out/inspector-control-pod.yaml"
awk '$0=="  name: rhiza-inspector-control"{n++} END{exit n!=1}' "$out/inspector-control-pod.yaml" &&
  awk -v expected="  labels: {chaos.rhiza.io/run: '$RHIZA_RUN_ID', app: 'rhiza-inspector-control'}" '$0==expected{n++} END{exit n!=1}' "$out/inspector-control-pod.yaml" &&
  awk '$0=="  serviceAccountName: rhiza-inspector"{n++} END{exit n!=1}' "$out/inspector-control-pod.yaml" &&
  awk '$0=="    - name: control"{n++} END{exit n!=1}' "$out/inspector-control-pod.yaml" ||
  die 'inspector control Pod render contract mismatch'
awk 'BEGIN{RS="---\n"} /name: rhiza-metadata/{print}' "$out/initial.yaml" > "$out/metadata.yaml"
awk 'BEGIN{RS="---\n"} !/name: rhiza-metadata/{print $0 "---"}' "$out/initial.yaml" > "$out/voters.yaml"
shutdown_events() {
  jq -s -e --slurpfile before "$1/shutdown-before.json" '
    . as $events |
    if any($events[]; .type=="ERROR") then error("watch ERROR event")
    elif any($before[0][]; . as $pod | any($events[];
      .object.metadata.name==$pod.name and (.object.metadata.uid!=$pod.uid or
        any(.object.status.containerStatuses[]?; .name=="rhiza" and
          (.containerID!=$pod.container_id or .imageID!=$pod.image_id or .restartCount!=$pod.restart_count or
            (.state.terminated!=null and (.state.terminated.exitCode!=0 or (.state.terminated.signal // 0)!=0)))))))
      then error("watch writer incarnation/termination changed")
    elif all($before[0][]; . as $pod | any($events[]; .object.metadata.uid==$pod.uid and
      any(.object.status.containerStatuses[]?; .name=="rhiza" and .containerID==$pod.container_id and
        .imageID==$pod.image_id and .restartCount==$pod.restart_count and
        .state.terminated.exitCode==0 and (.state.terminated.signal // 0)==0)))
      then $events else empty end
  ' "$1/shutdown-watch.json"
}
wait_shutdown_watch() {
  # Partial JSON while this stream is still writing is pending, never proof.
  # Preserve the first parser error; EOF/error/deadline without full events fails.
  watch_finished=false
  while :; do
    shutdown_events "$1" > "$1/shutdown-watch-proof.next.json" 2> "$1/shutdown-watch-parse.stderr"
    watch_parse_code=$?
    if [ -s "$1/shutdown-watch-parse.stderr" ] && [ ! -e "$1/shutdown-watch-first-parse.stderr" ]; then
      cp "$1/shutdown-watch-parse.stderr" "$1/shutdown-watch-first-parse.stderr"
      printf '%s\n' "$watch_parse_code" > "$1/shutdown-watch-first-parse.exit"
    fi
    if [ "$watch_parse_code" = 0 ]; then
      mv "$1/shutdown-watch-proof.next.json" "$1/shutdown-watch-proof.json"
      return 0
    fi
    case "$watch_parse_code" in
      4) ;; # Complete JSON, terminal events not yet present.
      5) grep -q '^jq: parse error:' "$1/shutdown-watch-parse.stderr" || return 1 ;;
      *) return 1 ;;
    esac
    [ ! -s "$1/shutdown-watch.stderr" ] || return 1
    [ "$watch_finished" = false ] || return 1
    [ "$(date +%s)" -lt "$3" ] || return 124
    # A stream can finish between parsing and observing EOF. Re-read ONCE at
    # EOF so complete final bytes are not discarded; invalid bytes still fail.
    if [ -n "$2" ]; then
      if ! kill -0 "$2" 2>/dev/null; then watch_finished=true; continue; fi
    elif [ -e "$1/shutdown-watch.exit" ]; then watch_finished=true; continue; fi
    sleep 1
  done
}
verify_shutdown() {
  proof_dir=$1
  case "$(cat "$proof_dir/shutdown-watch.exit")" in 0|143) ;; *) return 1 ;; esac
  jq -e '.items|length==0' "$proof_dir/shutdown-writers-after.json" >/dev/null || return 1
  jq -e '.spec.replicas==0 and (.status.replicas // 0)==0' "$proof_dir/shutdown-controller.json" >/dev/null || return 1
  jq -e --slurpfile expected "$proof_dir/shutdown-expected.json" --arg ns "$ns" --arg run "$RHIZA_RUN_ID" --arg image "$RHIZA_HOST_IMAGE" '
    ($expected[0]|sort_by(.name)) as $expected |
    ($expected|map(.name)) as $names |
    ($names==["rhiza-voter-0","rhiza-voter-1","rhiza-voter-2"] or
      $names==["rhiza-learner","rhiza-voter-0","rhiza-voter-1","rhiza-voter-2"]) and
    all($expected[]; .namespace==$ns and .run==$run and (.uid|type)=="string" and (.uid|length)>0) and
    (map({name,namespace,run,uid})|sort_by(.name))==$expected and
    type=="array" and all(.[]; .namespace==$ns and .run==$run and
      (.uid|type)=="string" and (.uid|length)>0 and (.container_id|type)=="string" and (.container_id|length)>0 and
      .image==$image and (.image_id|endswith($image|split("@")[1])) and
      (.name|test("^rhiza-voter-[012]$|^rhiza-learner$")) and .restart_count>=0) and
    ([.[].uid]|unique|length)==length
  ' "$proof_dir/shutdown-before.json" >/dev/null || return 1
  # Use the complete parsed stream captured before intentionally stopping it.
  [ -s "$proof_dir/shutdown-watch-proof.json" ] || return 1
  for proof_pod in $(jq -r '.[].name' "$proof_dir/shutdown-before.json"); do
    [ "$(cat "$proof_dir/$proof_pod-shutdown-log.exit")" = 0 ] || return 1
    jq -R -s -e --arg name "$proof_pod" --slurpfile before "$proof_dir/shutdown-before.json" '
      ($before[0][]|select(.name==$name)) as $pod |
      [split("\n")[]|fromjson?|select(.namespace==$pod.namespace and .run==$pod.run and
        .pod_uid==$pod.uid and .node_id==$pod.name)] as $markers |
      [$markers[]|select(.event=="qualification-host-start" and (.process|test("^[a-f0-9]{32}$")))] as $starts |
      ($starts|length)==1 and
      ([$markers[]|select(.event=="qualification-host-shutdown-failed")]|length)==0 and
      ([$markers[]|select(.event=="qualification-host-shutdown-complete" and .process==$starts[0].process and
        .http_drained==true and .db_closed==true)]|length)==1
    ' "$proof_dir/$proof_pod-shutdown.log" >/dev/null || return 1
  done
}
if [ "$mode" = check-shutdown ]; then
  : "${RHIZA_SHUTDOWN_FIXTURE:?offline proof directory required}"
  if [ "${RHIZA_SHUTDOWN_WAIT:-}" = yes ]; then
    set +e
    wait_shutdown_watch "$RHIZA_SHUTDOWN_FIXTURE" '' "$(( $(date +%s) + 5 ))"
    wait_code=$?
    [ "$wait_code" = 0 ] || exit "$wait_code"
  else
    shutdown_events "$RHIZA_SHUTDOWN_FIXTURE" > "$RHIZA_SHUTDOWN_FIXTURE/shutdown-watch-proof.json"
  fi
  verify_shutdown "$RHIZA_SHUTDOWN_FIXTURE"
  exit $?
fi
if [ "$mode" = render ]; then exit 0; fi
if [ "$mode" = run ]; then
  [ "${GITHUB_ACTIONS:-}" = true ] && [ "${GITHUB_EVENT_NAME:-}" = workflow_dispatch ] && [ "${GITHUB_REF:-}" = refs/heads/main ] || die 'qualification must run in approved CI'
  [ "${RHIZA_EXECUTION_GO:-}" = "$GITHUB_SHA:$RHIZA_RUN_ID" ] || die 'exact harness/run GO required'
else
  [ -z "${GITHUB_ACTIONS:-}${GITHUB_EVENT_NAME:-}${GITHUB_REF:-}${GITHUB_SHA:-}" ] || die 'local entry must not impersonate CI'
  : "${RHIZA_HARNESS_SHA:?}" "${RHIZA_AUTH_STATE:?}" "${RHIZA_LOCAL_RUNTIME_KUBECONFIG:?}"
  : "${RHIZA_LOCAL_API_SERVER:?}" "${RHIZA_LOCAL_CA_DATA:?}"
  : "${RHIZA_AUTH_STARTED_EPOCH:?}" "${RHIZA_AUTH_EXPIRES:?}" "${RHIZA_LOCAL_TOKEN_EXPIRES:?}"
  printf '%s' "$RHIZA_HARNESS_SHA" | grep -Eq '^[a-f0-9]{40}$' || die 'exact local harness SHA required'
  [ "$(git -C "$script_dir/../.." rev-parse HEAD)" = "$RHIZA_HARNESS_SHA" ] || die 'local harness HEAD mismatch'
  [ "${RHIZA_EXECUTION_GO:-}" = "$RHIZA_HARNESS_SHA:$RHIZA_RUN_ID" ] || die 'exact local harness/run GO required'
  command -v timeout >/dev/null || die 'native GNU timeout required'
  timeout --version | grep -Fq 'GNU coreutils' || die 'native GNU timeout required'
  case "$RHIZA_AUTH_STATE" in /*) ;; *) die 'private absolute auth state required' ;; esac
  [ -d "$RHIZA_AUTH_STATE" ] && [ ! -L "$RHIZA_AUTH_STATE" ] || die 'private auth state directory required'
  runtime_receipt_dir=$(CDPATH='' cd -- "$RHIZA_AUTH_STATE" && pwd -P)
  prepare_workload_identity_receipt || die 'trusted workload identity binding receipt rejected'
  supplied_kubeconfig=$(CDPATH='' cd -- "$(dirname -- "$RHIZA_LOCAL_RUNTIME_KUBECONFIG")" && pwd -P)/$(basename -- "$RHIZA_LOCAL_RUNTIME_KUBECONFIG")
  [ "$supplied_kubeconfig" = "$runtime_receipt_dir/runtime.kubeconfig" ] || die 'runtime kubeconfig must be the mint-local receipt'
  RHIZA_LOCAL_RUNTIME_KUBECONFIG=$supplied_kubeconfig
  private_file "$RHIZA_LOCAL_RUNTIME_KUBECONFIG"
  RHIZA_CLUSTER_METADATA="$runtime_receipt_dir/mint-cluster.json"
  # Native parsing stays in memory, never artifacts/logs. A rejected config may
  # contain credentials, so suppress parser diagnostics and disclose only cause.
  if config=$(timeout --kill-after=5s 5s kubectl --kubeconfig="$RHIZA_LOCAL_RUNTIME_KUBECONFIG" \
    --context="$context" --namespace="$ns" config view --raw -o json 2>/dev/null); then
    :
  else
    parser_exit=$?
    die "local kubeconfig parsing failed (exit $parser_exit)"
  fi
  printf '%s' "$config" | jq -e --arg context "$context" --arg ns "$ns" \
    --arg server "$RHIZA_LOCAL_API_SERVER" --arg ca "$RHIZA_LOCAL_CA_DATA" '
    .kind=="Config" and .apiVersion=="v1" and .["current-context"]==$context and
    (.contexts|length)==1 and (.clusters|length)==1 and (.users|length)==1 and
    .contexts[0].name==$context and .contexts[0].context.namespace==$ns and
    (.contexts[0].context|keys)==["cluster","namespace","user"] and
    .contexts[0].context.cluster==.clusters[0].name and .contexts[0].context.user==.users[0].name and
    (.clusters[0].cluster|keys)==["certificate-authority-data","server"] and
    (.clusters[0].cluster.server==$server and ($server|startswith("https://"))) and
    .clusters[0].cluster["certificate-authority-data"]==$ca and ($ca|length)>0 and
    (.users[0].user|keys)==["tokenFile"] and (.users[0].user.tokenFile|type)=="string"
  ' >/dev/null || die 'local kubeconfig must be one pinned tokenFile-only identity'
  token_file=$(printf '%s' "$config" | jq -r '.users[0].user.tokenFile')
  unset config
  private_file "$token_file"
  [ -s "$token_file" ] || die 'local token file is empty'
  printf '%s' "$RHIZA_AUTH_STARTED_EPOCH" | grep -Eq '^[1-9][0-9]{0,11}$' || die 'fixed auth start required'
  auth_expiry=$(jq -ner --arg value "$RHIZA_AUTH_EXPIRES" '$value|fromdateiso8601') || die 'fixed auth expiry required'
  token_expiry=$(jq -ner --arg value "$RHIZA_LOCAL_TOKEN_EXPIRES" '$value|fromdateiso8601') || die 'actual token expiry required'
  now=$(date +%s)
  [ "$((auth_expiry - RHIZA_AUTH_STARTED_EPOCH))" -eq 3300 ] && \
    [ "$now" -ge "$RHIZA_AUTH_STARTED_EPOCH" ] && [ "$((now - RHIZA_AUTH_STARTED_EPOCH))" -le 900 ] && \
    [ "$token_expiry" -le "$auth_expiry" ] && [ "$((token_expiry - now))" -ge 2400 ] || die 'fixed 55-minute scope/preparation/runtime/cleanup reserve invalid'
  local_deadline=$((now + 1200))
fi
[ "${RHIZA_APPLICATION_SHA:-}" = "$application_sha" ] || die 'candidate application mismatch'
: "${RHIZA_APPROVED_CAP_KRW:?}" "${RHIZA_BOOTSTRAP_UID:?}"
[ "$mode" = run ] || : "${RHIZA_CLUSTER_METADATA:?cluster metadata receipt required}"
prepare_cluster_metadata || die 'trusted cluster metadata preparation failed'
[ "$RHIZA_APPROVED_CAP_KRW" = 10000 ] || die 'this run requires the approved 10000 KRW cap'
if [ "$requested_mode" = metadata-readiness-worker ]; then
  metadata_credentials_ready_inner
  exit $?
fi
seq=0
run() {
  seq=$((seq + 1)); label=$1; shift
  set +e
  if [ "$mode" = run-local ] && [ "$1" = curl ]; then
    seconds=$(remaining)
    code=$?
    if [ "$code" = 0 ]; then timeout --kill-after=5s "${seconds}s" "$@" > "$out/$seq-$label.stdout" 2> "$out/$seq-$label.stderr"; code=$?; fi
  else
    "$@" > "$out/$seq-$label.stdout" 2> "$out/$seq-$label.stderr"
    code=$?
  fi
  set -e
  printf '%s\n' "$code" > "$out/$seq-$label.exit"
  if [ "$code" != 0 ]; then
    if [ "$mode" = run-local ]; then printf 'first failure: %s exit %s\n' "$label" "$code" >&2; exit "$code"; fi
    die "first failure: $label exit $code"
  fi
}
reject_server_dry_run() {
  label=$1; manifest=$2
  seq=$((seq + 1))
  set +e
  k create --dry-run=server -f "$manifest" -o json > "$out/$seq-$label.stdout" 2> "$out/$seq-$label.stderr"
  code=$?
  set -e
  printf '%s\n' "$code" > "$out/$seq-$label.exit"
  [ "$code" != 0 ] && [ ! -s "$out/$seq-$label.stdout" ] &&
    grep -Fq "$ns-runtime" "$out/$seq-$label.stderr" ||
    die "negative server dry-run unexpectedly admitted: $label"
}
reject_runtime_networkpolicy_mutation() {
  label=$1; shift
  seq=$((seq + 1))
  set +e
  k "$@" > "$out/$seq-$label.stdout" 2> "$out/$seq-$label.stderr"
  code=$?
  set -e
  printf '%s\n' "$code" > "$out/$seq-$label.exit"
  [ "$code" != 0 ] && [ ! -s "$out/$seq-$label.stdout" ] &&
    grep -Eiq 'forbidden|cannot (create|update|patch|delete)' "$out/$seq-$label.stderr" ||
    die "runtime NetworkPolicy mutation unexpectedly admitted: $label"
}
parse_https_443() {
  api_server_url=$1
  case "$api_server_url" in https://*) api_authority=${api_server_url#https://} ;; *) return 1 ;; esac
  [ -n "$api_authority" ] || return 1
  case "$api_authority" in */*|*\?*|*\#*|*@*|*[[:space:]]*) return 1 ;; esac
  case "$api_authority" in
    \[*\]*)
      api_host=${api_authority#\[}
      api_host=${api_host%%\]*}
      api_suffix=${api_authority#*\]}
      case "$api_suffix" in '') api_port=443 ;; :443) api_port=443 ;; *) return 1 ;; esac
      case "$api_host" in ''|*\[*|*\]*) return 1 ;; esac
      ;;
    \[* ) return 1 ;;
    *:*)
      api_host=${api_authority%:*}
      api_port=${api_authority##*:}
      case "$api_host" in ''|*:*) return 1 ;; esac
      [ "$api_port" = 443 ] || return 1
      ;;
    *) api_host=$api_authority; api_port=443 ;;
  esac
  [ -n "$api_host" ] && [ "$api_port" = 443 ]
}
probe_inspector_control_egress() {
  probe_host=$1; probe_port=$2; probe_phase=$3
  k exec rhiza-inspector-control -- sh -c '
    busybox nc -w 3 "$1" "$2" </dev/null >/dev/null 2>&1 || exit 43
    printf "paired-network-policy-proof=control-reachable phase=%s target=kubernetes-api host=%s port=%s\n" "$3" "$1" "$2"
  ' sh "$probe_host" "$probe_port" "$probe_phase"
}
probe_inspector_egress() {
  probe_host=$1; probe_port=$2
  k exec rhiza-inspector -- sh -c '
    started=$(date +%s)
    if busybox nc -w 3 "$1" "$2" </dev/null >/dev/null 2>&1; then exit 41; fi
    elapsed=$(($(date +%s)-started))
    [ "$elapsed" -ge 2 ] || exit 42
    printf "paired-network-policy-proof=inspector-denied elapsed_seconds=%s target=kubernetes-api host=%s port=%s\n" "$elapsed" "$1" "$2"
  ' sh "$probe_host" "$probe_port"
}
verify_inspector_network_isolation() {
  parse_https_443 "$1" || die 'exact HTTPS Kubernetes API endpoint on port 443 required for isolation probe'
  run inspector-control-before-reachable probe_inspector_control_egress "$api_host" "$api_port" before
  run inspector-egress-denied probe_inspector_egress "$api_host" "$api_port"
  run inspector-control-after-reachable probe_inspector_control_egress "$api_host" "$api_port" after
}
started=${RHIZA_JOB_STARTED:-$(date +%s)}
[ "$mode" != run-local ] || started=$now
watchdog=''
stop_watchdog() {
  if [ -n "$watchdog" ]; then kill "$watchdog" 2>/dev/null || true; wait "$watchdog" 2>/dev/null || true; watchdog=''; fi
}
stop_forward() {
  forward_stop_pid=$1
  kill -TERM "-$forward_stop_pid" 2>/dev/null || kill -TERM "$forward_stop_pid" 2>/dev/null || true
  forward_stop_tries=0
  while kill -0 "-$forward_stop_pid" 2>/dev/null; do
    forward_stop_tries=$((forward_stop_tries + 1))
    [ "$forward_stop_tries" -lt 5 ] || break
    sleep 1
  done
  if kill -0 "-$forward_stop_pid" 2>/dev/null; then
    kill -KILL "-$forward_stop_pid" 2>/dev/null || kill -KILL "$forward_stop_pid" 2>/dev/null || true
    forward_stop_tries=0
    while kill -0 "-$forward_stop_pid" 2>/dev/null; do
      forward_stop_tries=$((forward_stop_tries + 1))
      [ "$forward_stop_tries" -lt 5 ] || return 1
      sleep 1
    done
  fi
  wait "$forward_stop_pid" 2>/dev/null || true
}
pf_pids=''
active_fault=''
owned_created=false
voters_attempted=false
learner_attempted=false
inspector_attempted=false
shutdown_watch_pid=''
shutdown_log_pids=''
shutdown_finish_watch_pid=''
shutdown_finish_log_pids=''
shutdown_capture_state=idle
source_quiesced=false
# A watch started from the pre-stop list version replays termination events even
# if the request connects after scale0. No privileged node/container API is used.
shutdown_stream() {
  if [ "$mode" = run-local ]; then
    stream_limit=$(remaining) || return 124
    [ "$stream_limit" -le 240 ] || stream_limit=240
    exec timeout --kill-after=5s "${stream_limit}s" kubectl --kubeconfig="$RHIZA_LOCAL_RUNTIME_KUBECONFIG" \
      --context="$context" --namespace="$ns" --request-timeout=0 "$@"
  fi
  exec timeout --kill-after=5s 240s kubectl --context="$context" --namespace="$ns" --request-timeout=0 "$@"
}
shutdown_watch() {
  # get has no --resource-version flag. The raw namespace URI retains the
  # authentic list version and selector, using the same scoped transport.
  watch_version=$(jq -ner --arg version "$1" '$version|select(length>0)|@uri') || return 1
  watch_selector=$(jq -nr --arg run "$RHIZA_RUN_ID" '"chaos.rhiza.io/run="+$run|@uri') || return 1
  watch_seconds=$(( $2 - $(date +%s) ))
  [ "$watch_seconds" -gt 0 ] || return 124
  shutdown_stream get --raw="/api/v1/namespaces/$ns/pods?watch=true&resourceVersion=$watch_version&labelSelector=$watch_selector&timeoutSeconds=$watch_seconds"
}
capture_shutdown() {
  [ "$shutdown_capture_state" = idle ] || return 1
  # Original PodUIDs cover all created writers. Planned container-kill history
  # is not a clean Close: only the current incarnation is pinned below.
  jq -e '[.[]|select(.name|test("^rhiza-voter-[012]$"))] as $v |
    ($v|map(.name)|sort)==["rhiza-voter-0","rhiza-voter-1","rhiza-voter-2"] and
    all($v[]; (.uid|type)=="string" and (.uid|length)>0)
  ' "$out/voters-before.json" >/dev/null || return 1
  jq --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '[.[]|select(.name|test("^rhiza-voter-[012]$"))|{name,uid,namespace:$ns,run:$run}]' \
    "$out/voters-before.json" > "$out/shutdown-expected.next.json" || return $?
  if [ "$learner_attempted" = true ]; then
    jq -e --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '.metadata.name=="rhiza-learner" and
      .metadata.namespace==$ns and .metadata.labels["chaos.rhiza.io/run"]==$run and
      (.metadata.uid|type)=="string" and (.metadata.uid|length)>0' "$out/learner-created.json" >/dev/null || return 1
    jq --slurpfile learner "$out/learner-created.json" '.+[$learner[0].metadata|{name,uid,namespace,run:.labels["chaos.rhiza.io/run"]}]' \
      "$out/shutdown-expected.next.json" > "$out/shutdown-expected.with-learner.next.json" || return $?
    mv "$out/shutdown-expected.with-learner.next.json" "$out/shutdown-expected.next.json"
  fi
  jq -e 'type=="array" and length>0' "$out/shutdown-expected.next.json" >/dev/null || return 1
  mv "$out/shutdown-expected.next.json" "$out/shutdown-expected.json"
  snapshot_selector=$(jq -nr --arg run "$RHIZA_RUN_ID" '"chaos.rhiza.io/run="+$run|@uri') || return 1
  # Generic get output rebuilds a client-side List with an empty ListMeta.
  # Keep the server's opaque collection RV, using exactly the watch selector.
  k get --raw="/api/v1/namespaces/$ns/pods?labelSelector=$snapshot_selector" > "$out/shutdown-pods-before.json" 2> "$out/shutdown-pods-before.stderr" || return $?
  jq -e --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '.apiVersion=="v1" and .kind=="PodList" and
    (.metadata.resourceVersion|type)=="string" and (.metadata.resourceVersion|length)>0 and
    (.metadata.continue==null or .metadata.continue=="") and (.items|type)=="array" and
    all(.items[]; .metadata.namespace==$ns and .metadata.labels["chaos.rhiza.io/run"]==$run)
  ' "$out/shutdown-pods-before.json" >/dev/null || return 1
  # Never silently omit a writer whose status/log incarnation is unavailable.
  jq -e 'all(.items[]|select(.metadata.labels.app=="rhiza-voter" or .metadata.name=="rhiza-learner");
    [.status.containerStatuses[]?|select(.name=="rhiza" and (.containerID|type)=="string" and (.containerID|length)>0)]|length==1)
  ' "$out/shutdown-pods-before.json" >/dev/null || return 1
  jq '[.items[]|select(.metadata.labels.app=="rhiza-voter" or .metadata.name=="rhiza-learner")|
    . as $pod | .status.containerStatuses[]?|select(.name=="rhiza")|
    {name:$pod.metadata.name,namespace:$pod.metadata.namespace,run:$pod.metadata.labels["chaos.rhiza.io/run"],uid:$pod.metadata.uid,
      image:([ $pod.spec.containers[]|select(.name=="rhiza")|.image ][0]),image_id:.imageID,
      container_id:.containerID,restart_count:.restartCount}]' "$out/shutdown-pods-before.json" > "$out/shutdown-before.next.json" || return $?
  jq -e --slurpfile expected "$out/shutdown-expected.json" \
    'length>0 and (map({name,uid,namespace,run})|sort_by(.name))==($expected[0]|sort_by(.name))' "$out/shutdown-before.next.json" >/dev/null || return 1
  mv "$out/shutdown-before.next.json" "$out/shutdown-before.json"
  shutdown_watch_deadline=$(( $(date +%s) + 240 ))
  if [ "$mode" = run-local ] && [ "$shutdown_watch_deadline" -gt "$local_deadline" ]; then shutdown_watch_deadline=$local_deadline; fi
  resource_version=$(jq -er '.metadata.resourceVersion|select(type=="string" and length>0)' "$out/shutdown-pods-before.json") || return 1
  shutdown_watch_pid=''
  shutdown_log_pids=''
  shutdown_capture_state=active
  shutdown_watch "$resource_version" "$shutdown_watch_deadline" > "$out/shutdown-watch.json" 2> "$out/shutdown-watch.stderr" & shutdown_watch_pid=$!
  for shutdown_pod in $(jq -r '.[].name' "$out/shutdown-before.json"); do
    shutdown_stream logs "$shutdown_pod" --container=rhiza --follow > "$out/$shutdown_pod-shutdown.log" 2> "$out/$shutdown_pod-shutdown.stderr" &
    shutdown_log_pids="$shutdown_log_pids $shutdown_pod:$!"
  done
}
finish_shutdown_capture() {
  if [ "$shutdown_capture_state" = finished ]; then
    [ -f "$out/shutdown-finish.exit" ] || return 1
    return "$(cat "$out/shutdown-finish.exit")"
  fi
  [ "$shutdown_capture_state" = active ] || return 1
  shutdown_finish_watch_pid=$shutdown_watch_pid
  shutdown_finish_log_pids=$shutdown_log_pids
  shutdown_watch_pid=''
  shutdown_log_pids=''
  shutdown_capture_state=finishing
  collector_code=0
  for log_owner in $shutdown_finish_log_pids; do
    if wait "${log_owner#*:}"; then log_code=0; else log_code=$?; fi
    printf '%s\n' "$log_code" > "$out/${log_owner%:*}-shutdown-log.exit.next"
    mv "$out/${log_owner%:*}-shutdown-log.exit.next" "$out/${log_owner%:*}-shutdown-log.exit"
    [ "$log_code" = 0 ] || collector_code=1
    remaining_log_pids=''
    for remaining_log_owner in $shutdown_finish_log_pids; do
      [ "$remaining_log_owner" = "$log_owner" ] || remaining_log_pids="$remaining_log_pids $remaining_log_owner"
    done
    shutdown_finish_log_pids=$remaining_log_pids
  done
  if wait_shutdown_watch "$out" "$shutdown_finish_watch_pid" "$shutdown_watch_deadline"; then watch_proof_code=0; else watch_proof_code=$?; fi
  printf '%s\n' "$watch_proof_code" > "$out/shutdown-watch-proof.exit.next"
  mv "$out/shutdown-watch-proof.exit.next" "$out/shutdown-watch-proof.exit"
  watch_collector_code=0
  if [ -n "$shutdown_finish_watch_pid" ]; then
    kill "$shutdown_finish_watch_pid" 2>/dev/null || true
    if wait "$shutdown_finish_watch_pid"; then watch_collector_code=0; else watch_collector_code=$?; fi
    shutdown_finish_watch_pid=''
  fi
  printf '%s\n' "$watch_collector_code" > "$out/shutdown-watch.exit.next"
  mv "$out/shutdown-watch.exit.next" "$out/shutdown-watch.exit"
  printf '%s\n' "$collector_code" > "$out/shutdown-log-collectors.exit.next"
  mv "$out/shutdown-log-collectors.exit.next" "$out/shutdown-log-collectors.exit"
  finish_code=0
  [ "$collector_code" = 0 ] || finish_code=1
  [ ! -s "$out/shutdown-watch.stderr" ] || finish_code=1
  [ "$watch_proof_code" = 0 ] || finish_code=$watch_proof_code
  case "$watch_collector_code" in 0|143) ;; *) [ "$finish_code" != 0 ] || finish_code=1 ;; esac
  if [ "$finish_code" = 0 ]; then
    k get statefulset/rhiza-voter -o json > "$out/shutdown-controller.json" 2> "$out/shutdown-controller.stderr" || finish_code=$?
  fi
  if [ "$finish_code" = 0 ]; then
    k get pods -l "chaos.rhiza.io/run=$RHIZA_RUN_ID" -o json > "$out/shutdown-pods-after.json" 2> "$out/shutdown-pods-after.stderr" || finish_code=$?
  fi
  if [ "$finish_code" = 0 ]; then
    jq '{items:[.items[]|select(.metadata.labels.app=="rhiza-voter" or .metadata.name=="rhiza-learner")]}' \
      "$out/shutdown-pods-after.json" > "$out/shutdown-writers-after.json" || finish_code=$?
  fi
  if [ "$finish_code" = 0 ]; then verify_shutdown "$out" || finish_code=$?; fi
  printf '%s\n' "$finish_code" > "$out/shutdown-finish.exit.next"
  mv "$out/shutdown-finish.exit.next" "$out/shutdown-finish.exit"
  shutdown_capture_state=finished
  return "$finish_code"
}
cleanup_shutdown_capture_children() {
  if [ "$shutdown_capture_state" = active ]; then
    shutdown_finish_watch_pid=$shutdown_watch_pid
    shutdown_finish_log_pids=$shutdown_log_pids
    shutdown_watch_pid=''
    shutdown_log_pids=''
    shutdown_capture_state=finishing
  fi
  [ "$shutdown_capture_state" = finishing ] || return 0
  cleanup_child_code=0
  for cleanup_log_owner in $shutdown_finish_log_pids; do
    if kill -0 "${cleanup_log_owner#*:}" 2>/dev/null; then
      kill "${cleanup_log_owner#*:}" 2>/dev/null || true
      if wait "${cleanup_log_owner#*:}"; then cleanup_log_code=0; else cleanup_log_code=$?; fi
    else
      cleanup_log_code=0
    fi
    printf '%s\n' "$cleanup_log_code" > "$out/${cleanup_log_owner%:*}-shutdown-log-cleanup.exit"
    [ "$cleanup_log_code" = 0 ] || cleanup_child_code=1
  done
  shutdown_finish_log_pids=''
  if [ -n "$shutdown_finish_watch_pid" ]; then
    if kill -0 "$shutdown_finish_watch_pid" 2>/dev/null; then
      kill "$shutdown_finish_watch_pid" 2>/dev/null || true
      if wait "$shutdown_finish_watch_pid"; then cleanup_watch_code=0; else cleanup_watch_code=$?; fi
    else
      cleanup_watch_code=0
    fi
    printf '%s\n' "$cleanup_watch_code" > "$out/shutdown-watch-cleanup.exit"
    case "$cleanup_watch_code" in 0|143) ;; *) cleanup_child_code=1 ;; esac
  fi
  shutdown_finish_watch_pid=''
  if [ ! -e "$out/shutdown-finish.exit" ]; then
    printf '%s\n' 1 > "$out/shutdown-finish.exit.next"
    mv "$out/shutdown-finish.exit.next" "$out/shutdown-finish.exit"
  fi
  shutdown_capture_state=finished
  return "$cleanup_child_code"
}
verify_voter_absence() {
  k get statefulset/rhiza-voter -o json > "$out/cold-controller.json" || return $?
  jq -e --slurpfile before "$out/shutdown-controller-before.json" '
    .metadata.uid==$before[0].metadata.uid and .spec.replicas==0 and
    (.status.replicas // 0)==0 and (.status.readyReplicas // 0)==0 and (.status.currentReplicas // 0)==0
  ' "$out/cold-controller.json" >/dev/null || return 1
  k get pods -o json > "$out/cold-pods.json" || return $?
  k get endpointslices.discovery.k8s.io -l kubernetes.io/service-name=rhiza-peers -o json > "$out/cold-endpointslices.json" || return $?
  k get endpoints rhiza-peers -o json > "$out/cold-endpoints.json" || return $?
  jq -n -e --slurpfile expected "$out/shutdown-before.json" --slurpfile pods "$out/cold-pods.json" \
    --slurpfile slices "$out/cold-endpointslices.json" --slurpfile endpoints "$out/cold-endpoints.json" \
    --slurpfile before "$out/shutdown-pods-before.json" --arg sts_uid "$(jq -r '.metadata.uid' "$out/shutdown-controller-before.json")" '
    [$expected[0][]|select(.name|test("^rhiza-voter-[012]$"))] as $voters |
    [$voters[].uid] as $uids | [$voters[].name] as $names |
    [$before[0].items[]|select(.metadata.uid as $uid|$uids|index($uid))|.status.podIP] as $ips |
    all($pods[0].items[]; all(.metadata.ownerReferences[]?; .uid!=$sts_uid)) and
    all($slices[0].items[]|select(.metadata.labels["kubernetes.io/service-name"]=="rhiza-peers");
      all(.endpoints[]?; ((.targetRef.uid // "") as $uid|$uids|index($uid)|not) and
        ((.targetRef.name // "") as $name|$names|index($name)|not) and
        all(.addresses[]?; . as $ip|$ips|index($ip)|not))) and
    $endpoints[0].metadata.name=="rhiza-peers" and
    all($endpoints[0].subsets[]?;
      all(((.addresses // [])+(.notReadyAddresses // []))[]?;
        ((.targetRef.uid // "") as $uid|$uids|index($uid)|not) and
        ((.targetRef.name // "") as $name|$names|index($name)|not) and
        (.ip as $ip|$ips|index($ip)|not)))
  ' >/dev/null
}
delete_inspector_owned() {
  pod_cleanup_ok=true
  terminal_ok=true
  k get pods -o json > "$out/inspector-cleanup-pods-before.json" 2> "$out/inspector-cleanup-pods-before.stderr" || pod_cleanup_ok=false
  pod_inventory_ok=$pod_cleanup_ok
  for probe_spec in 'rhiza-inspector-control:inspector-control:rhiza-inspector-control' 'rhiza-inspector:inspector:rhiza-inspector'; do
    probe_name=${probe_spec%%:*}; probe_rest=${probe_spec#*:}; probe_label=${probe_rest%%:*}; probe_app=${probe_rest#*:}
    probe_cleanup_ok=$pod_inventory_ok
    if [ "$probe_cleanup_ok" = true ]; then
      jq -e --arg name "$probe_name" '[.items[]|select(.metadata.name==$name)]|length<=1' "$out/inspector-cleanup-pods-before.json" >/dev/null || probe_cleanup_ok=false
    fi
    if [ "$probe_cleanup_ok" = true ] && jq -e --arg name "$probe_name" 'any(.items[]; .metadata.name==$name)' "$out/inspector-cleanup-pods-before.json" >/dev/null; then
      [ -f "$out/$probe_label-created.json" ] || probe_cleanup_ok=false
      if [ "$probe_cleanup_ok" = true ]; then
        jq -e --arg name "$probe_name" '.items[]|select(.metadata.name==$name)' "$out/inspector-cleanup-pods-before.json" > "$out/$probe_label-delete-before.json" || probe_cleanup_ok=false
        jq -e --slurpfile created "$out/$probe_label-created.json" --arg ns "$ns" --arg run "$RHIZA_RUN_ID" --arg app "$probe_app" '
          .metadata.namespace==$ns and .metadata.uid==$created[0].metadata.uid and
          .metadata.labels=={"app":$app,"chaos.rhiza.io/run":$run} and
          (.metadata.resourceVersion|type)=="string" and (.metadata.resourceVersion|length)>0
        ' "$out/$probe_label-delete-before.json" >/dev/null || probe_cleanup_ok=false
      fi
      if [ "$probe_cleanup_ok" = true ]; then
        jq '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:.metadata.uid,resourceVersion:.metadata.resourceVersion}}' "$out/$probe_label-delete-before.json" > "$out/$probe_label-delete-options.json" || probe_cleanup_ok=false
        [ "$probe_cleanup_ok" = false ] || k delete --raw "/api/v1/namespaces/$ns/pods/$probe_name" -f "$out/$probe_label-delete-options.json" > "$out/$probe_label-delete.stdout" 2> "$out/$probe_label-delete.stderr" || probe_cleanup_ok=false
        [ "$probe_cleanup_ok" = false ] || k wait "pod/$probe_name" --for=delete --timeout=90s > "$out/$probe_label-gone.stdout" 2> "$out/$probe_label-gone.stderr" || probe_cleanup_ok=false
      fi
    fi
    [ "$probe_cleanup_ok" = true ] || pod_cleanup_ok=false
  done
  k get pods -o json > "$out/inspector-terminal-pods.json" 2> "$out/inspector-terminal-pods.stderr" || terminal_ok=false
  k get networkpolicy.networking.k8s.io rhiza-inspector-deny-all -o json > "$out/inspector-terminal-networkpolicy.json" 2> "$out/inspector-terminal-networkpolicy.stderr" || terminal_ok=false
  k get networkpolicies.networking.k8s.io -o json > "$out/inspector-terminal-networkpolicies.json" 2> "$out/inspector-terminal-networkpolicies.stderr" || terminal_ok=false
  if [ "$terminal_ok" = true ]; then
    validate_inspector_policy "$out/inspector-terminal-networkpolicy.json" || terminal_ok=false
    jq -e --slurpfile canonical "$out/inspector-networkpolicy-bootstrap.json" '
      (.items|length)==1 and .items[0].metadata.uid==$canonical[0].metadata.uid
    ' "$out/inspector-terminal-networkpolicies.json" >/dev/null || terminal_ok=false
  fi
  if [ "$pod_cleanup_ok" = true ] && [ "$terminal_ok" = true ] &&
    jq -e 'all(.items[]; .metadata.name!="rhiza-inspector" and .metadata.name!="rhiza-inspector-control")' "$out/inspector-terminal-pods.json" >/dev/null; then
    rm -f "$out/inspector-manual-cleanup-required"
    return 0
  fi
  return 1
}
cleanup_metadata_only() (
  # Reuse the scoped transport deadline for all reads, delete and wait together.
  voter_wait_deadline=$(( $(date +%s) + 180 ))
  [ "$learner_attempted" = false ] && [ -z "$active_fault$pf_pids" ] && [ "$shutdown_capture_state" = idle ] || return 1
  # An absent or ambiguous create ACK cannot authorize a name-only delete.
  jq -e --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '.kind=="Pod" and .metadata.name=="rhiza-metadata" and
    .metadata.namespace==$ns and .metadata.labels["chaos.rhiza.io/run"]==$run and
    (.metadata.uid|type)=="string" and (.metadata.uid|length)>0' "$out/metadata-created.json" >/dev/null || return 1
  k get namespace "$ns" -o json > "$out/metadata-cleanup-namespace.json" 2> "$out/metadata-cleanup-namespace.stderr" || return $?
  jq -e --arg uid "$RHIZA_BOOTSTRAP_UID" --arg run "$RHIZA_RUN_ID" \
    --arg owner "rhiza-postrelease-$RHIZA_RUN_ID-$RHIZA_AUTH_CREATION_SHA" '
    .metadata.uid==$uid and .metadata.labels["chaos.rhiza.io/run"]==$run and
    .metadata.annotations["rhiza.dev/auth-owner"]==$owner
  ' "$out/metadata-cleanup-namespace.json" >/dev/null || return 1
  # Do not filter by a label: an unexpected namespace actor must fail closed.
  k get pods,statefulsets,deployments,replicasets,daemonsets,jobs,cronjobs,podchaos,networkchaos -o json > "$out/metadata-cleanup-before.json" 2> "$out/metadata-cleanup-before.stderr" || return $?
  jq -e --slurpfile ack "$out/metadata-created.json" --arg ns "$ns" --arg run "$RHIZA_RUN_ID" \
    --arg image "$RHIZA_METADATA_IMAGE" --arg node "$RHIZA_NODE_A" '
    (.items|type)=="array" and (.items|length)==1 and (.metadata.continue==null or .metadata.continue=="") and
    (.items[0] | .kind=="Pod" and .metadata.name=="rhiza-metadata" and .metadata.namespace==$ns and
      .metadata.uid==$ack[0].metadata.uid and .metadata.labels["chaos.rhiza.io/run"]==$run and
      (.metadata.resourceVersion|type)=="string" and (.metadata.resourceVersion|length)>0 and
      (.metadata.ownerReferences // []|length)==0 and .spec.serviceAccountName=="rhiza-gcs" and
      .spec.nodeName==$node and (.spec.containers|length)==1 and
      .spec.containers[0].name=="metadata" and .spec.containers[0].image==$image)
  ' "$out/metadata-cleanup-before.json" >/dev/null || return 1
  jq '.items[0] | {apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:.metadata.uid,resourceVersion:.metadata.resourceVersion}}' \
    "$out/metadata-cleanup-before.json" > "$out/metadata-delete-options.json" || return $?
  k delete --raw "/api/v1/namespaces/$ns/pods/rhiza-metadata" -f "$out/metadata-delete-options.json" \
    > "$out/metadata-delete.stdout" 2> "$out/metadata-delete.stderr" || return $?
  k wait pod/rhiza-metadata --for=delete --timeout=90s > "$out/metadata-gone.stdout" 2> "$out/metadata-gone.stderr" || return $?
  k get pods,statefulsets,deployments,replicasets,daemonsets,jobs,cronjobs,podchaos,networkchaos -o json > "$out/metadata-cleanup-after.json" 2> "$out/metadata-cleanup-after.stderr" || return $?
  jq -e '(.items|type)=="array" and (.items|length)==0 and (.metadata.continue==null or .metadata.continue=="")' \
    "$out/metadata-cleanup-after.json" >/dev/null
)
cleanup() {
  code=$?
  trap '' USR1
  trap - EXIT HUP INT TERM
  set +e
  stop_watchdog
  if [ "$mode" = run-local ]; then
    local_deadline=$(( $(date +%s) + 1200 ))
    [ "$local_deadline" -le "$token_expiry" ] || local_deadline=$token_expiry
    [ "$local_deadline" -le "$auth_expiry" ] || local_deadline=$auth_expiry
  fi
  clean=true
  # Never replace the first failure with a diagnostics/cleanup result.
  printf '%s\n' "$code" > "$out/workload.exit"
  if [ "$owned_created" = true ] && [ "$voters_attempted" = false ]; then
    cleanup_metadata_only
    cleanup_code=$?
    printf '%s\n' "$cleanup_code" > "$out/metadata-cleanup.exit"
    [ "$cleanup_code" = 0 ] || clean=false
    if [ "$code" = 0 ] && [ "$cleanup_code" != 0 ]; then code=$cleanup_code; fi
  elif [ "$owned_created" = true ]; then
  for pod in rhiza-voter-0 rhiza-voter-1 rhiza-voter-2 rhiza-learner rhiza-inspector rhiza-inspector-control; do
    k logs "$pod" --tail=2000 > "$out/$pod.log" 2> "$out/$pod-log.stderr" || true
  done
  if [ "$inspector_attempted" = true ] && [ -f "$out/inspector-manual-cleanup-required" ]; then
    delete_inspector_owned
    cleanup_code=$?
    printf '%s\n' "$cleanup_code" > "$out/inspector-cleanup.exit"
    [ "$cleanup_code" = 0 ] || clean=false
    if [ "$code" = 0 ] && [ "$cleanup_code" != 0 ]; then code=$cleanup_code; fi
  fi
  if [ -n "$active_fault" ]; then
    k delete "$active_fault" --ignore-not-found --wait=true --timeout=60s > "$out/fault-cleanup.stdout" 2> "$out/fault-cleanup.stderr"
    cleanup_code=$?
    printf '%s\n' "$cleanup_code" > "$out/fault-cleanup.exit"
    [ "$cleanup_code" = 0 ] || clean=false
    if [ "$code" = 0 ] && [ "$cleanup_code" != 0 ]; then code=$cleanup_code; fi
  fi
  # On failure, remove the recovery process before touching the sealed source.
  k delete pod/rhiza-learner --ignore-not-found --wait=true --timeout=60s > "$out/stop-learner.stdout" 2> "$out/stop-learner.stderr"
  cleanup_code=$?
  printf '%s\n' "$cleanup_code" > "$out/stop-learner.exit"
  [ "$cleanup_code" = 0 ] || clean=false
  if [ "$code" = 0 ] && [ "$cleanup_code" != 0 ]; then code=$cleanup_code; fi
  if [ "${source_quiesced:-false}" = false ]; then
    if [ "$shutdown_capture_state" = idle ]; then
      capture_shutdown
      capture_code=$?
      printf '%s\n' "$capture_code" > "$out/shutdown-cleanup-capture.exit"
      [ "$capture_code" = 0 ] || clean=false
      if [ "$code" = 0 ] && [ "$capture_code" != 0 ]; then code=$capture_code; fi
    fi
    # Run-owned writers stop normally; this is NOT fencing evidence.
    k scale statefulset/rhiza-voter --replicas=0 > "$out/stop-voters.stdout" 2> "$out/stop-voters.stderr"
    cleanup_code=$?
    printf '%s\n' "$cleanup_code" > "$out/stop-voters.exit"
    [ "$cleanup_code" = 0 ] || clean=false
    if [ "$code" = 0 ] && [ "$cleanup_code" != 0 ]; then code=$cleanup_code; fi
    k wait pod --selector="app=rhiza-voter,chaos.rhiza.io/run=$RHIZA_RUN_ID" --for=delete --timeout=90s > "$out/voters-gone.stdout" 2> "$out/voters-gone.stderr"
    cleanup_code=$?
    printf '%s\n' "$cleanup_code" > "$out/voters-gone.exit"
    [ "$cleanup_code" = 0 ] || clean=false
    if [ "$code" = 0 ] && [ "$cleanup_code" != 0 ]; then code=$cleanup_code; fi
    k get pods --selector="app=rhiza-voter,chaos.rhiza.io/run=$RHIZA_RUN_ID" -o json > "$out/voters-absent.json" 2> "$out/voters-absent.stderr"
    cleanup_code=$?
    if [ "$cleanup_code" = 0 ]; then jq -e '.items|length==0' "$out/voters-absent.json" >/dev/null; cleanup_code=$?; fi
    printf '%s\n' "$cleanup_code" > "$out/voters-absent.exit"
    [ "$cleanup_code" = 0 ] || clean=false
    if [ "$code" = 0 ] && [ "$cleanup_code" != 0 ]; then code=$cleanup_code; fi
    if [ "$shutdown_capture_state" = active ]; then
      finish_shutdown_capture
      proof_code=$?
      printf '%s\n' "$proof_code" > "$out/shutdown-cleanup-proof.exit"
      [ "$proof_code" = 0 ] || clean=false
      if [ "$code" = 0 ] && [ "$proof_code" != 0 ]; then code=$proof_code; fi
    elif [ "$shutdown_capture_state" = finishing ]; then
      cleanup_shutdown_capture_children
      proof_code=$?
      printf '%s\n' "$proof_code" > "$out/shutdown-cleanup-reap.exit"
      clean=false
      if [ "$code" = 0 ]; then code=1; fi
    elif [ "$shutdown_capture_state" = finished ]; then
      if [ -f "$out/shutdown-finish.exit" ]; then proof_code=$(cat "$out/shutdown-finish.exit"); else proof_code=1; fi
      printf '%s\n' "$proof_code" > "$out/shutdown-cleanup-existing-proof.exit"
      [ "$proof_code" = 0 ] || clean=false
      if [ "$code" = 0 ] && [ "$proof_code" != 0 ]; then code=$proof_code; fi
    fi
  fi
  fi
  forward_cleanup_code=0
  for pid in $pf_pids; do stop_forward "$pid" || forward_cleanup_code=1; done
  printf '%s\n' "$forward_cleanup_code" > "$out/port-forward-cleanup.exit"
  [ "$forward_cleanup_code" = 0 ] || clean=false
  if [ "$code" = 0 ] && [ "$forward_cleanup_code" != 0 ]; then code=$forward_cleanup_code; fi
  if [ "$owned_created" = false ]; then
    printf '%s\n' 'No owned runtime resources created; Root auth cleanup still pending.' > "$out/cleanup-status.txt"
  elif [ "$clean" = true ] && [ "$voters_attempted" = false ]; then
    printf '%s\n' 'Metadata-only Pod cleanup verified; database writer creation was not attempted. No HTTP/DB Close proof. Root storage/IAM reconciliation still pending.' > "$out/cleanup-status.txt"
  elif [ "$clean" = true ]; then
    printf '%s\n' 'Owned HTTP/DB close and pinned container termination verified; NOT universal remote-request quiescence. Root storage/IAM reconciliation still pending.' > "$out/cleanup-status.txt"
  else
    printf '%s\n' 'NOT CLEAN: bounded run-owned shutdown failed; Root must reconcile before storage/IAM cleanup.' > "$out/cleanup-status.txt"
    printf '%s\n' 'NOT CLEAN (see workload.exit for the preserved original result).' >> "$out/result.txt"
  fi
  printf '%s\n' 'Managed-folder objects, soft-delete retention and namespace/IAM teardown remain Root-owned; no recursive cleanup here.' > "$out/cleanup-owner.txt"
  printf '%s\n' "$code" > "$out/root.exit"
  seal_code=0
  manifest_files=$(mktemp "$out/.SHA256SUMS.files.XXXXXXXX") || seal_code=$?
  if [ "$seal_code" = 0 ]; then
    (cd "$out" && find . -type f ! -name SHA256SUMS ! -name '.SHA256SUMS.files.*' -print) > "$manifest_files" || seal_code=$?
  fi
  if [ "$seal_code" = 0 ]; then
    (cd "$out" && while IFS= read -r file; do
      digest=$(sha256_file "$file") || exit $?
      printf '%s  %s\n' "$digest" "$file" || exit $?
    done < "$manifest_files") > "$out/SHA256SUMS" || seal_code=$?
  fi
  [ -z "${manifest_files:-}" ] || rm -f "$manifest_files"
  if [ "$seal_code" != 0 ]; then
    rm -f "$out/SHA256SUMS"
    if [ "$code" = 0 ]; then code=$seal_code; fi
    printf '%s\n' "$code" > "$out/root.exit"
  fi
  exit "$code"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
if [ "$mode" = run-local ]; then
  trap 'exit 124' USR1
  runtime_pid=$$
  watchdog_seconds=$(remaining) || exit 124
  (
    sleep_pid=''
    trap 'if [ -n "$sleep_pid" ]; then kill "$sleep_pid" 2>/dev/null || true; wait "$sleep_pid" 2>/dev/null || true; fi; exit 0' HUP INT TERM
    sleep "$watchdog_seconds" & sleep_pid=$!
    wait "$sleep_pid"
    kill -USR1 "$runtime_pid"
  ) & watchdog=$!
  printf '%s\n' "$watchdog" > "$out/watchdog.pid"
  run local-identity k auth whoami -o json
  jq -e --arg user "system:serviceaccount:$ns:rhiza-runtime" --arg group "system:serviceaccounts:$ns" \
    '.status.userInfo.username==$user and (.status.userInfo.groups|sort)==(["system:authenticated","system:serviceaccounts",$group]|sort)' \
    "$out/$seq-local-identity.stdout" >/dev/null || die 'local runtime server identity mismatch'
  for resource in secrets serviceaccounts/token roles validatingadmissionpolicies; do
    verb='create'
    [ "$resource" != secrets ] || verb='get'
    case "$resource" in
      serviceaccounts/token) set -- create serviceaccounts --subresource=token ;;
      validatingadmissionpolicies) set -- create validatingadmissionpolicies.admissionregistration.k8s.io --all-namespaces ;;
      *) set -- "$verb" "$resource" ;;
    esac
    seq=$((seq + 1))
    set +e
    k auth can-i "$@" > "$out/$seq-permission-denied.stdout" 2> "$out/$seq-permission-denied.stderr"
    denied_code=$?
    set -e
    printf '%s\n' "$denied_code" > "$out/$seq-permission-denied.exit"
    if [ "$denied_code" = 124 ] || [ "$denied_code" = 137 ]; then exit "$denied_code"; fi
    [ "$denied_code" = 1 ] && grep -qx no "$out/$seq-permission-denied.stdout" && [ ! -s "$out/$seq-permission-denied.stderr" ] || die 'local runtime forbidden permission/authorization check failed'
  done
  for verb in create update patch delete; do
    seq=$((seq + 1))
    set +e
    k auth can-i "$verb" networkpolicies.networking.k8s.io > "$out/$seq-networkpolicy-$verb-denied.stdout" 2> "$out/$seq-networkpolicy-$verb-denied.stderr"
    denied_code=$?
    set -e
    printf '%s\n' "$denied_code" > "$out/$seq-networkpolicy-$verb-denied.exit"
    [ "$denied_code" = 1 ] && grep -qx no "$out/$seq-networkpolicy-$verb-denied.stdout" && [ ! -s "$out/$seq-networkpolicy-$verb-denied.stderr" ] || die 'runtime NetworkPolicy mutation permission present'
  done
fi
run namespace k get namespace "$ns" -o json
jq -e --arg uid "$RHIZA_BOOTSTRAP_UID" --arg run "$RHIZA_RUN_ID" --arg owner "rhiza-postrelease-$RHIZA_RUN_ID-$RHIZA_AUTH_CREATION_SHA" '.metadata.uid==$uid and .metadata.labels["chaos.rhiza.io/run"]==$run and .metadata.annotations["rhiza.dev/auth-owner"]==$owner and .metadata.annotations["chaos-mesh.org/inject"]=="enabled"' "$out/$seq-namespace.stdout" >/dev/null || die 'bootstrap namespace/workflow identity mismatch'
run collision k get pods,statefulsets,services,configmaps,podchaos,networkchaos -l "chaos.rhiza.io/run=$RHIZA_RUN_ID" -o json
jq -e '.items|length==0' "$out/$seq-collision.stdout" >/dev/null || die 'run resources already exist'
run inspector-networkpolicy-bootstrap k get networkpolicy.networking.k8s.io rhiza-inspector-deny-all -o json
cp "$out/$seq-inspector-networkpolicy-bootstrap.stdout" "$out/inspector-networkpolicy-bootstrap.json"
validate_inspector_policy "$out/inspector-networkpolicy-bootstrap.json" || die 'bootstrap-owned inspector NetworkPolicy identity or shape mismatch'
run inspector-networkpolicies-bootstrap k get networkpolicies.networking.k8s.io -o json
jq -e --slurpfile canonical "$out/inspector-networkpolicy-bootstrap.json" '
  (.items|length)==1 and .items[0].metadata.uid==$canonical[0].metadata.uid
' "$out/$seq-inspector-networkpolicies-bootstrap.stdout" >/dev/null || die 'unexpected additive NetworkPolicy present before runtime'

# Authorization, not an admission-policy convention, must make the runtime
# credential incapable of replacing, deleting, or overlaying the isolation.
jq '.spec.egress=[{}]' "$out/inspector-networkpolicy-bootstrap.json" > "$out/inspector-networkpolicy-replacement.json"
jq -nc --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '{apiVersion:"networking.k8s.io/v1",kind:"NetworkPolicy",metadata:{name:"rhiza-inspector-additive-allow",namespace:$ns,labels:{"chaos.rhiza.io/run":$run}},spec:{podSelector:{matchLabels:{app:"rhiza-inspector","chaos.rhiza.io/run":$run}},policyTypes:["Egress"],egress:[{}]}}' > "$out/inspector-networkpolicy-additive.json"
reject_runtime_networkpolicy_mutation runtime-networkpolicy-delete-denied delete --dry-run=server -f "$out/inspector-networkpolicy-bootstrap.json" -o json
reject_runtime_networkpolicy_mutation runtime-networkpolicy-replace-denied replace --dry-run=server -f "$out/inspector-networkpolicy-replacement.json" -o json
reject_runtime_networkpolicy_mutation runtime-networkpolicy-additive-denied create --dry-run=server -f "$out/inspector-networkpolicy-additive.json" -o json
# Compile/admit the exact rendered runtime manifests without persistence.
# These receipts are deliberately recorded before owned_created: a dry-run is
# neither an apply nor evidence that cleanup owns any resource.
run runtime-initial-preflight k create --dry-run=server -f "$out/initial.yaml" -o json
run runtime-learner-preflight k create --dry-run=server -f "$out/learner.yaml" -o json
run runtime-inspector-preflight k create --dry-run=server -f "$out/inspector-pod.yaml" -o json
run runtime-inspector-control-preflight k create --dry-run=server -f "$out/inspector-control-pod.yaml" -o json
owned_created=true
run create-metadata k create -f "$out/metadata.yaml" -o json
cp "$out/$seq-create-metadata.stdout" "$out/metadata-created.json"
run metadata-ready k wait pod/rhiza-metadata --for=condition=Ready --timeout=180s
run metadata-credentials metadata_credentials_ready
storage="gs://$RHIZA_GCS_BUCKET/$prefix"
seq=$((seq + 1))
set +e
k exec rhiza-metadata -- gcloud storage cat "gs://$RHIZA_GCS_BUCKET/${prefix%$RHIZA_RUN_ID/}denied-$RHIZA_RUN_ID/no-object" > "$out/$seq-outside-scope.stdout" 2> "$out/$seq-outside-scope.stderr"
denied_code=$?
set -e
printf '%s\n' "$denied_code" > "$out/$seq-outside-scope.exit"
[ "$denied_code" != 0 ] && grep -Eq '403|PERMISSION_DENIED|AccessDenied' "$out/$seq-outside-scope.stderr" && [ ! -s "$out/$seq-outside-scope.stdout" ] || die 'outside-folder access did not fail closed'
# A single exact folder list, before any database writer exists. gcloud's
# documented empty-match error is an expected probe, never an auth fallback.
seq=$((seq + 1))
set +e
k exec rhiza-metadata -- gcloud storage ls --json "${storage}**" > "$out/$seq-prefix-empty.stdout" 2> "$out/$seq-prefix-empty.stderr"
list_code=$?
set -e
printf '%s\n' "$list_code" > "$out/$seq-prefix-empty.exit"
if [ "$list_code" = 0 ]; then
  jq -e 'length==0' "$out/$seq-prefix-empty.stdout" >/dev/null || die 'prefix not empty'
else
  [ "$list_code" = 1 ] && grep -Fq 'matched no objects' "$out/$seq-prefix-empty.stderr" || die 'prefix/list permission gate failed'
fi
voters_attempted=true
run create-voters k create -f "$out/voters.yaml"
run voters-ready wait_voters 180
forward() {
  pod=$1; port=$2; expected_uid=${3:-}
  case "$port" in
    18080) old_pid=${pf_18080:-} ;;
    18081) old_pid=${pf_18081:-} ;;
    18082) old_pid=${pf_18082:-} ;;
    18083) old_pid=${pf_18083:-} ;;
    *) die 'unsupported port-forward port' ;;
  esac
  if [ -n "$old_pid" ]; then
    stop_forward "$old_pid" || die 'old port-forward process group did not stop'
    current_pids=''
    for pid in $pf_pids; do [ "$pid" = "$old_pid" ] || current_pids="$current_pids $pid"; done
    pf_pids=$current_pids
  fi
  pf_generation=$(( ${pf_generation:-0} + 1 ))
  pf_before="$out/port-forward-$port-generation-$pf_generation-before.json"
  host_digest=${RHIZA_HOST_IMAGE#*@}
  k get pod "$pod" -o json | jq --argjson pid 0 --arg image "$RHIZA_HOST_IMAGE" --arg digest "$host_digest" --arg expected_uid "$expected_uid" '
    (.status.containerStatuses|length) as $status_count | (.spec.containers|length) as $spec_count |
    [.status.containerStatuses[]?|select(.name=="rhiza")] as $status |
    [.spec.containers[]?|select(.name=="rhiza")] as $spec |
    {pid:$pid,pod:.metadata.name,uid:.metadata.uid,deleting:.metadata.deletionTimestamp,
      ready:any(.status.conditions[]?;.type=="Ready" and .status=="True"),
      container_id:$status[0].containerID,image:$spec[0].image,image_id:$status[0].imageID} |
    select($status_count==1 and $spec_count==1 and
      ($status|length)==1 and ($spec|length)==1 and .image==$image and (.image_id|endswith($digest)) and
      (.uid|type)=="string" and (.uid|length)>0 and ($expected_uid=="" or .uid==$expected_uid) and
      .deleting==null and .ready==true and (.container_id|type)=="string" and (.container_id|length)>0 and
      (.image|type)=="string" and (.image|length)>0 and (.image_id|type)=="string" and (.image_id|length)>0)
  ' > "$pf_before" || die 'port-forward target identity invalid'
  forward_deadline=$((started + 1200))
  if [ "$mode" = run-local ] && [ "$local_deadline" -lt "$forward_deadline" ]; then forward_deadline=$local_deadline; fi
  forward_seconds=$((forward_deadline - $(date +%s)))
  [ "$forward_seconds" -gt 0 ] || exit 124
  if [ "$mode" = run-local ]; then
    timeout --kill-after=5s "${forward_seconds}s" kubectl --kubeconfig="$RHIZA_LOCAL_RUNTIME_KUBECONFIG" \
      --context="$context" --namespace="$ns" --request-timeout=30s port-forward "pod/$pod" "$port:8080" "$((port + 100)):8081" > "$out/$pod-portforward.log" 2>&1 &
  else
    timeout --kill-after=5s "${forward_seconds}s" kubectl --context="$context" --namespace="$ns" \
      port-forward "pod/$pod" "$port:8080" "$((port + 100)):8081" > "$out/$pod-portforward.log" 2>&1 &
  fi
  forward_pid=$!
  case "$port" in
    18080) pf_18080=$forward_pid ;;
    18081) pf_18081=$forward_pid ;;
    18082) pf_18082=$forward_pid ;;
    18083) pf_18083=$forward_pid ;;
  esac
  pf_pids="$pf_pids $forward_pid"
  jq --argjson pid "$forward_pid" '.pid=$pid' "$pf_before" > "$pf_before.next"
  mv "$pf_before.next" "$pf_before"
  tries=0
  until curl -fsS --max-time 2 "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; do
    kill -0 "$forward_pid" 2>/dev/null || die 'port-forward exited before readiness'
    [ "$(date +%s)" -lt "$forward_deadline" ] || exit 124
    tries=$((tries + 1)); [ "$tries" -lt 30 ] || die 'port-forward timeout'; sleep 1
  done
  kill -0 "$forward_pid" 2>/dev/null || die 'port-forward exited after readiness'
  pf_after="$out/port-forward-$port-generation-$pf_generation-after.json"
  k get pod "$pod" -o json | jq --argjson pid "$forward_pid" --arg image "$RHIZA_HOST_IMAGE" --arg digest "$host_digest" --arg expected_uid "$expected_uid" '
    (.status.containerStatuses|length) as $status_count | (.spec.containers|length) as $spec_count |
    [.status.containerStatuses[]?|select(.name=="rhiza")] as $status |
    [.spec.containers[]?|select(.name=="rhiza")] as $spec |
    {pid:$pid,pod:.metadata.name,uid:.metadata.uid,deleting:.metadata.deletionTimestamp,
      ready:any(.status.conditions[]?;.type=="Ready" and .status=="True"),
      container_id:$status[0].containerID,image:$spec[0].image,image_id:$status[0].imageID} |
    select($status_count==1 and $spec_count==1 and
      ($status|length)==1 and ($spec|length)==1 and .image==$image and (.image_id|endswith($digest)) and
      (.uid|type)=="string" and (.uid|length)>0 and ($expected_uid=="" or .uid==$expected_uid) and
      .deleting==null and .ready==true and (.container_id|type)=="string" and (.container_id|length)>0 and
      (.image|type)=="string" and (.image|length)>0 and (.image_id|type)=="string" and (.image_id|length)>0)
  ' > "$pf_after" || die 'port-forward target identity invalid after readiness'
  jq -e --slurpfile before "$pf_before" '.==$before[0]' "$pf_after" >/dev/null || die 'port-forward target changed during startup'
}
forward rhiza-voter-0 18080
forward rhiza-voter-1 18081
forward rhiza-voter-2 18082
pod_snapshot() { k get pods -l "chaos.rhiza.io/run=$RHIZA_RUN_ID" -o json | jq '[.items[]|{name:.metadata.name,uid:.metadata.uid,node:.spec.nodeName,volumes:.spec.volumes,containers:.status.containerStatuses,deleting:.metadata.deletionTimestamp,ready:any(.status.conditions[]?;.type=="Ready" and .status=="True")}]'; }
run before pod_snapshot
cp "$out/$seq-before.stdout" "$out/voters-before.json"
jq -e 'all(.[]|select(.name|startswith("rhiza-voter-"));all(.containers[]; .restartCount==0))' "$out/voters-before.json" >/dev/null || die 'unexpected startup restart; unseen incarnation not budgeted'
printf '%s\n' '{}' > "$out/counter-segments.json"
voter0_restarts=0
learner_started=false
# Sampled high-water sums, NOT a strict billing cap. Five expected process
# incarnations: three voters, one voter-0 restart, one fresh learner. Stop at
# 40k observed attempts/640MiB upload/body/download, leaving 10k/384MiB planned
# reserve for unseen startup/SIGKILL tail, metadata probes and shutdown. Those
# reserves are conservative planning allowances, not measured upper bounds.
metrics() {
  [ "$(( $(date +%s) - started ))" -lt 1200 ] || die '20-minute workload ceiling; cleanup reserved'
  ports='18080 18081 18082'
  if [ "$source_quiesced" = true ]; then ports=18083
  elif [ "$learner_started" = true ]; then ports="$ports 18083"
  fi
  for port in $ports; do
    # Preserve the pre-kill sample; SIGKILL tail is a disclosed reserve, not
    # falsely reconstructed from the restarted process's zeroed counters.
    if [ "$active_fault" = PodChaos/process-one ] && [ "$port" = 18080 ]; then continue; fi
    case "$port" in
      18080) pod=rhiza-voter-0; expected=$voter0_restarts;;
      18081) pod=rhiza-voter-1; expected=0;;
      18082) pod=rhiza-voter-2; expected=0;;
      18083) pod=rhiza-learner; expected=0;;
    esac
    run counter-identity k get pod "$pod" -o json
    identity="$out/$seq-counter-identity.stdout"
    jq -e --argjson expected "$expected" '.status.containerStatuses|length==1 and .[0].restartCount==$expected and (.[0].containerID|length)>0' "$identity" >/dev/null || die 'unexpected incarnation/restart; budget cannot be reconstructed'
    if [ "$pod" != rhiza-learner ]; then
      jq -e --slurpfile before "$out/voters-before.json" '. as $p|any($before[0][];.name==$p.metadata.name and .uid==$p.metadata.uid)' "$identity" >/dev/null || die 'unplanned voter Pod replacement'
    else
      jq -e --arg uid "$learner_uid" '.metadata.uid==$uid' "$identity" >/dev/null || die 'learner UID changed before metrics/readback'
    fi
    run counters curl -fsS --max-time 5 "http://127.0.0.1:$port/qualification/object-store"
    counters="$out/$seq-counters.stdout"
    run counter-identity-after k get pod "$pod" -o json
    jq -e --slurpfile before "$identity" '.metadata.uid==$before[0].metadata.uid and .status.containerStatuses[0].containerID==$before[0].status.containerStatuses[0].containerID and .status.containerStatuses[0].restartCount==$before[0].status.containerStatuses[0].restartCount' "$out/$seq-counter-identity-after.stdout" >/dev/null || die 'incarnation changed during counter sample'
    key=$(jq -r '.metadata.uid+"/"+.status.containerStatuses[0].containerID' "$identity")
    jq -e --arg key "$key" --slurpfile sample "$counters" '
      .[$key] as $old|$sample[0] as $new|
      if $old!=null and any($old|keys[]; . as $field|$new[$field]<$old[$field]) then error("counter reset within incarnation")
      else .[$key]=$new end' "$out/counter-segments.json" > "$out/counter-segments.next.json" || die 'counter reset within UID/container segment'
    mv "$out/counter-segments.next.json" "$out/counter-segments.json"
    jq '[.[]]|reduce .[] as $s ({};reduce ($s|keys[]) as $k (. ;.[$k]=((.[$k]//0)+$s[$k])))' "$out/counter-segments.json" > "$out/counter-totals.json"
    jq -e 'length<=5 and all(.[];.http_requests<8000 and .bytes_uploaded<134217728 and .http_request_body_bytes<134217728 and .bytes_downloaded<134217728)' "$out/counter-segments.json" >/dev/null || die 'incarnation bound reached'
    jq -e '.http_requests<40000 and .bytes_uploaded<671088640 and .http_request_body_bytes<671088640 and .bytes_downloaded<671088640' "$out/counter-totals.json" >/dev/null || die 'sampled aggregate reserve threshold reached'
  done
}
network_fault_current() (
  voter_wait_deadline=$(( $(date +%s) + 10 ))
  k get "$active_fault" -o json
)
execute() {
  id=$1; sql=$2
  payload=$(jq -nc --arg id "$id" --arg sql "$sql" '{request_id:$id,sql:$sql}')
  network_ack=false
  if [ "${active_fault:-}" = NetworkChaos/network-one ]; then
    seq=$((seq + 1)); label=ack
    set +e
    curl --silent --show-error --max-time 5 --output "$out/$seq-$label.stdout" \
      --write-out '%{http_code}\n%{time_total}\n' -H 'Content-Type: application/json' -d "$payload" \
      http://127.0.0.1:18081/sql/execute > "$out/$seq-$label.http-timing" 2> "$out/$seq-$label.stderr"
    code=$?
    set -e
    printf '%s\n' "$code" > "$out/$seq-$label.exit"
    sed -n '1p' "$out/$seq-$label.http-timing" > "$out/$seq-$label.http-status"
    sed -n '2p' "$out/$seq-$label.http-timing" > "$out/$seq-$label.time-total"
    status=$(cat "$out/$seq-$label.http-status")
    if [ "$code" != 0 ] || [ "$status" != 200 ]; then
      capture_network_failure "$id"
      [ "$code" != 0 ] || code=1
      printf 'phase=network-mutation category=unconfirmed exit=%s status=%s\n' "$code" "${status:-missing}" >&2
      return "$code"
    fi
    network_ack=true
  else
  run ack curl -fsS --max-time 30 -H 'Content-Type: application/json' -d "$payload" http://127.0.0.1:18081/sql/execute
  fi
  # SQL receipts omit Applied; committed plus no execution error is their
  # success contract. The configured before-ack barrier remains engine-owned.
  jq -e '(.slot|type)=="number" and .slot>0 and (.slot|floor)==.slot and .status=="committed" and (.error_code==null or .error_code=="")' "$out/$seq-ack.stdout" >/dev/null || die 'not a durable successful ACK'
  if [ "$network_ack" = true ]; then
    run ack-fault-current network_fault_current
    validate_network_injection "$out/$seq-ack-fault-current.stdout" || die 'network fault recovered or changed before durable ACK validation'
  fi
  printf '%s\n' "$id" >> "$out/ack-ids.txt"
  metrics
}
diagnostic_k() {
  if [ "$mode" = run-local ]; then
    timeout --kill-after=1s 3s kubectl --kubeconfig="$RHIZA_LOCAL_RUNTIME_KUBECONFIG" \
      --context="$context" --namespace="$ns" --request-timeout=2s "$@"
  else
    timeout --kill-after=1s 3s kubectl --context="$context" --namespace="$ns" --request-timeout=2s "$@"
  fi
}
capture_network_failure() {
  failed_id=$1
  status_payload=$(jq -nc --arg id "$failed_id" '{kind:"sql",request_id:$id}')
  query_payload=$(jq -nc '{sql:"SELECT id,value FROM qualification ORDER BY id",consistency:"linearizable"}')
  diagnostic_pids=''
  for diagnostic_port in 18080 18081 18082; do
    for diagnostic_name in request-status recovery-status ready; do
      case "$diagnostic_name" in
        request-status) diagnostic_url="http://127.0.0.1:$diagnostic_port/qualification/request-status"; diagnostic_method=POST; diagnostic_payload=$status_payload ;;
        recovery-status) diagnostic_url="http://127.0.0.1:$((diagnostic_port + 100))/recovery/status"; diagnostic_method=GET; diagnostic_payload='' ;;
        ready) diagnostic_url="http://127.0.0.1:$diagnostic_port/ready"; diagnostic_method=GET; diagnostic_payload='' ;;
      esac
      diagnostic_label="network-failure-$diagnostic_port-$diagnostic_name"
      (
        set +e
        if [ "$diagnostic_method" = POST ]; then
          timeout --kill-after=1s 4s curl --silent --show-error --max-time 3 --output "$out/$diagnostic_label.stdout" \
            --write-out '%{http_code}\n%{time_total}\n' -H 'Content-Type: application/json' -d "$diagnostic_payload" \
            "$diagnostic_url" > "$out/$diagnostic_label.http-timing" 2> "$out/$diagnostic_label.stderr"
        else
          timeout --kill-after=1s 4s curl --silent --show-error --max-time 3 --output "$out/$diagnostic_label.stdout" \
            --write-out '%{http_code}\n%{time_total}\n' "$diagnostic_url" \
            > "$out/$diagnostic_label.http-timing" 2> "$out/$diagnostic_label.stderr"
        fi
        printf '%s\n' "$?" > "$out/$diagnostic_label.exit"
      ) & diagnostic_pids="$diagnostic_pids $!"
    done
    diagnostic_label="network-failure-$diagnostic_port-linearizable-read"
    (
      set +e
      timeout --kill-after=1s 4s curl --silent --show-error --max-time 3 --output "$out/$diagnostic_label.stdout" \
        --write-out '%{http_code}\n%{time_total}\n' -H 'Content-Type: application/json' -d "$query_payload" \
        "http://127.0.0.1:$diagnostic_port/sql/query" > "$out/$diagnostic_label.http-timing" 2> "$out/$diagnostic_label.stderr"
      printf '%s\n' "$?" > "$out/$diagnostic_label.exit"
    ) & diagnostic_pids="$diagnostic_pids $!"
  done
  for diagnostic_resource in fault pods endpoints endpointslices.discovery.k8s.io; do
    diagnostic_label="network-failure-${diagnostic_resource%%.*}"
    (
      set +e
      case "$diagnostic_resource" in
        fault) diagnostic_k get "$active_fault" -o json > "$out/$diagnostic_label.stdout" 2> "$out/$diagnostic_label.stderr" ;;
        pods) diagnostic_k get pods rhiza-voter-0 rhiza-voter-1 rhiza-voter-2 -o json > "$out/$diagnostic_label.stdout" 2> "$out/$diagnostic_label.stderr" ;;
        endpoints) diagnostic_k get endpoints rhiza-peers -o json > "$out/$diagnostic_label.stdout" 2> "$out/$diagnostic_label.stderr" ;;
        endpointslices.discovery.k8s.io) diagnostic_k get endpointslices.discovery.k8s.io -l kubernetes.io/service-name=rhiza-peers -o json > "$out/$diagnostic_label.stdout" 2> "$out/$diagnostic_label.stderr" ;;
      esac
      printf '%s\n' "$?" > "$out/$diagnostic_label.exit"
    ) & diagnostic_pids="$diagnostic_pids $!"
  done
  set +e
  for diagnostic_pid in $diagnostic_pids; do wait "$diagnostic_pid"; done
  set -e
}
readback() {
  port=$1; consistency=$2; expected=$3
  payload=$(jq -nc --arg c "$consistency" '{sql:"SELECT id,value FROM qualification ORDER BY id",consistency:$c}')
  run readback curl -fsS --max-time 30 -H 'Content-Type: application/json' -d "$payload" "http://127.0.0.1:$port/sql/query"
  jq -e --argjson expected "$expected" '.rows==$expected and .applied_slot>0 and .consensus_tip>=.applied_slot' "$out/$seq-readback.stdout" >/dev/null || die 'ACK/read-barrier mismatch'
}
post_network_readback() {
  port=$1; expected=$2
  payload=$(jq -nc '{sql:"SELECT id,value FROM qualification ORDER BY id",consistency:"linearizable"}')
  attempt=0
  while :; do
    post_network_seconds=$((post_network_deadline - $(date +%s)))
    if [ "$post_network_seconds" -le 0 ]; then
      printf 'phase=post-network-convergence category=unavailable-timeout port=%s attempts=%s\n' "$port" "$attempt" >&2
      return 124
    fi
    [ "$post_network_seconds" -le 5 ] || post_network_seconds=5
    attempt=$((attempt + 1)); seq=$((seq + 1)); label="post-network-readback-$port-attempt-$attempt"
    set +e
    curl --silent --show-error --max-time "$post_network_seconds" --output "$out/$seq-$label.stdout" \
      --write-out '%{http_code}\n' -H 'Content-Type: application/json' -d "$payload" \
      "http://127.0.0.1:$port/sql/query" > "$out/$seq-$label.http-status" 2> "$out/$seq-$label.stderr"
    code=$?
    set -e
    printf '%s\n' "$code" > "$out/$seq-$label.exit"
    status=$(cat "$out/$seq-$label.http-status")
    if [ "$code" != 0 ]; then
      printf 'phase=post-network-convergence category=transport-failed port=%s attempt=%s exit=%s status=%s\n' "$port" "$attempt" "$code" "${status:-missing}" >&2
      return "$code"
    fi
    if [ "$status" = 200 ]; then
      if jq -e --argjson expected "$expected" '.rows==$expected and .applied_slot>0 and .consensus_tip>=.applied_slot' "$out/$seq-$label.stdout" >/dev/null; then
        return 0
      fi
      printf 'phase=post-network-convergence category=response-mismatch port=%s attempt=%s\n' "$port" "$attempt" >&2
      return 1
    fi
    if [ "$status" != 503 ]; then
      printf 'phase=post-network-convergence category=http-status port=%s attempt=%s status=%s\n' "$port" "$attempt" "$status" >&2
      return 1
    fi
    [ "$(date +%s)" -lt "$post_network_deadline" ] || {
      printf 'phase=post-network-convergence category=unavailable-timeout port=%s attempts=%s\n' "$port" "$attempt" >&2
      return 124
    }
    sleep 2
  done
}
execute "$RHIZA_RUN_ID-schema" 'CREATE TABLE qualification (id INTEGER PRIMARY KEY,value TEXT NOT NULL)'
execute "$RHIZA_RUN_ID-seed" "INSERT INTO qualification VALUES (1,'before')"
for port in 18080 18081 18082; do readback "$port" linearizable '[[1,"before"]]'; done
validate_network_injection() {
  jq -e --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '
    def applied: ([.events[]?|select(.operation=="Apply")]|length)>0 and ([.events[]?|select(.operation=="Apply")]|last.type)=="Succeeded";
    [.status.experiment.containerRecords[]?|select(.selectorKey==".")] as $source |
    [.status.experiment.containerRecords[]?|select(.selectorKey==".Target")] as $targets |
    .kind=="NetworkChaos" and .metadata.name=="network-one" and .metadata.namespace==$ns and
    .metadata.labels["chaos.rhiza.io/run"]==$run and .spec.action=="partition" and .spec.direction=="both" and
    .spec.mode=="one" and .spec.duration=="45s" and
    .spec.selector.labelSelectors["statefulset.kubernetes.io/pod-name"]=="rhiza-voter-0" and
    .spec.target.mode=="all" and
    .spec.target.selector.expressionSelectors==[{key:"statefulset.kubernetes.io/pod-name",operator:"In",values:["rhiza-voter-1","rhiza-voter-2"]}] and
    ([.status.conditions[]?|select(.type=="AllInjected" and .status=="True")]|length)==1 and
    ([.status.conditions[]?|select(.type=="AllRecovered" and .status=="True")]|length)==0 and
    ($source|length)==1 and ($targets|length)==2 and
    ($source[0] | .id==($ns+"/rhiza-voter-0") and .phase=="Injected" and .injectedCount==1 and .recoveredCount==0 and applied) and
    ($targets|map(.id)|sort)==[$ns+"/rhiza-voter-1",$ns+"/rhiza-voter-2"] and
    all($targets[]; .phase=="Injected" and .injectedCount==1 and .recoveredCount==0 and applied) and
    ([($source+$targets)[].id]|unique|length)==3
  ' "$1" >/dev/null
}
fault() {
  kind=$1; action=$2; name=$3
  case "$kind" in
    NetworkChaos) expected_policy="$ns-network-faults" ;;
    PodChaos) expected_policy="$ns-faults" ;;
    *) die 'unsupported qualification fault kind' ;;
  esac
  jq -n --arg kind "$kind" --arg action "$action" --arg ns "$ns" --arg run "$RHIZA_RUN_ID" --arg name "$name" \
    '{apiVersion:"chaos-mesh.org/v1alpha1",kind:$kind,metadata:{name:$name,namespace:$ns,labels:{"chaos.rhiza.io/run":$run}},spec:{action:$action,mode:"one",duration:"10s",selector:{namespaces:[$ns],labelSelectors:{"app.kubernetes.io/part-of":"rhiza-chaos","chaos.rhiza.io/run":$run,"statefulset.kubernetes.io/pod-name":"rhiza-voter-0"}}}}' > "$out/$name.json"
  if [ "$kind" = NetworkChaos ]; then
    jq --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '.spec.duration="45s"|.spec.direction="both"|.spec.target={mode:"all",selector:{namespaces:[$ns],labelSelectors:{app:"rhiza-voter","chaos.rhiza.io/run":$run},expressionSelectors:[{key:"statefulset.kubernetes.io/pod-name",operator:"In",values:["rhiza-voter-1","rhiza-voter-2"]}]}}' "$out/$name.json" > "$out/$name-network.json"
    cp "$out/$name-network.json" "$out/$name.json"
  else
    jq '.spec.containerNames=["rhiza"]' "$out/$name.json" > "$out/$name-container.json"
    cp "$out/$name-container.json" "$out/$name.json"
  fi
  # Prove server admission rejects an out-of-namespace selector without
  # actually creating a fault or reading the foreign namespace.
  jq '.metadata.name="denied-scope"|.spec.selector.namespaces=["default"]' "$out/$name.json" > "$out/denied-scope.json"
  seq=$((seq + 1))
  set +e
  k create --dry-run=server -f "$out/denied-scope.json" > "$out/$seq-selector-denial.stdout" 2> "$out/$seq-selector-denial.stderr"
  denied_code=$?
  set -e
  printf '%s\n' "$denied_code" > "$out/$seq-selector-denial.exit"
  [ "$denied_code" != 0 ] && grep -Fq "ValidatingAdmissionPolicy '$expected_policy' " "$out/$seq-selector-denial.stderr" || die 'server-side selector confinement not proven by the expected policy'
  run fault-validate k create --dry-run=server -f "$out/$name.json"
  # Registration precedes the write: an accepted create with lost response
  # still has one exact run-owned target for safe absent-aware cleanup.
  active_fault="$kind/$name"
  run fault-create k create -f "$out/$name.json"
  run injected k wait "$active_fault" --for=condition=AllInjected=True --timeout=60s
  run injection k get "$active_fault" -o json
  if [ "$kind" = NetworkChaos ]; then
    validate_network_injection "$out/$seq-injection.stdout" || die 'exact disjoint network partition injection not proven'
  fi
}
container_kill() {
  run process-before pod_snapshot
  cp "$out/$seq-process-before.stdout" "$out/process-before.json"
  jq -e --slurpfile original "$out/voters-before.json" '
    def voters: [.[]|select(.name|test("^rhiza-voter-[012]$"))]|sort_by(.name);
    voters as $b | ($original[0]|voters) as $o |
    ($b|map(.name))==["rhiza-voter-0","rhiza-voter-1","rhiza-voter-2"] and
    ($b|map({name,uid,node,volumes,containers}))==($o|map({name,uid,node,volumes,containers})) and
    all($b[]; (.uid|type)=="string" and (.uid|length)>0 and .deleting==null and .ready==true and
      (.volumes|type)=="array" and any(.volumes[]; .name=="data" and (.emptyDir|type)=="object") and
      ([.containers[]?|select(.name=="rhiza" and .restartCount==0 and
        (.containerID|type)=="string" and (.containerID|length)>0)]|length)==1)
  ' "$out/process-before.json" >/dev/null || die 'original intact-WAL voters not present before container-kill'
  voter0_restarts=1
  fault PodChaos container-kill process-one
  jq -e --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '.kind=="PodChaos" and
    .metadata.name=="process-one" and .metadata.namespace==$ns and .metadata.labels["chaos.rhiza.io/run"]==$run and
    .spec.action=="container-kill" and .spec.mode=="one" and .spec.duration=="10s" and .spec.containerNames==["rhiza"] and
    .spec.selector.namespaces==[$ns] and .spec.selector.labelSelectors["chaos.rhiza.io/run"]==$run and
    .spec.selector.labelSelectors["statefulset.kubernetes.io/pod-name"]=="rhiza-voter-0" and
    ([.status.conditions[]?|select(.type=="AllInjected" and .status=="True")]|length)==1 and
    (.status.experiment.containerRecords|length)==1 and
    (.status.experiment.containerRecords[0] | .id==($ns+"/rhiza-voter-0/rhiza") and
      .phase=="Injected" and .injectedCount==1 and .recoveredCount==0 and
      ([.events[]?|select(.operation=="Apply" and .type=="Succeeded")]|length)==1)
  ' "$out/$seq-injection.stdout" >/dev/null || die 'exact single container-kill injection not proven'
  wait_process_restart() (
    voter_wait_deadline=$(( $(date +%s) + 30 ))
    restart_current="$out/process-restart-current.json"
    restart_next="$restart_current.next"
    trap 'rm -f "$restart_next"' 0 HUP INT TERM
    while :; do
      if k get pod rhiza-voter-0 -o json > "$restart_next"; then
        mv "$restart_next" "$restart_current"
      else
        restart_code=$?
        rm -f "$restart_next"
        case "$restart_code" in 124|137) printf '%s\n' 'phase=restart-identity category=deadline' >&2; exit "$restart_code" ;; esac
      fi
      if jq -e --slurpfile before "$out/process-before.json" '
        [.status.containerStatuses[]?|select(.name=="rhiza")] as $current |
        [($before[0][]|select(.name=="rhiza-voter-0")|.containers[]?)|select(.name=="rhiza")] as $sealed |
        .metadata.name=="rhiza-voter-0" and .metadata.uid==($before[0][]|select(.name=="rhiza-voter-0")|.uid) and
        .metadata.deletionTimestamp==null and
        ($current|length)==1 and ($sealed|length)==1 and
        $current[0].restartCount==$sealed[0].restartCount+1 and
        ($current[0].containerID|type)=="string" and ($current[0].containerID|length)>0 and
        $current[0].containerID!=$sealed[0].containerID
      ' "$restart_current"; then
        cat "$restart_current"
        exit 0
      fi
      [ "$(date +%s)" -lt "$voter_wait_deadline" ] || { printf '%s\n' 'phase=restart-identity category=deadline' >&2; exit 124; }
      sleep 1
    done
  )
  run restart-evidence wait_process_restart
  # ContainerKill is one-shot: duration does not recover it. Delete the exact
  # fault and wait for its finalizer before proving application recovery.
  run fault-delete k delete "$active_fault" --wait=true --timeout=60s
  active_fault=''
  wait_process_ready() (
    voter_wait_deadline=$(( $(date +%s) + 180 ))
    voter_wait_seconds=$((voter_wait_deadline - $(date +%s)))
    [ "$voter_wait_seconds" -gt 0 ] || exit 124
    wait_voters "$voter_wait_seconds" || { ready_code=$?; printf 'phase=application-readiness category=wait-failed exit=%s\n' "$ready_code" >&2; exit "$ready_code"; }
    ready_current="$out/process-ready-current.json"
    ready_next="$ready_current.next"
    trap 'rm -f "$ready_next"' 0 HUP INT TERM
    k get pod rhiza-voter-0 -o json > "$ready_next" || { ready_code=$?; printf 'phase=application-readiness category=read-failed exit=%s\n' "$ready_code" >&2; exit "$ready_code"; }
    mv "$ready_next" "$ready_current"
    jq -e --slurpfile before "$out/process-before.json" '
      [.status.containerStatuses[]?|select(.name=="rhiza")] as $current |
      .metadata.uid==($before[0][]|select(.name=="rhiza-voter-0")|.uid) and
      .metadata.deletionTimestamp==null and any(.status.conditions[]?;.type=="Ready" and .status=="True") and
      ($current|length)==1 and $current[0].restartCount==1 and
      ($current[0].containerID|type)=="string" and ($current[0].containerID|length)>0
    ' "$ready_current" || { printf '%s\n' 'phase=application-readiness category=identity-invalid' >&2; exit 1; }
    cat "$ready_current"
  )
  run voters-ready wait_process_ready
  execute "$RHIZA_RUN_ID-process" "INSERT INTO qualification VALUES (3,'process')"
  forward rhiza-voter-0 18080
  run after pod_snapshot
  jq -e --slurpfile before "$out/process-before.json" --slurpfile original "$out/voters-before.json" '
    def voters: [.[]|select(.name|test("^rhiza-voter-[012]$"))]|sort_by(.name);
    def container: [.containers[]?|select(.name=="rhiza")];
    voters as $a | ($before[0]|voters) as $b | ($original[0]|voters) as $o |
    ($a|map(.name))==["rhiza-voter-0","rhiza-voter-1","rhiza-voter-2"] and
    ($b|map(.name))==($a|map(.name)) and ($o|map(.name))==($a|map(.name)) and
    all(range(0;3); . as $i | $a[$i] as $new | $b[$i] as $old |
      ($new|container[0]) as $nc | ($old|container[0]) as $oc |
      $new.name==$old.name and $new.uid==$old.uid and $new.uid==$o[$i].uid and
      ($new.uid|type)=="string" and ($new.uid|length)>0 and
      $new.node==$old.node and $new.volumes==$old.volumes and $new.volumes==$o[$i].volumes and
      $new.deleting==null and $new.ready==true and
      ($new|container|length)==1 and ($old|container|length)==1 and
      ($nc.containerID|type)=="string" and ($nc.containerID|length)>0 and
      ($oc.containerID|type)=="string" and ($oc.containerID|length)>0 and
      ($nc.imageID|type)=="string" and ($nc.imageID|length)>0 and $nc.imageID==$oc.imageID and
      if $i==0 then $oc.restartCount==0 and $nc.restartCount==1 and $nc.containerID!=$oc.containerID
      else $nc.restartCount==0 and $oc.restartCount==0 and $nc.containerID==$oc.containerID end)
  ' "$out/$seq-after.stdout" >/dev/null || die 'voter UID/WAL identity changed or exact container restart not proven'
  metrics
  for port in 18080 18081 18082; do readback "$port" linearizable '[[1,"before"],[2,"network"],[3,"process"]]'; done
}
fault NetworkChaos partition network-one
execute "$RHIZA_RUN_ID-network" "INSERT INTO qualification VALUES (2,'network')"
run recovered k wait "$active_fault" --for=condition=AllRecovered=True --timeout=90s
run fault-delete k delete "$active_fault" --wait=true --timeout=60s
active_fault=''
run voters-ready wait_voters 120
post_network_now=$(date +%s)
post_network_deadline=$((post_network_now + 30))
post_network_workload_deadline=$((started + 1200))
[ "$post_network_deadline" -le "$post_network_workload_deadline" ] || post_network_deadline=$post_network_workload_deadline
if [ "$mode" = run-local ] && [ "$post_network_deadline" -gt "$local_deadline" ]; then post_network_deadline=$local_deadline; fi
if [ -n "${forward_deadline:-}" ] && [ "$post_network_deadline" -gt "$forward_deadline" ]; then post_network_deadline=$forward_deadline; fi
[ "$post_network_deadline" -gt "$post_network_now" ] || {
  printf '%s\n' 'phase=post-network-convergence category=budget-exhausted' >&2
  exit 124
}
for port in 18080 18081 18082; do post_network_readback "$port" '[[1,"before"],[2,"network"]]'; done
metrics
container_kill
execute "$RHIZA_RUN_ID-cold-sentinel" "INSERT INTO qualification VALUES (4,'$RHIZA_RUN_ID-cold-sentinel')"
readback 18081 linearizable "[[1,\"before\"],[2,\"network\"],[3,\"process\"],[4,\"$RHIZA_RUN_ID-cold-sentinel\"]]"
source_barrier=$(jq -r '.applied_slot' "$out/$seq-readback.stdout")
sleep 15
capture_shutdown || die 'source shutdown capture failed'
run controller-before k get statefulset/rhiza-voter -o json
cp "$out/$seq-controller-before.stdout" "$out/shutdown-controller-before.json"
jq -e '.spec.replicas==3 and (.metadata.uid|type)=="string" and (.metadata.uid|length)>0' "$out/shutdown-controller-before.json" >/dev/null || die 'voter StatefulSet identity invalid'
run stop-voters k scale statefulset/rhiza-voter --current-replicas=3 --replicas=0
run voters-gone k wait pod --selector="app=rhiza-voter,chaos.rhiza.io/run=$RHIZA_RUN_ID" --for=delete --timeout=90s
finish_shutdown_capture || die 'authentic voter shutdown proof failed'
verify_voter_absence || die 'voter or endpoint identity remains after scale zero'
source_quiesced=true

# Exercise the installed admission policy before creating any persistent
# inspector resource. A schema or transport failure is not accepted as a VAP
# rejection by reject_server_dry_run.
alternate_digest=$(printf '%064d' 0)
alternate_image="${RHIZA_HOST_IMAGE%@*}@sha256:$alternate_digest"
[ "$RHIZA_HOST_IMAGE" != "$alternate_image" ] || alternate_digest=$(printf '%064d' 1)
sed "s|$RHIZA_HOST_IMAGE|${RHIZA_HOST_IMAGE%@*}@sha256:$alternate_digest|" "$out/inspector-pod.yaml" > "$out/inspector-negative-digest.yaml"
awk '/command:/{print; print "      lifecycle: {postStart: {exec: {command: [sh, -c, \"true\"]}}}"; next} {print}' "$out/inspector-pod.yaml" > "$out/inspector-negative-lifecycle.yaml"
sed 's/serviceAccountName: rhiza-inspector/serviceAccountName: rhiza-gcs/' "$out/inspector-pod.yaml" > "$out/inspector-negative-ksa.yaml"
sed 's/name: rhiza-inspector$/name: rhiza-inspector-spoof/' "$out/inspector-pod.yaml" > "$out/inspector-negative-spoof.yaml"
awk '/command:/{print; print "      env: [{name: FORBIDDEN, value: \"true\"}]"; next} {print}' "$out/inspector-pod.yaml" > "$out/inspector-negative-env.yaml"
awk '/restartPolicy:/{print; print "  volumes: [{name: forbidden, emptyDir: {}}]"; next} {print}' "$out/inspector-pod.yaml" > "$out/inspector-negative-volume.yaml"
sed 's/hostPID: false/hostPID: true/' "$out/inspector-pod.yaml" > "$out/inspector-negative-host.yaml"
for negative in digest lifecycle ksa spoof env volume host; do
  reject_server_dry_run "inspector-negative-$negative" "$out/inspector-negative-$negative.yaml"
done
sed "s/app: 'rhiza-inspector-control'/app: 'rhiza-inspector'/" "$out/inspector-control-pod.yaml" > "$out/inspector-control-negative-app.yaml"
sed 's/^  name: rhiza-inspector-control$/  name: rhiza-inspector-control-spoof/' "$out/inspector-control-pod.yaml" > "$out/inspector-control-negative-name.yaml"
sed 's/^    - name: control$/    - name: inspector/' "$out/inspector-control-pod.yaml" > "$out/inspector-control-negative-container.yaml"
for negative in app name container; do
  reject_server_dry_run "inspector-control-negative-$negative" "$out/inspector-control-negative-$negative.yaml"
done

inspector_attempted=true
printf '%s\n' 'inspector resources may require exact cleanup' > "$out/inspector-manual-cleanup-required"
run inspector-ksa k get serviceaccount rhiza-inspector -o json
run inspector-rolebindings k get rolebindings.rbac.authorization.k8s.io -o json
run inspector-clusterrolebindings k get clusterrolebindings.rbac.authorization.k8s.io -o json
run inspector-networkpolicy k get networkpolicy.networking.k8s.io rhiza-inspector-deny-all -o json
validate_inspector_access "$out/$seq-inspector-ksa.stdout" "$out/$seq-inspector-rolebindings.stdout" \
  "$out/$seq-inspector-clusterrolebindings.stdout" "$out/$seq-inspector-networkpolicy.stdout" || \
  die 'inspector service account, RBAC, or default-deny network policy contract mismatch'

run inspector-create k create -f "$out/inspector-pod.yaml" -o json
cp "$out/$seq-inspector-create.stdout" "$out/inspector-created.json"
jq -e --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '.kind=="Pod" and .metadata.name=="rhiza-inspector" and
  .metadata.namespace==$ns and .metadata.labels=={"app":"rhiza-inspector","chaos.rhiza.io/run":$run} and
  (.metadata.uid|type)=="string" and (.metadata.uid|length)>0 and
  (.metadata.resourceVersion|type)=="string" and (.metadata.resourceVersion|length)>0' \
  "$out/inspector-created.json" >/dev/null || die 'inspector Pod create identity unresolved'
run inspector-ready k wait pod/rhiza-inspector --for=condition=Ready --timeout=180s
run inspector-identity k get pod rhiza-inspector -o json
validate_inspector_pod "$out/$seq-inspector-identity.stdout" || die 'inspector identity, security, readiness, or image contract mismatch'
run inspector-control-create k create -f "$out/inspector-control-pod.yaml" -o json
cp "$out/$seq-inspector-control-create.stdout" "$out/inspector-control-created.json"
jq -e --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '.kind=="Pod" and .metadata.name=="rhiza-inspector-control" and
  .metadata.namespace==$ns and .metadata.labels=={"app":"rhiza-inspector-control","chaos.rhiza.io/run":$run} and
  (.metadata.uid|type)=="string" and (.metadata.uid|length)>0 and
  (.metadata.resourceVersion|type)=="string" and (.metadata.resourceVersion|length)>0' \
  "$out/inspector-control-created.json" >/dev/null || die 'inspector control Pod create identity unresolved'
run inspector-control-ready k wait pod/rhiza-inspector-control --for=condition=Ready --timeout=180s
run inspector-control-identity k get pod rhiza-inspector-control -o json
validate_inspector_control_pod "$out/$seq-inspector-control-identity.stdout" || die 'inspector control identity, security, readiness, image, or node contract mismatch'
api_server=${RHIZA_LOCAL_API_SERVER:-$(kubectl --context="$context" config view --minify -o 'jsonpath={.clusters[0].cluster.server}')}
verify_inspector_network_isolation "$api_server"

# Freeze each mutable pointer only after writers and endpoints are absent. A
# metadata/body/metadata triple rejects a generation change during either read.
run head-metadata-freeze-1 k exec rhiza-metadata -- gcloud storage objects describe "${storage}${cluster}/archive/head.bin" '--format=json(bucket,name,generation,size)'
preserve_run_output head-metadata-freeze-1 "$out/archive-head-metadata-before.json"
validate_archive_head_metadata "$out/archive-head-metadata-before.json" || die 'published archive head metadata invalid'
run head-freeze k exec rhiza-metadata -- gcloud storage cat "${storage}${cluster}/archive/head.bin"
preserve_run_output head-freeze "$out/archive-head-before.bin"
sha256_file "$out/archive-head-before.bin" > "$out/archive-head-before.sha256"
run head-metadata-freeze-2 k exec rhiza-metadata -- gcloud storage objects describe "${storage}${cluster}/archive/head.bin" '--format=json(bucket,name,generation,size)'
preserve_run_output head-metadata-freeze-2 "$out/archive-head-metadata-freeze-2.json"
validate_archive_head_metadata "$out/archive-head-metadata-freeze-2.json" &&
  same_archive_head_metadata "$out/archive-head-metadata-before.json" "$out/archive-head-metadata-freeze-2.json" || die 'archive head drifted during frozen read'
run archive-head-decoded decode_archive_head "$out/archive-head-before.bin"
preserve_run_output archive-head-decoded "$out/archive-head-before.decoded.json"

archive_root_index=$(jq -er '.base|select(type=="number" and .>0 and .==floor)' "$out/archive-head-before.decoded.json") || die 'archive checkpoint index invalid'
archive_root_hash=$(jq -er '.base_root_hash|select(test("^[a-f0-9]{64}$"))' "$out/archive-head-before.decoded.json") || die 'archive checkpoint root hash invalid'
archive_root_index_padded=$(printf '%020d' "$archive_root_index")
run archive-root-freeze k exec rhiza-metadata -- gcloud storage cat "${storage}${cluster}/checkpoint/roots/${archive_root_index_padded}_${archive_root_hash}.json"
preserve_run_output archive-root-freeze "$out/archive-base-root-before.json"
sha256_file "$out/archive-base-root-before.json" > "$out/archive-base-root-before.sha256"
[ "$(cat "$out/archive-base-root-before.sha256")" = "$archive_root_hash" ] || die 'archive base root content hash does not match full seal'
validate_checkpoint_root_against_seal "$out/archive-base-root-before.json" "$out/archive-head-before.decoded.json" || die 'archive base root does not match full seal'

run checkpoint-metadata-freeze-1 k exec rhiza-metadata -- gcloud storage objects describe "${storage}${cluster}/checkpoint/CURRENT" '--format=json(bucket,name,generation,size)'
preserve_run_output checkpoint-metadata-freeze-1 "$out/checkpoint-CURRENT-metadata-before.json"
validate_checkpoint_current_metadata "$out/checkpoint-CURRENT-metadata-before.json" || die 'published checkpoint CURRENT metadata invalid'
run checkpoint-current-freeze k exec rhiza-metadata -- gcloud storage cat "${storage}${cluster}/checkpoint/CURRENT"
preserve_run_output checkpoint-current-freeze "$out/checkpoint-CURRENT-before.bin"
sha256_file "$out/checkpoint-CURRENT-before.bin" > "$out/checkpoint-CURRENT-before.sha256"
run checkpoint-metadata-freeze-2 k exec rhiza-metadata -- gcloud storage objects describe "${storage}${cluster}/checkpoint/CURRENT" '--format=json(bucket,name,generation,size)'
preserve_run_output checkpoint-metadata-freeze-2 "$out/checkpoint-CURRENT-metadata-freeze-2.json"
validate_checkpoint_current_metadata "$out/checkpoint-CURRENT-metadata-freeze-2.json" &&
  same_archive_head_metadata "$out/checkpoint-CURRENT-metadata-before.json" "$out/checkpoint-CURRENT-metadata-freeze-2.json" || die 'checkpoint CURRENT drifted during frozen read'
validate_current_against_archive "$out/checkpoint-CURRENT-before.bin" "$out/archive-head-before.decoded.json" || die 'checkpoint CURRENT conflicts with frozen archive base'
checkpoint_root_index=$(jq -er '.index' "$out/checkpoint-CURRENT-before.bin")
checkpoint_root_hash=$(jq -er '.root_hash' "$out/checkpoint-CURRENT-before.bin")
checkpoint_root_index_padded=$(printf '%020d' "$checkpoint_root_index")
run checkpoint-root-before k exec rhiza-metadata -- gcloud storage cat "${storage}${cluster}/checkpoint/roots/${checkpoint_root_index_padded}_${checkpoint_root_hash}.json"
preserve_run_output checkpoint-root-before "$out/checkpoint-root-before.json"
sha256_file "$out/checkpoint-root-before.json" > "$out/checkpoint-root-before.sha256"
[ "$(cat "$out/checkpoint-root-before.sha256")" = "$checkpoint_root_hash" ] || die 'checkpoint root content hash does not match CURRENT'
jq -e --argjson index "$checkpoint_root_index" '.config_id>0 and .index==$index and (.hash|type)=="array" and (.hash|length)==32' "$out/checkpoint-root-before.json" >/dev/null || die 'checkpoint CURRENT root invalid'

delete_inspector_owned || die 'owned inspector cleanup or terminal absence failed'
run inventory-before k exec rhiza-metadata -- gcloud storage ls --json "${storage}${cluster}/**"
normalize_object_inventory "$out/$seq-inventory-before.stdout" > "$out/object-inventory-before.json" || die 'cold restore baseline inventory invalid'
# Repeat both stable reads at the final pre-learner boundary and require exact
# frozen generations and body hashes. This is a scoped run-owned proof, not
# universal fencing against external writers.
run head-metadata-prelearner-1 k exec rhiza-metadata -- gcloud storage objects describe "${storage}${cluster}/archive/head.bin" '--format=json(bucket,name,generation,size)'
preserve_run_output head-metadata-prelearner-1 "$out/archive-head-metadata-prelearner-1.json"
run head-prelearner k exec rhiza-metadata -- gcloud storage cat "${storage}${cluster}/archive/head.bin"
preserve_run_output head-prelearner "$out/archive-head-prelearner.bin"
sha256_file "$out/archive-head-prelearner.bin" > "$out/archive-head-prelearner.sha256"
run head-metadata-prelearner-2 k exec rhiza-metadata -- gcloud storage objects describe "${storage}${cluster}/archive/head.bin" '--format=json(bucket,name,generation,size)'
preserve_run_output head-metadata-prelearner-2 "$out/archive-head-metadata-prelearner-2.json"
validate_archive_head_metadata "$out/archive-head-metadata-prelearner-1.json" &&
  validate_archive_head_metadata "$out/archive-head-metadata-prelearner-2.json" &&
  same_object_evidence "$out/archive-head-metadata-before.json" "$out/archive-head-metadata-prelearner-1.json" "$out/archive-head-before.sha256" "$out/archive-head-prelearner.sha256" &&
  same_archive_head_metadata "$out/archive-head-metadata-prelearner-1.json" "$out/archive-head-metadata-prelearner-2.json" || die 'archive head changed before learner creation'
run checkpoint-metadata-prelearner-1 k exec rhiza-metadata -- gcloud storage objects describe "${storage}${cluster}/checkpoint/CURRENT" '--format=json(bucket,name,generation,size)'
preserve_run_output checkpoint-metadata-prelearner-1 "$out/checkpoint-CURRENT-metadata-prelearner-1.json"
run checkpoint-current-prelearner k exec rhiza-metadata -- gcloud storage cat "${storage}${cluster}/checkpoint/CURRENT"
preserve_run_output checkpoint-current-prelearner "$out/checkpoint-CURRENT-prelearner.bin"
sha256_file "$out/checkpoint-CURRENT-prelearner.bin" > "$out/checkpoint-CURRENT-prelearner.sha256"
run checkpoint-metadata-prelearner-2 k exec rhiza-metadata -- gcloud storage objects describe "${storage}${cluster}/checkpoint/CURRENT" '--format=json(bucket,name,generation,size)'
preserve_run_output checkpoint-metadata-prelearner-2 "$out/checkpoint-CURRENT-metadata-prelearner-2.json"
validate_checkpoint_current_metadata "$out/checkpoint-CURRENT-metadata-prelearner-1.json" &&
  validate_checkpoint_current_metadata "$out/checkpoint-CURRENT-metadata-prelearner-2.json" &&
  same_object_evidence "$out/checkpoint-CURRENT-metadata-before.json" "$out/checkpoint-CURRENT-metadata-prelearner-1.json" "$out/checkpoint-CURRENT-before.sha256" "$out/checkpoint-CURRENT-prelearner.sha256" &&
  same_archive_head_metadata "$out/checkpoint-CURRENT-metadata-prelearner-1.json" "$out/checkpoint-CURRENT-metadata-prelearner-2.json" || die 'checkpoint CURRENT changed before learner creation'
learner_attempted=true
run cold-create k create -f "$out/learner.yaml" -o json
# kubectl writes one JSON document per created object; seal both identities in
# one valid ACK before extracting the Pod, rather than doing a name-only lookup.
normalize_learner_create_ack "$out/$seq-cold-create.stdout" > "$out/learner-create-ack.json" || die 'learner create identities unresolved'
jq '[.items[]|select(.kind=="Pod" and .metadata.name=="rhiza-learner")] |
  if length==1 then .[0] else error("learner create UID unresolved") end' \
  "$out/learner-create-ack.json" > "$out/learner-created.json" || die 'learner create UID unresolved'
jq -e --arg ns "$ns" --arg run "$RHIZA_RUN_ID" '.metadata.name=="rhiza-learner" and
  .metadata.namespace==$ns and .metadata.labels["chaos.rhiza.io/run"]==$run and
  (.metadata.uid|type)=="string" and (.metadata.uid|length)>0' "$out/learner-created.json" >/dev/null || die 'learner create UID unresolved'
run learner-identity k get pod rhiza-learner -o json
validate_learner_pod "$out/$seq-learner-identity.stdout" || die 'learner create identity or emptyDir contract mismatch'
run learner-ready k wait pod/rhiza-learner --for=condition=Ready --timeout=180s
learner_uid=$(jq -r '.metadata.uid' "$out/learner-created.json")
forward rhiza-learner 18083 "$learner_uid"
learner_started=true
metrics
jq -e '.bytes_downloaded>0' "$counters" >/dev/null || die 'fresh learner GCS download evidence absent'
cp "$counters" "$out/learner-counters-after-open.json"
tries=0
payload='{"sql":"SELECT id,value FROM qualification ORDER BY id","consistency":"local"}'
while :; do
  run learner-local curl -fsS --max-time 5 -H 'Content-Type: application/json' -d "$payload" http://127.0.0.1:18083/sql/query
  learner_read="$out/$seq-learner-local.stdout"
  run learner-status curl -fsS --max-time 5 http://127.0.0.1:18083/replica/status
  learner_status="$out/$seq-learner-status.stdout"
  if jq -e --argjson barrier "$source_barrier" \
      '.mode=="object-store" and .source=="object-store" and .lag_slots==0 and
       .applied_slot==.source_tip and .applied_slot>=$barrier and (.last_error==null or .last_error=="")' "$learner_status" >/dev/null &&
    jq -e --slurpfile status "$learner_status" --arg sentinel "$RHIZA_RUN_ID-cold-sentinel" \
      '.rows==[[1,"before"],[2,"network"],[3,"process"],[4,$sentinel]] and
       .applied_slot==$status[0].applied_slot and .consensus_tip==$status[0].source_tip' "$learner_read" >/dev/null; then break; fi
  tries=$((tries + 1)); [ "$tries" -lt 30 ] || die 'cold learner tips/readback did not match sealed source'; sleep 1
done
validate_cold_restore_status "$learner_status" "$out/archive-head-before.decoded.json" \
  "$out/archive-head-metadata-before.json" "$out/learner-counters-after-open.json" || die 'cold replica did not prove certified checkpoint plus archive restore'
mutation_payload=$(jq -nc --arg id "$RHIZA_RUN_ID-cold-rejected" '{request_id:$id,sql:"INSERT INTO qualification VALUES (5,\"rejected\")"}')
run learner-mutation-rejected curl -sS --max-time 5 --output "$out/learner-mutation-rejected.json" \
  --write-out '%{http_code}\n' -H 'Content-Type: application/json' -d "$mutation_payload" http://127.0.0.1:18083/sql/execute
[ "$(cat "$out/$seq-learner-mutation-rejected.stdout")" = 503 ] &&
  jq -e '.code=="not_ready"' "$out/learner-mutation-rejected.json" >/dev/null || die 'cold object-store replica accepted mutation'
linearizable_payload='{"sql":"SELECT id,value FROM qualification ORDER BY id","consistency":"linearizable"}'
run learner-linearizable-rejected curl -sS --max-time 5 --output "$out/learner-linearizable-rejected.json" \
  --write-out '%{http_code}\n' -H 'Content-Type: application/json' -d "$linearizable_payload" http://127.0.0.1:18083/sql/query
[ "$(cat "$out/$seq-learner-linearizable-rejected.stdout")" = 503 ] &&
  jq -e '.code=="quorum_unavailable"' "$out/learner-linearizable-rejected.json" >/dev/null || die 'cold object-store replica served linearizable read'
run learner-topology k get pod rhiza-learner -o json
validate_learner_pod "$out/$seq-learner-topology.stdout" || die 'final learner identity or emptyDir contract mismatch'
run head-metadata-after k exec rhiza-metadata -- gcloud storage objects describe "${storage}${cluster}/archive/head.bin" '--format=json(bucket,name,generation,size)'
preserve_run_output head-metadata-after "$out/archive-head-metadata-after.json"
validate_archive_head_metadata "$out/archive-head-metadata-after.json" || die 'published archive head metadata changed to invalid'
run head-after k exec rhiza-metadata -- gcloud storage cat "${storage}${cluster}/archive/head.bin"
preserve_run_output head-after "$out/archive-head-after.bin"
sha256_file "$out/archive-head-after.bin" > "$out/archive-head-after.sha256"
run checkpoint-current-after k exec rhiza-metadata -- gcloud storage cat "${storage}${cluster}/checkpoint/CURRENT"
preserve_run_output checkpoint-current-after "$out/checkpoint-CURRENT-after.bin"
sha256_file "$out/checkpoint-CURRENT-after.bin" > "$out/checkpoint-CURRENT-after.sha256"
run checkpoint-metadata-after k exec rhiza-metadata -- gcloud storage objects describe "${storage}${cluster}/checkpoint/CURRENT" '--format=json(bucket,name,generation,size)'
preserve_run_output checkpoint-metadata-after "$out/checkpoint-CURRENT-metadata-after.json"
validate_checkpoint_current_metadata "$out/checkpoint-CURRENT-metadata-after.json" || die 'published checkpoint CURRENT metadata changed to invalid'
recovery_pin_id=$(jq -r '.recovery_pin_id' "$learner_status")
run learner-clock k exec rhiza-learner -- rhiza qualification-unix-milli
preserve_run_output learner-clock "$out/learner-clock-before-terminal.txt"
learner_clock_ms=$(cat "$out/learner-clock-before-terminal.txt")
printf '%s' "$learner_clock_ms" | grep -Eq '^[1-9][0-9]{0,15}$' || die 'learner container clock invalid'
run archive-gc-lock-terminal redacted_storage_json "${storage}${cluster}/archive/GC_LOCK"
preserve_run_output archive-gc-lock-terminal "$out/archive-GC_LOCK-terminal.redacted.json"
run checkpoint-publisher-terminal redacted_storage_json "${storage}${cluster}/checkpoint/PUBLISHER"
preserve_run_output checkpoint-publisher-terminal "$out/checkpoint-PUBLISHER-terminal.redacted.json"
run archive-recovery-pin-terminal redacted_storage_json "${storage}${cluster}/archive/recovery-pins/$recovery_pin_id"
preserve_run_output archive-recovery-pin-terminal "$out/archive-recovery-pin-terminal.redacted.json"
run checkpoint-recovery-pin-terminal redacted_storage_json "${storage}${cluster}/checkpoint/recovery-pins/$recovery_pin_id"
preserve_run_output checkpoint-recovery-pin-terminal "$out/checkpoint-recovery-pin-terminal.redacted.json"
run inventory-after k exec rhiza-metadata -- gcloud storage ls --json "${storage}${cluster}/**"
normalize_object_inventory "$out/$seq-inventory-after.stdout" > "$out/object-inventory-after.json" || die 'cold restore final inventory invalid'
validate_cold_inventory "$out/object-inventory-before.json" "$out/object-inventory-after.json" "$recovery_pin_id" \
  "$learner_status" "$out/archive-head-before.decoded.json" "$out/archive-base-root-before.json" \
  "$out/archive-GC_LOCK-terminal.redacted.json" \
  "$out/checkpoint-PUBLISHER-terminal.redacted.json" "$out/archive-recovery-pin-terminal.redacted.json" \
  "$out/checkpoint-recovery-pin-terminal.redacted.json" "$out/archive-head-before.decoded.json" \
  "$learner_clock_ms" || die 'cold replica terminal coordination or durable object contract invalid'
same_object_evidence "$out/archive-head-metadata-before.json" "$out/archive-head-metadata-after.json" "$out/archive-head-before.sha256" "$out/archive-head-after.sha256" || die 'archive head changed during cold restore proof'
same_object_evidence "$out/checkpoint-CURRENT-metadata-before.json" "$out/checkpoint-CURRENT-metadata-after.json" "$out/checkpoint-CURRENT-before.sha256" "$out/checkpoint-CURRENT-after.sha256" || die 'checkpoint CURRENT changed during cold restore proof'
metrics
printf '%s\n' 'Sampled UID/container high-water totals exclude unseen startup/kill/shutdown tails and metadata SDK calls; 10k attempts/384MiB are planned reserves, NOT hard billing guarantees. Root must reconcile actual object generations/transfer and cleanup.' > "$out/budget-limitations.txt"
printf '%s\n' 'PASS selected GCS/process/network/fresh-object-store-read-replica layer; no writable whole-generation DR, promotion, fencing, IO/power-loss or published Rust package claim.' > "$out/result.txt"
