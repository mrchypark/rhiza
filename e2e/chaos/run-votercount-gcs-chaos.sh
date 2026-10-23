#!/bin/sh
# Launcher for the voterCount GCS/Chaos qualification (see README.md).
#
# Run e2e/chaos/bootstrap-gcs-postrelease.sh apply-local first with an
# administrator kubeconfig. Then run `install` with that same privileged
# kubeconfig, followed by `run` with the scoped runtime kubeconfig. The install
# phase owns CRDs, TLS, RBAC, Deployments and the RhizaCluster; the runtime phase
# cannot create those objects. Every step is checked.
#
# Required (images must be digest-pinned): RHIZA_RUN_ID, RHIZA_NAMESPACE,
# RHIZA_KUBE_CONTEXT, RHIZA_GCS_BUCKET, RHIZA_GCS_PREFIX, RHIZA_CLUSTER_ID,
# RHIZA_GCP_PROJECT_ID, RHIZA_DATA_GSA_ID (control-plane writer),
# RHIZA_READONLY_WORKLOAD_GSA_ID, node-qualified runtime profile inputs, and
# pre-provisioned runtime-agent CA/TLS Secrets.
set -eu

mode=${1:-install}
case "$mode" in install|run) ;; *) echo "usage: $0 install|run" >&2; exit 2 ;; esac
[ "${RHIZA_GCS_INDEPENDENT_LAYOUT:-}" = yes ] || {
  echo 'independent GCS layout must be explicitly bootstrapped; cooperative fallback is disabled' >&2
  exit 2
}

: "${RHIZA_RUN_ID:?8 lowercase hex run id}"
: "${RHIZA_NAMESPACE:?run namespace containing the run id}"
: "${RHIZA_KUBE_CONTEXT:?kube context}"
: "${RHIZA_GCS_BUCKET:?bucket}"
: "${RHIZA_GCS_PREFIX:?prefix ending in /<run id>/}"
: "${RHIZA_GCS_INDEPENDENT_LAYOUT:?set yes only after independent IAM bootstrap}"
: "${RHIZA_GCS_INDEPENDENT_CONSUMER:?independent bootstrap consumer contract}"
: "${RHIZA_AUTH_STATE:?private bootstrap receipt directory}"
[ "$RHIZA_GCS_INDEPENDENT_CONSUMER" = run-votercount-gcs-chaos/v2 ] || {
  echo 'independent bootstrap consumer contract mismatch' >&2
  exit 2
}
: "${RHIZA_CLUSTER_ID:?cluster id}"
: "${RHIZA_GCP_PROJECT_ID:?GCP project id}"
: "${RHIZA_DATA_GSA_ID:?data GSA account id}"
: "${RHIZA_READONLY_WORKLOAD_GSA_ID:?separate read-only workload GSA account id}"
: "${RHIZA_RUNTIME_AGENT_CA_SECRET:?pre-provisioned runtime-agent CA Secret name}"
: "${RHIZA_RUNTIME_AGENT_TLS_SECRET:?pre-provisioned runtime-agent TLS Secret name}"
: "${RHIZA_QUALIFIED_SANDBOX_EXECUTABLE_SHA256:=}"
: "${RHIZA_RUNTIME_AGENT_PASSIVE_KERNEL:=no}"
case "$RHIZA_RUNTIME_AGENT_PASSIVE_KERNEL" in
  no) ;;
  yes)
    [ -n "$RHIZA_QUALIFIED_SANDBOX_EXECUTABLE_SHA256" ] || {
      echo 'passive kernel proof requires an independently approved sandbox executable digest' >&2
      exit 2
    }
    ;;
  *) echo 'RHIZA_RUNTIME_AGENT_PASSIVE_KERNEL must be yes or no' >&2; exit 2 ;;
esac
: "${RHIZA_CONTAINERD_VERSION:?qualified containerd version for every eligible node}"
: "${RHIZA_CONTAINERD_RUNTIME_TYPE:?qualified containerd runtime type}"
: "${RHIZA_CGROUP_PROFILE_ID:?qualified cgroup profile identifier}"
: "${RHIZA_KERNEL_RELEASE:?qualified kernel release for every eligible node}"
: "${RHIZA_APP_IMAGE:?digest-pinned application image}"
: "${RHIZA_OPERATOR_IMAGE:?digest-pinned operator image}"
: "${RHIZA_KUBECTL_IMAGE:?digest-pinned kubectl image with /bin/sh and kubectl}"
: "${RHIZA_NODE_A:?first captured existing node}"
: "${RHIZA_NODE_B:?second captured existing node}"
: "${RHIZA_NODE_C:?third captured existing node}"
for node in "$RHIZA_NODE_A" "$RHIZA_NODE_B" "$RHIZA_NODE_C"; do
  printf '%s\n' "$node" | LC_ALL=C grep -Eq '^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$' || {
    echo 'existing node name rejected' >&2
    exit 2
  }
done
[ "$RHIZA_NODE_A" != "$RHIZA_NODE_B" ] && [ "$RHIZA_NODE_A" != "$RHIZA_NODE_C" ] && [ "$RHIZA_NODE_B" != "$RHIZA_NODE_C" ] || {
  echo 'three distinct existing nodes are required' >&2
  exit 2
}
initial_head_ack_loss=${RHIZA_GCS_INITIAL_HEAD_ACK_LOSS:-no}
case "$initial_head_ack_loss" in
  no|yes) ;;
  *) echo 'RHIZA_GCS_INITIAL_HEAD_ACK_LOSS must be yes or no' >&2; exit 2 ;;
esac
async_nonempty=${RHIZA_GCS_ASYNC_NONEMPTY:-no}
case "$async_nonempty" in
  no|yes) ;;
  *) echo 'RHIZA_GCS_ASYNC_NONEMPTY must be yes or no' >&2; exit 2 ;;
esac
manual_terminal=${RHIZA_GCS_MANUAL_TERMINAL:-no}
case "$manual_terminal" in
  no|yes) ;;
  *) echo 'RHIZA_GCS_MANUAL_TERMINAL must be yes or no' >&2; exit 2 ;;
esac
scenario_count=0
for selected in "$initial_head_ack_loss" "$async_nonempty" "$manual_terminal"; do
  [ "$selected" != yes ] || scenario_count=$((scenario_count + 1))
done
[ "$scenario_count" -le 1 ] || {
  echo 'initial-HEAD ACK-loss, async-nonempty, and manual-terminal qualifications require separate run scopes' >&2
  exit 2
}

require_digest() {
  value=$2
  case "$value" in
    *@sha256:*) digest=${value##*@sha256:} ;;
    *) echo "$1 must be digest-pinned as name@sha256:<digest>, got: $value" >&2; exit 2 ;;
  esac
  case "$digest" in
    *[!0-9a-f]*) echo "$1 sha256 digest must be lowercase hex, got: $value" >&2; exit 2 ;;
  esac
  [ "${#digest}" -eq 64 ] || { echo "$1 sha256 digest must be 64 hex characters, got: $value" >&2; exit 2; }
}
require_digest RHIZA_APP_IMAGE "$RHIZA_APP_IMAGE"
require_digest RHIZA_OPERATOR_IMAGE "$RHIZA_OPERATOR_IMAGE"
require_digest RHIZA_KUBECTL_IMAGE "$RHIZA_KUBECTL_IMAGE"
if [ -n "$RHIZA_QUALIFIED_SANDBOX_EXECUTABLE_SHA256" ]; then
  case "$RHIZA_QUALIFIED_SANDBOX_EXECUTABLE_SHA256" in
    *[!0-9a-f]*) echo 'qualified sandbox executable approval must be lowercase SHA-256 hex' >&2; exit 2 ;;
  esac
  [ "${#RHIZA_QUALIFIED_SANDBOX_EXECUTABLE_SHA256}" -eq 64 ] || {
    echo 'qualified sandbox executable approval must be 64 lowercase hex characters' >&2
    exit 2
  }
fi

here=$(cd -- "$(dirname -- "$0")" && pwd)
source_head=
if [ "$async_nonempty" = yes ]; then
  source_head=$(git -C "$here/../.." rev-parse --verify HEAD) || {
    echo 'async-nonempty qualification requires a readable source revision' >&2
    exit 2
  }
  printf '%s\n' "$source_head" | LC_ALL=C grep -Eq '^[a-f0-9]{40}$' || {
    echo 'async-nonempty source revision is not a full lowercase Git SHA' >&2
    exit 2
  }
fi
out=${RHIZA_OUT:-$PWD/votercount-gcs-chaos-results}
gcs_service_account=$RHIZA_DATA_GSA_ID@$RHIZA_GCP_PROJECT_ID.iam.gserviceaccount.com
readonly_workload_gsa=$RHIZA_READONLY_WORKLOAD_GSA_ID@$RHIZA_GCP_PROJECT_ID.iam.gserviceaccount.com
workload_prefix=${RHIZA_GCS_PREFIX}data/
authority_prefix=${RHIZA_GCS_PREFIX}control
[ "$RHIZA_DATA_GSA_ID" != "$RHIZA_READONLY_WORKLOAD_GSA_ID" ] || {
  echo 'read-only workload GSA must differ from the control-plane writer GSA' >&2
  exit 2
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    result=$(sha256sum "$1") || return
  elif command -v shasum >/dev/null 2>&1; then
    result=$(shasum -a 256 "$1") || return
  else
    return 1
  fi
  set -- $result
  [ "$#" -ge 1 ] && [ "${#1}" -eq 64 ] || return 1
  case "$1" in *[!a-f0-9]*) return 1 ;; esac
  printf '%s\n' "$1"
}

verify_bootstrap_receipt() {
  receipt=$RHIZA_AUTH_STATE/workload-identity.json
  checksum=$RHIZA_AUTH_STATE/workload-identity.sha256
  [ -d "$RHIZA_AUTH_STATE" ] && [ ! -L "$RHIZA_AUTH_STATE" ] &&
    [ -f "$receipt" ] && [ ! -L "$receipt" ] && [ -f "$checksum" ] && [ ! -L "$checksum" ] || {
      echo 'private independent-workload bootstrap receipt is missing' >&2
      return 1
    }
  for private_file in "$receipt" "$checksum"; do
    [ -n "$(find "$private_file" -type f -user "$(id -u)" -perm 0600 -print 2>/dev/null)" ] || {
      echo 'bootstrap receipt must be owned by this user and mode 0600' >&2
      return 1
    }
  done
  expected=$(cat "$checksum") || return 1
  actual=$(sha256_file "$receipt") || return 1
  [ "$expected" = "$actual" ] || { echo 'bootstrap receipt checksum mismatch' >&2; return 1; }
  command -v jq >/dev/null 2>&1 || { echo 'jq is required to validate bootstrap receipt' >&2; return 1; }
  jq -e --arg run "$RHIZA_RUN_ID" --arg ns "$RHIZA_NAMESPACE" \
    --arg prefix "$RHIZA_GCS_PREFIX" --arg writer "$gcs_service_account" \
    --arg reader "$readonly_workload_gsa" \
    --arg writer_role "projects/$RHIZA_GCP_PROJECT_ID/roles/rhiza19_${RHIZA_RUN_ID}_objects" '
      type=="object" and .run==$run and .namespace==$ns and .parentPrefix==$prefix and
      .controlPrefix==($prefix+"control") and
      .workload.ksa.name=="rhiza-workload" and
      .workload.gsa.email==$reader and .workload.gsa.role=="roles/storage.objectViewer" and
      .workload.gsa.managedFolder==($prefix+"data/") and
      .writer.gsa.email==$writer and .writer.gsa.role==$writer_role and
      (.writer.gsa.managedFolders|type)=="array" and
      (.writer.gsa.managedFolders|sort)==([$prefix+"data/",$prefix+"control/"]|sort) and
      (.writer.gsa|has("managedFolder")|not) and
      ([.writer.ksas[].name]|sort)==["rhiza-fencer","rhiza-operator"] and
      (.writer.workloadIdentityBinding.members|length)==2 and
      (.workload.workloadIdentityBinding.members|length)==1
    ' "$receipt" >/dev/null || {
      echo 'bootstrap receipt identity or scope does not match this run' >&2
      return 1
    }
  receipt_sha=$actual
}
for gsa_id in "$RHIZA_DATA_GSA_ID" "$RHIZA_READONLY_WORKLOAD_GSA_ID"; do
  printf '%s\n' "$gsa_id" | LC_ALL=C grep -Eq '^[a-z][a-z0-9-]{4,28}[a-z0-9]$' || {
    echo 'invalid GSA account ID' >&2
    exit 2
  }
done
printf '%s\n' "$RHIZA_RUN_ID" | LC_ALL=C grep -Eq '^[a-f0-9]{8}$' || {
  echo 'run ID must be eight lowercase hex characters' >&2
  exit 2
}
case "$RHIZA_GCS_PREFIX" in */"$RHIZA_RUN_ID"/) ;; *)
  echo 'GCS prefix must end in the fresh run ID' >&2
  exit 2
esac
for secret_name in "$RHIZA_RUNTIME_AGENT_CA_SECRET" "$RHIZA_RUNTIME_AGENT_TLS_SECRET"; do
  printf '%s\n' "$secret_name" | LC_ALL=C grep -Eq '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$' || {
    echo 'invalid runtime-agent Secret name' >&2
    exit 2
  }
done
for profile_value in "$RHIZA_CONTAINERD_VERSION" "$RHIZA_CONTAINERD_RUNTIME_TYPE" "$RHIZA_CGROUP_PROFILE_ID" "$RHIZA_KERNEL_RELEASE"; do
  printf '%s\n' "$profile_value" | LC_ALL=C grep -Eq '^[A-Za-z0-9._+-]+$' || {
    echo 'invalid qualified runtime profile input' >&2
    exit 2
  }
  case "$profile_value" in *QUALIFIED*|*PINNED*|*PLACEHOLDER*)
    echo 'runtime profile placeholders are not accepted' >&2
    exit 2
    ;;
  esac
done
umask 077
mkdir -p "$out"
admin_token_file=$out/admin-token
tls_tmp=
remove_admin_token=false
scope_uid_file=$out/gcs-namespace-uid
scope_nodes_file=$out/gcs-existing-nodes
cleanup_namespace=false

capture_run_scope() {
  namespace_json=$out/gcs-namespace-current.json
  kubectl --context "$RHIZA_KUBE_CONTEXT" get namespace "$RHIZA_NAMESPACE" -o json > "$namespace_json" || {
    echo 'run namespace identity could not be read' >&2
    return 1
  }
  namespace_uid=$(jq -er --arg ns "$RHIZA_NAMESPACE" --arg run "$RHIZA_RUN_ID" '
    select(.metadata.name==$ns and .metadata.labels["chaos.rhiza.io/run"]==$run) |
    .metadata.uid | select(type=="string" and length>0)
  ' "$namespace_json") || {
    echo 'namespace is not labeled as this run or has no UID' >&2
    return 1
  }
  printf '%s\n' "$namespace_uid" | LC_ALL=C grep -Eq '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$' || {
    echo 'namespace UID format rejected' >&2
    return 1
  }
  if [ "$mode" = install ]; then
    [ ! -e "$scope_uid_file" ] && [ ! -L "$scope_uid_file" ] &&
      [ ! -e "$scope_nodes_file" ] && [ ! -L "$scope_nodes_file" ] || {
        echo 'captured run-scope receipt already exists; refusing reuse' >&2
        return 1
      }
    for node in "$RHIZA_NODE_A" "$RHIZA_NODE_B" "$RHIZA_NODE_C"; do
      node_json=$out/node-$node.json
      kubectl --context "$RHIZA_KUBE_CONTEXT" get node "$node" -o json > "$node_json" || {
        echo 'a captured existing node could not be read' >&2
        return 1
      }
      jq -e --arg node "$node" '
        .metadata.name==$node and any(.status.conditions[]?; .type=="Ready" and .status=="True")
      ' "$node_json" >/dev/null || {
        echo 'a captured existing node is not Ready' >&2
        return 1
      }
    done
    printf '%s\n' "$namespace_uid" > "$scope_uid_file"
    printf '%s\n%s\n%s\n' "$RHIZA_NODE_A" "$RHIZA_NODE_B" "$RHIZA_NODE_C" > "$scope_nodes_file"
    chmod 600 "$scope_uid_file" "$scope_nodes_file"
  else
    [ -f "$scope_uid_file" ] && [ ! -L "$scope_uid_file" ] &&
      [ "$(cat "$scope_uid_file")" = "$namespace_uid" ] &&
      [ -f "$scope_nodes_file" ] && [ ! -L "$scope_nodes_file" ] || {
        echo 'run namespace or captured node identity changed since install' >&2
        return 1
      }
    expected_nodes=$(printf '%s\n%s\n%s' "$RHIZA_NODE_A" "$RHIZA_NODE_B" "$RHIZA_NODE_C")
    [ "$(cat "$scope_nodes_file")" = "$expected_nodes" ] || {
      echo 'run node selection differs from the installed namespace profile' >&2
      return 1
    }
  fi
}

cleanup_owned_namespace() {
  [ -f "$scope_uid_file" ] && [ ! -L "$scope_uid_file" ] || return 1
  expected_uid=$(cat "$scope_uid_file") || return 1
  current=$out/namespace-cleanup-current.json
  kubectl --context "$RHIZA_KUBE_CONTEXT" get namespace "$RHIZA_NAMESPACE" --ignore-not-found -o json > "$current" 2>/dev/null || return 1
  if [ ! -s "$current" ]; then
    printf '%s\n' 'namespace-absence-observed; cluster-rbac-owner-gc-not-verified' > "$out/namespace-cleanup.status"
    return 0
  fi
  jq -e --arg ns "$RHIZA_NAMESPACE" --arg run "$RHIZA_RUN_ID" --arg uid "$expected_uid" '
    .metadata.name==$ns and .metadata.uid==$uid and .metadata.labels["chaos.rhiza.io/run"]==$run
  ' "$current" >/dev/null || return 1
  options=$out/namespace-delete-options.json
  current_uid=$(jq -er '.metadata.uid' "$current") || return 1
  current_rv=$(jq -er '.metadata.resourceVersion' "$current") || return 1
  [ "$current_uid" = "$expected_uid" ] || return 1
  printf '{"apiVersion":"v1","kind":"DeleteOptions","preconditions":{"uid":"%s","resourceVersion":"%s"}}\n' \
    "$expected_uid" "$current_rv" > "$options" || return 1
  kubectl --context "$RHIZA_KUBE_CONTEXT" delete --raw "/api/v1/namespaces/$RHIZA_NAMESPACE" \
    -f "$options" >/dev/null 2>&1 || {
      jq -e '(.metadata.deletionTimestamp|type)=="string"' "$current" >/dev/null 2>&1 || return 1
    }
  attempts=0
  while [ "$attempts" -lt 120 ]; do
    observed=$out/namespace-cleanup-observed.txt
    if kubectl --context "$RHIZA_KUBE_CONTEXT" get namespace "$RHIZA_NAMESPACE" \
      --ignore-not-found -o name > "$observed" 2>/dev/null; then
      if [ ! -s "$observed" ]; then
        printf '%s\n' 'namespace-absence-observed; cluster-rbac-owner-gc-not-verified' > "$out/namespace-cleanup.status"
        return 0
      fi
    else
      return 1
    fi
    attempts=$((attempts + 1))
    sleep 1
  done
  printf '%s\n' 'delete-requested-absence-not-yet-observed' > "$out/namespace-cleanup.status"
  return 1
}

cleanup() {
  exit_status=$?
  initial_state=$out/gcs-initial-head-install-state.json
  if [ "$initial_head_ack_loss" = yes ] && { [ -e "$initial_state" ] || [ -L "$initial_state" ]; } &&
    { [ "$mode" = run ] || [ "$exit_status" -ne 0 ]; }; then
    cleanup_log=$out/initial-head-cleanup.log
    cleanup_error=
    if [ -L "$initial_state" ] || [ ! -f "$initial_state" ]; then
      cleanup_error='private restore state is not a regular file'
    elif [ -z "${FENCING_E2E_BIN:-}" ] || [ ! -x "$FENCING_E2E_BIN" ]; then
      cleanup_error='source-matched fencing-e2e cleanup binary is unavailable'
    elif { [ -e "$cleanup_log" ] || [ -L "$cleanup_log" ]; } && { [ -L "$cleanup_log" ] || [ ! -f "$cleanup_log" ]; }; then
      cleanup_error='private cleanup log path is unsafe'
    elif [ -e "$cleanup_log" ] && ! chmod 600 "$cleanup_log" 2>/dev/null; then
      cleanup_error='private cleanup log permissions could not be secured'
    else
      if [ ! -e "$cleanup_log" ]; then (umask 077; : > "$cleanup_log") || cleanup_error='private cleanup log could not be created'; fi
      if [ -z "$cleanup_error" ]; then
        if FENCING_E2E_OUT="$out" RHIZA_KUBE_CONTEXT="$RHIZA_KUBE_CONTEXT" RHIZA_NAMESPACE="$RHIZA_NAMESPACE" \
          RHIZA_RUN_ID="$RHIZA_RUN_ID" "$FENCING_E2E_BIN" cleanup-gcs-ha-initial-head-ack-loss >>"$cleanup_log" 2>&1; then
          :
        else
          cleanup_error='fencer restore was not verified; private restore state was preserved'
        fi
      fi
    fi
    if [ -n "$cleanup_error" ]; then
      echo "initial-HEAD cleanup failed: $cleanup_error" >&2
      [ "$exit_status" -ne 0 ] || exit_status=1
    fi
  fi
  if [ "$cleanup_namespace" = true ] && { [ "$mode" = run ] || [ "$exit_status" -ne 0 ]; }; then
    if cleanup_owned_namespace; then
      :
    else
      printf '%s\n' 'local-delete-unavailable-or-not-observed; run-owned expiry Job remains pending until its 7200s deadline' > "$out/namespace-cleanup.status"
      echo 'namespace cleanup was not verified locally; the in-cluster expiry Job remains pending until its deadline' >&2
      [ "$exit_status" -ne 0 ] || exit_status=1
    fi
  fi
  if [ "$mode" = run ] && [ "$async_nonempty" = yes ]; then
    artifact_error=
    if [ -z "${FENCING_E2E_BIN:-}" ] || [ ! -x "$FENCING_E2E_BIN" ]; then
      artifact_error='fencing-e2e driver is unavailable'
    elif [ -z "$source_head" ]; then
      artifact_error='source revision was not captured'
    else
      printf '%s\n' "$exit_status" > "$out/actual-exit.txt" || artifact_error='actual exit receipt could not be written'
      printf '%s\n' "$source_head" > "$out/source-head.txt" || artifact_error='source revision receipt could not be written'
      printf '%s\n%s\n' "$RHIZA_APP_IMAGE" "$RHIZA_OPERATOR_IMAGE" > "$out/image-ids.txt" || artifact_error='immutable image receipt could not be written'
      if [ -z "$artifact_error" ] && ! FENCING_E2E_OUT="$out" FENCING_E2E_QUALIFICATION=gcs-async-nonempty \
        "$FENCING_E2E_BIN" safe-artifacts; then
        artifact_error='strict async-nonempty artifact projection failed'
      fi
    fi
    if [ -n "$artifact_error" ]; then
      echo "async-nonempty artifact finalization failed: $artifact_error" >&2
      [ "$exit_status" -ne 0 ] || exit_status=1
    fi
  fi
  if [ "$mode" = run ] && [ "$manual_terminal" = yes ]; then
    artifact_error=
    if [ -z "${FENCING_E2E_BIN:-}" ] || [ ! -x "$FENCING_E2E_BIN" ]; then
      artifact_error='fencing-e2e driver is unavailable'
    elif [ -z "$source_head" ]; then
      artifact_error='source revision was not captured'
    else
      printf '%s\n' "$exit_status" >"$out/actual-exit.txt" || artifact_error='actual exit receipt could not be written'
      printf '%s\n' "$source_head" >"$out/source-head.txt" || artifact_error='source revision receipt could not be written'
      printf '%s\n%s\n' "$RHIZA_APP_IMAGE" "$RHIZA_OPERATOR_IMAGE" >"$out/image-ids.txt" || artifact_error='immutable image receipt could not be written'
      if [ -z "$artifact_error" ] && ! FENCING_E2E_OUT="$out" FENCING_E2E_QUALIFICATION=gcs-manual-terminal \
        "$FENCING_E2E_BIN" safe-artifacts; then
        artifact_error='strict manual-terminal artifact projection failed'
      fi
    fi
    if [ -n "$artifact_error" ]; then
      echo "manual-terminal artifact finalization failed: $artifact_error" >&2
      [ "$exit_status" -ne 0 ] || exit_status=1
    fi
  fi
  rm -f "$out/.admin-token.tmp"
  if [ "$remove_admin_token" = true ]; then rm -f "$admin_token_file"; fi
  if [ -n "$tls_tmp" ]; then
    rm -f "$tls_tmp/tls.key" "$tls_tmp/tls.crt" "$tls_tmp/openssl.cnf"
    rmdir "$tls_tmp" 2>/dev/null || :
  fi
  return "$exit_status"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM

# GNU timeout is also used by the repository's GCS post-release launcher. The
# trap is best-effort: SIGKILL, host loss, or an unavailable API can skip cleanup.
if [ "${RHIZA_GCS_DEADLINE_WRAPPED:-no}" != yes ]; then
  command -v timeout >/dev/null 2>&1 || { echo 'GNU timeout is required for bounded host execution' >&2; exit 2; }
  case "$mode" in install) host_limit=2100 ;; run) host_limit=3600 ;; esac
  export RHIZA_GCS_DEADLINE_WRAPPED=yes
  exec timeout --signal=TERM --kill-after=10m "${host_limit}s" /bin/sh "$0" "$mode"
fi

render() {
  sed -e "s|__NAMESPACE__|$RHIZA_NAMESPACE|g" \
      -e "s|__NAMESPACE_UID__|$namespace_uid|g" \
      -e "s|__RUN_ID__|$RHIZA_RUN_ID|g" \
      -e "s|__NODE_A__|$RHIZA_NODE_A|g" \
      -e "s|__NODE_B__|$RHIZA_NODE_B|g" \
      -e "s|__NODE_C__|$RHIZA_NODE_C|g" \
      -e "s|__CLUSTER_ID__|$RHIZA_CLUSTER_ID|g" \
      -e "s|__GCS_BUCKET__|$RHIZA_GCS_BUCKET|g" \
      -e "s|__WORKLOAD_PREFIX__|$workload_prefix|g" \
      -e "s|__AUTHORITY_PREFIX__|$authority_prefix|g" \
      -e "s|__CONTROLPLANE_GSA__|$gcs_service_account|g" \
      -e "s|__READONLY_WORKLOAD_GSA__|$readonly_workload_gsa|g" \
      -e "s|__RUNTIME_AGENT_CA_SECRET__|$RHIZA_RUNTIME_AGENT_CA_SECRET|g" \
      -e "s|__RUNTIME_AGENT_TLS_SECRET__|$RHIZA_RUNTIME_AGENT_TLS_SECRET|g" \
      -e "s|__HOST_IMAGE__|$RHIZA_APP_IMAGE|g" \
      -e "s|__OPERATOR_IMAGE__|$RHIZA_OPERATOR_IMAGE|g" \
      "$here/votercount_gcs_chaos.yaml.in" > "$out/rendered.yaml"
  if [ "$initial_head_ack_loss" = yes ] || [ "$async_nonempty" = yes ]; then
    initial_render="$out/rendered.initial-head.tmp"
    sed -e 's/RHIZA_OBJSTORE_DURABILITY: before-ack/RHIZA_OBJSTORE_DURABILITY: async/' \
      -e 's/allowDataLoss: false/allowDataLoss: true/' \
      "$out/rendered.yaml" > "$initial_render"
    [ "$(grep -c 'RHIZA_OBJSTORE_DURABILITY: async' "$initial_render")" -eq 1 ] &&
      [ "$(grep -c 'allowDataLoss: true' "$initial_render")" -eq 1 ] || {
        echo 'async GCS qualification did not render its explicit async data-loss policy' >&2
        exit 2
      }
    mv "$initial_render" "$out/rendered.yaml"
  fi
  ! grep -Eq '__[A-Z0-9_]+__' "$out/rendered.yaml"
  # The paired isolation probe Pods are created just-in-time by the driver: they
  # must not hold namespace quota while the scenarios run. Marker drift fails here.
  awk '/^# BEGIN INSPECTOR PROBE$/{probe=1;next} /^# END INSPECTOR PROBE$/{probe=0;next} !probe' \
    "$out/rendered.yaml" |
    awk '/^# BEGIN RHIZA CLUSTER$/{cluster=1;next} /^# END RHIZA CLUSTER$/{cluster=0;next} !cluster' > "$out/manifests.yaml"
  awk '/^# BEGIN RHIZA CLUSTER$/{cluster=1;next} /^# END RHIZA CLUSTER$/{cluster=0;next} cluster' \
    "$out/rendered.yaml" > "$out/rhiza-cluster.yaml"
  [ "$(grep -c '^kind: RhizaCluster$' "$out/rhiza-cluster.yaml")" -eq 1 ] &&
    ! grep -q '^kind: RhizaCluster$' "$out/manifests.yaml"
  awk '/^# BEGIN INSPECTOR PROBE$/{probe=1;next} /^# END INSPECTOR PROBE$/{probe=0;next} probe' \
    "$out/rendered.yaml" > "$out/inspector-probe.yaml"
  grep -q '^  name: rhiza-inspector$' "$out/inspector-probe.yaml" &&
    grep -q '^  name: rhiza-inspector-control$' "$out/inspector-probe.yaml" &&
    ! grep -q '^  name: rhiza-inspector' "$out/manifests.yaml"
}

render_iam_probe() {
  pod=$1; ksa=$2; expected_gsa=$3; probe_mode=$4
  sed -e "s|__PROBE_POD__|$pod|g" \
      -e "s|__PROBE_KSA__|$ksa|g" \
      -e "s|__EXPECTED_GSA__|$expected_gsa|g" \
      -e "s|__PROBE_MODE__|$probe_mode|g" \
      -e "s|__NAMESPACE__|$RHIZA_NAMESPACE|g" \
      -e "s|__RUN_ID__|$RHIZA_RUN_ID|g" \
      -e "s|__NODE_A__|$RHIZA_NODE_A|g" \
      -e "s|__NODE_B__|$RHIZA_NODE_B|g" \
      -e "s|__NODE_C__|$RHIZA_NODE_C|g" \
      -e "s|__APP_IMAGE__|$RHIZA_APP_IMAGE|g" \
      -e "s|__GCS_BUCKET__|$RHIZA_GCS_BUCKET|g" \
      -e "s|__GCS_PREFIX__|$RHIZA_GCS_PREFIX|g" \
      -e "s|__PROBE_NONCE__|$probe_nonce|g" \
      -e "s|__MANIFEST_GENERATION__|$probe_manifest_generation|g" \
      -e "s|__CREATE_GENERATION__|$probe_create_generation|g" \
      -e "s|__UPDATE_GENERATION__|$probe_update_generation|g" \
      "$here/gcs-iam-preflight.yaml.in" > "$out/$pod.yaml"
  ! grep -Eq '__[A-Z0-9_]+__' "$out/$pod.yaml"
}

render_expiry_job() {
  expiry_now=$(date +%s) || return 1
  printf '%s\n' "$expiry_now" | LC_ALL=C grep -Eq '^[0-9]+$' || {
    echo 'expiry epoch could not be validated' >&2
    return 1
  }
  expiry_deadline=$((expiry_now + 7200))
  sed -e "s|__NAMESPACE__|$RHIZA_NAMESPACE|g" \
      -e "s|__NAMESPACE_UID__|$namespace_uid|g" \
      -e "s|__RUN_ID__|$RHIZA_RUN_ID|g" \
      -e "s|__EXPIRY_DEADLINE__|$expiry_deadline|g" \
      -e "s|__NODE_A__|$RHIZA_NODE_A|g" \
      -e "s|__NODE_B__|$RHIZA_NODE_B|g" \
      -e "s|__NODE_C__|$RHIZA_NODE_C|g" \
      -e "s|__KUBECTL_IMAGE__|$RHIZA_KUBECTL_IMAGE|g" \
      "$here/gcs-namespace-expiry.yaml.in" > "$out/namespace-expiry.yaml"
  ! grep -Eq '__[A-Z0-9_]+__' "$out/namespace-expiry.yaml"
}

require_fresh_expiry_resources() {
  for object in "clusterrole/$RHIZA_NAMESPACE-namespace-expiry" \
    "clusterrolebinding/$RHIZA_NAMESPACE-namespace-expiry" \
    "serviceaccount/rhiza-namespace-expiry" "job/rhiza-namespace-expiry"; do
    resource=${object%%/*}
    name=${object#*/}
    if [ "$resource" = clusterrole ] || [ "$resource" = clusterrolebinding ]; then
      existing=$(kubectl --context "$RHIZA_KUBE_CONTEXT" get "$resource" "$name" --ignore-not-found -o name) || {
        echo 'could not verify expiry resource absence' >&2
        return 1
      }
    else
      existing=$(kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" get "$resource" "$name" --ignore-not-found -o name) || {
        echo 'could not verify expiry resource absence' >&2
        return 1
      }
    fi
    [ -z "$existing" ] || {
      echo 'expiry resource already exists; refusing adoption' >&2
      return 1
    }
  done
}

verified_probe_pod_uid() {
  object=$1; pod=$2
  jq -er --arg pod "$pod" --arg ns "$RHIZA_NAMESPACE" --arg run "$RHIZA_RUN_ID" \
    --arg node_a "$RHIZA_NODE_A" --arg node_b "$RHIZA_NODE_B" --arg node_c "$RHIZA_NODE_C" \
    --arg ksa "$probe_ksa" --arg image "$RHIZA_APP_IMAGE" --arg mode "$probe_mode" \
    --arg bucket "$RHIZA_GCS_BUCKET" --arg prefix "$RHIZA_GCS_PREFIX" \
    --arg nonce "$probe_nonce" --arg manifest "$probe_manifest_generation" \
    --arg create "$probe_create_generation" --arg update "$probe_update_generation" \
    --arg gsa "$probe_expected_gsa" '
    select(.metadata.name==$pod and .metadata.namespace==$ns and
      .metadata.labels["chaos.rhiza.io/run"]==$run and
      .metadata.annotations["chaos.rhiza.io/owner"]=="gcs-iam-preflight" and
      ((.metadata.ownerReferences // [])|length)==0 and
      .spec.serviceAccountName==$ksa and .spec.automountServiceAccountToken==false and
      .spec.restartPolicy=="Never" and
      ((.spec.hostNetwork // false)==false) and ((.spec.hostPID // false)==false) and
      ((.spec.hostIPC // false)==false) and ((.spec.shareProcessNamespace // false)==false) and
      .spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms==[
        {matchFields:[{key:"metadata.name",operator:"In",values:[$node_a,$node_b,$node_c]}]}] and
      ((.spec.ephemeralContainers // [])|length)==0 and
      .spec.securityContext.runAsNonRoot==true and .spec.securityContext.runAsUser==65532 and
      .spec.securityContext.seccompProfile.type=="RuntimeDefault" and
      ((.spec.volumes // [])|length)==0 and ((.spec.initContainers // [])|length)==0 and
      (.spec.containers|length)==1 and
      .spec.containers[0].name=="probe" and .spec.containers[0].image==$image and
      .spec.containers[0].command==["/usr/local/bin/rhiza-entrypoint","qualification-gcs-iam-probe",$mode] and
      .spec.containers[0].securityContext.runAsNonRoot==true and
      .spec.containers[0].securityContext.readOnlyRootFilesystem==true and
      .spec.containers[0].securityContext.allowPrivilegeEscalation==false and
      ((.spec.containers[0].securityContext.privileged // false)==false) and
      ((.spec.containers[0].securityContext.capabilities.add // [])|length)==0 and
      ((.spec.containers[0].securityContext.capabilities.drop // [])|index("ALL"))!=null and
      ([.spec.containers[0].env[]|{key:.name,value:.value}]|from_entries)=={
        RHIZA_GCS_BUCKET:$bucket,RHIZA_GCS_PREFIX:$prefix,RHIZA_RUN_ID:$run,
        RHIZA_GCS_IAM_PROBE_NONCE:$nonce,
        RHIZA_GCS_IAM_PROBE_MANIFEST_GENERATION:$manifest,
        RHIZA_GCS_IAM_PROBE_CREATE_GENERATION:$create,
        RHIZA_GCS_IAM_PROBE_UPDATE_GENERATION:$update,
        RHIZA_GCS_IAM_PROBE_EXPECTED_GSA:$gsa
      }) | .metadata.uid | select(type=="string" and length>0)
  ' "$object"
}

probe_pod_uid() {
  pod=$1
  object="$out/$pod.live.json"
  kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" get pod "$pod" --ignore-not-found -o json > "$object" 2>/dev/null || return 1
  [ -s "$object" ] || return 2
  verified_probe_pod_uid "$object" "$pod"
}

record_gcs_cleanup_evidence() {
  log=$1
  pending_file=$(mktemp "$out/.gcs-cleanup-pending-$probe_nonce.XXXXXXXX") || {
    echo 'could not create private cleanup evidence scratch file' >&2
    return 1
  }
  sed -nE \
    -e 's/^gcs-iam-probe=cleanup-pending:(read|update|delete|control|manifest|create):([1-9][0-9]*)$/pending \1 \2/p' \
    -e 's/^gcs-iam-probe=create-outcome-unknown:(read|update|delete|control|manifest|create)$/unknown \1 unknown/p' \
    "$log" > "$pending_file" || {
      rm -f "$pending_file"
      echo 'could not parse bounded cleanup status output' >&2
      return 1
    }
  if [ -s "$pending_file" ]; then
    cleanup_evidence=$(mktemp "$out/gcs-iam-cleanup-pending-$probe_nonce.XXXXXXXX") || {
      rm -f "$pending_file"
      echo 'could not create private cleanup evidence artifact' >&2
      return 1
    }
  else
    rm -f "$pending_file"
    return 0
  fi
  while IFS=' ' read -r kind label generation; do
    [ -n "$kind" ] || continue
    case "$label" in
      control) key=${RHIZA_GCS_PREFIX}control/iam-preflight/$RHIZA_RUN_ID/$probe_nonce/read ;;
      manifest) key=${RHIZA_GCS_PREFIX}data/iam-preflight/$RHIZA_RUN_ID/$probe_nonce/manifest.json ;;
      *) key=${RHIZA_GCS_PREFIX}data/iam-preflight/$RHIZA_RUN_ID/$probe_nonce/$label ;;
    esac
    printf '%s/%s\t%s\n' "$RHIZA_GCS_BUCKET" "$key" "$generation" >> "$cleanup_evidence" || {
      rm -f "$pending_file"
      echo 'could not record exact GCS cleanup evidence' >&2
      return 1
    }
    if [ "$kind" = pending ]; then
      case "$label" in
        create) probe_create_generation=$generation ;;
        update) [ "$probe_update_generation" != 0 ] || probe_update_generation=$generation ;;
      esac
    fi
  done < "$pending_file"
  rm -f "$pending_file"
  chmod 600 "$cleanup_evidence"
  echo "exact GCS cleanup evidence remains in $cleanup_evidence; manual review is required" >&2
}

record_pending_probe_pod() {
  pod=$1; object=$2; reason=$3
  uid=$(verified_probe_pod_uid "$object" "$pod" 2>/dev/null) || {
    echo "preflight Pod outcome is uncertain; no verified UID is available ($reason)" >&2
    return 1
  }
  evidence=$(mktemp "$out/gcs-iam-pod-cleanup-pending-$probe_nonce.XXXXXXXX") || return 1
  printf '%s\t%s\t%s\t%s\n' "$RHIZA_NAMESPACE" "$pod" "$uid" "$reason" > "$evidence" || return 1
  chmod 600 "$evidence" || return 1
  echo "verified preflight Pod remains; manual UID-scoped cleanup required (namespace/name/UID in $evidence)" >&2
}

record_known_probe_pod_uid() {
  pod=$1; uid=$2; reason=$3
  evidence=$(mktemp "$out/gcs-iam-pod-cleanup-pending-$probe_nonce.XXXXXXXX") || return 1
  printf '%s\t%s\t%s\t%s\n' "$RHIZA_NAMESPACE" "$pod" "$uid" "$reason" > "$evidence" || return 1
  chmod 600 "$evidence" || return 1
  echo "preflight Pod UID $uid requires manual identity-checked cleanup review ($evidence)" >&2
}

delete_probe_pod_uid() {
  pod=$1; expected_uid=$2
  uid=$(probe_pod_uid "$pod") || { echo 'preflight Pod identity unavailable for exact cleanup' >&2; return 1; }
  [ "$uid" = "$expected_uid" ] || { echo 'preflight Pod was replaced; refusing cleanup of new UID' >&2; return 1; }
  options="$out/$pod.delete-options.json"
  printf '{"apiVersion":"v1","kind":"DeleteOptions","preconditions":{"uid":"%s"}}\n' "$expected_uid" > "$options"
  kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" delete --raw \
    "/api/v1/namespaces/$RHIZA_NAMESPACE/pods/$pod" -f "$options" >/dev/null 2>&1 || return 1
  kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" wait --for=delete "pod/$pod" --timeout=60s >/dev/null 2>&1 || return 1
  if uid=$(probe_pod_uid "$pod"); then
    echo "preflight Pod name was recreated after UID-scoped deletion (observed UID $uid)" >&2
    return 1
  else
    status=$?
    [ "$status" -eq 2 ] || return 1
  fi
}

run_iam_probe() {
  pod=$1; ksa=$2; expected_gsa=$3; probe_mode=$4; expected_line=$5
  log="$out/$pod.log"
  probe_ksa=$ksa
  probe_expected_gsa=$expected_gsa
  probe_mode=$probe_mode
  render_iam_probe "$pod" "$ksa" "$expected_gsa" "$probe_mode" || return 1
  existing="$out/$pod.preexisting.json"
  kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" get pod "$pod" --ignore-not-found -o json > "$existing" 2>/dev/null || {
    echo 'preflight Pod absence could not be verified' >&2
    return 1
  }
  [ ! -s "$existing" ] || { echo 'refusing to adopt a pre-existing preflight Pod name' >&2; return 1; }
  created="$out/$pod.created.json"
  kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" create -f "$out/$pod.yaml" -o json > "$created" 2>/dev/null || {
    if live_uid=$(probe_pod_uid "$pod"); then
      record_pending_probe_pod "$pod" "$out/$pod.live.json" 'create response lost' || :
    else
      status=$?
      if [ "$status" -ne 2 ]; then
        echo 'create-only preflight Pod failed and live identity could not be determined; inspect private run artifacts' >&2
      else
        echo 'create-only preflight Pod creation failed with no Pod present' >&2
      fi
    fi
    return 1
  }
  uid=$(verified_probe_pod_uid "$created" "$pod") || {
    if live_uid=$(probe_pod_uid "$pod"); then
      record_pending_probe_pod "$pod" "$out/$pod.live.json" 'create response identity mismatch' || :
    else
      echo "created preflight Pod spec was not the expected run-owned identity; inspect private artifact $created" >&2
    fi
    return 1
  }
  live_uid=$(probe_pod_uid "$pod") || {
    echo "created preflight Pod UID $uid could not be reverified; inspect $out/$pod.live.json for manual cleanup" >&2
    record_known_probe_pod_uid "$pod" "$uid" 'live-identity-unavailable' || :
    return 1
  }
  [ "$live_uid" = "$uid" ] || {
    echo "preflight Pod changed after creation; refusing to wait on a different UID (created $uid, observed $live_uid)" >&2
    record_known_probe_pod_uid "$pod" "$uid" 'uid-changed-after-create' || :
    record_pending_probe_pod "$pod" "$out/$pod.live.json" 'UID changed after create' || :
    return 1
  }
  status=0
  kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" wait \
    --for=jsonpath='{.status.phase}'=Succeeded "pod/$pod" --timeout=120s >/dev/null 2>&1 || status=$?
  kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" logs "$pod" > "$log" 2>/dev/null || status=1
  after_uid=$(probe_pod_uid "$pod") || status=1
  [ "$uid" = "$after_uid" ] || status=1
  actual_line=$(cat "$log" 2>/dev/null || :)
  matches=false
  if [ "$status" -eq 0 ]; then
    if [ "$probe_mode" = writer-prepare ]; then
      case "$actual_line" in gcs-iam-probe=prepared:[1-9][0-9]*)
        probe_manifest_generation=${actual_line#gcs-iam-probe=prepared:}
        printf '%s\n' "$probe_manifest_generation" | LC_ALL=C grep -Eq '^[1-9][0-9]*$' && matches=true
        ;;
      esac
    elif [ "$actual_line" = "$expected_line" ]; then
      matches=true
    fi
  fi
  if [ "$matches" = true ] && [ "$(probe_pod_uid "$pod" 2>/dev/null || :)" = "$uid" ]; then
    if cleanup_probe_pod "$pod" "$uid"; then
      printf '%s\n' "$actual_line"
      return 0
    fi
    record_known_probe_pod_uid "$pod" "$uid" 'success-path-cleanup-failed' || :
    echo "preflight succeeded but Pod UID $uid cleanup is unresolved; refusing to report a completed probe" >&2
    return 1
  fi
  record_gcs_cleanup_evidence "$log"
  if ! cleanup_probe_pod "$pod" "$uid"; then
    record_pending_probe_pod "$pod" "$out/$pod.live.json" 'UID-scoped cleanup failed' || :
    echo "preflight failure cleanup is incomplete for Pod UID $uid; inspect $out/$pod.live.json and remove only that UID after verification" >&2
  fi
  echo "GCS IAM preflight phase failed: $probe_mode (sanitized output preserved at $log)" >&2
  return 1
}

cleanup_probe_pod() {
  pod=$1; expected_uid=$2
  if live_uid=$(probe_pod_uid "$pod" 2>/dev/null); then
    :
  else
    status=$?
    if [ "$status" -eq 2 ]; then return 0; fi
    echo 'preflight Pod cleanup could not verify live identity' >&2
    return 1
  fi
  if [ "$live_uid" != "$expected_uid" ]; then
    echo "preflight Pod name was replaced; refusing deletion (observed UID $live_uid)" >&2
    return 1
  fi
  if ! delete_probe_pod_uid "$pod" "$expected_uid"; then
    still_uid=$(probe_pod_uid "$pod" 2>/dev/null) || {
      status=$?
      if [ "$status" -eq 2 ]; then return 0; fi
      echo "preflight Pod UID $expected_uid cleanup could not be verified; manual UID-scoped cleanup required" >&2
      return 1
    }
    [ "$still_uid" != "$expected_uid" ] || {
      echo "preflight Pod UID $expected_uid remains; manual UID-scoped cleanup required" >&2
      return 1
    }
    echo "preflight Pod name was replaced after cleanup; refusing to claim cleanup complete (observed UID $still_uid)" >&2
    return 1
  fi
}

render_runtime_agent() {
  deployment_uid=$1
  root=$(CDPATH='' cd -- "$here/../.." && pwd)
  # Reuse the shipped node-qualified authority manifest, starting at its
  # opt-in runtime profile section so the run-owned fencer is not replaced.
  sed -n '/^# This profile is deliberately opt-in:/,$p' \
    "$root/deploy/operator/fencing/authority.yaml" |
    sed -e "s|namespace: rhiza$|namespace: $RHIZA_NAMESPACE|g" \
      -e "s|rhiza-runtime-agent-tokenreview|$RHIZA_NAMESPACE-runtime-agent-tokenreview|g" \
      -e "s|rhiza-runtime-agent-authority-identity|$RHIZA_NAMESPACE-runtime-agent-authority-identity|g" \
      -e "s|rhiza-operator:latest|$RHIZA_OPERATOR_IMAGE|g" \
      -e "s|QUALIFIED_CONTAINERD_VERSION|$RHIZA_CONTAINERD_VERSION|g" \
      -e "s|QUALIFIED_CONTAINERD_RUNTIME_TYPE|$RHIZA_CONTAINERD_RUNTIME_TYPE|g" \
      -e "s|QUALIFIED_PRIVATE_CGROUP_V2_PROFILE_ID|$RHIZA_CGROUP_PROFILE_ID|g" \
      -e "s|QUALIFIED_LINUX_KERNEL_RELEASE|$RHIZA_KERNEL_RELEASE|g" \
      -e "s|PINNED_RHIZA_FENCER_DEPLOYMENT_UID|$deployment_uid|g" \
      -e "s|QUALIFIED_SANDBOX_EXECUTABLE_SHA256|$RHIZA_QUALIFIED_SANDBOX_EXECUTABLE_SHA256|g" \
      -e "s|rhiza-runtime-agent-ca|$RHIZA_RUNTIME_AGENT_CA_SECRET|g" \
      -e "s|rhiza-runtime-agent-tls|$RHIZA_RUNTIME_AGENT_TLS_SECRET|g" \
      > "$out/runtime-agent.yaml"
  runtime_agent_pinned=$out/runtime-agent-pinned.yaml
  awk -v a="$RHIZA_NODE_A" -v b="$RHIZA_NODE_B" -v c="$RHIZA_NODE_C" '
    $0 == "      serviceAccountName: rhiza-runtime-agent" {
      print
      print "      affinity:"
      print "        nodeAffinity:"
      print "          requiredDuringSchedulingIgnoredDuringExecution:"
      print "            nodeSelectorTerms:"
      print "              - matchFields:"
      print "                  - key: metadata.name"
      print "                    operator: In"
      print "                    values: [" a ", " b ", " c "]"
      inserted++
      next
    }
    { print }
    END { if (inserted != 1) exit 1 }
  ' "$out/runtime-agent.yaml" > "$runtime_agent_pinned" || {
    echo 'runtime-agent node-affinity insertion was not unique' >&2
    return 1
  }
  runtime_agent_owned=$out/runtime-agent-owned.yaml
  awk -v ns="$RHIZA_NAMESPACE" -v uid="$namespace_uid" '
    /^kind:/ { cluster=($0=="kind: ClusterRole" || $0=="kind: ClusterRoleBinding"); print; next }
    cluster && $0=="metadata:" {
      print
      print "  ownerReferences:"
      print "    - apiVersion: v1"
      print "      kind: Namespace"
      print "      name: " ns
      print "      uid: " uid
      print "      blockOwnerDeletion: false"
      owners++
      cluster=0
      next
    }
    { print }
    END { if (owners != 2) exit 1 }
  ' "$runtime_agent_pinned" > "$runtime_agent_owned" || {
    echo 'runtime-agent cluster-object ownership binding was not exact' >&2
    return 1
  }
  mv "$runtime_agent_owned" "$out/runtime-agent.yaml"
  grep -q '^kind: DaemonSet$' "$out/runtime-agent.yaml" &&
    grep -q "^  authority-deployment-uid: $deployment_uid$" "$out/runtime-agent.yaml" &&
    grep -Fqx "  sandbox-executable-sha256: \"$RHIZA_QUALIFIED_SANDBOX_EXECUTABLE_SHA256\"" "$out/runtime-agent.yaml" &&
    grep -Fqx "        rhiza.dev/sandbox-executable-sha256: \"$RHIZA_QUALIFIED_SANDBOX_EXECUTABLE_SHA256\"" "$out/runtime-agent.yaml" &&
    grep -Fqx "  sandbox-args-json: '[\"/pause\"]'" "$out/runtime-agent.yaml" &&
    ! grep -Eq 'QUALIFIED_|PINNED_|PLACEHOLDER' "$out/runtime-agent.yaml" &&
    grep -Fq "values: [$RHIZA_NODE_A, $RHIZA_NODE_B, $RHIZA_NODE_C]" "$out/runtime-agent.yaml" &&
    grep -Fq "uid: $namespace_uid" "$out/runtime-agent.yaml"
}

render_passive_kernel_patch() {
  root=$(CDPATH='' cd -- "$here/../.." && pwd)
  sed "s/namespace: rhiza$/namespace: $RHIZA_NAMESPACE/" \
    "$root/deploy/operator/fencing/runtime-agent-passive-kernel-patch.yaml" \
    > "$out/runtime-agent-passive-kernel-patch.yaml"
  grep -Fqx '              add: [SYS_PTRACE]' "$out/runtime-agent-passive-kernel-patch.yaml" &&
    grep -Fqx '              drop: [ALL]' "$out/runtime-agent-passive-kernel-patch.yaml" &&
    grep -Fqx '        rhiza.mrchypark.dev/runtime-proof-profile: passive-kernel-v1' "$out/runtime-agent-passive-kernel-patch.yaml" &&
    ! grep -Eq 'SYS_ADMIN|privileged: true|CAP_SYS_ADMIN' "$out/runtime-agent-passive-kernel-patch.yaml"
}

require_fresh_runtime_agent_daemonset() {
  existing_runtime_agent=$(kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" \
    get daemonset rhiza-runtime-agent --ignore-not-found -o name) || {
      echo 'could not verify runtime-agent DaemonSet absence; refusing to reuse or overwrite it' >&2
      return 1
    }
  [ -z "$existing_runtime_agent" ] || {
    echo 'runtime-agent DaemonSet already exists; refusing to reuse or overwrite it in this qualification namespace' >&2
    return 1
  }
}

provision_tls() {
  command -v openssl >/dev/null || { echo 'Missing openssl' >&2; exit 2; }
  tls_tmp=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-fencer-tls.XXXXXXXX")
  printf '%s\n' '[req]' 'distinguished_name=dn' 'x509_extensions=ext' 'prompt=no' \
    '[dn]' 'CN=rhiza-fencer' \
    '[ext]' 'basicConstraints=critical,CA:TRUE' \
    'keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign' \
    'extendedKeyUsage=serverAuth' \
    "subjectAltName=DNS:rhiza-fencer,DNS:rhiza-fencer.$RHIZA_NAMESPACE.svc" \
    >"$tls_tmp/openssl.cnf"
  openssl req -x509 -newkey rsa:2048 -nodes -days 1 -config "$tls_tmp/openssl.cnf" \
    -keyout "$tls_tmp/tls.key" -out "$tls_tmp/tls.crt" 2>"$out/00-tls-generation.log"
  step 00-fencer-tls kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" \
    create secret tls rhiza-fencer-tls --cert="$tls_tmp/tls.crt" --key="$tls_tmp/tls.key"
  step 00-fencer-ca kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" \
    create configmap rhiza-fencer-ca --from-file="ca.crt=$tls_tmp/tls.crt"
}

# Run a step, stream its log and fail the launcher on its real exit status.
step() {
  name=$1; shift
  log="$out/$name.log"
  status=0
  "$@" > "$log" 2>&1 || status=$?
  cat "$log"
  if [ "$status" -ne 0 ]; then
    echo "step $name failed with status $status" >&2
    exit "$status"
  fi
}

if [ "$mode" = install ]; then
  verify_bootstrap_receipt || exit 2
  capture_run_scope || exit 2
  cleanup_namespace=true
  probe_nonce=$(openssl rand -hex 16 2>/dev/null) || {
    echo 'cryptographic preflight nonce generation failed' >&2
    exit 2
  }
  printf '%s\n' "$probe_nonce" | LC_ALL=C grep -Eq '^[a-f0-9]{32}$' || {
    echo 'cryptographic preflight nonce rejected' >&2
    exit 2
  }
  printf '%s\n' "$probe_nonce" > "$out/gcs-iam-probe-nonce"
  probe_manifest_generation=0
  probe_create_generation=0
  probe_update_generation=0
  kubectl --context "$RHIZA_KUBE_CONTEXT" get namespace "$RHIZA_NAMESPACE" >/dev/null
  secret_has_key() {
    secret=$1 key=$2
    value=$(kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" get secret "$secret" \
      -o "go-template={{if index .data \"$key\"}}ok{{end}}") || return 1
    [ "$value" = ok ]
  }
  secret_has_key "$RHIZA_RUNTIME_AGENT_CA_SECRET" ca.crt || {
    echo 'required runtime-agent CA Secret/key is missing' >&2
    exit 2
  }
  secret_has_key "$RHIZA_RUNTIME_AGENT_TLS_SECRET" tls.crt || {
    echo 'required runtime-agent TLS certificate Secret/key is missing' >&2
    exit 2
  }
  secret_has_key "$RHIZA_RUNTIME_AGENT_TLS_SECRET" tls.key || {
    echo 'required runtime-agent TLS private-key Secret/key is missing' >&2
    exit 2
  }
  render
  render_expiry_job
  require_fresh_expiry_resources || exit 2
  root=$(CDPATH='' cd -- "$here/../.." && pwd)
  step 00-crds kubectl --context "$RHIZA_KUBE_CONTEXT" apply \
    -f "$root/deploy/operator/crd.yaml" \
    -f "$root/deploy/operator/cluster-crd.yaml" \
    -f "$root/deploy/operator/fence-crd.yaml"
  step 00-crds-ready kubectl --context "$RHIZA_KUBE_CONTEXT" wait --for=condition=Established \
    crd/rhizarecoveries.rhiza.mrchypark.dev \
    crd/rhizaclusters.rhiza.mrchypark.dev \
    crd/rhizafences.rhiza.mrchypark.dev --timeout=120s
  provision_tls
  step 10-expiry kubectl --context "$RHIZA_KUBE_CONTEXT" apply -f "$out/namespace-expiry.yaml"
  step 10-expiry-ready kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" wait \
    --for=condition=Ready pod -l chaos.rhiza.io/owner=namespace-expiry --timeout=180s
  expiry_pods=$out/namespace-expiry-pods.json
  kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" get pods \
    -l chaos.rhiza.io/owner=namespace-expiry -o json > "$expiry_pods"
  jq -e --arg ns "$RHIZA_NAMESPACE" --arg run "$RHIZA_RUN_ID" \
    --arg a "$RHIZA_NODE_A" --arg b "$RHIZA_NODE_B" --arg c "$RHIZA_NODE_C" \
    --arg image "$RHIZA_KUBECTL_IMAGE" '
      (.items|length)==1 and .items[0].metadata.namespace==$ns and
      .items[0].metadata.labels["chaos.rhiza.io/run"]==$run and
      (.items[0].spec.nodeName as $node | ([$a,$b,$c]|index($node))!=null) and
      .items[0].spec.containers[0].image==$image and
      .items[0].status.phase=="Running"
    ' "$expiry_pods" >/dev/null || {
    echo 'namespace expiry Pod did not start on a captured node with the pinned kubectl image' >&2
    exit 1
  }
  expiry_pod_name=$(jq -er '.items[0].metadata.name' "$expiry_pods") || exit 1
  kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" exec "$expiry_pod_name" -- \
    /bin/sh -c 'command -v kubectl >/dev/null && command -v date >/dev/null && command -v sleep >/dev/null' || {
      echo 'namespace expiry image must provide kubectl, date, sleep, and /bin/sh' >&2
      exit 1
    }
  expiry_job_json=$out/namespace-expiry-live.json
  kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" get job rhiza-namespace-expiry -o json > "$expiry_job_json"
  jq -e --arg ns "$RHIZA_NAMESPACE" --arg run "$RHIZA_RUN_ID" '
    .metadata.name=="rhiza-namespace-expiry" and .metadata.namespace==$ns and
    .metadata.labels["chaos.rhiza.io/run"]==$run and
    .spec.activeDeadlineSeconds==10800 and .spec.template.spec.restartPolicy=="Never" and
    .spec.template.spec.serviceAccountName=="rhiza-namespace-expiry"
  ' "$expiry_job_json" >/dev/null || {
    echo 'run-owned namespace expiry Job identity or bounded deadline rejected' >&2
    exit 1
  }
  printf '%s\n' 'created; namespace UID and run label checked before delete; deadline 7200s' > "$out/namespace-cleanup.status"
  chmod 600 "$out/namespace-cleanup.status"
  step 10-apply kubectl --context "$RHIZA_KUBE_CONTEXT" apply -f "$out/manifests.yaml"
  step 20-fencer kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" rollout status \
    deployment/rhiza-fencer --timeout=180s
  fencer_deployment_uid=$(kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" \
    get deployment rhiza-fencer -o jsonpath='{.metadata.uid}') || {
      echo 'fencer Deployment UID unavailable for runtime-agent pinning' >&2
      exit 1
    }
  printf '%s\n' "$fencer_deployment_uid" | LC_ALL=C grep -Eq '^[a-z0-9-]+$' || {
    echo 'fencer Deployment UID rejected' >&2
    exit 1
  }
  render_runtime_agent "$fencer_deployment_uid"
  require_fresh_runtime_agent_daemonset
  step 20-runtime-agent-apply kubectl --context "$RHIZA_KUBE_CONTEXT" apply -f "$out/runtime-agent.yaml"
  if [ "$RHIZA_RUNTIME_AGENT_PASSIVE_KERNEL" = yes ]; then
    render_passive_kernel_patch
    step 20-runtime-agent-passive-kernel kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" \
      patch daemonset rhiza-runtime-agent --type=strategic --patch-file "$out/runtime-agent-passive-kernel-patch.yaml"
  fi
  step 20-runtime-agent kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" rollout status \
    daemonset/rhiza-runtime-agent --timeout=180s
  step 20-operator kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" rollout status \
    deployment/rhiza-operator --timeout=180s

  # Validate the actual workload ADC identity and IAM boundary before creating
  # the RhizaCluster. The StatefulSet remains at zero replicas until this passes.
  preflight_status=0
  run_iam_probe "rhiza-gcs-writer-prepare-$RHIZA_RUN_ID" rhiza-operator "$gcs_service_account" writer-prepare gcs-iam-probe=prepared || preflight_status=1
  if [ "$preflight_status" -eq 0 ]; then
    run_iam_probe "rhiza-gcs-workload-reader-$RHIZA_RUN_ID" rhiza-workload "$readonly_workload_gsa" reader gcs-iam-probe=reader_verified || preflight_status=1
  fi
  run_iam_probe "rhiza-gcs-writer-cleanup-$RHIZA_RUN_ID" rhiza-operator "$gcs_service_account" writer-cleanup gcs-iam-probe=cleaned || preflight_status=1
  if [ "$preflight_status" -ne 0 ]; then
    echo 'independent GCS IAM preflight failed; RhizaCluster was not installed' >&2
    exit 1
  fi
  if [ "$initial_head_ack_loss" = yes ]; then
    : "${FENCING_E2E_BIN:?initial HEAD fault requires the source-matched fencing-e2e driver}"
    [ -x "$FENCING_E2E_BIN" ] || { echo 'FENCING_E2E_BIN is not executable' >&2; exit 2; }
    FENCING_E2E_OUT=$out RHIZA_KUBE_CONTEXT=$RHIZA_KUBE_CONTEXT RHIZA_NAMESPACE=$RHIZA_NAMESPACE \
      RHIZA_RUN_ID=$RHIZA_RUN_ID RHIZA_GCS_BUCKET=$RHIZA_GCS_BUCKET RHIZA_GCS_PREFIX=$RHIZA_GCS_PREFIX \
      RHIZA_CLUSTER_ID=$RHIZA_CLUSTER_ID RHIZA_GCS_INITIAL_HEAD_ACK_LOSS=yes \
      "$FENCING_E2E_BIN" prepare-gcs-ha-initial-head-ack-loss
  fi
  step 25-apply-rhiza-cluster kubectl --context "$RHIZA_KUBE_CONTEXT" apply -f "$out/rhiza-cluster.yaml"
  printf '%s %s\n' "$RHIZA_RUN_ID" "$receipt_sha" > "$out/gcs-iam-preflight.ok"
  chmod 600 "$out/gcs-iam-preflight.ok"

  secret_name=
  attempts=0
  while [ "$attempts" -lt 120 ]; do
    secret_name=$(kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" get statefulset rhiza-voter \
      -o go-template='{{range .spec.template.spec.containers}}{{range .env}}{{if eq .name "RHIZA_ADMIN_TOKEN"}}{{.valueFrom.secretKeyRef.name}}{{end}}{{end}}{{end}}' \
      2>/dev/null) || secret_name=
    [ -n "$secret_name" ] && break
    attempts=$((attempts + 1))
    sleep 1
  done
  [ -n "$secret_name" ] || { echo 'operator did not install the bootstrap admin Secret' >&2; exit 1; }
  kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" get secret "$secret_name" \
    -o go-template='{{index .data "admin" | base64decode}}' >"$out/.admin-token.tmp"
  [ "$(wc -c <"$out/.admin-token.tmp" | tr -d ' ')" -ge 32 ] || { echo 'bootstrap admin credential is invalid' >&2; exit 1; }
  mv "$out/.admin-token.tmp" "$admin_token_file"
  chmod 600 "$admin_token_file"
  echo "privileged voterCount installation completed; run '$0 run' with the scoped runtime kubeconfig"
  exit 0
fi

[ -f "$admin_token_file" ] && [ ! -L "$admin_token_file" ] && [ -s "$admin_token_file" ] || {
  echo "missing privileged-install credential: $admin_token_file" >&2
  exit 2
}
verify_bootstrap_receipt || exit 2
capture_run_scope || exit 2
[ -f "$out/gcs-iam-preflight.ok" ] && [ ! -L "$out/gcs-iam-preflight.ok" ] &&
  [ "$(cat "$out/gcs-iam-preflight.ok")" = "$RHIZA_RUN_ID $receipt_sha" ] || {
    echo 'matching successful independent GCS IAM preflight receipt is required' >&2
    exit 2
  }
remove_admin_token=true
cleanup_namespace=true
if [ "$initial_head_ack_loss" = yes ]; then
  : "${FENCING_E2E_BIN:?initial HEAD qualification requires the source-matched fencing-e2e driver}"
  [ -x "$FENCING_E2E_BIN" ] || { echo 'FENCING_E2E_BIN is not executable' >&2; exit 2; }
  step 30-initial-head-collector env FENCING_E2E_OUT="$out" RHIZA_KUBE_CONTEXT="$RHIZA_KUBE_CONTEXT" \
    RHIZA_NAMESPACE="$RHIZA_NAMESPACE" RHIZA_RUN_ID="$RHIZA_RUN_ID" RHIZA_GCS_BUCKET="$RHIZA_GCS_BUCKET" \
    RHIZA_GCS_PREFIX="$RHIZA_GCS_PREFIX" RHIZA_CLUSTER_ID="$RHIZA_CLUSTER_ID" \
    RHIZA_GCS_INITIAL_HEAD_ACK_LOSS=yes "$FENCING_E2E_BIN" run-gcs-ha-initial-head-ack-loss
  echo 'initial canonical HEAD GCS/Chaos qualification collector completed'
  exit 0
fi
if [ "$async_nonempty" = yes ]; then
  : "${FENCING_E2E_BIN:?async-nonempty qualification requires the fencing-e2e driver}"
  [ -x "$FENCING_E2E_BIN" ] || { echo 'FENCING_E2E_BIN is not executable' >&2; exit 2; }
  step 30-async-nonempty-collector env FENCING_E2E_OUT="$out" RHIZA_KUBE_CONTEXT="$RHIZA_KUBE_CONTEXT" \
    RHIZA_NAMESPACE="$RHIZA_NAMESPACE" RHIZA_RUN_ID="$RHIZA_RUN_ID" RHIZA_GCS_BUCKET="$RHIZA_GCS_BUCKET" \
    RHIZA_GCS_PREFIX="$RHIZA_GCS_PREFIX" RHIZA_CLUSTER_ID="$RHIZA_CLUSTER_ID" \
    "$FENCING_E2E_BIN" run-gcs-ha-async-nonempty
  echo 'nonempty async GCS quorum-loss and exact archived-tip recovery collector completed'
  exit 0
fi
if [ "$manual_terminal" = yes ]; then
  : "${FENCING_E2E_BIN:?manual-terminal qualification requires the fencing-e2e driver}"
  [ -x "$FENCING_E2E_BIN" ] || { echo 'FENCING_E2E_BIN is not executable' >&2; exit 2; }
  step 30-manual-terminal-collector env FENCING_E2E_OUT="$out" RHIZA_KUBE_CONTEXT="$RHIZA_KUBE_CONTEXT" \
    RHIZA_NAMESPACE="$RHIZA_NAMESPACE" RHIZA_RUN_ID="$RHIZA_RUN_ID" RHIZA_GCS_BUCKET="$RHIZA_GCS_BUCKET" \
    RHIZA_GCS_PREFIX="$RHIZA_GCS_PREFIX" RHIZA_CLUSTER_ID="$RHIZA_CLUSTER_ID" RHIZA_GCS_MANUAL_TERMINAL=yes \
    "$FENCING_E2E_BIN" run-gcs-ha-manual-terminal
  echo 'manual-terminal GCS challenge, independent approval, recovery, and readback collector completed'
  exit 0
fi
step 30-driver python3 -u "$here/votercount_gcs_chaos.py" \
  --context "$RHIZA_KUBE_CONTEXT" \
  --namespace "$RHIZA_NAMESPACE" \
  --run-id "$RHIZA_RUN_ID" \
  --app-image "$RHIZA_APP_IMAGE" \
  --operator-image "$RHIZA_OPERATOR_IMAGE" \
  --admin-token-file "$admin_token_file" \
  --scenarios "${RHIZA_SCENARIOS:-positive}" \
  --inspector-probe "$out/inspector-probe.yaml" \
  --output "$out"
echo "voterCount GCS/Chaos qualification launcher completed"
