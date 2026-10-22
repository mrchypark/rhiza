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
: "${RHIZA_CONTAINERD_VERSION:?qualified containerd version for every eligible node}"
: "${RHIZA_CONTAINERD_RUNTIME_TYPE:?qualified containerd runtime type}"
: "${RHIZA_CGROUP_PROFILE_ID:?qualified cgroup profile identifier}"
: "${RHIZA_KERNEL_RELEASE:?qualified kernel release for every eligible node}"
: "${RHIZA_APP_IMAGE:?digest-pinned application image}"
: "${RHIZA_OPERATOR_IMAGE:?digest-pinned operator image}"

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

here=$(cd -- "$(dirname -- "$0")" && pwd)
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
    --arg reader "$readonly_workload_gsa" '
      type=="object" and .run==$run and .namespace==$ns and .parentPrefix==$prefix and
      .controlPrefix==($prefix+"control") and
      .workload.ksa.name=="rhiza-workload" and
      .workload.gsa.email==$reader and .workload.gsa.role=="roles/storage.objectViewer" and
      .workload.gsa.managedFolder==($prefix+"data/") and
      .writer.gsa.email==$writer and .writer.gsa.managedFolder==$prefix and
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
cleanup() {
  rm -f "$out/.admin-token.tmp"
  if [ "$remove_admin_token" = true ]; then rm -f "$admin_token_file"; fi
  if [ -n "$tls_tmp" ]; then
    rm -f "$tls_tmp/tls.key" "$tls_tmp/tls.crt" "$tls_tmp/openssl.cnf"
    rmdir "$tls_tmp" 2>/dev/null || :
  fi
}
trap cleanup EXIT

render() {
  sed -e "s|__NAMESPACE__|$RHIZA_NAMESPACE|g" \
      -e "s|__RUN_ID__|$RHIZA_RUN_ID|g" \
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

verified_probe_pod_uid() {
  object=$1; pod=$2
  jq -er --arg pod "$pod" --arg ns "$RHIZA_NAMESPACE" --arg run "$RHIZA_RUN_ID" \
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
      -e "s|rhiza-runtime-agent-ca|$RHIZA_RUNTIME_AGENT_CA_SECRET|g" \
      -e "s|rhiza-runtime-agent-tls|$RHIZA_RUNTIME_AGENT_TLS_SECRET|g" \
      > "$out/runtime-agent.yaml"
  grep -q '^kind: DaemonSet$' "$out/runtime-agent.yaml" &&
    grep -q "^  authority-deployment-uid: $deployment_uid$" "$out/runtime-agent.yaml" &&
    ! grep -Eq 'QUALIFIED_|PINNED_|PLACEHOLDER' "$out/runtime-agent.yaml"
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
  step 20-runtime-agent-apply kubectl --context "$RHIZA_KUBE_CONTEXT" apply -f "$out/runtime-agent.yaml"
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
[ -f "$out/gcs-iam-preflight.ok" ] && [ ! -L "$out/gcs-iam-preflight.ok" ] &&
  [ "$(cat "$out/gcs-iam-preflight.ok")" = "$RHIZA_RUN_ID $receipt_sha" ] || {
    echo 'matching successful independent GCS IAM preflight receipt is required' >&2
    exit 2
  }
remove_admin_token=true
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
