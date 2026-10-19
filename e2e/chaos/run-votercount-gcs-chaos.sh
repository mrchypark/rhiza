#!/bin/sh
# Launcher for the voterCount GCS/Chaos qualification (see README.md).
#
# Run e2e/chaos/bootstrap-gcs-postrelease.sh first: it owns the namespace, the
# rhiza-gcs workload-identity KSA, the quota and the admission policies this run
# set relies on. This script renders and applies the voterCount manifests and
# runs the driver with digest-pinned images. Every step is checked: a failing
# admission proof, scenario assertion or cleanup makes this launcher exit non-zero.
#
# Required (images must be digest-pinned): RHIZA_RUN_ID, RHIZA_NAMESPACE,
# RHIZA_KUBE_CONTEXT, RHIZA_GCS_BUCKET, RHIZA_GCS_PREFIX, RHIZA_CLUSTER_ID,
# RHIZA_APP_IMAGE, RHIZA_OPERATOR_IMAGE.
set -eu

: "${RHIZA_RUN_ID:?8 lowercase hex run id}"
: "${RHIZA_NAMESPACE:?run namespace containing the run id}"
: "${RHIZA_KUBE_CONTEXT:?kube context}"
: "${RHIZA_GCS_BUCKET:?bucket}"
: "${RHIZA_GCS_PREFIX:?prefix ending in /<run id>/}"
: "${RHIZA_CLUSTER_ID:?cluster id}"
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
mkdir -p "$out"

kubectl --context "$RHIZA_KUBE_CONTEXT" get namespace "$RHIZA_NAMESPACE" >/dev/null

render() {
  sed -e "s|__NAMESPACE__|$RHIZA_NAMESPACE|g" \
      -e "s|__RUN_ID__|$RHIZA_RUN_ID|g" \
      -e "s|__CLUSTER_ID__|$RHIZA_CLUSTER_ID|g" \
      -e "s|__GCS_BUCKET__|$RHIZA_GCS_BUCKET|g" \
      -e "s|__GCS_PREFIX__|$RHIZA_GCS_PREFIX|g" \
      -e "s|__HOST_IMAGE__|$RHIZA_APP_IMAGE|g" \
      -e "s|__OPERATOR_IMAGE__|$RHIZA_OPERATOR_IMAGE|g" \
      "$here/votercount_gcs_chaos.yaml.in" > "$out/manifests.yaml"
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

render
step 10-apply kubectl --context "$RHIZA_KUBE_CONTEXT" apply -f "$out/manifests.yaml"
# rhiza-fencer-tls and rhiza-fencer-ca are supplied out of band; the authority
# and the supervised voters mount them.
step 20-fencer kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" rollout status \
  deployment/rhiza-fencer --timeout=180s
step 30-driver python3 -u "$here/votercount_gcs_chaos.py" \
  --context "$RHIZA_KUBE_CONTEXT" \
  --namespace "$RHIZA_NAMESPACE" \
  --run-id "$RHIZA_RUN_ID" \
  --app-image "$RHIZA_APP_IMAGE" \
  --operator-image "$RHIZA_OPERATOR_IMAGE" \
  --scenarios "${RHIZA_SCENARIOS:-positive}" \
  --output "$out"
echo "voterCount GCS/Chaos qualification launcher completed"
