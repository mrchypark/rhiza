#!/bin/sh
# Launcher for the voterCount GCS/Chaos qualification.
#
# Run e2e/chaos/bootstrap-gcs-postrelease.sh first: it owns the namespace, the
# rhiza-gcs workload-identity KSA, the quota and the admission policies that this
# run set relies on. This script renders and applies the voterCount manifests,
# runs the driver with the pinned digests, and leaves cleanup to the driver,
# which deletes the Chaos resources and the run-owned cluster-scoped objects and
# verifies their absence.
#
# Required: RHIZA_RUN_ID, RHIZA_NAMESPACE, RHIZA_KUBE_CONTEXT, RHIZA_GCS_BUCKET,
# RHIZA_GCS_PREFIX, RHIZA_CLUSTER_ID, RHIZA_APP_IMAGE, RHIZA_OPERATOR_IMAGE.
set -eu

: "${RHIZA_RUN_ID:?8 lowercase hex run id}"
: "${RHIZA_NAMESPACE:?run namespace containing the run id}"
: "${RHIZA_KUBE_CONTEXT:?kube context}"
: "${RHIZA_GCS_BUCKET:?bucket}"
: "${RHIZA_GCS_PREFIX:?prefix ending in /<run id>/}"
: "${RHIZA_CLUSTER_ID:?cluster id}"
: "${RHIZA_APP_IMAGE:?digest-pinned application image}"
: "${RHIZA_OPERATOR_IMAGE:?digest-pinned operator image}"

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

render
kubectl --context "$RHIZA_KUBE_CONTEXT" apply -f "$out/manifests.yaml" | tee "$out/apply.log"

# rhiza-fencer-tls and rhiza-fencer-ca are supplied out of band before this run;
# the authority and the supervised voters mount them.
kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" rollout status \
  deployment/rhiza-fencer --timeout=180s | tee "$out/fencer.log"

python3 -u "$here/votercount_gcs_chaos.py" \
  --context "$RHIZA_KUBE_CONTEXT" \
  --namespace "$RHIZA_NAMESPACE" \
  --run-id "$RHIZA_RUN_ID" \
  --app-image "$RHIZA_APP_IMAGE" \
  --operator-image "$RHIZA_OPERATOR_IMAGE" \
  --output "$out" 2>&1 | tee "$out/driver.log"
