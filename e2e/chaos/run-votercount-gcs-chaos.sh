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
# RHIZA_GCP_PROJECT_ID, RHIZA_DATA_GSA_ID, RHIZA_APP_IMAGE, RHIZA_OPERATOR_IMAGE.
set -eu

mode=${1:-install}
case "$mode" in install|run) ;; *) echo "usage: $0 install|run" >&2; exit 2 ;; esac

: "${RHIZA_RUN_ID:?8 lowercase hex run id}"
: "${RHIZA_NAMESPACE:?run namespace containing the run id}"
: "${RHIZA_KUBE_CONTEXT:?kube context}"
: "${RHIZA_GCS_BUCKET:?bucket}"
: "${RHIZA_GCS_PREFIX:?prefix ending in /<run id>/}"
: "${RHIZA_CLUSTER_ID:?cluster id}"
: "${RHIZA_GCP_PROJECT_ID:?GCP project id}"
: "${RHIZA_DATA_GSA_ID:?data GSA account id}"
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
      -e "s|__GCS_PREFIX__|$RHIZA_GCS_PREFIX|g" \
      -e "s|__POD_GSA__|$gcs_service_account|g" \
      -e "s|__HOST_IMAGE__|$RHIZA_APP_IMAGE|g" \
      -e "s|__OPERATOR_IMAGE__|$RHIZA_OPERATOR_IMAGE|g" \
      "$here/votercount_gcs_chaos.yaml.in" > "$out/rendered.yaml"
  # The paired isolation probe Pods are created just-in-time by the driver: they
  # must not hold namespace quota while the scenarios run. Marker drift fails here.
  awk '/^# BEGIN INSPECTOR PROBE$/{probe=1;next} /^# END INSPECTOR PROBE$/{probe=0;next} !probe' \
    "$out/rendered.yaml" > "$out/manifests.yaml"
  awk '/^# BEGIN INSPECTOR PROBE$/{probe=1;next} /^# END INSPECTOR PROBE$/{probe=0;next} probe' \
    "$out/rendered.yaml" > "$out/inspector-probe.yaml"
  grep -q '^  name: rhiza-inspector$' "$out/inspector-probe.yaml" &&
    grep -q '^  name: rhiza-inspector-control$' "$out/inspector-probe.yaml" &&
    ! grep -q '^  name: rhiza-inspector' "$out/manifests.yaml"
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
  kubectl --context "$RHIZA_KUBE_CONTEXT" get namespace "$RHIZA_NAMESPACE" >/dev/null
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
  step 20-operator kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" rollout status \
    deployment/rhiza-operator --timeout=180s

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
