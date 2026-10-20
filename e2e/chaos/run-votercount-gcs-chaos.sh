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

die() { printf '%s\n' "$*" >&2; exit 2; }

here=$(cd -- "$(dirname -- "$0")" && pwd)

# The release source of truth is the Rust SDK manifest version. The launcher
# identity family (namespace, data GSA, GCS prefix) is derived from it so a
# release bump cannot leave a stale family behind. Same capture idiom and same
# naming rules as run-gcs-postrelease.sh and bootstrap-gcs-postrelease.sh.
repo_root=$(CDPATH='' cd -- "$here/../.." && pwd)
# BEGIN VERSION PREFIX DERIVATION
manifest_version=$(sed -n 's/^version = "\(.*\)"/\1/p' "$repo_root/sdk/rust/Cargo.toml" | head -n 1)
[ -n "$manifest_version" ] || die 'Cannot read the SDK version from sdk/rust/Cargo.toml'
version_prefix=v$(printf '%s' "$manifest_version" | tr -d '.')
printf '%s\n' "$version_prefix" | LC_ALL=C grep -Eq '^v[0-9]+$' || die 'Malformed SDK version prefix'
printf '%s\n' "$manifest_version" | LC_ALL=C grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || die 'Malformed SDK version'
# END VERSION PREFIX DERIVATION

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

# Fencer TLS certificate lifetime.
#
# The install phase and the run phase are separate invocations, so the
# self-signed fencer certificate must not expire because of the gap between
# them. A one-day certificate made every namespace unusable one day after it
# was installed (the voter then crashloops on an x509 expiry error), which is
# a structural defect: the lifetime was tied to the install-to-run interval
# instead of to the qualification run. The default is 30 days, and the value
# is validated so a typo cannot silently produce a short-lived certificate.
: "${RHIZA_FENCE_CERT_DAYS:=30}"
case "$RHIZA_FENCE_CERT_DAYS" in
  ''|*[!0-9]*) die "RHIZA_FENCE_CERT_DAYS must be a positive integer, got: $RHIZA_FENCE_CERT_DAYS" ;;
esac
[ "$RHIZA_FENCE_CERT_DAYS" -ge 1 ] ||
  die "RHIZA_FENCE_CERT_DAYS must be at least 1 day, got: $RHIZA_FENCE_CERT_DAYS"
[ "$RHIZA_FENCE_CERT_DAYS" -le 3650 ] ||
  die "RHIZA_FENCE_CERT_DAYS must be at most 3650 days, got: $RHIZA_FENCE_CERT_DAYS"

# Exact identity-family gate. Containment of the run ID is not enough: a stale
# release family (for example rhiza-v0191-<runid>) would still satisfy a
# containment check and could reach Kubernetes or GCS mutation before being
# rejected. The namespace, the data GSA and the GCS prefix must be exactly the
# family derived from the current SDK version. Same rule as
# run-gcs-postrelease.sh and bootstrap-gcs-postrelease.sh.
#
# This gate runs before any mutating or remote action: before the output
# directory is created, before any kubectl/gcloud/gh call, and before any
# manifest is rendered or applied. It applies to both the install and the run
# mode, so neither entry point can be pointed at a stale release family.
validate_identity_family() {
  expected_namespace="rhiza-$version_prefix-$RHIZA_RUN_ID"
  expected_data_gsa="rhiza-$version_prefix-gcs-$RHIZA_RUN_ID"
  expected_prefix="postrelease/v$manifest_version/$RHIZA_RUN_ID/"
  [ "$RHIZA_NAMESPACE" = "$expected_namespace" ] ||
    die "Namespace identity mismatch: expected $expected_namespace, got $RHIZA_NAMESPACE"
  [ "$RHIZA_DATA_GSA_ID" = "$expected_data_gsa" ] ||
    die "Data GSA identity mismatch: expected $expected_data_gsa, got $RHIZA_DATA_GSA_ID"
  [ "$RHIZA_GCS_PREFIX" = "$expected_prefix" ] ||
    die "GCS prefix identity mismatch: expected $expected_prefix, got $RHIZA_GCS_PREFIX"
}

out=${RHIZA_OUT:-$PWD/votercount-gcs-chaos-results}
gcs_service_account=$RHIZA_DATA_GSA_ID@$RHIZA_GCP_PROJECT_ID.iam.gserviceaccount.com
admin_token_file=$out/admin-token

# The runtime phase first checks its own local prerequisite: the privileged
# install credential produced by the install phase. That check is purely local
# (a file test) and performs no remote call, so it cannot leak a stale family
# to Kubernetes, GCS or GitHub. The identity gate below still runs before the
# driver reaches the cluster.
if [ "$mode" = run ]; then
  [ -f "$admin_token_file" ] && [ ! -L "$admin_token_file" ] && [ -s "$admin_token_file" ] || {
    echo "missing privileged-install credential: $admin_token_file" >&2
    exit 2
  }
fi

validate_identity_family

umask 077
mkdir -p "$out"
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
  # The certificate lifetime must not depend on the interval between the
  # install phase and the run phase. A one-day certificate made the fencer
  # and every voter fail with an x509 expiry error once the namespace had been
  # installed for more than a day. The lifetime is now a validated parameter
  # (default 30 days), and the issued window is recorded so the receipt shows
  # how long the qualification is allowed to run.
  openssl req -x509 -newkey rsa:2048 -nodes -days "$RHIZA_FENCE_CERT_DAYS" -config "$tls_tmp/openssl.cnf" \
    -keyout "$tls_tmp/tls.key" -out "$tls_tmp/tls.crt" 2>"$out/00-tls-generation.log"
  # Verify the issued window instead of trusting the requested day count.
  openssl x509 -in "$tls_tmp/tls.crt" -noout -dates >"$out/00-tls-dates.log" 2>&1 ||
    { echo 'cannot read the issued fencer certificate window' >&2; exit 1; }
  openssl x509 -in "$tls_tmp/tls.crt" -noout -checkend 0 >/dev/null 2>&1 ||
    { echo 'the issued fencer certificate is already expired' >&2; exit 1; }
  # Assert the issued window really covers the requested lifetime. `-checkend 0`
  # only proves the certificate is valid *now*; a short-lived certificate would
  # pass it while still reproducing the install-to-run expiry defect. One day
  # of slack absorbs the clock skew between issuance and this check.
  minimum_remaining=$(( (RHIZA_FENCE_CERT_DAYS - 1) * 86400 ))
  openssl x509 -in "$tls_tmp/tls.crt" -noout -checkend "$minimum_remaining" >/dev/null 2>&1 ||
    { echo "the issued fencer certificate does not cover the requested ${RHIZA_FENCE_CERT_DAYS} day window" >&2; exit 1; }
  cat "$out/00-tls-dates.log"
  printf 'fencer TLS certificate issued with %s day(s) validity; install-to-run delay must stay below that window\n' \
    "$RHIZA_FENCE_CERT_DAYS"
  # Reissue path: an already installed namespace still holds the previous
  # Secret/ConfigMap. The install phase owns both objects, so replacing them
  # is safe and lets an expired certificate be renewed without recreating the
  # namespace. Deleting first keeps `kubectl create` idempotent for a re-run.
  kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" \
    delete secret rhiza-fencer-tls --ignore-not-found=true >"$out/00-fencer-tls-reissue.log" 2>&1
  kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" \
    delete configmap rhiza-fencer-ca --ignore-not-found=true >>"$out/00-fencer-tls-reissue.log" 2>&1
  step 00-fencer-tls kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" \
    create secret tls rhiza-fencer-tls --cert="$tls_tmp/tls.crt" --key="$tls_tmp/tls.key"
  step 00-fencer-ca kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" \
    create configmap rhiza-fencer-ca --from-file="ca.crt=$tls_tmp/tls.crt"
  printf '%s\n' "fencer TLS reissue complete: rhiza-fencer-tls Secret and rhiza-fencer-ca ConfigMap replaced with a ${RHIZA_FENCE_CERT_DAYS} day certificate" \
    >>"$out/00-fencer-tls-reissue.log"
  # A running fencer keeps the certificate it loaded at start-up in memory, so
  # replacing the Secret alone would leave the old (possibly expired) material
  # in use. Restart the Deployment when it already exists; on a fresh install
  # it is created later by the manifest apply and picks the new Secret up.
  if kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" \
    get deployment rhiza-fencer >/dev/null 2>&1; then
    step 00-fencer-restart kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" \
      rollout restart deployment/rhiza-fencer
    step 00-fencer-restart-status kubectl --context "$RHIZA_KUBE_CONTEXT" -n "$RHIZA_NAMESPACE" \
      rollout status deployment/rhiza-fencer --timeout=180s
    printf '%s\n' 'fencer Deployment restarted so the reissued certificate is loaded' \
      >>"$out/00-fencer-tls-reissue.log"
  else
    printf '%s\n' 'fencer Deployment not present yet; the manifest apply will start it with the new certificate' \
      >>"$out/00-fencer-tls-reissue.log"
  fi
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
