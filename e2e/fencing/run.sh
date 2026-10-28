#!/bin/sh
# Disposable Kind only. Images and the Go driver are built by GitHub Actions.
set -eu

test -n "${FENCING_E2E_BIN:-}" || { echo 'FENCING_E2E_BIN is required' >&2; exit 2; }
test -x "$FENCING_E2E_BIN"
fencing_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$fencing_root"
fencing_cluster=rhiza-generic-fencing
fencing_context=kind-rhiza-generic-fencing
fencing_namespace=rhiza-fencing-e2e
fencing_chaos_name=rhiza-initial-replacement
FENCING_E2E_OUT=${FENCING_E2E_OUT:-generic-fencing-results}
export FENCING_E2E_OUT
mkdir -p "$FENCING_E2E_OUT"
git rev-parse HEAD >"$FENCING_E2E_OUT/source-head.txt"
docker image inspect rhiza-fencing-host:local rhiza-fencing-operator:local rhiza-anchor-service:local --format '{{.Id}}' >"$FENCING_E2E_OUT/image-ids.txt"
if kind get clusters | grep -Fx "$fencing_cluster" >/dev/null; then
  echo 'Refusing to replace an existing qualification cluster' >&2
  exit 2
fi
fencing_tmp=$(mktemp -d)
fencing_owned=false
fencing_chaos_installed=false
k() { kubectl --context "$fencing_context" -n "$fencing_namespace" "$@"; }
cleanup() {
  fencing_exit=$?
  fencing_primary_exit=$fencing_exit
  trap - EXIT INT TERM
  if [ "$fencing_owned" = true ]; then
    if ! printf '%s\n' "$fencing_primary_exit" >"$FENCING_E2E_OUT/pre-cleanup-exit.txt"; then
      echo 'Pre-cleanup exit receipt could not be written' >&2
      if [ "$fencing_exit" -eq 0 ]; then fencing_exit=1; fi
    fi
    kind export logs "$FENCING_E2E_OUT/kind" --name "$fencing_cluster" >"$FENCING_E2E_OUT/export.log" 2>&1 || true
    k logs deployment/rhiza-fencer >"$FENCING_E2E_OUT/authority.log" 2>&1 || true
    k logs deployment/rhiza-operator >"$FENCING_E2E_OUT/operator.log" 2>&1 || true
    k get pods,rhizafences,rhizarecoveries,rhizaclusters,configmaps -o json >"$FENCING_E2E_OUT/terminal.json" 2>"$FENCING_E2E_OUT/terminal.err" || true
    k get podchaos "$fencing_chaos_name" -o yaml >"$FENCING_E2E_OUT/podchaos-before-cleanup.yaml" 2>"$FENCING_E2E_OUT/podchaos-before-cleanup.err" || true
    if ! "$FENCING_E2E_BIN" capture-runtime-diagnostics >/dev/null 2>&1; then
      echo 'Pre-cleanup source Pod diagnostics could not be captured' >&2
      if [ "$fencing_exit" -eq 0 ]; then fencing_exit=1; fi
    fi
    if [ "$fencing_chaos_installed" = true ]; then
      if ! k delete podchaos rhiza-initial-replacement --ignore-not-found=true --wait=true --timeout=30s >"$FENCING_E2E_OUT/podchaos-cleanup.log" 2>&1; then fencing_exit=1; fi
      if ! k get podchaos "$fencing_chaos_name" --ignore-not-found=true -o name >"$FENCING_E2E_OUT/podchaos-after-cleanup.txt" 2>"$FENCING_E2E_OUT/podchaos-after-cleanup.err"; then
        echo 'Run-owned PodChaos absence could not be verified' >&2
        fencing_exit=1
      elif test -s "$FENCING_E2E_OUT/podchaos-after-cleanup.txt"; then
        echo 'Run-owned PodChaos remains after cleanup' >&2
        fencing_exit=1
      fi
    fi
    if ! kind delete cluster --name "$fencing_cluster" >"$FENCING_E2E_OUT/cleanup.log" 2>&1; then fencing_exit=1; fi
    if kind get clusters | grep -Fx "$fencing_cluster" >/dev/null; then
      echo 'Owned qualification cluster remains' >&2
      fencing_exit=1
    fi
  fi
  rm -f "$fencing_tmp/tls.key" "$fencing_tmp/tls.crt" "$fencing_tmp/openssl.cnf" "$fencing_tmp/anchor-tls.key" "$fencing_tmp/anchor-tls.crt" "$fencing_tmp/anchor-openssl.cnf" "$fencing_tmp/authority.yaml" "$fencing_tmp/supervised.patch.yaml" "$fencing_tmp/sa.yaml" "$fencing_tmp/rbac.yaml" "$fencing_tmp/manifests.json"
  rmdir "$fencing_tmp" || fencing_exit=1
  if ! printf '%s\n' "$fencing_exit" >"$FENCING_E2E_OUT/actual-exit.txt"; then
    echo 'Final exit receipt could not be written' >&2
    if [ "$fencing_exit" -eq 0 ]; then fencing_exit=1; fi
  fi
  exit "$fencing_exit"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
fencing_owned=true
kind create cluster --name "$fencing_cluster" --wait 120s
kind load docker-image rhiza-fencing-host:local rhiza-fencing-operator:local rhiza-anchor-service:local --name "$fencing_cluster"
kubectl --context "$fencing_context" create namespace "$fencing_namespace"
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm repo update chaos-mesh
helm upgrade --install --kube-context "$fencing_context" chaos-mesh chaos-mesh/chaos-mesh --namespace chaos-mesh --create-namespace --version 2.8.3 \
  --set chaosDaemon.runtime=containerd \
  --set chaosDaemon.socketPath=/run/containerd/containerd.sock \
  --set chaosDaemon.privileged=true \
  --set dashboard.create=false
fencing_chaos_installed=true
kubectl --context "$fencing_context" wait --for=condition=Established crd/podchaos.chaos-mesh.org --timeout=180s
kubectl --context "$fencing_context" -n chaos-mesh rollout status deployment/chaos-controller-manager --timeout=180s
kubectl --context "$fencing_context" -n chaos-mesh rollout status daemonset/chaos-daemon --timeout=180s
for fencing_crd in crd cluster-crd fence-crd; do k apply -f "deploy/operator/$fencing_crd.yaml"; done
k wait --for=condition=Established crd/rhizarecoveries.rhiza.mrchypark.dev crd/rhizaclusters.rhiza.mrchypark.dev crd/rhizafences.rhiza.mrchypark.dev --timeout=60s
sed "s/namespace: rhiza$/namespace: $fencing_namespace/" deploy/operator/rbac.yaml >"$fencing_tmp/rbac.yaml"
k apply -f "$fencing_tmp/rbac.yaml"
printf '%s\n' '[req]' 'distinguished_name=dn' 'x509_extensions=ext' 'prompt=no' '[dn]' 'CN=rhiza-fencer' '[ext]' 'basicConstraints=critical,CA:TRUE' 'keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign' 'extendedKeyUsage=serverAuth' "subjectAltName=DNS:rhiza-fencer,DNS:rhiza-fencer.$fencing_namespace.svc" >"$fencing_tmp/openssl.cnf"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -config "$fencing_tmp/openssl.cnf" -keyout "$fencing_tmp/tls.key" -out "$fencing_tmp/tls.crt" 2>"$FENCING_E2E_OUT/tls-generation.log"
k create secret tls rhiza-fencer-tls --cert="$fencing_tmp/tls.crt" --key="$fencing_tmp/tls.key"
k create configmap rhiza-fencer-ca --from-file="ca.crt=$fencing_tmp/tls.crt"
printf '%s\n' '[req]' 'distinguished_name=dn' 'req_extensions=ext' 'prompt=no' '[dn]' 'CN=anchor-service' '[ext]' 'basicConstraints=critical,CA:FALSE' 'keyUsage=critical,digitalSignature,keyEncipherment' 'extendedKeyUsage=serverAuth' "subjectAltName=DNS:anchor-service.$fencing_namespace.svc" >"$fencing_tmp/anchor-openssl.cnf"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -config "$fencing_tmp/anchor-openssl.cnf" -extensions ext -keyout "$fencing_tmp/anchor-tls.key" -out "$fencing_tmp/anchor-tls.crt" 2>"$FENCING_E2E_OUT/anchor-tls-generation.log"
k create secret tls anchor-tls --cert="$fencing_tmp/anchor-tls.crt" --key="$fencing_tmp/anchor-tls.key"
k create secret generic rhiza-recovery-anchor-token --from-literal=token=anchor-test-token
k create configmap rhiza-recovery-anchor-ca --from-file="ca.crt=$fencing_tmp/anchor-tls.crt"
"$FENCING_E2E_BIN" manifests >"$fencing_tmp/manifests.json"
k apply -f "$fencing_tmp/manifests.json"
sed -e "s/namespace: rhiza$/namespace: $fencing_namespace/" -e 's/image: rhiza-operator:latest$/image: rhiza-fencing-operator:local/' deploy/operator/fencing/authority.yaml >"$fencing_tmp/authority.yaml"
k apply -f "$fencing_tmp/authority.yaml"
# The second YAML document is the application's unprivileged ServiceAccount.
sed -n '/^---$/,$p' deploy/operator/fencing/supervised-statefulset.patch.yaml | sed "s/namespace: rhiza$/namespace: $fencing_namespace/" >"$fencing_tmp/sa.yaml"
k apply -f "$fencing_tmp/sa.yaml"
sed '/^---$/,$d' deploy/operator/fencing/supervised-statefulset.patch.yaml | sed "s/namespace: rhiza$/namespace: $fencing_namespace/" >"$fencing_tmp/supervised.patch.yaml"
k patch statefulset rhiza --type=strategic --patch-file "$fencing_tmp/supervised.patch.yaml"
k rollout status deployment/anchor-service --timeout=180s
k rollout status deployment/rhiza-minio --timeout=180s
k patch job rhiza-create-bucket --type=merge -p '{"spec":{"suspend":false}}'
k wait --for=condition=Complete job/rhiza-create-bucket --timeout=180s
k rollout status deployment/rhiza-fencer --timeout=180s
k scale deployment rhiza-operator --replicas=1
k rollout status deployment/rhiza-operator --timeout=120s
# The real automatic controller reserves fresh bootstrap, writes immutable
# membership/admin/peer credentials, and starts the source voters. Do not seed
# members or scale the StatefulSet from this driver.
"$FENCING_E2E_BIN" wait-source
"$FENCING_E2E_BIN" run
