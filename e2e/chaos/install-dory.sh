#!/usr/bin/env bash
set -euo pipefail

controller_image="$(dory k8s get deployment/chaos-controller-manager -n chaos-mesh -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)"
daemon_image="$(dory k8s get daemonset/chaos-daemon -n chaos-mesh -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)"
if [[ "$controller_image" != "ghcr.io/chaos-mesh/chaos-mesh:v2.8.3" || "$daemon_image" != "ghcr.io/chaos-mesh/chaos-daemon:v2.8.3" ]]; then
  helm repo add chaos-mesh https://charts.chaos-mesh.org --force-update
  dory k8s create namespace chaos-mesh --dry-run=client -o yaml | dory k8s apply -f -
  helm template chaos-mesh chaos-mesh/chaos-mesh --version 2.8.3 \
    --namespace chaos-mesh --include-crds \
    --set chaosDaemon.runtime=containerd \
    --set chaosDaemon.socketPath=/run/k3s/containerd/containerd.sock \
    --set controllerManager.replicaCount=1 \
    --set controllerManager.leaderElection.enabled=false \
    --set dashboard.create=false \
    --set dnsServer.create=false |
    dory k8s apply --server-side --force-conflicts -f -
fi
dory pull registry.k8s.io/pause:3.10 >/dev/null
dory tag registry.k8s.io/pause:3.10 gcr.io/google-containers/pause:latest
dory save gcr.io/google-containers/pause:latest | dory exec -i dory-k8s ctr -n k8s.io images import - >/dev/null
dory k8s rollout status deployment/chaos-controller-manager -n chaos-mesh --timeout=180s
dory k8s rollout status daemonset/chaos-daemon -n chaos-mesh --timeout=180s
