#!/bin/sh
# Functional NoPVC test: every kubectl command runs inside local dory-k8s.
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
run_dir=$(mktemp -d /tmp/rhiza-embedded-k8s.XXXXXX)
namespace="rhiza-embedded-$(date +%s)-$$"
image="rhiza-embedded-test:$namespace"
namespace_created=0
image_built=0
image_imported=0
k() { docker exec -i dory-k8s kubectl "$@"; }
cleanup() {
 trap - EXIT HUP INT TERM
 cleanup_failed=0
 if [ "$namespace_created" -eq 1 ]; then
  k -n "$namespace" get pods,deployments -o yaml >"$run_dir/final-state.yaml" 2>&1 || :
  k -n "$namespace" get events --sort-by=.metadata.creationTimestamp >"$run_dir/events.txt" 2>&1 || :
  k -n "$namespace" logs -l app=floci --all-containers --prefix >"$run_dir/floci.log" 2>&1 || :
  k -n "$namespace" logs -l app=embedded --all-containers --prefix >"$run_dir/final-pod.log" 2>&1 || :
  if ! k delete namespace "$namespace" --wait=true --timeout=120s >"$run_dir/namespace-cleanup.log" 2>&1; then cleanup_failed=1; fi
  if k get namespace "$namespace" >/dev/null 2>&1; then echo "Owned namespace remains: $namespace" >&2; cleanup_failed=1; fi
 fi
 if [ "$image_imported" -eq 1 ]; then
  if ! docker exec dory-k8s ctr -n k8s.io images rm "docker.io/library/$image" >"$run_dir/containerd-cleanup.log" 2>&1; then cleanup_failed=1; fi
  docker exec dory-k8s ctr -n k8s.io images ls -q >"$run_dir/containerd-after-cleanup.txt" 2>&1 || cleanup_failed=1
  if rg -F -x "docker.io/library/$image" "$run_dir/containerd-after-cleanup.txt" >/dev/null; then echo "Owned containerd image remains: $image" >&2; cleanup_failed=1; fi
 fi
 if [ "$image_built" -eq 1 ]; then
  if ! docker image rm "$image" >"$run_dir/docker-image-cleanup.log" 2>&1; then cleanup_failed=1; fi
  if docker image inspect "$image" >/dev/null 2>&1; then echo "Owned Docker image remains: $image" >&2; cleanup_failed=1; fi
 fi
 rm -f "$run_dir/embedded.test" "$run_dir/Dockerfile"
 echo "Kubernetes results and diagnostics: $run_dir" >&2
 if [ "$cleanup_failed" -ne 0 ]; then exit 1; fi
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
# Never inspect or use the host's ambient (GKE) context.
k get nodes >"$run_dir/nodes.txt"
case "$(docker exec dory-k8s uname -m)" in aarch64|arm64) arch=arm64 ;; x86_64) arch=amd64 ;; *) echo "Unsupported local node architecture" >&2; exit 1 ;; esac
CGO_ENABLED=0 GOOS=linux GOARCH="$arch" TMPDIR=/tmp go test -c -o "$run_dir/embedded.test" .
cat >"$run_dir/Dockerfile" <<'DOCKERFILE'
FROM mirror.gcr.io/library/alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a
COPY embedded.test /embedded.test
ENTRYPOINT ["/embedded.test"]
DOCKERFILE
docker build --platform "linux/$arch" -t "$image" "$run_dir"
image_built=1
docker image inspect "$image" --format '{{.Id}}' >"$run_dir/test-image-id.txt"
docker save "$image" | docker exec -i dory-k8s ctr -n k8s.io images import -
image_imported=1
k create namespace "$namespace"
namespace_created=1
cat <<YAML | k -n "$namespace" apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: floci
spec:
  replicas: 1
  selector:
    matchLabels: {app: floci}
  template:
    metadata:
      labels: {app: floci}
    spec:
      enableServiceLinks: false
      containers:
      - name: floci
        image: floci/floci:2.3.0
        ports: [{containerPort: 4566}]
        readinessProbe:
          tcpSocket: {port: 4566}
          periodSeconds: 1
---
apiVersion: v1
kind: Service
metadata:
  name: floci
spec:
  selector: {app: floci}
  ports: [{port: 4566, targetPort: 4566}]
YAML
k -n "$namespace" rollout status deployment/floci --timeout=120s
k -n "$namespace" get pods -l app=floci -o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}' >"$run_dir/floci-initial-uid.txt"
for generation in 0 1 2; do
 if [ "$generation" -gt 0 ]; then
  k -n "$namespace" logs -l app=embedded --prefix >"$run_dir/generation-$generation-before-loss.log"
  k -n "$namespace" get pods -l app=embedded -o yaml >"$run_dir/generation-$generation-before-stop.yaml"
  k -n "$namespace" scale deployment/embedded --replicas=0
  k -n "$namespace" wait --for=delete pod -l app=embedded --timeout=90s
  docker exec dory-k8s crictl ps --label "io.kubernetes.pod.namespace=$namespace" --name embedded -q >"$run_dir/generation-$generation-running-containers.txt"
  if [ -s "$run_dir/generation-$generation-running-containers.txt" ]; then echo "Previous owned embedded container is still running" >&2; exit 1; fi
  k -n "$namespace" get pods -l app=embedded -o yaml >"$run_dir/generation-$generation-after-stop.yaml"
 fi
 # A new Pod gets a new emptyDir: the whole prior DataDir is lost. The separate
 # emulator Pod remains alive. Init can read remote objects and write only staging.
 cat <<YAML | k -n "$namespace" apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: embedded
spec:
  replicas: 1
  strategy: {type: Recreate}
  selector:
    matchLabels: {app: embedded}
  template:
    metadata:
      labels: {app: embedded}
    spec:
      enableServiceLinks: false
      terminationGracePeriodSeconds: 30
      initContainers:
      - name: prepare
        image: $image
        imagePullPolicy: Never
        args: ['-test.run=^TestEmbeddedObjectStorageKubernetesWorker$', '-test.v', '-test.timeout=2m']
        env:
        - {name: RHIZA_EMBEDDED_K8S_STAGE, value: init}
        - {name: RHIZA_EMBEDDED_GENERATION, value: '$generation'}
        - {name: RHIZA_EMBEDDED_PROVIDER, value: s3}
        - {name: RHIZA_EMBEDDED_ENDPOINT, value: 'http://floci:4566'}
        - {name: RHIZA_EMBEDDED_BUCKET, value: rhiza-embedded-k8s}
        volumeMounts: [{name: staging, mountPath: /staging}]
      containers:
      - name: embedded
        image: $image
        imagePullPolicy: Never
        args: ['-test.run=^TestEmbeddedObjectStorageKubernetesWorker$', '-test.v', '-test.timeout=16m']
        env:
        - {name: RHIZA_EMBEDDED_K8S_STAGE, value: main}
        - {name: RHIZA_EMBEDDED_GENERATION, value: '$generation'}
        - {name: RHIZA_EMBEDDED_PROVIDER, value: s3}
        - {name: RHIZA_EMBEDDED_ENDPOINT, value: 'http://floci:4566'}
        - {name: RHIZA_EMBEDDED_BUCKET, value: rhiza-embedded-k8s}
        readinessProbe:
          exec: {command: [/bin/sh, -c, 'test -s /tmp/embedded-ready']}
          periodSeconds: 1
        volumeMounts:
        - {name: data, mountPath: /data}
        - {name: staging, mountPath: /staging, readOnly: true}
      volumes:
      - name: data
        emptyDir: {}
      - name: staging
        emptyDir: {}
YAML
 k -n "$namespace" rollout status deployment/embedded --timeout=120s
 k -n "$namespace" logs -l app=embedded -c prepare --prefix >"$run_dir/generation-$generation-init.log"
 k -n "$namespace" logs -l app=embedded --prefix >"$run_dir/generation-$generation-ready.log"
 k -n "$namespace" get pods -o yaml >"$run_dir/generation-$generation-pods.yaml"
 k -n "$namespace" get pods -l app=floci -o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}' >"$run_dir/floci-current-uid.txt"
 if ! cmp -s "$run_dir/floci-initial-uid.txt" "$run_dir/floci-current-uid.txt"; then echo "Remote emulator Pod changed; backup persistence is not established" >&2; exit 1; fi
 k -n "$namespace" get pods -l app=floci -o jsonpath='{range .items[*].status.containerStatuses[*]}{.imageID}{"\n"}{end}' >"$run_dir/floci-image-id.txt"
 cat "$run_dir/generation-$generation-ready.log"
 cat "$run_dir/generation-$generation-init.log"
done
