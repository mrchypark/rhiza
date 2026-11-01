#!/bin/sh
# CI-only isolated virtual API; never apply Rhiza CRDs to the shared host API.
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
[ "${GITHUB_ACTIONS-}" = true ] || { echo 'GitHub Actions only' >&2; exit 1; }
: "${RUNNER_TEMP:?}" "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}" "${GITHUB_SHA:?}"
: "${RHIZA_HOST_KUBECONFIG:?}" "${RHIZA_HOST_CONTEXT:?}" "${RHIZA_HOST_NAMESPACE:?}" "${RHIZA_HOST_NAMESPACE_UID:?}" "${RHIZA_HOST_SERVICEACCOUNT_UID:?}" "${RHIZA_RELEASE_IMAGE:?}"
: "${RHIZA_HOST_NODE:?existing Ready node hostname label, verified by administrator}"
printf '%s\n' "$RHIZA_HOST_NODE" | grep -Eq '^[a-z0-9][a-z0-9.-]{0,252}$'
case "$RHIZA_HOST_NAMESPACE" in rhiza-ha-chaos-*) ;; *) echo 'isolated test namespace required' >&2; exit 1;; esac
printf '%s\n' "$RHIZA_RELEASE_IMAGE" | grep -Eq '^ghcr\.io/mrchypark/rhiza-operator@sha256:[0-9a-f]{64}$'
logs="$RUNNER_TEMP/ha-managed-cluster-chaos"
mkdir -p "$logs"
private=$(mktemp -d "$RUNNER_TEMP/ha-managed-cluster-private.XXXXXX")
chmod 700 "$private"
nonce=$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')
namespace=managed-bootstrap
image=$RHIZA_RELEASE_IMAGE
release="rhiza-chaos-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
kubeconfig="$private/virtual.config"
installed=0
forward_pid=
bucket_pid=
h() { kubectl --kubeconfig "$RHIZA_HOST_KUBECONFIG" --context "$RHIZA_HOST_CONTEXT" --request-timeout=30s "$@"; }
hn() { h -n "$RHIZA_HOST_NAMESPACE" "$@"; }
k() {
  if kubectl --kubeconfig "$kubeconfig" --request-timeout=30s "$@"; then
    if [ "$1" = "-n" ] && [ "$2" = "$namespace" ] &&
       [ "$3" = rollout ] && [ "$4" = status ] && [ "$5" = deployment/floci ]; then
      run_floci_dns_probe after-floci-rollout || :
    fi
    return 0
  else
    kubectl_status=$?
    return "$kubectl_status"
  fi
}
host_guard() {
  # A recreated namespace cannot recreate the token's original ServiceAccount UID.
  # This avoids granting the namespace-only CI account cluster-scoped Namespace GET.
  [ "$(hn get serviceaccount rhiza-chaos-runner -o jsonpath='{.metadata.uid}')" = "$RHIZA_HOST_SERVICEACCOUNT_UID" ]
}
project_virtual_dns_json() {
  resource=$1
  case "$resource" in
    pods)
      jq -c '
        def dnsname: test("(^|[-])(coredns|kube-dns)([-]|$)"; "i");
        def dnspod:
          ((.metadata.name // "") | dnsname) or
          ((.metadata.labels // {}) | .["k8s-app"] == "kube-dns" or
            .["app.kubernetes.io/name"] == "coredns" or .app == "coredns");
        {kind:"PodList",items:[.items[] | select(dnspod) | {
          name:.metadata.name,createdAt:.metadata.creationTimestamp,
          nodeName:.spec.nodeName,nodeSelectorHostname:(.spec.nodeSelector["kubernetes.io/hostname"] // null),
          dnsPolicy:.spec.dnsPolicy,podIP:.status.podIP,phase:.status.phase,
          conditions:[.status.conditions[]? | {type,status,reason}],
          containers:[.status.containerStatuses[]? | {
            name,ready,restartCount,started,
            state:{running:(.state.running // null | if . == null then null else {startedAt} end),
              waiting:(.state.waiting // null | if . == null then null else {reason} end),
              terminated:(.state.terminated // null | if . == null then null else {reason,exitCode,startedAt,finishedAt} end)},
            lastTermination:(.lastState.terminated // null | if . == null then null else {reason,exitCode,startedAt,finishedAt} end)
          }]
        }]}'
      ;;
    services)
      jq -c '
        def dnsname: test("(^|[-])(coredns|kube-dns)([-]|$)"; "i");
        def dnsservice:
          ((.metadata.name // "") | dnsname) or
          ((.metadata.labels // {}) | .["k8s-app"] == "kube-dns" or
            .["app.kubernetes.io/name"] == "coredns" or .app == "coredns") or
          ((.spec.selector // {}) | .["k8s-app"] == "kube-dns" or .app == "coredns");
        {kind:"ServiceList",items:[.items[] | select(dnsservice) | {
          name:.metadata.name,type:.spec.type,clusterIP:.spec.clusterIP,clusterIPs:.spec.clusterIPs,
          selector:{k8sApp:.spec.selector["k8s-app"],app:.spec.selector.app,
            appName:.spec.selector["app.kubernetes.io/name"]},
          ports:[.spec.ports[]? | {name,port,targetPort,protocol}]
        }]}'
      ;;
    endpoints)
      jq -c '
        def dnsname: test("(^|[-])(coredns|kube-dns)([-]|$)"; "i");
        {kind:"EndpointsList",items:[.items[] | select(((.metadata.name // "") | dnsname)) | {
          name:.metadata.name,
          subsets:[.subsets[]? | {
            addresses:[.addresses[]? | {ip,nodeName,targetRef:{kind:.targetRef.kind,name:.targetRef.name,namespace:.targetRef.namespace}}],
            notReadyAddresses:[.notReadyAddresses[]? | {ip,nodeName,targetRef:{kind:.targetRef.kind,name:.targetRef.name,namespace:.targetRef.namespace}}],
            ports:[.ports[]? | {name,port,protocol}]
          }]
        }]}'
      ;;
    endpointslices)
      jq -c '
        def dnsname: test("(^|[-])(coredns|kube-dns)([-]|$)"; "i");
        {kind:"EndpointSliceList",items:[.items[] | select(((.metadata.labels // {})["kubernetes.io/service-name"] // "") | dnsname) | {
          name:.metadata.name,serviceName:.metadata.labels["kubernetes.io/service-name"],addressType:.addressType,
          ports:[.ports[]? | {name,port,protocol}],
          endpoints:[.endpoints[]? | {
            addresses,conditions:{ready:.conditions.ready,serving:.conditions.serving,terminating:.conditions.terminating},
            targetRef:{kind:.targetRef.kind,name:.targetRef.name,namespace:.targetRef.namespace}
          }]
        }]}'
      ;;
    *) return 2 ;;
  esac
}
summarize_coredns_log() {
  stage=$1
  pod=$2
  stream=$3
  retrieved=$4
  raw=$5
  destination=$6
  case "$stage" in before-sql-fixture|before-vcluster-uninstall) ;; *) return 2 ;; esac
  case "$pod" in *[!a-zA-Z0-9.-]*|'') return 2 ;; esac
  case "$stream" in current|previous) ;; *) return 2 ;; esac
  if [ "$retrieved" != true ]; then
    jq -n --arg stage "$stage" --arg pod "$pod" --arg stream "$stream" \
      '{component:"coredns",stage:$stage,pod:$pod,stream:$stream,retrieved:false,lines:0,counts:{connection_refused:0,timeout:0,servfail:0,nxdomain:0,dns_refused:0,no_route:0,not_ready:0,ready:0,plugin_error:0,other:0}}' > "$destination"
    return 0
  fi
  counts=$(awk '
    BEGIN { refused=timeout=servfail=nxdomain=dnsrefused=noroute=notready=ready=pluginerror=other=lines=0 }
    {
      lines++
      message=tolower($0)
      if (message ~ /connection refused/) refused++
      else if (message ~ /(i\/o timeout|timed out|timeout)/) timeout++
      else if (message ~ /servfail/) servfail++
      else if (message ~ /nxdomain/) nxdomain++
      else if (message ~ /(^|[^a-z])refused([^a-z]|$)/) dnsrefused++
      else if (message ~ /(no route to host|network is unreachable)/) noroute++
      else if (message ~ /(still waiting|not ready)/) notready++
      else if (message ~ /plugin\/ready/ && message ~ /ready/) ready++
      else if (message ~ /plugin\/errors/) pluginerror++
      else other++
    }
    END { printf "%d %d %d %d %d %d %d %d %d %d %d\n", lines, refused, timeout, servfail, nxdomain, dnsrefused, noroute, notready, ready, pluginerror, other }
  ' "$raw")
  IFS=' ' read -r lines refused timeout servfail nxdomain dnsrefused noroute notready ready pluginerror other <<EOF
$counts
EOF
  jq -n --arg stage "$stage" --arg pod "$pod" --arg stream "$stream" \
    --argjson lines "$lines" --argjson refused "$refused" --argjson timeout "$timeout" \
    --argjson servfail "$servfail" --argjson nxdomain "$nxdomain" --argjson dnsrefused "$dnsrefused" \
    --argjson noroute "$noroute" --argjson notready "$notready" --argjson ready "$ready" \
    --argjson pluginerror "$pluginerror" --argjson other "$other" \
    '{component:"coredns",stage:$stage,pod:$pod,stream:$stream,retrieved:true,lines:$lines,counts:{connection_refused:$refused,timeout:$timeout,servfail:$servfail,nxdomain:$nxdomain,dns_refused:$dnsrefused,no_route:$noroute,not_ready:$notready,ready:$ready,plugin_error:$pluginerror,other:$other}}' > "$destination"
}
capture_virtual_dns() {
  phase=$1
  printf '%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" > "$logs/virtual-dns-$phase-captured-at.txt"
  for resource in pods services endpoints endpointslices; do
    raw="$private/virtual-dns-$resource.raw.json"
    destination="$logs/virtual-dns-$phase-$resource.json"
    if k -n kube-system get "$resource" -o json > "$raw" 2> "$private/virtual-dns-$resource.stderr"; then
      if ! project_virtual_dns_json "$resource" < "$raw" > "$destination"; then
        printf '%s\n' 'projection failed' > "$destination"
      fi
      rm -f "$private/virtual-dns-$resource.stderr"
    else
      printf '%s\n' 'API collection failed; see bounded collection error' > "$destination"
      sed -n '1,8p' "$private/virtual-dns-$resource.stderr" > "$logs/virtual-dns-$phase-$resource.error.txt"
      rm -f "$private/virtual-dns-$resource.stderr"
    fi
    rm -f "$raw"
  done
  for pod in $(jq -r '.items[] | select(any(.containers[]?; .name == "coredns")) | .name' "$logs/virtual-dns-$phase-pods.json" 2>/dev/null | head -4); do
    case "$pod" in *[!a-zA-Z0-9.-]*|'') continue ;; esac
    for stream in current previous; do
      raw="$private/coredns-$stream.raw.log"
      if [ "$stream" = previous ]; then
        if k -n kube-system logs "$pod" -c coredns --previous --tail=100 --limit-bytes=32768 > "$raw" 2>&1; then retrieved=true; else retrieved=false; fi
      else
        if k -n kube-system logs "$pod" -c coredns --tail=100 --limit-bytes=32768 > "$raw" 2>&1; then retrieved=true; else retrieved=false; fi
      fi
      if ! summarize_coredns_log "$phase" "$pod" "$stream" "$retrieved" "$raw" "$logs/virtual-dns-$phase-$pod-$stream.json"; then
        printf '{"component":"coredns","stage":"%s","pod":"%s","stream":"%s","summary":"unavailable"}\n' "$phase" "$pod" "$stream" > "$logs/virtual-dns-$phase-$pod-$stream.json"
      fi
      rm -f "$raw"
    done
  done
}
write_floci_dns_probe_manifest() {
  manifest_phase=$1
  case "$manifest_phase" in after-floci-rollout|before-vcluster-uninstall) ;; *) return 2 ;; esac
  probe_name="rhiza-dns-probe-$manifest_phase"
  jq -n --arg name "$probe_name" '{apiVersion:"v1",kind:"Pod",metadata:{name:$name,labels:{"rhiza-ci-component":"dns-probe"}},spec:{activeDeadlineSeconds:15,automountServiceAccountToken:false,enableServiceLinks:false,dnsPolicy:"ClusterFirst",restartPolicy:"Never",securityContext:{runAsUser:65532,runAsGroup:65532,runAsNonRoot:true},containers:[{name:"probe",image:"curlimages/curl:8.12.1@sha256:94e9e444bcba979c2ea12e27ae39bee4cd10bc7041a472c4727a558e213744e6",imagePullPolicy:"IfNotPresent",command:["curl"],args:["--silent","--show-error","--noproxy","*","--connect-timeout","3","--max-time","8","--output","/dev/null","--write-out","curl_writeout remote_ip=%{remote_ip} remote_port=%{remote_port} http_code=%{http_code}\\n","http://floci.managed-bootstrap.svc.cluster.local:4566/"],resources:{requests:{cpu:"10m",memory:"32Mi","ephemeral-storage":"8Mi"},limits:{cpu:"100m",memory:"64Mi","ephemeral-storage":"32Mi"}},securityContext:{allowPrivilegeEscalation:false,readOnlyRootFilesystem:true,capabilities:{drop:["ALL"]}}}]}}'
}
run_floci_dns_probe() {
  phase=$1
  case "$phase" in after-floci-rollout|before-vcluster-uninstall) ;; *) return 2 ;; esac
  name="rhiza-dns-probe-$phase"
  raw="$private/floci-endpoints.raw.json"
  if k -n "$namespace" get endpoints floci -o json > "$raw" 2> "$private/floci-endpoints.stderr"; then
    jq -c '{name:.metadata.name,subsets:[.subsets[]? | {addresses:[.addresses[]? | {ip,nodeName,targetRef:{kind:.targetRef.kind,name:.targetRef.name,namespace:.targetRef.namespace}}],notReadyAddresses:[.notReadyAddresses[]? | {ip,nodeName}],ports:[.ports[]? | {name,port,protocol}]}]}' "$raw" > "$logs/virtual-dns-$phase-floci-endpoints.json"
    ready=$(jq '[.subsets[]?.addresses[]?] | length' "$raw")
    rm -f "$private/floci-endpoints.stderr"
  else
    printf '%s\n' 'API collection failed' > "$logs/virtual-dns-$phase-floci-endpoints.json"
    sed -n '1,8p' "$private/floci-endpoints.stderr" > "$logs/virtual-dns-$phase-floci-endpoints.error.txt"
    ready=0
  fi
  rm -f "$raw" "$private/floci-endpoints.stderr"
  if [ "$ready" -eq 0 ]; then
    printf '%s\n' '{"phase":"SkippedNoReadyFlociEndpoint"}' > "$logs/virtual-dns-$phase-probe-status.json"
    return 0
  fi
  if ! write_floci_dns_probe_manifest "$phase" > "$private/dns-probe.json"; then
    printf '%s\n' '{"phase":"ManifestCreationFailed"}' > "$logs/virtual-dns-$phase-probe-status.json"
    return 0
  fi
  if ! k -n "$namespace" create -f "$private/dns-probe.json" > "$logs/virtual-dns-$phase-probe-create.txt" 2>&1; then
    printf '%s\n' '{"phase":"NotCreated"}' > "$logs/virtual-dns-$phase-probe-status.json"
    rm -f "$private/dns-probe.json"
    return 0
  fi
  rm -f "$private/dns-probe.json"
  raw="$private/dns-probe-status.raw.json"
  probe_status=Unknown
  tries=0
  while [ "$tries" -lt 20 ]; do
    if k -n "$namespace" get pod "$name" -o json > "$raw" 2>/dev/null; then
      probe_status=$(jq -r '.status.phase // "Unknown"' "$raw" 2>/dev/null || printf '%s' Unknown)
      case "$probe_status" in Succeeded|Failed) break ;; esac
    fi
    tries=$((tries + 1))
    sleep 1
  done
  if [ -f "$raw" ]; then
    jq -c '{name:.metadata.name,phase:.status.phase,conditions:[.status.conditions[]? | {type,status,reason}],containers:[.status.containerStatuses[]? | {name,ready,restartCount,state:{waiting:(.state.waiting // null | if . == null then null else {reason} end),terminated:(.state.terminated // null | if . == null then null else {reason,exitCode,startedAt,finishedAt} end)} }]}' "$raw" > "$logs/virtual-dns-$phase-probe-status.json" 2>/dev/null || printf '%s\n' '{"phase":"StatusProjectionFailed"}' > "$logs/virtual-dns-$phase-probe-status.json"
    rm -f "$raw"
  else
    printf '%s\n' '{"phase":"StatusUnavailable"}' > "$logs/virtual-dns-$phase-probe-status.json"
  fi
  k -n "$namespace" logs "$name" -c probe --tail=40 --limit-bytes=8192 > "$logs/virtual-dns-$phase-probe.log" 2>&1 || :
  k -n "$namespace" delete pod "$name" --wait=true --timeout=10s > "$logs/virtual-dns-$phase-probe-delete.txt" 2>&1 || :
}
cleanup() {
  result=$?
  original_result=$result
  printf '%s\n' "$original_result" > "$logs/driver-original.exit"
  trap - EXIT HUP INT TERM
  if [ "$installed" = 1 ]; then
    if host_guard; then
      if [ "$result" -ne 0 ] && [ -n "${fault-}" ]; then capture_chaos_failure "$fault" cleanup || :; fi
      hn get podchaos -o json > "$logs/faults-final.json" 2>&1 || result=1
      hn delete podchaos -l "rhiza-ci-run=$release" --wait=true --timeout=60s > "$logs/fault-cleanup.txt" 2>&1 || result=1
      if [ -f "$kubeconfig" ]; then
        capture_managed_logs
        k -n "$namespace" get pods,rhizaclusters,jobs,deployments -o json > "$logs/virtual-final.json" 2>&1 || :
        k -n "$namespace" get events > "$logs/virtual-events.txt" 2>&1 || :
        # Preserve DNS service/backend evidence before Helm removes the
        # virtual control plane. This is diagnostic only, not a readiness gate.
        if [ "$result" -ne 0 ]; then
          run_floci_dns_probe before-vcluster-uninstall || :
        fi
        capture_virtual_dns before-vcluster-uninstall
      fi
      helm uninstall "$release" --kubeconfig "$RHIZA_HOST_KUBECONFIG" --kube-context "$RHIZA_HOST_CONTEXT" --namespace "$RHIZA_HOST_NAMESPACE" --wait --timeout=3m > "$logs/helm-cleanup.txt" 2>&1 || result=1
      hn get pods -o json > "$logs/host-pods-after.json" 2>&1 || result=1
      jq -e '.items|length==0' "$logs/host-pods-after.json" >/dev/null || result=1
      hn get podchaos -l "rhiza-ci-run=$release" -o json > "$logs/faults-after.json" 2>&1 || result=1
      jq -e '.items|length==0' "$logs/faults-after.json" >/dev/null || result=1
    else
      echo 'namespace identity changed; cleanup refused' > "$logs/cleanup-blocked.txt"
      result=1
    fi
  fi
  for pid in "$bucket_pid" "$forward_pid"; do
    [ -n "$pid" ] || continue
    kill "$pid" 2>/dev/null || :
    wait "$pid" 2>/dev/null || :
    if kill -0 "$pid" 2>/dev/null; then result=1; fi
  done
  # Only this private run directory: credentials are never uploaded as artifacts.
  rm -f "$private/virtual.config" "$private/runtime.json" "$private/admin.token" "$private/curl.conf" "$private/request.body" "$private/chart.tgz" "$private/values.yaml" "$private/rendered.yaml" "$private/forward.log" "$private/bucket-forward.log" "$private/dns-probe.json" "$private/dns-probe-status.raw.json" "$private/floci-endpoints.raw.json" "$private/floci-endpoints.stderr" "$private/coredns-current.raw.log" "$private/coredns-previous.raw.log" "$private/virtual-dns-pods.raw.json" "$private/virtual-dns-services.raw.json" "$private/virtual-dns-endpoints.raw.json" "$private/virtual-dns-endpointslices.raw.json" "$private/virtual-dns-pods.stderr" "$private/virtual-dns-services.stderr" "$private/virtual-dns-endpoints.stderr" "$private/virtual-dns-endpointslices.stderr" "$RUNNER_TEMP/ha-managed-cluster-bucket-setup"
  rmdir "$private" || result=1
  [ "$original_result" -eq 0 ] || result=$original_result
  printf '%s\n' "$result" > "$logs/driver.exit"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
host_guard
printf '%s\n' "$RHIZA_HOST_NAMESPACE" "$RHIZA_HOST_NAMESPACE_UID" "$RHIZA_HOST_SERVICEACCOUNT_UID" > "$logs/host-scope.txt"
# The credential may administer only this disposable host namespace, never CRDs.
[ "$(h auth can-i create customresourcedefinitions.apiextensions.k8s.io)" = no ]
[ "$(h auth can-i create namespaces)" = no ]
hn get pods -o json > "$logs/host-pods-before.json"
jq -e '.items|length==0' "$logs/host-pods-before.json" >/dev/null
curl --fail --silent --show-error --max-time 60 https://charts.loft.sh/charts/vcluster-0.28.0.tgz -o "$private/chart.tgz"
printf '%s  %s\n' d65fa9a49dc423e62377fde56e5cbec727d3260d2dd17acfdca3bcc6d58110e4 "$private/chart.tgz" | sha256sum -c -
cat > "$private/values.yaml" <<'VALUES'
sync:
  fromHost:
    nodes: {enabled: false, syncBackChanges: false}
  toHost:
    persistentVolumeClaims: {enabled: false}
    pods:
      useSecretsForSATokens: true
      rewriteHosts: {enabled: false}
controlPlane:
  statefulSet:
    image: {registry: ghcr.io, repository: loft-sh/vcluster-oss, tag: 0.28.0}
    persistence:
      volumeClaim: {enabled: false}
    resources:
      requests: {cpu: 200m, memory: 256Mi, ephemeral-storage: 1Gi}
      limits: {cpu: '1', memory: 2Gi, ephemeral-storage: 4Gi}
rbac:
  clusterRole: {enabled: false}
VALUES
node_selector=$(jq -cn --arg node "$RHIZA_HOST_NODE" '{"kubernetes.io/hostname":$node}')
# OSS 0.28 podSyncer applies Nodes.Selector.Labels to EVERY physical Pod even
# when real-node synchronization is disabled. Do not use paid translate patches
# or an unsupported RhizaCluster podTemplate field to pretend this is enforced.
helm template "$release" "$private/chart.tgz" --namespace "$RHIZA_HOST_NAMESPACE" -f "$private/values.yaml" --set-json "sync.fromHost.nodes.selector.labels=$node_selector" --set-json "controlPlane.statefulSet.scheduling.nodeSelector=$node_selector" --set-json "controlPlane.coredns.deployment.nodeSelector=$node_selector" > "$private/rendered.yaml"
if grep -Eq '^kind: (ClusterRole|ClusterRoleBinding|CustomResourceDefinition|Namespace|PersistentVolume|PersistentVolumeClaim)$|privileged: true|hostPID: true|hostNetwork: true|hostPath:' "$private/rendered.yaml"; then
  echo 'forbidden host scope or privileges in pinned chart' >&2; exit 1
fi
installed=1
helm install "$release" "$private/chart.tgz" --kubeconfig "$RHIZA_HOST_KUBECONFIG" --kube-context "$RHIZA_HOST_CONTEXT" --namespace "$RHIZA_HOST_NAMESPACE" -f "$private/values.yaml" --set-json "sync.fromHost.nodes.selector.labels=$node_selector" --set-json "controlPlane.statefulSet.scheduling.nodeSelector=$node_selector" --set-json "controlPlane.coredns.deployment.nodeSelector=$node_selector" --wait --timeout=5m > "$logs/helm-install.txt" 2>&1
hn get secret "vc-$release" -o jsonpath='{.data.config}' | base64 --decode > "$kubeconfig"
hn port-forward --address=127.0.0.1 "service/$release" :443 > "$private/forward.log" 2>&1 &
forward_pid=$!
forward_port() {
  tries=0
  while :; do
    port=$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9]*\) -> .*$/\1/p' "$1" | head -1)
    [ -z "$port" ] || break
    kill -0 "$2" 2>/dev/null || return 1
    tries=$((tries+1)); [ "$tries" -lt 30 ] || return 1
    sleep 1
  done
}
forward_port "$private/forward.log" "$forward_pid"
config_cluster=$(kubectl --kubeconfig "$kubeconfig" config view -o jsonpath='{.contexts[0].context.cluster}')
kubectl --kubeconfig "$kubeconfig" config set-cluster "$config_cluster" --server="https://127.0.0.1:$port" >/dev/null
k get --raw=/readyz > "$logs/virtual-ready.txt"
k create namespace "$namespace"
k apply -f deploy/operator/cluster-crd.yaml -f deploy/operator/crd.yaml
k wait --for=condition=Established --timeout=90s crd/rhizaclusters.rhiza.mrchypark.dev crd/rhizarecoveries.rhiza.mrchypark.dev
managed_bucket_setup() {
  k -n "$namespace" port-forward --address=127.0.0.1 service/floci :4566 > "$private/bucket-forward.log" 2>&1 &
  bucket_pid=$!
  forward_port "$private/bucket-forward.log" "$bucket_pid"
  # Reuse the exact actual SDK helper; no ambient provider credentials or cloud.
  (cd scripts/ha-managed-bucket-setup-src && go build -mod=readonly -o "$RUNNER_TEMP/ha-managed-cluster-bucket-setup" .)
  "$RUNNER_TEMP/ha-managed-cluster-bucket-setup" "http://127.0.0.1:$port" "$bucket"
  kill "$bucket_pid"
  wait "$bucket_pid" 2>/dev/null || :
  bucket_pid=
}
capture_floci() { k -n "$namespace" logs deployment/floci --tail=80 > "$logs/floci.log" 2>&1 || :; }
capture_managed_logs() {
  capture_selector_logs() {
    source=$1 selector=$2
    pod_json=$(k -n "$namespace" get pods -l "$selector" -o json 2>/dev/null) || return 0
    if [ "$source" = operator ]; then
      records=$(printf '%s' "$pod_json" | jq -r '
        .items[] | select(.metadata.name | type == "string")
        | select(any(.spec.containers[]?; .name == "operator"))
        | [.metadata.name, "operator"] | join("/")
      ') || return 0
    else
      records=$(printf '%s' "$pod_json" | jq -r '
        .items[] as $pod | select($pod.metadata.name | type == "string")
        | ($pod.spec.containers[]? | select(.name == "sql") | [$pod.metadata.name, .name] | join("/")),
          ($pod.spec.initContainers[]? | select(.name == "rhiza-prepare-identity") | [$pod.metadata.name, .name] | join("/")),
          ($pod.spec.initContainers[]? | select(.name == "rhiza-await-bootstrap-plan") | [$pod.metadata.name, .name] | join("/"))
      ') || return 0
    fi
    for record in $records; do
      pod=${record%%/*}
      container=${record#*/}
      case "$pod" in *[!a-z0-9.-]*|'') continue ;; esac
      case "$container" in operator|sql|rhiza-prepare-identity|rhiza-await-bootstrap-plan) ;; *) continue ;; esac
      for version in current previous; do
        destination="$logs/$source-$pod-$container-$version.log"
        if [ "$version" = previous ]; then
          k -n "$namespace" logs "$pod" -c "$container" --tail=200 --limit-bytes=65536 --previous > "$destination" 2>/dev/null || :
        else
          k -n "$namespace" logs "$pod" -c "$container" --tail=200 --limit-bytes=65536 > "$destination" 2>/dev/null || :
        fi
      done
    done
  }

  # Extract only Pod/container names from API responses; never save Pod specs,
  # environment values, Secret references or projected profile data.
  capture_selector_logs operator 'app=operator'
  if [ -n "${uid-}" ]; then
    capture_selector_logs managed "rhiza.mrchypark.dev/cluster-uid=$uid"
  fi
}
mapped_host_pod() {
  virtual_pod=$1 virtual_uid=$2
  host_guard
  k -n "$namespace" get pod "$virtual_pod" -o json > "$logs/$fault-virtual-before.json"
  jq -e --arg uid "$virtual_uid" '.metadata.uid==$uid and .metadata.deletionTimestamp==null' "$logs/$fault-virtual-before.json" >/dev/null
  hn get pods -o json | jq --arg name "$virtual_pod" --arg ns "$namespace" --arg uid "$virtual_uid" '[.items[]|select(.metadata.annotations["vcluster.loft.sh/object-name"]==$name and .metadata.annotations["vcluster.loft.sh/object-namespace"]==$ns and .metadata.annotations["vcluster.loft.sh/object-uid"]==$uid)]' > "$logs/$fault-host-before.json"
  jq -e --arg node "$RHIZA_HOST_NODE" 'length==1 and .[0].metadata.deletionTimestamp==null and .[0].spec.nodeName==$node and .[0].spec.nodeSelector["kubernetes.io/hostname"]==$node and any(.[0].status.containerStatuses[]; .name=="sql" and .ready)' "$logs/$fault-host-before.json" >/dev/null
  host_pod=$(jq -r '.[0].metadata.name' "$logs/$fault-host-before.json")
  host_uid=$(jq -r '.[0].metadata.uid' "$logs/$fault-host-before.json")
}
chaos_record_oracle() {
  jq -e --arg id "$2" '
    any(.status.conditions[]?; .type=="AllInjected" and .status=="True") and
    (.status.experiment.containerRecords|length)==1 and
    all(.status.experiment.containerRecords[];
      .id==$id and .phase=="Injected" and
      (.injectedCount|type)=="number" and (.recoveredCount|type)=="number" and
      .injectedCount>0 and .recoveredCount>=0 and .injectedCount>.recoveredCount)
  ' "$1" >/dev/null
}
capture_chaos_failure() {
  case "$1" in pod-kill|container-kill) ;; *) return 2 ;; esac
  case "$2" in AllInjected|cleanup) ;; *) return 2 ;; esac
  host_guard || return 1
  failure_json="$private/chaos-failure.json"
  if hn get "podchaos/$1" -o json > "$failure_json" 2>/dev/null; then
    if jq -e --arg ns "$RHIZA_HOST_NAMESPACE" --arg run "$release" '.metadata.namespace==$ns and .metadata.labels["rhiza-ci-run"]==$run' "$failure_json" >/dev/null; then
      jq '{apiVersion,kind,metadata:{name:.metadata.name,namespace:.metadata.namespace,uid:.metadata.uid,generation:.metadata.generation},spec,status}' "$failure_json" | head -c 65536 > "$logs/$1-$2-failure.json"
      failure_uid=$(jq -r '.metadata.uid' "$failure_json")
      hn get events --field-selector "involvedObject.kind=PodChaos,involvedObject.uid=$failure_uid" -o json 2>/dev/null |
        jq '{items:[.items[]? | {reason,message,count,firstTimestamp,lastTimestamp,involvedObject}] | .[-20:]}' |
        head -c 65536 > "$logs/$1-$2-failure-events.json" || :
    fi
  fi
  rm -f "$failure_json"
}
wait_chaos_injected() {
  if hn wait --for=condition=AllInjected "podchaos/$1" --timeout=90s; then
    return 0
  else
    wait_failure_exit=$?
    printf '%s\n' "$wait_failure_exit" > "$logs/$1-AllInjected-wait.exit"
    capture_chaos_failure "$1" AllInjected || :
    return "$wait_failure_exit"
  fi
}
chaos_fault() {
  action=$1 fault=$2
  mapped_host_pod "$3" "$4"
  old_container=$(jq -r '.[0].status.containerStatuses[]|select(.name=="sql")|.containerID' "$logs/$fault-host-before.json")
  [ -n "$old_container" ] && [ "$old_container" != null ]
  expected_record="$RHIZA_HOST_NAMESPACE/$host_pod"
  if [ "$action" = container-kill ]; then expected_record="$expected_record/sql"; fi
  jq -n --arg fault "$fault" --arg ns "$RHIZA_HOST_NAMESPACE" --arg run "$release" --arg action "$action" --arg pod "$host_pod" --arg uid "$4" '{apiVersion:"chaos-mesh.org/v1alpha1",kind:"PodChaos",metadata:{name:$fault,namespace:$ns,labels:{"rhiza-ci-run":$run}},spec:{action:$action,mode:"one",duration:"10s",containerNames:["sql"],selector:{namespaces:[$ns],pods:{($ns):[$pod]},annotationSelectors:{"vcluster.loft.sh/object-uid":$uid}}}}' > "$logs/$fault-request.json"
  hn create -f "$logs/$fault-request.json"
  wait_chaos_injected "$fault"
  hn get "podchaos/$fault" -o json > "$logs/$fault-injected.json"
  chaos_record_oracle "$logs/$fault-injected.json" "$expected_record"
  if [ "$action" = container-kill ]; then
    tries=0
    until hn get pod "$host_pod" -o json > "$logs/$fault-host-after.json" &&
      jq -e --arg uid "$host_uid" --arg old "$old_container" '.metadata.uid==$uid and any(.status.containerStatuses[]; .name=="sql" and .containerID!=$old and .lastState.terminated.containerID==$old)' "$logs/$fault-host-after.json" >/dev/null; do
      tries=$((tries+1)); [ "$tries" -lt 60 ] || exit 1; sleep 1
    done
  else
    hn get pods --field-selector "metadata.name=$host_pod" -o json > "$logs/$fault-host-after.json"
    jq -e '.items|length==0' "$logs/$fault-host-after.json" >/dev/null
  fi
  # Kill actions are one-shot: remove their object before application recovery checks.
  hn get "podchaos/$fault" -o json > "$logs/$fault-before-delete.json"
  hn delete "podchaos/$fault" --wait=true --timeout=60s
  hn get "podchaos/$fault" --ignore-not-found -o json > "$logs/$fault-after-delete.json"
  [ ! -s "$logs/$fault-after-delete.json" ]
  fault=
}
managed_pod_fault() {
  k -n "$namespace" exec "$1" -c sql -- sha256sum /var/lib/rhiza/data/qlog/voter-identity.json > "$logs/old-voter-identity.txt"
  chaos_fault pod-kill pod-kill "$1" "$2"
}
managed_container_fault() {
  k -n "$namespace" exec "$1" -c sql -- sha256sum /var/lib/rhiza/data/qlog/voter-identity.json > "$logs/replacement-voter-identity.txt"
  if cmp -s "$logs/old-voter-identity.txt" "$logs/replacement-voter-identity.txt"; then
    echo 'emptyDir replacement incorrectly reused old voter identity' >&2; exit 1
  fi
  chaos_fault container-kill container-kill "$1" "$2"
}
. scripts/ha-managed-bootstrap-fixture.sh
capture_virtual_dns before-sql-fixture || :
managed_fixture
# No synthetic virtual delete fallback: common oracle requires new virtual UID
# and a Completed certified replacement after the host Chaos Mesh pod-kill.
run_client insert-after-chaos execute '{"request_id":"chaos-final-insert","sql":"INSERT INTO bootstrap_ci VALUES (2, '\''after-chaos'\'')"}'
run_client query-after-chaos query '{"sql":"SELECT id, value FROM bootstrap_ci ORDER BY id"}'
jq -e '.rows==[[1,"real-kubelet"],[2,"after-chaos"]]' "$logs/query-after-chaos.json" >/dev/null
printf '%s\n' 'PASS: released-image managed HA in isolated Kubernetes vcluster; actual host Chaos Mesh pod-kill/container-kill, certified replacement, same-Pod identity retention, NoPVC, ACK readback and new write' 'SCOPE: pinned in-namespace Floci S3; not live GCS, network partition, physical fencing, whole-quorum disaster recovery or performance qualification' > "$logs/result.txt"
