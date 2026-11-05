#!/bin/sh
# Offline replay of the production virtual-to-host identity guard. No API calls.
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
for source in scripts/test-ha-managed-bootstrap.sh scripts/ha-managed-bootstrap-fixture.sh scripts/test-ha-managed-cluster-chaos.sh; do sh -n "$source"; done
helper=$(sed -n '/^mapped_host_pod() {/,/^}/p' scripts/test-ha-managed-cluster-chaos.sh)
[ -n "$helper" ]
scratch=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-cluster-mapping.XXXXXX")
cleanup() {
  unlink "$scratch/test-virtual-before.json" 2>/dev/null || :
  unlink "$scratch/test-host-before.json" 2>/dev/null || :
  unlink "$scratch/fault.json" 2>/dev/null || :
  for kill in pod-kill container-kill; do
    for suffix in request.json pod-before.json pod-after.json pods-after.json host-before.json host-after.json injected.json before-delete.json after-delete.json; do
      unlink "$scratch/$kill-$suffix" 2>/dev/null || :
    done
  done
  for file in fault-valid.json fault-invalid.json fault-events.json capture-trace.txt pod-kill-AllInjected-wait.exit pod-kill-AllInjected-failure.json pod-kill-AllInjected-failure-events.json chaos-failure.json driver-original.exit driver.exit; do
    unlink "$scratch/$file" 2>/dev/null || :
  done
  for file in dns-pods-input.json dns-services-input.json dns-endpoints-input.json dns-endpointslices-input.json dns-pods-output.json dns-services-output.json dns-endpoints-output.json dns-endpointslices-output.json coredns-log-input.txt coredns-log-output.json coredns-log-unavailable.json; do
    unlink "$scratch/$file" 2>/dev/null || :
  done
  unlink "$scratch/dns-probe-manifest.json" 2>/dev/null || :
  unlink "$scratch/dns-probe-invalid-manifest.json" 2>/dev/null || :
  rmdir "$scratch"
}
trap cleanup EXIT HUP INT TERM
virtual='{"metadata":{"name":"pod-1","uid":"virtual-1"}}'
host='{"items":[{"metadata":{"name":"host-1","uid":"host-uid","annotations":{"vcluster.loft.sh/object-name":"pod-1","vcluster.loft.sh/object-namespace":"managed-bootstrap","vcluster.loft.sh/object-uid":"virtual-1"}},"spec":{"nodeName":"existing-node","nodeSelector":{"kubernetes.io/hostname":"existing-node"}},"status":{"containerStatuses":[{"name":"sql","ready":true}]}}]}'
replay() {
  sh -c 'set -eu
    logs=$1 virtual_json=$2 host_json=$3; namespace=managed-bootstrap; fault=test; RHIZA_HOST_NODE=existing-node
    host_guard() { :; }
    k() { printf "%s\n" "$virtual_json"; }
    hn() { printf "%s\n" "$host_json"; }
    eval "$4"
    mapped_host_pod pod-1 virtual-1
  ' fixture "$scratch" "$virtual" "$1" "$helper"
}
replay "$host"
[ "$(jq -r '.[0].metadata.uid' "$scratch/test-host-before.json")" = host-uid ]
for invalid in \
  "$(printf '%s' "$host" | jq '.items+=.items')" \
  "$(printf '%s' "$host" | jq '.items[0].metadata.annotations["vcluster.loft.sh/object-uid"]="other"')" \
  "$(printf '%s' "$host" | jq '.items[0].spec.nodeName="new-node"')" \
  "$(printf '%s' "$host" | jq '.items[0].status.containerStatuses[0].ready=false')"; do
  code=0
  replay "$invalid" || code=$?
  [ "$code" -ne 0 ] || { echo 'unsafe host mapping accepted' >&2; exit 1; }
done
for driver in scripts/test-ha-managed-cluster-chaos.sh scripts/test-ha-managed-direct-host-chaos.sh; do
  fault_source=$driver
  case "$driver" in *direct-host*) fault_source=scripts/ha-managed-podchaos.sh ;; esac
  oracle=$(sed -n '/^chaos_record_oracle() {/,/^}/p' "$fault_source")
  [ -n "$oracle" ]
  for target in test-ns/host-1 test-ns/host-1/sql; do
    for condition in AllInjected AllRecovered; do
      phase=Injected recovered=0
      if [ "$condition" = AllRecovered ]; then phase='Not Injected'; recovered=1; fi
      jq -n --arg id "$target" --arg condition "$condition" --arg phase "$phase" --argjson recovered "$recovered" '{status:{conditions:[{type:$condition,status:"True"}],experiment:{containerRecords:[{id:$id,phase:$phase,injectedCount:1,recoveredCount:$recovered}]}}}' > "$scratch/fault-valid.json"
      code=0
      sh -c 'set -eu; eval "$1"; chaos_record_oracle "$2" "$3"' fixture "$oracle" "$scratch/fault-valid.json" "$target" || code=$?
      if [ "$condition" = AllRecovered ]; then
        [ "$code" -ne 0 ] || { echo 'recovered record accepted as injected' >&2; exit 1; }
        continue
      fi
      [ "$code" -eq 0 ]
      for invalid in '.status.experiment.containerRecords[0].id="wrong-ns/other-pod"' 'del(.status.experiment.containerRecords[0].injectedCount)' 'del(.status.experiment.containerRecords[0].recoveredCount)' '.status.experiment.containerRecords[0].injectedCount=0' '.status.experiment.containerRecords[0].recoveredCount=4' '.status.experiment.containerRecords[0].phase="wrong"' '.status.conditions[0].status="False"' '.status.experiment.containerRecords+=.status.experiment.containerRecords'; do
        jq "$invalid" "$scratch/fault-valid.json" > "$scratch/fault-invalid.json"
        code=0
        sh -c 'set -eu; eval "$1"; chaos_record_oracle "$2" "$3"' fixture "$oracle" "$scratch/fault-invalid.json" "$target" || code=$?
        [ "$code" -ne 0 ] || { echo 'invalid Chaos phase/target/count evidence accepted' >&2; exit 1; }
      done
    done
  done
  capture=$(sed -n '/^capture_chaos_failure() {/,/^}/p' "$fault_source")
  waiter=$(sed -n '/^wait_chaos_injected() {/,/^}/p' "$fault_source")
  [ -n "$capture" ] && [ -n "$waiter" ]
  jq -n '{apiVersion:"chaos-mesh.org/v1alpha1",kind:"PodChaos",metadata:{name:"pod-kill",namespace:"test-ns",uid:"fault-uid",labels:{"rhiza-ci-run":"test-run"}},status:{conditions:[{type:"AllInjected",status:"False"}]}}' > "$scratch/fault.json"
  jq -n '{items:[{reason:"Failed",message:("x" * 70000),involvedObject:{kind:"PodChaos",name:"pod-kill",uid:"fault-uid"}}]}' > "$scratch/fault-events.json"
  sh -c 'set -eu
    private=$1 logs=$1; RHIZA_HOST_NAMESPACE=test-ns; namespace=test-ns; release=test-run
    host_guard() { return 0; }
    chaos_scope_guard() { host_guard; }
    chaos_k() { hn "$@"; }
    hn() {
      printf "%s\n" "$*" >> "$logs/capture-trace.txt"
      case "$1/$2" in wait/*) return 7 ;; get/podchaos/pod-kill) cat "$logs/fault.json" ;; get/events) cat "$logs/fault-events.json" ;; *) return 9 ;; esac
    }
    eval "$2"; eval "$3"
    code=0; wait_chaos_injected pod-kill || code=$?
    [ "$code" -eq 7 ] && [ "$(cat "$logs/pod-kill-AllInjected-wait.exit")" -eq 7 ]
    jq -e ".metadata.uid==\"fault-uid\" and .status.conditions[0].status==\"False\"" "$logs/pod-kill-AllInjected-failure.json" >/dev/null
    [ "$(wc -c < "$logs/pod-kill-AllInjected-failure-events.json")" -le 65536 ]
    grep -Fq "involvedObject.uid=fault-uid" "$logs/capture-trace.txt"
    [ ! -e "$private/chaos-failure.json" ]
    before=$(wc -l < "$logs/capture-trace.txt")
    code=0; capture_chaos_failure arbitrary cleanup || code=$?
    [ "$code" -eq 2 ] && [ "$(wc -l < "$logs/capture-trace.txt")" -eq "$before" ]
    host_guard() { return 1; }
    code=0; capture_chaos_failure pod-kill cleanup || code=$?
    [ "$code" -eq 1 ] && [ "$(wc -l < "$logs/capture-trace.txt")" -eq "$before" ]
  ' fixture "$scratch" "$capture" "$waiter"
  cleanup_helper=$(sed -n '/^cleanup() {/,/^}/p' "$driver")
  mkdir "$scratch/cleanup-private"
  cleanup_code=0
  sh -c 'set +e
    private=$1/cleanup-private logs=$1 RUNNER_TEMP=$1
    owned=0 installed=0 bucket_pid= forward_pid=
    eval "$2"
    (exit 7)
    cleanup
  ' fixture "$scratch" "$cleanup_helper" || cleanup_code=$?
  [ "$cleanup_code" -eq 7 ] && [ "$(cat "$scratch/driver-original.exit")" -eq 7 ] && [ "$(cat "$scratch/driver.exit")" -eq 7 ]
  [ ! -e "$scratch/cleanup-private" ]
  if grep -Fq 'AllRecovered' "$fault_source"; then echo 'one-shot kill waits for unsupported pre-delete recovery condition' >&2; exit 1; fi
  fault_function=chaos_fault
  case "$driver" in *direct-host*) fault_function=direct_chaos_fault ;; esac
  runner=$(sed -n "/^$fault_function() {/,/^}/p" "$fault_source")
  request_builder=$(sed -n '/^managed_chaos_request() {/,/^}/p' "$fault_source")
  for kill in pod-kill container-kill; do
    sh -c 'set -eu
      logs=$1; namespace=test-ns RHIZA_HOST_NAMESPACE=test-ns release=test-run uid=cluster-uid
      event_deleted=0 pod_reads=0
      chaos_scope_guard() { return 0; }
      chaos_k() { hn "$@"; }
      : > "$logs/capture-trace.txt"
      hn() {
        printf "%s\n" "$*" >> "$logs/capture-trace.txt"
        case "$1/$2" in
          create/*|wait/*) return 0 ;;
          get/pod)
            pod_reads=$((pod_reads+1)); cid=old; [ "$pod_reads" -eq 1 ] || cid=new
            jq -n --arg cid "$cid" "{metadata:{name:\"host-1\",uid:\"pod-uid\",labels:{\"rhiza.mrchypark.dev/cluster-uid\":\"cluster-uid\"}},status:{containerStatuses:[{name:\"sql\",ready:true,containerID:\$cid,lastState:{terminated:{containerID:\"old\"}}}]}}" ;;
          get/pods) printf "%s\n" "{\"items\":[]}" ;;
          get/podchaos/*)
            [ "$event_deleted" -eq 0 ] || return 0
            record=test-ns/host-1; [ "$action" != container-kill ] || record=$record/sql
            jq -n --arg id "$record" "{status:{conditions:[{type:\"AllInjected\",status:\"True\"}],experiment:{containerRecords:[{id:\$id,phase:\"Injected\",injectedCount:1,recoveredCount:0}]}}}" ;;
          delete/podchaos/*) event_deleted=1 ;;
          *) return 9 ;;
        esac
      }
      mapped_host_pod() { host_pod=host-1 host_uid=pod-uid; hn get pod host-1 -o json | jq "[.]" > "$logs/$fault-host-before.json"; }
      eval "$2"; eval "$3"; eval "$4"; eval "$7"
      "$5" "$6" "$6" host-1 pod-uid
      [ "$event_deleted" -eq 1 ]
      printf "%s\n" app-recovery-check >> "$logs/capture-trace.txt"
      deleted=$(grep -n "^delete podchaos/$6 " "$logs/capture-trace.txt" | cut -d: -f1)
      absence=$(grep -n "^get podchaos/$6 --ignore-not-found " "$logs/capture-trace.txt" | cut -d: -f1)
      app=$(grep -n "^app-recovery-check$" "$logs/capture-trace.txt" | cut -d: -f1)
      [ "$deleted" -lt "$absence" ] && [ "$absence" -lt "$app" ]
    ' fixture "$scratch" "$oracle" "$waiter" "$runner" "$fault_function" "$kill" "$request_builder"
  done
done
[ "$(grep -c 'set-json "sync.fromHost.nodes.selector.labels=' scripts/test-ha-managed-cluster-chaos.sh)" -eq 2 ]
if grep -q 'podTemplate.spec.nodeSelector' scripts/test-ha-managed-cluster-chaos.sh; then echo 'unsupported RhizaCluster node pin' >&2; exit 1; fi
projector=$(sed -n '/^project_virtual_dns_json() {/,/^}/p' scripts/test-ha-managed-cluster-chaos.sh)
[ -n "$projector" ]
jq -n '{items:[
  {metadata:{name:"rhiza-coredns-0",uid:"dns-uid",creationTimestamp:"2026-11-01T00:00:00Z",labels:{app:"coredns"},annotations:{secret:"DO_NOT_LEAK_ANNOTATION"}},spec:{nodeName:"node-a",nodeSelector:{"kubernetes.io/hostname":"node-a"},dnsPolicy:"ClusterFirst",containers:[{name:"coredns",env:[{name:"TOKEN",value:"DO_NOT_LEAK_ENV"}],volumeMounts:[{name:"token",mountPath:"/var/run/token"}]}]},status:{podIP:"10.1.0.4",phase:"Running",conditions:[{type:"Ready",status:"True",reason:"ContainersReady",message:"DO_NOT_LEAK_MESSAGE"}],containerStatuses:[{name:"coredns",ready:true,restartCount:2,started:true,state:{running:{startedAt:"2026-11-01T00:00:00Z"}},lastState:{terminated:{reason:"Error",exitCode:1,startedAt:"a",finishedAt:"b",message:"DO_NOT_LEAK_TERMINATION"}}}]}},
  {metadata:{name:"floci",labels:{app:"floci"}},spec:{containers:[{name:"floci"}]},status:{phase:"Running"}}
]}' > "$scratch/dns-pods-input.json"
jq -n '{items:[
  {metadata:{name:"kube-dns",labels:{"k8s-app":"kube-dns"}},spec:{type:"ClusterIP",clusterIP:"10.12.1.242",clusterIPs:["10.12.1.242"],selector:{"k8s-app":"kube-dns",secret:"DO_NOT_LEAK_SELECTOR"},ports:[{name:"dns",port:53,targetPort:53,protocol:"UDP"},{name:"dns-tcp",port:53,targetPort:53,protocol:"TCP"}]}},
  {metadata:{name:"unrelated"},spec:{clusterIP:"10.0.0.1",ports:[{port:443}]}}
]}' > "$scratch/dns-services-input.json"
jq -n '{items:[
  {metadata:{name:"kube-dns"},subsets:[{addresses:[{ip:"10.1.0.4",nodeName:"node-a",targetRef:{kind:"Pod",name:"rhiza-coredns-0",namespace:"kube-system",uid:"dns-uid"}}],notReadyAddresses:[],ports:[{name:"dns",port:53,protocol:"UDP"}]}]},
  {metadata:{name:"unrelated"},subsets:[]}
]}' > "$scratch/dns-endpoints-input.json"
jq -n '{items:[
  {metadata:{name:"kube-dns-abc",labels:{"kubernetes.io/service-name":"kube-dns",secret:"DO_NOT_LEAK_SLICE_LABEL"}},addressType:"IPv4",ports:[{name:"dns",port:53,protocol:"UDP"}],endpoints:[{addresses:["10.1.0.4"],conditions:{ready:true,serving:true,terminating:false},targetRef:{kind:"Pod",name:"rhiza-coredns-0",namespace:"kube-system",uid:"dns-uid"},hostname:"DO_NOT_LEAK_HOSTNAME"}]},
  {metadata:{name:"unrelated-abc",labels:{"kubernetes.io/service-name":"unrelated"}},addressType:"IPv4",ports:[],endpoints:[]}
]}' > "$scratch/dns-endpointslices-input.json"
for resource in pods services endpoints endpointslices; do
  sh -c 'set -eu; eval "$1"; project_virtual_dns_json "$2" < "$3"' fixture "$projector" "$resource" "$scratch/dns-$resource-input.json" > "$scratch/dns-$resource-output.json"
done
jq -e '.items|length==1 and .[0].name=="rhiza-coredns-0" and .[0].nodeName=="node-a" and .[0].containers[0].restartCount==2 and .[0].containers[0].state.running.startedAt=="2026-11-01T00:00:00Z"' "$scratch/dns-pods-output.json" >/dev/null
jq -e '.items|length==1 and .[0].name=="kube-dns" and .[0].clusterIP=="10.12.1.242" and (.[0].ports|length)==2' "$scratch/dns-services-output.json" >/dev/null
jq -e '.items|length==1 and .[0].name=="kube-dns" and .[0].subsets[0].addresses[0].targetRef.name=="rhiza-coredns-0"' "$scratch/dns-endpoints-output.json" >/dev/null
jq -e '.items|length==1 and .[0].serviceName=="kube-dns" and .[0].endpoints[0].conditions.ready==true' "$scratch/dns-endpointslices-output.json" >/dev/null
if grep -En 'DO_NOT_LEAK|annotations|"env"|volumeMounts|secret|"message"|uid' "$scratch"/dns-*-output.json; then
  echo 'virtual DNS projection leaked non-allowlisted source fields' >&2; exit 1
fi
cat > "$scratch/coredns-log-input.txt" <<'LOGS'
[ERROR] plugin/errors: 2 customer.private.example. A: read udp 10.1.2.3:53->10.2.3.4:53: connection refused
[ERROR] plugin/errors: 1 sensitive-record.internal. TXT: NXDOMAIN
[INFO] plugin/ready: Still waiting on: "private-zone-name"
[INFO] plugin/ready: Ready
[ERROR] plugin/errors: 1 confidential-query.invalid. AAAA: upstream failure
Bearer DO_NOT_LEAK_SECRET=abc123 customer.private.example
LOGS
summarizer=$(sed -n '/^summarize_coredns_log() {/,/^}/p' scripts/test-ha-managed-cluster-chaos.sh)
[ -n "$summarizer" ]
sh -c 'set -eu; eval "$1"; summarize_coredns_log before-sql-fixture coredns-abc123 current true "$2" "$3"' fixture "$summarizer" "$scratch/coredns-log-input.txt" "$scratch/coredns-log-output.json"
jq -e '.component=="coredns" and .stage=="before-sql-fixture" and .pod=="coredns-abc123" and .stream=="current" and .retrieved==true and .lines==6 and .counts.connection_refused==1 and .counts.nxdomain==1 and .counts.not_ready==1 and .counts.ready==1 and .counts.plugin_error==1 and .counts.other==1' "$scratch/coredns-log-output.json" >/dev/null
if grep -E 'customer\.private\.example|sensitive-record\.internal|private-zone-name|confidential-query\.invalid|DO_NOT_LEAK_SECRET|10\.1\.2\.3|Bearer' "$scratch/coredns-log-output.json"; then
  echo 'CoreDNS summary leaked an arbitrary log field or query name' >&2; exit 1
fi
sh -c 'set -eu; eval "$1"; summarize_coredns_log before-vcluster-uninstall coredns-abc123 previous false "$2" "$3"' fixture "$summarizer" "$scratch/coredns-log-input.txt" "$scratch/coredns-log-unavailable.json"
jq -e '.stage=="before-vcluster-uninstall" and .stream=="previous" and .retrieved==false and .lines==0 and ([.counts[]]|add)==0' "$scratch/coredns-log-unavailable.json" >/dev/null
if ! grep -q 'capture_virtual_dns before-sql-fixture' scripts/test-ha-managed-cluster-chaos.sh ||
   ! grep -q 'capture_virtual_dns before-vcluster-uninstall' scripts/test-ha-managed-cluster-chaos.sh; then
  echo 'virtual DNS capture is missing at fixture startup or before vcluster cleanup' >&2; exit 1
fi
before_capture_line=$(grep -n 'capture_virtual_dns before-sql-fixture' scripts/test-ha-managed-cluster-chaos.sh | cut -d: -f1)
fixture_line=$(grep -n '^managed_fixture$' scripts/test-ha-managed-cluster-chaos.sh | tail -1 | cut -d: -f1)
test "$before_capture_line" -lt "$fixture_line"
cleanup_capture_line=$(grep -n 'capture_virtual_dns before-vcluster-uninstall' scripts/test-ha-managed-cluster-chaos.sh | head -1 | cut -d: -f1)
cleanup_uninstall_line=$(grep -n 'helm uninstall' scripts/test-ha-managed-cluster-chaos.sh | head -1 | cut -d: -f1)
test "$cleanup_capture_line" -lt "$cleanup_uninstall_line"
grep -q 'run_floci_dns_probe after-floci-rollout' scripts/test-ha-managed-cluster-chaos.sh
probe=$(sed -n '/^run_floci_dns_probe() {/,/^}/p' scripts/test-ha-managed-cluster-chaos.sh)
printf '%s\n' "$probe" | grep -Fq 'ready=$(jq'
printf '%s\n' "$probe" | grep -Fq '.subsets[]?.addresses[]?'
manifest_writer=$(sed -n '/^write_floci_dns_probe_manifest() {/,/^}/p' scripts/test-ha-managed-cluster-chaos.sh)
[ -n "$manifest_writer" ]
sh -c 'set -eu; eval "$1"; write_floci_dns_probe_manifest after-floci-rollout' fixture "$manifest_writer" > "$scratch/dns-probe-manifest.json"
jq -e '
  .apiVersion=="v1" and .kind=="Pod" and .metadata.name=="rhiza-dns-probe-after-floci-rollout" and
  .spec.activeDeadlineSeconds==15 and .spec.automountServiceAccountToken==false and
  .spec.enableServiceLinks==false and .spec.dnsPolicy=="ClusterFirst" and
  .spec.securityContext.runAsNonRoot==true and (.spec.containers|length)==1 and
  .spec.containers[0].image=="curlimages/curl:8.12.1@sha256:94e9e444bcba979c2ea12e27ae39bee4cd10bc7041a472c4727a558e213744e6" and
  .spec.containers[0].command==["curl"] and
  .spec.containers[0].args[-1]=="http://floci.managed-bootstrap.svc.cluster.local:4566/" and
  (.spec.containers[0].args|index("--fail")==null) and
  (.spec.containers[0].args|index("curl_writeout remote_ip=%{remote_ip} remote_port=%{remote_port} http_code=%{http_code}\\n")!=null) and
  .spec.containers[0].resources.requests["ephemeral-storage"]=="8Mi" and
  .spec.containers[0].resources.limits["ephemeral-storage"]=="32Mi" and
  .spec.containers[0].securityContext.readOnlyRootFilesystem==true and
  (.spec|has("hostNetwork")|not)
' "$scratch/dns-probe-manifest.json" >/dev/null
probe_manifest_code=0
sh -c 'set -eu; eval "$1"; write_floci_dns_probe_manifest invalid-phase' fixture "$manifest_writer" > "$scratch/dns-probe-invalid-manifest.json" || probe_manifest_code=$?
[ "$probe_manifest_code" -ne 0 ] && [ ! -s "$scratch/dns-probe-invalid-manifest.json" ] || { echo 'invalid DNS probe phase produced a Pod manifest' >&2; exit 1; }
grep -q -- '--tail=100 --limit-bytes=32768' scripts/test-ha-managed-cluster-chaos.sh
grep -q '2>/dev/null | head -4' scripts/test-ha-managed-cluster-chaos.sh
grep -q 'summarize_coredns_log "$phase" "$pod" "$stream" "$retrieved" "$raw"' scripts/test-ha-managed-cluster-chaos.sh
kubectl_wrapper=$(sed -n '/^k() {/,/^}/p' scripts/test-ha-managed-cluster-chaos.sh)
probe_hook=$(sh -c 'set -eu; kubeconfig=unused; namespace=managed-bootstrap; kubectl() { :; }; run_floci_dns_probe() { printf "probe:%s\n" "$1"; }; eval "$1"; k -n "$namespace" rollout status deployment/floci --timeout=120s' fixture "$kubectl_wrapper")
[ "$probe_hook" = 'probe:after-floci-rollout' ]
probe_hook_code=0
sh -c 'set -eu; kubeconfig=unused; namespace=managed-bootstrap; kubectl() { return 7; }; run_floci_dns_probe() { echo unexpected-probe; }; eval "$1"; k -n "$namespace" rollout status deployment/floci --timeout=120s' fixture "$kubectl_wrapper" >/dev/null 2>&1 || probe_hook_code=$?
[ "$probe_hook_code" -eq 7 ]
if ! grep -q 'controlPlane\.coredns\.deployment\.nodeSelector' scripts/test-ha-managed-cluster-chaos.sh ||
   ! grep -q -- '--wait --timeout=5m' scripts/test-ha-managed-cluster-chaos.sh; then
  echo 'pinned CoreDNS deployment or bounded existing Helm readiness gate was removed' >&2; exit 1
fi
printf '%s\n' 'PASS: exact ready host mapping; unsafe mappings rejected; virtual DNS projection allowlisted and curl probe diagnostic-only'
