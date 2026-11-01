#!/bin/sh
# Offline replay of the production virtual-to-host identity guard. No API calls.
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
for source in scripts/test-ha-managed-bootstrap.sh scripts/ha-managed-bootstrap-fixture.sh scripts/test-ha-managed-ied-chaos.sh; do sh -n "$source"; done
helper=$(sed -n '/^mapped_host_pod() {/,/^}/p' scripts/test-ha-managed-ied-chaos.sh)
[ -n "$helper" ]
scratch=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-ied-mapping.XXXXXX")
cleanup() {
  unlink "$scratch/test-virtual-before.json" 2>/dev/null || :
  unlink "$scratch/test-host-before.json" 2>/dev/null || :
  unlink "$scratch/fault.json" 2>/dev/null || :
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
oracle=$(sed -n '/^chaos_record_oracle() {/,/^}/p' scripts/test-ha-managed-ied-chaos.sh)
[ -n "$oracle" ]
for target in test-ns/host-1 test-ns/host-1/sql; do
  jq -n --arg id "$target" '{status:{conditions:[{type:"AllInjected",status:"True"}],experiment:{containerRecords:[{id:$id,injectedCount:1}]}}}' > "$scratch/fault.json"
  sh -c 'set -eu; eval "$1"; chaos_record_oracle "$2" "$3"' fixture "$oracle" "$scratch/fault.json" "$target"
  code=0
  sh -c 'set -eu; eval "$1"; chaos_record_oracle "$2" "$3"' fixture "$oracle" "$scratch/fault.json" wrong-ns/other-pod || code=$?
  [ "$code" -ne 0 ] || { echo 'wrong Chaos target accepted' >&2; exit 1; }
done
[ "$(grep -c 'set-json "sync.fromHost.nodes.selector.labels=' scripts/test-ha-managed-ied-chaos.sh)" -eq 2 ]
if grep -q 'podTemplate.spec.nodeSelector' scripts/test-ha-managed-ied-chaos.sh; then echo 'unsupported RhizaCluster node pin' >&2; exit 1; fi
printf '%s\n' 'PASS: exact ready host mapping; duplicate, wrong virtual UID, unpinned node and unready Pod rejected'
