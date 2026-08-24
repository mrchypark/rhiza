#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
base_port="${RHIZA_GRAPH_E2E_PORT:-18100}"
one_failure="rhiza-graph-one-peer-failure"
two_failures="rhiza-graph-two-peer-failure"
tmp_dir="$(mktemp -d -t rhiza-graph-3peer.XXXXXX)"
pf_pids=()

cleanup() {
  dory k8s delete podchaos "$one_failure" "$two_failures" -n rhiza-graph-3peer-e2e --ignore-not-found --wait=true >/dev/null
  for pid in "${pf_pids[@]}"; do
    (( pid > 0 )) || continue
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

for peer in 0 1 2; do
  dory k8s port-forward "pod/rhiza-graph-${peer}" -n rhiza-graph-3peer-e2e "$((base_port + peer)):8080" >"$tmp_dir/port-forward-${peer}.log" 2>&1 &
  pf_pids+=("$!")
done

for peer in 0 1 2; do
  url="http://127.0.0.1:$((base_port + peer))"
  deadline=$((SECONDS + 30))
  until curl -fsS "$url/ready" >/dev/null 2>&1; do
    (( SECONDS < deadline )) || exit 1
    sleep 0.5
  done
done

suffix="$(date +%s)"
table="ChaosNode_${suffix}"
node0="http://127.0.0.1:${base_port}"
node1="http://127.0.0.1:$((base_port + 1))"
node2="http://127.0.0.1:$((base_port + 2))"
post() {
  curl -fsS --max-time 25 -H 'Content-Type: application/json' -d "$2" "$1" >/dev/null
}
wait_for_pod() {
  local pod="$1" deadline=$((SECONDS + 60))
  until dory k8s get pod "$pod" -n rhiza-graph-3peer-e2e >/dev/null 2>&1; do
    (( SECONDS < deadline )) || return 1
    sleep 0.5
  done
}
post "$node0/v1/graph/execute" "{\"request_id\":\"schema-${suffix}\",\"cypher\":\"CREATE NODE TABLE ${table}(id STRING, value STRING, PRIMARY KEY(id))\"}"
post "$node1/v1/graph/execute" "{\"request_id\":\"seed-${suffix}\",\"cypher\":\"CREATE (:${table} {id: 'before', value: 'fault'})\"}"

dory k8s apply -f "$script_dir/graph-one-peer-failure.yaml"
dory k8s wait podchaos/"$one_failure" -n rhiza-graph-3peer-e2e --for=condition=AllInjected=True --timeout=60s
write_seconds="$(curl -fsS --max-time 25 -o /dev/null -w '%{time_total}' -H 'Content-Type: application/json' -d \
  "{\"request_id\":\"during-${suffix}\",\"cypher\":\"CREATE (:${table} {id: 'during', value: 'fault'})\"}" \
  "$node1/v1/graph/execute")"
dory k8s wait podchaos/"$one_failure" -n rhiza-graph-3peer-e2e --for=condition=AllRecovered=True --timeout=180s
dory k8s delete pod rhiza-graph-2 -n rhiza-graph-3peer-e2e --wait=true >/dev/null
wait_for_pod rhiza-graph-2
dory k8s wait pod/rhiza-graph-2 -n rhiza-graph-3peer-e2e --for=condition=Ready --timeout=180s
kill "${pf_pids[2]}" 2>/dev/null || true
wait "${pf_pids[2]}" 2>/dev/null || true
pf_pids[2]=0
deadline=$((SECONDS + 90))
while true; do
  dory k8s port-forward pod/rhiza-graph-2 -n rhiza-graph-3peer-e2e "$((base_port + 2)):8080" >"$tmp_dir/port-forward-2.log" 2>&1 &
	pf_pids[2]="$!"
  sleep 0.5
  if result="$(curl -fsS --max-time 5 -H 'Content-Type: application/json' -d \
    "{\"cypher\":\"MATCH (n:${table}) RETURN n.id\",\"consistency\":\"local\"}" \
    "$node2/v1/graph/query" 2>/dev/null)" && [[ "$result" == *during* ]]; then
    break
  fi
  kill "${pf_pids[2]}" 2>/dev/null || true
  wait "${pf_pids[2]}" 2>/dev/null || true
  pf_pids[2]=0
  (( SECONDS < deadline )) || exit 1
  sleep 0.5
done

deadline=$((SECONDS + 30))
until curl -fsS "$node1/healthz" >/dev/null 2>&1; do
  (( SECONDS < deadline )) || exit 1
  sleep 0.5
done
dory k8s apply -f "$script_dir/graph-two-peer-failure.yaml"
dory k8s wait podchaos/"$two_failures" -n rhiza-graph-3peer-e2e --for=condition=AllInjected=True --timeout=60s
deadline=$((SECONDS + 10))
while curl -fsS --max-time 1 "$node0/ready" >/dev/null 2>&1 || \
  curl -fsS --max-time 1 "$node2/ready" >/dev/null 2>&1; do
  (( SECONDS < deadline )) || exit 1
  sleep 0.25
done
status=000
deadline=$((SECONDS + 15))
while [[ "$status" == 000 ]]; do
  status="$(curl -sS --max-time 6 -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' -d \
    "{\"request_id\":\"no-quorum-${suffix}\",\"cypher\":\"CREATE (:${table} {id: 'rejected', value: 'fault'})\"}" \
    "$node1/v1/graph/execute" 2>/dev/null || true)"
  (( SECONDS < deadline )) || break
  [[ "$status" != 000 ]] || sleep 0.25
done
if [[ "$status" != 503 ]]; then
  echo "FAIL: graph peers=3 failed=2 expected-status=503 actual-status=$status" >&2
  exit 1
fi
dory k8s wait podchaos/"$two_failures" -n rhiza-graph-3peer-e2e --for=condition=AllRecovered=True --timeout=90s
dory k8s delete pod rhiza-graph-0 rhiza-graph-2 -n rhiza-graph-3peer-e2e --wait=true >/dev/null
wait_for_pod rhiza-graph-0
wait_for_pod rhiza-graph-2
dory k8s wait pod/rhiza-graph-0 pod/rhiza-graph-1 pod/rhiza-graph-2 -n rhiza-graph-3peer-e2e --for=condition=Ready --timeout=180s
deadline=$((SECONDS + 90))
until curl -fsS "$node1/ready" >/dev/null 2>&1; do
  (( SECONDS < deadline )) || exit 1
  sleep 0.5
done
deadline=$((SECONDS + 30))
until post "$node1/v1/graph/execute" "{\"request_id\":\"after-${suffix}\",\"cypher\":\"CREATE (:${table} {id: 'after', value: 'fault'})\"}" 2>/dev/null; do
  (( SECONDS < deadline )) || exit 1
  sleep 0.25
done

printf 'PASS: graph peers=3 failed=1 quorum-write=%ss converged=true; failed=2 write-status=%s recovered-write=true\n' "$write_seconds" "$status"
