#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
base_port="${RHIZA_E2E_PORT:-18090}"
one_failure="rhiza-preferred-peer-failure"
two_failures="rhiza-two-peer-failure"
tmp_dir="$(mktemp -d -t rhiza-3peer.XXXXXX)"
pf_pids=()

for peer in 0 1 2; do
  port="$((base_port + peer))"
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN -t >/dev/null; then
    echo "local port ${port} is already in use; set RHIZA_E2E_PORT" >&2
    exit 1
  fi
done

cleanup() {
  dory k8s delete podchaos "$one_failure" "$two_failures" -n rhiza-3peer-e2e --ignore-not-found --wait=true >/dev/null
  for pid in "${pf_pids[@]}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

for peer in 0 1 2; do
  dory k8s port-forward "pod/rhiza-sql-${peer}" -n rhiza-3peer-e2e "$((base_port + peer)):8080" >"$tmp_dir/port-forward-${peer}.log" 2>&1 &
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
table="chaos_3peer_${suffix}"
node0="http://127.0.0.1:${base_port}"
node1="http://127.0.0.1:$((base_port + 1))"
wait_value() {
  local peer="$1" value="$2" deadline=$((SECONDS + 15)) result
  until result="$(curl -fsS -H 'Content-Type: application/json' -d \
    "{\"sql\":\"SELECT value FROM ${table} ORDER BY id\"}" \
    "http://127.0.0.1:$((base_port + peer))/sql/query" 2>/dev/null)" && [[ "$result" == *"$value"* ]]; do
    (( SECONDS < deadline )) || return 1
    sleep 0.1
  done
}
curl -fsS -H 'Content-Type: application/json' -d \
  "{\"request_id\":\"schema-${suffix}\",\"sql\":\"CREATE TABLE ${table} (id INTEGER PRIMARY KEY, value TEXT NOT NULL)\"}" \
  "$node0/sql/execute" >/dev/null
curl -fsS -H 'Content-Type: application/json' -d \
  "{\"request_id\":\"seed-${suffix}\",\"sql\":\"INSERT INTO ${table} VALUES (1, 'before-fault')\"}" \
  "$node1/sql/execute" >/dev/null

for peer in 0 1 2; do
  wait_value "$peer" before-fault
done

for peer in 0 1 2; do
  deadline=$((SECONDS + 60))
  until dory k8s logs "pod/rhiza-sql-${peer}" -n rhiza-3peer-e2e | grep -Fq 'checkpoint prepared: index='; do
    (( SECONDS < deadline )) || {
      echo "peer ${peer} did not durably prepare a checkpoint before restart" >&2
      exit 1
    }
    sleep 1
  done
done

restart_before="$(dory k8s get pod rhiza-sql-0 -n rhiza-3peer-e2e -o jsonpath='{.status.containerStatuses[0].restartCount}')"

dory k8s apply -f "$script_dir/preferred-peer-failure.yaml"
dory k8s wait podchaos/"$one_failure" -n rhiza-3peer-e2e --for=condition=Selected=True --timeout=60s
dory k8s wait podchaos/"$one_failure" -n rhiza-3peer-e2e --for=condition=AllInjected=True --timeout=60s
if curl -fsS --max-time 2 "$node0/ready" >/dev/null 2>&1; then
  echo "preferred peer remained reachable after fault injection" >&2
  exit 1
fi

write_seconds="$(curl -fsS --max-time 25 -o /dev/null -w '%{time_total}' -H 'Content-Type: application/json' -d \
  "{\"request_id\":\"during-${suffix}\",\"sql\":\"INSERT INTO ${table} VALUES (2, 'during-fault')\"}" \
  "$node1/sql/execute")"

for peer in 1 2; do
  wait_value "$peer" during-fault
done

dory k8s wait podchaos/"$one_failure" -n rhiza-3peer-e2e --for=condition=AllRecovered=True --timeout=180s
dory k8s delete podchaos/"$one_failure" -n rhiza-3peer-e2e --wait=true >/dev/null
deadline=$((SECONDS + 60))
while true; do
  restart_after="$(dory k8s get pod rhiza-sql-0 -n rhiza-3peer-e2e -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || true)"
  ready="$(dory k8s get pod rhiza-sql-0 -n rhiza-3peer-e2e -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || true)"
  if [[ "$restart_after" =~ ^[0-9]+$ ]] && (( restart_after > restart_before )) && [[ "$ready" == "true" ]]; then
    break
  fi
  (( SECONDS < deadline )) || exit 1
  sleep 1
done
kill "${pf_pids[0]}" 2>/dev/null || true
wait "${pf_pids[0]}" 2>/dev/null || true
dory k8s port-forward pod/rhiza-sql-0 -n rhiza-3peer-e2e "${base_port}:8080" >"$tmp_dir/port-forward-0.log" 2>&1 &
pf_pids[0]="$!"
deadline=$((SECONDS + 30))
until curl -fsS "$node0/ready" >/dev/null 2>&1; do
  if (( SECONDS >= deadline )); then
    cat "$tmp_dir/port-forward-0.log" >&2
    exit 1
  fi
  sleep 0.5
done
result="$(curl -fsS -H 'Content-Type: application/json' -d \
  "{\"sql\":\"SELECT value FROM ${table} ORDER BY id\"}" \
  "$node0/sql/query")"
if [[ "$result" != *'during-fault'* ]]; then
  echo "FAIL: peers=3 failed=1 quorum-write=${write_seconds}s recovered-node-converged=false" >&2
	exit 1
fi

old_uid="$(dory k8s get pod rhiza-sql-0 -n rhiza-3peer-e2e -o jsonpath='{.metadata.uid}')"
dory k8s delete pod rhiza-sql-0 -n rhiza-3peer-e2e --wait=true >/dev/null
deadline=$((SECONDS + 60))
while true; do
  new_uid="$(dory k8s get pod rhiza-sql-0 -n rhiza-3peer-e2e -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
  [[ -n "$new_uid" && "$new_uid" != "$old_uid" ]] && break
  (( SECONDS < deadline )) || exit 1
  sleep 0.5
done
dory k8s wait pod/rhiza-sql-0 -n rhiza-3peer-e2e --for=condition=Ready --timeout=90s
kill "${pf_pids[0]}" 2>/dev/null || true
wait "${pf_pids[0]}" 2>/dev/null || true
dory k8s port-forward pod/rhiza-sql-0 -n rhiza-3peer-e2e "${base_port}:8080" >"$tmp_dir/port-forward-0.log" 2>&1 &
pf_pids[0]="$!"
deadline=$((SECONDS + 30))
until result="$(curl -fsS -H 'Content-Type: application/json' -d \
  "{\"sql\":\"SELECT value FROM ${table} ORDER BY id\"}" \
  "$node0/sql/query" 2>/dev/null)"; do
  (( SECONDS < deadline )) || exit 1
  sleep 0.5
done
[[ "$result" == *'before-fault'* && "$result" == *'during-fault'* ]]

dory k8s apply -f "$script_dir/two-peer-failure.yaml"
dory k8s wait podchaos/"$two_failures" -n rhiza-3peer-e2e --for=condition=AllInjected=True --timeout=60s
deadline=$((SECONDS + 10))
while curl -fsS --max-time 1 "$node0/ready" >/dev/null 2>&1 || \
  curl -fsS --max-time 1 "http://127.0.0.1:$((base_port + 2))/ready" >/dev/null 2>&1; do
  (( SECONDS < deadline )) || exit 1
  sleep 0.25
done
unknown_response="$tmp_dir/no-quorum.json"
status="$(curl -sS --max-time 15 -o "$unknown_response" -w '%{http_code}' -H 'Content-Type: application/json' -d \
	"{\"request_id\":\"no-quorum-${suffix}\",\"sql\":\"INSERT INTO ${table} VALUES (3, 'uncertain-commit')\"}" \
	"$node1/sql/execute")"
if [[ "$status" != "503" ]]; then
  echo "FAIL: peers=3 failed=2 expected-status=503 actual-status=$status" >&2
  exit 1
fi
jq -e --arg request_id "no-quorum-${suffix}" \
  '.code == "commit_unknown" and .request_id == $request_id and .slot > 0 and .retry_through_slot >= .slot' \
  "$unknown_response" >/dev/null
dory k8s wait podchaos/"$two_failures" -n rhiza-3peer-e2e --for=condition=AllRecovered=True --timeout=90s
dory k8s delete podchaos/"$two_failures" -n rhiza-3peer-e2e --wait=true >/dev/null
dory k8s wait pod/rhiza-sql-0 pod/rhiza-sql-1 pod/rhiza-sql-2 -n rhiza-3peer-e2e --for=condition=Ready --timeout=180s
deadline=$((SECONDS + 60))
until curl -fsS "$node1/ready" >/dev/null 2>&1; do
  (( SECONDS < deadline )) || exit 1
  sleep 0.5
done
deadline=$((SECONDS + 30))
until curl -fsS --max-time 20 -o /dev/null -H 'Content-Type: application/json' -d \
  "{\"request_id\":\"no-quorum-${suffix}\",\"sql\":\"INSERT INTO ${table} VALUES (3, 'uncertain-commit')\"}" \
  "$node1/sql/execute" 2>/dev/null; do
  (( SECONDS < deadline )) || exit 1
  sleep 0.25
done
deadline=$((SECONDS + 30))
until curl -fsS --max-time 20 -o /dev/null -H 'Content-Type: application/json' -d \
  "{\"request_id\":\"after-quorum-${suffix}\",\"sql\":\"INSERT INTO ${table} VALUES (4, 'after-quorum')\"}" \
  "$node1/sql/execute" 2>/dev/null; do
  (( SECONDS < deadline )) || exit 1
  sleep 0.25
done
result="$(curl -fsS -H 'Content-Type: application/json' -d \
  "{\"sql\":\"SELECT value FROM ${table} WHERE id = 4\",\"consistency\":\"linearizable\"}" \
  "$node1/sql/query")"
[[ "$result" == *'after-quorum'* ]]

for peer in 0 1 2; do
  kill "${pf_pids[$peer]}" 2>/dev/null || true
  wait "${pf_pids[$peer]}" 2>/dev/null || true
done
for peer in 0 1 2; do
  pod="rhiza-sql-${peer}"
  old_uid="$(dory k8s get pod "$pod" -n rhiza-3peer-e2e -o jsonpath='{.metadata.uid}')"
  dory k8s delete pod "$pod" -n rhiza-3peer-e2e --wait=true >/dev/null
  deadline=$((SECONDS + 90))
  while true; do
    new_uid="$(dory k8s get pod "$pod" -n rhiza-3peer-e2e -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
    [[ -n "$new_uid" && "$new_uid" != "$old_uid" ]] && break
    (( SECONDS < deadline )) || exit 1
    sleep 0.5
  done
  dory k8s wait "pod/$pod" -n rhiza-3peer-e2e --for=condition=Ready --timeout=180s
  if ! dory k8s logs "pod/$pod" -n rhiza-3peer-e2e | grep -Fq 'checkpoint recovered: state='; then
    echo "peer ${peer} did not recover its locally verified checkpoint from PVC" >&2
    exit 1
  fi
done
for peer in 0 1 2; do
  dory k8s port-forward "pod/rhiza-sql-${peer}" -n rhiza-3peer-e2e "$((base_port + peer)):8080" >"$tmp_dir/port-forward-${peer}.log" 2>&1 &
  pf_pids[$peer]="$!"
done
deadline=$((SECONDS + 90))
until result="$(curl -fsS -H 'Content-Type: application/json' -d \
  "{\"sql\":\"SELECT id, value FROM ${table} ORDER BY id\",\"consistency\":\"linearizable\"}" \
  "$node0/sql/query" 2>/dev/null)"; do
  (( SECONDS < deadline )) || exit 1
  sleep 0.5
done
[[ "$result" == *'before-fault'* && "$result" == *'during-fault'* && "$result" == *'uncertain-commit'* && "$result" == *'after-quorum'* ]]
count="$(curl -fsS -H 'Content-Type: application/json' -d \
  "{\"sql\":\"SELECT COUNT(*) FROM ${table} WHERE id = 3\",\"consistency\":\"linearizable\"}" \
  "$node0/sql/query")"
jq -e '.rows == [[1]]' <<<"$count" >/dev/null

printf 'PASS: peers=3 failed=1 quorum-write=%ss converged=true rebuilt=true; failed=2 write-status=%s commit-unknown-resolved=true recovered-write=true; shared-object-recovery=true\n' "$write_seconds" "$status"
