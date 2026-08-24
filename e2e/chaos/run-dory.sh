#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
port="${RHIZA_E2E_PORT:-18086}"
base_url="http://127.0.0.1:${port}"
chaos="rhiza-sql-container-kill"
pf_log="$(mktemp -t rhiza-chaos.XXXXXX)"
pf_pid=""

cleanup() {
  dory k8s delete podchaos "$chaos" -n rhiza-e2e --ignore-not-found --wait=true >/dev/null
  if [[ -n "$pf_pid" ]]; then
    kill "$pf_pid" 2>/dev/null || true
    wait "$pf_pid" 2>/dev/null || true
  fi
  rm -f "$pf_log"
}
trap cleanup EXIT

dory k8s get crd podchaos.chaos-mesh.org >/dev/null
dory k8s wait deployment/rhiza-sql -n rhiza-e2e --for=condition=Available --timeout=120s
dory k8s port-forward service/rhiza-sql -n rhiza-e2e "${port}:8080" >"$pf_log" 2>&1 &
pf_pid=$!

deadline=$((SECONDS + 30))
until curl -fsS "$base_url/ready" >/dev/null 2>&1; do
  if (( SECONDS >= deadline )); then
    cat "$pf_log" >&2
    exit 1
  fi
  sleep 0.5
done

suffix="$(date +%s)"
table="chaos_e2e_${suffix}"
curl -fsS -H 'Content-Type: application/json' -d \
  "{\"request_id\":\"chaos-schema-${suffix}\",\"sql\":\"CREATE TABLE ${table} (id INTEGER PRIMARY KEY, value TEXT NOT NULL)\"}" \
  "$base_url/v1/sql/execute" >/dev/null
curl -fsS -H 'Content-Type: application/json' -d \
  "{\"request_id\":\"chaos-seed-${suffix}\",\"sql\":\"INSERT INTO ${table} VALUES (1, 'survived')\"}" \
  "$base_url/v1/sql/execute" >/dev/null

pod="$(dory k8s get pods -n rhiza-e2e -l app=rhiza-sql -o jsonpath='{.items[0].metadata.name}')"
restart_before="$(dory k8s get pod "$pod" -n rhiza-e2e -o jsonpath='{.status.containerStatuses[?(@.name=="rhiza-sql")].restartCount}')"
started=$SECONDS
dory k8s apply -f "$script_dir/container-kill.yaml"
dory k8s wait podchaos/"$chaos" -n rhiza-e2e --for=condition=Selected=True --timeout=60s
dory k8s wait podchaos/"$chaos" -n rhiza-e2e --for=condition=AllInjected=True --timeout=60s

deadline=$((SECONDS + 120))
while true; do
  restart_after="$(dory k8s get pod "$pod" -n rhiza-e2e -o jsonpath='{.status.containerStatuses[?(@.name=="rhiza-sql")].restartCount}' 2>/dev/null || true)"
  ready="$(dory k8s get pod "$pod" -n rhiza-e2e -o jsonpath='{.status.containerStatuses[?(@.name=="rhiza-sql")].ready}' 2>/dev/null || true)"
  if [[ "$restart_after" =~ ^[0-9]+$ ]] && (( restart_after > restart_before )) && [[ "$ready" == "true" ]] && curl -fsS "$base_url/ready" >/dev/null 2>&1; then
    break
  fi
  if (( SECONDS >= deadline )); then
    dory k8s describe podchaos "$chaos" -n rhiza-e2e >&2 || true
    dory k8s describe pod "$pod" -n rhiza-e2e >&2 || true
    exit 1
  fi
  sleep 1
done

curl -fsS -H 'Content-Type: application/json' -d \
  "{\"request_id\":\"chaos-after-${suffix}\",\"sql\":\"INSERT INTO ${table} VALUES (2, 'after-recovery')\"}" \
  "$base_url/v1/sql/execute" >/dev/null
result="$(curl -fsS -H 'Content-Type: application/json' -d \
  "{\"sql\":\"SELECT value FROM ${table} ORDER BY id\"}" \
  "$base_url/v1/sql/query")"
[[ "$result" == *'survived'* ]]
[[ "$result" == *'after-recovery'* ]]

printf 'PASS: injected=true restart=%s->%s recovery=%ss data=survived write=after-recovery\n' \
  "$restart_before" "$restart_after" "$((SECONDS - started))"
