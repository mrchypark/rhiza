#!/usr/bin/env bash
set -euo pipefail

if (( $# < 5 || $# > 6 )); then
	echo "usage: $0 <sql|graph> <image> <async|before-ack> <label> <output-dir> [healthy|one-fault]" >&2
	exit 2
fi

profile="$1"
image="$2"
mode="$3"
label="$4"
output_dir="$5"
failure="${6:-healthy}"
case "$profile" in
	sql) namespace=rhiza-3peer-e2e; statefulset=rhiza-sql; container=rhiza-sql; port=18200; chaos=rhiza-preferred-peer-failure; chaos_file=e2e/chaos/preferred-peer-failure.yaml ;;
	graph) namespace=rhiza-graph-3peer-e2e; statefulset=rhiza-graph; container=rhiza-graph; port=18210; chaos=rhiza-graph-one-peer-failure; chaos_file=e2e/chaos/graph-one-peer-failure.yaml ;;
	*) echo "profile must be sql or graph" >&2; exit 2 ;;
esac
[[ "$mode" == async || "$mode" == before-ack ]] || { echo "invalid durability mode" >&2; exit 2; }
[[ "$failure" == healthy || "$failure" == one-fault ]] || { echo "invalid failure mode" >&2; exit 2; }

mkdir -p "$output_dir/raw" "$output_dir/resources"
results="$output_dir/raw/$label.ndjson"
: >"$results"
go build -o /tmp/rhiza-bench-client ./cmd/rhiza-bench
run_nonce="$(date +%s%N)"
checkpoint_interval="${RHIZA_BENCH_CHECKPOINT_INTERVAL:-0}"

dory k8s set image "statefulset/$statefulset" -n "$namespace" "$container=$image" >/dev/null
dory k8s set env "statefulset/$statefulset" -n "$namespace" \
	"RHIZA_OBJSTORE_DURABILITY=$mode" \
	"RHIZA_OBJSTORE_PREFIX=bench-$label-$run_nonce" \
	"RHIZA_CHECKPOINT_INTERVAL=$checkpoint_interval" \
	RHIZA_OBJSTORE_SYNC_INTERVAL=1m >/dev/null
pods=("$statefulset-0" "$statefulset-1" "$statefulset-2")
dory k8s delete pod -n "$namespace" "${pods[@]}" --ignore-not-found --wait=true >/dev/null
for pod in "${pods[@]}"; do
	until dory k8s get "pod/$pod" -n "$namespace" >/dev/null 2>&1; do sleep 0.5; done
done
dory k8s wait "pod/${pods[0]}" "pod/${pods[1]}" "pod/${pods[2]}" \
	-n "$namespace" --for=condition=Ready --timeout=180s >/dev/null

pod="${pods[1]}"
dory k8s port-forward "pod/$pod" -n "$namespace" "$port:8080" >"/tmp/$label-port-forward.log" 2>&1 &
port_forward_pid=$!
cleanup() {
	if [[ "$failure" == one-fault ]]; then
		dory k8s delete "podchaos/$chaos" -n "$namespace" --ignore-not-found --wait=true >/dev/null
	fi
	kill "$port_forward_pid" 2>/dev/null || true
	wait "$port_forward_pid" 2>/dev/null || true
}
trap cleanup EXIT
for _ in {1..60}; do
	curl -fsS "http://127.0.0.1:$port/ready" >/dev/null 2>&1 && break
	sleep 0.5
done
curl -fsS "http://127.0.0.1:$port/ready" >/dev/null
if [[ "$failure" == one-fault ]]; then
	dory k8s apply -f "$chaos_file" >/dev/null
	dory k8s wait "podchaos/$chaos" -n "$namespace" --for=condition=AllInjected=True --timeout=60s >/dev/null
fi

resource_snapshot() {
	local suffix="$1"
	dory k8s top pods -n "$namespace" >"$output_dir/resources/$label-$suffix-top.txt" 2>&1 || true
	dory k8s exec -n "$namespace" "$pod" -- sh -c \
		'cat /sys/fs/cgroup/cpu.stat; cat /sys/fs/cgroup/memory.current; cat /sys/fs/cgroup/memory.peak' \
		>"$output_dir/resources/$label-$suffix-cgroup.txt"
}

capture() {
	local workload="$1" endpoint_path="$2" body="$3" count="$4" concurrency="$5"
	local before after measured
	before="$(curl -fsS "http://127.0.0.1:$port/metrics/object-store")"
	measured="$(/tmp/rhiza-bench-client -url "http://127.0.0.1:$port" -path "$endpoint_path" \
		-body "$body" -n "$count" -c "$concurrency")"
	after="$(curl -fsS "http://127.0.0.1:$port/metrics/object-store")"
	jq -nc --arg config "$label" --arg workload "$workload" \
		--argjson result "$measured" --argjson before "$before" --argjson after "$after" \
		'{config:$config,workload:$workload,result:$result,object_delta:($after|with_entries(.value -= $before[.key]))}' \
		| tee -a "$results"
}

suffix="$(date +%s%N)"
if [[ "$profile" == sql ]]; then
	curl -fsS -H 'Content-Type: application/json' \
		-d "{\"request_id\":\"schema-$suffix\",\"sql\":\"CREATE TABLE bench_$suffix (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)\"}" \
		"http://127.0.0.1:$port/sql/execute" >/dev/null
else
	curl -fsS -H 'Content-Type: application/json' \
		-d "{\"request_id\":\"seed-graph-$suffix\",\"cypher\":\"CREATE (:BenchmarkNode {id: 'seed-$suffix'})\"}" \
		"http://127.0.0.1:$port/graph/execute" >/dev/null
fi
curl -fsS -H 'Content-Type: application/json' \
	-d "{\"request_id\":\"seed-kv-$suffix\",\"key\":\"seed-$suffix\",\"value\":\"dg==\"}" \
	"http://127.0.0.1:$port/kv/put" >/dev/null

date -u +%s >"$output_dir/resources/$label-started-epoch.txt"
resource_snapshot before
for repetition in 1 2 3; do
	if [[ "$profile" == sql ]]; then
		capture sql_read_local /sql/query '{"sql":"SELECT 1","consistency":"local"}' 1000 16
		capture sql_read_linearizable /sql/query '{"sql":"SELECT 1","consistency":"linearizable"}' 1000 16
		capture sql_write /sql/execute \
			"{\"request_id\":\"sql-$suffix-$repetition-{{id}}\",\"sql\":\"INSERT INTO bench_$suffix(value) VALUES (1)\"}" 100 16
	else
		capture graph_read_local /graph/query \
			'{"cypher":"MATCH (n) RETURN n LIMIT 1","consistency":"local"}' 1000 16
		capture graph_read_linearizable /graph/query \
			'{"cypher":"MATCH (n) RETURN n LIMIT 1","consistency":"linearizable"}' 1000 16
		capture graph_write /graph/execute \
			"{\"request_id\":\"graph-$suffix-$repetition-{{id}}\",\"cypher\":\"CREATE (:BenchmarkNode {id: '$suffix-$repetition-{{id}}'})\"}" 100 16
	fi
	capture kv_read_local /kv/get \
		"{\"key\":\"seed-$suffix\",\"consistency\":\"local\"}" 1000 16
	capture kv_read_linearizable /kv/get \
		"{\"key\":\"seed-$suffix\",\"consistency\":\"linearizable\"}" 1000 16
	capture kv_write /kv/put \
		"{\"request_id\":\"kv-$suffix-$repetition-{{id}}\",\"key\":\"key-$suffix-$repetition-{{id}}\",\"value\":\"dg==\"}" 100 16
done
resource_snapshot after
date -u +%s >"$output_dir/resources/$label-finished-epoch.txt"
