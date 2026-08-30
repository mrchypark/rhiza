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
	sql) namespace=rhiza-3peer-e2e; statefulset=rhiza-sql; container=rhiza-sql; port=18200; chaos=rhiza-preferred-peer-failure; chaos_file=e2e/chaos/preferred-peer-failure.yaml; failed_pod=rhiza-sql-0 ;;
	graph) namespace=rhiza-graph-3peer-e2e; statefulset=rhiza-graph; container=rhiza-graph; port=18210; chaos=rhiza-graph-one-peer-failure; chaos_file=e2e/chaos/graph-one-peer-failure.yaml; failed_pod=rhiza-graph-2 ;;
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
sync_interval="${RHIZA_BENCH_SYNC_INTERVAL:-1m}"
chaos_duration="${RHIZA_BENCH_CHAOS_DURATION:-2m}"

dory k8s set image "statefulset/$statefulset" -n "$namespace" "$container=$image" >/dev/null
dory k8s set env "statefulset/$statefulset" -n "$namespace" \
	"RHIZA_OBJSTORE_DURABILITY=$mode" \
	"RHIZA_OBJSTORE_PREFIX=bench-$label-$run_nonce" \
	"RHIZA_CHECKPOINT_INTERVAL=$checkpoint_interval" \
	"RHIZA_OBJSTORE_SYNC_INTERVAL=$sync_interval" >/dev/null
pods=("$statefulset-0" "$statefulset-1" "$statefulset-2")
measured_pods=("${pods[@]}")
if [[ "$failure" == one-fault ]]; then
	if [[ "$profile" == sql ]]; then
		measured_pods=("${pods[1]}" "${pods[2]}")
	else
		measured_pods=("${pods[0]}" "${pods[1]}")
	fi
fi
dory k8s delete pod -n "$namespace" "${pods[@]}" --ignore-not-found --wait=true >/dev/null
for pod in "${pods[@]}"; do
	until dory k8s get "pod/$pod" -n "$namespace" >/dev/null 2>&1; do sleep 0.5; done
done
dory k8s wait "pod/${pods[0]}" "pod/${pods[1]}" "pod/${pods[2]}" \
	-n "$namespace" --for=condition=Ready --timeout=180s >/dev/null

pod="${pods[1]}"
dory k8s port-forward "pod/$pod" -n "$namespace" "$port:8080" >"/tmp/$label-port-forward.log" 2>&1 &
port_forward_pid=$!
chaos_active=false
cleanup() {
	if [[ "$chaos_active" == true ]]; then
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
wait_for_quorum() {
	for _ in {1..120}; do
		if curl -fsS -H 'Content-Type: application/json' \
			-d '{"key":"__rhiza_bench_probe__","consistency":"linearizable"}' \
			"http://127.0.0.1:$port/kv/get" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.5
	done
	return 1
}
object_snapshot_for() {
	local metrics_pod stats
	for metrics_pod in "$@"; do
		stats="$(dory k8s exec -n "$namespace" "$metrics_pod" -- wget -qO- http://127.0.0.1:8080/metrics/object-store 2>/dev/null || echo '{}')"
		jq -nc --arg pod "$metrics_pod" --argjson stats "$stats" '{pod:$pod,stats:$stats}'
	done | jq -s .
}
object_snapshot() {
	object_snapshot_for "${measured_pods[@]}"
}
object_delta() {
	jq -nc --argjson before "$1" --argjson after "$2" \
		'def bypod: reduce .[] as $item ({}; .[$item.pod] = $item.stats);
		 ($before|bypod) as $b | ($after|bypod) as $a |
		 reduce (([($b|keys_unsorted[])]+[($a|keys_unsorted[])])|unique[]) as $pod
		   ({};
		    reduce (([($b[$pod]//{}|keys_unsorted[])]+[($a[$pod]//{}|keys_unsorted[])])|unique[]) as $key
		      (.; ($b[$pod][$key]//0) as $bv | ($a[$pod][$key]//0) as $av |
		       .[$key] = ((.[$key]//0) + (if $av >= $bv then $av-$bv else $av end))))'
}
wait_for_failed_peer() {
	local probe_pod="${measured_pods[0]}"
	local host="$failed_pod.$statefulset-headless.$namespace.svc.cluster.local"
	for _ in {1..60}; do
		if ! dory k8s exec -n "$namespace" "$probe_pod" -- wget -qO- -T 1 "http://$host:8080/healthz" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.2
	done
	return 1
}
wait_for_object_idle() {
	local before after delta
	before="$(object_snapshot)"
	for _ in {1..20}; do
		sleep 0.5
		after="$(object_snapshot)"
		delta="$(object_delta "$before" "$after")"
		if jq -e 'all(.[]; . == 0)' <<<"$delta" >/dev/null; then
			return 0
		fi
		before="$after"
	done
	return 1
}
wait_for_quorum
if [[ "$failure" == one-fault ]]; then
	fault_before="$(object_snapshot_for "${pods[@]}")"
	fault_survivor_before="$(object_snapshot)"
	dory k8s create -f "$chaos_file" --dry-run=client -o json \
		| jq --arg duration "$chaos_duration" '.spec.duration = $duration' \
		| dory k8s apply -f - >/dev/null
	chaos_active=true
	dory k8s wait "podchaos/$chaos" -n "$namespace" --for=condition=AllInjected=True --timeout=60s >/dev/null
	failure_started=$SECONDS
	failure_started_ns="$(date +%s%N)"
	wait_for_failed_peer
	wait_for_quorum
	echo "$((SECONDS - failure_started))" >"$output_dir/resources/$label-post-fault-quorum-seconds.txt"
	echo "$(( $(date +%s%N) - failure_started_ns ))" >"$output_dir/resources/$label-post-fault-quorum-ns.txt"
	if [[ "$checkpoint_interval" == 0 ]]; then
		wait_for_object_idle
	fi
fi

top_snapshot() {
	local suffix="$1"
	dory k8s top pods -n "$namespace" >"$output_dir/resources/$label-$suffix-top.txt" 2>&1 || true
}
cgroup_snapshot() {
	local suffix="$1" resource_pod
	for resource_pod in "${measured_pods[@]}"; do
		dory k8s exec -n "$namespace" "$resource_pod" -- sh -c \
			'cat /sys/fs/cgroup/cpu.stat; cat /sys/fs/cgroup/memory.current; cat /sys/fs/cgroup/memory.peak' \
			>"$output_dir/resources/$label-$resource_pod-$suffix-cgroup.txt" 2>/dev/null || true
	done
}
capture() {
	local workload="$1" endpoint_path="$2" body="$3" count="$4" concurrency="$5"
	local before after measured delta
	before="$(object_snapshot)"
	measured="$(/tmp/rhiza-bench-client -url "http://127.0.0.1:$port" -path "$endpoint_path" \
		-body "$body" -n "$count" -c "$concurrency")"
	after="$(object_snapshot)"
	delta="$(object_delta "$before" "$after")"
	jq -nc --arg config "$label" --arg workload "$workload" \
		--argjson result "$measured" --argjson delta "$delta" \
		'{config:$config,workload:$workload,result:$result,object_delta:$delta}' \
		| tee -a "$results"
}
capture_resource() {
	local workload="$1" endpoint_path="$2" body="$3" count="$4" concurrency="$5"
	local measured
	measured="$(/tmp/rhiza-bench-client -url "http://127.0.0.1:$port" -path "$endpoint_path" \
		-body "$body" -n "$count" -c "$concurrency")"
	jq -nc --arg config "$label" --arg workload "$workload" --argjson result "$measured" \
		'{config:$config,workload:$workload,result:$result}' >>"$resource_results"
}

warmup() {
	local endpoint body linearizable_body
	if [[ "$profile" == sql ]]; then
		endpoint=/sql/query
		body='{"sql":"SELECT 1","consistency":"local"}'
	else
		endpoint=/graph/query
		body='{"cypher":"MATCH (n) RETURN n LIMIT 1","consistency":"local"}'
	fi
	/tmp/rhiza-bench-client -url "http://127.0.0.1:$port" -path "$endpoint" -body "$body" -n 200 -c 16 | jq -e '.errors == 0' >/dev/null
	linearizable_body="${body/\"local\"/\"linearizable\"}"
	/tmp/rhiza-bench-client -url "http://127.0.0.1:$port" -path "$endpoint" -body "$linearizable_body" -n 200 -c 16 | jq -e '.errors == 0' >/dev/null
	/tmp/rhiza-bench-client -url "http://127.0.0.1:$port" -path /kv/get \
		-body "{\"key\":\"seed-$suffix\",\"consistency\":\"local\"}" -n 200 -c 16 | jq -e '.errors == 0' >/dev/null
	/tmp/rhiza-bench-client -url "http://127.0.0.1:$port" -path /kv/get \
		-body "{\"key\":\"seed-$suffix\",\"consistency\":\"linearizable\"}" -n 200 -c 16 | jq -e '.errors == 0' >/dev/null
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
warmup
if [[ "$checkpoint_interval" == 0 ]]; then
	wait_for_object_idle
fi

run_suite() {
	local capture_fn="$1" request_run="$2" repetition
	for repetition in 1 2 3; do
		if [[ "$profile" == sql ]]; then
			"$capture_fn" sql_read_local /sql/query '{"sql":"SELECT 1","consistency":"local"}' 1000 16
			"$capture_fn" sql_read_linearizable /sql/query '{"sql":"SELECT 1","consistency":"linearizable"}' 1000 16
			"$capture_fn" sql_write /sql/execute \
				"{\"request_id\":\"sql-$request_run-$suffix-$repetition-{{id}}\",\"sql\":\"INSERT INTO bench_$suffix(value) VALUES (1)\"}" 100 16
		else
			"$capture_fn" graph_read_local /graph/query \
				'{"cypher":"MATCH (n) RETURN n LIMIT 1","consistency":"local"}' 1000 16
			"$capture_fn" graph_read_linearizable /graph/query \
				'{"cypher":"MATCH (n) RETURN n LIMIT 1","consistency":"linearizable"}' 1000 16
			"$capture_fn" graph_write /graph/execute \
				"{\"request_id\":\"graph-$request_run-$suffix-$repetition-{{id}}\",\"cypher\":\"CREATE (:BenchmarkNode {id: '$request_run-$suffix-$repetition-{{id}}'})\"}" 100 16
		fi
		"$capture_fn" kv_read_local /kv/get \
			"{\"key\":\"seed-$suffix\",\"consistency\":\"local\"}" 1000 16
		"$capture_fn" kv_read_linearizable /kv/get \
			"{\"key\":\"seed-$suffix\",\"consistency\":\"linearizable\"}" 1000 16
		"$capture_fn" kv_write /kv/put \
			"{\"request_id\":\"kv-$request_run-$suffix-$repetition-{{id}}\",\"key\":\"key-$request_run-$suffix-$repetition-{{id}}\",\"value\":\"dg==\"}" 100 16
	done
}

run_suite capture object
if [[ "$checkpoint_interval" == 0 ]]; then
	wait_for_object_idle
fi
resource_results="$output_dir/resources/$label-resource.ndjson"
: >"$resource_results"
top_snapshot before
cgroup_snapshot before
date -u +%s >"$output_dir/resources/$label-started-epoch.txt"
date +%s%N >"$output_dir/resources/$label-started-ns.txt"
run_suite capture_resource resource
date +%s%N >"$output_dir/resources/$label-finished-ns.txt"
date -u +%s >"$output_dir/resources/$label-finished-epoch.txt"
cgroup_snapshot after
top_snapshot after

if [[ "$failure" == one-fault ]]; then
	fault_steady_after="$(object_snapshot)"
	object_delta "$fault_survivor_before" "$fault_steady_after" >"$output_dir/resources/$label-fault-survivor-object-delta.json"
	recovery_started_ns="$(date +%s%N)"
	dory k8s delete "podchaos/$chaos" -n "$namespace" --ignore-not-found --wait=true >/dev/null
	chaos_active=false
	dory k8s wait "pod/${pods[0]}" "pod/${pods[1]}" "pod/${pods[2]}" \
		-n "$namespace" --for=condition=Ready --timeout=180s >/dev/null
	wait_for_quorum
	recovery_after="$(object_snapshot_for "${pods[@]}")"
	object_delta "$fault_before" "$recovery_after" >"$output_dir/resources/$label-fault-recovery-object-delta.json"
	echo "$(( $(date +%s%N) - recovery_started_ns ))" >"$output_dir/resources/$label-recovery-ns.txt"
fi
