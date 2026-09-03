#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 2 ]]; then
	printf 'usage: %s SOURCE_DIR OUTPUT_FILE\n' "$0" >&2
	exit 2
fi

source_dir=$(cd "$1" && pwd)
output_file=$2
requests=${RHIZA_SERVER_BENCH_REQUESTS:-100000}
concurrency=${RHIZA_SERVER_BENCH_CONCURRENCY:-16}
failed_node=${RHIZA_SERVER_BENCH_FAILED_NODE:-none}
target_node=${RHIZA_SERVER_BENCH_TARGET_NODE:-n1}
fault_after=${RHIZA_SERVER_BENCH_FAULT_AFTER:-1}
hedge_delay=${RHIZA_SERVER_BENCH_HEDGE_DELAY:-100ms}
base_http_port=${RHIZA_SERVER_BENCH_HTTP_PORT:-18100}
base_peer_port=${RHIZA_SERVER_BENCH_PEER_PORT:-19100}
minio_port=${RHIZA_SERVER_BENCH_MINIO_PORT:-19000}
if [[ ! $requests =~ ^[1-9][0-9]*$ || ! $concurrency =~ ^[1-9][0-9]*$ || $concurrency -gt $requests ]]; then
	printf 'request count and concurrency must be positive, with concurrency <= requests\n' >&2
	exit 2
fi
if [[ $failed_node != none && $failed_node != n0 && $failed_node != n1 && $failed_node != n2 ]]; then
	printf 'RHIZA_SERVER_BENCH_FAILED_NODE must be none, n0, n1, or n2\n' >&2
	exit 2
fi
if [[ $target_node != n0 && $target_node != n1 && $target_node != n2 ]]; then
	printf 'RHIZA_SERVER_BENCH_TARGET_NODE must be n0, n1, or n2\n' >&2
	exit 2
fi
if [[ $failed_node == "$target_node" ]]; then
	printf 'failed node and client target must differ\n' >&2
	exit 2
fi
if [[ ! $fault_after =~ ^[0-9]+([.][0-9]+)?$ ]]; then
	printf 'RHIZA_SERVER_BENCH_FAULT_AFTER must be seconds expressed as a non-negative number\n' >&2
	exit 2
fi

run_dir=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-server-bench.XXXXXX")
container="rhiza-bench-minio-$$"
cleanup() {
	output_stem=$(basename "$output_file" .json)
	for log in "$run_dir"/node-*.log; do
		[[ -f $log ]] || continue
		cp "$log" "$(dirname "$output_file")/$output_stem-$(basename "$log")"
	done
	for pid_file in "$run_dir"/node-*.pid; do
		[[ -f $pid_file ]] || continue
		pid=$(<"$pid_file")
		kill "$pid" 2>/dev/null || true
		wait "$pid" 2>/dev/null || true
	done
	docker rm -f "$container" >/dev/null 2>&1 || true
	case "$run_dir" in
		"${TMPDIR:-/tmp}"/rhiza-server-bench.*) rm -rf -- "$run_dir" ;;
	esac
}
trap cleanup EXIT

(cd "$source_dir" && CGO_ENABLED=0 go build -o "$run_dir/rhiza" ./cmd/rhiza)
(cd "$source_dir" && CGO_ENABLED=0 go build -o "$run_dir/rhiza-bench" ./cmd/rhiza-bench)

docker run --rm -d --name "$container" -p "$minio_port:9000" \
	-e MINIO_ROOT_USER=rhiza-e2e -e MINIO_ROOT_PASSWORD=rhiza-e2e-secret \
	minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e server /data >/dev/null
for _ in {1..100}; do
	curl -fsS "http://127.0.0.1:$minio_port/minio/health/ready" >/dev/null 2>&1 && break
	sleep 0.1
done
curl -fsS "http://127.0.0.1:$minio_port/minio/health/ready" >/dev/null
docker run --rm --network host --entrypoint /bin/sh \
	minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727 \
	-c "mc alias set local http://127.0.0.1:$minio_port rhiza-e2e rhiza-e2e-secret >/dev/null && mc mb --ignore-existing local/rhiza >/dev/null"

members=$(printf '[{"node_id":"n0","url":"http://127.0.0.1:%d","peer_url":"quic://127.0.0.1:%d","token":"n0-token"},{"node_id":"n1","url":"http://127.0.0.1:%d","peer_url":"quic://127.0.0.1:%d","token":"n1-token"},{"node_id":"n2","url":"http://127.0.0.1:%d","peer_url":"quic://127.0.0.1:%d","token":"n2-token"}]' \
	"$base_http_port" "$base_peer_port" "$((base_http_port + 1))" "$((base_peer_port + 1))" "$((base_http_port + 2))" "$((base_peer_port + 2))")
for i in 0 1 2; do
	env RHIZA_CLUSTER_ID=server-bench RHIZA_NODE_ID="n$i" \
		RHIZA_BIND_ADDR="127.0.0.1:$((base_http_port + i))" RHIZA_PEER_ADDR="127.0.0.1:$((base_peer_port + i))" \
		RHIZA_DATA_DIR="$run_dir/node-$i" RHIZA_CLUSTER_MEMBERS="$members" \
		RHIZA_OBJSTORE_PROVIDER=s3 RHIZA_OBJSTORE_ENDPOINT="127.0.0.1:$minio_port" RHIZA_OBJSTORE_BUCKET=rhiza \
		RHIZA_OBJSTORE_PREFIX=server-bench RHIZA_OBJSTORE_REGION=us-east-1 RHIZA_OBJSTORE_INSECURE=true \
		RHIZA_OBJSTORE_ACCESS_KEY=rhiza-e2e RHIZA_OBJSTORE_SECRET_KEY=rhiza-e2e-secret \
		RHIZA_OBJSTORE_DURABILITY=async RHIZA_OBJSTORE_SYNC_INTERVAL=1h RHIZA_CHECKPOINT_INTERVAL=0 \
		RHIZA_HEDGE_DELAY="$hedge_delay" "$run_dir/rhiza" >"$run_dir/node-$i.log" 2>&1 &
	printf '%s' "$!" >"$run_dir/node-$i.pid"
done
for i in 0 1 2; do
	port=$((base_http_port + i))
	for _ in {1..300}; do
		curl -fsS "http://127.0.0.1:$port/ready" >/dev/null 2>&1 && break
		sleep 0.1
	done
	if ! curl -fsS "http://127.0.0.1:$port/ready" >/dev/null; then
		sed -n '1,200p' "$run_dir/node-$i.log" >&2
		exit 1
	fi
done

target_index=${target_node#n}
target="http://127.0.0.1:$((base_http_port + target_index))"
curl -fsS -H 'Content-Type: application/json' \
	-d '{"request_id":"schema","sql":"CREATE TABLE benchmark_writes (id INTEGER PRIMARY KEY)"}' \
	"$target/sql/execute" >/dev/null
result_file="$run_dir/result.json"
"$run_dir/rhiza-bench" -url "$target" -path /sql/execute \
	-body '{"request_id":"bench-{{id}}","sql":"INSERT INTO benchmark_writes(id) VALUES ({{id}})"}' \
	-n "$requests" -c "$concurrency" -commit-unknown-retries 3 >"$result_file" &
bench_pid=$!
if [[ $failed_node != none ]]; then
	sleep "$fault_after"
	if ! kill -0 "$bench_pid" 2>/dev/null; then
		printf 'benchmark completed before fault injection; increase requests or reduce RHIZA_SERVER_BENCH_FAULT_AFTER\n' >&2
		exit 1
	fi
	failed_index=${failed_node#n}
	failed_pid=$(<"$run_dir/node-$failed_index.pid")
	kill -KILL "$failed_pid"
	wait "$failed_pid" 2>/dev/null || true
fi
wait "$bench_pid"
result=$(<"$result_file")
# Preserve the client result even when the subsequent correctness gate fails.
tee "$output_file" <<<"$result" >/dev/null
count=$(curl -fsS -H 'Content-Type: application/json' \
	-d '{"sql":"SELECT COUNT(*) FROM benchmark_writes WHERE id >= 0","consistency":"linearizable"}' \
	"$target/sql/query")
jq -e --argjson requests "$requests" '.errors == 0 and .successes == $requests' <<<"$result" >/dev/null
jq -e --argjson requests "$requests" '.rows == [[$requests]]' <<<"$count" >/dev/null
runtime_failure_lines=0
for log in "$run_dir"/node-*.log; do
	log_failures=$(grep -Ei 'error|failed|timeout' "$log" | grep -Eivc 'failed to sufficiently increase receive buffer size' || true)
	runtime_failure_lines=$((runtime_failure_lines + log_failures))
done
jq -nc --argjson result "$result" --argjson count "$count" --argjson concurrency "$concurrency" --argjson runtime_failure_lines "$runtime_failure_lines" --arg failed_node "$failed_node" --arg target_node "$target_node" --arg fault_after "$fault_after" --arg hedge_delay "$hedge_delay" \
	'{transport:"HTTP client + three QUIC voters",durability:"quorum WAL sync",failure_mode:(if $failed_node == "none" then "healthy" else "peer-sigkill-during-load" end),failed_node:$failed_node,target_node:$target_node,fault_after:(if $failed_node == "none" then null else $fault_after end),hedge_delay:$hedge_delay,concurrency:$concurrency,runtime_failure_lines:$runtime_failure_lines,result:$result,verification:$count}' \
	| tee "$output_file"
