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
failed_role=${RHIZA_SERVER_BENCH_FAILED_ROLE:-none}
base_http_port=${RHIZA_SERVER_BENCH_HTTP_PORT:-18100}
base_peer_port=${RHIZA_SERVER_BENCH_PEER_PORT:-19100}
minio_port=${RHIZA_SERVER_BENCH_MINIO_PORT:-19000}
if [[ ! $requests =~ ^[1-9][0-9]*$ || ! $concurrency =~ ^[1-9][0-9]*$ || $concurrency -gt $requests ]]; then
	printf 'request count and concurrency must be positive, with concurrency <= requests\n' >&2
	exit 2
fi
if [[ $failed_role != none && $failed_role != leader && $failed_role != non-leader ]]; then
	printf 'RHIZA_SERVER_BENCH_FAILED_ROLE must be none, leader, or non-leader\n' >&2
	exit 2
fi

run_dir=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-server-bench.XXXXXX")
container="rhiza-bench-minio-$$"
on_error() {
	local code=$?
	for log in "$run_dir"/node-*.log; do
		[[ -f $log ]] || continue
		cp "$log" "$(dirname "$output_file")/rhiza-server-$(basename "$log")"
	done
	return "$code"
}
cleanup() {
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
trap on_error ERR
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
		RHIZA_HEDGE_DELAY=100ms "$run_dir/rhiza" >"$run_dir/node-$i.log" 2>&1 &
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

target="http://127.0.0.1:$((base_http_port + 1))"
curl -fsS -H 'Content-Type: application/json' \
	-d '{"request_id":"schema","sql":"CREATE TABLE benchmark_writes (id INTEGER PRIMARY KEY)"}' \
	"$target/sql/execute" >/dev/null
if [[ $failed_role != none ]]; then
	# Slot 1 creates the schema; QuePaxa's deterministic epoch-0 leader for the
	# next slot is n0. n2 is an unambiguous non-leader and n1 remains the client.
	failed_node=2
	[[ $failed_role == leader ]] && failed_node=0
	failed_pid=$(<"$run_dir/node-$failed_node.pid")
	kill "$failed_pid"
	wait "$failed_pid" || true
	curl -fsS -H 'Content-Type: application/json' \
		-d '{"request_id":"fault-warmup","sql":"INSERT INTO benchmark_writes(id) VALUES (-1)"}' \
		"$target/sql/execute" >/dev/null
fi
result=$("$run_dir/rhiza-bench" -url "$target" -path /sql/execute \
	-body '{"request_id":"bench-{{id}}","sql":"INSERT INTO benchmark_writes(id) VALUES ({{id}})"}' \
	-n "$requests" -c "$concurrency")
# Preserve the client result even when the subsequent correctness gate fails.
tee "$output_file" <<<"$result" >/dev/null
count=$(curl -fsS -H 'Content-Type: application/json' \
	-d '{"sql":"SELECT COUNT(*) FROM benchmark_writes WHERE id >= 0","consistency":"linearizable"}' \
	"$target/sql/query")
jq -e --argjson requests "$requests" '.errors == 0 and .successes == $requests' <<<"$result" >/dev/null
jq -e --argjson requests "$requests" '.rows == [[$requests]]' <<<"$count" >/dev/null
jq -nc --argjson result "$result" --argjson count "$count" --argjson concurrency "$concurrency" --arg failed_role "$failed_role" \
	'{transport:"HTTP client + three QUIC voters",durability:"quorum WAL sync",failure_mode:(if $failed_role == "none" then "healthy" else "one-peer-unavailable" end),failed_role:$failed_role,concurrency:$concurrency,result:$result,verification:$count}' \
	| tee "$output_file"
