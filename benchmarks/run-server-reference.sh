#!/usr/bin/env bash

set -euo pipefail

# The PID remains an owned, unwaited child until wait clears it. Only a running
# shell job may be signalled; never signal a stale PID after an explicit wait.
stop_benchmark_child() {
	[[ -n ${bench_pid:-} ]] || return 0
	if jobs -pr | grep -qx "$bench_pid"; then
		kill -KILL "$bench_pid" 2>/dev/null || true
	fi
	wait "$bench_pid" 2>/dev/null || true
	bench_pid=
}

wait_benchmark_child() {
	local status=0
	wait "$bench_pid" || status=$?
	bench_pid=
	return "$status"
}

verify_benchmark_rows() {
	# INTEGER PRIMARY KEY uniqueness plus N total and N in [0,N) proves the set.
	jq -e --argjson requests "$1" '.rows == [[$requests, $requests]]' >/dev/null
}

inject_benchmark_fault() {
	if ! jobs -pr | grep -qx "$failed_pid"; then
		printf 'fault target is not a current owned running child; refusing injection\n' >&2
		return 1
	fi
	# The client already pinned the birth identity with a pidfd before requests.
	# A single call records/signals/checks in its monotonic clock domain.
	local status=0
	curl -fsS --max-time 2 -X POST "$control_url/fault" || status=$?
	if [[ $status != 0 ]]; then
		printf 'fault coordinator failed; interruption not qualified\n' >&2
		return "$status"
	fi
}

# Unit mocks source only the same small helpers used by the actual runner.
if [[ ${BASH_SOURCE[0]} != "$0" ]]; then return 0; fi

if [[ $# -ne 2 ]]; then
	printf 'usage: %s SOURCE_DIR OUTPUT_FILE\n' "$0" >&2
	exit 2
fi

source_dir=$(cd "$1" && pwd)
client_source_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
client_revision=$(git -C "$client_source_dir" rev-parse HEAD)
output_file=$2
requests=${RHIZA_SERVER_BENCH_REQUESTS:-100000}
concurrency=${RHIZA_SERVER_BENCH_CONCURRENCY:-16}
failed_node=${RHIZA_SERVER_BENCH_FAILED_NODE:-none}
target_node=${RHIZA_SERVER_BENCH_TARGET_NODE:-n1}
durability=${RHIZA_SERVER_BENCH_DURABILITY:-async}
fault_after=${RHIZA_SERVER_BENCH_FAULT_AFTER:-1}
max_fault_latency_ms=${RHIZA_SERVER_BENCH_MAX_FAULT_LATENCY_MS:-1500}
base_http_port=${RHIZA_SERVER_BENCH_HTTP_PORT:-18100}
base_peer_port=${RHIZA_SERVER_BENCH_PEER_PORT:-19100}
s3_port=${RHIZA_SERVER_BENCH_S3_PORT:-19000}
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
if [[ $durability != async && $durability != before-ack ]]; then
	printf 'RHIZA_SERVER_BENCH_DURABILITY must be async or before-ack\n' >&2
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
if [[ ! $max_fault_latency_ms =~ ^[1-9][0-9]*([.][0-9]+)?$ ]]; then
	printf 'RHIZA_SERVER_BENCH_MAX_FAULT_LATENCY_MS must be a positive number\n' >&2
	exit 2
fi

run_dir=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-server-bench.XXXXXX")
container="rhiza-bench-s3-$$"
bench_pid=
cleanup() {
	set +e
	stop_benchmark_child
	output_stem=$(basename "$output_file" .json)
	for log in "$run_dir"/node-*.log; do
		[[ -f $log ]] || continue
		cp "$log" "$(dirname "$output_file")/$output_stem-$(basename "$log")"
	done
	if [[ -f $run_dir/fault-metadata.txt ]]; then
		cp "$run_dir/fault-metadata.txt" "$(dirname "$output_file")/$output_stem-fault-metadata.txt"
	fi
	for pid_file in "$run_dir"/node-*.pid; do
		[[ -f $pid_file ]] || continue
		pid=$(<"$pid_file")
		if jobs -pr | grep -qx "$pid"; then kill "$pid" 2>/dev/null || true; fi
		wait "$pid" 2>/dev/null || true
	done
	docker rm -f "$container" >/dev/null 2>&1 || true
	case "$run_dir" in
		"${TMPDIR:-/tmp}"/rhiza-server-bench.*) rm -rf -- "$run_dir" ;;
	esac
}
trap 'exit_status=$?; trap - EXIT; cleanup; exit "$exit_status"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

(cd "$source_dir" && CGO_ENABLED=0 go build -o "$run_dir/rhiza" ./cmd/rhiza)
# Both server revisions use the exact same candidate client/oracle, not each
# revision's potentially different receipt parser or fault instrumentation.
(cd "$client_source_dir" && CGO_ENABLED=0 go build -o "$run_dir/rhiza-bench" ./cmd/rhiza-bench)
if command -v sha256sum >/dev/null; then
	client_sha256=$(sha256sum "$run_dir/rhiza-bench" | awk '{print $1}')
else
	client_sha256=$(shasum -a 256 "$run_dir/rhiza-bench" | awk '{print $1}')
fi

mkdir -p "$run_dir/s3-data"
docker run --rm -d --name "$container" -p "$s3_port:9000" --user "$(id -u):$(id -g)" \
	-v "$run_dir/s3-data:/data" \
	-e ROOT_ACCESS_KEY=rhiza-e2e -e ROOT_SECRET_KEY=rhiza-e2e-secret \
	ghcr.io/versity/versitygw@sha256:30292fc2eeacc67a36993b01f7a7a5e3361a19cced0e80c1d71cfa2a4b0a2499 \
	--port :9000 --health /_/health posix /data >/dev/null
for _ in {1..100}; do
	curl -fsS "http://127.0.0.1:$s3_port/_/health" >/dev/null 2>&1 && break
	sleep 0.1
done
curl -fsS "http://127.0.0.1:$s3_port/_/health" >/dev/null
docker run --rm --add-host host.docker.internal:host-gateway \
	-e AWS_ACCESS_KEY_ID=rhiza-e2e -e AWS_SECRET_ACCESS_KEY=rhiza-e2e-secret \
	-e AWS_DEFAULT_REGION=us-east-1 \
	amazon/aws-cli@sha256:83f8ffe939569070c5b66d22231862ab78718766d9d8e4c44ca84dd0be5569a5 \
	--endpoint-url "http://host.docker.internal:$s3_port" s3 mb s3://rhiza >/dev/null

# The membership record shape follows the revision under test: v2 publishes
# only the public key each node's private peer token derives for cluster
# "server-bench", while the pre-v2 record carries the token itself. This script
# also runs against the base worktree, so emit the shape that revision decodes.
legacy_membership=0
if git -C "$source_dir" grep -q 'PublicKey \[32\]byte' -- pkg/quepaxa/types.go; then
	members=$(printf '[{"node_id":"n0","url":"http://127.0.0.1:%d","peer_url":"quic://127.0.0.1:%d","public_key":"LTZoxqDu2CGyrI5Wv0OEm29x8p8EJy5VVopcfsiay1Y="},{"node_id":"n1","url":"http://127.0.0.1:%d","peer_url":"quic://127.0.0.1:%d","public_key":"9vOZpf7yRO0hOBDSsvaRFR2I/+YDpy44SoH8Alns6DI="},{"node_id":"n2","url":"http://127.0.0.1:%d","peer_url":"quic://127.0.0.1:%d","public_key":"BA8M2Nuij6qw5VpdNB0R/toOoSI4WnvWN1stW8qh5NQ="}]' \
		"$base_http_port" "$base_peer_port" "$((base_http_port + 1))" "$((base_peer_port + 1))" "$((base_http_port + 2))" "$((base_peer_port + 2))")
else
	legacy_membership=1
	members=$(printf '[{"node_id":"n0","url":"http://127.0.0.1:%d","peer_url":"quic://127.0.0.1:%d","token":"n0-token"},{"node_id":"n1","url":"http://127.0.0.1:%d","peer_url":"quic://127.0.0.1:%d","token":"n1-token"},{"node_id":"n2","url":"http://127.0.0.1:%d","peer_url":"quic://127.0.0.1:%d","token":"n2-token"}]' \
		"$base_http_port" "$base_peer_port" "$((base_http_port + 1))" "$((base_peer_port + 1))" "$((base_http_port + 2))" "$((base_peer_port + 2))")
fi
for i in 0 1 2; do
	peer_token_env=()
	[[ $legacy_membership -eq 1 ]] || peer_token_env=(RHIZA_PEER_TOKEN="n$i-token")
	env RHIZA_CLUSTER_ID=server-bench RHIZA_NODE_ID="n$i" \
		RHIZA_BIND_ADDR="127.0.0.1:$((base_http_port + i))" RHIZA_PEER_ADDR="127.0.0.1:$((base_peer_port + i))" \
		RHIZA_DATA_DIR="$run_dir/node-$i" RHIZA_CLUSTER_MEMBERS="$members" "${peer_token_env[@]+${peer_token_env[@]}}" \
		RHIZA_OBJSTORE_PROVIDER=s3 RHIZA_OBJSTORE_ENDPOINT="127.0.0.1:$s3_port" RHIZA_OBJSTORE_BUCKET=rhiza \
		RHIZA_OBJSTORE_PREFIX=server-bench RHIZA_OBJSTORE_REGION=us-east-1 RHIZA_OBJSTORE_INSECURE=true \
		RHIZA_OBJSTORE_ACCESS_KEY=rhiza-e2e RHIZA_OBJSTORE_SECRET_KEY=rhiza-e2e-secret \
		RHIZA_OBJSTORE_DURABILITY="$durability" RHIZA_OBJSTORE_SYNC_INTERVAL=1h RHIZA_CHECKPOINT_INTERVAL=0 \
		"$run_dir/rhiza" >"$run_dir/node-$i.log" 2>&1 &
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
metric_nodes=(n0 n1 n2)
case "$failed_node" in
	n0) metric_nodes=(n1 n2) ;;
	n1) metric_nodes=(n0 n2) ;;
	n2) metric_nodes=(n0 n1) ;;
esac
object_stats() {
	local node index
	for node in "${metric_nodes[@]}"; do
		index=${node#n}
		curl -fsS "http://127.0.0.1:$((base_http_port + index))/metrics/object-store" \
			| jq -c --arg node "$node" '{node:$node,stats:.}'
	done | jq -sc 'reduce .[] as $item ({}; reduce ($item.stats | to_entries[]) as $entry (. ; .[$entry.key] = ((.[$entry.key] // 0) + $entry.value)))'
}
object_before=$(object_stats)
result_file="$run_dir/result.json"
control_args=()
if [[ $failed_node != none ]]; then
	failed_index=${failed_node#n}
	failed_pid=$(<"$run_dir/node-$failed_index.pid")
	if ! jobs -pr | grep -qx "$failed_pid"; then printf 'fault target is not a current owned running child; refusing binding\n' >&2; exit 1; fi
	# Linux fault lanes only; bind exact start-time ticks as well as the PID.
	process_stat=$(cat "/proc/$failed_pid/stat")
	read -ra process_fields <<< "${process_stat##*) }"
	[[ ${process_fields[19]:-} =~ ^[0-9]+$ ]] || { printf 'fault target start identity unavailable\n' >&2; exit 1; }
	control_args=(-fault-control-file "$run_dir/fault-control.url" -fault-pid "$failed_pid" -fault-start-time "${process_fields[19]}")
fi
"$run_dir/rhiza-bench" -url "$target" -path /sql/execute \
	-body '{"request_id":"bench-{{id}}","sql":"INSERT INTO benchmark_writes(id) VALUES ({{id}})"}' \
	-n "$requests" -c "$concurrency" -commit-unknown-retries 3 -verify-insert-receipt "${control_args[@]+${control_args[@]}}" >"$result_file" &
bench_pid=$!
overlap='null'
if [[ $failed_node != none ]]; then
	sleep "$fault_after"
	# Bounded control startup only; never wait/retry for an in-flight request.
	for _ in {1..100}; do
		[[ -s $run_dir/fault-control.url ]] && break
		kill -0 "$bench_pid" 2>/dev/null || break
		sleep 0.01
	done
	if [[ ! -s $run_dir/fault-control.url ]]; then
		printf 'fault overlap not qualified: client control unavailable\n' >&2
		exit 1
	fi
	control_url=$(<"$run_dir/fault-control.url")
	[[ $control_url =~ ^http://127\.0\.0\.1:[0-9]+/[a-f0-9]{32}$ ]] || { printf 'invalid owned fault control address\n' >&2; exit 1; }
	printf 'target=%s\npid=%s\nsignal=SIGKILL\nbefore=%s\n' "$failed_node" "$failed_pid" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" > "$run_dir/fault-metadata.txt"
	# Run in this shell, not command substitution: PID ownership/jobs stay local.
	inject_benchmark_fault > "$run_dir/fault-overlap.json"
	overlap=$(<"$run_dir/fault-overlap.json")
	jq -e '.attempted == true and .signal_succeeded == true' <<<"$overlap" >/dev/null
	printf 'overlap=%s\n' "$overlap" >> "$run_dir/fault-metadata.txt"
	printf 'after=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >> "$run_dir/fault-metadata.txt"
	wait "$failed_pid" 2>/dev/null || true
	rm -- "$run_dir/node-$failed_index.pid"
fi
wait_benchmark_child
result=$(<"$result_file")
# Preserve the client result even when the subsequent correctness gate fails.
tee "$output_file" <<<"$result" >/dev/null
if [[ $failed_node != none ]]; then
	overlap=$(jq -c '.fault_window' <<<"$result")
	if ! jq -e '.attempted == true and .signal_succeeded == true and .overlap == true and .witnesses > 0' <<<"$overlap" >/dev/null; then
		printf 'fault interval not qualified: %s\n' "$(jq -r '.reason // "missing_window"' <<<"$overlap")" >&2
		exit 1
	fi
fi
count=$(curl -fsS -H 'Content-Type: application/json' \
	-d "{\"sql\":\"SELECT COUNT(*), COUNT(CASE WHEN id >= 0 AND id < $requests THEN 1 END) FROM benchmark_writes\",\"consistency\":\"linearizable\"}" \
	"$target/sql/query")
object_after=$(object_stats)
object_delta=$(jq -nc --argjson before "$object_before" --argjson after "$object_after" \
	'reduce ((($before|keys_unsorted) + ($after|keys_unsorted)) | unique[]) as $key
		({}; .[$key] = (($after[$key] // 0) - ($before[$key] // 0)))')
object_metric_nodes=$(printf '%s\n' "${metric_nodes[@]}" | jq -R . | jq -sc .)
jq -e --argjson requests "$requests" '.errors == 0 and .successes == $requests and .receipt_oracle == "sql-insert-id-v1"' <<<"$result" >/dev/null
verify_benchmark_rows "$requests" <<<"$count"
runtime_failure_lines=0
for log in "$run_dir"/node-*.log; do
	log_failures=$(grep -Ei 'error|failed|timeout' "$log" | grep -Eivc 'failed to sufficiently increase receive buffer size' || true)
	runtime_failure_lines=$((runtime_failure_lines + log_failures))
done
jq -nc --argjson result "$result" --argjson count "$count" --argjson overlap "$overlap" --arg client_revision "$client_revision" --arg client_sha256 "$client_sha256" --argjson concurrency "$concurrency" --argjson runtime_failure_lines "$runtime_failure_lines" --argjson max_fault_latency_ms "$max_fault_latency_ms" --argjson object_delta "$object_delta" --argjson object_metric_nodes "$object_metric_nodes" --arg failed_node "$failed_node" --arg target_node "$target_node" --arg durability "$durability" --arg fault_after "$fault_after" \
	'{transport:"HTTP client + three QUIC voters",client_revision:$client_revision,client_sha256:$client_sha256,receipt_oracle:"sql-insert-id-v1",fault_overlap:$overlap,fault_instrumentation:(if $failed_node == "none" then "disabled" else "pidfd-single-coordinator-monotonic-interval-v2" end),durability_mode:$durability,ack_contract:(if $durability == "before-ack" then "quorum WAL sync and object-store durability before acknowledgement" else "quorum WAL sync; object-store publication async" end),failure_mode:(if $failed_node == "none" then "healthy" else "peer-sigkill-during-load" end),failed_node:$failed_node,target_node:$target_node,fault_after:(if $failed_node == "none" then null else $fault_after end),max_fault_latency_ms:(if $failed_node == "none" then null else $max_fault_latency_ms end),concurrency:$concurrency,object_metric_nodes:$object_metric_nodes,object_measurement:"after schema through final linearizable verification",object_store_delta:$object_delta,runtime_failure_lines:$runtime_failure_lines,result:$result,verification:$count}' \
	| tee "$output_file"
jq -e '.runtime_failure_lines == 0 and (.failed_node == "none" or .result.max_ms <= .max_fault_latency_ms)' "$output_file" >/dev/null
