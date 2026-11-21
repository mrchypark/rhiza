#!/usr/bin/env bash

set -euo pipefail

# The PID remains an owned, unwaited child until wait clears it. Only a running
# shell job may be signalled; never signal a stale PID after an explicit wait.
stop_owned_child() {
	local pid=$1 deadline=$((SECONDS + 2)) failed=0
	if jobs -pr | grep -qx "$pid"; then
		kill "$pid" 2>/dev/null || failed=1
		while jobs -pr | grep -qx "$pid" && (( SECONDS < deadline )); do sleep 0.05; done
		if jobs -pr | grep -qx "$pid"; then
			if ! kill -KILL "$pid" 2>/dev/null && jobs -pr | grep -qx "$pid"; then
				printf 'cleanup: child %s could not be stopped\n' "$pid" >&2
				return 1
			fi
		fi
	fi
	wait "$pid" 2>/dev/null || true
	if jobs -pr | grep -qx "$pid" || [[ $failed == 1 ]]; then
		printf 'cleanup: child %s termination not confirmed\n' "$pid" >&2
		return 1
	fi
}

stop_benchmark_child() {
	[[ -n ${bench_pid:-} ]] || return 0
	stop_owned_child "$bench_pid" || return 1
	bench_pid=
}

wait_benchmark_child() {
	local status=0
	wait "$bench_pid" || status=$?
	bench_pid=
	return "$status"
}

wait_fault_child() {
	local status=0
	wait "$failed_pid" || status=$?
	fault_exit_status=$status
	failed_pid= # never signal an explicitly waited handle during failure cleanup
	if [[ $status != 137 ]]; then
		printf 'fault child exit not qualified: observed=%s expected=137\n' "$status" >&2
		[[ $status != 0 ]] || return 1
		return "$status"
	fi
}

verify_benchmark_rows() {
	# INTEGER PRIMARY KEY uniqueness plus N total and N in [0,N) proves the set.
	jq -e --argjson requests "$1" '.rows == [[$requests, $requests]]' >/dev/null
}

monotonic_ticks() {
	awk 'NR == 1 && $1 ~ /^[0-9]+([.][0-9]+)?$/ { printf "%.0f\n", $1 * 100; found = 1 } END { if (!found) exit 1 }' /proc/uptime
}

deadline_expired() {
	local ticks
	ticks=$(monotonic_ticks) && [[ $ticks =~ ^[0-9]+$ ]] || { printf 'monotonic clock unavailable\n' >&2; exit 1; }
	(( ticks >= deadline ))
}

# /proc stat's command field can contain spaces and parentheses.
server_resource_snapshot() {
	local pid=$1 proc_root=${2:-/proc} stat hwm
	local -a fields
	stat=$(cat "$proc_root/$pid/stat") || return 1
	read -ra fields <<< "${stat##*) }"
	[[ ${fields[11]:-} =~ ^[0-9]+$ && ${fields[12]:-} =~ ^[0-9]+$ && ${fields[19]:-} =~ ^[0-9]+$ ]] || return 1
	hwm=$(awk '$1 == "VmHWM:" && $2 ~ /^[0-9]+$/ && $3 == "kB" && NF == 3 { print $2; found++ } END { if (found != 1) exit 1 }' "$proc_root/$pid/status") || return 1
	printf '%s %s %s %s\n' "${fields[19]}" "${fields[11]}" "${fields[12]}" "$hwm"
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
publication_diagnostic=${RHIZA_SERVER_BENCH_PUBLICATION_DIAGNOSTIC:-0}
sync_interval=${RHIZA_SERVER_BENCH_SYNC_INTERVAL:-}
if [[ -z $sync_interval ]]; then
	if [[ $publication_diagnostic == 1 ]]; then sync_interval=1s; else sync_interval=1h; fi
fi
drain_timeout=${RHIZA_SERVER_BENCH_DRAIN_TIMEOUT_SECONDS:-60}
profile_seconds=${RHIZA_SERVER_BENCH_PROFILE_SECONDS:-30}
base_profile_port=${RHIZA_SERVER_BENCH_PROFILE_PORT:-20100}
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
if [[ $publication_diagnostic != 0 && $publication_diagnostic != 1 ]] || [[ ! $drain_timeout =~ ^[1-9][0-9]*$ ]] || [[ $drain_timeout -gt 3600 ]] || [[ -z $sync_interval ]]; then
	printf 'invalid publication diagnostic, sync interval, or drain timeout (1-3600 seconds)\n' >&2
	exit 2
fi
if [[ $publication_diagnostic == 1 ]] && { [[ ! $profile_seconds =~ ^[1-9][0-9]*$ || $profile_seconds -lt 10 || $profile_seconds -gt 3600 ]] || [[ ! $base_profile_port =~ ^[1-9][0-9]*$ || $base_profile_port -gt 65533 ]]; }; then
	printf 'invalid CPU profile duration or base port\n' >&2
	exit 2
fi
if [[ $publication_diagnostic == 1 && ( $durability != async || $failed_node != none ) ]]; then
	printf 'publication diagnostic requires healthy async mode\n' >&2
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
profile_pids=()
container_id=
container_identity_unavailable=0
cleanup() {
	set +e
	local failed=$container_identity_unavailable remaining
	stop_benchmark_child || failed=1
	for pid in "${profile_pids[@]+${profile_pids[@]}}"; do
		stop_owned_child "$pid" || failed=1
	done
	output_stem=$(basename "$output_file" .json)
	for log in "$run_dir"/node-*.log; do
		[[ -f $log ]] || continue
		cp "$log" "$(dirname "$output_file")/$output_stem-$(basename "$log")" || { printf 'cleanup: could not preserve log %s\n' "$log" >&2; failed=1; }
	done
	if [[ -f $run_dir/fault-metadata.txt ]]; then
		cp "$run_dir/fault-metadata.txt" "$(dirname "$output_file")/$output_stem-fault-metadata.txt" || { printf 'cleanup: could not preserve fault metadata\n' >&2; failed=1; }
	fi
	for pid_file in "$run_dir"/node-*.pid; do
		[[ -f $pid_file ]] || continue
		pid=$(<"$pid_file")
		stop_owned_child "$pid" || failed=1
	done
	if [[ -n $container_id ]]; then
		docker rm -f "$container_id" >/dev/null 2>&1 || true
		if ! remaining=$(docker ps -aq --no-trunc --filter "id=$container_id") || [[ -n $remaining ]]; then
			printf 'cleanup: container %s removal not confirmed\n' "$container_id" >&2
			failed=1
		fi
	fi
	if [[ $failed == 0 ]]; then
		case "$run_dir" in
			"${TMPDIR:-/tmp}"/rhiza-server-bench.*) rm -rf -- "$run_dir" || { printf 'cleanup: run directory removal failed: %s\n' "$run_dir" >&2; failed=1; } ;;
		esac
	else
		printf 'cleanup: retained run directory %s\n' "$run_dir" >&2
	fi
	return "$failed"
}
finish_cleanup() {
	local exit_status=$1 tmp
	cleanup || { [[ $exit_status != 0 ]] || exit_status=1; }
	if [[ $exit_status == 0 && $publication_diagnostic == 1 ]]; then
		tmp=$(mktemp "$(dirname "$output_file")/.rhiza-cleanup-result.XXXXXX") || return 1
		if ! jq -e 'select(.publication_diagnostic.outcome == "incomplete" and .publication_diagnostic.reason == "cleanup_unconfirmed") | .publication_diagnostic.outcome = "complete" | .publication_diagnostic.reason = "archive_tip_reached"' "$output_file" > "$tmp" || ! mv "$tmp" "$output_file"; then
			printf 'cleanup: could not finalize diagnostic result\n' >&2
			rm -f -- "$tmp"
			return 1
		fi
		cat "$output_file"
	fi
	return "$exit_status"
}
trap 'exit_status=$?; trap - EXIT; finish_cleanup "$exit_status"; exit $?' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

build_tags=()
[[ $publication_diagnostic != 1 ]] || build_tags=(-tags ci_profile)
(cd "$source_dir" && CGO_ENABLED=0 go build "${build_tags[@]+${build_tags[@]}}" -o "$run_dir/rhiza" ./cmd/rhiza)
if [[ $publication_diagnostic == 1 ]]; then
	profile_dir=$(mktemp -d "$(dirname "$output_file")/$(basename "$output_file" .json)-profiles.XXXXXX")
	cp "$run_dir/rhiza" "$profile_dir/server-binary"
	printf 'CPU profile artifacts: %s\n' "$profile_dir" >&2
fi
# Both server revisions use the exact same candidate client/oracle, not each
# revision's potentially different receipt parser or fault instrumentation.
(cd "$client_source_dir" && CGO_ENABLED=0 go build -o "$run_dir/rhiza-bench" ./cmd/rhiza-bench)
if command -v sha256sum >/dev/null; then
	client_sha256=$(sha256sum "$run_dir/rhiza-bench" | awk '{print $1}')
else
	client_sha256=$(shasum -a 256 "$run_dir/rhiza-bench" | awk '{print $1}')
fi

mkdir -p "$run_dir/s3-data"
container_id=$(docker run --rm -d --name "$container" -p "$s3_port:9000" --user "$(id -u):$(id -g)" \
	-v "$run_dir/s3-data:/data" \
	-e ROOT_ACCESS_KEY=rhiza-e2e -e ROOT_SECRET_KEY=rhiza-e2e-secret \
	ghcr.io/versity/versitygw@sha256:30292fc2eeacc67a36993b01f7a7a5e3361a19cced0e80c1d71cfa2a4b0a2499 \
	--port :9000 --health /_/health posix /data) || {
	launch_status=$?
	container_id=
	container_identity_unavailable=1
	printf 'container launch failed; cleanup cannot be verified without its identity\n' >&2
	exit "$launch_status"
}
if [[ ! $container_id =~ ^[a-f0-9]{12,64}$ ]]; then
	container_id=
	container_identity_unavailable=1
	printf 'container identity unavailable; cleanup cannot be verified\n' >&2
	exit 1
fi
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
	profile_env=()
	[[ $publication_diagnostic != 1 ]] || profile_env=(RHIZA_CI_CPU_PROFILE_PORT="$((base_profile_port + i))")
	env RHIZA_CLUSTER_ID=server-bench RHIZA_NODE_ID="n$i" \
		RHIZA_BIND_ADDR="127.0.0.1:$((base_http_port + i))" RHIZA_PEER_ADDR="127.0.0.1:$((base_peer_port + i))" \
		RHIZA_DATA_DIR="$run_dir/node-$i" RHIZA_CLUSTER_MEMBERS="$members" "${peer_token_env[@]+${peer_token_env[@]}}" "${profile_env[@]+${profile_env[@]}}" \
		RHIZA_OBJSTORE_PROVIDER=s3 RHIZA_OBJSTORE_ENDPOINT="127.0.0.1:$s3_port" RHIZA_OBJSTORE_BUCKET=rhiza \
		RHIZA_OBJSTORE_PREFIX=server-bench RHIZA_OBJSTORE_REGION=us-east-1 RHIZA_OBJSTORE_INSECURE=true \
		RHIZA_OBJSTORE_ACCESS_KEY=rhiza-e2e RHIZA_OBJSTORE_SECRET_KEY=rhiza-e2e-secret \
		RHIZA_OBJSTORE_DURABILITY="$durability" RHIZA_OBJSTORE_SYNC_INTERVAL="$sync_interval" RHIZA_CHECKPOINT_INTERVAL=0 \
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
publication_metrics() {
	local node index stats metrics='{}' failed=0
	for node in "${metric_nodes[@]}"; do
		index=${node#n}
		if ! stats=$(curl -fsS --max-time 2 "http://127.0.0.1:$((base_http_port + index))/metrics/object-store") || ! jq -e '. as $s | ["uploads","gets","lists","heads","deletes","failures","bytes_uploaded","bytes_downloaded","s3_http_requests","s3_http_failures","http_requests","http_failures"] | all(.[]; $s[.] | type == "number" and . >= 0 and floor == .)' <<<"$stats" >/dev/null || ! jq -e 'all(.[]; type == "number" and . >= 0 and floor == .)' <<<"$stats" >/dev/null; then
			stats=null
			failed=1
		fi
		metrics=$(jq -nc --argjson metrics "$metrics" --arg node "$node" --argjson stats "$stats" '$metrics + {($node):$stats}')
	done
	printf '%s\n' "$metrics"
	return "$failed"
}
if [[ $publication_diagnostic == 1 ]]; then
	if ! publication_before=$(publication_metrics); then
		jq -nc --argjson before "$publication_before" --arg interval "$sync_interval" --arg profile_dir "$profile_dir" '{publication_diagnostic:{outcome:"incomplete",reason:"baseline_metrics_unavailable",sync_interval:$interval,profile_directory:$profile_dir,baseline:$before}}' > "$output_file"
		printf 'publication baseline metrics incomplete\n' >&2
		exit 1
	fi
	object_before=$(jq -c 'reduce (.[] | to_entries[]) as $entry ({}; .[$entry.key] = ((.[$entry.key] // 0) + $entry.value))' <<<"$publication_before")
else
	object_before=$(object_stats)
fi
result_file="$run_dir/result.json"
control_args=()
if [[ $publication_diagnostic == 1 ]]; then control_args=(-profile-control-file "$run_dir/profile-control.url"); fi
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
if [[ $publication_diagnostic == 1 ]]; then
	for i in 0 1 2; do
		curl -fsS --max-time "$((profile_seconds + 5))" -o "$run_dir/node-$i.cpu.pprof" "http://127.0.0.1:$((base_profile_port + i))/debug/pprof/profile?seconds=$profile_seconds" &
		profile_pids+=("$!")
	done
	profiles_alive() {
		local pid
		for pid in "${profile_pids[@]}"; do jobs -pr | grep -qx "$pid" || return 1; done
	}
	profiles_ready() {
		local i
		profiles_alive || return 1
		for i in 0 1 2; do
			curl -fsS --max-time 1 "http://127.0.0.1:$((base_profile_port + i))/debug/pprof/ready" >/dev/null 2>&1 || return 1
		done
		profiles_alive
	}
	ready_deadline=$((SECONDS + 5))
	until profiles_ready; do
		if (( SECONDS >= ready_deadline )); then
			jq -nc --argjson baseline "$publication_before" --arg profile_dir "$profile_dir" '{publication_diagnostic:{outcome:"incomplete",reason:"cpu_profile_start_unconfirmed",profile_directory:$profile_dir,baseline:$baseline}}' > "$output_file"
			printf 'CPU profiles did not become ready\n' >&2
			exit 1
		fi
		sleep 0.1
	done
fi
if [[ $failed_node == none ]]; then
	clock_ticks=$(getconf CLK_TCK)
	[[ $clock_ticks =~ ^[1-9][0-9]*$ ]] || { printf 'server CPU clock ticks unavailable\n' >&2; exit 1; }
	for i in 0 1 2; do
		pid=$(<"$run_dir/node-$i.pid")
		jobs -pr | grep -qx "$pid" || { printf 'server n%s is not a running child\n' "$i" >&2; exit 1; }
		server_resource_snapshot "$pid" > "$run_dir/node-$i-resource-before" || { printf 'server n%s resource snapshot unavailable\n' "$i" >&2; exit 1; }
	done
fi
"$run_dir/rhiza-bench" -url "$target" -path /sql/execute \
	-body '{"request_id":"bench-{{id}}","sql":"INSERT INTO benchmark_writes(id) VALUES ({{id}})"}' \
	-n "$requests" -c "$concurrency" -commit-unknown-retries 3 -verify-insert-receipt "${control_args[@]+${control_args[@]}}" >"$result_file" &
bench_pid=$!
if [[ $publication_diagnostic == 1 ]]; then
	for _ in {1..100}; do
		[[ -s $run_dir/profile-control.url ]] && break
		jobs -pr | grep -qx "$bench_pid" || break
		sleep 0.01
	done
	profile_control_url=$(<"$run_dir/profile-control.url") || profile_control_url=
	profile_success_seen() {
		local status deadline=$((SECONDS + 5))
		while profiles_alive && jobs -pr | grep -qx "$bench_pid" && (( SECONDS < deadline )); do
			status=$(curl -fsS --max-time 1 -X POST "$profile_control_url/profile/status" 2>/dev/null) || return 1
			jq -e '.successes | type == "number" and . >= 0 and floor == .' <<<"$status" >/dev/null 2>&1 || return 1
			if jq -e '.successes >= 1' <<<"$status" >/dev/null; then return 0; fi
			sleep 0.05
		done
		return 1
	}
	if [[ ! $profile_control_url =~ ^http://127\.0\.0\.1:[0-9]+/[a-f0-9]{32}$ ]] ||
		! profiles_ready ||
		! curl -fsS --max-time 2 -X POST "$profile_control_url/profile/begin" >/dev/null 2>&1 ||
		! profile_success_seen ||
		! profiles_ready ||
		! curl -fsS --max-time 2 -X POST "$profile_control_url/profile/end" >/dev/null 2>&1 ||
		! profiles_alive; then
		client_result=null
		client_status=null
		failure_status=1
		reason=cpu_profile_overlap_unconfirmed
		if ! jobs -pr | grep -qx "$bench_pid"; then
			client_status=0
			wait_benchmark_child || client_status=$?
			client_result=$(jq -ce 'select(type == "object")' "$result_file" 2>/dev/null) || client_result=null
			if [[ $client_status != 0 ]]; then failure_status=$client_status; reason=workload_failed; fi
		fi
		jq -nc --argjson result "$client_result" --argjson status "$client_status" --arg reason "$reason" --argjson baseline "$publication_before" --arg profile_dir "$profile_dir" '{result:$result,publication_diagnostic:{outcome:"incomplete",reason:$reason,profile_directory:$profile_dir,baseline:$baseline,client_exit_status:$status}}' > "$output_file"
		printf 'CPU profile overlap with workload not confirmed\n' >&2
		exit "$failure_status"
	fi
fi
overlap='null'
fault_exit_status=null
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
	wait_status=0
	wait_fault_child || wait_status=$?
	printf 'fault_exit_status=%s\n' "$fault_exit_status" >> "$run_dir/fault-metadata.txt"
	rm -- "$run_dir/node-$failed_index.pid"
	if [[ $wait_status != 0 ]]; then
		jq -nc --argjson status "$fault_exit_status" --argjson overlap "$overlap" \
			'{qualification:"not_qualified",reason:"fault_child_exit_status",fault_exit_status:$status,fault_overlap:$overlap}' | tee "$output_file"
		exit "$wait_status"
	fi
fi
benchmark_status=0
wait_benchmark_child || benchmark_status=$?
if [[ $benchmark_status != 0 ]]; then
	if [[ $publication_diagnostic == 1 ]]; then
		client_result=$(jq -ce 'select(type == "object")' "$result_file" 2>/dev/null) || client_result=null
		jq -nc --argjson result "$client_result" --argjson baseline "$publication_before" --argjson status "$benchmark_status" --arg profile_dir "$profile_dir" '{result:$result,publication_diagnostic:{outcome:"incomplete",reason:"workload_failed",profile_directory:$profile_dir,baseline:$baseline,client_exit_status:$status}}' > "$output_file"
	fi
	exit "$benchmark_status"
fi
result=$(<"$result_file")
if [[ $publication_diagnostic == 1 ]]; then
	completion_ns=$(jq -er '.last_request_completion_boottime_ns | select(type == "number" and . > 0 and floor == .)' <<<"$result") || completion_ns=
	now_ticks=$(monotonic_ticks) || now_ticks=
	if [[ ! $completion_ns =~ ^[1-9][0-9]*$ || ${#completion_ns} -gt 19 || ( ${#completion_ns} == 19 && $completion_ns > 9223372036854775807 ) || ! $now_ticks =~ ^[0-9]+$ ]] ||
		(( completion_ns / 10000000 > now_ticks + 1 )); then
		jq -nc --argjson result "$result" --argjson baseline "$publication_before" --arg profile_dir "$profile_dir" '{result:$result,publication_diagnostic:{outcome:"incomplete",reason:"workload_completion_time_unavailable",profile_directory:$profile_dir,baseline:$baseline}}' > "$output_file"
		printf 'last request completion clock unavailable or invalid\n' >&2
		exit 1
	fi
	# One conservative tick also covers JSON number rounding at large uptimes.
	workload_end_ticks=$((completion_ns / 10000000 - 1))
fi
if [[ $publication_diagnostic == 1 ]] && ! jq -e '.profile_window.overlap == true and .profile_window.incomplete == false and (.profile_window.successes | type == "number" and . >= 1 and floor == .)' <<<"$result" >/dev/null; then
	jq -nc --argjson result "$result" --argjson baseline "$publication_before" --arg profile_dir "$profile_dir" '{result:$result,publication_diagnostic:{outcome:"incomplete",reason:"cpu_profile_overlap_unconfirmed",profile_directory:$profile_dir,baseline:$baseline}}' > "$output_file"
	printf 'CPU profile overlap with a completed workload request not confirmed\n' >&2
	exit 1
fi
# Preserve the client result even when a subsequent measurement or gate fails.
tee "$output_file" <<<"$result" >/dev/null
result_with_exit=$(jq --argjson status "$fault_exit_status" '. + {fault_exit_status:$status}' <<<"$result")
tee "$output_file" <<<"$result_with_exit" >/dev/null
if [[ $failed_node == none ]]; then
	server_resources='{}'
	for i in 0 1 2; do
		pid=$(<"$run_dir/node-$i.pid")
		jobs -pr | grep -qx "$pid" || { printf 'server n%s is not a running child\n' "$i" >&2; exit 1; }
		read -r start_before user_before system_before _ < "$run_dir/node-$i-resource-before"
		snapshot=$(server_resource_snapshot "$pid") || { printf 'server n%s resource snapshot unavailable\n' "$i" >&2; exit 1; }
		read -r start_after user_after system_after hwm <<< "$snapshot"
		[[ $start_before == "$start_after" && $user_after -ge $user_before && $system_after -ge $system_before ]] || { printf 'server n%s resource identity or CPU counters changed unexpectedly\n' "$i" >&2; exit 1; }
		server_resources=$(jq -nc --argjson resources "$server_resources" --arg node "n$i" --argjson ticks "$((user_after - user_before + system_after - system_before))" --argjson hz "$clock_ticks" --argjson hwm "$hwm" '$resources + {($node): {cpu_seconds: ($ticks / $hz), vm_hwm_kib: $hwm}}')
	done
fi
if [[ $publication_diagnostic == 1 ]]; then
	metrics_failed=0
	publication_end=$(publication_metrics) || metrics_failed=1
	if [[ $metrics_failed == 0 ]] && ! jq -en --argjson before "$publication_before" --argjson end "$publication_end" 'all($before | keys[]; . as $node | ($before[$node] | keys) == ($end[$node] | keys))' >/dev/null; then metrics_failed=1; fi
fi
if [[ $publication_diagnostic == 1 ]]; then
	publication_drain=null
	publication_status=null
	publication_target=null
	publication_outcome=incomplete
	publication_reason=workload_metrics_unavailable
	publication_write() {
		jq --argjson before "$publication_before" --argjson end "$publication_end" --argjson drain "$publication_drain" --argjson status "$publication_status" --argjson target "$publication_target" --arg outcome "$publication_outcome" --arg reason "$publication_reason" --arg interval "$sync_interval" --argjson timeout "$drain_timeout" --arg profile_dir "$profile_dir" \
			'. + {publication_diagnostic:{outcome:(if $outcome == "complete" then "incomplete" else $outcome end),reason:(if $outcome == "complete" then "cleanup_unconfirmed" else $reason end),sync_interval:$interval,drain_timeout_seconds:$timeout,profile_directory:$profile_dir,cpu_profile_readiness:"three sequential ready checks before begin and before end, with the same profile requests alive",observation:"per-node snapshots are sequential; /recovery/status performs ReadIndex and adds quorum overhead; CPU profiles may extend past the workload",baseline:$before,workload_end:$end,drain_end:$drain,fixed_certified_tip:$target,last_status:$status}}' <<<"$result_with_exit" > "$output_file"
	}
	publication_write
	if [[ $metrics_failed == 0 ]]; then
		publication_reason=status_unavailable
		publication_write
		deadline=$((workload_end_ticks + drain_timeout * 100))
		if deadline_expired; then
			publication_outcome=timeout
			publication_reason=archive_tip_timeout
		elif publication_status=$(curl -fsS --max-time 2 "$target/recovery/status") && jq -e --arg node "$target_node" '.node_id == $node and .quorum == true and (.certified_tip | type == "number" and . >= 0 and floor == .) and (.archive_tip | type == "number" and . >= 0 and floor == .)' <<<"$publication_status" >/dev/null; then
			publication_target=$(jq -c '.certified_tip' <<<"$publication_status")
			publication_reason=archive_tip_timeout
			while :; do
				publication_write
				if deadline_expired; then publication_outcome=timeout; break; fi
				if jq -e --argjson tip "$publication_target" '.archive_tip >= $tip' <<<"$publication_status" >/dev/null; then
					publication_reason=archive_tip_reached
					break
				fi
				sleep 0.2
				if deadline_expired; then publication_outcome=timeout; break; fi
				if ! next_status=$(curl -fsS --max-time 2 "$target/recovery/status") || ! jq -e --arg node "$target_node" '.node_id == $node and .quorum == true and (.archive_tip | type == "number" and . >= 0 and floor == .)' <<<"$next_status" >/dev/null; then
					if deadline_expired; then publication_outcome=timeout; else publication_reason=status_unavailable; fi
					break
				fi
				publication_status=$next_status
			done
		else
			publication_status=null
			if deadline_expired; then publication_outcome=timeout; publication_reason=archive_tip_timeout; fi
		fi
	fi
	if ! publication_drain=$(publication_metrics); then
		publication_outcome=incomplete
		if [[ $publication_reason != workload_metrics_unavailable ]]; then publication_reason=drain_metrics_unavailable; fi
	fi
	if [[ $publication_reason == archive_tip_reached ]] && ! jq -en --argjson before "$publication_before" --argjson drain "$publication_drain" 'all($before | keys[]; . as $node | ($before[$node] | keys) == ($drain[$node] | keys))' >/dev/null; then
		publication_outcome=incomplete
		publication_reason=drain_metrics_unavailable
	fi
	publication_write
fi
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
if [[ $publication_diagnostic == 1 && $publication_reason != archive_tip_reached ]]; then
	object_after='{}'
else
	object_after=$(object_stats)
fi
runtime_failure_lines=0
for log in "$run_dir"/node-*.log; do
	log_failures=$(grep -Ei 'error|failed|timeout' "$log" | grep -Eivc 'failed to sufficiently increase receive buffer size' || true)
	runtime_failure_lines=$((runtime_failure_lines + log_failures))
done
if ! jq -e --argjson requests "$requests" '.errors == 0 and .successes == $requests and .receipt_oracle == "sql-insert-id-v1"' <<<"$result" >/dev/null || ! verify_benchmark_rows "$requests" <<<"$count" || [[ $runtime_failure_lines != 0 ]]; then
	if [[ $publication_diagnostic == 1 ]]; then publication_outcome=incomplete; publication_reason=workload_verification_failed; publication_write; fi
	printf 'workload verification failed\n' >&2
	exit 1
fi
if [[ $publication_diagnostic == 1 ]]; then
	profile_failed=0
	for i in 0 1 2; do
		if ! wait "${profile_pids[$i]}" || [[ ! -s $run_dir/node-$i.cpu.pprof ]] || ! go tool pprof -top "$run_dir/rhiza" "$run_dir/node-$i.cpu.pprof" >/dev/null 2>&1; then
			profile_failed=1
		else
			cp "$run_dir/node-$i.cpu.pprof" "$profile_dir/node-$i.cpu.pprof"
		fi
	done
	profile_pids=()
	if [[ $profile_failed == 1 ]]; then publication_outcome=incomplete; publication_reason=cpu_profile_unavailable; fi
	if [[ $publication_reason == archive_tip_reached ]]; then publication_outcome=complete; fi
	publication_write
	if [[ $publication_outcome != complete ]]; then
		printf 'publication diagnostic %s: %s\n' "$publication_outcome" "$publication_reason" >&2
		exit 1
	fi
	publication_result=$(jq -c '.publication_diagnostic' "$output_file")
fi
object_delta=$(jq -nc --argjson before "$object_before" --argjson after "$object_after" \
	'reduce ((($before|keys_unsorted) + ($after|keys_unsorted)) | unique[]) as $key
		({}; .[$key] = (($after[$key] // 0) - ($before[$key] // 0)))')
object_metric_nodes=$(printf '%s\n' "${metric_nodes[@]}" | jq -R . | jq -sc .)
jq -nc --argjson result "$result" --argjson count "$count" --argjson overlap "$overlap" --argjson fault_exit_status "$fault_exit_status" --arg client_revision "$client_revision" --arg client_sha256 "$client_sha256" --argjson concurrency "$concurrency" --argjson runtime_failure_lines "$runtime_failure_lines" --argjson max_fault_latency_ms "$max_fault_latency_ms" --argjson object_delta "$object_delta" --argjson object_metric_nodes "$object_metric_nodes" --arg failed_node "$failed_node" --arg target_node "$target_node" --arg durability "$durability" --arg fault_after "$fault_after" --arg diagnostic "$publication_diagnostic" \
	'{transport:"HTTP client + three QUIC voters",client_revision:$client_revision,client_sha256:$client_sha256,receipt_oracle:"sql-insert-id-v1",fault_overlap:$overlap,fault_exit_status:$fault_exit_status,fault_instrumentation:(if $failed_node == "none" then "disabled" else "pidfd-single-coordinator-monotonic-interval-v2" end),durability_mode:$durability,ack_contract:(if $durability == "before-ack" then "quorum WAL sync and object-store durability before acknowledgement" else "quorum WAL sync; object-store publication async" end),failure_mode:(if $failed_node == "none" then "healthy" else "peer-sigkill-during-load" end),failed_node:$failed_node,target_node:$target_node,fault_after:(if $failed_node == "none" then null else $fault_after end),max_fault_latency_ms:(if $failed_node == "none" then null else $max_fault_latency_ms end),concurrency:$concurrency,object_metric_nodes:$object_metric_nodes,object_measurement:(if $diagnostic == "1" then "after schema through publication drain and final linearizable verification" else "after schema through final linearizable verification" end),object_store_delta:$object_delta,runtime_failure_lines:$runtime_failure_lines,result:$result,verification:$count}' \
	| if [[ $failed_node == none ]]; then jq --argjson resources "$server_resources" '. + {server_resources: {scope: "per server process; CPU delta between sequential snapshots bracketing the client workload; VmHWM lifetime high-water RSS", nodes: $resources}}'; else cat; fi \
	| if [[ $publication_diagnostic == 1 ]]; then jq --argjson publication "$publication_result" '. + {publication_diagnostic:$publication}'; else cat; fi \
	| if [[ $publication_diagnostic == 1 ]]; then cat > "$output_file"; else tee "$output_file"; fi
jq -e '.runtime_failure_lines == 0 and (.failed_node == "none" or .result.max_ms <= .max_fault_latency_ms)' "$output_file" >/dev/null
