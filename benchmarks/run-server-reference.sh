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

verify_graph_rows() {
	jq -e --argjson requests "$1" '(.rows | sort) == [range($requests) | [.]]' >/dev/null
}

verify_publication_cut() {
	local node=$1 before=$2 after=$3
	jq -ne --arg node "$node" --argjson before "$before" --argjson after "$after" '
		def observed: .node_id == $node and .cluster_id == "server-bench" and .durability == "async"
			and .ready == true and .quorum == true
			and ([.archive_tip,.applied_tip,.certified_tip] | all(type == "number" and . >= 0 and floor == .));
		($before|observed) and ($after|observed)
		and $before.archive_tip < $before.certified_tip
		and $before.applied_tip >= $before.certified_tip
		and $after.certified_tip >= $before.certified_tip
		and $after.applied_tip >= $before.certified_tip
		and $after.archive_tip >= $before.certified_tip'
}

object_stats_delta() {
	jq -nc --argjson before "$1" --argjson after "$2" 'reduce ((($before|keys_unsorted) + ($after|keys_unsorted)) | unique[]) as $key
			({}; .[$key] = (($after[$key] // 0) - ($before[$key] // 0)))'
}

sample_benchmark_process() {
	local node=$1 pid=$2 stat rss hwm start
	local -a fields
	stat=$(cat "/proc/$pid/stat") || return 1
	[[ $stat == "$pid ("* && $stat == *") "* ]] || return 1
	# Strip the command name, which can contain spaces and closing parentheses.
	read -ra fields <<< "${stat##*) }"
	[[ ${fields[0]:-} != Z && ${fields[0]:-} != X &&
		${fields[11]:-} =~ ^[0-9]+$ && ${fields[12]:-} =~ ^[0-9]+$ &&
		${fields[19]:-} =~ ^[1-9][0-9]*$ ]] || return 1
	start=${fields[19]}
	read -r rss hwm < <(awk '
		$1 == "VmRSS:" && $3 == "kB" { rss=$2 }
		$1 == "VmHWM:" && $3 == "kB" { hwm=$2 }
		END { print rss, hwm }
	' "/proc/$pid/status") || return 1
	[[ $rss =~ ^[0-9]+$ && $hwm =~ ^[0-9]+$ ]] || return 1
	stat=$(cat "/proc/$pid/stat") || return 1
	[[ $stat == "$pid ("* && $stat == *") "* ]] || return 1
	read -ra fields <<< "${stat##*) }"
	[[ ${fields[19]:-} == "$start" && ${fields[0]:-} != Z && ${fields[0]:-} != X &&
		${fields[11]:-} =~ ^[0-9]+$ && ${fields[12]:-} =~ ^[0-9]+$ ]] || return 1
	jq -nc --arg node "$node" --argjson pid "$pid" --argjson start "$start" \
		--argjson user "${fields[11]}" --argjson system "${fields[12]}" --argjson rss "$rss" --argjson hwm "$hwm" \
		'{node:$node,pid:$pid,process_start_ticks:$start,user_ticks:$user,system_ticks:$system,rss_kib:$rss,lifetime_hwm_kib:$hwm}'
}

benchmark_resources() {
	local before=$1 after=$2 ticks_per_second=$3
	jq -ne --argjson before "$before" --argjson after "$after" --argjson hz "$ticks_per_second" '
		if ($before|length) != 3 or ($after|length) != 3 then error("expected three voter resource samples") else
			{clock_ticks_per_second:$hz,scope:"per voter, before and after measured write client; excludes final verification",
			memory_peak_scope:"individual process lifetime, not simultaneous cluster peak",
			nodes:[range(3) as $i | $before[$i] as $b | $after[$i] as $a |
				if [$b.node,$b.pid,$b.process_start_ticks] != [$a.node,$a.pid,$a.process_start_ticks]
				or $a.user_ticks < $b.user_ticks or $a.system_ticks < $b.system_ticks
				then error("voter identity changed or CPU counters regressed: " + $b.node)
				else {node:$b.node,pid:$b.pid,process_start_ticks:$b.process_start_ticks,
					cpu_user_ticks:($a.user_ticks-$b.user_ticks),cpu_system_ticks:($a.system_ticks-$b.system_ticks),
					rss_kib_before:$b.rss_kib,rss_kib_after:$a.rss_kib,
					lifetime_hwm_kib_before:$b.lifetime_hwm_kib,lifetime_hwm_kib_after:$a.lifetime_hwm_kib}
				end]}
		end'
}

sample_benchmark_host() {
	local proc=$1 kind line cpu=null io=null steal=null
	local -a stat
	for kind in cpu io; do
		if { read -r line; } 2>/dev/null < "$proc/pressure/$kind" &&
			[[ $line =~ ^some[[:space:]].*[[:space:]]total=(0|[1-9][0-9]*)$ ]]; then
			printf -v "$kind" '%s' "${BASH_REMATCH[1]}"
		fi
	done
	if { read -ra stat; } 2>/dev/null < "$proc/stat" &&
		[[ ${stat[0]:-} == cpu && ${stat[8]:-} =~ ^(0|[1-9][0-9]*)$ ]]; then
		steal=${stat[8]}
	fi
	printf '[%s,%s,%s]\n' "$cpu" "$io" "$steal"
}

benchmark_host_pressure() {
	jq -nc --argjson before "$1" --argjson after "$2" '
		def delta($i):
			if $before[$i] == null or $after[$i] == null or $after[$i] < $before[$i]
			then null else $after[$i] - $before[$i] end;
		{cpu_some_us:delta(0),io_some_us:delta(1),cpu_steal_ticks:delta(2)}'
}

sample_benchmark_disks() {
	local proc=$1
	shift
	[[ -r $proc/diskstats ]] || { printf '{}\n'; return 0; }
	awk -v ids="$*" '
		BEGIN { n=split(ids, list, " "); for (i=1; i<=n; i++) wanted[list[i]]=1 }
		{
			device=$1 ":" $2
			if (device in wanted) print device, $8, $11, $14, ($19==""?"-":$19), ($20==""?"-":$20)
		}
	' "$proc/diskstats" | jq -Rsc '
		def number: if test("^[0-9]+$") then tonumber else null end;
		split("\n") | map(select(length>0) | split(" ")) | map({
			key:.[0], value: (.[1:] | map(number))
		}) | from_entries'
}

benchmark_disk_deltas() {
	local before=$1 after=$2
	shift 2
	printf '%s\n' "$@" | jq -Rsc --argjson b "$before" --argjson a "$after" '
		def delta($id; $i):
			$b[$id][$i] as $old | $a[$id][$i] as $new |
			if $old == null or $new == null or $new < $old then null else $new-$old end;
		split("\n") | map(select(length>0)) as $layout |
		{voter_devices: $layout, devices: [$layout | unique[] as $id |
			{device:$id,write_ios:delta($id;0),write_ms:delta($id;1),
			 weighted_io_ms:delta($id;2),flush_ios:delta($id;3),flush_ms:delta($id;4)}]}'
}

sample_benchmark_sched() {
	local proc=$1 pid=$2 enabled=null dir tid stat runtime wait slices identity
	local -a fields
	if { read -r enabled; } 2>/dev/null < "$proc/sys/kernel/sched_schedstats"; then
		case $enabled in 1) enabled=true ;; 0) enabled=false ;; *) enabled=null ;; esac
	else
		enabled=null
	fi
	{
		for dir in "$proc/$pid/task/"*; do
			tid=${dir##*/}
			[[ $tid =~ ^[0-9]+$ ]] || continue
			printf '%s\n' "$tid"
			{ read -r stat; } 2>/dev/null < "$dir/stat" || continue
			[[ $stat == "$tid ("* && $stat == *") "* ]] || continue
			read -ra fields <<< "${stat##*) }"
			[[ ${fields[19]:-} =~ ^[0-9]+$ ]] || continue
			{ read -r runtime wait slices; } 2>/dev/null < "$dir/schedstat" || continue
			[[ $runtime =~ ^[0-9]+$ && $wait =~ ^[0-9]+$ && $slices =~ ^[0-9]+$ ]] || continue
			{ read -r stat; } 2>/dev/null < "$dir/stat" || continue
			[[ $stat == "$tid ("* && $stat == *") "* ]] || continue
			identity=${fields[19]}
			read -ra fields <<< "${stat##*) }"
			[[ ${fields[19]:-} == "$identity" ]] || continue
			printf '%s:%s %s %s %s\n' "$tid" "$identity" "$runtime" "$wait" "$slices"
		done
	} | jq -Rsc --argjson enabled "$enabled" '
		split("\n") | map(select(length>0)) as $lines |
		{enabled:$enabled, visible:([$lines[] | select(contains(" ")|not)]|length),
		 threads:($lines | map(select(contains(" ")) | split(" ")
		  | {key:.[0], value:(.[1:] | map(tonumber))}) | from_entries)}'
}

benchmark_sched_deltas() {
	local before=$1 after=$2 voters=$3
	jq -nc --argjson b "$before" --argjson a "$after" --argjson voters "$voters" '
		[range($voters | length) as $i |
		 $b[$i] as $old | $a[$i] as $new |
		 ($old.threads | keys) as $old_keys | ($new.threads | keys) as $new_keys |
		 [$old_keys[] as $id | select($new.threads[$id] != null) | $id] as $ids |
		 [$ids[] as $id | [range(3) as $j | $new.threads[$id][$j] - $old.threads[$id][$j]]] as $matched |
		 (if $old.enabled == true and $new.enabled == true then true
		  elif $old.enabled == false and $new.enabled == false then false else null end) as $enabled |
		 # The sysctl gates optional schedstats, not CONFIG_SCHED_INFO. A
		 # live nonzero proc counter is evidence even when the sysctl is 0.
		 (if ($matched | length) == 0 or any($matched[][]; . < 0) or
		    (any($ids[] as $id | $new.threads[$id][]; . > 0) | not) then "unavailable"
		  elif ($matched|length) != $old.visible or ($matched|length) != $new.visible
		    then "partial_thread_coverage" else "matched_threads" end) as $status |
		 {node:$voters[$i].node,pid:$voters[$i].pid,schedstats_enabled:$enabled,status:$status,
		  threads_before:$old.visible,threads_after:$new.visible,threads_matched:($matched|length),
		  runtime_ns:(if $status == "matched_threads" or $status == "partial_thread_coverage" then [$matched[][0]]|add else null end),
		  runqueue_wait_ns:(if $status == "matched_threads" or $status == "partial_thread_coverage" then [$matched[][1]]|add else null end)}]'
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
workload=${RHIZA_SERVER_BENCH_WORKLOAD:-sql}
publish_async=${RHIZA_SERVER_BENCH_ASYNC_PUBLISH:-0}
stage_probe=${RHIZA_ASYNC_STAGE_PROBE:-0}
provider=${RHIZA_SERVER_BENCH_PROVIDER:-s3}
gcs_log=${RHIZA_SERVER_BENCH_GCS_LOG:-}
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
if [[ $workload != sql && $workload != graph && $workload != kv ]]; then
	printf 'RHIZA_SERVER_BENCH_WORKLOAD must be sql, graph, or kv\n' >&2
	exit 2
fi
if [[ $provider != s3 && $provider != gcs ]] || [[ $provider == gcs && $failed_node != none ]]; then
	printf 'RHIZA_SERVER_BENCH_PROVIDER must be s3 or gcs; gcs requires a healthy lane\n' >&2
	exit 2
fi
if [[ -n $gcs_log ]] && [[ $provider != gcs || $gcs_log != /* || ! -f $gcs_log || ! -r $gcs_log || -L $gcs_log ]]; then
	printf 'RHIZA_SERVER_BENCH_GCS_LOG requires GCS and an absolute readable regular file\n' >&2
	exit 2
fi
if [[ $publish_async != 0 && $publish_async != 1 ]] ||
	[[ $publish_async == 1 && ( $durability != async || $failed_node != none ) ]] ||
	[[ $workload != sql && $failed_node != none ]]; then
	printf 'non-SQL workloads and forced Async publication require a healthy lane; publication requires Async\n' >&2
	exit 2
fi
# Exact-set Graph verification reads all rows under the materializer's 10,000-row cap.
if [[ $workload == graph && $requests -gt 10000 ]]; then
	printf 'Graph benchmark supports at most 10000 requests\n' >&2
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
voter_roots=${RHIZA_SERVER_BENCH_VOTER_ROOTS:-}
kernel_probe=${RHIZA_SERVER_BENCH_KERNEL_COUNTERS:-0}
if [[ $kernel_probe != 0 && ( $kernel_probe != 1 || -z $voter_roots || $failed_node != none || $(uname -s) != Linux ) ]]; then
	printf 'RHIZA_SERVER_BENCH_KERNEL_COUNTERS=1 requires Linux, voter roots and a healthy lane\n' >&2
	exit 2
fi
if [[ -n $voter_roots ]]; then
	if [[ ! $voter_roots =~ ^/[^:]+:/[^:]+:/[^:]+$ || $voter_roots =~ [[:cntrl:]] ]]; then
		printf 'RHIZA_SERVER_BENCH_VOTER_ROOTS must contain three colon-separated absolute directories\n' >&2
		exit 2
	fi
	IFS=: read -r -a voter_roots_array <<< "$voter_roots"
	for root in "${voter_roots_array[@]}"; do
		if [[ ! -d $root || ! -w $root || $(cd -P -- "$root" && pwd -P) != "$root" ]]; then
			printf 'voter root is not a canonical writable directory: %s\n' "$root" >&2
			exit 2
		fi
	done
fi

run_dir=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-server-bench.XXXXXX")
container="rhiza-bench-$provider-$$"
gcs_container_id=
bench_pid=
owned_voter_dirs=()
cleanup() {
	local cleanup_failed=0 voter_dir
	set +e
	stop_benchmark_child
	output_stem=$(basename "$output_file" .json)
	for log in "$run_dir"/node-*.log; do
		[[ -f $log ]] || continue
		cp "$log" "$(dirname "$output_file")/$output_stem-$(basename "$log")"
	done
	for log in "$run_dir"/gcs-*-classes.json; do
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
	for voter_dir in "${owned_voter_dirs[@]+${owned_voter_dirs[@]}}"; do
		if ! rm -rf -- "$voter_dir" || [[ -e $voter_dir || -L $voter_dir ]]; then
			printf 'could not verify removal of owned voter directory: %s\n' "$voter_dir" >&2
			cleanup_failed=1
		fi
	done
	if [[ $provider == s3 ]]; then
		docker rm -f "$container" >/dev/null 2>&1 || true
	elif [[ $gcs_container_id =~ ^[a-f0-9]{64}$ ]]; then
		if ! docker rm -f "$gcs_container_id" >/dev/null; then
			printf 'could not verify removal of owned GCS emulator %s\n' "$gcs_container_id" >&2
			cleanup_failed=1
		fi
	fi
	case "$run_dir" in
		"${TMPDIR:-/tmp}"/rhiza-server-bench.*) rm -rf -- "$run_dir" ;;
	esac
	return "$cleanup_failed"
}
trap 'exit_status=$?; trap - EXIT; cleanup || { [[ $exit_status != 0 ]] || exit_status=1; }; exit "$exit_status"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

binary_dir=${RHIZA_SERVER_BENCH_BINARY_DIR:-$run_dir}
expected_sha256=${RHIZA_SERVER_BENCH_EXPECTED_SHA256:-}
if [[ $binary_dir == "$run_dir" ]]; then
	(cd "$source_dir" && CGO_ENABLED=0 go build -o "$binary_dir/rhiza" ./cmd/rhiza)
	# Both server revisions use the same candidate client/oracle.
	(cd "$client_source_dir" && CGO_ENABLED=0 go build -o "$binary_dir/rhiza-bench" ./cmd/rhiza-bench)
elif [[ $binary_dir != /* || -z $expected_sha256 || ! -x $binary_dir/rhiza || ! -x $binary_dir/rhiza-bench ]]; then
	printf 'prebuilt benchmark requires an absolute binary directory, two executables, and expected hashes\n' >&2
	exit 2
fi
benchmark_sha256() {
	if command -v sha256sum >/dev/null; then
		sha256sum "$1" | awk '{print $1}'
	else
		shasum -a 256 "$1" | awk '{print $1}'
	fi
}
client_sha256=$(benchmark_sha256 "$binary_dir/rhiza-bench")
if [[ -n $expected_sha256 ]]; then
	server_sha256=$(benchmark_sha256 "$binary_dir/rhiza")
	if [[ "$server_sha256:$client_sha256" != "$expected_sha256" ]]; then
		printf 'benchmark server/client binary hash mismatch\n' >&2
		exit 2
	fi
fi

node_environment=(env)
if [[ $provider == gcs ]]; then
	# Same native JSON API as the embedded provider test. The Kind helper's
	# generated account uses an in-cluster token URI, so bind this ADC locally.
	gcs_image=floci/floci-gcp:0.11.0@sha256:eae9683d806ceb643bcb36aee97cdb8cad13c3e2959cd053bf40e481994fde12
	if [[ -n $gcs_log ]]; then
		gcs_endpoint=http://127.0.0.1:4588
		# The disposable sidecar can outlive individual benchmark samples.
		gcs_bucket="rhiza-server-bench-$(openssl rand -hex 12)"
	else
		# The periodic health GET would enter measured access-log windows.
		gcs_container_id=$(docker run --rm -d --no-healthcheck --name "$container" -p 127.0.0.1::4588 \
			-e QUARKUS_HTTP_ACCESS_LOG_ENABLED=true \
			-e 'QUARKUS_HTTP_ACCESS_LOG_PATTERN=RHIZA_GCS_ACCESS %m %R %s' "$gcs_image")
		[[ $gcs_container_id =~ ^[a-f0-9]{64}$ ]]
		gcs_address=$(docker port "$gcs_container_id" 4588/tcp)
		[[ $gcs_address =~ ^127\.0\.0\.1:[1-9][0-9]{0,4}$ ]]
		gcs_endpoint="http://$gcs_address"
		gcs_bucket="rhiza-server-bench-$$-${RANDOM}"
	fi
	gcs_project=rhiza-local-test
	for _ in {1..100}; do
		curl -q -fsS --noproxy '*' --max-time 1 "$gcs_endpoint/storage/v1/b?project=$gcs_project" >/dev/null 2>&1 && break
		sleep 0.1
	done
	curl -q -fsS --noproxy '*' --max-time 5 -H 'Content-Type: application/json' \
		-d "{\"name\":\"$gcs_bucket\"}" "$gcs_endpoint/storage/v1/b?project=$gcs_project" \
		| jq -e --arg bucket "$gcs_bucket" '.name == $bucket' >/dev/null
	(
		umask 077
		openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$run_dir/gcs-key.pem" 2>/dev/null
		jq -nc --rawfile key "$run_dir/gcs-key.pem" --arg project "$gcs_project" --arg token "$gcs_endpoint/token" \
			'{type:"service_account",project_id:$project,private_key_id:$project,private_key:$key,
			  client_email:("rhiza@" + $project + ".iam.gserviceaccount.com"),token_uri:$token}' >"$run_dir/gcs-adc.json"
	)
	node_environment=(env -i PATH="$PATH" HOME="$run_dir" TMPDIR="$run_dir" GCE_METADATA_HOST=127.0.0.1:1)
	object_env=(RHIZA_OBJSTORE_PROVIDER=gcs RHIZA_OBJSTORE_BUCKET="$gcs_bucket" STORAGE_EMULATOR_HOST="$gcs_endpoint"
		GOOGLE_APPLICATION_CREDENTIALS="$run_dir/gcs-adc.json")
else
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
	object_env=(RHIZA_OBJSTORE_PROVIDER=s3 RHIZA_OBJSTORE_ENDPOINT="127.0.0.1:$s3_port" RHIZA_OBJSTORE_BUCKET=rhiza
		RHIZA_OBJSTORE_REGION=us-east-1 RHIZA_OBJSTORE_INSECURE=true RHIZA_OBJSTORE_ACCESS_KEY=rhiza-e2e
		RHIZA_OBJSTORE_SECRET_KEY=rhiza-e2e-secret)
fi

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
admin_env=()
if [[ $publish_async == 1 ]]; then
	admin_token="benchmark-only-${run_dir##*/}"
	admin_env=(RHIZA_ADMIN_TOKEN="$admin_token")
fi
for i in 0 1 2; do
	voter_data_dir="$run_dir/node-$i"
	if [[ -n $voter_roots ]]; then
		voter_data_dir=$(mktemp -d "${voter_roots_array[i]}/rhiza-server-voter-n$i.XXXXXX")
		owned_voter_dirs+=("$voter_data_dir")
	fi
	peer_token_env=()
	[[ $legacy_membership -eq 1 ]] || peer_token_env=(RHIZA_PEER_TOKEN="n$i-token")
	"${node_environment[@]}" RHIZA_CLUSTER_ID=server-bench RHIZA_NODE_ID="n$i" \
		RHIZA_BIND_ADDR="127.0.0.1:$((base_http_port + i))" RHIZA_PEER_ADDR="127.0.0.1:$((base_peer_port + i))" \
		RHIZA_DATA_DIR="$voter_data_dir" RHIZA_CLUSTER_MEMBERS="$members" "${peer_token_env[@]+${peer_token_env[@]}}" \
		"${admin_env[@]+${admin_env[@]}}" \
		"${object_env[@]}" RHIZA_OBJSTORE_PREFIX=server-bench \
		RHIZA_ASYNC_STAGE_PROBE="$stage_probe" \
		RHIZA_OBJSTORE_DURABILITY="$durability" RHIZA_OBJSTORE_SYNC_INTERVAL=1h RHIZA_CHECKPOINT_INTERVAL=0 \
		"$binary_dir/rhiza" >"$run_dir/node-$i.log" 2>&1 &
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
	if [[ $stage_probe == 1 ]] && ! grep -Fxq 'ASYNC_STAGE_PROBE_READY=1' "$run_dir/node-$i.log"; then
		printf 'missing Async stage probe startup marker for voter n%s\n' "$i" >&2
		exit 1
	fi
done

target_index=${target_node#n}
target="http://127.0.0.1:$((base_http_port + target_index))"
case "$workload" in
	sql)
		curl -fsS -H 'Content-Type: application/json' \
			-d '{"request_id":"schema","sql":"CREATE TABLE benchmark_writes (id INTEGER PRIMARY KEY)"}' \
			"$target/sql/execute" >/dev/null
		request_path=/sql/execute
		request_body='{"request_id":"bench-{{id}}","sql":"INSERT INTO benchmark_writes(id) VALUES ({{id}})"}'
		oracle_flag=-verify-insert-receipt
		receipt_oracle=sql-insert-id-v1 ;;
	graph)
		request_path=/graph/execute
		request_body='{"request_id":"bench-{{id}}","cypher":"CREATE (:BenchmarkNode {id: $id})","args":{"id":{{id}}}}'
		oracle_flag=-verify-applied-receipt
		receipt_oracle=applied-mutation-v1 ;;
	kv)
		request_path=/kv/put
		request_body='{"request_id":"bench-{{id}}","key":"bench-{{id}}","value":"{{base64id}}"}'
		oracle_flag=-verify-applied-receipt
		receipt_oracle=applied-mutation-v1 ;;
esac
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

gcs_snapshot() {
	# Quarkus %R is the path; %U includes the query. Redact defensively before
	# writing snapshots and keep only shape metadata for malformed records.
	if [[ -n $gcs_log ]]; then
		cat -- "$gcs_log"
	else
		docker logs "$gcs_container_id" 2>&1
	fi | awk '
		match($0, /(^|[[:space:]])RHIZA_GCS_ACCESS([[:space:]]|$)/) {
			record = substr($0, RSTART)
			sub(/^[[:space:]]+/, "", record)
			sub(/[[:space:]]+$/, "", record)
			if (split(record, f, /[[:space:]]+/) != 4 || f[2] !~ /^[A-Z]+$/ ||
			    f[3] !~ /^\// || f[4] !~ /^[1-5][0-9][0-9]$/) {
				print "malformed\tRHIZA_GCS_ACCESS"
				next
			}
			query = index(f[3], "?") > 0
			fragment = index(f[3], "#") > 0
			shape = query ? (fragment ? "query_and_fragment" : "query") : (fragment ? "fragment" : "path_only")
			sub(/[?#].*$/, "", f[3])
			printf "%s\t%s %s %s %s\n", shape, f[1], f[2], f[3], f[4]
		}' >"$1"
}

gcs_phase() {
	local before=$1 after=$2 phase=$3 transport=$4 lines records classified shapes summary
	lines=$(wc -l <"$before")
	lines=$((lines + 0))
	if ! head -n "$lines" "$after" | cmp -s "$before" -; then
		printf 'GCS access log no longer extends the %s baseline\n' "$phase" >&2
		return 1
	fi
	records="$run_dir/gcs-$phase.log"
	tail -n "+$((lines + 1))" "$after" >"$records"
	classified=$(cut -f 2 "$records" | awk -v bucket="$gcs_bucket" -f "$client_source_dir/benchmarks/gcs-request-classes.awk") || return 1
	shapes=$(awk -F '\t' '
		{ counts[$1]++ }
		END {
			printf "{\"path_only\":%d,\"query\":%d,\"fragment\":%d,\"query_and_fragment\":%d,\"malformed\":%d}",
				counts["path_only"], counts["query"], counts["fragment"], counts["query_and_fragment"], counts["malformed"]
		}' "$records") || return 1
	summary="$run_dir/gcs-$phase-classes.json"
	jq -c --argjson transport "$transport" --argjson shapes "$shapes" '
		(.http_attempts - .auth_attempts) as $observed
		| . + {transport_attempts:$transport.http_requests,transport_counts:$transport,access_record_shapes:$shapes,
		       observed_storage_responses:$observed,
		       transport_minus_observed:(if ($transport.http_requests | type) == "number"
		                                  then $transport.http_requests - $observed else null end)}' \
		<<<"$classified" >"$summary" || return 1
	if ! jq -e '(.transport_attempts | type) == "number" and .transport_attempts >= 0
		and .unknown_attempts == 0 and .malformed_access_lines == 0
		and (.transport_attempts == 0 or .observed_storage_responses > 0)' "$summary" >/dev/null; then
		printf 'GCS %s phase lacks classifiable completed-request evidence\n' "$phase" >&2
		return 1
	fi
	cat "$summary"
}

object_before=$(object_stats)
if [[ $provider == gcs ]]; then
	gcs_snapshot "$run_dir/gcs-setup.log"
	[[ -s $run_dir/gcs-setup.log ]] || { printf 'missing native GCS access records after setup\n' >&2; exit 1; }
fi
resource_before=null
resource_summary=null
resource_status=not_collected_non_linux
host_pressure=null
kernel_counters=null
sample_benchmark_schedulers() {
	local i pid
	for i in 0 1 2; do
		pid=$(<"$run_dir/node-$i.pid")
		sample_benchmark_sched /proc "$pid"
	done | jq -sc .
}
sample_benchmark_voters() {
	local i pid
	for i in 0 1 2; do
		pid=$(<"$run_dir/node-$i.pid")
		sample_benchmark_process "n$i" "$pid" || {
			printf 'cannot sample Linux voter resources: n%s (pid %s)\n' "$i" "$pid" >&2
			return 1
		}
	done | jq -sc .
}
result_file="$run_dir/result.json"
control_args=()
if [[ $failed_node != none ]]; then
	resource_status=not_collected_fault_lane
	failed_index=${failed_node#n}
	failed_pid=$(<"$run_dir/node-$failed_index.pid")
	if ! jobs -pr | grep -qx "$failed_pid"; then printf 'fault target is not a current owned running child; refusing binding\n' >&2; exit 1; fi
	# Linux fault lanes only; bind exact start-time ticks as well as the PID.
	process_stat=$(cat "/proc/$failed_pid/stat")
	read -ra process_fields <<< "${process_stat##*) }"
	[[ ${process_fields[19]:-} =~ ^[0-9]+$ ]] || { printf 'fault target start identity unavailable\n' >&2; exit 1; }
	control_args=(-fault-control-file "$run_dir/fault-control.url" -fault-pid "$failed_pid" -fault-start-time "${process_fields[19]}")
elif [[ $(uname -s) == Linux ]]; then
	resource_hz=$(getconf CLK_TCK)
	[[ $resource_hz =~ ^[1-9][0-9]*$ ]]
	resource_before=$(sample_benchmark_voters)
	resource_status=measured
	host_before=$(sample_benchmark_host /proc)
fi
if [[ $kernel_probe == 1 ]]; then
	voter_devices=()
	for root in "${voter_roots_array[@]}"; do
		device=$(findmnt -nr -o MAJ:MIN --target "$root")
		[[ $device =~ ^[0-9]+:[0-9]+$ ]] || { printf 'cannot resolve voter block device\n' >&2; exit 1; }
		voter_devices+=("$device")
	done
	sched_before=$(sample_benchmark_schedulers)
	disk_before=$(sample_benchmark_disks /proc "${voter_devices[@]}")
fi
if [[ $stage_probe == 1 ]]; then
	printf 'ASYNC_WINDOW_BEGIN_NS=%s\n' "$(date +%s%N)" >&2
fi
"$binary_dir/rhiza-bench" -url "$target" -path "$request_path" -body "$request_body" \
	-n "$requests" -c "$concurrency" -commit-unknown-retries 3 "$oracle_flag" "${control_args[@]+${control_args[@]}}" >"$result_file" &
bench_pid=$!
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
wait_benchmark_child
if [[ $stage_probe == 1 ]]; then
	printf 'ASYNC_WINDOW_END_NS=%s\n' "$(date +%s%N)" >&2
fi
if [[ $kernel_probe == 1 ]]; then
	disk_after=$(sample_benchmark_disks /proc "${voter_devices[@]}")
	sched_after=$(sample_benchmark_schedulers)
fi
if [[ $resource_status == measured ]]; then
	host_after=$(sample_benchmark_host /proc)
fi
result=$(<"$result_file")
# Preserve the client result even when the subsequent correctness gate fails.
tee "$output_file" <<<"$result" >/dev/null
if [[ $resource_status == measured ]]; then
	resource_after=$(sample_benchmark_voters)
	resource_summary=$(benchmark_resources "$resource_before" "$resource_after" "$resource_hz")
	host_pressure=$(benchmark_host_pressure "$host_before" "$host_after")
fi
if [[ $kernel_probe == 1 ]]; then
	disk_summary=$(benchmark_disk_deltas "$disk_before" "$disk_after" "${voter_devices[@]}")
	sched_summary=$(benchmark_sched_deltas "$sched_before" "$sched_after" "$resource_before")
	kernel_counters=$(jq -c --argjson sched "$sched_summary" '. + {voter_sched:$sched}' <<<"$disk_summary")
	if jq -e 'any(.voter_sched[]; .runqueue_wait_ns == null)' <<<"$kernel_counters" >/dev/null; then
		printf 'kernel scheduling counters disabled or unavailable for at least one voter\n' >&2
	fi
fi
result_with_exit=$(jq --argjson status "$fault_exit_status" '. + {fault_exit_status:$status}' <<<"$result")
tee "$output_file" <<<"$result_with_exit" >/dev/null
if [[ $failed_node != none ]]; then
	overlap=$(jq -c '.fault_window' <<<"$result")
	if ! jq -e '.attempted == true and .signal_succeeded == true and .overlap == true and .witnesses > 0' <<<"$overlap" >/dev/null; then
		printf 'fault interval not qualified: %s\n' "$(jq -r '.reason // "missing_window"' <<<"$overlap")" >&2
		exit 1
	fi
fi
write_object_delta=null
publication=null
gcs_classes=null
gcs_publication=null
if [[ $failed_node == none && ( -n ${RHIZA_SERVER_BENCH_WORKLOAD:-} || $publish_async == 1 || $provider == gcs ) ]]; then
	write_object_after=$(object_stats)
	write_object_delta=$(object_stats_delta "$object_before" "$write_object_after")
fi
if [[ $provider == gcs ]]; then
	gcs_snapshot "$run_dir/gcs-write.snapshot"
	gcs_write=$(gcs_phase "$run_dir/gcs-setup.log" "$run_dir/gcs-write.snapshot" write "$write_object_delta")
fi
jq -e --argjson requests "$requests" --arg oracle "$receipt_oracle" \
	'.errors == 0 and .successes == $requests and .receipt_oracle == $oracle' <<<"$result" >/dev/null
case "$workload" in
	sql)
		count=$(curl -fsS -H 'Content-Type: application/json' \
			-d "{\"sql\":\"SELECT COUNT(*), COUNT(CASE WHEN id >= 0 AND id < $requests THEN 1 END) FROM benchmark_writes\",\"consistency\":\"linearizable\"}" \
			"$target/sql/query")
		verify_benchmark_rows "$requests" <<<"$count" ;;
	graph)
		rows=$(curl -fsS -H 'Content-Type: application/json' \
			-d '{"cypher":"MATCH (n:BenchmarkNode) RETURN n.id","consistency":"linearizable"}' "$target/graph/query")
		verify_graph_rows "$requests" <<<"$rows"
		count=$(jq -c '{exact_identity_rows:(.rows | length),applied_slot}' <<<"$rows") ;;
	kv)
		# One linearizable read establishes the read point on the same voter;
		# the immutable benchmark keys are then read without N read barriers.
		curl -fsS -H 'Content-Type: application/json' \
			-d '{"key":"bench-0","consistency":"linearizable"}' "$target/kv/get" >/dev/null
		count=$("$binary_dir/rhiza-bench" -url "$target" -path /kv/get \
			-body '{"key":"bench-{{id}}","consistency":"local"}' -n "$requests" -c "$concurrency" -verify-kv-value)
		jq -e --argjson requests "$requests" \
			'.errors == 0 and .successes == $requests and .receipt_oracle == "kv-benchmark-value-v1"' <<<"$count" >/dev/null ;;
esac
object_after=$(object_stats)
object_delta=$(object_stats_delta "$object_before" "$object_after")
object_metric_nodes=$(printf '%s\n' "${metric_nodes[@]}" | jq -R . | jq -sc .)
if [[ $provider == gcs ]]; then
	gcs_snapshot "$run_dir/gcs-verification.snapshot"
	gcs_verification=$(gcs_phase "$run_dir/gcs-write.snapshot" "$run_dir/gcs-verification.snapshot" verification \
		"$(object_stats_delta "$write_object_after" "$object_after")")
fi
if [[ $publish_async == 1 ]]; then
	cut_before=$(curl -fsS --max-time 5 "$target/recovery/status")
	publish_object_before=$(object_stats)
	if [[ $provider == gcs ]]; then gcs_snapshot "$run_dir/gcs-prepublication.snapshot"; fi
	publish_seconds=$(curl -fsS --max-time 35 -X POST -H "Authorization: Bearer $admin_token" \
		-o "$run_dir/publication.json" -w '%{time_total}' "$target/recovery/archive")
	publish_object_after=$(object_stats)
	publish_delta=$(object_stats_delta "$publish_object_before" "$publish_object_after")
	if [[ $provider == gcs ]]; then
		gcs_snapshot "$run_dir/gcs-publication.snapshot"
		gcs_publication=$(gcs_phase "$run_dir/gcs-prepublication.snapshot" "$run_dir/gcs-publication.snapshot" publication "$publish_delta")
	fi
	cut_after=$(curl -fsS --max-time 5 "$target/recovery/status")
	if ! jq -e '.status == "archived"' "$run_dir/publication.json" >/dev/null ||
		! verify_publication_cut "$target_node" "$cut_before" "$cut_after" >/dev/null; then
		printf 'Async publication did not cover the certified applied cut: before=%s after=%s\n' "$cut_before" "$cut_after" >&2
		exit 1
	fi
	publication=$(jq -nc --argjson seconds "$publish_seconds" --argjson before "$cut_before" --argjson after "$cut_after" \
		--argjson delta "$publish_delta" \
		'{forced:true,elapsed_seconds:$seconds,object_store_delta:$delta,before:$before,after:$after}')
fi
if [[ $provider == gcs ]]; then
	gcs_classes=$(jq -nc --arg image "$gcs_image" --argjson write "$gcs_write" \
		--argjson verification "$gcs_verification" --argjson publication "$gcs_publication" \
		'{scope:"completed requests observed in the native GCS emulator; not a billing statement",
			emulator_image:$image,write:$write,verification:$verification,publication:$publication}')
fi
runtime_failure_lines=0
for log in "$run_dir"/node-*.log; do
	log_failures=$(grep -Ei 'error|failed|timeout' "$log" | grep -Eivc 'failed to sufficiently increase receive buffer size' || true)
	runtime_failure_lines=$((runtime_failure_lines + log_failures))
done
jq -nc --argjson result "$result" --argjson count "$count" --argjson overlap "$overlap" --argjson fault_exit_status "$fault_exit_status" --arg client_revision "$client_revision" --arg client_sha256 "$client_sha256" --argjson concurrency "$concurrency" --argjson runtime_failure_lines "$runtime_failure_lines" --argjson max_fault_latency_ms "$max_fault_latency_ms" --argjson object_delta "$object_delta" --argjson object_metric_nodes "$object_metric_nodes" --arg failed_node "$failed_node" --arg target_node "$target_node" --arg durability "$durability" --arg fault_after "$fault_after" \
	'{transport:"HTTP client + three QUIC voters",client_revision:$client_revision,client_sha256:$client_sha256,receipt_oracle:"sql-insert-id-v1",fault_overlap:$overlap,fault_exit_status:$fault_exit_status,fault_instrumentation:(if $failed_node == "none" then "disabled" else "pidfd-single-coordinator-monotonic-interval-v2" end),durability_mode:$durability,ack_contract:(if $durability == "before-ack" then "quorum WAL sync and object-store durability before acknowledgement" else "quorum WAL sync; object-store publication async" end),failure_mode:(if $failed_node == "none" then "healthy" else "peer-sigkill-during-load" end),failed_node:$failed_node,target_node:$target_node,fault_after:(if $failed_node == "none" then null else $fault_after end),max_fault_latency_ms:(if $failed_node == "none" then null else $max_fault_latency_ms end),concurrency:$concurrency,object_metric_nodes:$object_metric_nodes,object_measurement:"after schema through final linearizable verification",object_store_delta:$object_delta,runtime_failure_lines:$runtime_failure_lines,result:$result,verification:$count}' \
	| jq -c --arg status "$resource_status" --argjson resources "$resource_summary" \
		--argjson host_pressure "$host_pressure" --argjson kernel_counters "$kernel_counters" \
		--arg workload "$workload" --arg oracle "$receipt_oracle" --argjson writes "$write_object_delta" --argjson publication "$publication" \
		--arg provider "$provider" --argjson gcs_classes "$gcs_classes" \
		'. + {workload:$workload,receipt_oracle:$oracle,voter_resource_status:$status,voter_resources:$resources,host_pressure:$host_pressure,kernel_counters:$kernel_counters,
			write_object_store_delta:$writes,async_publication:$publication,object_store_provider:$provider,gcs_request_classes:$gcs_classes}
		 | if $workload != "sql" then .object_measurement = "after setup through final verification" else . end' \
	| tee "$output_file"
jq -e '.runtime_failure_lines == 0 and (.failed_node == "none" or .result.max_ms <= .max_fault_latency_ms)' "$output_file" >/dev/null
