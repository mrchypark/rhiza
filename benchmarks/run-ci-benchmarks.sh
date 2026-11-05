#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 3 ]]; then
	printf 'usage: %s BASE_SHA HEAD_SHA OUTPUT_DIR\n' "$0" >&2
	exit 2
fi

repo_root=$(git rev-parse --show-toplevel)
base_sha=$(git rev-parse --verify "$1^{commit}")
head_sha=$(git rev-parse --verify "$2^{commit}")
output_dir=$3
if [[ $output_dir != /* ]]; then
	output_dir="$repo_root/$output_dir"
fi
mkdir -p "$output_dir"

bench_count=${RHIZA_BENCH_COUNT:-10}
bench_time=${RHIZA_BENCH_TIME:-1s}
bench_procs=${RHIZA_BENCH_PROCS:-2}
network_optimization=${RHIZA_BENCH_NETWORK_OPTIMIZATION:-none}
packages=(qlog quepaxa recovery materializer network internal-types)
network_benchmark='^Benchmark(ThreePeerSQLExecute|CertifiedThreePeerSQLExecute|ThreePeerSQLExecuteReturning)'
case "$network_optimization" in
	none) ;;
	transport) packages=(network); network_benchmark='^BenchmarkBoundTransportQUICReadTip$' ;;
	auth) packages=(network); network_benchmark='^BenchmarkPeerAuthentication$' ;;
	catchup) packages=(network); network_benchmark='^BenchmarkCatchUpSameSourceQUIC$' ;;
	sql) packages=(network); network_benchmark='^Benchmark(ThreePeerSQLExecute|ThreePeerSQLExecuteReturning)$' ;;
	*) printf 'unknown RHIZA_BENCH_NETWORK_OPTIMIZATION: %s\n' "$network_optimization" >&2; exit 2 ;;
esac
if [[ ! $bench_count =~ ^[1-9][0-9]*$ || ! $bench_procs =~ ^[1-9][0-9]*$ ]]; then
	printf 'RHIZA_BENCH_COUNT and RHIZA_BENCH_PROCS must be positive integers\n' >&2
	exit 2
fi

work_root=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-ci-bench.XXXXXX")
capture_pid=
cleanup() {
	if [[ -n $capture_pid ]]; then
		kill "$capture_pid" 2>/dev/null || true
		wait "$capture_pid" 2>/dev/null || true
	fi
	git -C "$repo_root" worktree remove --force "$work_root/base" >/dev/null 2>&1 || true
	git -C "$repo_root" worktree remove --force "$work_root/candidate" >/dev/null 2>&1 || true
	case "$work_root" in
		"${TMPDIR:-/tmp}"/rhiza-ci-bench.*) rm -rf -- "$work_root" ;;
	esac
}
trap cleanup EXIT

git -C "$repo_root" worktree add --detach "$work_root/base" "$base_sha"
git -C "$repo_root" worktree add --detach "$work_root/candidate" "$head_sha"
mkdir -p "$work_root/bin"

# Both revisions must run identical benchmark code, including when the base
# predates these benchmarks. The fixtures use the pre-optimization Core API.
if [[ $network_optimization != none && $network_optimization != sql ]]; then
	for fixture in auth_benchmark_test.go transport_benchmark_test.go catchup_benchmark_test.go; do
		git show "$head_sha:pkg/network/$fixture" >"$work_root/base/pkg/network/$fixture"
	done
fi

# Keep new candidate-only measurements comparable by running identical fixtures
# against the base implementation.
for fixture in \
	internal/types/value_benchmark_test.go \
	pkg/quepaxa/durable_prefix_bench_test.go \
	pkg/materializer/sql_authorizer_bench_test.go \
	pkg/materializer/graph_prune_bench_test.go; do
	git show "$head_sha:$fixture" >"$work_root/base/$fixture"
done

build_revision() {
	local label=$1
	local source=$2
	local package test_package
	for package in "${packages[@]}"; do
		test_package="./pkg/$package"
		if [[ $package == internal-types ]]; then
			test_package=./internal/types
		fi
		(
			cd "$source"
			CGO_ENABLED=0 go test -c -o "$work_root/bin/$label-$package.test" "$test_package"
		)
	done
}

build_revision base "$work_root/base"
build_revision candidate "$work_root/candidate"
if [[ $network_optimization != none ]]; then
	for label in base candidate; do
		if ! "$work_root/bin/$label-network.test" -test.list "$network_benchmark" | grep -q '^Benchmark'; then
			printf '%s is missing the selected network benchmark\n' "$label" >&2
			exit 1
		fi
	done
fi

: >"$output_dir/base.txt"
: >"$output_dir/candidate.txt"
mkdir -p "$output_dir/diagnostics"

run_revision() {
	local label=$1
	local source=$2
	local output="$output_dir/$label.txt"
	local package benchmark sample_time benchmark_index stderr_log capture_fifo status capture_status
	local -a selected
	local -a statuses
	for package in "${packages[@]}"; do
		benchmark_index=0
		case "$package" in
			qlog) selected=('^BenchmarkWAL(AppendSync|ScanScratch)$') ;;
			quepaxa) selected=('^BenchmarkCorePropose(ThreePeersParallel|CertifiedThreePeersParallel)$' '^BenchmarkDecisionsFromBounded$' '^BenchmarkEnsureDurableThroughRetainedPrefix$' '^BenchmarkCoreReadIndexThreePeers$') ;;
			recovery) selected=('^BenchmarkArchiveMemoryPublication$' '^BenchmarkAuthenticatedMembershipCheckpointProof$' '^BenchmarkArchiveHeadEvidenceStages$') ;;
			materializer) selected=('^Benchmark(SQLBatchApply|GraphApply|GraphQuery4096Nodes|LatticeAppMetadataUpdate4096Keys)$' '^BenchmarkGraphSnapshotFreezeByDatabaseSize$' '^BenchmarkSQLAuthorizerSchemaCount$' '^BenchmarkGraphPruneSparseSlotsAndBatchSize$') ;;
			network)
				if [[ $network_optimization == sql ]]; then
					selected=('^BenchmarkThreePeerSQLExecute$/^c4$' '^BenchmarkThreePeerSQLExecuteReturning$/^rows1$')
				else
					selected=("$network_benchmark")
				fi
				;;
			internal-types) selected=('^BenchmarkSQLBatchCodec$') ;;
		esac
		# Go treats benchmark patterns with no match as a successful run, so
		# candidate-only benchmarks remain compatible with older baselines.
		for benchmark in "${selected[@]}"; do
			benchmark_index=$((benchmark_index + 1))
			sample_time=$bench_time
			# Snapshot setup is excluded from timing; bound iterations to avoid
			# calibration multiplying expensive untimed database updates.
			if [[ $benchmark == '^BenchmarkGraphSnapshotFreezeByDatabaseSize$' ]]; then
				sample_time=20x
			fi
			if [[ $benchmark == '^BenchmarkGraphPruneSparseSlotsAndBatchSize$' ]]; then
				sample_time=1x
			fi
			stderr_log="$output_dir/diagnostics/$label-$package-$sample-$benchmark_index.stderr.log"
			capture_fifo="$work_root/benchmark-stderr"
			mkfifo "$capture_fifo"
			# Keep only the last 64 KiB, without mixing calibration logs into the
			# benchmark name/iteration line or retaining an unbounded stderr file.
			tail -c 65536 <"$capture_fifo" >"$stderr_log" &
			capture_pid=$!
			if (
				cd "$source"
				GOMAXPROCS=$bench_procs "$work_root/bin/$label-$package.test" \
					-test.run '^$' \
					-test.bench "$benchmark" \
					-test.benchtime "$sample_time" \
					-test.count 1 \
					-test.benchmem \
					-test.cpu "$bench_procs" \
					-test.timeout 20m
			) 2>"$capture_fifo" | tee -a "$output"; then
				statuses=("${PIPESTATUS[@]}")
			else
				statuses=("${PIPESTATUS[@]}")
			fi
			if wait "$capture_pid"; then capture_status=0; else capture_status=$?; fi
			capture_pid=
			rm "$capture_fifo"
			status=${statuses[0]}
			# Match pipefail's rightmost nonzero status, including a failed sink.
			if ((statuses[1] != 0)); then status=${statuses[1]}; fi
			if ((status == 0 && capture_status != 0)); then status=$capture_status; fi
			printf 'binary_exit=%s\nstdout_exit=%s\nstderr_exit=%s\nexit=%s\n' "${statuses[0]}" "${statuses[1]}" "$capture_status" "$status" >"${stderr_log%.stderr.log}.exit"
			if [[ -s $stderr_log ]]; then cat "$stderr_log" >&2; fi
			if ((status == 0)); then continue; fi
			if [[ $label == candidate || $network_optimization != none ]]; then
				return "$status"
			fi
			printf 'baseline %s benchmark failed; continuing with available comparisons\n' "$package" | tee -a "$output"
		done
	done
}

for ((sample = 1; sample <= bench_count; sample++)); do
	printf 'benchmark sample %d/%d\n' "$sample" "$bench_count"
	if ((sample % 2 == 1)); then
		run_revision base "$work_root/base"
		run_revision candidate "$work_root/candidate"
	else
		run_revision candidate "$work_root/candidate"
		run_revision base "$work_root/base"
	fi
done

profile_revision() {
	local label=$1
	local source=$2
	local package=$3 profile_case=$4 benchmark=$5 measurement=$6
	local profile_dir="$output_dir/profiles"
	local prefix="$profile_dir/$label${profile_case:+-$profile_case}"
	local binary="$work_root/bin/$label-$package.test"
	local status capture_status
	local capture_fifo="$work_root/profile-stderr"
	set --
	if [[ $network_optimization == sql ]]; then
		set -- -test.blockprofile "$prefix-block.pprof" -test.blockprofilerate 1 \
			-test.mutexprofile "$prefix-mutex.pprof" -test.mutexprofilefraction 1
	fi
	mkdir -p "$profile_dir"
	# Profiling is a separate diagnostic run, never another paired sample.
	mkfifo "$capture_fifo"
	tail -c 65536 <"$capture_fifo" >"$prefix.stderr.log" &
	capture_pid=$!
	if (
		cd "$source"
		GOMAXPROCS=$bench_procs "$binary" \
			-test.run '^$' \
			-test.bench "$benchmark" \
			-test.benchtime 3s \
			-test.count 1 \
			-test.benchmem \
			-test.cpu "$bench_procs" \
			-test.cpuprofile "$prefix-cpu.pprof" \
			-test.memprofile "$prefix-heap.pprof" \
			"$@" \
			-test.timeout 1m
	) >"$prefix.txt" 2>"$capture_fifo"; then
		status=0
	else
		status=$?
	fi
	if wait "$capture_pid"; then capture_status=0; else capture_status=$?; fi
	capture_pid=
	rm "$capture_fifo"
	if ((status == 0 && capture_status != 0)); then status=$capture_status; fi
	# Go exits successfully when no benchmark matches, which is not evidence.
	if ((status == 0)); then
		if ! grep -Eq "^$measurement(-[1-9][0-9]*)?[[:space:]]+[1-9][0-9]*[[:space:]]+[0-9]+([.][0-9]+)?[[:space:]]+ns/op([[:space:]]|$)" "$prefix.txt" || [[ ! -s $prefix-cpu.pprof || ! -s $prefix-heap.pprof ]]; then
			printf 'diagnostic profile lacks its exact measurement or nonempty profiles: %s\n' "$label${profile_case:+-$profile_case}" >&2
			status=1
		fi
	fi
	if ((status == 0)) && [[ $network_optimization == sql && (! -s $prefix-block.pprof || ! -s $prefix-mutex.pprof) ]]; then
		printf 'diagnostic contention profile is empty: %s\n' "$label-$profile_case" >&2
		status=1
	fi
	printf '%s\n' "$status" >"$prefix.exit"
	if [[ -s $prefix.stderr.log ]]; then cat "$prefix.stderr.log" >&2; fi
	if ((status != 0)); then return "$status"; fi
	if go tool pprof -top "$binary" "$prefix-cpu.pprof" >"$prefix-cpu-top.txt" &&
		go tool pprof -top -cum "$binary" "$prefix-cpu.pprof" >"$prefix-cpu-cum-top.txt" &&
		go tool pprof -top -alloc_space "$binary" "$prefix-heap.pprof" >"$prefix-alloc-space-top.txt" &&
		go tool pprof -top -alloc_objects "$binary" "$prefix-heap.pprof" >"$prefix-alloc-objects-top.txt" &&
		test -s "$prefix-cpu-top.txt" && test -s "$prefix-cpu-cum-top.txt" &&
		test -s "$prefix-alloc-space-top.txt" && test -s "$prefix-alloc-objects-top.txt"; then
		status=0
	else
		status=$?
	fi
	if ((status == 0)) && [[ $network_optimization == sql ]]; then
		if go tool pprof -top "$binary" "$prefix-block.pprof" >"$prefix-block-top.txt" &&
			go tool pprof -top -cum "$binary" "$prefix-block.pprof" >"$prefix-block-cum-top.txt" &&
			go tool pprof -top "$binary" "$prefix-mutex.pprof" >"$prefix-mutex-top.txt" &&
			go tool pprof -top -cum "$binary" "$prefix-mutex.pprof" >"$prefix-mutex-cum-top.txt" &&
			test -s "$prefix-block-top.txt" && test -s "$prefix-block-cum-top.txt" &&
			test -s "$prefix-mutex-top.txt" && test -s "$prefix-mutex-cum-top.txt"; then
			status=0
		else
			status=$?
		fi
	fi
	printf '%s\n' "$status" >"$prefix.exit"
	return "$status"
}

if [[ $network_optimization == none ]]; then
	profile_revision base "$work_root/base" internal-types '' '^BenchmarkSQLBatchCodec$/^blob_1k_encode$' 'BenchmarkSQLBatchCodec/blob_1k_encode'
	profile_revision candidate "$work_root/candidate" internal-types '' '^BenchmarkSQLBatchCodec$/^blob_1k_encode$' 'BenchmarkSQLBatchCodec/blob_1k_encode'
elif [[ $network_optimization == sql ]]; then
	for label in base candidate; do
		profile_revision "$label" "$work_root/$label" network sql-execute-c4 '^BenchmarkThreePeerSQLExecute$/^c4$' 'BenchmarkThreePeerSQLExecute/c4'
		profile_revision "$label" "$work_root/$label" network sql-returning-rows1 '^BenchmarkThreePeerSQLExecuteReturning$/^rows1$' 'BenchmarkThreePeerSQLExecuteReturning/rows1'
	done
fi

{
	printf 'base_sha=%s\n' "$base_sha"
	printf 'candidate_sha=%s\n' "$head_sha"
	printf 'network_optimization=%s\n' "$network_optimization"
	printf 'samples=%s\n' "$bench_count"
	printf 'profiles=diagnostic-only-not-paired\n'
	printf 'benchtime=%s\n' "$bench_time"
	printf 'graph_snapshot_benchtime=20x\n'
	printf 'gomaxprocs=%s\n' "$bench_procs"
	printf 'go=%s\n' "$(go version)"
	printf 'kernel=%s\n' "$(uname -srvmo)"
} >"$output_dir/environment.txt"
