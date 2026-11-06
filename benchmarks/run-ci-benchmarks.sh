#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 3 ]]; then
	printf 'usage: %s BASE_SHA HEAD_SHA OUTPUT_DIR\n' "$0" >&2
	exit 2
fi

repo_root=$(git rev-parse --show-toplevel)
matrix=$(cd -- "$(dirname -- "$0")" && pwd)/ci-benchmark-matrix.txt
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
if [[ $network_optimization != none ]]; then
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
awk -v mode="$network_optimization" -v procs="$bench_procs" '$1 == mode {
	name=$3
	if (name ~ /^Benchmark(Certified)?ThreePeerSQLExecute\/c[0-9]+$/) {
		parallel=name; sub(/.*\/c/, "", parallel)
		sub(/\/c[0-9]+$/, "/c" (parallel / 2 * procs), name)
	}
	print name
}' "$matrix" >"$output_dir/expected-names.txt"
test -s "$output_dir/expected-names.txt"

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
			network) selected=("$network_benchmark") ;;
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
			# Bound diagnostics without splitting Go's benchmark name/iteration line.
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
			) 2>"$capture_fifo" | tee "${stderr_log%.stderr.log}.stdout.log"; then
				statuses=("${PIPESTATUS[@]}")
			else
				statuses=("${PIPESTATUS[@]}")
			fi
			if wait "$capture_pid"; then capture_status=0; else capture_status=$?; fi
			capture_pid=
			rm "$capture_fifo"
			status=${statuses[0]}
			# Preserve pipefail's rightmost nonzero exit, including a failed sink.
			if ((statuses[1] != 0)); then status=${statuses[1]}; fi
			if ((status == 0 && capture_status != 0)); then status=$capture_status; fi
			printf 'binary_exit=%s\nstdout_exit=%s\nstderr_exit=%s\nexit=%s\n' "${statuses[0]}" "${statuses[1]}" "$capture_status" "$status" >"${stderr_log%.stderr.log}.exit"
			if [[ -s $stderr_log ]]; then cat "$stderr_log" >&2; fi
			if ((status == 0)); then
				cat "${stderr_log%.stderr.log}.stdout.log" >>"$output"
				continue
			fi
			if [[ $label == candidate || $network_optimization != none ]]; then
				return "$status"
			fi
			printf 'baseline %s benchmark failed; raw retained, unavailable for comparison\n' "$package" | tee -a "$output_dir/unavailable.txt"
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

for label in candidate base; do
	awk -v expected="$bench_count" -v label="$label" -v unavailable="$output_dir/unavailable.txt" '
		NR == FNR { required[$1]=1; next }
		/^Benchmark/ {
			name=$1; sub(/-[0-9]+$/, "", name)
			if ($2 !~ /^[1-9][0-9]*$/ || $3 !~ /^[0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?$/ || $4 != "ns/op") { bad=1; next }
			count[name]++
			if (!(name in required)) {
				if (label == "candidate") { print "unexpected benchmark: " name > "/dev/stderr"; bad=1 }
				else if (!reported[name]++) print "base baseline-only: " name >> unavailable
			}
		}
		END {
			for (name in required) if (count[name] != expected) {
				message=label " unavailable: " name " samples=" (count[name]+0) "/" expected
				if (label == "candidate") { print message > "/dev/stderr"; bad=1 }
				else print message >> unavailable
			}
			exit bad
		}' "$output_dir/expected-names.txt" "$output_dir/$label.txt"
	# A partial baseline case is unavailable, not an apparently complete pair.
	if [[ $label == base ]]; then
		awk -v expected="$bench_count" -v names="$output_dir/expected-names.txt" '
			BEGIN { while ((getline name < names) > 0) required[name]=1; close(names) }
			FNR == NR { if ($1 ~ /^Benchmark/) { name=$1; sub(/-[0-9]+$/, "", name); count[name]++ }; next }
			# Preserve Go configuration headers: benchstat groups rows by pkg, etc.
			!/^Benchmark/ { print; next }
			{ name=$1; sub(/-[0-9]+$/, "", name); if (name in required && count[name] == expected) print }
		' "$output_dir/base.txt" "$output_dir/base.txt" >"$work_root/base-valid.txt"
		mv "$work_root/base-valid.txt" "$output_dir/base.txt"
	fi
done

profile_codec_revision() {
	local label=$1
	local source=$2
	local profile_dir="$output_dir/profiles"
	local binary="$work_root/bin/$label-internal-types.test"
	local status
	mkdir -p "$profile_dir"
	# Profiling is a separate diagnostic run, never another paired sample.
	if (
		cd "$source"
		GOMAXPROCS=$bench_procs "$binary" \
			-test.run '^$' \
			-test.bench '^BenchmarkSQLBatchCodec$/^blob_1k_encode$' \
			-test.benchtime 3s \
			-test.count 1 \
			-test.benchmem \
			-test.cpu "$bench_procs" \
			-test.cpuprofile "$profile_dir/$label-cpu.pprof" \
			-test.memprofile "$profile_dir/$label-heap.pprof" \
			-test.timeout 1m
	) >"$profile_dir/$label.txt" 2>&1; then
		printf '0\n' >"$profile_dir/$label.exit"
	else
		status=$?
		printf '%s\n' "$status" >"$profile_dir/$label.exit"
		cat "$profile_dir/$label.txt" >&2
		return "$status"
	fi
	# Go exits successfully when no benchmark matches, which is not evidence.
	grep -Eq '^BenchmarkSQLBatchCodec/blob_1k_encode(-[1-9][0-9]*)?[[:space:]]' "$profile_dir/$label.txt"
	test -s "$profile_dir/$label-cpu.pprof"
	test -s "$profile_dir/$label-heap.pprof"
	go tool pprof -top "$binary" "$profile_dir/$label-cpu.pprof" >"$profile_dir/$label-cpu-top.txt"
	go tool pprof -top -alloc_space "$binary" "$profile_dir/$label-heap.pprof" >"$profile_dir/$label-alloc-space-top.txt"
	go tool pprof -top -alloc_objects "$binary" "$profile_dir/$label-heap.pprof" >"$profile_dir/$label-alloc-objects-top.txt"
}

if [[ $network_optimization == none ]]; then
	profile_codec_revision base "$work_root/base"
	profile_codec_revision candidate "$work_root/candidate"
fi

{
	printf 'base_sha=%s\n' "$base_sha"
	printf 'candidate_sha=%s\n' "$head_sha"
	printf 'network_optimization=%s\n' "$network_optimization"
	printf 'samples=%s\n' "$bench_count"
	printf 'benchtime=%s\n' "$bench_time"
	printf 'graph_snapshot_benchtime=20x\n'
	printf 'gomaxprocs=%s\n' "$bench_procs"
	printf 'go=%s\n' "$(go version)"
	printf 'kernel=%s\n' "$(uname -srvmo)"
} >"$output_dir/environment.txt"
