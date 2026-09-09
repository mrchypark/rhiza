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
packages=(qlog quepaxa materializer network)
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
cleanup() {
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

build_revision() {
	local label=$1
	local source=$2
	for package in "${packages[@]}"; do
		(
			cd "$source"
			CGO_ENABLED=0 go test -c -o "$work_root/bin/$label-$package.test" "./pkg/$package"
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

run_revision() {
	local label=$1
	local source=$2
	local output="$output_dir/$label.txt"
	local package benchmark
	for package in "${packages[@]}"; do
		case "$package" in
			qlog) benchmark='^BenchmarkWAL(AppendSync|ScanScratch)$' ;;
			quepaxa) benchmark='^BenchmarkCorePropose(ThreePeersParallel|CertifiedThreePeersParallel)$' ;;
			materializer) benchmark='^BenchmarkSQLBatchApply/(1|8|32|64|128)$' ;;
			network) benchmark=$network_benchmark ;;
		esac
		(
			cd "$source"
			GOMAXPROCS=$bench_procs "$work_root/bin/$label-$package.test" \
				-test.run '^$' \
				-test.bench "$benchmark" \
				-test.benchtime "$bench_time" \
				-test.count 1 \
				-test.benchmem \
				-test.cpu "$bench_procs" \
				-test.timeout 20m
		) 2>&1 | tee -a "$output" && continue
		if [[ $label == candidate || $network_optimization != none ]]; then
			return 1
		fi
		printf 'baseline %s benchmark failed; continuing with available comparisons\n' "$package" | tee -a "$output"
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

{
	printf 'base_sha=%s\n' "$base_sha"
	printf 'candidate_sha=%s\n' "$head_sha"
	printf 'network_optimization=%s\n' "$network_optimization"
	printf 'samples=%s\n' "$bench_count"
	printf 'benchtime=%s\n' "$bench_time"
	printf 'gomaxprocs=%s\n' "$bench_procs"
	printf 'go=%s\n' "$(go version)"
	printf 'kernel=%s\n' "$(uname -srvmo)"
} >"$output_dir/environment.txt"
