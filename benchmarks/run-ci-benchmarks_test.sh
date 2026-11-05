#!/bin/sh
# Real shell pipelines/worktree cleanup, mock Git/Go/binaries; no compilation.
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-bench-parser-test.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
mkdir -p "$scratch/bin" "$scratch/repo" "$scratch/tmp"
MOCK_REPO="$scratch/repo"
MOCK_RUN_LOG="$scratch/runs.tsv"
MOCK_BUILD_LOG="$scratch/builds.tsv"
MOCK_PROFILE_LOG="$scratch/profiles.tsv"
REAL_TEE=$(command -v tee)
export MOCK_REPO MOCK_RUN_LOG MOCK_BUILD_LOG MOCK_PROFILE_LOG REAL_TEE
cat > "$scratch/bin/git" <<'SH'
#!/bin/sh
set -eu
if [ "${1-}" = -C ]; then shift 2; fi
case "$1" in
    rev-parse)
        if [ "$2" = --show-toplevel ]; then printf '%s\n' "$MOCK_REPO"; else printf '%s\n' "${3%\^\{commit\}}"; fi
        ;;
    show) printf 'package fixture\n';;
    worktree)
        case "$2" in
            add)
                mkdir -p "$4/pkg/network" "$4/pkg/quepaxa" "$4/pkg/materializer" "$4/internal/types"
                ;;
            remove)
                case "$4" in "$TMPDIR"/rhiza-ci-bench.*/base|"$TMPDIR"/rhiza-ci-bench.*/candidate) rm -rf "$4";; *) exit 94;; esac
                ;;
            *) exit 95;;
        esac
        ;;
    *) exit 96;;
esac
SH
cat > "$scratch/bin/go" <<'SH'
#!/bin/sh
set -eu
case "$1" in
    version) echo 'go version mock';;
    tool)
        if [ "${MOCK_PPROF_FAIL:-0}" -eq 1 ]; then echo 'pprof failure' >&2; exit 31; fi
        case "${MOCK_CONTENTION_PPROF_FAIL:-}:$*" in
            block:*-block.pprof|mutex:*-mutex.pprof) echo 'contention pprof failure' >&2; exit 37;;
        esac
        echo 'mock profile report'
        ;;
    test)
        [ "$#" -eq 5 ] && [ "$2" = -c ] && [ "$3" = -o ]
        printf '%s\n' "$5" >> "$MOCK_BUILD_LOG"
        cat > "$4" <<'BIN'
#!/bin/sh
set -eu
name=${0##*/}
label=${name%%-*}
package=${name#*-}
package=${package%.test}
bench= cpu= heap= benchtime=
block= mutex= block_rate= mutex_rate=
while [ "$#" -gt 0 ]; do
    case "$1" in
        -test.bench) bench=$2; shift;;
        -test.benchtime) benchtime=$2; shift;;
        -test.cpuprofile) cpu=$2; shift;;
        -test.memprofile) heap=$2; shift;;
        -test.blockprofile) block=$2; shift;;
        -test.blockprofilerate) block_rate=$2; shift;;
        -test.mutexprofile) mutex=$2; shift;;
        -test.mutexprofilefraction) mutex_rate=$2; shift;;
        -test.list) echo BenchmarkMock; exit 0;;
    esac
    shift
done
if [ -n "$cpu" ]; then
    [ "$benchtime" = 3s ]
    printf '%s\t%s\t%s\n' "$label" "$package" "$bench" >> "$MOCK_PROFILE_LOG"
    if [ "${MOCK_PROFILE_FAIL:-0}" -eq 1 ]; then echo 'profile failure stderr retained' >&2; exit 29; fi
    : > "$cpu"
    : > "$heap"
    if [ "${MOCK_EMPTY_PROFILE:-0}" -eq 0 ]; then
        printf 'profile\n' > "$cpu"
        printf 'profile\n' > "$heap"
    fi
    if [ "$package" = network ]; then
        [ -n "$block" ] && [ -n "$mutex" ] && [ "$block_rate" = 1 ] && [ "$mutex_rate" = 1 ]
        : > "$block"
        : > "$mutex"
        if [ "${MOCK_EMPTY_CONTENTION:-}" != block ]; then printf 'block profile\n' > "$block"; fi
        if [ "${MOCK_EMPTY_CONTENTION:-}" != mutex ]; then printf 'mutex profile\n' > "$mutex"; fi
    else
        [ -z "$block$mutex$block_rate$mutex_rate" ]
    fi
    if [ "${MOCK_PROFILE_NO_MATCH:-0}" -eq 1 ]; then echo 'PASS'; exit 0; fi
else
    [ -z "$block$mutex$block_rate$mutex_rate" ]
    printf '%s\t%s\t%s\n' "$label" "$package" "$bench" >> "$MOCK_RUN_LOG"
    suffix=$(printf '%s' "$bench" | tr -cs 'A-Za-z0-9' '_')
    measurement="BenchmarkMock_${package}_${suffix}-2"
fi
case "$bench" in
    '^BenchmarkThreePeerSQLExecute$/^c4$') measurement=BenchmarkThreePeerSQLExecute/c4-2;;
    '^BenchmarkThreePeerSQLExecuteReturning$/^rows1$') measurement=BenchmarkThreePeerSQLExecuteReturning/rows1-2;;
    '^BenchmarkSQLBatchCodec$/^blob_1k_encode$') measurement=BenchmarkSQLBatchCodec/blob_1k_encode-2;;
esac
# Go prints the name before calibration; stderr must not split that line.
printf '%s' "$measurement"
printf 'stderr-only diagnostic\nBenchmarkStderrIsNotMeasurement\n' >&2
printf '\t100\t1.0 ns/op\t8 B/op\t1 allocs/op\n'
if [ "${MOCK_FAIL_LABEL:-}" = "$label" ] && [ "$package" = qlog ]; then
    awk 'BEGIN {for(i=0;i<1500;i++) print "bounded stderr padding 01234567890123456789012345678901234567890123456789"}' >&2
    printf 'failure-tail-retained\n' >&2
    exit 17
fi
BIN
        chmod +x "$4"
        ;;
    *) exit 97;;
esac
SH
cat > "$scratch/bin/uname" <<'SH'
#!/bin/sh
echo 'mock Linux runner'
SH
cat > "$scratch/bin/tee" <<'SH'
#!/bin/sh
if [ "${MOCK_TEE_FAIL:-0}" -eq 1 ]; then cat >/dev/null; exit 23; fi
exec "$REAL_TEE" "$@"
SH
chmod +x "$scratch/bin/git" "$scratch/bin/go" "$scratch/bin/uname" "$scratch/bin/tee"
PATH="$scratch/bin:$PATH"
TMPDIR="$scratch/tmp"
export PATH TMPDIR
unset RHIZA_BENCH_COUNT RHIZA_BENCH_PROCS RHIZA_BENCH_TIME RHIZA_BENCH_NETWORK_OPTIMIZATION
run_case() {
    label=$1 expected=$2
    : > "$MOCK_RUN_LOG"
    : > "$MOCK_BUILD_LOG"
    : > "$MOCK_PROFILE_LOG"
    if bash "$script_dir/run-ci-benchmarks.sh" base candidate "$scratch/$label" > "$scratch/$label.log" 2>&1; then actual=0; else actual=$?; fi
    [ "$actual" -eq "$expected" ] || { tail -12 "$scratch/$label.log"; echo "FAIL $label exit=$actual expected=$expected"; exit 1; }
    [ -z "$(find "$TMPDIR" -mindepth 1 -print)" ] || { echo 'FAIL retained owned worktree/capture'; exit 1; }
    printf 'PASS %s exit=%s owned_resources=0\n' "$label" "$actual"
}
run_case success 0
[ "$(wc -l < "$MOCK_BUILD_LOG" | tr -d ' ')" -eq 12 ]
for label in base candidate; do
    awk '/^Benchmark/ { if ($2 !~ /^[0-9]+$/ || $4!="ns/op") exit 1; n++ } END { if(n!=140) exit 1 }' "$scratch/success/$label.txt"
    ! grep -Eq 'stderr-only|BenchmarkStderr' "$scratch/success/$label.txt"
    awk -F '\t' -v label="$label" '$1==label {count[$2]++} END {if(count["qlog"]!=10 || count["quepaxa"]!=40 || count["recovery"]!=30 || count["materializer"]!=40 || count["network"]!=10 || count["internal-types"]!=10) exit 1}' "$MOCK_RUN_LOG"
done
MOCK_FAIL_LABEL=candidate; export MOCK_FAIL_LABEL
run_case candidate-failure 17
grep -q 'failure-tail-retained' "$scratch/candidate-failure/diagnostics/candidate-qlog-1-1.stderr.log"
grep -q '^binary_exit=17$' "$scratch/candidate-failure/diagnostics/candidate-qlog-1-1.exit"
[ "$(wc -c < "$scratch/candidate-failure/diagnostics/candidate-qlog-1-1.stderr.log" | tr -d ' ')" -le 65536 ]
MOCK_FAIL_LABEL=base; export MOCK_FAIL_LABEL
run_case baseline-failure 0
[ "$(grep -c '^baseline qlog benchmark failed;' "$scratch/baseline-failure/base.txt")" -eq 10 ]
grep -q '^binary_exit=17$' "$scratch/baseline-failure/diagnostics/base-qlog-1-1.exit"
grep -q 'failure-tail-retained' "$scratch/baseline-failure/diagnostics/base-qlog-1-1.stderr.log"
unset MOCK_FAIL_LABEL
MOCK_TEE_FAIL=1; export MOCK_TEE_FAIL
run_case stdout-failure 23
grep -q '^binary_exit=0$' "$scratch/stdout-failure/diagnostics/base-qlog-1-1.exit"
grep -q '^stdout_exit=23$' "$scratch/stdout-failure/diagnostics/base-qlog-1-1.exit"
unset MOCK_TEE_FAIL
RHIZA_BENCH_NETWORK_OPTIMIZATION=sql; export RHIZA_BENCH_NETWORK_OPTIMIZATION
run_case sql-success 0
[ "$(wc -l < "$MOCK_BUILD_LOG" | tr -d ' ')" -eq 2 ]
[ "$(wc -l < "$MOCK_PROFILE_LOG" | tr -d ' ')" -eq 4 ]
awk -F '\t' '{block=int((NR-1)/2)%4; expected=(block==0 || block==3)?"base":"candidate"; if($1!=expected || $2!="network") exit 1} END {if(NR!=40) exit 1}' "$MOCK_RUN_LOG"
for label in base candidate; do
    awk '/^Benchmark/ {if($2!~/^[0-9]+$/ || $4!="ns/op") exit 1; count[$1]++} END {for(name in count) names++; if(count["BenchmarkThreePeerSQLExecute/c4-2"]!=10 || count["BenchmarkThreePeerSQLExecuteReturning/rows1-2"]!=10 || names!=2) exit 1}' "$scratch/sql-success/$label.txt"
    for profile in sql-execute-c4 sql-returning-rows1; do
        prefix="$scratch/sql-success/profiles/$label-$profile"
        [ "$(cat "$prefix.exit")" -eq 0 ]
        for suffix in cpu.pprof heap.pprof block.pprof mutex.pprof cpu-top.txt cpu-cum-top.txt alloc-space-top.txt alloc-objects-top.txt block-top.txt block-cum-top.txt mutex-top.txt mutex-cum-top.txt; do [ -s "$prefix-$suffix" ]; done
        ! grep -Eq 'stderr-only|BenchmarkStderr' "$prefix.txt"
        grep -q 'stderr-only' "$prefix.stderr.log"
    done
done
MOCK_PROFILE_FAIL=1; export MOCK_PROFILE_FAIL
run_case sql-profile-failure 29
[ "$(cat "$scratch/sql-profile-failure/profiles/base-sql-execute-c4.exit")" -eq 29 ]
grep -q 'profile failure stderr retained' "$scratch/sql-profile-failure/profiles/base-sql-execute-c4.stderr.log"
unset MOCK_PROFILE_FAIL
MOCK_PROFILE_NO_MATCH=1; export MOCK_PROFILE_NO_MATCH
run_case sql-profile-nonmatch 1
[ "$(cat "$scratch/sql-profile-nonmatch/profiles/base-sql-execute-c4.exit")" -eq 1 ]
unset MOCK_PROFILE_NO_MATCH
MOCK_EMPTY_PROFILE=1; export MOCK_EMPTY_PROFILE
run_case sql-empty-profile 1
[ "$(cat "$scratch/sql-empty-profile/profiles/base-sql-execute-c4.exit")" -eq 1 ]
unset MOCK_EMPTY_PROFILE
MOCK_PPROF_FAIL=1; export MOCK_PPROF_FAIL
run_case sql-pprof-failure 31
[ "$(cat "$scratch/sql-pprof-failure/profiles/base-sql-execute-c4.exit")" -eq 31 ]
unset MOCK_PPROF_FAIL
for kind in block mutex; do
    MOCK_EMPTY_CONTENTION=$kind; export MOCK_EMPTY_CONTENTION
    run_case "sql-empty-$kind-profile" 1
    [ "$(cat "$scratch/sql-empty-$kind-profile/profiles/base-sql-execute-c4.exit")" -eq 1 ]
done
unset MOCK_EMPTY_CONTENTION
for kind in block mutex; do
    MOCK_CONTENTION_PPROF_FAIL=$kind; export MOCK_CONTENTION_PPROF_FAIL
    run_case "sql-$kind-pprof-failure" 37
    [ "$(cat "$scratch/sql-$kind-pprof-failure/profiles/base-sql-execute-c4.exit")" -eq 37 ]
done
echo 'PASS interleaved stderr isolation, all ten paired samples/six packages, original failure and bounded diagnostics'
