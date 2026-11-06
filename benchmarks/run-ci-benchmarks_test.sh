#!/bin/sh
# Real shell pipelines/worktree cleanup, mock Git/Go/binaries; no compilation.
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-bench-parser-test.XXXXXX")
trap 'status=$?; if [ "$status" -eq 0 ]; then rm -rf "$scratch"; else printf "FAIL evidence retained: %s\n" "$scratch" >&2; fi' EXIT
mkdir -p "$scratch/bin" "$scratch/repo" "$scratch/tmp"
MOCK_REPO="$scratch/repo"
MOCK_RUN_LOG="$scratch/runs.tsv"
MOCK_BUILD_LOG="$scratch/builds.tsv"
MOCK_PROFILE_LOG="$scratch/profiles.tsv"
REAL_TEE=$(command -v tee)
export MOCK_REPO MOCK_RUN_LOG MOCK_BUILD_LOG MOCK_PROFILE_LOG REAL_TEE
MOCK_MATRIX="$script_dir/ci-benchmark-matrix.txt"
export MOCK_MATRIX
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
    tool) echo 'mock profile report';;
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
bench= cpu= heap= benchtime= procs=2
while [ "$#" -gt 0 ]; do
    case "$1" in
        -test.bench) bench=$2; shift;;
        -test.benchtime) benchtime=$2; shift;;
        -test.cpuprofile) cpu=$2; shift;;
        -test.memprofile) heap=$2; shift;;
        -test.cpu) procs=$2; shift;;
        -test.list) echo BenchmarkMock; exit 0;;
    esac
    shift
done
if [ -n "$cpu" ]; then
    [ "$benchtime" = 3s ]
    printf '%s\t%s\t%s\n' "$label" "$package" "$bench" >> "$MOCK_PROFILE_LOG"
    printf 'profile\n' > "$cpu"
    printf 'profile\n' > "$heap"
    # Existing codec profiles merge stderr; only paired runs exercise split lines.
    [ "$bench" = '^BenchmarkSQLBatchCodec$/^blob_1k_encode$' ]
    printf 'codec profile diagnostic\n' >&2
    printf 'BenchmarkSQLBatchCodec/blob_1k_encode-2\t100\t1.0 ns/op\n'
    exit 0
else
    printf '%s\t%s\t%s\n' "$label" "$package" "$bench" >> "$MOCK_RUN_LOG"
fi
# Go prints the name before calibration; stderr must not split that line.
emit_measurement() {
    printf '%s' "$1"
    printf 'stderr-only diagnostic\nBenchmarkStderrIsNotMeasurement\n' >&2
    if [ "${MOCK_MALFORMED:-}" = "$label" ]; then ns=invalid; else ns=1.0; fi
    printf '\t100\t%s ns/op\t8 B/op\t1 allocs/op\n' "$ns"
}
sample=$(awk -F '\t' -v label="$label" -v package="$package" -v bench="$bench" '$1==label && $2==package && $3==bench {n++} END {print n}' "$MOCK_RUN_LOG")
awk -v package="$package" -v pattern="$bench" -v mode="${RHIZA_BENCH_NETWORK_OPTIMIZATION:-none}" -v procs="$procs" '$1==mode && $2==package {
    root=$3; sub(/\/.*/, "", root)
    name=$3
    if (name ~ /^Benchmark(Certified)?ThreePeerSQLExecute\/c[0-9]+$/) {
        c=name; sub(/.*\/c/, "", c); sub(/\/c[0-9]+$/, "/c" (c/2*procs), name)
    }
    if(root ~ pattern) print name
}' "$MOCK_MATRIX" |
while IFS= read -r name; do
    if [ "$label" = "${MOCK_OMIT_LABEL:-candidate}" ]; then
        case "${MOCK_OMIT:-}" in
            case) case "$name" in BenchmarkThreePeerSQLExecuteReturning/*) continue;; esac;;
            subcase) [ "$name" != BenchmarkThreePeerSQLExecuteReturning/rows100 ] || continue;;
            no-match) [ "$package" != network ] || continue;;
            nine) [ "$name:$sample" != BenchmarkThreePeerSQLExecuteReturning/rows100:10 ] || continue;;
        esac
    fi
    emit_measurement "$name-$procs"
done
if [ "${MOCK_EXTRA:-}" = "$label" ] && [ "$package" = network ]; then
    emit_measurement "BenchmarkThreePeerSQLExecuteReturning/rows10-$procs"
fi
if [ "${MOCK_FAIL_LABEL:-}" = "$label" ] && [ "$bench" = '^BenchmarkWAL(AppendSync|ScanScratch)$' ]; then
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
[ "$(wc -l < "$MOCK_PROFILE_LOG" | tr -d ' ')" -eq 2 ]
awk -F '\t' '{block=int((NR-1)/14)%4; expected=(block==0 || block==3)?"base":"candidate"; if($1!=expected) exit 1} END {if(NR!=280) exit 1}' "$MOCK_RUN_LOG"
for label in base candidate; do
    expected=$(awk '$1=="none" {n++} END {print n*10}' "$MOCK_MATRIX")
    awk -v expected="$expected" '/^Benchmark/ { if ($2 !~ /^[0-9]+$/ || $4!="ns/op") exit 1; n++ } END { if(n!=expected) exit 1 }' "$scratch/success/$label.txt"
    for name in small-head duplicate-head-boundary near-max-decision-value real-large-history; do
        [ "$(grep -c "^BenchmarkAuthenticatedMembershipCheckpointProof/$name-2[[:space:]]100[[:space:]]" "$scratch/success/$label.txt")" -eq 10 ]
    done
    ! grep -Eq 'stderr-only|BenchmarkStderr' "$scratch/success/$label.txt"
    grep -q '^codec profile diagnostic$' "$scratch/success/profiles/$label.txt"
    awk -F '\t' -v label="$label" '$1==label {count[$2]++} END {if(count["qlog"]!=10 || count["quepaxa"]!=40 || count["recovery"]!=30 || count["materializer"]!=40 || count["network"]!=10 || count["internal-types"]!=10) exit 1}' "$MOCK_RUN_LOG"
done
MOCK_FAIL_LABEL=candidate; export MOCK_FAIL_LABEL
run_case candidate-failure 17
grep -q 'failure-tail-retained' "$scratch/candidate-failure/diagnostics/candidate-qlog-1-1.stderr.log"
grep -q '^binary_exit=17$' "$scratch/candidate-failure/diagnostics/candidate-qlog-1-1.exit"
[ "$(wc -c < "$scratch/candidate-failure/diagnostics/candidate-qlog-1-1.stderr.log" | tr -d ' ')" -le 65536 ]
MOCK_FAIL_LABEL=base; export MOCK_FAIL_LABEL
run_case baseline-failure 0
[ "$(grep -c '^baseline qlog benchmark failed;' "$scratch/baseline-failure/unavailable.txt")" -eq 10 ]
! grep -q '^BenchmarkWAL' "$scratch/baseline-failure/base.txt"
grep -q '^BenchmarkCorePropose' "$scratch/baseline-failure/base.txt"
grep -q '^BenchmarkWAL' "$scratch/baseline-failure/diagnostics/base-qlog-1-1.stdout.log"
grep -q '^binary_exit=17$' "$scratch/baseline-failure/diagnostics/base-qlog-1-1.exit"
grep -q 'failure-tail-retained' "$scratch/baseline-failure/diagnostics/base-qlog-1-1.stderr.log"
unset MOCK_FAIL_LABEL
for missing in case subcase no-match nine; do
    MOCK_OMIT=$missing; export MOCK_OMIT
    run_case "candidate-missing-$missing" 1
done
MOCK_OMIT_LABEL=base; MOCK_OMIT=no-match; export MOCK_OMIT_LABEL MOCK_OMIT
run_case baseline-no-match 0
grep -q '^base unavailable: BenchmarkThreePeerSQLExecuteReturning/rows100 samples=0/10$' "$scratch/baseline-no-match/unavailable.txt"
unset MOCK_OMIT_LABEL MOCK_OMIT
MOCK_EXTRA=base; export MOCK_EXTRA
run_case baseline-extra-name 0
grep -q '^base baseline-only: BenchmarkThreePeerSQLExecuteReturning/rows10$' "$scratch/baseline-extra-name/unavailable.txt"
! grep -q 'Returning/rows10-' "$scratch/baseline-extra-name/base.txt"
grep -q 'Returning/rows100-' "$scratch/baseline-extra-name/base.txt"
grep -q 'Returning/rows10-' "$scratch/baseline-extra-name/diagnostics/base-network-1-1.stdout.log"
MOCK_EXTRA=candidate; export MOCK_EXTRA
run_case candidate-extra-name 1
unset MOCK_EXTRA
for label in candidate base; do
    MOCK_MALFORMED=$label; export MOCK_MALFORMED
    run_case "$label-malformed-ns" 1
done
unset MOCK_MALFORMED
RHIZA_BENCH_COUNT=1; export RHIZA_BENCH_COUNT
for mode in transport auth catchup; do
    RHIZA_BENCH_NETWORK_OPTIMIZATION=$mode; export RHIZA_BENCH_NETWORK_OPTIMIZATION
    run_case "focused-$mode" 0
done
unset RHIZA_BENCH_NETWORK_OPTIMIZATION
RHIZA_BENCH_PROCS=3; export RHIZA_BENCH_PROCS
run_case procs-three 0
grep -q '^BenchmarkThreePeerSQLExecute/c6-3' "$scratch/procs-three/candidate.txt"
grep -q '^BenchmarkCertifiedThreePeerSQLExecute/c96-3' "$scratch/procs-three/candidate.txt"
unset RHIZA_BENCH_COUNT RHIZA_BENCH_PROCS
MOCK_TEE_FAIL=1; export MOCK_TEE_FAIL
run_case stdout-failure 23
grep -q '^binary_exit=0$' "$scratch/stdout-failure/diagnostics/base-qlog-1-1.exit"
grep -q '^stdout_exit=23$' "$scratch/stdout-failure/diagnostics/base-qlog-1-1.exit"
unset MOCK_TEE_FAIL
# Exercise the actual workflow helper without Go, gateways, or containers.
export script_dir scratch
bash <<'SH'
set -eu
evidence_dir="$scratch/issue185"
mkdir -p "$evidence_dir/logs"
gateway_run_dirs() { :; }
wait_for_gateway_completion() { return 0; }
eval "$(awk '/^          run_logged\(\)/ {copy=1} copy {print} copy && /^          }$/ {exit}' "$script_dir/../.github/workflows/performance.yml")"
for scenario in pass empty warning failed; do
    case "$scenario" in
        pass) body="printf '%s\\n' '--- PASS: TestReal (0.00s)'"; expected=0;;
        empty) body="printf '%s\\n' 'testing: warning: no tests to run' 'PASS'"; expected=1;;
        warning) body="printf '%s\\n' 'PASS'"; expected=1;;
        failed) body="printf '%s\\n' '--- PASS: TestReal (0.00s)'; exit 17"; expected=17;;
    esac
    if run_logged "$scenario" sh -c "$body"; then actual=0; else actual=$?; fi
    test "$actual" -eq "$expected"
    printf 'PASS issue185-%s exit=%s\n' "$scenario" "$actual"
done
SH
echo 'PASS default paired stderr isolation, sample/package coverage, failure exits and owned cleanup'
