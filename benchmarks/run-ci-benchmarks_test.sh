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
bench= cpu= heap= benchtime=
while [ "$#" -gt 0 ]; do
    case "$1" in
        -test.bench) bench=$2; shift;;
        -test.benchtime) benchtime=$2; shift;;
        -test.cpuprofile) cpu=$2; shift;;
        -test.memprofile) heap=$2; shift;;
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
    suffix=$(printf '%s' "$bench" | tr -cs 'A-Za-z0-9' '_')
    measurement="BenchmarkMock_${package}_${suffix}-2"
fi
# Go prints the name before calibration; stderr must not split that line.
emit_measurement() {
    printf '%s' "$1"
    printf 'stderr-only diagnostic\nBenchmarkStderrIsNotMeasurement\n' >&2
    printf '\t100\t1.0 ns/op\t8 B/op\t1 allocs/op\n'
}
if [ "$bench" = '^BenchmarkAuthenticatedMembershipCheckpointProof$' ]; then
    for name in small-head duplicate-head-boundary near-max-decision-value real-large-history; do
        emit_measurement "BenchmarkAuthenticatedMembershipCheckpointProof/$name-2"
    done
else
    emit_measurement "$measurement"
fi
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
[ "$(wc -l < "$MOCK_PROFILE_LOG" | tr -d ' ')" -eq 2 ]
awk -F '\t' '{block=int((NR-1)/14)%4; expected=(block==0 || block==3)?"base":"candidate"; if($1!=expected) exit 1} END {if(NR!=280) exit 1}' "$MOCK_RUN_LOG"
for label in base candidate; do
    awk '/^Benchmark/ { if ($2 !~ /^[0-9]+$/ || $4!="ns/op") exit 1; n++ } END { if(n!=170) exit 1 }' "$scratch/success/$label.txt"
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
[ "$(grep -c '^baseline qlog benchmark failed;' "$scratch/baseline-failure/base.txt")" -eq 10 ]
grep -q '^binary_exit=17$' "$scratch/baseline-failure/diagnostics/base-qlog-1-1.exit"
grep -q 'failure-tail-retained' "$scratch/baseline-failure/diagnostics/base-qlog-1-1.stderr.log"
unset MOCK_FAIL_LABEL
MOCK_TEE_FAIL=1; export MOCK_TEE_FAIL
run_case stdout-failure 23
grep -q '^binary_exit=0$' "$scratch/stdout-failure/diagnostics/base-qlog-1-1.exit"
grep -q '^stdout_exit=23$' "$scratch/stdout-failure/diagnostics/base-qlog-1-1.exit"
unset MOCK_TEE_FAIL
echo 'PASS default paired stderr isolation, sample/package coverage, failure exits and owned cleanup'
