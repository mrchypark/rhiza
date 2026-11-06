#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# Sourcing is deliberately limited to the runner's real small helpers: no build,
# server, Docker or benchmark invocation occurs in this test.
source "$here/run-server-reference.sh"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-server-oracle-test.XXXXXX")
scratch=$(cd "$scratch" && pwd -P)
bench_pid=
cleanup_test() {
	stop_benchmark_child
	if [[ ${test_status:-1} == 0 ]]; then rm -rf -- "$scratch"; else printf 'preserved failure evidence: %s\n' "$scratch" >&2; fi
}
trap 'test_status=$?; trap - EXIT; cleanup_test; exit "$test_status"' EXIT

# Unique INTEGER PRIMARY KEY rows: [0,4) is valid; shifted [1,5) has only 3
# in-range rows despite total cardinality 4. Extra negative/out-of-range rows fail.
verify_benchmark_rows 4 <<< '{"rows":[[4,4]]}'
for rows in '[[4,3]]' '[[5,4]]' '[[3,3]]' '[[4]]' '[["4",4]]'; do
	if verify_benchmark_rows 4 <<< "{\"rows\":$rows}"; then printf 'invalid exact set accepted: %s\n' "$rows" >&2; exit 1; fi
done

# Probe failure is not silently converted to availability or successful overlap.
control_url=http://127.0.0.1:1/owned
curl() { return 22; }
if fault_overlap_probe begin >"$scratch/probe.out" 2>"$scratch/probe.err"; then exit 1; fi
grep -q 'fault overlap not qualified' "$scratch/probe.err"
unset -f curl

# Normal wait preserves the child's exit and clears the handle before cleanup.
(exit 0) & bench_pid=$!
wait_benchmark_child
[[ -z $bench_pid ]]
(exit 19) & bench_pid=$!
status=0
wait_benchmark_child || status=$?
[[ $status == 19 && -z $bench_pid ]]

# A controlled fault command failure occurs while the exact owned client is
# alive. EXIT cleanup must kill/wait that child and preserve the original 47.
status=0
(
	source "$here/run-server-reference.sh"
	trap 'status=$?; trap - EXIT; stop_benchmark_child; printf "cleared=%s\n" "$bench_pid" > "$scratch/cleanup"; exit "$status"' EXIT
	sleep 60 & bench_pid=$!
	printf '%s\n' "$bench_pid" > "$scratch/client.pid"
	failed_pid=12345
	fault_overlap_probe() { return 0; }
	kill() { if [[ $1 == -KILL && $2 == 12345 ]]; then return 47; fi; builtin kill "$@"; }
	inject_benchmark_fault
) >"$scratch/fault.out" 2>"$scratch/fault.err" || status=$?
[[ $status == 47 ]]
grep -qx 'cleared=' "$scratch/cleanup"
owned_pid=$(<"$scratch/client.pid")
if kill -0 "$owned_pid" 2>/dev/null; then printf 'owned child survived cleanup\n' >&2; exit 1; fi

# Begin refusal must prevent the fault command; end refusal must remain a
# failure, never a retry that could manufacture an overlap witness.
failed_pid=12345
(
	fault_overlap_probe() { [[ $1 != begin ]] || return 23; }
	kill() { printf 'unexpected kill\n' >&2; return 99; }
	status=0; inject_benchmark_fault || status=$?
	[[ $status == 23 ]]
)
(
	fault_overlap_probe() { [[ $1 != end ]] || return 24; }
	kill() { return 0; }
	status=0; inject_benchmark_fault || status=$?
	[[ $status == 24 ]]
)
# Exercise the complete runner wiring with only tiny shell stand-ins. The fake
# go command copies these fixtures; it never invokes a compiler.
mkdir -p "$scratch/bin" "$scratch/base-server"
cat > "$scratch/bin/go" <<'MOCK'
#!/usr/bin/env bash
set -eu
printf '%s|%s\n' "$PWD" "$*" >> "$MOCK_HOME/builds"
while [[ $1 != -o ]]; do shift; done
output=$2
case $output in
*/rhiza-bench) cp "$MOCK_HOME/bench" "$output" ;;
*/rhiza) cp "$MOCK_HOME/node" "$output" ;;
*) exit 91 ;;
esac
MOCK
cat > "$scratch/bin/git" <<'MOCK'
#!/usr/bin/env bash
case "$*" in *rev-parse*) printf '%040d\n' 1 ;; *grep*) exit 0 ;; *) exit 92 ;; esac
MOCK
cat > "$scratch/bin/docker" <<'MOCK'
#!/usr/bin/env bash
exit 0
MOCK
cat > "$scratch/node" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$$" >> "$MOCK_HOME/nodes"
exec sleep 60
MOCK
cat > "$scratch/bench" <<'MOCK'
#!/usr/bin/env bash
set -eu
printf '%s\n' "$$" > "$MOCK_HOME/bench.pid"
printf '%s\n' "$*" > "$MOCK_HOME/bench.args"
control=
while [[ $# -gt 0 ]]; do
 case $1 in -fault-control-file) control=$2; shift ;; esac
 shift
done
if [[ -n $control ]]; then
 printf 'http://127.0.0.1:1234/0123456789abcdef0123456789abcdef' > "$control"
 while [[ ! -f $MOCK_HOME/release ]]; do sleep 0.01; done
fi
printf '{"requests":4,"successes":4,"errors":0,"max_ms":1,"receipt_oracle":"sql-insert-id-v1"}\n'
MOCK
cat > "$scratch/bin/curl" <<'MOCK'
#!/usr/bin/env bash
set -eu
for url in "$@"; do :; done
case $url in
*/metrics/object-store) printf '{"puts":0}\n' ;;
*/begin) case $MOCK_MODE in before-first|after-last) exit 22 ;; esac ;;
*/end)
 if [[ $MOCK_MODE == no-intersection ]]; then exit 22; fi
 touch "$MOCK_HOME/release"
 printf '{"overlap":true,"witnesses":1}\n' ;;
*/sql/query)
 if [[ $MOCK_MODE == shifted ]]; then printf '{"rows":[[4,3]]}\n'; else printf '{"rows":[[4,4]]}\n'; fi ;;
esac
MOCK
cat > "$scratch/fault-env" <<'MOCK'
kill() {
 if [[ ${MOCK_MODE:-} == fault-command-failure && $1 == -KILL && $2 != "$(cat "$MOCK_HOME/bench.pid")" ]]; then return 47; fi
 builtin kill "$@"
}
MOCK
chmod +x "$scratch/bin/"* "$scratch/node" "$scratch/bench"
for mode in normal shifted before-first after-last no-intersection fault-command-failure; do
 mkdir "$scratch/$mode"
 # Each run's stand-ins and process evidence remain in its own scratch scope.
 cp "$scratch/node" "$scratch/bench" "$scratch/$mode/"
 fault=n0
 [[ $mode != shifted ]] || fault=none
 status=0
 BASH_ENV="$scratch/fault-env" MOCK_HOME="$scratch/$mode" MOCK_MODE="$mode" PATH="$scratch/bin:$PATH" TMPDIR="$scratch/$mode" \
 RHIZA_SERVER_BENCH_REQUESTS=4 RHIZA_SERVER_BENCH_CONCURRENCY=2 RHIZA_SERVER_BENCH_FAULT_AFTER=0 RHIZA_SERVER_BENCH_FAILED_NODE="$fault" \
 bash "$here/run-server-reference.sh" "$scratch/base-server" "$scratch/$mode/result.json" >"$scratch/$mode/stdout" 2>"$scratch/$mode/stderr" || status=$?
 if [[ $mode == normal ]]; then
  [[ $status == 0 ]]
  jq -e '.fault_overlap.overlap == true and .receipt_oracle == "sql-insert-id-v1" and (.client_sha256 | length == 64)' "$scratch/$mode/result.json" >/dev/null
 else
  [[ $status != 0 ]]
  if [[ $mode == fault-command-failure ]]; then [[ $status == 47 ]];
  elif [[ $mode != shifted ]]; then grep -q 'fault overlap not qualified' "$scratch/$mode/stderr"; fi
 fi
 grep -q -- '-verify-insert-receipt' "$scratch/$mode/bench.args"
 grep -Fq "$scratch/base-server|build -o" "$scratch/$mode/builds"
 grep -Fq "$(cd "$here/.." && pwd)|build -o" "$scratch/$mode/builds"
 while IFS= read -r pid; do
  if kill -0 "$pid" 2>/dev/null; then printf 'mock owned process survived: %s\n' "$pid" >&2; exit 1; fi
 done < <(cat "$scratch/$mode/nodes" "$scratch/$mode/bench.pid")
done
printf 'server oracle helpers + six complete mocked runner cases PASS\n'
