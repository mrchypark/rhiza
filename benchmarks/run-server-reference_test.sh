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

# Normal wait preserves the child's exit and clears the handle before cleanup.
(exit 0) & bench_pid=$!
wait_benchmark_child
[[ -z $bench_pid ]]
(exit 19) & bench_pid=$!
status=0
wait_benchmark_child || status=$?
[[ $status == 19 && -z $bench_pid ]]

# Exercise the actual shared wait helper, including a real non-child wait
# error. Bash deliberately cannot distinguish explicit exit 137 from SIGKILL.
for raw in 0 137 139; do
	(exit "$raw") & failed_pid=$!
	status=0
	wait_fault_child 2>"$scratch/wait-$raw.err" || status=$?
	[[ $fault_exit_status == "$raw" && -z $failed_pid ]]
	if [[ $raw == 137 ]]; then [[ $status == 0 ]];
	elif [[ $raw == 0 ]]; then [[ $status == 1 ]];
	else [[ $status == "$raw" ]]; fi
done
failed_pid=$$
status=0
wait_fault_child 2>"$scratch/wait-error.err" || status=$?
[[ $status == 127 && $fault_exit_status == 127 && -z $failed_pid ]]

# A controlled fault command failure occurs while the exact owned client is
# alive. EXIT cleanup must kill/wait that child and preserve the original 47.
status=0
(
	source "$here/run-server-reference.sh"
	trap 'status=$?; trap - EXIT; stop_benchmark_child; builtin kill -KILL "$failed_pid" 2>/dev/null || true; wait "$failed_pid" 2>/dev/null || true; printf "cleared=%s\n" "$bench_pid" > "$scratch/cleanup"; exit "$status"' EXIT
	sleep 60 & bench_pid=$!
	printf '%s\n' "$bench_pid" > "$scratch/client.pid"
	sleep 60 & failed_pid=$!
	control_url=http://127.0.0.1:1/owned
	curl() { return 47; }
	inject_benchmark_fault
) >"$scratch/fault.out" 2>"$scratch/fault.err" || status=$?
[[ $status == 47 ]]
grep -qx 'cleared=' "$scratch/cleanup"
owned_pid=$(<"$scratch/client.pid")
if kill -0 "$owned_pid" 2>/dev/null; then printf 'owned child survived cleanup\n' >&2; exit 1; fi

# An unrelated PID and an already waited child must fail before the coordinator.
# The mocked curl cannot signal the negative target even if the guard regresses;
# the real owned benchmark child still has to be cleaned with original exit 1.
for ownership in unowned stale; do
	status=0
	(
		source "$here/run-server-reference.sh"
		failed_pid=$$
		if [[ $ownership == stale ]]; then (exit 0) & failed_pid=$!; wait "$failed_pid"; fi
		sleep 60 & bench_pid=$!
		printf '%s\n' "$bench_pid" > "$scratch/$ownership-client.pid"
		trap 'status=$?; trap - EXIT; stop_benchmark_child; printf "cleared=%s\n" "$bench_pid" > "$scratch/$ownership-cleanup"; exit "$status"' EXIT
		curl() { printf 'unexpected coordinator call\n' >> "$scratch/$ownership-calls"; return 99; }
		inject_benchmark_fault
	) >"$scratch/$ownership.out" 2>"$scratch/$ownership.err" || status=$?
	[[ $status == 1 && ! -e $scratch/$ownership-calls ]]
	grep -q 'not a current owned running child' "$scratch/$ownership.err"
	grep -qx 'cleared=' "$scratch/$ownership-cleanup"
	owned_pid=$(<"$scratch/$ownership-client.pid")
	if kill -0 "$owned_pid" 2>/dev/null; then printf 'negative-case client survived cleanup\n' >&2; exit 1; fi
done
# Exercise the complete runner wiring with only tiny shell stand-ins. The fake
# go command copies these fixtures; it never invokes a compiler.
for sample in '{"error_samples":{"http_500":"SECRET_SENTINEL"},"object_store_delta":{"gets":2,"private_count":918273645},"result":{"requests":1}}' '{"object_store_delta":{"gets":2,"private_count":918273645},"result":{"error_samples":{"http_500":"SECRET_SENTINEL"},"requests":1,"p99_ms":{"secret":"SECRET_SENTINEL"},"slot_evidence":{"private":"SECRET_SENTINEL","slot_samples":[{"slot":16,"latency_ns":{"secret":"SECRET_SENTINEL"},"secret":"SECRET_SENTINEL"}]}}}'; do
 sanitized=$(jq -f "$here/../scripts/ci/sanitize-before-ack-diagnostic.jq" <<<"$sample")
 [[ $sanitized != *SECRET_SENTINEL* && $sanitized != *918273645* ]]
 jq -e '.object_store_delta.gets == 2 and (.object_store_delta | has("private_count") | not) and .result.requests == 1' <<<"$sanitized" >/dev/null
done
# The upload allowlist must not accidentally include raw logs or arbitrary text.
workflow="$here/../.github/workflows/performance.yml"
! grep -Fq 'before-ack-sampled-diagnostics/*.txt' "$workflow"
! grep -Fq 'before-ack-sampled-diagnostics/*.log' "$workflow"
grep -Fq 'before-ack-sampled-diagnostics/*-sanitized.json' "$workflow"
grep -Fq 'before-ack-sampled-diagnostics/concurrency-after.json' "$workflow"
grep -Fq '"$dir/.runs-after.ndjson" >/dev/null' "$workflow"
overlap_filter="$here/../scripts/ci/assess-performance-overlap.jq"
run_assessment() {
 jq -s -e --arg own 1 --arg start '2026-01-01T10:00:00Z' --arg end '2026-01-01T10:05:00Z' -f "$overlap_filter"
}
no_overlap='{"id":1,"status":"in_progress","created_at":"2026-01-01T09:00:00Z","run_started_at":"2026-01-01T09:00:00Z","updated_at":"2026-01-01T10:00:00Z"}
{"id":2,"status":"completed","created_at":"2026-01-01T08:00:00Z","run_started_at":"2026-01-01T08:00:00Z","updated_at":"2026-01-01T09:59:59Z"}'
run_assessment <<<"$no_overlap" | jq -e '.overlaps | length == 0' >/dev/null
for bad in '2026-99-99T99:99:99Z' '2026-02-30T10:00:00Z'; do
 if jq -s -e --arg own 1 --arg start "$bad" --arg end '2026-01-01T10:05:00Z' -f "$overlap_filter" <<<"$no_overlap" >/dev/null 2>&1; then
  printf 'impossible start timestamp accepted\n' >&2; exit 1
 fi
 if jq -s -e --arg own 1 --arg start '2026-01-01T10:00:00Z' --arg end "$bad" -f "$overlap_filter" <<<"$no_overlap" >/dev/null 2>&1; then
  printf 'impossible end timestamp accepted\n' >&2; exit 1
 fi
done
if jq -s -e --arg own 1 --arg start '2026-01-01T10:05:00Z' --arg end '2026-01-01T10:00:00Z' -f "$overlap_filter" <<<"$no_overlap" >/dev/null 2>&1; then
 printf 'reversed measurement interval accepted\n' >&2; exit 1
fi
jq -s -e --arg own 1 --arg start '2026-01-01T10:00:00Z' --arg end '2026-01-01T10:00:00Z' -f "$overlap_filter" <<<"$no_overlap" | jq -e '.overlaps | length == 0' >/dev/null
if run_assessment <<<'{"id":1,"status":"in_progress","created_at":"2026-02-30T10:00:00Z","run_started_at":"2026-02-30T10:00:00Z","updated_at":"2026-03-01T10:00:00Z"}' >/dev/null 2>&1; then
 printf 'impossible run timestamp accepted\n' >&2; exit 1
fi
if run_assessment <<<'{"id":1,"status":"in_progress","created_at":"2026-01-01T09:00:00Z","run_started_at":"2026-01-01T10:00:01Z","updated_at":"2026-01-01T10:00:00Z"}' >/dev/null 2>&1; then
 printf 'run start later than update accepted\n' >&2; exit 1
fi
if run_assessment <<<'{"id":1,"status":"in_progress","created_at":"2026-01-01T09:30:00Z","run_started_at":"2026-01-01T09:00:00Z","updated_at":"2026-01-01T10:00:00Z"}' >/dev/null 2>&1; then
 printf 'run created later than start accepted\n' >&2; exit 1
fi
if run_assessment <<<'{"id":1,"status":"queued","created_at":"2026-01-01T10:00:01Z","run_started_at":null,"updated_at":"2026-01-01T10:00:00Z"}' >/dev/null 2>&1; then
 printf 'queued run created later than update accepted\n' >&2; exit 1
fi
if run_assessment <<<'{"id":1,"status":"completed","created_at":"2026-01-01T10:00:02Z","run_started_at":"2026-01-01T10:00:01Z","updated_at":"2026-01-01T10:00:00Z"}' >/dev/null 2>&1; then
 printf 'started run created later than update accepted\n' >&2; exit 1
fi
run_assessment <<<'{"id":1,"status":"in_progress","created_at":"2026-01-01T09:00:00Z","run_started_at":"2026-01-01T10:00:00Z","updated_at":"2026-01-01T10:00:00Z"}' | jq -e '.overlaps | length == 0' >/dev/null
run_assessment <<<'{"id":1,"status":"in_progress","created_at":"2026-01-01T09:00:00Z","run_started_at":"2026-01-01T09:00:00Z","updated_at":"2026-01-01T10:00:00Z"}' | jq -e '.overlaps | length == 0' >/dev/null
run_assessment <<<'{"id":1,"status":"completed","created_at":"2026-01-01T10:00:00Z","run_started_at":"2026-01-01T10:00:00Z","updated_at":"2026-01-01T10:00:00Z"}' | jq -e '.overlaps | length == 0' >/dev/null
run_assessment <<<'{"id":1,"status":"queued","created_at":"2026-01-01T10:00:00Z","run_started_at":null,"updated_at":"2026-01-01T10:00:00Z"}' | jq -e '.overlaps | length == 0' >/dev/null
run_assessment <<<'{"id":1,"status":"queued","created_at":"2026-01-01T09:00:00Z","run_started_at":null,"updated_at":"2026-01-01T10:00:00Z"}' | jq -e '.overlaps | length == 0' >/dev/null
if run_assessment <<<'{"id":1,"status":"in_progress","created_at":"2026-01-01T09:00:00Z","run_started_at":"2026-01-01T10:00:00Z","updated_at":"2026-01-01T10:00:00Z"}
{"id":2,"status":"completed","created_at":"2026-01-01T09:00:00Z","run_started_at":"2026-01-01T10:00:01Z","updated_at":"2026-01-01T10:00:00Z"}' >/dev/null 2>&1; then
 printf 'another run start later than update accepted\n' >&2; exit 1
fi
overlap='{"id":1,"status":"in_progress","created_at":"2026-01-01T09:00:00Z","run_started_at":"2026-01-01T09:00:00Z","updated_at":"2026-01-01T10:00:00Z"}
{"id":3,"status":"completed","created_at":"2026-01-01T09:00:00Z","run_started_at":"2026-01-01T09:30:00Z","updated_at":"2026-01-01T10:02:00Z"}'
run_assessment <<<"$overlap" | jq -e '.overlaps | length == 1 and .[0].id == 3' >/dev/null
if run_assessment </dev/null >/dev/null 2>&1; then
 printf 'empty overlap inventory accepted\n' >&2; exit 1
fi
if run_assessment <<<'{"id":3,"status":"completed","created_at":"2026-01-01T09:00:00Z","run_started_at":"2026-01-01T09:30:00Z","updated_at":"2026-01-01T10:02:00Z"}' >/dev/null 2>&1; then
 printf 'overlap inventory without own run accepted\n' >&2; exit 1
fi
if run_assessment <<<'{"id":4,"status":"SENTINEL_PRIVATE"}' >/dev/null 2>&1; then
 printf 'malformed overlap inventory accepted\n' >&2; exit 1
fi
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
if [[ ${MOCK_MODE:-} == cleanup-fail || ${MOCK_MODE:-} == client-fail-cleanup-fail || ${MOCK_MODE:-} == client-fail-cleanup-fail-live ]]; then
 case ${1:-} in rm) exit 44 ;; ps) printf 'owned-container\n'; exit 0 ;; esac
fi
exit 0
MOCK
cat > "$scratch/bin/timeout" <<'MOCK'
#!/usr/bin/env bash
if [[ ${MOCK_MODE:-} == docker-timeout ]]; then exit 124; fi
[[ $1 == -k && $2 == 2s && $3 == 10s ]] || exit 99
shift 3
exec "$@"
MOCK
cat > "$scratch/node" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$$" >> "$MOCK_HOME/nodes"
printf '%s\n' "${RHIZA_LATENCY_SAMPLE_SLOTS:-unset}" >> "$MOCK_HOME/node-sampling"
if [[ ${MOCK_MODE:-} == node-timeout ]]; then trap '' TERM; else
trap 'printf "node-shutdown-flushed\n"; if [[ ${MOCK_MODE:-} == node-shutdown-fail ]]; then exit 42; fi; exit 0' TERM
fi
while :; do sleep 0.05; done
MOCK
cat > "$scratch/bench" <<'MOCK'
#!/usr/bin/env bash
set -eu
printf '%s\n' "$$" > "$MOCK_HOME/bench.pid"
printf '%s\n' "$*" > "$MOCK_HOME/bench.args"
control=
pid=
while [[ $# -gt 0 ]]; do
 case $1 in -fault-control-file) control=$2; shift ;; -fault-pid) pid=$2; shift ;; esac
 shift
done
if [[ -n $control ]]; then
 printf '%s\n' "$pid" > "$MOCK_HOME/fault.pid"
 printf 'http://127.0.0.1:1234/0123456789abcdef0123456789abcdef' > "$control"
 while [[ ! -f $MOCK_HOME/release ]]; do sleep 0.01; done
fi
window=null
if [[ -n $control ]]; then
 if [[ $MOCK_MODE == after-last || $MOCK_MODE == no-witness ]]; then
  window='{"attempted":true,"signal_succeeded":true,"overlap":false,"witnesses":0,"reason":"no_completed_interval_spans_signal"}'
 else
  window='{"attempted":true,"signal_succeeded":true,"overlap":true,"witnesses":1,"reason":"qualified"}'
 fi
fi
printf '{"requests":4,"successes":4,"errors":0,"max_ms":1,"receipt_oracle":"sql-insert-id-v1","fault_window":%s}\n' "$window"
if [[ $MOCK_MODE == client-fail || $MOCK_MODE == client-fail-live || $MOCK_MODE == client-fail-cleanup-fail || $MOCK_MODE == client-fail-cleanup-fail-live ]]; then exit 23; fi
MOCK
cat > "$scratch/bin/curl" <<'MOCK'
#!/usr/bin/env bash
set -eu
for url in "$@"; do :; done
case $url in
*/metrics/object-store) printf '{"puts":0}\n' ;;
*/fault)
 case $MOCK_MODE in before-first) exit 22 ;; signal-failure|signal-failure-stop-fail) exit 47 ;; esac
 kill -KILL "$(cat "$MOCK_HOME/fault.pid")"
 touch "$MOCK_HOME/release"
 printf '{"attempted":true,"signal_succeeded":true}\n' ;;
*/sql/query)
 if [[ $MOCK_MODE == shifted ]]; then printf '{"rows":[[4,3]]}\n'; else printf '{"rows":[[4,4]]}\n'; fi ;;
esac
MOCK
cat > "$scratch/bin/cat" <<'MOCK'
#!/usr/bin/env bash
case ${1:-} in
/proc/*/stat) printf '1 (mock node) S 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 123\n' ;;
*) exec /bin/cat "$@" ;;
esac
MOCK
cat > "$scratch/bin/cp" <<'MOCK'
#!/usr/bin/env bash
if [[ ${MOCK_MODE:-} == copy-fail && ${*: -1} == *-client-result.json ]]; then exit 46; fi
exec /bin/cp "$@"
MOCK
cat > "$scratch/wait-env" <<'MOCK'
# Simulate the three alternate visible statuses only after reaping the exact
# mock target. The helper-only tests above exercise genuine wait statuses.
wait() {
 if [[ ${MOCK_MODE:-} == wait-* && ${1:-} == "$(cat "$MOCK_HOME/fault.pid")" ]]; then
  builtin wait "$1" || true
  case $MOCK_MODE in wait-normal) return 0 ;; wait-crash) return 139 ;; wait-error) return 127 ;; esac
 fi
 builtin wait "$@"
}
kill() {
 if [[ ${MOCK_MODE:-} == signal-failure-stop-fail && ${1:-} == -KILL && -f $MOCK_HOME/bench.pid && ${2:-} == "$(<"$MOCK_HOME/bench.pid")" ]]; then return 77; fi
 builtin kill "$@"
}
MOCK
chmod +x "$scratch/bin/"* "$scratch/node" "$scratch/bench"
for mode in normal normal-live ring sampled shifted before-first after-last no-witness signal-failure signal-failure-stop-fail wait-normal wait-crash wait-error client-fail client-fail-live cleanup-fail client-fail-cleanup-fail client-fail-cleanup-fail-live copy-fail node-shutdown-fail node-timeout docker-timeout; do
 mkdir "$scratch/$mode"
 # Each run's stand-ins and process evidence remain in its own scratch scope.
 cp "$scratch/node" "$scratch/bench" "$scratch/$mode/"
 fault=n0
 diagnostic=0
 live=0
 slot_samples=0
 [[ $mode != *-live ]] || live=1
 [[ $mode != ring ]] || diagnostic=1
 [[ $mode != sampled ]] || { diagnostic=1; live=1; slot_samples=1; }
 [[ $mode != shifted && $mode != client-fail && $mode != client-fail-live && $mode != cleanup-fail && $mode != client-fail-cleanup-fail && $mode != client-fail-cleanup-fail-live && $mode != copy-fail && $mode != node-shutdown-fail && $mode != node-timeout && $mode != docker-timeout ]] || fault=none
 status=0
 BASH_ENV="$scratch/wait-env" MOCK_HOME="$scratch/$mode" MOCK_MODE="$mode" PATH="$scratch/bin:$PATH" TMPDIR="$scratch/$mode" \
 RHIZA_SERVER_BENCH_RECORD_BUILD_ID="$slot_samples" RHIZA_SERVER_BENCH_LIVE_PROGRESS="$live" RHIZA_SERVER_BENCH_SLOT_SAMPLES="$slot_samples" RHIZA_SERVER_BENCH_LATENCY_DIAGNOSTICS="$diagnostic" RHIZA_SERVER_BENCH_REQUESTS=4 RHIZA_SERVER_BENCH_CONCURRENCY=2 RHIZA_SERVER_BENCH_FAULT_AFTER=0 RHIZA_SERVER_BENCH_FAILED_NODE="$fault" \
 bash "$here/run-server-reference.sh" "$scratch/base-server" "$scratch/$mode/result.json" >"$scratch/$mode/stdout" 2>"$scratch/$mode/stderr" || status=$?
 if [[ $mode == normal || $mode == normal-live || $mode == ring || $mode == sampled ]]; then
  [[ $status == 0 ]]
  jq -e '.fault_overlap.overlap == true and .fault_exit_status == 137 and .receipt_oracle == "sql-insert-id-v1" and (.client_sha256 | length == 64)' "$scratch/$mode/result.json" >/dev/null
  if [[ $mode == normal-live ]]; then
   jq -e '.live_progress_requested == true' "$scratch/$mode/result.json" >/dev/null
   grep -q -- '-live-progress-file' "$scratch/$mode/bench.args"
  fi
  grep -qx 'fault_exit_status=137' "$scratch/$mode/result-fault-metadata.txt"
  if [[ $mode == ring ]]; then grep -qx 'survivor_buffered_events=possibly_lost' "$scratch/$mode/result-cleanup-status.txt"; fi
  if [[ $mode == sampled ]]; then
   grep -q -- '-live-progress-slot-samples' "$scratch/$mode/bench.args"
   [[ $(grep -c '^1$' "$scratch/$mode/node-sampling") == 3 ]]
   grep -qx 'tagged=1' "$scratch/$mode/result-build.txt"
   grep -qx 'slot_samples=1' "$scratch/$mode/result-build.txt"
   grep -Eq '^server_sha256=[a-f0-9]{64}$' "$scratch/$mode/result-build.txt"
   grep -Eq '^client_sha256=[a-f0-9]{64}$' "$scratch/$mode/result-build.txt"
  else
   [[ $(grep -c '^0$' "$scratch/$mode/node-sampling") == 3 ]]
  fi
 else
  [[ $status != 0 ]]
  if [[ $mode == wait-* ]]; then
   case $mode in wait-normal) raw=0; want=1 ;; wait-crash) raw=139; want=139 ;; wait-error) raw=127; want=127 ;; esac
   [[ $status == "$want" ]]
   grep -qx "fault_exit_status=$raw" "$scratch/$mode/result-fault-metadata.txt"
   jq -e --argjson raw "$raw" '.qualification == "not_qualified" and .fault_exit_status == $raw' "$scratch/$mode/result.json" >/dev/null
  elif [[ $mode == signal-failure ]]; then [[ $status == 47 ]];
  elif [[ $mode == signal-failure-stop-fail ]]; then
   [[ $status == 47 ]]
   grep -qx 'benchmark_stop_exit=1 unreaped=1' "$scratch/$mode/result-cleanup-status.txt"
   grep -qx 'nodes=retained_client_alive' "$scratch/$mode/result-cleanup-status.txt"
   grep -qx 'node_logs=possibly_incomplete' "$scratch/$mode/result-cleanup-status.txt"
   grep -qx 'container=retained_node_alive' "$scratch/$mode/result-cleanup-status.txt"
   grep -q 'benchmark client termination not verified:' "$scratch/$mode/stderr"
   while IFS= read -r pid; do kill -KILL "$pid" 2>/dev/null || true; done < <(cat "$scratch/$mode/nodes" "$scratch/$mode/bench.pid")
   sleep 0.2
  elif [[ $mode == client-fail || $mode == client-fail-live ]]; then
   [[ $status == 23 ]]
   jq -e '.requests == 4' "$scratch/$mode/result-client-result.json" >/dev/null
   grep -qx 'client_result=preserved' "$scratch/$mode/result-cleanup-status.txt"
  elif [[ $mode == cleanup-fail ]]; then
   [[ $status == 1 ]]
   grep -qx 'container=unverified remove_exit=44 inspect_exit=0' "$scratch/$mode/result-cleanup-status.txt"
   grep -q 'benchmark container cleanup not verified:' "$scratch/$mode/stderr"
   grep -q 'benchmark evidence retained:' "$scratch/$mode/stderr"
  elif [[ $mode == client-fail-cleanup-fail || $mode == client-fail-cleanup-fail-live ]]; then
   [[ $status == 23 ]]
   grep -qx 'container=unverified remove_exit=44 inspect_exit=0' "$scratch/$mode/result-cleanup-status.txt"
   jq -e '.requests == 4' "$scratch/$mode/result-client-result.json" >/dev/null
  elif [[ $mode == copy-fail ]]; then
   [[ $status == 1 ]]
   grep -qx 'client_result=copy_failed' "$scratch/$mode/result-cleanup-status.txt"
   grep -q 'benchmark evidence retained:' "$scratch/$mode/stderr"
  elif [[ $mode == node-shutdown-fail ]]; then
   [[ $status == 1 ]]
   grep -q 'wait_exit=42' "$scratch/$mode/result-cleanup-status.txt"
   grep -q 'benchmark node shutdown failed:' "$scratch/$mode/stderr"
  elif [[ $mode == node-timeout ]]; then
   [[ $status == 1 ]]
   grep -q 'timed_out=1' "$scratch/$mode/result-cleanup-status.txt"
   grep -q 'benchmark evidence retained:' "$scratch/$mode/stderr"
  elif [[ $mode == docker-timeout ]]; then
   [[ $status == 1 ]]
   grep -qx 'container=unverified remove_exit=124 inspect_exit=124' "$scratch/$mode/result-cleanup-status.txt"
   grep -q 'benchmark evidence retained:' "$scratch/$mode/stderr"
  elif [[ $mode != shifted ]]; then grep -Eq 'fault (coordinator failed|interval not qualified)' "$scratch/$mode/stderr"; fi
 fi
 for log in "$scratch/$mode"/result-node-*.log; do
  [[ -f $log ]] || continue
  # A SIGKILL target cannot flush; every surviving mock node must be copied after wait.
  if ! grep -q 'node-shutdown-flushed' "$log"; then
   [[ $mode != signal-failure-stop-fail ]] || continue
   [[ $mode != node-timeout ]] || continue
   [[ $fault != none && $log == *result-node-0.log ]] || { printf 'shutdown log missing: %s\n' "$log" >&2; exit 1; }
  fi
 done
 if [[ $fault != none ]]; then grep -qx 'sigkill_buffered_events=possibly_lost' "$scratch/$mode/result-cleanup-status.txt"; fi
 grep -q -- '-verify-insert-receipt' "$scratch/$mode/bench.args"
 if [[ $live == 1 ]]; then grep -Fq -- "-live-progress-file $scratch/$mode/result-live-progress.jsonl" "$scratch/$mode/bench.args"; else ! grep -q -- '-live-progress-file' "$scratch/$mode/bench.args"; fi
 if [[ $fault != none ]]; then grep -q -- '-fault-start-time 123' "$scratch/$mode/bench.args"; fi
 if [[ $mode == ring || $mode == sampled ]]; then grep -Fq "$scratch/base-server|build -tags rhiza_latency_diag -o" "$scratch/$mode/builds"; else grep -Fq "$scratch/base-server|build -o" "$scratch/$mode/builds"; fi
 grep -Fq "$(cd "$here/.." && pwd)|build -o" "$scratch/$mode/builds"
 while IFS= read -r pid; do
  if kill -0 "$pid" 2>/dev/null; then printf 'mock owned process survived: %s\n' "$pid" >&2; exit 1; fi
 done < <(cat "$scratch/$mode/nodes" "$scratch/$mode/bench.pid")
done
printf 'server oracle helpers + complete mocked runner cases PASS\n'
