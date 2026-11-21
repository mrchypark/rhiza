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

# A one-second deadline remains open just before the deadline, regardless of
# the wall-clock second boundary, and closes exactly at the deadline.
deadline=100100
monotonic_ticks() { printf '%s\n' "$fake_ticks"; }
fake_ticks=100099
if deadline_expired; then printf 'deadline expired before elapsed second\n' >&2; exit 1; fi
fake_ticks=100100
deadline_expired

mkdir -p "$scratch/proc/42"
printf '42 (name with ) parentheses) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 123\n' > "$scratch/proc/42/stat"
printf 'Name: test\nVmHWM: 456 kB\n' > "$scratch/proc/42/status"
[[ $(server_resource_snapshot 42 "$scratch/proc") == '123 11 12 456' ]]
for bad in 'VmRSS: 456 kB' 'VmHWM: unknown kB' 'VmHWM: 456 MB'; do
	printf '%s\n' "$bad" > "$scratch/proc/42/status"
	if server_resource_snapshot 42 "$scratch/proc" >/dev/null; then printf 'invalid VmHWM accepted: %s\n' "$bad" >&2; exit 1; fi
done
printf 'bad stat\n' > "$scratch/proc/42/stat"
if server_resource_snapshot 42 "$scratch/proc" >/dev/null; then printf 'invalid stat accepted\n' >&2; exit 1; fi

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

# A failed termination signal cannot be reported as successful cleanup, even
# when the fallback kills and reaps the owned child.
sleep 60 & owned_pid=$!
kill() { if [[ ${1:-} != -KILL ]]; then return 1; fi; builtin kill "$@"; }
status=0
stop_owned_child "$owned_pid" 2>"$scratch/expected-signal-failure.err" || status=$?
unset -f kill
[[ $status == 1 ]]
grep -q 'termination not confirmed' "$scratch/expected-signal-failure.err"
if kill -0 "$owned_pid" 2>/dev/null; then printf 'cleanup fallback left a child running\n' >&2; exit 1; fi

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
mkdir -p "$scratch/bin" "$scratch/base-server"
cat > "$scratch/bin/go" <<'MOCK'
#!/usr/bin/env bash
set -eu
printf '%s|%s\n' "$PWD" "$*" >> "$MOCK_HOME/builds"
if [[ ${1:-} == tool ]]; then [[ $MOCK_MODE != diag-profile-invalid ]]; exit; fi
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
cat > "$scratch/bin/cp" <<'MOCK'
#!/usr/bin/env bash
if [[ ( ${MOCK_MODE:-} == diag-log-copy-fail || ${MOCK_MODE:-} == diag-log-copy-preserve ) && ${1:-} == */rhiza-server-bench.*/node-0.log ]]; then exit 1; fi
exec /bin/cp "$@"
MOCK
cat > "$scratch/bin/docker" <<'MOCK'
#!/usr/bin/env bash
case $1 in
 run) if [[ ${3:-} == -d ]]; then
  if [[ $MOCK_MODE == launch-fail ]]; then exit 47; fi
  touch "$MOCK_HOME/container"
  if [[ $MOCK_MODE == launch-bad-id ]]; then printf 'invalid\n'; else printf '%064d\n' 1; fi
 fi ;;
 rm) if [[ $MOCK_MODE == healthy-cleanup-fail || $MOCK_MODE == diag-cleanup-preserve || $MOCK_MODE == diag-cleanup-success ]]; then exit 1; fi; rm -f "$MOCK_HOME/container" ;;
 ps) if [[ -f $MOCK_HOME/container ]]; then printf '%064d\n' 1; fi ;;
esac
exit 0
MOCK
cat > "$scratch/node" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$$" >> "$MOCK_HOME/nodes"
printf '%s\n' "${RHIZA_CI_CPU_PROFILE_PORT:-disabled}" >> "$MOCK_HOME/profile-ports"
exec sleep 60
MOCK
cat > "$scratch/bench" <<'MOCK'
#!/usr/bin/env bash
set -eu
printf '%s\n' "$$" > "$MOCK_HOME/bench.pid"
printf '%s\n' "$*" > "$MOCK_HOME/bench.args"
control=
profile_control=
pid=
while [[ $# -gt 0 ]]; do
 case $1 in -fault-control-file) control=$2; shift ;; -fault-pid) pid=$2; shift ;; -profile-control-file) profile_control=$2; shift ;; esac
 shift
done
if [[ -n $profile_control && $MOCK_MODE != diag-fast ]]; then
 printf 'http://127.0.0.1:1234/0123456789abcdef0123456789abcdef' > "$profile_control"
 if [[ $MOCK_MODE == diag-status-client-exit || $MOCK_MODE == diag-status-client-success ]]; then
  while [[ ! -f $MOCK_HOME/status-trigger ]]; do sleep 0.01; done
  printf '{"profile_window":{"incomplete":true,'
  sleep 0.1
  printf '"reason":"client_stopped"},"marker":"complete"}\n'
  if [[ $MOCK_MODE == diag-status-client-exit ]]; then exit 23; else exit 0; fi
 fi
 while [[ ! -f $MOCK_HOME/profile-end ]]; do sleep 0.01; done
 sleep 1
fi
if [[ $MOCK_MODE == diag-client-fail || $MOCK_MODE == diag-cleanup-preserve || $MOCK_MODE == diag-log-copy-preserve ]]; then exit 19; fi
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
profile_window=null
completion_ns=0
if [[ -n $profile_control ]]; then
 if [[ $MOCK_MODE == diag-no-witness || $MOCK_MODE == diag-no-witness-missing || $MOCK_MODE == diag-client-result-fail ]]; then profile_window='{"overlap":false,"incomplete":false,"successes":0,"reason":"no_successful_request_inside_bracket","begin_ns":123,"end_ns":456}'; elif [[ $MOCK_MODE == diag-incomplete ]]; then profile_window='{"overlap":true,"incomplete":true,"successes":1}'; else profile_window='{"overlap":true,"incomplete":false,"successes":1}'; fi
 if command -v gdate >/dev/null 2>&1; then now_ms=$(gdate +%s%3N); else now_ms=$(date +%s%3N); fi
 completion_ns=$((now_ms * 1000000))
 case $MOCK_MODE in
  diag-late-serialization) touch "$MOCK_HOME/profile-release"; sleep 1.2 ;;
  diag-missing-time|diag-no-witness-missing) completion_ns= ;;
  diag-invalid-time) completion_ns=-1 ;;
  diag-future-time) completion_ns=$((completion_ns + 10000000000)) ;;
 esac
fi
completion_field=
[[ -z $completion_ns ]] || completion_field=",\"last_request_completion_boottime_ns\":$completion_ns"
printf '{"requests":4,"successes":4,"errors":0,"max_ms":1,"receipt_oracle":"sql-insert-id-v1","fault_window":%s,"profile_window":%s%s}\n' "$window" "$profile_window" "$completion_field"
if [[ $MOCK_MODE == diag-client-result-fail ]]; then exit 19; fi
MOCK
cat > "$scratch/bin/curl" <<'MOCK'
#!/usr/bin/env bash
set -eu
for url in "$@"; do :; done
case $url in
*/debug/pprof/profile\?*)
 port=${url#http://127.0.0.1:}; port=${port%%/*}
 touch "$MOCK_HOME/profile-$port-active"
 printf '%s\n' "$$" > "$MOCK_HOME/profile-$port-pid"
 output=
 while [[ $# -gt 0 ]]; do if [[ $1 == -o ]]; then output=$2; shift; fi; shift; done
 sleeper=
 trap 'if [[ -n $sleeper ]]; then kill "$sleeper" 2>/dev/null || true; wait "$sleeper" 2>/dev/null || true; fi; rm -f "$MOCK_HOME/profile-$port-active"; exit 143' TERM
 for _ in {1..200}; do
  [[ ! -f $MOCK_HOME/profile-release ]] || break
  sleep 0.05 & sleeper=$!
  wait "$sleeper"
  sleeper=
 done
 rm -f "$MOCK_HOME/profile-$port-active"
 [[ -f $MOCK_HOME/profile-release ]] || { printf 'mock profile release timed out\n' >&2; exit 124; }
 touch "$MOCK_HOME/profile-finished"
 if [[ $MOCK_MODE == diag-profile-fail ]]; then exit 22; fi
 printf 'fake profile\n' > "$output" ;;
*/debug/pprof/ready)
 port=${url#http://127.0.0.1:}; port=${port%%/*}
 phase=before
 [[ ! -f $MOCK_HOME/ready-before-complete ]] || phase=during
 printf '%s %s\n' "$phase" "$port" >> "$MOCK_HOME/ready-calls"
 printf 'ready %s\n' "$port" >> "$MOCK_HOME/profile-events"
 [[ -f $MOCK_HOME/profile-$port-active ]] || exit 22
 if [[ $MOCK_MODE == diag-ready-start || ( $MOCK_MODE == diag-ready-during && $phase == during ) || ( $MOCK_MODE == diag-ready-between && -f $MOCK_HOME/profile-begin ) ]]; then exit 22; fi
 if [[ $phase == before && $port == 20102 ]]; then touch "$MOCK_HOME/ready-before-complete"; fi ;;
*/metrics/object-store)
 if [[ $MOCK_MODE == diag-slow-snapshot && -f $MOCK_HOME/bench.pid && ! -f $MOCK_HOME/snapshot-delayed ]]; then touch "$MOCK_HOME/snapshot-delayed" "$MOCK_HOME/profile-release"; sleep 1.2; fi
 if [[ $MOCK_MODE == diag-metrics-fail && -f $MOCK_HOME/bench.pid ]]; then touch "$MOCK_HOME/profile-release"; exit 22; fi
 if [[ $MOCK_MODE == diag-schema-change && -f $MOCK_HOME/bench.pid ]]; then touch "$MOCK_HOME/profile-release"; fi
 if [[ $MOCK_MODE == diag-empty ]]; then printf '{}\n'; exit; fi
 if [[ $MOCK_MODE == diag-missing-metric ]]; then printf '{"uploads":0}\n'; exit; fi
 if [[ $MOCK_MODE == diag-schema-change && ! -f $MOCK_HOME/bench.pid ]]; then printf '{"uploads":0,"gets":0,"lists":0,"heads":0,"deletes":0,"failures":0,"bytes_uploaded":0,"bytes_downloaded":0,"s3_http_requests":0,"s3_http_failures":0,"http_requests":0,"http_failures":0,"extra":0}\n'; exit; fi
 uploads=0
 [[ ! -f $MOCK_HOME/query-done ]] || uploads=7
 printf '{"uploads":%s,"gets":0,"lists":0,"heads":0,"deletes":0,"failures":0,"bytes_uploaded":0,"bytes_downloaded":0,"s3_http_requests":0,"s3_http_failures":0,"http_requests":0,"http_failures":0}\n' "$uploads" ;;
*/recovery/status)
 touch "$MOCK_HOME/status-called"
 if [[ ! -f $MOCK_HOME/profile-finished ]]; then touch "$MOCK_HOME/status-before-profile-finished"; fi
 touch "$MOCK_HOME/profile-release"
 case $MOCK_MODE in
  diag-missing) printf '{}\n' ;;
  diag-failure) exit 22 ;;
  diag-timeout) printf '{"node_id":"n1","quorum":true,"certified_tip":4,"archive_tip":2}\n' ;;
  diag-late-status) sleep 1.2; printf '{"node_id":"n1","quorum":true,"certified_tip":4,"archive_tip":4}\n' ;;
  diag-success|diag-default|diag-status-poll|diag-cleanup-success|diag-log-copy-fail)
   if [[ -f $MOCK_HOME/polled ]]; then archive=4; else archive=2; touch "$MOCK_HOME/polled"; fi
   printf '{"node_id":"n1","quorum":true,"certified_tip":4,"archive_tip":%s}\n' "$archive" ;;
 esac ;;
*/fault)
 case $MOCK_MODE in before-first) exit 22 ;; signal-failure) exit 47 ;; esac
 kill -KILL "$(cat "$MOCK_HOME/fault.pid")"
 touch "$MOCK_HOME/release"
 printf '{"attempted":true,"signal_succeeded":true}\n' ;;
*/profile/begin)
 printf 'begin\n' >> "$MOCK_HOME/profile-events"
 if [[ $MOCK_MODE == diag-control-fail ]]; then printf '%s\n' "$url" >&2; exit 22; fi
 touch "$MOCK_HOME/profile-begin" ;;
*/profile/status)
 printf 'status\n' >> "$MOCK_HOME/profile-events"
 [[ -f $MOCK_HOME/profile-begin ]] || exit 22
 case $MOCK_MODE in
  diag-status-fail) printf '%s\n' "$url" >&2; exit 22 ;;
  diag-status-invalid) printf '{"successes":"1"}\n' ;;
  diag-status-zero) printf '{"successes":0}\n' ;;
  diag-status-client-exit|diag-status-client-success) touch "$MOCK_HOME/status-trigger"; sleep 0.4; printf '{"successes":0}\n' ;;
  diag-status-poll) if [[ ! -f $MOCK_HOME/status-seen ]]; then touch "$MOCK_HOME/status-seen"; printf '{"successes":0}\n'; else printf '{"successes":1}\n'; fi ;;
  *) printf '{"successes":1}\n' ;;
 esac ;;
*/profile/end)
 printf 'end\n' >> "$MOCK_HOME/profile-events"
 [[ -f $MOCK_HOME/profile-begin ]] || exit 22
 if [[ $MOCK_MODE == diag-control-end-fail ]]; then printf '%s\n' "$url" >&2; exit 22; fi
 if [[ $MOCK_MODE == diag-profile-end-exit ]]; then kill -TERM "$(<"$MOCK_HOME/profile-20100-pid")"; sleep 0.1; fi
 touch "$MOCK_HOME/profile-end" ;;
*/sql/query)
 touch "$MOCK_HOME/query-done"
 if [[ $MOCK_MODE == shifted || $MOCK_MODE == diag-shifted ]]; then printf '{"rows":[[4,3]]}\n'; else printf '{"rows":[[4,4]]}\n'; fi ;;
esac
MOCK
cat > "$scratch/bin/cat" <<'MOCK'
#!/usr/bin/env bash
case ${1:-} in
/proc/*/stat)
 start=123 user=10
 if [[ -f $MOCK_HOME/bench.pid ]]; then
  user=15
  if [[ $MOCK_MODE == changed ]]; then start=124; fi
 fi
 printf '1 (mock node) S 1 0 0 0 0 0 0 0 0 0 %s 20 0 0 0 0 0 0 %s\n' "$user" "$start" ;;
*) exec /bin/cat "$@" ;;
esac
MOCK
cat > "$scratch/bin/awk" <<'MOCK'
#!/usr/bin/env bash
case ${2:-} in
/proc/uptime)
 if command -v gdate >/dev/null 2>&1; then ticks=$(gdate +%s%3N); else ticks=$(date +%s%3N); fi
 printf '%s\n' "$((ticks / 10))" ;;
/proc/*/status)
 if [[ $MOCK_MODE == missing || ( $MOCK_MODE == missing-after && -f $MOCK_HOME/bench.pid ) ]]; then exit 1; fi
 printf '456\n' ;;
*) exec /usr/bin/awk "$@" ;;
esac
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
MOCK
chmod +x "$scratch/bin/"* "$scratch/node" "$scratch/bench"
for mode in healthy healthy-cleanup-fail launch-fail launch-bad-id missing missing-after changed normal shifted before-first after-last no-witness signal-failure wait-normal wait-crash wait-error diag-success diag-default diag-status-poll diag-cleanup-success diag-log-copy-fail diag-log-copy-preserve diag-timeout diag-slow-snapshot diag-late-status diag-late-serialization diag-missing-time diag-invalid-time diag-future-time diag-missing diag-failure diag-metrics-fail diag-client-fail diag-client-result-fail diag-cleanup-preserve diag-profile-fail diag-profile-invalid diag-empty diag-missing-metric diag-schema-change diag-shifted diag-ready-start diag-ready-during diag-ready-between diag-profile-end-exit diag-fast diag-control-fail diag-control-end-fail diag-status-fail diag-status-invalid diag-status-zero diag-status-client-exit diag-status-client-success diag-incomplete diag-no-witness diag-no-witness-missing; do
 mkdir "$scratch/$mode"
 # Each run's stand-ins and process evidence remain in its own scratch scope.
 cp "$scratch/node" "$scratch/bench" "$scratch/$mode/"
 fault=n0
 case $mode in healthy|healthy-cleanup-fail|launch-fail|launch-bad-id|missing|missing-after|changed|shifted|diag-*) fault=none ;; esac
 diagnostic=0
 [[ $mode != diag-* ]] || diagnostic=1
 drain_timeout=1
 [[ $mode != diag-success && $mode != diag-default && $mode != diag-status-poll && $mode != diag-cleanup-success && $mode != diag-log-copy-fail ]] || drain_timeout=3
 sync_interval=50ms
 [[ $mode != diag-default ]] || sync_interval=
 status=0
 BASH_ENV="$scratch/wait-env" MOCK_HOME="$scratch/$mode" MOCK_MODE="$mode" PATH="$scratch/bin:$PATH" TMPDIR="$scratch/$mode" \
 RHIZA_SERVER_BENCH_REQUESTS=4 RHIZA_SERVER_BENCH_CONCURRENCY=2 RHIZA_SERVER_BENCH_FAULT_AFTER=0 RHIZA_SERVER_BENCH_FAILED_NODE="$fault" \
 RHIZA_SERVER_BENCH_PUBLICATION_DIAGNOSTIC="$diagnostic" RHIZA_SERVER_BENCH_SYNC_INTERVAL="$sync_interval" RHIZA_SERVER_BENCH_DRAIN_TIMEOUT_SECONDS="$drain_timeout" \
 bash "$here/run-server-reference.sh" "$scratch/base-server" "$scratch/$mode/result.json" >"$scratch/$mode/stdout" 2>"$scratch/$mode/stderr" || status=$?
 if [[ $mode == diag-* && ! -f $scratch/$mode/result.json ]]; then
  printf 'diagnostic %s produced no result (runner exit %s); runner stderr follows:\n' "$mode" "$status" >&2
  cat "$scratch/$mode/stderr" >&2
  exit 1
 fi
 if [[ $mode == diag-success || $mode == diag-default || $mode == diag-status-poll ]]; then
  [[ $status == 0 ]]
  [[ $mode != diag-default ]] || sync_interval=1s
  jq -e --arg interval "$sync_interval" '.publication_diagnostic | .outcome == "complete" and .fixed_certified_tip == 4 and .last_status.archive_tip == 4 and .sync_interval == $interval and (.workload_end | keys == ["n0","n1","n2"]) and (.drain_end | keys == ["n0","n1","n2"]) and (.observation | contains("ReadIndex"))' "$scratch/$mode/result.json" >/dev/null
  jq -e '.object_measurement | contains("publication drain and final linearizable verification")' "$scratch/$mode/result.json" >/dev/null
  jq -e '.object_store_delta.uploads == 21 and .result.profile_window.overlap == true and (.publication_diagnostic.cpu_profile_readiness | contains("before begin and before end"))' "$scratch/$mode/result.json" >/dev/null
  [[ $(tail -9 "$scratch/$mode/profile-events") == $'ready 20100\nready 20101\nready 20102\nbegin\nstatus\nready 20100\nready 20101\nready 20102\nend' || $mode == diag-status-poll ]]
  if [[ $mode == diag-status-poll ]]; then [[ $(tail -10 "$scratch/$mode/profile-events") == $'ready 20100\nready 20101\nready 20102\nbegin\nstatus\nstatus\nready 20100\nready 20101\nready 20102\nend' ]]; fi
  [[ -f $scratch/$mode/profile-begin && -f $scratch/$mode/profile-end ]]
  for port in 20100 20101 20102; do grep -qx "before $port" "$scratch/$mode/ready-calls"; grep -qx "during $port" "$scratch/$mode/ready-calls"; done
  [[ -f $scratch/$mode/status-before-profile-finished ]]
  grep -q -- '-tags ci_profile' "$scratch/$mode/builds"
  [[ $(sort "$scratch/$mode/profile-ports" | tr '\n' ' ') == '20100 20101 20102 ' ]]
  profile_dir=$(jq -r '.publication_diagnostic.profile_directory' "$scratch/$mode/result.json")
  [[ -s $profile_dir/server-binary ]]
  for i in 0 1 2; do [[ -s $profile_dir/node-$i.cpu.pprof ]]; done
 elif [[ $mode == diag-cleanup-success || $mode == diag-log-copy-fail ]]; then
  [[ $status == 1 ]]
  jq -e '.publication_diagnostic.outcome == "incomplete" and .publication_diagnostic.reason == "cleanup_unconfirmed" and .result.successes == 4' "$scratch/$mode/result.json" >/dev/null
  grep -q 'cleanup: retained run directory' "$scratch/$mode/stderr"
  if [[ $mode == diag-cleanup-success ]]; then
   grep -q 'container .* removal not confirmed' "$scratch/$mode/stderr"
  else
   grep -q 'cleanup: could not preserve log' "$scratch/$mode/stderr"
   retained=$(sed -n 's/^cleanup: retained run directory //p' "$scratch/$mode/stderr")
   [[ -f $retained/node-0.log ]]
  fi
 elif [[ $mode == diag-status-client-exit || $mode == diag-status-client-success ]]; then
  if [[ $mode == diag-status-client-exit ]]; then [[ $status == 23 ]]; expected_client_status=23; expected_reason=workload_failed; else [[ $status == 1 ]]; expected_client_status=0; expected_reason=cpu_profile_overlap_unconfirmed; fi
  jq -e --argjson status "$expected_client_status" --arg reason "$expected_reason" '.result.marker == "complete" and .result.profile_window.reason == "client_stopped" and .publication_diagnostic.client_exit_status == $status and .publication_diagnostic.reason == $reason' "$scratch/$mode/result.json" >/dev/null
  [[ $(grep -c '^end$' "$scratch/$mode/profile-events" || true) == 0 ]]
 elif [[ $mode == diag-* ]]; then
  [[ $status != 0 ]]
  case $mode in diag-timeout|diag-slow-snapshot|diag-late-status|diag-late-serialization) outcome=timeout; reason=archive_tip_timeout ;; diag-missing-time|diag-no-witness-missing|diag-invalid-time|diag-future-time) outcome=incomplete; reason=workload_completion_time_unavailable ;; diag-metrics-fail|diag-schema-change) outcome=incomplete; reason=workload_metrics_unavailable ;; diag-client-fail|diag-client-result-fail|diag-cleanup-preserve|diag-log-copy-preserve) outcome=incomplete; reason=workload_failed ;; diag-profile-fail|diag-profile-invalid) outcome=incomplete; reason=cpu_profile_unavailable ;; diag-empty|diag-missing-metric) outcome=incomplete; reason=baseline_metrics_unavailable ;; diag-shifted) outcome=incomplete; reason=workload_verification_failed ;; diag-ready-start) outcome=incomplete; reason=cpu_profile_start_unconfirmed ;; diag-ready-during|diag-ready-between|diag-profile-end-exit|diag-fast|diag-control-fail|diag-control-end-fail|diag-status-fail|diag-status-invalid|diag-status-zero|diag-incomplete|diag-no-witness) outcome=incomplete; reason=cpu_profile_overlap_unconfirmed ;; *) outcome=incomplete; reason=status_unavailable ;; esac
  jq -e --arg outcome "$outcome" --arg reason "$reason" '.publication_diagnostic.outcome == $outcome and .publication_diagnostic.reason == $reason' "$scratch/$mode/result.json" >/dev/null
  if [[ $mode == diag-slow-snapshot ]]; then [[ ! -f $scratch/$mode/status-before-profile-finished ]]; fi
  if [[ $mode == diag-late-status ]]; then jq -e '.publication_diagnostic.last_status.archive_tip == 4' "$scratch/$mode/result.json" >/dev/null; fi
  if [[ $mode == diag-late-serialization ]]; then [[ ! -e $scratch/$mode/status-called ]]; fi
  if [[ $mode == diag-no-witness || $mode == diag-no-witness-missing || $mode == diag-client-result-fail ]]; then jq -e '.result.successes == 4 and .result.profile_window == {"overlap":false,"incomplete":false,"successes":0,"reason":"no_successful_request_inside_bracket","begin_ns":123,"end_ns":456}' "$scratch/$mode/result.json" >/dev/null; fi
  if [[ $mode == diag-control-fail || $mode == diag-control-end-fail || $mode == diag-status-fail ]]; then ! grep -q '0123456789abcdef0123456789abcdef' "$scratch/$mode/stderr"; fi
  if [[ $mode == diag-status-invalid || $mode == diag-status-fail || $mode == diag-status-zero ]]; then [[ $(grep -c '^end$' "$scratch/$mode/profile-events" || true) == 0 ]]; fi
  if [[ $mode == diag-control-fail || $mode == diag-control-end-fail ]]; then [[ $(grep -c '^begin$' "$scratch/$mode/profile-events") == 1 ]]; [[ $(grep -c '^end$' "$scratch/$mode/profile-events" || true) -le 1 ]]; fi
  if [[ $mode == diag-ready-between ]]; then
   [[ $(grep -c '^begin$' "$scratch/$mode/profile-events") == 1 ]]
   [[ $(grep -c '^end$' "$scratch/$mode/profile-events" || true) == 0 ]]
   [[ $(tail -1 "$scratch/$mode/profile-events") == 'ready 20100' ]]
  fi
  if [[ $mode == diag-incomplete ]]; then jq -e '.result.profile_window.incomplete == true' "$scratch/$mode/result.json" >/dev/null; fi
  if [[ $mode == diag-client-fail || $mode == diag-client-result-fail || $mode == diag-cleanup-preserve || $mode == diag-log-copy-preserve ]]; then [[ $status == 19 ]]; elif [[ $mode != diag-empty && $mode != diag-missing-metric && $mode != diag-ready-start && $mode != diag-ready-during && $mode != diag-ready-between && $mode != diag-profile-end-exit && $mode != diag-fast && $mode != diag-control-fail && $mode != diag-control-end-fail && $mode != diag-status-fail && $mode != diag-status-invalid && $mode != diag-status-zero && $mode != diag-incomplete && $mode != diag-no-witness && $mode != diag-no-witness-missing && $mode != diag-missing-time && $mode != diag-invalid-time && $mode != diag-future-time ]]; then jq -e '.successes == 4' "$scratch/$mode/result.json" >/dev/null; fi
  if [[ $mode == diag-log-copy-preserve ]]; then
   grep -q 'cleanup: could not preserve log' "$scratch/$mode/stderr"
   retained=$(sed -n 's/^cleanup: retained run directory //p' "$scratch/$mode/stderr")
   [[ -f $retained/node-0.log ]]
  fi
 elif [[ $mode == healthy ]]; then
  [[ $status == 0 ]]
  jq -e --argjson hz "$(getconf CLK_TCK)" '.server_resources.nodes | keys == ["n0","n1","n2"] and all(.[]; .cpu_seconds == (5 / $hz) and .vm_hwm_kib == 456)' "$scratch/$mode/result.json" >/dev/null
  jq -e '.server_resources.scope | contains("sequential snapshots bracketing the client workload")' "$scratch/$mode/result.json" >/dev/null
 elif [[ $mode == normal ]]; then
  [[ $status == 0 ]]
  jq -e '.fault_overlap.overlap == true and .fault_exit_status == 137 and .receipt_oracle == "sql-insert-id-v1" and (.client_sha256 | length == 64)' "$scratch/$mode/result.json" >/dev/null
  grep -qx 'fault_exit_status=137' "$scratch/$mode/result-fault-metadata.txt"
 else
  [[ $status != 0 ]]
  if [[ $mode == launch-fail || $mode == launch-bad-id ]]; then
   if [[ $mode == launch-fail ]]; then [[ $status == 47 ]]; fi
   grep -q 'cleanup cannot be verified' "$scratch/$mode/stderr"
   grep -q 'cleanup: retained run directory' "$scratch/$mode/stderr"
  elif [[ $mode == healthy-cleanup-fail ]]; then
   [[ $status == 1 ]]
   jq -e '.result.successes == 4' "$scratch/$mode/result.json" >/dev/null
  elif [[ $mode == wait-* ]]; then
   case $mode in wait-normal) raw=0; want=1 ;; wait-crash) raw=139; want=139 ;; wait-error) raw=127; want=127 ;; esac
   [[ $status == "$want" ]]
   grep -qx "fault_exit_status=$raw" "$scratch/$mode/result-fault-metadata.txt"
   jq -e --argjson raw "$raw" '.qualification == "not_qualified" and .fault_exit_status == $raw' "$scratch/$mode/result.json" >/dev/null
  elif [[ $mode == signal-failure ]]; then [[ $status == 47 ]];
  elif [[ $mode == missing || $mode == missing-after || $mode == changed ]]; then
   grep -Eq 'resource snapshot unavailable|resource identity or CPU counters' "$scratch/$mode/stderr"
   if [[ $mode != missing ]]; then jq -e '.successes == 4 and .fault_exit_status == null' "$scratch/$mode/result.json" >/dev/null; fi
  elif [[ $mode != shifted ]]; then grep -Eq 'fault (coordinator failed|interval not qualified)' "$scratch/$mode/stderr"; fi
 fi
 if [[ $mode == healthy-cleanup-fail || $mode == diag-cleanup-preserve ]]; then
  grep -q 'container .* removal not confirmed' "$scratch/$mode/stderr"
  grep -q 'cleanup: retained run directory' "$scratch/$mode/stderr"
 fi
 if [[ -f $scratch/$mode/bench.args ]]; then
  grep -q -- '-verify-insert-receipt' "$scratch/$mode/bench.args"
 else
  case $mode in launch-fail|launch-bad-id|missing|diag-empty|diag-missing-metric|diag-ready-start|diag-ready-during|diag-fast) ;; *) printf 'benchmark never started: %s\n' "$mode" >&2; exit 1 ;; esac
 fi
 if [[ $fault != none ]]; then grep -q -- '-fault-start-time 123' "$scratch/$mode/bench.args"; fi
 grep -Eq "^$scratch/base-server\\|build (.* )?-o " "$scratch/$mode/builds"
 grep -Fq "$(cd "$here/.." && pwd)|build -o" "$scratch/$mode/builds"
 while IFS= read -r pid; do
  if kill -0 "$pid" 2>/dev/null; then printf 'mock owned process survived: %s\n' "$pid" >&2; exit 1; fi
 done < <([[ ! -f $scratch/$mode/nodes ]] || cat "$scratch/$mode/nodes"; [[ ! -f $scratch/$mode/bench.pid ]] || cat "$scratch/$mode/bench.pid")
done
first_profile_dir=$(jq -r '.publication_diagnostic.profile_directory' "$scratch/diag-success/result.json")
printf 'previous run\n' > "$first_profile_dir/ownership-marker"
mkdir "$scratch/reuse"
cp "$scratch/node" "$scratch/bench" "$scratch/reuse/"
BASH_ENV="$scratch/wait-env" MOCK_HOME="$scratch/reuse" MOCK_MODE=diag-success PATH="$scratch/bin:$PATH" TMPDIR="$scratch/reuse" \
 RHIZA_SERVER_BENCH_REQUESTS=4 RHIZA_SERVER_BENCH_CONCURRENCY=2 RHIZA_SERVER_BENCH_PUBLICATION_DIAGNOSTIC=1 RHIZA_SERVER_BENCH_DRAIN_TIMEOUT_SECONDS=3 \
 bash "$here/run-server-reference.sh" "$scratch/base-server" "$scratch/diag-success/result.json" >"$scratch/reuse/stdout" 2>"$scratch/reuse/stderr"
second_profile_dir=$(jq -r '.publication_diagnostic.profile_directory' "$scratch/diag-success/result.json")
[[ $first_profile_dir != "$second_profile_dir" && -s $second_profile_dir/server-binary && -s $second_profile_dir/node-0.cpu.pprof ]]
grep -qx 'previous run' "$first_profile_dir/ownership-marker"
for i in 0 1 2; do [[ -s $first_profile_dir/node-$i.cpu.pprof && -s $second_profile_dir/node-$i.cpu.pprof ]]; done
while IFS= read -r pid; do
 if kill -0 "$pid" 2>/dev/null; then printf 'reused-output owned process survived: %s\n' "$pid" >&2; exit 1; fi
done < <(cat "$scratch/reuse/nodes" "$scratch/reuse/bench.pid")
printf 'server oracle helpers and mocked runner cases PASS\n'
