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

mkdir -p "$scratch/proc/pressure"
printf 'some avg10=0.00 avg60=0.00 avg300=0.00 total=120\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=2\n' > "$scratch/proc/pressure/cpu"
printf 'some avg10=0.00 avg60=0.00 avg300=0.00 total=90\n' > "$scratch/proc/pressure/io"
printf 'cpu  9 8 7 6 5 4 3 2 1 0\ncpu0 0 0 0 0 0 0 0 99\n' > "$scratch/proc/stat"
host_before=$(sample_benchmark_host "$scratch/proc")
[[ $host_before == '[120,90,2]' ]]
benchmark_host_pressure "$host_before" '[127,90,5]' |
	jq -e '. == {cpu_some_us:7,io_some_us:0,cpu_steal_ticks:3}' >/dev/null
benchmark_host_pressure "$host_before" "$(sample_benchmark_host "$scratch/absent")" |
	jq -e '. == {cpu_some_us:null,io_some_us:null,cpu_steal_ticks:null}' >/dev/null
benchmark_host_pressure "$host_before" '[119,null,2]' |
	jq -e '. == {cpu_some_us:null,io_some_us:null,cpu_steal_ticks:0}' >/dev/null

# Unique INTEGER PRIMARY KEY rows: [0,4) is valid; shifted [1,5) has only 3
# in-range rows despite total cardinality 4. Extra negative/out-of-range rows fail.
verify_benchmark_rows 4 <<< '{"rows":[[4,4]]}'
for rows in '[[4,3]]' '[[5,4]]' '[[3,3]]' '[[4]]' '[["4",4]]'; do
	if verify_benchmark_rows 4 <<< "{\"rows\":$rows}"; then printf 'invalid exact set accepted: %s\n' "$rows" >&2; exit 1; fi
done
verify_graph_rows 4 <<< '{"rows":[[3],[0],[1],[2]]}'
for rows in '[[0],[1],[1],[3]]' '[[0],[1],[2],[4]]' '[["0"],[1],[2],[3]]' '[[0],[1],[2],[3],[4]]'; do
	if verify_graph_rows 4 <<< "{\"rows\":$rows}"; then printf 'invalid Graph identity set accepted: %s\n' "$rows" >&2; exit 1; fi
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
set -eu
[[ $MOCK_MODE == gcs-* ]] || exit 0
id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
case $1 in
 run)
  [[ $* == *'--no-healthcheck'* && $* == *'127.0.0.1::4588'* && $* == *'RHIZA_GCS_ACCESS %m %R %s'* && $* == *'floci/floci-gcp:0.11.0@sha256:'* ]]
  printf '%s\n' "$id" ;;
 port)
  [[ $2 == "$id" && $3 == 4588/tcp ]]
  printf '127.0.0.1:24588\n' ;;
 logs)
  [[ $2 == "$id" ]]
  bucket=$(<"$MOCK_HOME/gcs-bucket")
  printf 'not an access record\n'
  printf 'quarkus-prefix RHIZA_GCS_ACCESS POST /storage/v1/b 200\n'
  if [[ -f $MOCK_HOME/client-done && $MOCK_MODE != gcs-missing ]]; then
   suffix=
   [[ $MOCK_MODE != gcs-async ]] || suffix='?projection=synthetic-private-query#synthetic-private-fragment'
   printf 'quarkus-prefix RHIZA_GCS_ACCESS GET /storage/v1/b/%s/o/bench-head%s 404\n' "$bucket" "$suffix"
   if [[ $MOCK_MODE == gcs-unknown ]]; then printf 'RHIZA_GCS_ACCESS GET /other 200\n'; fi
   if [[ $MOCK_MODE == gcs-malformed ]]; then printf 'quarkus-prefix RHIZA_GCS_ACCESS GET /storage/v1/b?synthetic-private-query missing_status\n'; fi
  fi
  if [[ -f $MOCK_HOME/archived ]]; then
   printf 'quarkus-prefix RHIZA_GCS_ACCESS POST /upload/storage/v1/b/%s/o?upload_id=synthetic-private-query 200\n' "$bucket"
  fi ;;
 rm)
  [[ $2 == -f && $3 == "$id" ]]
  touch "$MOCK_HOME/gcs-removed" ;;
 *) exit 93 ;;
esac
MOCK
cat > "$scratch/node" <<'MOCK'
#!/usr/bin/env bash
set -eu
if [[ $RHIZA_OBJSTORE_PROVIDER == gcs ]]; then
 [[ $STORAGE_EMULATOR_HOST == http://127.0.0.1:24588 && $GOOGLE_APPLICATION_CREDENTIALS == "$TMPDIR/gcs-adc.json" &&
    ! ${RHIZA_OBJSTORE_SERVICE_ACCOUNT+x} && ! ${HTTP_PROXY+x} ]]
 jq -e '.type == "service_account" and .token_uri == "http://127.0.0.1:24588/token" and (.private_key | length > 0)' \
  "$GOOGLE_APPLICATION_CREDENTIALS" >/dev/null
 MOCK_HOME=${TMPDIR%/*}
 printf '%s\n' "$RHIZA_NODE_ID" >> "$MOCK_HOME/gcs-local-adc"
fi
printf '%s\n' "$$" >> "$MOCK_HOME/nodes"
exec sleep 60
MOCK
cat > "$scratch/bench" <<'MOCK'
#!/usr/bin/env bash
set -eu
printf '%s\n' "$$" > "$MOCK_HOME/bench.pid"
printf '%s\n' "$*" > "$MOCK_HOME/bench.args"
control=
pid=
oracle=sql-insert-id-v1
successes=4
errors=0
while [[ $# -gt 0 ]]; do
 case $1 in
  -fault-control-file) control=$2; shift ;;
  -fault-pid) pid=$2; shift ;;
  -verify-applied-receipt) oracle=applied-mutation-v1 ;;
  -verify-kv-value)
   oracle=kv-benchmark-value-v1
   cp "$MOCK_HOME/bench.args" "$MOCK_HOME/kv-verification.args"
   if [[ $MOCK_MODE == kv-wrong-value ]]; then successes=3; errors=1; fi ;;
 esac
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
printf '{"requests":4,"successes":%d,"errors":%d,"max_ms":1,"receipt_oracle":"%s","fault_window":%s}\n' "$successes" "$errors" "$oracle" "$window"
touch "$MOCK_HOME/client-done"
MOCK
cat > "$scratch/bin/curl" <<'MOCK'
#!/usr/bin/env bash
set -eu
for url in "$@"; do :; done
case $url in
http://127.0.0.1:24588/storage/v1/b\?project=rhiza-local-test)
 [[ $* == *"--noproxy *"* ]]
 while [[ $# -gt 1 ]]; do
  if [[ $1 == -d ]]; then
   jq -er '.name | select(test("^rhiza-server-bench-[0-9]+-[0-9]+$"))' <<<"$2" >"$MOCK_HOME/gcs-bucket"
   printf '%s\n' "$2"
   break
  fi
  shift
 done ;;
*/metrics/object-store)
 if [[ -f $MOCK_HOME/archived ]]; then puts=11
 elif [[ -f $MOCK_HOME/client-done ]]; then puts=2
 else puts=0; fi
 printf '{"puts":%d,"http_requests":%d}\n' "$puts" "$puts" ;;
*/fault)
 case $MOCK_MODE in before-first) exit 22 ;; signal-failure) exit 47 ;; esac
 kill -KILL "$(cat "$MOCK_HOME/fault.pid")"
 touch "$MOCK_HOME/release"
 printf '{"attempted":true,"signal_succeeded":true}\n' ;;
*/sql/query)
 if [[ $MOCK_MODE == shifted ]]; then printf '{"rows":[[4,3]]}\n'; else printf '{"rows":[[4,4]]}\n'; fi ;;
*/graph/query)
 if [[ $MOCK_MODE == graph-wrong-ids ]]; then printf '{"rows":[[0],[1],[1],[3]]}\n'; else printf '{"rows":[[3],[0],[2],[1]],"applied_slot":8}\n'; fi ;;
*/recovery/status)
 if [[ -f $MOCK_HOME/archived && $MOCK_MODE != async-stale-cut ]]; then archive_tip=8; else archive_tip=1; fi
 printf '{"node_id":"n1","cluster_id":"server-bench","durability":"async","ready":true,"quorum":true,"certified_tip":8,"applied_tip":8,"archive_tip":%d}\n' "$archive_tip" ;;
*/recovery/archive)
 [[ -f $MOCK_HOME/client-done ]]
 while [[ $# -gt 1 ]]; do
  case $1 in
   -H) [[ $2 == 'Authorization: Bearer benchmark-only-rhiza-server-bench.'* ]]; shift ;;
   -o) printf '{"status":"archived"}\n' > "$2"; shift ;;
  esac
  shift
 done
 touch "$MOCK_HOME/archived"
 printf '0.123456' ;;
esac
MOCK
cat > "$scratch/bin/cat" <<'MOCK'
#!/usr/bin/env bash
case ${1:-} in
/proc/*/stat)
 pid=${1#/proc/}
 pid=${pid%/stat}
 user=12 system=3 start=123
 if [[ -f $MOCK_HOME/client-done ]]; then
  user=19 system=5
  [[ $MOCK_MODE != resource-replaced ]] || start=124
 fi
 printf '%s (mock ) node) S 1 0 0 0 0 0 0 0 0 0 %s %s 0 0 0 0 0 0 %s\n' "$pid" "$user" "$system" "$start" ;;
*) exec /bin/cat "$@" ;;
esac
MOCK
cat > "$scratch/bin/awk" <<'MOCK'
#!/usr/bin/env bash
if [[ ${2:-} == /proc/*/status ]]; then
 if [[ $MOCK_MODE == resource-missing && -f $MOCK_HOME/client-done ]]; then exit 1; fi
 if [[ -f $MOCK_HOME/client-done ]]; then printf '800 1200\n'; else printf '500 700\n'; fi
else
 exec /usr/bin/awk "$@"
fi
MOCK
cat > "$scratch/bin/uname" <<'MOCK'
#!/usr/bin/env bash
if [[ $MOCK_MODE == healthy-nonlinux ]]; then printf 'Darwin\n'; else printf 'Linux\n'; fi
MOCK
cat > "$scratch/bin/getconf" <<'MOCK'
#!/usr/bin/env bash
[[ $1 == CLK_TCK ]] && printf '100\n'
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
for mode in normal shifted before-first after-last no-witness signal-failure wait-normal wait-crash wait-error healthy healthy-nonlinux resource-replaced resource-missing graph graph-wrong-ids kv kv-wrong-value async-stale-cut gcs-before-ack gcs-async gcs-missing gcs-unknown gcs-malformed; do
 mkdir "$scratch/$mode"
 # Each run's stand-ins and process evidence remain in its own scratch scope.
 cp "$scratch/node" "$scratch/bench" "$scratch/$mode/"
 fault=n0
 workload=sql
 durability=async
 publish=0
 provider=s3
 case $mode in
  shifted|healthy*|resource-*) fault=none ;;
  graph*) fault=none; workload=graph; durability=before-ack ;;
  kv*) fault=none; workload=kv; publish=1 ;;
  async-stale-cut) fault=none; publish=1 ;;
  gcs-*) fault=none; provider=gcs
   if [[ $mode == gcs-async ]]; then publish=1; else durability=before-ack; fi ;;
 esac
 status=0
 BASH_ENV="$scratch/wait-env" MOCK_HOME="$scratch/$mode" MOCK_MODE="$mode" PATH="$scratch/bin:$PATH" TMPDIR="$scratch/$mode" \
 RHIZA_SERVER_BENCH_REQUESTS=4 RHIZA_SERVER_BENCH_CONCURRENCY=2 RHIZA_SERVER_BENCH_FAULT_AFTER=0 RHIZA_SERVER_BENCH_FAILED_NODE="$fault" \
 RHIZA_SERVER_BENCH_WORKLOAD="$workload" RHIZA_SERVER_BENCH_DURABILITY="$durability" RHIZA_SERVER_BENCH_ASYNC_PUBLISH="$publish" \
 RHIZA_SERVER_BENCH_PROVIDER="$provider" RHIZA_OBJSTORE_SERVICE_ACCOUNT=ambient-must-not-inherit HTTP_PROXY=http://ambient-must-not-inherit \
 bash "$here/run-server-reference.sh" "$scratch/base-server" "$scratch/$mode/result.json" >"$scratch/$mode/stdout" 2>"$scratch/$mode/stderr" || status=$?
 if [[ $mode == normal ]]; then
  [[ $status == 0 ]]
  jq -e '.fault_overlap.overlap == true and .fault_exit_status == 137 and .receipt_oracle == "sql-insert-id-v1" and (.client_sha256 | length == 64)' "$scratch/$mode/result.json" >/dev/null
  jq -e '.object_store_provider == "s3" and .gcs_request_classes == null' "$scratch/$mode/result.json" >/dev/null
  jq -e '.voter_resource_status == "not_collected_fault_lane" and .voter_resources == null and .host_pressure == null' "$scratch/$mode/result.json" >/dev/null
  grep -qx 'fault_exit_status=137' "$scratch/$mode/result-fault-metadata.txt"
 elif [[ $mode == healthy ]]; then
  [[ $status == 0 ]]
  jq -e '.voter_resource_status == "measured" and .voter_resources.clock_ticks_per_second == 100
    and ([.voter_resources.nodes[].node] == ["n0", "n1", "n2"])
    and (.host_pressure | keys == ["cpu_some_us", "cpu_steal_ticks", "io_some_us"])
    and all(.voter_resources.nodes[]; .process_start_ticks == 123 and .cpu_user_ticks == 7 and .cpu_system_ticks == 2
      and .rss_kib_before == 500 and .rss_kib_after == 800
      and .lifetime_hwm_kib_before == 700 and .lifetime_hwm_kib_after == 1200)' "$scratch/$mode/result.json" >/dev/null
 elif [[ $mode == healthy-nonlinux ]]; then
  [[ $status == 0 ]]
  jq -e '.voter_resource_status == "not_collected_non_linux" and .voter_resources == null and .host_pressure == null' "$scratch/$mode/result.json" >/dev/null
 elif [[ $mode == resource-* ]]; then
  [[ $status != 0 ]]
  if [[ $mode == resource-replaced ]]; then
   grep -q 'voter identity changed' "$scratch/$mode/stderr"
  else
   grep -q 'cannot sample Linux voter resources' "$scratch/$mode/stderr"
  fi
  jq -e '.successes == 4 and (has("voter_resources") | not)' "$scratch/$mode/result.json" >/dev/null
 elif [[ $mode == graph ]]; then
  [[ $status == 0 ]]
  jq -e '.workload == "graph" and .receipt_oracle == "applied-mutation-v1" and .verification.exact_identity_rows == 4
    and .write_object_store_delta.puts == 6 and .async_publication == null' "$scratch/$mode/result.json" >/dev/null
 elif [[ $mode == kv ]]; then
  [[ $status == 0 ]]
  jq -e '.workload == "kv" and .receipt_oracle == "applied-mutation-v1" and .verification.receipt_oracle == "kv-benchmark-value-v1"
    and .write_object_store_delta.puts == 6 and .object_store_delta.puts == 6
    and .async_publication.elapsed_seconds == 0.123456 and .async_publication.object_store_delta.puts == 27
    and .async_publication.before.archive_tip == 1 and .async_publication.after.archive_tip == 8' "$scratch/$mode/result.json" >/dev/null
 elif [[ $mode == graph-wrong-ids || $mode == kv-wrong-value || $mode == async-stale-cut ]]; then
  [[ $status != 0 ]]
  jq -e '.successes == 4 and (has("async_publication") | not)' "$scratch/$mode/result.json" >/dev/null
  if [[ $mode == async-stale-cut ]]; then
   grep -q 'did not cover the certified applied cut' "$scratch/$mode/stderr"
  else
   [[ ! -f $scratch/$mode/archived ]]
  fi
 elif [[ $mode == gcs-before-ack || $mode == gcs-async ]]; then
  [[ $status == 0 ]]
  jq -e '.object_store_provider == "gcs" and .gcs_request_classes.write.class_b_attempts == 1
    and .gcs_request_classes.write.transport_attempts == 6
    and .gcs_request_classes.write.transport_minus_observed == 5
    and .gcs_request_classes.verification.http_attempts == 0' "$scratch/$mode/result.json" >/dev/null
  if [[ $mode == gcs-before-ack ]]; then
   jq -e '.gcs_request_classes.publication == null and .gcs_request_classes.write.access_record_shapes.path_only == 1' "$scratch/$mode/result.json" >/dev/null
  else
   jq -e '.gcs_request_classes.publication.class_a_attempts == 1 and .gcs_request_classes.publication.transport_attempts == 27
    and .gcs_request_classes.write.access_record_shapes.query_and_fragment == 1
    and .gcs_request_classes.publication.access_record_shapes.query == 1' \
    "$scratch/$mode/result.json" >/dev/null
  fi
 elif [[ $mode == gcs-missing || $mode == gcs-unknown || $mode == gcs-malformed ]]; then
  [[ $status != 0 ]]
  grep -q 'lacks classifiable completed-request evidence' "$scratch/$mode/stderr"
  [[ -f $scratch/$mode/result-gcs-write-classes.json ]]
  if [[ $mode == gcs-malformed ]]; then
   jq -e '.malformed_access_lines == 1 and .access_record_shapes.malformed == 1' "$scratch/$mode/result-gcs-write-classes.json" >/dev/null
  fi
 else
  [[ $status != 0 ]]
  if [[ $mode == wait-* ]]; then
   case $mode in wait-normal) raw=0; want=1 ;; wait-crash) raw=139; want=139 ;; wait-error) raw=127; want=127 ;; esac
   [[ $status == "$want" ]]
   grep -qx "fault_exit_status=$raw" "$scratch/$mode/result-fault-metadata.txt"
   jq -e --argjson raw "$raw" '.qualification == "not_qualified" and .fault_exit_status == $raw' "$scratch/$mode/result.json" >/dev/null
  elif [[ $mode == signal-failure ]]; then [[ $status == 47 ]];
  elif [[ $mode != shifted ]]; then grep -Eq 'fault (coordinator failed|interval not qualified)' "$scratch/$mode/stderr"; fi
 fi
 if [[ $provider == gcs ]]; then
  [[ -f $scratch/$mode/gcs-removed ]]
  [[ $(sort "$scratch/$mode/gcs-local-adc") == $'n0\nn1\nn2' ]]
  if grep -Eq 'synthetic-private-(query|fragment)' "$scratch/$mode/stderr" "$scratch/$mode"/result*.json; then
   printf 'GCS diagnostics exposed a discarded URI suffix\n' >&2
   exit 1
  fi
 fi
 case $workload in
  sql) grep -q -- '-verify-insert-receipt' "$scratch/$mode/bench.args" ;;
  graph) grep -q -- '-path /graph/execute.*-verify-applied-receipt' "$scratch/$mode/bench.args" ;;
  kv) grep -q -- '-path /kv/get.*-verify-kv-value' "$scratch/$mode/kv-verification.args" ;;
 esac
 if [[ $fault != none ]]; then grep -q -- '-fault-start-time 123' "$scratch/$mode/bench.args"; fi
 grep -Fq "$scratch/base-server|build -o" "$scratch/$mode/builds"
 grep -Fq "$(cd "$here/.." && pwd)|build -o" "$scratch/$mode/builds"
 while IFS= read -r pid; do
  if kill -0 "$pid" 2>/dev/null; then printf 'mock owned process survived: %s\n' "$pid" >&2; exit 1; fi
 done < <(cat "$scratch/$mode/nodes" "$scratch/$mode/bench.pid")
done
printf 'server oracle helpers + 23 complete mocked runner cases PASS\n'
