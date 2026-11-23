#!/bin/sh
set -eu
if [ "$#" -ne 5 ]; then echo 'usage: MODE SCENARIO CLIENT_BINARY SERVER_BINARY EVIDENCE_DIR' >&2; exit 2; fi
mode=$1
scenario=$2
driver=$3
server=$4
out=$5
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
mkdir "$out/temp"
docker ps -aq --filter name=rhiza-bench-s3- | sort > "$out/containers-before.txt"
status=0
TMPDIR="$out/temp" RHIZA_SERVICE_READ_EXPERIMENT=1 RHIZA_SERVICE_READ_DRIVER="$driver" \
 RHIZA_SERVICE_SERVER_BIN="$server" RHIZA_READ_MODE="$mode" RHIZA_READ_SCENARIO="$scenario" \
 RHIZA_READ_REQUESTS="${RHIZA_READ_REQUESTS:-200}" RHIZA_SERVER_BENCH_REQUESTS=200 \
 RHIZA_SERVER_BENCH_CONCURRENCY=1 RHIZA_SERVER_BENCH_TARGET_NODE=n1 \
 bash "$root/benchmarks/run-server-reference.sh" "$root" "$out/driver-result.json" > "$out/fixture.log" 2>&1 || status=$?
printf '%s\n' "$status" > "$out/fixture.exit"
docker ps -aq --filter name=rhiza-bench-s3- | sort > "$out/containers-after.txt"
diff -u "$out/containers-before.txt" "$out/containers-after.txt" > "$out/cleanup-containers.diff"
find "$out/temp" -mindepth 1 -print > "$out/cleanup-temp.txt"
[ ! -s "$out/cleanup-temp.txt" ]
[ "$status" = 0 ]
awk -F '\t' 'NF!=4 || $4!="gone" || seen[$1]++ { bad=1 } $2=="node" { n++ } $2=="driver" { d++ } END { exit (bad || NR!=4 || n!=3 || d!=1) }' "$out/driver-result-children-cleanup.tsv"
if [ "$scenario" = slow-ingress ]; then
	jq -e '.fault_request_observed == true and .fault_release_observed == true' "$out/driver-result.json" >/dev/null
fi
if [ "$scenario" = dead-ingress ]; then
	jq -e '.fault_stage == "upstream_response_received_then_held" and .fault_release_observed == true' "$out/driver-result.json" >/dev/null
fi
jq -e '.unknown_mutation_verified == true and (.first_request_ms | type == "number")' "$out/driver-result.json" >/dev/null
if [ "$mode" = enabled ] && [ "$scenario" != healthy ]; then
	jq -e '(.first_success_after_fault_ms | type == "number" and . >= 0) and .alternate_calls > 0 and .sticky_write_verified == true' "$out/driver-result.json" >/dev/null
fi
jq '. + {cleanup_verified:true,fault_verified:true}' "$out/driver-result.json" > "$out/result.json"
