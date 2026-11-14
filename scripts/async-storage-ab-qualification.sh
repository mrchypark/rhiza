#!/bin/sh
# Compare the same instrumented fixture against two clean checked-out revisions.
set -eu

reconcile_requests() {
  awk '
    /node_s3_transport_request_count / {
      phase=count=""
      for (i=1; i<=NF; i++) {
        split($i, p, "=")
        if (p[1] == "phase") phase=p[2]
        if (p[1] == "count") count=p[2]
      }
      if (phase == "measurement" && count ~ /^[0-9]+$/) { client+=count; groups++ }
    }
    /versity_server_phase=measurement / {
      status=count=""
      for (i=1; i<=NF; i++) {
        split($i, p, "=")
        if (p[1] == "status") status=p[2]
        if (p[1] == "request_count_delta") count=p[2]
      }
      if (status == "measured" && count ~ /^[0-9]+$/) { server=count; observations++ }
    }
    END {
      if (groups == 0 || observations != 1) {
        print "status=unverifiable reason=missing_or_ambiguous_evidence"
      } else if (client != server) {
        printf "status=unverifiable reason=boundary_or_request_mismatch client=%d server=%d\n", client, server
      } else {
        printf "status=unverifiable reason=counts_equal_but_boundaries_not_synchronized client=%d server=%d\n", client, server
      }
    }
  '
}

if [ "$#" -eq 1 ] && [ "$1" = --self-test ]; then
  actual=$(printf '%s\n' 'node_s3_transport_request_count phase=measurement count=2' 'versity_server_phase=measurement status=measured request_count_delta=3' | reconcile_requests)
  [ "$actual" = 'status=unverifiable reason=boundary_or_request_mismatch client=2 server=3' ]
  actual=$(printf '%s\n' 'node_s3_transport_request_count phase=measurement count=2' 'versity_server_phase=measurement status=measured request_count_delta=2' | reconcile_requests)
  [ "$actual" = 'status=unverifiable reason=counts_equal_but_boundaries_not_synchronized client=2 server=2' ]
  actual=$(printf '%s\n' 'node_s3_transport_request_count phase=measurement count=2' | reconcile_requests)
  [ "$actual" = 'status=unverifiable reason=missing_or_ambiguous_evidence' ]
  echo 'reconciliation reporting self-test passed'
  exit 0
fi

if [ "$#" -ne 7 ]; then
  echo 'usage: sh scripts/async-storage-ab-qualification.sh BASE_DIR BASE_SHA CANDIDATE_DIR CANDIDATE_SHA BATCH_COMMIT VERSITY_BIN NEW_EVIDENCE_DIR' >&2
  exit 2
fi
base_dir=$1
base_sha=$2
candidate_dir=$3
candidate_sha=$4
batch_commit=$5
versity=$6
evidence=$7
for path in "$base_dir" "$candidate_dir" "$versity" "$evidence"; do
  case $path in /*) ;; *) echo 'all paths must be absolute' >&2; exit 2 ;; esac
done
# Verify the only semantic source difference is the reviewed batch commit.
test "$(git -C "$candidate_dir" rev-parse "$batch_commit^{commit}")" = "$batch_commit"
expected_patch=$(git -C "$candidate_dir" diff "$batch_commit^" "$batch_commit" | git patch-id --stable)
actual_patch=$(git -C "$candidate_dir" diff "$base_sha" "$candidate_sha" | git patch-id --stable)
test -n "$expected_patch"
test "${expected_patch%% *}" = "${actual_patch%% *}" || {
  echo 'base-to-candidate patch differs from batch commit' >&2
  exit 2
}
test -x "$versity"
test ! -e "$evidence"
for pair in "${base_dir}|${base_sha}" "${candidate_dir}|${candidate_sha}"; do
  dir=${pair%%|*}
  sha=${pair#*|}
  test "$(git -C "$dir" rev-parse HEAD)" = "$sha"
  test -z "$(git -C "$dir" status --porcelain)"
done
# Identical instrumentation is necessary for per-class comparisons.
for file in pkg/node/foreground_cost_lifecycle_test.go internal/objstore/transport_observer_enabled.go internal/objstore/transport_observer_test.go; do
  cmp -s "$base_dir/$file" "$candidate_dir/$file" || {
    echo "fixture differs: $file" >&2
    exit 2
  }
done
mkdir "$evidence"
printf 'base=%s\ncandidate=%s\nversity_sha256=%s\ngo=%s\n' "$base_sha" "$candidate_sha" "$(shasum -a 256 "$versity" | awk '{print $1}')" "$(go version)" > "$evidence/environment.txt"
printf 'run\tmode\trevision\texit\n' > "$evidence/exits.tsv"

run_one() {
  label=$1 mode=$2 dir=$3 revision=$4
  log="$evidence/$label-$mode.log"
  raw="$evidence/$label-$mode-gateway"
  mkdir "$raw"
  status=0
  (cd "$dir" && env RHIZA_VERSITYGW_BIN="$versity" RHIZA_VERSITY_EVIDENCE_DIR="$raw" RHIZA_NODE_FOREGROUND_API_COST=1 RHIZA_AB_PUBLICATION_EVIDENCE=1 RHIZA_NODE_FOREGROUND_API_COST_SCENARIO=baseline RHIZA_NODE_FOREGROUND_DURABILITY="$mode" go test -count=1 -v -tags rhiza_local_testhooks -timeout=7m ./pkg/node -run '^TestNodeForegroundAPICostS3$') > "$log" 2>&1 || status=$?
  printf '%s\t%s\t%s\t%s\n' "$label" "$mode" "$revision" "$status" >> "$evidence/exits.tsv"
  found=0
  for run_dir in "$raw"/versity-*; do
    [ -d "$run_dir" ] || continue
    found=1
    grep -Fqx 'gateway_reaped=true' "$run_dir/gateway-complete" || { echo "gateway cleanup not verified: $run_dir" >&2; return 1; }
  done
  [ "$found" -eq 1 ] || { echo "no gateway evidence: $raw" >&2; return 1; }
  [ "$status" -eq 0 ] || { echo "failed: $log" >&2; return "$status"; }
  grep -q '^--- PASS: TestNodeForegroundAPICostS3 ' "$log" || { echo "no passing test: $log" >&2; return 1; }
  grep -E 'node_foreground_phase=measurement |node_foreground_cohort phase=measurement |node_s3_transport_request_count |versity_server_phase=|node_objectstore_phase=|node_foreground_publication_boundary |node_foreground_scenario=|node_foreground_.*p99' "$log" > "$evidence/$label-$mode-counts.txt"
  grep -q 'node_s3_transport_request_count ' "$evidence/$label-$mode-counts.txt"
  grep -q 'node_foreground_phase=measurement ' "$evidence/$label-$mode-counts.txt"
  grep -q 'node_foreground_publication_boundary ' "$evidence/$label-$mode-counts.txt"
  reconcile_requests < "$log" > "$evidence/$label-$mode-reconciliation.txt"
  awk '
    /node_foreground_cohort phase=measurement cohort=maintenance_(overlap|non_overlap) / {
      for (i=1; i<=NF; i++) {
        split($i, p, "=")
        if (p[1] == "puts_in_window") puts+=p[2]
        if (p[1] == "gets_in_window") gets+=p[2]
        if (p[1] == "put_errors_in_window") put_errors+=p[2]
        if (p[1] == "get_errors_in_window") get_errors+=p[2]
      }
    }
    /node_s3_transport_request_count / {
      method=resource=count=phase=""
      for (i=1; i<=NF; i++) {
        split($i, p, "=")
        if (p[1] == "method") method=p[2]
        if (p[1] == "resource_class") resource=p[2]
        if (p[1] == "phase") phase=p[2]
        if (p[1] == "count") count=p[2]
      }
      if (phase == "measurement" && method != "" && resource != "" && count ~ /^[0-9]+$/) { counts[method SUBSEP resource]+=count; groups++ }
    }
    END {
      operations=puts+gets-put_errors-get_errors
      if (operations <= 0 || groups == 0) exit 1
      print "method\tresource_class\tmeasurement_attempts\tper_1000_successful_ops_in_window"
      for (key in counts) {
        split(key, parts, SUBSEP)
        printf "%s\t%s\t%d\t%.4f\n", parts[1], parts[2], counts[key], counts[key]*1000/operations
      }
    }
  ' "$log" > "$evidence/$label-$mode-normalized.tsv"
}

# ABBA is run for each durability mode; stop at the first failure.
for mode in async before-ack; do
  run_one a1 "$mode" "$base_dir" "$base_sha"
  run_one b1 "$mode" "$candidate_dir" "$candidate_sha"
  run_one b2 "$mode" "$candidate_dir" "$candidate_sha"
  run_one a2 "$mode" "$base_dir" "$base_sha"
done
printf '%s\n' 'Counts are client physical HTTP attempts; compare Versity server deltas in raw counts, but observer and server boundaries differ and request-start and completed-operation cohorts need not match.' 'Published records/value bytes use verified archive tip snapshots before the window and after drain, not exact cutoff. Node tips are sampled sequentially and slot lag is not an ACKed-write backlog.' 'Provider-specific billing class mapping, independent retry counts, exact cutoff publication and ACKed-write backlog remain unverified. Async ACK is not publication.' > "$evidence/interpretation.txt"
