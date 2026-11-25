#!/bin/sh
set -eu
validator=scripts/ci-local-compaction-summary.jq
base='{"boundaries_valid":true,"metrics_present":true,"p99_comparable":false,"phase_observation_valid":true,"phase_events_dropped":0,"observation":"limited_successful_samples","restore_mode":"graceful_shutdown_then_empty_data_dir","overlap_requests":2,"overlap_success":1,"overlap_errors":1,"acknowledged":1,"seed":256,"workers":8,"cycles_planned":32,"cycles_completed":32,"cycles_total_ms":2,"p50_ms":0.2,"p99_ms":0.2,"build_ms":1,"build_start_unix_ns":1000000000,"build_end_unix_ns":1001000000}'
base=$(printf '%s\n' "$base" | jq -c '. + {phase_quiesce_starts:32,phase_quiesce_ends:32,phase_quiesce_sequence_valid:true,phase_unlinked_success:0,phase_unlinked_failure:0,phase_trace_mismatch:0,phase_instrumentation:"bounded_channel_nonblocking_but_nonzero_overhead"}')
valid() { printf '%s\n' "$1" | jq -e -s -f "$validator" >/dev/null; }
invalid() { if valid "$1" 2>/dev/null; then echo "invalid fixture accepted: $1" >&2; exit 1; fi; }
mutate() { printf '%s\n' "$base" | jq -c "$1"; }
valid "$base"
valid "$(mutate '.overlap_success=0 | .overlap_errors=2 | .p50_ms=null | .p99_ms=null | .observation="all_overlapping_requests_failed"')"
valid "$(mutate '.overlap_requests=100 | .overlap_success=100 | .overlap_errors=0 | .acknowledged=100 | .p99_comparable=true | .observation="p99_sample_sufficient"')"
invalid "$(mutate '.acknowledged=0')"
invalid "$(mutate '.build_end_unix_ns=999999999')"
invalid "$(mutate '.build_ms=-1')"
invalid "$(mutate '.build_ms=2')"
invalid "$(mutate '.cycles_completed=31')"
invalid "$(mutate '.phase_events_dropped=1')"
invalid "$(mutate '.phase_quiesce_starts=0 | .phase_quiesce_ends=0')"
invalid "$(mutate '.phase_unlinked_success=1')"
invalid "$(mutate '.phase_unlinked_failure=1')"
invalid "$(mutate '.phase_trace_mismatch=1')"
invalid "$(mutate '.observation="p99_sample_sufficient"')"
invalid "$(mutate '.overlap_success=0 | .overlap_errors=2 | .observation="all_overlapping_requests_failed"')"
invalid "$(mutate '.p99_ms=0.1')"
invalid "$(mutate '.overlap_errors=1.5')"
invalid '{}'
invalid '{'
invalid ''
invalid "$(printf '%s\n%s\n' "$base" "$base")"
echo 'Local compaction summary validation fixtures passed'
