length == 1 and
(.[0] |
  type == "object" and
  ((["boundaries_valid", "metrics_present", "p99_comparable", "phase_observation_valid", "phase_events_dropped", "phase_quiesce_starts", "phase_quiesce_ends", "phase_quiesce_sequence_valid", "phase_unlinked_success", "phase_unlinked_failure", "phase_trace_mismatch", "phase_instrumentation", "observation", "restore_mode", "overlap_requests", "overlap_success", "overlap_errors", "acknowledged", "seed", "workers", "cycles_planned", "cycles_completed", "cycles_total_ms", "p50_ms", "p99_ms"] - keys) | length == 0) and
  (.boundaries_valid | type == "boolean") and
  (.metrics_present | type == "boolean") and
  (.p99_comparable | type == "boolean") and
  (.phase_observation_valid | type == "boolean") and
  (.phase_quiesce_sequence_valid | type == "boolean") and
  (.phase_instrumentation == "bounded_channel_nonblocking_but_nonzero_overhead") and
  (.observation | type == "string") and
  (.restore_mode == "graceful_shutdown_then_empty_data_dir") and
  ([.overlap_requests, .overlap_success, .overlap_errors, .acknowledged, .seed, .workers, .cycles_planned, .cycles_completed, .phase_events_dropped, .phase_quiesce_starts, .phase_quiesce_ends, .phase_unlinked_success, .phase_unlinked_failure, .phase_trace_mismatch] | all(type == "number" and . >= 0 and . == floor)) and
  (.cycles_planned == 32 and .cycles_completed <= .cycles_planned) and
  (.cycles_total_ms | type == "number" and . >= 0) and
  ((.p50_ms == null) or (.p50_ms | type == "number" and . >= 0)) and
  ((.p99_ms == null) or (.p99_ms | type == "number" and . >= 0)) and
  (if .overlap_success == 0 then .p50_ms == null and .p99_ms == null else .p50_ms != null and .p99_ms != null end) and
  (if .overlap_success > 0 then .p50_ms <= .p99_ms else true end) and
  (.overlap_requests == (.overlap_success + .overlap_errors)) and
  (.acknowledged >= .overlap_success) and
  (.metrics_present == (.boundaries_valid and .overlap_requests > 0)) and
  (.phase_observation_valid == (.phase_events_dropped == 0 and (has("shutdown_error") | not) and .phase_quiesce_sequence_valid and .phase_quiesce_starts >= .cycles_planned and .phase_quiesce_starts == .phase_quiesce_ends and .phase_unlinked_success == 0 and .phase_unlinked_failure == 0 and .phase_trace_mismatch == 0)) and
  (.p99_comparable == (.boundaries_valid and .phase_observation_valid and .overlap_success >= 100)) and
  (.observation == (if .boundaries_valid | not then "missing_build_boundaries"
    elif .overlap_requests == 0 then "no_overlapping_requests"
    elif .overlap_success == 0 then "all_overlapping_requests_failed"
    elif .overlap_success < 100 then "limited_successful_samples"
    else "p99_sample_sufficient" end)) and
  (if .boundaries_valid then
    ([.build_ms, .build_start_unix_ns, .build_end_unix_ns] | all(type == "number" and . >= 0)) and
    (.cycles_completed == .cycles_planned) and
    (.build_end_unix_ns >= .build_start_unix_ns) and
    (.build_ms <= ((.build_end_unix_ns - .build_start_unix_ns) / 1000000 + 0.002))
   else .overlap_requests == 0 end)
)
