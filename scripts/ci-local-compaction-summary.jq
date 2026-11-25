length == 1 and
(.[0] |
  type == "object" and
  ((["boundaries_valid", "metrics_present", "p99_comparable", "observation", "restore_mode", "overlap_requests", "overlap_success", "overlap_errors", "acknowledged", "seed", "workers", "reclaim_ms", "p50_ms", "p99_ms"] - keys) | length == 0) and
  (.boundaries_valid | type == "boolean") and
  (.metrics_present | type == "boolean") and
  (.p99_comparable | type == "boolean") and
  (.observation | type == "string") and
  (.restore_mode == "graceful_shutdown_then_empty_data_dir") and
  ([.overlap_requests, .overlap_success, .overlap_errors, .acknowledged, .seed, .workers] | all(type == "number" and . >= 0 and . == floor)) and
  (.reclaim_ms | type == "number" and . >= 0) and
  ((.p50_ms == null) or (.p50_ms | type == "number" and . >= 0)) and
  ((.p99_ms == null) or (.p99_ms | type == "number" and . >= 0)) and
  (if .overlap_success == 0 then .p50_ms == null and .p99_ms == null else .p50_ms != null and .p99_ms != null end) and
  (if .overlap_success > 0 then .p50_ms <= .p99_ms else true end) and
  (.overlap_requests == (.overlap_success + .overlap_errors)) and
  (.acknowledged >= .overlap_success) and
  (.metrics_present == (.boundaries_valid and .overlap_requests > 0)) and
  (.p99_comparable == (.boundaries_valid and .overlap_success >= 100)) and
  (.observation == (if .boundaries_valid | not then "missing_build_boundaries"
    elif .overlap_requests == 0 then "no_overlapping_requests"
    elif .overlap_success == 0 then "all_overlapping_requests_failed"
    elif .overlap_success < 100 then "limited_successful_samples"
    else "p99_sample_sufficient" end)) and
  (if .boundaries_valid then
    ([.build_ms, .build_start_unix_ns, .build_end_unix_ns] | all(type == "number" and . >= 0)) and
    (.build_end_unix_ns >= .build_start_unix_ns) and
    (((.build_end_unix_ns - .build_start_unix_ns) / 1000000 - .build_ms) | if . < 0 then -. else . end) <= 0.002
   else .overlap_requests == 0 end)
)
