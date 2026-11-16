def number_or_null: if type == "number" then . else null end;
def bool_or_null: if type == "boolean" then . else null end;
def sample: if type != "object" then null else {
  start_unix_ns: (.start_unix_ns | number_or_null),
  measured_end_unix_ns: (.measured_end_unix_ns | number_or_null),
  latency_ns: (.latency_ns | number_or_null), retries: (.retries | number_or_null),
  succeeded: (.succeeded | bool_or_null), slot_known: (.slot_known | bool_or_null),
  slot: (.slot | number_or_null), tag_known: (.tag_known | bool_or_null),
  tag: (.tag | number_or_null), send_unix_ns: (.send_unix_ns | number_or_null),
  response_headers_unix_ns: (.response_headers_unix_ns | number_or_null)
} end;
{
  client_revision: (if (try (.client_revision | test("^[0-9a-f]{40}$")) catch false) then .client_revision else null end),
  client_sha256: (if (try (.client_sha256 | test("^[0-9a-f]{64}$")) catch false) then .client_sha256 else null end),
  durability_mode: (if .durability_mode == "before-ack" or .durability_mode == "async" then .durability_mode else null end),
  concurrency: (.concurrency | number_or_null), runtime_failure_lines: (.runtime_failure_lines | number_or_null),
  object_store_delta: ((if (.object_store_delta | type) == "object" then .object_store_delta else {} end) |
    {bytes_downloaded, bytes_uploaded, condition_conflicts, dedup_hits, deletes,
     failures, gets, heads, lists, uploads, http_delete_requests, http_get_requests,
     http_head_requests, http_other_requests, http_put_requests, http_requests,
     http_failures, http_4xx_unexpected, http_5xx, sdk_retries, transport_failures} |
    with_entries(select(.value | type == "number"))),
  result: (.result | if type != "object" then null else {
    requests: (.requests | number_or_null), successes: (.successes | number_or_null),
    errors: (.errors | number_or_null), retries: (.retries | number_or_null),
    duration_ms: (.duration_ms | number_or_null), ops_per_sec: (.ops_per_sec | number_or_null),
    success_ops_per_sec: (.success_ops_per_sec | number_or_null), p50_ms: (.p50_ms | number_or_null),
    p95_ms: (.p95_ms | number_or_null), p99_ms: (.p99_ms | number_or_null), max_ms: (.max_ms | number_or_null),
    slot_evidence: (.slot_evidence | if type != "object" then null else {
      slot_samples_enabled: (.slot_samples_enabled | bool_or_null),
      slot_samples_dropped: (.slot_samples_dropped | number_or_null), slot_unknown: (.slot_unknown | number_or_null),
      slot_samples: (if (.slot_samples | type) == "array" then [.slot_samples[:64][] | sample] else [] end)
    } end),
    tag_evidence: (.tag_evidence | if type != "object" then null else {
      tag_samples_enabled: (.tag_samples_enabled | bool_or_null),
      tag_samples_dropped: (.tag_samples_dropped | number_or_null),
      tag_sample_stride: (.tag_sample_stride | number_or_null),
      tag_sample_max: (.tag_sample_max | number_or_null),
      tag_samples: (if (.tag_samples | type) == "array" then [.tag_samples[:64][] | sample] else [] end)
    } end)
  } end)
}
