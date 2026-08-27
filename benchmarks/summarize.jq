def median: sort | .[length / 2 | floor];

group_by(.config, .workload)
| map({
    config: .[0].config,
    workload: .[0].workload,
    runs: length,
    requests: (map(.result.requests) | add),
    errors: (map(.result.errors) | add),
    ops_per_sec: (map(.result.ops_per_sec) | median),
    p50_ms: (map(.result.p50_ms) | median),
    p95_ms: (map(.result.p95_ms) | median),
    p99_ms: (map(.result.p99_ms) | median),
    max_ms: (map(.result.max_ms) | max),
    uploads: (map(.object_delta.uploads) | median),
    gets: (map(.object_delta.gets) | median),
    heads: (map(.object_delta.heads) | median),
    lists: (map(.object_delta.lists) | median),
    deletes: (map(.object_delta.deletes) | median),
    bytes_uploaded: (map(.object_delta.bytes_uploaded) | median),
    bytes_downloaded: (map(.object_delta.bytes_downloaded) | median),
    s3_http_requests: (map(.object_delta.s3_http_requests) | median),
    s3_http_failures: (map(.object_delta.s3_http_failures) | add)
  })
