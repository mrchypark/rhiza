# Final re-review validation

## Verdict

The complete 3-peer Dory matrix served 151,200 requests with zero client errors. A separate four-profile confirmation run served 50,400 requests with zero errors. Final Graph checkpoint/recovery validation served another 12,600 requests with zero client, unexpected S3 4xx/5xx, transport, or S3 HTTP failures.

The final validation also deleted a peer pod backed by `emptyDir`, thereby removing all local state. SQL returned ready in 3 seconds at applied/consensus slot 406; Graph returned ready in 3 seconds at applied/consensus slot 415. Both recovered peers then served a linearizable query. This is a direct object-storage-backed recovery proof, not the surviving-quorum availability metric.

## Latency and throughput

Healthy values below are the independent confirmation medians. One-fault values are the complete matrix medians. Reads use 1,000 requests and writes use 100 requests per repetition, concurrency 16, three repetitions.

| Profile | State | local read p50 | consensus read p50 | write p50 | local read ops/s | consensus read ops/s | write ops/s |
|---|---|---:|---:|---:|---:|---:|---:|
| SQL async | healthy | 1.398 ms | 1.968 ms | 15.034 ms | 9,600 | 6,051 | 593 |
| SQL async | one fault | 1.539 ms | 2.166 ms | 16.569 ms | 9,133 | 6,699 | 633 |
| SQL before-ack | healthy | 1.340 ms | 1.923 ms | 23.473 ms | 10,594 | 7,327 | 562 |
| SQL before-ack | one fault | 1.407 ms | 1.908 ms | 16.890 ms | 8,703 | 7,306 | 611 |
| Graph async | healthy | 1.750 ms | 2.813 ms | 11.626 ms | 6,054 | 4,007 | 772 |
| Graph async | one fault | 1.632 ms | 2.049 ms | 10.057 ms | 4,816 | 5,613 | 1,105 |
| Graph before-ack | healthy | 1.855 ms | 2.712 ms | 29.375 ms | 5,922 | 3,840 | 439 |
| Graph before-ack | one fault | 1.560 ms | 1.947 ms | 18.636 ms | 7,962 | 6,635 | 647 |

The single-host short runs remain scheduler-sensitive: the first full matrix and the confirmation run sometimes differ by 20-40%. The QuePaxa in-memory control stayed at 25.7 ms for three-peer propose and 20.4 ms with one peer down, matching the preceding 25.6/21.9 ms range. There is no consistent consensus-core regression. Against `2026-08-29-review-fixes`, confirmed healthy p50 is mixed: Graph async write improved 12.3%, while the other confirmed primary operations range from +4.2% to +52.8%. The added same-snapshot read contract and object-store/recovery fencing are correctness costs; a capacity claim requires longer isolated runs, not these short Dory samples.

## CPU and memory

Values are aggregate CPU cores and mean per-pod cgroup memory from the complete matrix. One-fault rows observe two pods.

| Profile | State | CPU cores | memory current | memory peak |
|---|---|---:|---:|---:|
| SQL async | healthy | 0.270 | 38.5 MiB | 45.8 MiB |
| SQL async | one fault | 0.224 | 38.4 MiB | 46.3 MiB |
| SQL before-ack | healthy | 0.297 | 38.7 MiB | 45.4 MiB |
| SQL before-ack | one fault | 0.205 | 36.3 MiB | 43.5 MiB |
| Graph async | healthy | 0.384 | 49.6 MiB | 56.4 MiB |
| Graph async | one fault | 0.310 | 50.6 MiB | 56.9 MiB |
| Graph before-ack | healthy | 0.417 | 51.5 MiB | 58.2 MiB |
| Graph before-ack | one fault | 0.260 | 47.3 MiB | 53.2 MiB |

## Object-store calls

Exact totals cover all 18 workload samples in each configuration.

| Profile | State | HTTP calls | PUT | GET | HEAD | LIST | uploaded | downloaded |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| SQL async | healthy / one fault | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| SQL before-ack | healthy / one fault | 545 / 420 | 218 / 168 | 109 / 84 | 218 / 168 | 0 / 0 | 557,190 / 517,461 B | 13,516 / 10,416 B |
| Graph async | healthy / one fault | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Graph before-ack | healthy / one fault | 605 / 465 | 242 / 186 | 121 / 93 | 242 / 186 | 0 / 0 | 619,207 / 584,253 B | 15,004 / 11,532 B |
| SQL async, checkpoint 1s | healthy / one fault | 896 / 27 | 174 / 5 | 320 / 7 | 391 / 15 | 11 / 0 | 2,651,053 / 379,101 B | 3,397,196 / 6,066 B |
| Graph async, checkpoint 1s | healthy / one fault | 877 / 79 | 167 / 14 | 338 / 23 | 360 / 42 | 12 / 0 | 3,847,164 / 423,498 B | 9,399,502 / 206,234 B |

Conditional CAS conflicts remain visible as logical conflicts but are not HTTP failures. The final post-fix checkpoint run recorded five such expected conflicts and zero unexpected 4xx, 5xx, retries, transport failures, or S3 HTTP failures.

## Additional defects found and closed during validation

- Concurrent checkpoint completion and archive ticks could compact the same floor twice. The complete compaction sequence is now single-flight and rechecks the floor after acquiring the lock.
- A local tip below the archive recovery base repeatedly loaded an unusable suffix while waiting for a peer error. It now enters the pinned object-storage restore path immediately.
- A recreated peer rejected the cached 0-RTT ticket and consumed a full catch-up period. The RPC now promotes the same connection to 1-RTT and retries exactly once within the original deadline.
- Only explicitly normal initial `CURRENT`/publisher GET or HEAD misses are marked expected. Required root/block loss remains an unexpected failure.

The final Graph run has no `archive starts after checkpoint`, duplicate compaction-floor, or `0-RTT rejected` log entries.

## Evidence

- `raw/*.ndjson`: complete SQL/Graph, async/before-ack, healthy/one-fault matrix
- `summary.json`, `summary.csv`: medians and error totals
- `comparison.json`: row-level comparison with `2026-08-29-review-fixes`
- `resources.json`, `resources/`: CPU and memory evidence
- `object-store-totals.json`: API method and byte totals
- `confirmation/`, `confirmation-summary.json`: independent healthy rerun
- `post-marker-validation/`, `post-marker-validation-summary.json`: final image checkpoint/recovery proof
- `quepaxa.txt`: consensus microbenchmark
- `environment.json`: commit, diff, image, runtime, and cluster identity

Recovery certificate signing/key rotation and peer mTLS are intentionally excluded by user decision. These results are local single-host Dory/MinIO evidence, not WAN or production capacity claims.
