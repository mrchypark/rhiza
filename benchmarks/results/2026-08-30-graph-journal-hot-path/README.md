# Deferred Graph journal confirmation

Baseline: `2b7b0d7` (`feature/pure-go-latticedb`)

Candidate: remove the second per-batch LatticeDB transaction that only deleted
recovery-journal entries. The next Graph transaction prunes only the prefix that
was already durable in SQLite before the current SQLite batch began. Entries for
the entire in-flight batch remain durable until SQLite commits, preserving crash
gap coverage.

## Materializer microbenchmark

Same Apple M3 host, `BenchmarkGraphApply`, five 500 ms samples:

| Metric | Baseline median | Candidate median | Change |
|---|---:|---:|---:|
| time | 692,616 ns/op | 607,453 ns/op | -12.3% |
| bytes | 513,893 B/op | 405,677 B/op | -21.1% |
| allocations | 3,603 allocs/op | 2,326 allocs/op | -35.4% |

Raw Go benchmark output is in `before.txt` and `after.txt`.

## Dory 3-peer async comparison

Each result is the median of three runs with zero request errors. The host load
rose from roughly 10 to 23 during these runs, so the absolute values and the
cross-image deltas are noisier than the microbenchmark.

| Workload | Baseline p50 | Candidate p50 | Baseline throughput | Candidate throughput |
|---|---:|---:|---:|---:|
| SQL write | 47.659 ms | 27.080 ms | 259 ops/s | 480 ops/s |
| SQL-side KV write | 37.033 ms | 33.007 ms | 369 ops/s | 467 ops/s |
| Graph write | 50.256 ms | 46.458 ms | 252 ops/s | 264 ops/s |
| Graph-side KV write | 82.301 ms | 63.363 ms | 135 ops/s | 112 ops/s |

The deterministic microbenchmark is the primary performance evidence. The Dory
run confirms no request failures and no obvious latency regression, but should be
repeated on an idle host before treating its percentages as release targets.

## Recovery verification

The candidate image passed both Dory Chaos Mesh suites:

- SQL: one-peer quorum write in 13.897 ms, convergence and rebuilt peer; two-peer loss returned `503 commit_unknown`, retry resolved exactly once, and shared-object recovery passed.
- Graph: one-peer quorum write in 51.367 ms and convergence; two-peer loss returned `503 commit_unknown`, retry resolved exactly once, and shared-object recovery passed.

The Graph before-ack run with a 1-second checkpoint interval also completed 300
Graph writes and 300 KV writes with zero request errors and active object-store
traffic. A subsequent full Graph chaos/restart cycle passed shared-object recovery
again. Its latency samples were load-contaminated (host load about 21), so they are
kept as correctness evidence rather than a performance comparison.
