# LatticeDB graph-engine qualification

Rhiza used three graph peers on local Dory K3s. Each workload has three runs;
the table reports the median. The comparison baseline is the 2026-08-27
GoraphDB result in `../2026-08-27-current-vs-72034b5`.

| mode | fault | workload | ops/s | p50 ms | p95 ms | p99 ms | S3 calls/100 writes |
|---|---:|---|---:|---:|---:|---:|---:|
| async | 0 | local read | 3448.05 | 2.678 | 9.490 | 64.916 | 0 |
| async | 0 | linearizable read | 2859.81 | 4.494 | 10.498 | 26.624 | 0 |
| async | 0 | write | 863.68 | 15.342 | 28.133 | 34.987 | 0 |
| async | 1 | local read | 4818.31 | 2.406 | 7.943 | 13.933 | 0 |
| async | 1 | linearizable read | 2652.26 | 3.284 | 15.474 | 47.118 | 0 |
| async | 1 | write | 616.51 | 16.814 | 56.014 | 60.525 | 0 |
| before-ack | 0 | local read | 3304.11 | 2.266 | 11.510 | 36.476 | 0 |
| before-ack | 0 | linearizable read | 2476.06 | 4.023 | 15.264 | 45.907 | 0 |
| before-ack | 0 | write | 419.16 | 34.184 | 53.331 | 72.294 | 76 |
| before-ack | 1 | local read | 2913.98 | 4.157 | 9.390 | 24.548 | 0 |
| before-ack | 1 | linearizable read | 1568.40 | 7.813 | 23.789 | 54.583 | 0 |
| before-ack | 1 | write | 260.96 | 43.739 | 81.446 | 92.795 | 80 |

Against GoraphDB, healthy graph-write throughput improved 46.5% in async mode
and 40.0% in before-ack mode. With one failed peer it improved 16.0% and 8.4%.
Healthy local-read throughput regressed 33.0% and healthy linearizable-read
throughput regressed 25.4%; the one-fault before-ack read run was especially
noisy. This is therefore a write-path and allocation improvement, not an
unconditional read-performance win.

The in-process graph apply microbenchmark was 305,061 ns/op, 5,213 B/op, and
141 allocs/op. The prior GoraphDB result was about 0.35 ms/op, 356.6 KiB/op,
and 459 allocs/op. Cluster CPU seconds were higher in the healthy runs, while
peak memory was 69.0-81.4 MiB. Raw cgroup samples are retained under
`resources/`; workload and object-store counters are retained under `raw/`.

Before-ack graph writes produced a median 38 uploads, 19 GETs, and 19 HEADs
per 100 writes when healthy, and 40/20/20 with one failed peer. Engine choice
does not change the archive protocol; differences from the GoraphDB call count
are batching and run-timing effects. Async writes made no foreground object
calls. Two S3 failures recorded in one async fault run came from background
recovery, while all benchmark requests still completed without errors.

The Dory Chaos Mesh E2E result was: one failed peer preserved quorum writes
(51.629 ms sample) and converged after recovery; two failed peers rejected
writes with HTTP 503; recovery restored writes and shared-object recovery
completed. Graph checkpointing currently takes a bounded-memory LatticeDB
backup while graph transactions are paused. An online page snapshot should be
added only if measured checkpoint pauses exceed the service budget.
