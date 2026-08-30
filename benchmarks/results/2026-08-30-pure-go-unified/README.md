# Pure-Go unified Rhiza verification

Candidate: `feature/pure-go-latticedb`

Dependency: `github.com/mrchypark/latticedb-go@32915dd7fe91d5d2ca36735a2fa410b90916f1ac`

All measurements used the same `rhiza-e2e:dev` image with SQL, Graph, and KV enabled.
Each benchmark summary is the median of three runs; raw NDJSON and cgroup samples are
stored beside the summaries.

| Mode | Workload | p50 | p95 | Throughput | Errors | Object calls |
|---|---|---:|---:|---:|---:|---:|
| async | SQL local read | 1.327 ms | 2.875 ms | 9,966 ops/s | 0/3,000 | 0 |
| async | SQL write | 18.815 ms | 60.600 ms | 529 ops/s | 0/300 | 0 |
| async | Graph local read | 1.549 ms | 3.502 ms | 8,323 ops/s | 0/3,000 | 0 |
| async | Graph write | 23.784 ms | 60.867 ms | 480 ops/s | 0/300 | 0 |
| before-ack | SQL write | 23.545 ms | 41.963 ms | 588 ops/s | 0/300 | 20 PUT / 10 GET / 20 HEAD |
| before-ack | Graph write | 26.779 ms | 58.728 ms | 458 ops/s | 0/300 | 28 PUT / 14 GET / 28 HEAD |

Compared with `2026-08-30-pro-p1-final-verification`, local reads remain in the
same range. Before-ack write latency is also close to the prior checkpoint-backed
measurements. Async writes are slower than the former split-profile builds because
every committed slot now advances the always-present Graph recovery metadata.

Final image verification:

- `CGO_ENABLED=0 GOEXPERIMENT=arenas,greenteagc go test ./...` inside the Linux image: pass.
- SQL 3-peer: one-peer quorum write and convergence, two-peer `503 commit_unknown`, retry, and shared-object recovery: pass.
- Graph 3-peer: one-peer quorum write and convergence, two-peer `503 commit_unknown`, retry, and shared-object recovery: pass.
- The scheduling-sensitive `TestPipelineAllowsLaterSlotToFinishFirst` failed once during the first image build, then passed 20 consecutive focused runs and the complete image rebuild.
