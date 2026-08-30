# Review fixes: Dory 3-peer benchmark

## Result

The review fixes passed the complete SQL/KV and Graph/KV test suites, focused race tests, and the Dory three-peer matrix. All 78 aggregated workload rows have zero client errors. The newest checkpoint-stress runs also report zero unexpected S3 4xx/5xx responses.

Compared with `2026-08-28-archive-refresh`, healthy median latency improved across every common read/write workload:

| Profile / mode | Workload | Baseline p50 | Candidate p50 | Change |
|---|---:|---:|---:|---:|
| Graph async | local read | 3.165 ms | 1.680 ms | -46.9% |
| Graph async | consensus read | 5.706 ms | 2.341 ms | -59.0% |
| Graph async | write | 45.942 ms | 13.264 ms | -71.1% |
| Graph before-ack | write | 30.474 ms | 23.952 ms | -21.4% |
| SQL async | local read | 1.914 ms | 1.264 ms | -34.0% |
| SQL async | consensus read | 2.769 ms | 1.578 ms | -43.0% |
| SQL async | write | 22.272 ms | 12.332 ms | -44.6% |
| SQL before-ack | write | 31.279 ms | 15.363 ms | -50.9% |

The primary consensus change removes the separate StageValue quorum from the normal proposal path and inlines the value in the first Record. The in-memory mock remains fsync-dominated (healthy 25.6 ms; one peer unavailable 21.9 ms), but the deployed service removes one peer frame/round trip and shows the latency reductions above.

## One-peer fault and recovery

All SQL and Graph workloads completed without client errors with one peer killed. The runner now waits for a successful linearizable probe before injecting chaos and records recovery time. A Graph checkpoint-stress fault run completed 18,900 requests without error; after the killed peer restarted it became ready and returned a linearizable KV response at consensus/materialized slot 396.

The newest 1-second checkpoint runs completed without `history compacted` loops or `unknown replicated command`. SQL and Graph checkpoint publishing continued while reads and writes were served.

## CPU and memory

Values are three-peer aggregate CPU and per-pod cgroup memory. One-fault rows can observe two pods and carry `observed_pods` in `resources.json`.

| Profile / mode | CPU baseline | CPU candidate | Memory baseline | Memory candidate |
|---|---:|---:|---:|---:|
| Graph async healthy | 0.356 cores | 0.421 cores | 58.8 MiB | 61.2 MiB |
| Graph before-ack healthy | 0.429 cores | 0.385 cores | 58.3 MiB | 53.3 MiB |
| SQL async healthy | 0.303 cores | 0.236 cores | 41.4 MiB | 41.2 MiB |
| SQL before-ack healthy | 0.277 cores | 0.229 cores | 42.3 MiB | 39.2 MiB |

Graph async CPU increased 18.2% in this short run while its latency and throughput improved materially. The other healthy profiles use 10-22% less aggregate CPU; memory is within +4.1% to -9.0% of baseline.

## Object-store calls

Before-ack totals cover 600 writes per configuration plus background work:

| Profile | Baseline healthy | Candidate healthy | Baseline one fault | Candidate one fault |
|---|---:|---:|---:|---:|
| Graph | 640 | 550 (-14.1%) | 760 | 595 (-21.7%) |
| SQL | 550 | 485 (-11.8%) | 632 | 480 (-24.1%) |

The candidate records conditional PUT conflicts and content-addressed dedup separately from HTTP failures. Expected 409/412 outcomes and missing-object HEAD probes no longer inflate `s3_http_failures`; logical not-found and condition results remain visible in logical counters. Exact per-method totals, bytes, conflicts, retries, and failures are in `object-store-totals.json` and `summary.json`.

## Evidence and limits

- Raw request samples: `raw/*.ndjson`
- CPU and cgroup memory: `resources/` and `resources.json`
- Medians: `summary.json` / `summary.csv`
- Baseline deltas: `comparison.json`
- QuePaxa microbenchmark: `quepaxa-inline-record.txt`

These are short single-host Dory/MinIO measurements, not capacity or WAN claims. The older `current-graph-async-checkpoint-1s` row predates the final live-recovery and missing-HEAD classification changes; use `current-graph-live-recovery-checkpoint-1s` and its one-fault counterpart for checkpoint conclusions.
