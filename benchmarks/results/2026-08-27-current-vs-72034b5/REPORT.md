# Rhiza benchmark: current working tree versus `72034b5`

## Verdict

The protocol migration improves the primary healthy async paths and dramatically
reduces Graph checkpoint cost. It does not yet produce a uniform win: SQL
checkpoint overhead, Graph apply allocations, and several KV/before-ack paths
regressed. All 3-peer availability and shared-object recovery checks passed.

The candidate is the dirty working tree on `72034b5`; exact Rhiza and GoraphDB
patch fingerprints and image digests are in `environment.json`.

## Method

- Apple M3, 24 GiB, Go 1.27.0, `GOEXPERIMENT=arenas,greenteagc`.
- Local Dory K3s, three peers, QUIC peer transport, MinIO object store.
- Three repetitions; 1,000 requests per read run, 100 per write run, concurrency 16.
- SQL, Graph, and shared KV; local and linearizable reads; async and before-ack writes.
- Healthy and one-peer-fault candidate runs; two-peer-fault behavioral qualification.
- Normal serving benchmarks disabled checkpoints. Separate async runs used a deliberately
  aggressive 1 second checkpoint interval to expose checkpoint interference.
- Values below are medians. This is a closed-loop localhost benchmark, so tail values are
  useful for regression discovery but not production SLO prediction.

## Healthy serving path

| Mode | Workload | Baseline ops/s | Current ops/s | Change | Baseline p50 | Current p50 |
|---|---|---:|---:|---:|---:|---:|
| SQL async | local read | 3,605 | 2,550 | -29.3% | 3.55 ms | 3.52 ms |
| SQL async | linearizable read | 1,965 | 3,529 | +79.6% | 6.13 ms | 3.94 ms |
| SQL async | write | 678 | 745 | +9.9% | 18.49 ms | 18.93 ms |
| SQL before-ack | local read | 3,747 | 4,671 | +24.6% | 2.79 ms | 2.65 ms |
| SQL before-ack | linearizable read | 3,338 | 2,612 | -21.7% | 3.60 ms | 4.46 ms |
| SQL before-ack | write | 255 | 249 | -2.5% | 45.98 ms | 37.98 ms |
| Graph async | local read | 5,232 | 5,148 | -1.6% | 2.48 ms | 2.02 ms |
| Graph async | linearizable read | 3,425 | 3,834 | +12.0% | 3.70 ms | 3.19 ms |
| Graph async | write | 490 | 590 | +20.3% | 23.24 ms | 18.15 ms |
| Graph before-ack | local read | 4,258 | 3,911 | -8.1% | 2.92 ms | 3.22 ms |
| Graph before-ack | linearizable read | 2,472 | 3,153 | +27.5% | 4.39 ms | 4.21 ms |
| Graph before-ack | write | 294 | 299 | +1.7% | 47.81 ms | 49.85 ms |

KV results are mixed rather than uniformly improved. Async KV write throughput rose
49.1% in SQL and 9.9% in Graph. Before-ack Graph KV write fell 26.9%, and its p50
rose from 72.00 ms to 108.66 ms. The complete KV table is in `comparison.json`.

For each 100-write before-ack run, median billable S3 attempts were:

| Workload | Baseline | Current | Change |
|---|---:|---:|---:|
| SQL write | 65 | 44 | -32.3% |
| SQL KV write | 95 | 92 | -3.2% |
| Graph write | 45 | 56 | +24.4% |
| Graph KV write | 95 | 92 | -3.2% |

Normal async runs finished before the configured one-minute archive sync, so their
timed workload deltas are correctly zero; checkpoint object calls are measured below.

## Checkpoint stress

| Profile | CPU seconds | Peak memory | S3 requests | Upload calls | Uploaded bytes |
|---|---:|---:|---:|---:|---:|
| SQL baseline | 7.08 | 82.2 MiB | 57 | 28 | 1,278,905 |
| SQL current | 8.13 | 93.5 MiB | 95 | 36 | 1,658,129 |
| Graph baseline | 11.37 | 126.7 MiB | 149 | 68 | 1,428,200 |
| Graph current | 8.62 | 88.3 MiB | 29 | 7 | 287,548 |

Graph per-file checkpointing reduced CPU 24.2%, peak memory 30.3%, S3 requests
80.5%, upload calls 89.7%, and uploaded bytes 79.9%. Graph write throughput during
checkpoint stress rose 44.3%; p50 and p99 improved 19.1% and 19.7%.

SQL checkpoint stress regressed: CPU rose 14.8%, peak memory 13.8%, S3 requests
66.7%, and uploaded bytes 29.7%. Local SQL reads improved, but KV write throughput
fell 67.4%. This is the clearest remaining performance issue.

HTTP 4xx responses from conditional object creation are included in
`s3_http_failures`; they are expected CAS/dedup outcomes in this test, not failed
client operations. All 50,400 checkpoint-stress client requests succeeded.

## CPU, memory, and microbenchmarks

Without checkpoint stress, candidate CPU seconds / peak memory changed as follows:

| Mode | CPU change | Peak memory change |
|---|---:|---:|
| SQL async | +1.8% | +25.1% |
| SQL before-ack | -16.5% | -28.1% |
| Graph async | -0.4% | -15.4% |
| Graph before-ack | +3.7% | +3.9% |

QuePaxa microbenchmark medians changed from 24.18 ms to 24.04 ms for a healthy
three-peer proposal (-0.6%), from 17.57 ms to 19.93 ms with one peer down (+13.4%),
and from 1.05 us to 0.96 us for healthy read-index (-8.7%). Local read-index remained
allocation-free at about 10.6 ns.

Graph apply time was effectively flat (192.5 us to 190.7 us), but bytes/op rose
33.7% (265,979 to 355,598) and allocs/op rose 32.6% (344 to 456). The file-snapshot
optimization therefore solved checkpoint amplification without solving Graph apply
allocation pressure.

## Fault behavior

- One peer unavailable: all SQL, Graph, and KV benchmark requests succeeded in both
  durability modes. Main write throughput varied from -19.6% to +56.9%; the positive
  values reflect fewer active peers and localhost noise, not a scaling guarantee.
- Two peers unavailable: writes returned HTTP 503 as required.
- After quorum restoration: writes resumed, rebuilt peers converged, and both SQL and
  Graph recovered from shared object storage.
- Chaos samples: SQL quorum write 44.989 ms; Graph quorum write 49.175 ms.

## Follow-up priorities

1. Profile SQL checkpoint GET/HEAD/publication work and remove the measured request,
   CPU, and memory regression.
2. Reduce Graph apply allocations introduced by atomic idempotency receipts.
3. Re-run with open-loop load and longer samples before setting p99 performance gates;
   several closed-loop p99 values remain noisy.
4. A breaking archive-format deployment must use a fresh object prefix or explicitly
   discard old objects. Reusing the old `active` prefix correctly failed startup on
   the removed `manifest_hash` field during post-benchmark restore.
