# LatticeDB v0.2.1 current-state qualification

## Verdict

The current candidate completed every repository Go benchmark and the full
12-profile Dory matrix. The primary integration pass served 151,200/151,200
requests successfully. Its independent resource pass served 151,195/151,200;
one SQL checkpoint-at-1s, one-peer-fault write sample returned four HTTP 503s
and one transport error. A clean repeat of that complete profile served
25,200/25,200 requests successfully across its primary and resource passes, so
the client failure was not reproduced.

Graph checkpoint-at-1s did reproduce a separate background object-store issue:
the primary matrix recorded three S3 transport failures and the clean healthy
repeat recorded fourteen. Foreground requests remained successful, with no
unexpected HTTP 4xx or HTTP 5xx in either run. This candidate therefore passes
the normal and one-peer-fault request matrix, but the reproducible background
S3 transport failures under aggressive Graph checkpointing remain an open
qualification warning.

## Primary latency and throughput

Each row is the median of three sequential samples at concurrency 16. Reads use
1,000 requests per sample and writes use 100. These are single-host qualification
numbers, not capacity claims.

| Profile | State | local read p50 | linearizable read p50 | write p50 | local read ops/s | linearizable read ops/s | write ops/s |
|---|---|---:|---:|---:|---:|---:|---:|
| SQL async | healthy | 8.449 ms | 10.631 ms | 182.403 ms | 1,355 | 1,172 | 63 |
| SQL async | one fault | 3.227 ms | 9.275 ms | 50.810 ms | 4,421 | 1,378 | 272 |
| SQL before-ack | healthy | 2.051 ms | 5.123 ms | 40.564 ms | 6,629 | 2,041 | 340 |
| SQL before-ack | one fault | 2.406 ms | 5.855 ms | 41.869 ms | 5,785 | 2,370 | 337 |
| Graph async | healthy | 4.195 ms | 4.587 ms | 51.212 ms | 2,429 | 3,098 | 234 |
| Graph async | one fault | 6.039 ms | 6.673 ms | 46.334 ms | 1,961 | 1,722 | 316 |
| Graph before-ack | healthy | 7.200 ms | 16.457 ms | 159.950 ms | 1,385 | 767 | 69 |
| Graph before-ack | one fault | 7.939 ms | 10.817 ms | 79.503 ms | 1,637 | 984 | 126 |

The counter-intuitive cases where a fault or before-ack row is faster reflect
substantial host variance during this sequential single-machine run. Raw first,
second, and third samples are retained; no cross-run performance regression
claim should be made from these numbers alone.

## Checkpoint-at-1s stress

| Profile | State | local read p50 | linearizable read p50 | write p50 | client errors | S3 transport failures |
|---|---|---:|---:|---:|---:|---:|
| SQL async | healthy | 4.262 ms | 5.455 ms | 74.503 ms | 0 | 0 |
| SQL async | one fault | 2.148 ms | 7.905 ms | 42.210 ms | 0 | 0 |
| Graph async | healthy | 6.193 ms | 11.892 ms | 127.435 ms | 0 | 3 |
| Graph async | one fault | 6.727 ms | 8.393 ms | 94.596 ms | 0 | 0 |

The Graph healthy confirmation again produced transport failures (14 across
25,200 primary and resource-pass requests), confirming that the signal is not a
one-off. Conditional publish conflicts are reported separately and are expected
when three voters race to publish checkpoints; the primary matrix observed 100
such conflicts. The three primary and fourteen confirmation transport failures
are not conditional conflicts.

## Local Go benchmark medians

| Benchmark | Median |
|---|---:|
| WAL scan scratch | 0.822 ms/op |
| Graph apply | 0.695 ms/op |
| Checkpoint files | 13.048 ms/op |
| Graph snapshot freeze, 1 MiB input (1.335 MiB DB) | 2.279 ms/op |
| Graph snapshot freeze, 16 MiB input (21.33 MiB DB) | 11.066 ms/op |
| Graph snapshot freeze, 64 MiB input (85.34 MiB DB) | 66.456 ms/op |
| QuePaxa propose, three peers | 27.011 ms/op |
| QuePaxa propose, one peer down | 26.941 ms/op |
| QuePaxa read index, three peers | 8.725 us/op |
| QuePaxa read index, one peer down | 9.619 us/op |
| QuePaxa local read index | 57.45 ns/op |
| HTTP server local query | 25.681 us/op |
| HTTP server linearizable query | 13.860 us/op |
| Object-store replica catch-up | 4.213 ms/op, 3 GET + 3 HEAD/op |
| Learner catch-up | 5.698 ms/op, 0 object-store calls/op |

Allocation counts and all five samples are in `go/`. Snapshot and replica
benchmarks use fixed operation counts because their untimed setup makes Go's
automatic duration calibration disproportionately expensive.

## Resource and fault observations

The resource pass records aggregate CPU usage divided by wall time and mean
per-pod cgroup memory. Primary profiles ranged from 0.262 to 1.113 aggregate CPU
cores, 32.3 to 46.2 MiB current memory per observed pod, and 38.4 to 53.6 MiB
peak memory per pod. Healthy profiles observe three pods; fault profiles observe
the two surviving pods.

Post-fault quorum probes completed in 102–1,020 ms. Full recovery after removing
Chaos Mesh isolation took 3.0–22.8 s. Exact values and object-store deltas are in
`resources/` and `resources.json`.

## Evidence

- `environment.json`: exact commit, dirty-tree fingerprints, image digest,
  toolchain, host, and matrix.
- `go/`: all repository Go benchmark samples.
- `raw/`, `summary.json`, `summary.csv`: 216 primary samples and medians.
- `request-totals.json`, `object-store-totals.json`: request and object-store
  counters by profile.
- `resources/`, `resources.json`, `resource-request-totals.json`: independent
  resource-pass requests, cgroup snapshots, and fault timing.
- `confirmation/`: the two isolated repeats, including their raw and resource
  samples.
- `integration-run.log`: console stream from the complete primary matrix.

The run also exposed and fixed an E2E manifest drift: current voters require
distinct membership tokens, while the qualification manifests supplied only an
admin token. The manifests now provide three distinct local-only voter tokens,
and a regression test prevents admin-token reuse.
