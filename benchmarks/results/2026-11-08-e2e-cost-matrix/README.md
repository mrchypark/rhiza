# E2E write-cost matrix: Versity S3

[Performance run 42460056899](https://github.com/mrchypark/rhiza/actions/runs/42460056899)
passed all 36 samples. The [raw artifact](https://github.com/mrchypark/rhiza/actions/runs/42460056899/artifacts/13609333426)
contains per-sample JSON, process logs, exit statuses, and runner metadata.

- Base: `fd323dbb37ad86c614eb0983c406adbccd16acba`.
- Candidate and shared harness: `f390eee01d1745913814365b1ec74cb44d827df5`.
- Hosted Ubuntu 24.04 (`ubuntu24-20261101.360.1`), Go 1.27.3, Linux/amd64.
  Same HTTP client, three healthy QUIC voters, disposable VersityGW S3,
  1,000 successful writes per sample at concurrency 16. Each cell has three
  interleaved samples per revision; no request errors were observed.

## Timed writes

Each cell is the median of three run-level measurements, base → candidate.
CPU seconds sum user and system time across the three voters during writes.
Memory is the largest individual voter's lifetime RSS high-water mark at the
end of each sample, not a simultaneous cluster peak or a write-only peak.
Neither resource measure includes the HTTP client or Versity process.

| Workload | Mode | Successful writes/s | p99 (ms) | Voter CPU (s) | Largest voter HWM (MiB) |
| --- | --- | ---: | ---: | ---: | ---: |
| SQL | Async | 1,367.79 → 1,155.84 | 84.570 → 61.489 | 0.71 → 0.81 | 64.95 → 64.46 |
| SQL | BeforeAck | 365.29 → 485.87 | 197.750 → 219.297 | 1.40 → 1.37 | 66.23 → 66.01 |
| Graph | Async | 236.49 → 278.33 | 409.394 → 215.342 | 5.17 → 5.16 | 60.73 → 60.21 |
| Graph | BeforeAck | 215.36 → 197.59 | 169.142 → 189.361 | 6.02 → 6.05 | 61.81 → 61.21 |
| KV | Async | 753.34 → 1,366.49 | 182.850 → 18.505 | 1.45 → 1.31 | 60.38 → 59.61 |
| KV | BeforeAck | 411.67 → 450.44 | 226.768 → 107.177 | 2.15 → 2.16 | 60.77 → 60.31 |

The sample count is small and latency varies materially: for example, base SQL
BeforeAck p99 spans 53.205–420.805 ms. These observations do not establish a
speedup or regression. Graph used the most voter CPU in both modes for these
workloads, making it the first profiling target. The table does not identify
which Graph code accounts for that CPU.

## Object-store requests and publication

The client HTTP attempt counts were identical between revisions in every cell.
Counts are per sample, in the order **total (PUT / GET / HEAD)**. No DELETE or
other HTTP methods were recorded in these windows.

- BeforeAck, through the timed writes: SQL **1,512 (252 / 378 / 882)**;
  Graph and KV **1,512 (252 / 379 / 881)** each. Final verification added no
  requests in this run.
- Async: zero requests through timed writes and final verification. The separate
  forced archive call made **24 (4 / 7 / 13)** requests for every workload on
  both revisions. Zero foreground requests does not mean zero publication cost.

| Async workload | Median forced-call time (ms), base → candidate | Certified cut / archive tip after the call, base → candidate |
| --- | ---: | --- |
| SQL | 19.432 → 20.367 | 65–85 → 68–91 |
| Graph | 21.095 → 20.775 | 65–74 → 70–74 |
| KV | 21.146 → 24.479 | 82–90 → 72–80 |

Every Async sample started with archive tip zero. The archive tip after the
forced call equaled that sample's pre-call certified and applied tip. These are
slot numbers, not write counts. This verifies the publication cut observed by
the voter, not an independent restore or scheduled background publication cost.

There was no observed request-count reduction. These are S3 transport attempts,
not server-side operation counts, GCS Class A/B counts, or billing evidence.
The run does not qualify live cloud behavior, recovery, or fault handling.

## Optimized publication release: Versity S3

[Performance run 42461845943](https://github.com/mrchypark/rhiza/actions/runs/42461845943)
compared base `fd323dbb37ad86c614eb0983c406adbccd16acba` with candidate and
shared harness `cac733ab5c5c932819f7a4286a0dbd8f0f23ad0c`. All 36 samples
passed (1,000 writes each, concurrency 16, no request errors). The
[artifact](https://github.com/mrchypark/rhiza/actions/runs/42461845943/artifacts/13610038015)
contains the exact revisions, runner metadata, raw sample JSON, logs, and
exit statuses. The candidate includes both the WAL staging-copy removal and
the confirmed-version publication lock release. Compare revisions within this
run; do not compare timing values across separate hosted runners.

| Workload | Mode | Successful writes/s | p99 (ms) | Voter CPU (s) | Largest voter HWM (MiB) |
| --- | --- | ---: | ---: | ---: | ---: |
| SQL | Async | 1,717.62 → 1,653.12 | 13.403 → 14.077 | 1.02 → 1.14 | 63.71 → 63.55 |
| SQL | BeforeAck | 500.31 → 542.38 | 38.125 → 34.836 | 1.95 → 1.85 | 65.62 → 65.72 |
| Graph | Async | 277.85 → 279.54 | 93.949 → 88.326 | 7.24 → 7.76 | 61.72 → 61.81 |
| Graph | BeforeAck | 205.91 → 210.93 | 112.247 → 105.971 | 8.31 → 8.28 | 61.57 → 61.79 |
| KV | Async | 1,062.08 → 1,021.58 | 22.583 → 23.752 | 2.17 → 2.48 | 60.53 → 61.64 |
| KV | BeforeAck | 419.09 → 457.24 | 46.620 → 42.313 | 3.20 → 3.01 | 61.21 → 60.82 |

Values are medians of three samples per revision, with the same resource scopes
as above. BeforeAck throughput rose 2.4–9.1% and p99 fell 5.6–9.2% in these
samples. Async SQL and KV throughput fell about 3.8%, p99 rose about 5%, and
Async voter CPU rose in all three workloads. The largest individual lifetime
HWM changed by −0.39 to +1.11 MiB across the six cells. Candidate SQL Async p99
spanned 13.398–27.588 ms versus 13.042–14.973 ms at base; three samples are
not enough to establish a general speedup, regression, or unchanged memory.

- BeforeAck timed writes: median HTTP attempts per 1,000 writes fell
  **1,512 → 1,323 (−12.5%)** for each workload. In **PUT / GET / HEAD** order,
  SQL was `252 / 378 / 882 → 252 / 315 / 756`; Graph and KV were
  `252 / 379 / 881 → 252 / 316 / 755`. The candidate Graph range was
  1,323–1,344; one sample had `256 / 321 / 767`. SQL and KV counts were
  identical across their respective three candidate samples. Final verification
  added no attempts.
- Async timed writes and final verification: zero attempts on both revisions.
  The separate forced publication fell **24 → 21** in every paired sample
  (`4 / 7 / 13 → 4 / 6 / 11`). Every sample started with archive tip zero
  and reached its pre-call certified tip; base and candidate cut ranges were
  SQL 71–92 → 71–106, Graph 76–87 → 91–128, and KV 95–111 → 156–169.
  These are certified slots, not counts of writes or proof of independent
  restore. The forced publish is not a measure of scheduled background cost.

The decrease is in S3 client transport attempts, consistent with removing two
Attributes calls and one GET from each successful release. It is not a
server-side operation count or a billing result.

The artifact also contains a **separate candidate-only Graph Apply
microbenchmark**: 6.105 ms/op, 2,888,309 B/op, and 51,790 allocs/op. In its CPU
profile, LatticeDB `commitPages` accounted for 67.6% cumulative and
`PageGraph.LoadGraph` for 59.9% cumulative (nested, not additive). The largest
flat allocation sites were bbolt `Cursor.search` (24.9%), LatticeDB
`AppMetadata.Set` (15.0%), and `bytes.Clone` (12.8%). This points to the Graph
commit path for investigation, not to an E2E allocation result or a measured
Graph engine improvement.

## Optimized publication release: Floci GCS

[Performance run 42462242395](https://github.com/mrchypark/rhiza/actions/runs/42462242395)
passed all 36 samples. Base was `fd323dbb37ad86c614eb0983c406adbccd16acba`;
candidate and shared harness were `50aa8c2eb1da33bb78b926563e1942bc875813ad`.
The [artifact](https://github.com/mrchypark/rhiza/actions/runs/42462242395/artifacts/13610168488)
records the exact pinned Floci GCS 0.11.0 image digest, runner environment,
native classifications, client counters, and exit statuses. Each sample used
three healthy voters, the Go GCS SDK, and 1,000 writes at concurrency 16. All
writes succeeded. This is Performance CI, separate from full functional CI.

| Workload | Mode | Successful writes/s | p99 (ms) | Voter CPU (s) | Largest voter HWM (MiB) |
| --- | --- | ---: | ---: | ---: | ---: |
| SQL | Async | 1,715.24 → 1,739.73 | 12.801 → 14.381 | 0.94 → 1.06 | 96.05 → 99.32 |
| SQL | BeforeAck | 425.44 → 479.52 | 50.768 → 48.495 | 2.81 → 2.57 | 106.44 → 106.47 |
| Graph | Async | 267.38 → 273.50 | 92.518 → 100.340 | 7.76 → 7.96 | 97.05 → 96.34 |
| Graph | BeforeAck | 173.38 → 174.12 | 125.730 → 127.447 | 9.58 → 9.52 | 117.61 → 106.95 |
| KV | Async | 991.44 → 1,015.56 | 24.074 → 27.054 | 2.40 → 2.39 | 96.24 → 99.01 |
| KV | BeforeAck | 362.18 → 368.00 | 60.534 → 61.518 | 3.94 → 4.03 | 104.58 → 104.46 |

Values are medians of three samples per revision. CPU sums user and system
time for the three voters during writes; memory is the largest individual
voter lifetime RSS high-water mark, not a simultaneous cluster peak. Neither
includes the client or emulator. Graph remained the most CPU-intensive of these
workloads. Three samples do not establish a general timing or memory improvement.

| Phase | Workload | Native HTTP attempts | Class A attempts | Class B attempts |
| --- | --- | ---: | ---: | ---: |
| BeforeAck writes | SQL | 1,512 → 1,323 | 252 → 252 | 1,260 → 1,071 |
| BeforeAck writes | Graph or KV, each | 1,512 → 1,323 | 254 → 254 | 1,258 → 1,069 |
| Async writes | Each | 0 → 0 | 0 → 0 | 0 → 0 |
| Async forced publication | Each | 24 → 21 | 6 → 6 | 18 → 15 |
| Final verification | Each, both modes | 0 → 0 | 0 → 0 | 0 → 0 |

Counts are per sample, base → candidate, identical across repetitions. In all
90 measured phases, native storage responses matched client transport attempts
exactly. No unknown, malformed, or resumable-transfer/probe requests were
reported. Graph/KV BeforeAck writes and all Async forced-publication phases
each recorded three error-status responses on both revisions, consistent with
expected missing-object probes: no unexpected HTTP failures, condition
conflicts, or transport failures were recorded. The artifact does not retain
individual status codes. Total attempts fell 12.5% in BeforeAck writes and
forced publication, entirely from Class B; Class A did not decrease.

Median forced-publication times (ms) were SQL 37.495 → 35.890, Graph 42.808 →
32.442, and KV 37.207 → 38.346. Every Async sample advanced from archive tip
zero through its pre-call certified/applied cut. This verifies the voter’s
reported cut, not independent restore or scheduled publication overhead. The
class counts classify emulator requests, including expected failed attempts;
they are not [GCS billing measurements](https://cloud.google.com/storage/pricing#operations-pricing).

Earlier failures were preserved:

- [Run 42461340417](https://github.com/mrchypark/rhiza/actions/runs/42461340417)
  stopped at SQL/Async/base/sample 1: the publication parser rejected 19 of 24
  records as malformed. The next harness changed the access-log path field
  and sanitized query and fragment suffixes before classification.
  [Failure artifact](https://github.com/mrchypark/rhiza/actions/runs/42461340417/artifacts/13610166035).
- [Run 42461844080](https://github.com/mrchypark/rhiza/actions/runs/42461844080)
  stopped at Graph/Async/base/sample 1: 25 native responses versus 24 client
  attempts, one unknown path, no malformed records. The pinned image's automatic
  health check was independently identified as an extra request source; the
  final harness disabled it and retained explicit startup readiness checks and
  the strict classification gate.
  [Failure artifact](https://github.com/mrchypark/rhiza/actions/runs/42461844080/artifacts/13609857997).

The separate [functional CI run 42461847925](https://github.com/mrchypark/rhiza/actions/runs/42461847925)
failed its Go and container jobs on an obsolete physical-call assertion:
it expected 14 Attributes and 6 GETs, and observed 12 and 5. HEAD remained
at 4 Attributes and 1 GET, with zero block GETs. Commit `e8c6e881` checks
both the lower total and those unchanged safety-path counters. See the
[corrected functional CI run 42462502583](https://github.com/mrchypark/rhiza/actions/runs/42462502583).

## WAL buffer reuse and publication lease version checks

Baseline: `a845a33f5fd8bfcfbd82475459330e95161776de`. Candidate and harness:
`fa870b132e6b93957892b1907b21d1d7e84a7082`. The
[S3 run](https://github.com/mrchypark/rhiza/actions/runs/42473460816) and
[GCS run](https://github.com/mrchypark/rhiza/actions/runs/42473462715) each passed
36 samples: 1,000 writes at concurrency 16, with exact receipt and state checks.
The candidate combines bounded per-WAL encoding scratch and lease confirmation
by the acquired version; a changed version still uses the full stable read.

| Provider | Workload | Mode | Writes/s | p99 (ms) | Voter CPU (s) | Largest voter HWM (MiB) |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| S3 | SQL | Async | 1708.74 → 1703.15 | 14.23 → 12.82 | 1.07 → 1.03 | 63.90 → 63.65 |
| S3 | SQL | BeforeAck | 525.19 → 591.93 | 35.09 → 32.13 | 1.88 → 1.72 | 65.48 → 65.34 |
| S3 | KV | Async | 1029.65 → 1018.70 | 25.27 → 24.66 | 2.57 → 2.71 | 61.07 → 61.28 |
| S3 | KV | BeforeAck | 444.10 → 499.50 | 43.12 → 41.21 | 3.10 → 2.94 | 60.94 → 60.34 |
| S3 | GRAPH | Async | 275.93 → 273.98 | 93.67 → 86.74 | 7.61 → 7.39 | 61.05 → 60.23 |
| S3 | GRAPH | BeforeAck | 203.88 → 217.06 | 105.33 → 103.90 | 8.33 → 8.67 | 62.11 → 61.80 |
| GCS | SQL | Async | 1588.27 → 1676.13 | 52.38 → 64.88 | 0.70 → 0.68 | 96.80 → 95.70 |
| GCS | SQL | BeforeAck | 431.09 → 361.58 | 135.48 → 234.54 | 1.96 → 1.80 | 106.48 → 105.66 |
| GCS | KV | Async | 1063.92 → 1224.74 | 120.91 → 68.21 | 1.31 → 1.26 | 96.73 → 96.57 |
| GCS | KV | BeforeAck | 388.71 → 355.75 | 122.14 → 192.37 | 2.68 → 2.83 | 103.75 → 103.81 |
| GCS | GRAPH | Async | 220.79 → 262.22 | 310.41 → 216.50 | 5.20 → 5.03 | 96.41 → 97.95 |
| GCS | GRAPH | BeforeAck | 145.51 → 170.60 | 347.40 → 334.22 | 6.76 → 6.81 | 110.04 → 107.10 |

Values are medians of three samples per revision. CPU and HWM use the scopes
defined above. S3 BeforeAck throughput improved in all three cells, but GCS
SQL and KV BeforeAck throughput decreased in all three paired samples per
workload. Async and process resource results were mixed. These are not
evidence of a universal throughput, CPU, or RSS improvement.

BeforeAck write HTTP attempts fell **1,323 → 1,071** (−19.0%) per 1,000 writes
in every paired sample. Forced Async publication fell **21 → 17** (−19.0%);
Async timed writes still made no object requests. GCS observed and transport
counts matched in every classified phase. Class A was unchanged (SQL 252,
Graph/KV 254, forced publication 6). Class B was SQL **1,071 → 819**,
Graph/KV **1,069 → 817**, and forced publication **15 → 11**. These are
emulator operations, not cloud billing.

The separate WAL append-and-sync benchmark with 256-byte payloads changed from
320 B/op and 1 alloc/op to 0 B/op and 0 allocs/op in all three runs. The
reusable buffer holds at most 64 KiB per WAL; lower allocation traffic does not
imply lower process RSS.

The candidate-only Graph Apply profile still attributes 68.1% cumulative CPU
to LatticeDB `commitPages` and 58.9% to its nested `PageGraph.LoadGraph`.
The LatticeDB engine, durability settings, and transaction boundaries were
not changed.

The independent [GCS repeat](https://github.com/mrchypark/rhiza/actions/runs/42474088475)
used the same baseline, candidate, and harness and passed 36/36. It reproduced
the HTTP and class counts exactly, including transport/server agreement.
The first run's SQL/KV BeforeAck throughput drops did not reproduce: repeat
medians improved 447.82 → 483.23 and 380.10 → 407.22 writes/s, respectively.
Their p99 medians fell 49.53 → 44.29 and 60.49 → 55.84 ms. GCS SQL Async p99
increased in both runs (52.38 → 64.88 and 13.43 → 14.50 ms). These paired
samples establish request-count savings, but not a repeatable latency or
resource gain across all cells; the Async latency cause remains unresolved.

Local full recovery and QLog tests and focused race tests passed. The initial
combined local command passed QLog but reached its 55-second timeout in
QuePaxa. Focused QuePaxa recording, recovery, and quorum tests passed; the
[full functional CI](https://github.com/mrchypark/rhiza/actions/runs/42473464663)
ran against the exact candidate.
