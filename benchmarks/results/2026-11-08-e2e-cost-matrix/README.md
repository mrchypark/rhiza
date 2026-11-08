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
passed all seven jobs against the exact candidate.

## GCS SQL Async: separate WAL and lease changes

[Run 42474927651](https://github.com/mrchypark/rhiza/actions/runs/42474927651)
tested the same base and candidate plus WAL-only and lease-only trees. It ran
eight blocks of four samples in rotating order on one runner. All 32 samples
passed the exact receipt and row checks for 1,000 writes with zero errors.
The [raw artifact](https://github.com/mrchypark/rhiza/actions/runs/42474927651/artifacts/13615127499)
contains exact trees, metrics, and logs. Its `samples_per_cell=3` is stale
metadata inherited from the full matrix; the isolation loop ran eight samples
per variant.

| Variant | Writes/s | p99 (ms) | Voter CPU (s) | Largest voter HWM (MiB) |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 1,432 | 85.93 | 0.735 | 96.71 |
| WAL only | 1,461 | 115.16 | 0.705 | 90.44 |
| Lease only | 1,370 | 84.53 | 0.730 | 97.41 |
| Both | 1,602 | 57.37 | 0.710 | 96.55 |

These are medians of the eight run-level observations per variant. Comparing
both changes against baseline *within each block*, the median paired change
was +5.82% p99 and +10.67% throughput. The paired result differs from the
unpaired column medians. Two blocks had p99 above 131 ms for every variant.
That shows variation shared by revisions, but does not identify runner CPU
contention, I/O stalls, or any particular Rhiza path as the cause.
The five slowest samples had p99 between 280 and 519 ms, but p50 remained
8.0–8.6 ms and total voter CPU 0.73–0.75 s. This was a tail event rather
than a comparable increase in median latency or measured voter CPU time.

All timed writes used zero GCS requests. Forced publication still used 21
requests without the lease change and 17 with it. The result does not justify
changing the Async ACK or WAL sync contract to improve p99.

The [instrumented repeat](https://github.com/mrchypark/rhiza/actions/runs/42475756318)
passed all 32 samples using the same comparison trees. Median p99 was
10.90 ms baseline, 11.37 ms WAL-only, 11.29 ms lease-only, and 11.48 ms
combined. The combined within-block median changes were +5.03% p99 and
−1.76% throughput. The earlier 280–519 ms tail did not recur. The six highest
p99 samples had approximately 8.3–9.6% host I/O pressure versus 5.6–6.5% for
the eight lowest, dividing PSI stall time by the reported client duration
(an approximation; the host window also covers launch and wait). This
supports investigating I/O wait, but it does not isolate Rhiza WAL `fsync`
or explain the larger earlier stalls. CPU steal was zero in all samples;
that does not exclude contention. The next discriminating measurement is
queue wait, flush scheduling and actual WAL sync duration on both revisions.

The Graph engine follow-up is tracked as
[LatticeDB Go #253](https://github.com/mrchypark/latticedb-go/issues/253).
It is not part of these measured revisions.

## LatticeDB Go v0.12.1

[LatticeDB Go #255](https://github.com/mrchypark/latticedb-go/pull/255) is
included in tag `v0.12.1` (`a3155939`). Successful commits reuse the exact
committed metadata view; errors and reopen still reload it from storage.
Rhiza compared `addbce0a` (v0.11.3) with `0567370d` (v0.12.1); only the root
Go module version and sums changed. Both [Versity S3](https://github.com/mrchypark/rhiza/actions/runs/42481741130)
and [Floci GCS](https://github.com/mrchypark/rhiza/actions/runs/42481740766)
passed all 36 samples (three per cell per revision, 1,000 writes at concurrency
16, zero errors). Raw artifacts: [S3](https://github.com/mrchypark/rhiza/actions/runs/42481741130/artifacts/13618280249),
[GCS](https://github.com/mrchypark/rhiza/actions/runs/42481740766/artifacts/13617738548).

| Provider | Workload | Mode | Writes/s | p99 (ms) | Voter CPU (s) | Largest voter HWM (MiB) |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| S3 | SQL | Async | 1745.80 → 1683.20 | 13.64 → 13.80 | 0.86 → 0.92 | 63.79 → 64.53 |
| S3 | SQL | BeforeAck | 608.89 → 602.10 | 31.91 → 33.90 | 1.48 → 1.51 | 65.21 → 66.16 |
| S3 | KV | Async | 1282.72 → 1192.02 | 18.94 → 21.34 | 1.50 → 1.79 | 59.21 → 60.93 |
| S3 | KV | BeforeAck | 568.78 → 562.73 | 37.91 → 37.42 | 2.03 → 2.06 | 60.26 → 60.85 |
| S3 | Graph | Async | 268.09 → 390.95 | 86.55 → 58.30 | 6.54 → 2.67 | 60.82 → 60.24 |
| S3 | Graph | BeforeAck | 207.45 → 278.64 | 105.72 → 67.33 | 7.06 → 3.26 | 61.65 → 60.79 |
| GCS | SQL | Async | 1681.29 → 1673.59 | 13.84 → 14.24 | 1.02 → 1.06 | 96.79 → 96.92 |
| GCS | SQL | BeforeAck | 503.07 → 511.44 | 48.81 → 50.89 | 2.54 → 2.50 | 105.97 → 106.75 |
| GCS | KV | Async | 1009.49 → 1022.53 | 25.36 → 25.86 | 2.63 → 2.33 | 98.31 → 98.33 |
| GCS | KV | BeforeAck | 380.52 → 398.51 | 56.77 → 56.70 | 4.10 → 3.85 | 103.99 → 104.47 |
| GCS | Graph | Async | 265.45 → 446.32 | 94.05 → 63.16 | 7.78 → 3.29 | 96.95 → 97.93 |
| GCS | Graph | BeforeAck | 178.10 → 284.76 | 127.80 → 72.43 | 9.52 → 4.70 | 114.34 → 106.59 |

Per-cell medians use the same CPU and memory scopes as above. Every sample
retained 1,071 BeforeAck attempts, zero timed Async attempts and 17 forced
Async publication attempts. The GCS Class A/B splits were unchanged:
252/819 (SQL BeforeAck), 254/817 (Graph or KV BeforeAck), and 6/11 (forced
publication). Graph improved on both backends; SQL/KV did not improve across
the board. These runs do not resolve the earlier GCS SQL Async tail anomaly.

The separate candidate-only Graph Apply profile recorded 1.31 ms/op,
174,901 B/op and 858 allocs/op. In a previous v0.11.3 run on another runner,
the same benchmark recorded 6.11 ms/op, 2,729,575 B/op and 49,047 allocs/op.
The old `LoadGraph` frame used 58.39% cumulative CPU; its replacement
`LoadGraphWithAppMetadata` used 0.82% in the new profile. These profile runs
were not paired on one runner; the E2E table is the controlled comparison.

## SQL and KV Async follow-up

[Same-revision Versity A/A](https://github.com/mrchypark/rhiza/actions/runs/42484194658)
used identical server code for both labels. All three KV Async candidate
labels nevertheless had worse p99 (28.35→30.80, 28.02→32.43 and
25.03→26.08 ms). This is a control for ordinary variation, not evidence
that the LatticeDB change caused or could never cause a KV regression.

[GCS stage diagnostics](https://github.com/mrchypark/rhiza/actions/runs/42484768841)
passed 36/36, with probe startup markers for all 54 voters in the 18 Async
samples. In one SQL sample, client p99 was 280.47 ms, the longest in-window
WAL `file.Sync` was 280.17 ms and group flush was 280.23 ms. Another had
140.63 ms p99 and a 139.57 ms WAL sync. Timed GCS requests were zero.
In the 280 ms sample, two voters' sync intervals overlapped by 274.23 ms;
all three voters used the same runner disk. This can stall a quorum, not
just one slow voter. The measured `file.Sync` wall time can also include
time the process spent descheduled during the call.
These wall timings localize the stall to the sync call in those samples;
they do not prove why the underlying I/O stalled or attribute every
outlier request to a particular voter. The earlier [cancelled probe](https://github.com/mrchypark/rhiza/actions/runs/42484194715)
did not forward the flag through `env -i` and is not diagnostic evidence.
The runner now requires an exact startup marker on all three ready voters.
The [separate block mount A/B](https://github.com/mrchypark/rhiza/actions/runs/42485565210)
stopped before measuring: `/tmp` resolved to `/dev/sda1` and no distinct
`/mnt` block mount was present. This leaves the disk-placement mitigation
unqualified. No same-disk fallback, tmpfs, or weaker sync was used.

[Versity](https://github.com/mrchypark/rhiza/actions/runs/42484340256) and
[GCS](https://github.com/mrchypark/rhiza/actions/runs/42484340029) each passed
36/36 comparing `36a867ee` with `73b18aec` (probe disabled). All samples
preserved 1,071 BeforeAck requests, zero timed Async requests and 17
forced Async publication requests; GCS Class A/B splits were unchanged.
The same-run KV batch benchmark at 16 commands fell from 1.82 to 1.23 ms
and 2,099 to 1,677 allocations per batch. Single-command allocation rose
from 195 to 197, so `a2872f82` removes method-value indirection from that
path. SQL skipped redundant maintenance tokenization, reducing the
single-command microbenchmark from 248 to 237 allocations with essentially
unchanged median time. E2E latency and throughput were mixed; in particular
the uninstrumented GCS SQL Async comparison still included 112 and 414 ms
candidate tails. The optimizations do not remove the durable sync barrier.

[CI at the dependency update](https://github.com/mrchypark/rhiza/actions/runs/42481741693)
failed waiting for repeated certified progress in the Graph Close fixture.
The corrected fixture passed that step in
[CI 42484226962](https://github.com/mrchypark/rhiza/actions/runs/42484226962).
[CI 42484340027](https://github.com/mrchypark/rhiza/actions/runs/42484340027)
failed separately on a truncated PUT body in the Graph UNKNOWN fixture.
The test now reports the wire path and cancellation state and accepts
only a proven, scoped cancellation. The original failed runs remain linked.
The [final functional CI](https://github.com/mrchypark/rhiza/actions/runs/42485467166)
passed all seven jobs at `d324be61`, including both repaired race steps,
the full Go race suite, vulnerability scan, and all three Floci providers.

The [final Versity retest](https://github.com/mrchypark/rhiza/actions/runs/42485117382)
and [final GCS retest](https://github.com/mrchypark/rhiza/actions/runs/42485117409)
compared `36a867ee` with `a2872f82` and passed 36/36 each. The medians below
retain the earlier CPU and memory scopes.

| Provider | Workload | Mode | Writes/s | p99 (ms) | Voter CPU (s) | Largest voter HWM (MiB) |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| S3 | SQL | Async | 1647.24 → 1657.84 | 14.17 → 13.91 | 1.08 → 1.06 | 64.11 → 63.70 |
| S3 | SQL | BeforeAck | 558.72 → 561.81 | 34.18 → 32.71 | 1.85 → 1.84 | 66.34 → 65.94 |
| S3 | KV | Async | 1002.24 → 1183.62 | 29.60 → 24.68 | 2.41 → 1.96 | 62.76 → 61.21 |
| S3 | KV | BeforeAck | 487.46 → 516.58 | 42.28 → 39.62 | 2.88 → 2.65 | 61.13 → 61.20 |
| S3 | GRAPH | Async | 458.14 → 452.40 | 60.50 → 61.12 | 3.24 → 3.27 | 59.54 → 60.47 |
| S3 | GRAPH | BeforeAck | 323.53 → 323.63 | 60.42 → 61.97 | 3.85 → 3.86 | 60.87 → 61.05 |
| GCS | SQL | Async | 1742.58 → 1493.24 | 29.06 → 93.00 | 0.66 → 0.65 | 97.09 → 96.54 |
| GCS | SQL | BeforeAck | 577.49 → 516.37 | 88.67 → 84.50 | 1.72 → 1.95 | 106.25 → 115.84 |
| GCS | KV | Async | 1343.89 → 1515.56 | 45.19 → 64.27 | 1.24 → 1.17 | 100.02 → 97.42 |
| GCS | KV | BeforeAck | 483.58 → 421.80 | 95.99 → 154.35 | 2.62 → 2.43 | 110.18 → 104.17 |
| GCS | GRAPH | Async | 548.64 → 564.84 | 131.61 → 57.12 | 2.08 → 2.14 | 97.02 → 100.37 |
| GCS | GRAPH | BeforeAck | 324.69 → 359.91 | 118.67 → 82.48 | 3.37 → 3.33 | 106.36 → 105.90 |

BeforeAck medians remained at 1,071 attempts for every workload, timed Async
stayed at zero, and forced Async publication remained at 17 in every sample.
One GCS KV BeforeAck candidate sample made 1,088 attempts (258 Class A and
830 Class B). All other GCS samples retained Class A/B counts of 252/819
(SQL BeforeAck), 254/817 (Graph/KV BeforeAck), and 6/11 (forced publication).
The Graph Apply profile still used the optimized
metadata path (1.17 ms/op, 180,206 B/op, 862 allocs/op, not a paired microbenchmark).

The same-run KV microbenchmark regained 195 allocs for one command and
reduced 16-command median time from 2.88 to 1.94 ms, allocations from
2,099 to 1,640. SQL single-command allocation remained 248→237, and 128
commands used about 100 KiB less per batch; the elapsed improvement was small.
Versity KV Async improved in this run, while GCS SQL Async p99 did not.
These are emulator and hosted-runner measurements, not live cloud latency.

## SQLite rowid guard and GCS setup control

`b901c6c8` combines resetting last-insert ROWID and installing the pre-update
guard into one native connection callback. [Versity](https://github.com/mrchypark/rhiza/actions/runs/42491833393)
and [GCS](https://github.com/mrchypark/rhiza/actions/runs/42491833382) passed
36/36 each against `a44b3191`. The SQL one-command microbenchmark was
0.290 → 0.288 ms, still 237 allocations; the 128-command median was
6.56 → 6.59 ms. This reduces one callback per statement, but the measurement
does not establish a latency improvement.
[Functional CI](https://github.com/mrchypark/rhiza/actions/runs/42491851471)
passed all seven jobs at `b901c6c8`.

[GCS prebuild A/B](https://github.com/mrchypark/rhiza/actions/runs/42492194681)
compared eight alternating SQL Async pairs at `cd1d9d25`. Both arms used
byte-identical binaries from the same source checkout, fresh voters and
emulator state, the same sync barrier, and the same receipt/publication
checks. All 16 samples passed, with zero timed GCS requests and 17 forced
publication requests each. Median p99 was 13.15 ms with per-sample builds
and 13.71 ms with prebuilt binaries; median throughput was 1,725 → 1,719
writes/s. No p99 exceeded 16.24 ms. The earlier tail did not recur, so this
does not identify repeated builds as its cause or show a mitigation.

## Reuse encoded SQL RETURNING receipts

`8b50cb1c` reuses the RSQL bytes already used for retention accounting,
instead of holding decoded rows and encoding them again during flush. The
format and policy limits are unchanged. Tests cover a slot-gap rollback,
releasing pending buffers, and replaying the same slot and result.

[GCS E2E](https://github.com/mrchypark/rhiza/actions/runs/42492457219)
passed 36/36. The first [Versity workflow](https://github.com/mrchypark/rhiza/actions/runs/42492457099)
also completed its 36 E2E samples, but the new isolated receipt benchmark
failed setup: a four-slot window is below the supported minimum of 1,024.
`6d1c25b9` fixes the fixture, not the runtime, and uses 200 fixed operations
per run to stay below the 256 MiB retained-result limit.

The [corrected Versity workflow](https://github.com/mrchypark/rhiza/actions/runs/42493215236)
passed 36/36 E2E samples and both receipt workloads. The same fixture was
overlaid on both revisions, with three runs per cell on one runner.

| Result payload | Median ms/op | Allocated B/op | Allocations/op |
| --- | ---: | ---: | ---: |
| 4 KiB | 0.282 → 0.296 | 30,104 → 24,970 | 255 → 232 |
| 600 KB | 3.706 → 3.643 | 2,437,451 → 1,830,982 | 258 → 235 |

The 600 KB workload allocates 24.9% fewer bytes. The small, mixed elapsed
changes are not a general latency improvement. The 72 final S3/GCS E2E
samples all used 1,071 BeforeAck attempts, zero timed Async attempts, and
17 forced Async publication attempts. Graph throughput and p99 were mixed
in the hosted runs; the Graph engine version and metadata path are unchanged.
[Functional CI](https://github.com/mrchypark/rhiza/actions/runs/42492457051)
passed all seven jobs at the production-code revision `8b50cb1c`; only
the benchmark fixture and workflow changed in `6d1c25b9`.

## SQL Async: voter data placement and disk pressure

At `0274f38c8c9ce41fef33d86fd64f0a620eadeb63`, a Kubernetes Job compared
three voters sharing one block volume with the same three voters using one
volume each. Both arms used the same host and the same pool of three
independent block volumes, rotating the shared volume and voter assignments
and alternating arm order. This moved each voter's entire data directory, not
just its WAL. All 12 samples across six pairs verified 1,000 SQL writes each.
Timed writes used zero object-store HTTP attempts and forced publication used
17 in every sample, against the Floci GCS emulator.

| Per-arm median | Shared | Split |
| --- | ---: | ---: |
| p99 (ms) | 422.54 | 326.86 |
| Successful writes/s | 444.24 | 371.39 |
| Voter CPU sum (s) | 2.045 | 2.140 |
| Largest individual voter lifetime RSS HWM (MiB) | 97.81 | 96.25 |

The median of paired changes was **−6.20% p99** and **−5.13% throughput**;
split lowered p99 in 3/6 pairs and increased throughput in 3/6. Shared/split
p99 pairs in execution-pair order (ms): `48.71/451.52`, `489.14/575.67`,
`438.16/645.47`, `731.92/202.21`, `29.99/20.97`, `406.93/114.42`. There is
no consistent placement win. In all six pairs, the higher-p99 arm had a
larger fraction of its write window with overlapping slow fsyncs on at least
two voters. The probe used a 2 ms threshold; individual sync wall times
reached 332.95 ms shared and 373.31 ms split. These are wall durations, not
device service latencies. The host and potentially shared upstream storage
were not isolated, so the association does not establish the cause of the spikes.

Earlier, [hosted pressure-control run 42494789160](https://github.com/mrchypark/rhiza/actions/runs/42494789160)
passed 16/16 samples on the same code revision in both arms. The median p99
was 24.13 ms with the pressure writer on the voter filesystem and 23.66 ms
with the writer on tmpfs. This control did not reproduce the large spikes.

The placement benchmark container exited 0 and the Pod reached `Succeeded`;
Floci exited 143 during sidecar shutdown. The terminal snapshot did not
capture a Job `Complete` condition. The test namespace, volumes, and backing
disks were removed. Neither experiment changes durability or verifies live GCS.

### Block-device and scheduler diagnostic

A follow-up at `ee735418ba3f9c9cbcde9845e50fe2dba66a01de` repeated six
shared/split pairs with Linux block-device and voter-thread scheduler counters.
All 12 samples exited 0, verified 1,000 SQL writes each, and reached the
certified archive cut. The higher-p99 arm had a higher maximum per-device
`write_ms / write_ios` in all six pairs. In the slowest pair, p99 was
**988.49 ms shared / 943.91 ms split** and this block-write average was
**3.45 / 2.54 ms**. The kernel-reported mean flush time per operation stayed
between 0.060 and 0.072 ms across measured devices and samples.

In that slowest pair, the longest in-window WAL fsync call took **406.87 ms
shared / 298.27 ms split** in process wall time. The kernel flush average is
not a tail measure or the same interval as a WAL syscall. A syscall can
include filesystem work, waiting for writeback, and scheduling delays; these
samples cannot allocate that time among them. Summed voter run-queue wait
rose with p99 in raw totals, but after dividing by timed client duration,
its Pearson correlation with p99 across the 12 samples was only **0.04**.
The scheduler sample uses threads visible at both endpoints and has partial
coverage in some voters. The WAL probe logs only calls above its 2 ms
threshold; device counters are window aggregates, not request-level traces.
This diagnostic establishes an association, not a cause or a storage-tier
recommendation. It did not change the WAL or the durability policy.

### Storage-tier comparison

At `7df2e84cd2a0a9c0e01d4bf9e7273071cf48d577`, one isolated Pod alternated
standard/premium and premium/standard order across six pairs. Both arms put
all three voters on one mount at a time. The two 10 GiB standard (`pd-balanced`)
volumes rotated A/B/B/A/A/B against one 10 GiB premium (`pd-ssd`) volume. All
12 samples verified 1,000 SQL Async writes each, with zero object-store HTTP
attempts during writes and 17 in the forced-publication phase.

Median p99 was **33.33 ms standard / 61.69 ms premium**. Premium had higher
p99 in 5/6 pairs (median paired change **+55.38%**) and lower median throughput
(**898.50 / 770.85 writes/s**; median paired change **−12.72%**). The worst
observed p99 was **554.05 / 142.08 ms**, so premium lowered the maximum in
this sample without improving latency consistently.

In zero-based pair 2, premium p99 reached 142.08 ms although the longest
in-window WAL fsync call was only 6.0 ms (standard: 24.26 ms p99 and 4.8 ms
fsync). The longest group flush in that premium sample was 86.95 ms, but the
probe does not bind those events to the same client request. In pair 5,
standard p99 reached 554.05 ms, with a 336.6 ms maximum
fsync and 2.44 ms mean device write time per operation. Long WAL syncs may
contribute to some spikes, but do not account for every observed spike.

The evidence supports neither switching storage tiers nor changing the WAL
path. A next diagnostic, if separately authorized, should correlate request
and commit timing with fsync and filesystem or block events and track scheduling
of the same voter threads, while retaining quorum sync and receipt checks.
