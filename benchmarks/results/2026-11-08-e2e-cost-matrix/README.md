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
