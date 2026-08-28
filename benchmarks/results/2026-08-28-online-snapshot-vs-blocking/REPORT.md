# LatticeDB online snapshot qualification

Rhiza `77b9d33` and LatticeDB `8ef87c9` ran as three peers on local Dory K3s
with Go 1.27, arenas, and GreenTeaGC. The affected Graph/KV surface covered
healthy and one-peer-fault operation in async and before-ack modes, plus two
independent 1-second-checkpoint runs. All 109,800 measured HTTP requests
completed successfully.

## Result

LatticeDB now freezes a page generation briefly and preserves overwritten
pages in an on-disk COW sidecar. Rhiza starts SQLite and Graph snapshots at the
same applied index while holding the materializer lock, then releases the lock
before copying either database. The checkpoint is sealable only after both
copies finish. No `.snapshot-cow` file remained on any peer after the runs.

The local freeze benchmark changed only one metadata page before each snapshot.
Database size grew 60x while median begin/close time grew from 3.184 ms to
3.952 ms (24.1%, 0.768 ms), rather than scaling with full database size.

| LatticeDB size | freeze median |
|---:|---:|
| 1.07 MiB | 3.184 ms |
| 16.15 MiB | 3.571 ms |
| 64.41 MiB | 3.952 ms |

## One-second checkpoint comparison

The old blocking result has three samples per workload. The online result below
uses the median of six samples from two fresh object-store prefixes. Throughput
and latency changed together because every benchmark uses fixed request counts.

| workload | old ops/s | online ops/s | change | old p95 | online p95 | worst old/new |
|---|---:|---:|---:|---:|---:|---:|
| Graph local read | 5,785 | 6,802 | +17.6% | 8.770 ms | 6.839 ms | 80.725 / 33.775 ms |
| Graph linearizable read | 4,296 | 3,575 | -16.8% | 10.062 ms | 13.407 ms | 31.198 / 83.044 ms |
| Graph write | 502 | 744 | +48.3% | 58.351 ms | 51.924 ms | 66.002 / 88.823 ms |
| KV local read | 10,695 | 10,116 | -5.4% | 2.656 ms | 2.865 ms | 9.566 / 24.001 ms |
| KV linearizable read | 7,677 | 7,368 | -4.0% | 3.610 ms | 3.776 ms | 10.175 / 19.097 ms |
| KV write | 531 | 753 | +41.8% | 60.507 ms | 49.096 ms | 124.092 / 105.122 ms |

The checkpoint write path improved materially: Graph and KV write throughput
rose 48.3% and 41.8%, and the overall worst request fell 15.3%. Linearizable
Graph-read tails regressed in these short runs, so the snapshot change is not a
general read-path win; that workload needs a longer isolated run before making
a read-latency claim.

## Full affected-surface medians

| mode | failed peers | workload | ops/s | p50 | p95 | p99 | max |
|---|---:|---|---:|---:|---:|---:|---:|
| async | 0 | Graph local read | 5,975 | 1.792 | 6.757 | 23.867 | 33.309 |
| async | 0 | Graph linearizable read | 4,749 | 2.282 | 8.435 | 18.359 | 28.105 |
| async | 0 | Graph write | 875 | 10.841 | 50.862 | 53.857 | 73.671 |
| async | 1 | Graph local read | 5,863 | 1.715 | 6.892 | 13.850 | 35.578 |
| async | 1 | Graph linearizable read | 6,008 | 2.148 | 6.165 | 14.396 | 41.516 |
| async | 1 | Graph write | 905 | 13.163 | 31.084 | 36.500 | 96.530 |
| before-ack | 0 | Graph local read | 6,914 | 1.748 | 6.338 | 10.469 | 23.434 |
| before-ack | 0 | Graph linearizable read | 4,229 | 2.604 | 9.095 | 21.506 | 26.772 |
| before-ack | 0 | Graph write | 652 | 20.737 | 49.902 | 49.944 | 85.048 |
| before-ack | 1 | Graph local read | 7,361 | 1.668 | 6.300 | 9.010 | 38.036 |
| before-ack | 1 | Graph linearizable read | 4,893 | 2.152 | 8.472 | 14.190 | 18.779 |
| before-ack | 1 | Graph write | 450 | 27.431 | 60.884 | 64.926 | 192.758 |

Values are milliseconds except ops/s. KV rows and every individual run remain
in `summary.csv` and `raw/`.

## Resource and object-store cost

Across the two checkpoint runs, weighted CPU was 0.429 cores versus 0.463 in
the blocking baseline (-7.5%). Maximum current/peak memory was 58.61/64.69 MiB
versus 56.44/62.03 MiB (+3.8%/+4.3%), consistent with the bitmap and COW
working set.

Per checkpoint profile, object-store totals averaged 141 uploads, 257 gets,
296 heads, and 694.5 HTTP requests. The blocking baseline used 136 uploads,
234 gets, 281 heads, and 651 requests. Bytes averaged 4.68 MB uploaded and
9.02 MB downloaded versus 4.22 MB and 8.89 MB. Run-to-run variance is high,
so these counts are a cost observation, not evidence that COW intrinsically
adds remote calls.

There were no object-store 5xx responses and no client request errors. The two
fresh-prefix checkpoint runs recorded 2 and 1 `http_4xx_unexpected` events
during initial local-read capture; pod logs showed normal publisher conflicts
and archive-head movement. This remains an instrumentation/startup item rather
than being silently classified as clean.

## Verification

- LatticeDB unit, integration, shared-library, C API, and Go binding tests pass.
- Rhiza SQL and Graph test suites pass; focused Graph checkpoint tests pass
  under the race detector and ten repeated concurrent-writer runs.
- The production Graph image built from the committed Rhiza and LatticeDB refs.
- Dory one-peer Chaos Mesh runs completed in async and before-ack modes with no
  measured request failures.

Raw requests, cgroup samples, summaries, comparisons, object-store counters,
and exact environment metadata are stored beside this report.
