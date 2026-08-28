# Concurrent WAL compaction and binary archive benchmark

Commit `91c3a86eebd0a332429604ee7e0cc31885203efa` was measured against
`b98b87759f042d0ff42075a4fffa11e83e74bf58` on a three-peer local Dory K3s
cluster. The suite issued 126,000 HTTP operations at concurrency 16, using
three repetitions per workload. Every operation succeeded.

The correctness result is positive, but the performance result is not: most
write profiles and linearizable reads regressed materially. The only clear
write improvement is Graph async with one failed peer (+9.5% throughput and
-20.6% p95). This candidate should not be called performance-qualified yet.

## Primary engine results

Values are medians of three repetitions. `max` is the worst request across the
three runs.

| profile | workload | ops/s | p50 ms | p95 ms | p99 ms | max ms |
|---|---|---:|---:|---:|---:|---:|
| Graph async healthy | local read | 4,789 | 2.395 | 8.136 | 12.722 | 19.346 |
| Graph async healthy | linearizable read | 1,708 | 7.170 | 20.656 | 34.022 | 157.607 |
| Graph async healthy | write | 385 | 31.958 | 79.170 | 82.808 | 180.054 |
| Graph async, one failed | local read | 3,989 | 2.946 | 9.092 | 14.408 | 30.682 |
| Graph async, one failed | linearizable read | 3,047 | 3.717 | 11.896 | 22.259 | 57.311 |
| Graph async, one failed | write | 581 | 22.971 | 40.245 | 48.093 | 67.557 |
| Graph before-ack healthy | local read | 2,186 | 5.043 | 19.713 | 29.806 | 65.036 |
| Graph before-ack healthy | linearizable read | 1,861 | 5.686 | 18.057 | 54.671 | 80.857 |
| Graph before-ack healthy | write | 239 | 48.833 | 149.488 | 174.483 | 320.243 |
| Graph before-ack, one failed | local read | 3,924 | 2.694 | 11.374 | 22.432 | 34.555 |
| Graph before-ack, one failed | linearizable read | 1,796 | 6.401 | 18.462 | 65.119 | 122.796 |
| Graph before-ack, one failed | write | 275 | 46.924 | 84.569 | 102.819 | 137.232 |
| Graph checkpoint every 1s | local read | 1,519 | 6.454 | 28.714 | 86.285 | 96.240 |
| Graph checkpoint every 1s | linearizable read | 2,119 | 5.719 | 20.328 | 27.654 | 128.529 |
| Graph checkpoint every 1s | write | 465 | 26.091 | 73.409 | 103.864 | 555.252 |
| SQL async healthy | local read | 5,056 | 1.858 | 7.297 | 34.198 | 55.532 |
| SQL async healthy | linearizable read | 2,576 | 2.999 | 18.943 | 35.595 | 103.364 |
| SQL async healthy | write | 540 | 22.248 | 49.831 | 63.165 | 132.666 |
| SQL async, one failed | local read | 2,838 | 4.029 | 10.039 | 26.681 | 38.641 |
| SQL async, one failed | linearizable read | 2,002 | 5.586 | 16.863 | 22.984 | 191.111 |
| SQL async, one failed | write | 257 | 40.480 | 175.462 | 208.913 | 844.870 |
| SQL before-ack healthy | local read | 7,491 | 1.862 | 3.565 | 10.923 | 16.320 |
| SQL before-ack healthy | linearizable read | 2,801 | 3.428 | 15.032 | 34.547 | 81.433 |
| SQL before-ack healthy | write | 212 | 49.427 | 151.921 | 153.757 | 244.100 |
| SQL before-ack, one failed | local read | 2,943 | 3.843 | 14.674 | 32.269 | 177.417 |
| SQL before-ack, one failed | linearizable read | 1,122 | 7.256 | 33.419 | 82.664 | 309.848 |
| SQL before-ack, one failed | write | 157 | 89.080 | 167.650 | 186.137 | 1,050.509 |
| SQL checkpoint every 1s | local read | 3,765 | 3.485 | 8.779 | 20.563 | 81.272 |
| SQL checkpoint every 1s | linearizable read | 2,051 | 5.444 | 14.253 | 33.059 | 52.051 |
| SQL checkpoint every 1s | write | 260 | 51.223 | 92.652 | 129.037 | 134.745 |

## Write comparison with the baseline

| profile | old ops/s | new ops/s | throughput | old p95 | new p95 | p95 |
|---|---:|---:|---:|---:|---:|---:|
| Graph async healthy | 795 | 385 | -51.6% | 34.21 | 79.17 | +131.5% |
| Graph async, one failed | 530 | 581 | +9.5% | 50.66 | 40.24 | -20.6% |
| Graph before-ack healthy | 553 | 239 | -56.9% | 43.87 | 149.49 | +240.8% |
| Graph before-ack, one failed | 583 | 275 | -52.9% | 39.90 | 84.57 | +112.0% |
| Graph checkpoint every 1s | 502 | 465 | -7.3% | 58.35 | 73.41 | +25.8% |
| SQL async healthy | 598 | 540 | -9.6% | 51.66 | 49.83 | -3.5% |
| SQL async, one failed | 480 | 257 | -46.5% | 60.48 | 175.46 | +190.1% |
| SQL before-ack healthy | 462 | 212 | -54.1% | 58.49 | 151.92 | +159.7% |
| SQL before-ack, one failed | 640 | 157 | -75.5% | 58.30 | 167.65 | +187.6% |
| SQL checkpoint every 1s | 841 | 260 | -69.1% | 48.38 | 92.65 | +91.5% |

Graph async healthy was rerun with a ten-minute async publication interval.
The first run lasted beyond the normal one-minute interval and mixed 18
object-store requests into the supposedly quiet profile. The isolated rerun
made zero object-store calls and improved write throughput from 145 to 385
ops/s, but it remains 51.6% below the baseline. This confirms a real foreground
regression in addition to the initial timer contamination.

## CPU and memory

CPU is the summed cgroup CPU delta divided by wall time. Memory is the maximum
single-pod observation, matching the baseline method.

| profile | average cores | current MiB/pod max | peak MiB/pod max |
|---|---:|---:|---:|
| Graph async healthy | 0.411 | 56.5 | 62.9 |
| Graph async, one failed | 0.337 | 59.5 | 67.4 |
| Graph before-ack healthy | 0.395 | 58.8 | 65.0 |
| Graph before-ack, one failed | 0.346 | 55.4 | 61.6 |
| Graph checkpoint every 1s | 0.427 | 57.1 | 63.6 |
| SQL async healthy | 0.294 | 40.8 | 48.6 |
| SQL async, one failed | 0.327 | 44.4 | 51.5 |
| SQL before-ack healthy | 0.297 | 39.9 | 46.4 |
| SQL before-ack, one failed | 0.270 | 40.4 | 46.7 |
| SQL checkpoint every 1s | 0.296 | 39.9 | 46.1 |

Memory is effectively flat relative to the baseline. CPU is mixed: Graph
healthy is slightly lower than 0.454 cores, while SQL healthy rises from 0.235
to 0.294 cores. Because throughput fell, CPU per completed operation is worse
for most profiles even when absolute core use is flat.

## Object-store API cost

Counts cover all 18 workload samples in each profile (12,600 operations).

| profile | PUT | GET | HEAD | total HTTP | uploaded | downloaded | conditional failures |
|---|---:|---:|---:|---:|---:|---:|---:|
| Graph async healthy | 0 | 0 | 0 | 0 | 0 B | 0 B | 0 |
| Graph async, one failed | 0 | 0 | 0 | 0 | 0 B | 0 B | 0 |
| Graph before-ack healthy | 250 | 250 | 375 | 875 | 632,029 B | 632,029 B | 0 |
| Graph before-ack, one failed | 262 | 403 | 397 | 1,062 | 994,682 B | 1,586,922 B | 1 |
| Graph checkpoint every 1s | 171 | 351 | 368 | 890 | 5,018,801 B | 12,216,679 B | 23 |
| SQL async healthy | 0 | 0 | 0 | 0 | 0 B | 0 B | 0 |
| SQL async, one failed | 0 | 0 | 0 | 0 | 0 B | 0 B | 0 |
| SQL before-ack healthy | 240 | 240 | 360 | 840 | 601,111 B | 601,111 B | 0 |
| SQL before-ack, one failed | 290 | 394 | 439 | 1,123 | 1,007,630 B | 1,495,756 B | 1 |
| SQL checkpoint every 1s | 199 | 383 | 442 | 1,024 | 3,971,002 B | 5,436,759 B | 20 |

The binary archive reduced healthy before-ack upload bytes versus the baseline:
SQL fell 11.0% (675,176 to 601,111 B), and Graph fell 15.7% (750,071 to
632,029 B). The new post-CAS stable reread deliberately adds a GET and HEAD for
published groups, so healthy total calls rose 28.4% for SQL and 20.5% for
Graph. Download bytes also rise because readers now verify and install the
stable published generation rather than trusting local publication state.
There were no LIST or DELETE calls and no unexpected 5xx responses.

## Interpretation and next check

Concurrent WAL compaction is not a plausible cause for the broad healthy
regression because it is active on checkpoint paths. Binary archive encoding
also cannot explain SQL async healthy, which made no object-store calls. The
comparison spans seven commits, including online snapshots, durable Graph
streams, recovery fencing, bounded proposal paths, batching/admission changes,
and archive validation. The next smallest useful experiment is an intermediate
commit bisect using only SQL async healthy, Graph async healthy, and both
before-ack healthy write profiles, followed by CPU profiles on the first bad
commit. That isolates the foreground consensus/batching cost before changing
code.

The host had concurrent macOS Storage Management activity during the suite and
individual repetitions varied substantially. Absolute numbers should therefore
be treated as local-machine measurements, not release SLOs. The paired baseline
delta is still strong enough to reject the candidate as performance-qualified,
especially for before-ack and one-fault SQL.

Raw measurements are in `raw/`, cgroup evidence in `resources/`, normalized
results in `summary.json` and `summary.csv`, exact API totals in
`object-store-totals.json`, and every row-level baseline delta in
`comparison.json`.
