# Publisher-fenced archive and LatticeDB final qualification

The final SQL and Graph images ran as three peers on local Dory K3s with Go
1.27, arenas, and GreenTeaGC. All 126,000 measured HTTP operations completed
without errors. Values below are medians of three runs at concurrency 16.

## Primary engine results

| profile | mode | failed peers | workload | ops/s | p50 ms | p95 ms | p99 ms | max ms |
|---|---|---:|---|---:|---:|---:|---:|---:|
| SQL | async | 0 | local read | 8,845 | 1.368 | 3.068 | 7.105 | 15.665 |
| SQL | async | 0 | linearizable read | 5,173 | 2.251 | 7.224 | 10.597 | 20.445 |
| SQL | async | 0 | write | 598 | 12.910 | 51.660 | 71.948 | 100.426 |
| SQL | async | 1 | local read | 7,189 | 1.843 | 3.978 | 11.537 | 41.565 |
| SQL | async | 1 | linearizable read | 5,634 | 2.207 | 7.563 | 11.040 | 30.465 |
| SQL | async | 1 | write | 479 | 24.609 | 60.485 | 62.744 | 126.007 |
| SQL | before-ack | 0 | local read | 9,375 | 1.394 | 2.788 | 10.144 | 23.687 |
| SQL | before-ack | 0 | linearizable read | 6,343 | 2.189 | 4.251 | 7.934 | 47.143 |
| SQL | before-ack | 0 | write | 463 | 26.183 | 58.489 | 66.716 | 118.403 |
| SQL | before-ack | 1 | local read | 10,747 | 1.256 | 2.526 | 7.092 | 21.862 |
| SQL | before-ack | 1 | linearizable read | 6,683 | 1.693 | 3.906 | 10.045 | 31.992 |
| SQL | before-ack | 1 | write | 641 | 18.297 | 58.303 | 58.407 | 78.901 |
| Graph | async | 0 | local read | 4,290 | 1.987 | 10.185 | 17.605 | 54.345 |
| Graph | async | 0 | linearizable read | 4,410 | 2.486 | 8.688 | 28.430 | 55.190 |
| Graph | async | 0 | write | 795 | 10.885 | 34.206 | 45.646 | 73.187 |
| Graph | async | 1 | local read | 4,993 | 2.278 | 7.649 | 10.564 | 26.928 |
| Graph | async | 1 | linearizable read | 5,021 | 2.264 | 7.975 | 10.887 | 39.473 |
| Graph | async | 1 | write | 530 | 22.985 | 50.663 | 58.191 | 69.960 |
| Graph | before-ack | 0 | local read | 6,888 | 1.710 | 7.043 | 11.394 | 28.207 |
| Graph | before-ack | 0 | linearizable read | 4,473 | 2.274 | 9.631 | 11.746 | 30.180 |
| Graph | before-ack | 0 | write | 553 | 22.568 | 43.869 | 50.503 | 58.876 |
| Graph | before-ack | 1 | local read | 4,364 | 2.018 | 10.623 | 16.322 | 35.157 |
| Graph | before-ack | 1 | linearizable read | 4,832 | 2.188 | 8.280 | 13.389 | 59.853 |
| Graph | before-ack | 1 | write | 583 | 25.133 | 39.895 | 43.431 | 118.275 |

KV batching removed the earlier admission failure. Healthy async KV writes were
750 ops/s in the SQL image and 881 ops/s in the Graph image; their local reads
were 10,493 and 9,425 ops/s. No final KV sample contained an error.

## Checkpoint, CPU, and memory

| profile | average cores | current MiB/pod max | peak MiB/pod max |
|---|---:|---:|---:|
| SQL async | 0.235 | 42.6 | 49.2 |
| SQL before-ack | 0.228 | 40.6 | 47.7 |
| SQL async, one failed | 0.192 | 47.9 | 55.9 |
| SQL checkpoint every 1s | 0.265 | 40.2 | 47.6 |
| Graph async | 0.454 | 56.5 | 62.9 |
| Graph before-ack | 0.399 | 59.3 | 65.2 |
| Graph async, one failed | 0.287 | 54.7 | 61.8 |
| Graph checkpoint every 1s | 0.463 | 56.4 | 62.0 |

At an intentionally aggressive one-second checkpoint interval, SQL writes were
840 ops/s and Graph writes 502 ops/s. Graph's worst request was 124 ms. SQL had
one 1.043 s linearizable KV-read outlier while its median p95 was 4.47 ms; all
requests still succeeded. This is the remaining checkpoint-pause tail to watch.

## Object-store API cost

Counts cover all 18 workload runs in each profile (600 writes plus reads).

| profile | PUT | GET | HEAD | LIST | DELETE | total HTTP | uploaded | downloaded |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| SQL async healthy | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| SQL before-ack healthy | 218 | 109 | 327 | 0 | 0 | 654 | 675,176 B | 26,387 B |
| SQL before-ack, one failed | 226 | 114 | 339 | 0 | 0 | 679 | 994,715 B | 304,481 B |
| SQL checkpoint every 1s | 118 | 228 | 284 | 0 | 0 | 630 | 2,266,151 B | 3,272,802 B |
| Graph async healthy | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| Graph before-ack healthy | 242 | 121 | 363 | 0 | 0 | 726 | 750,071 B | 29,294 B |
| Graph before-ack, one failed | 226 | 122 | 343 | 0 | 0 | 691 | 1,041,414 B | 567,368 B |
| Graph checkpoint every 1s | 136 | 234 | 281 | 0 | 0 | 651 | 4,222,677 B | 8,893,019 B |

Conditional-write conflicts were expected publisher-election losses: 17 for
SQL and 16 for Graph in the one-second checkpoint profiles, and one in each
before-ack fault profile. There were no unexpected 5xx responses, SDK retries,
LISTs, or DELETEs. Stable archive reads add HEAD calls deliberately so a reader
never installs a mixed object generation.

## Comparison and chaos result

Against the preceding LatticeDB run, healthy Graph async local and linearizable
reads improved 24.4% and 54.2%; async writes regressed 8.0%. Healthy before-ack
Graph local reads, linearizable reads, and writes improved 108.5%, 80.6%, and
32.0%. With one failed peer, before-ack Graph writes improved 123.4% and their
p95 fell 51.0%. Graph async one-fault writes regressed 14.0%, although p95
improved 9.6%.

Against the 2026-08-27 SQL run, before-ack SQL writes improved 85.9% when healthy
and 64.1% with one failed peer. Async SQL writes regressed 19.8% and 39.0%, with
worse p95; that is the clearest remaining throughput optimization target. Read
and KV paths improved broadly. Exact row-by-row deltas are in `comparison.json`.

Final Chaos Mesh E2E passed for both images. One failed peer preserved writes
(SQL 9.18 ms, Graph 15.27 ms samples), converged, and rebuilt. Two failed peers
returned HTTP 503 without committing. After recovery, writes resumed and a full
three-pod restart restored the certified state from shared object storage.

Raw measurements are in `raw/`, cgroup evidence in `resources/`, normalized
tables in `summary.json`/`summary.csv`, and environment/digests in
`environment.json`. Three sibling `diagnostic-*` directories retain the KV
admission and five-second QUIC-lock failures that led to the final fixes.
