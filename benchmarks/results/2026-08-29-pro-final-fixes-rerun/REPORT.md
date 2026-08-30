# Pro final-fix validation

## Verdict

The complete Dory 3-peer matrix served 100,800 requests across SQL and Graph, async and before-ACK durability, and healthy and one-peer-fault states with zero client errors. Checkpoint-at-1s validation served another 50,400 requests with zero client errors. Deleting an `emptyDir` peer removed all local state; both SQL and Graph returned ready in 5 seconds, matched applied and consensus tips, and served a linearizable query after object-storage-backed recovery.

## Healthy latency and throughput

These are sequential confirmation medians, used to avoid contention between simultaneous SQL and Graph runs. Each workload has three repetitions at concurrency 16; reads use 1,000 requests and writes use 100 requests.

| Profile | local read p50 | consensus read p50 | write p50 | local read ops/s | consensus read ops/s | write ops/s |
|---|---:|---:|---:|---:|---:|---:|
| SQL async | 1.206 ms | 1.634 ms | 14.462 ms | 11,442 | 8,041 | 676 |
| SQL before-ack | 1.298 ms | 1.662 ms | 17.756 ms | 10,316 | 6,621 | 630 |
| Graph async | 1.540 ms | 2.138 ms | 10.890 ms | 8,032 | 4,581 | 880 |
| Graph before-ack | 1.642 ms | 2.035 ms | 27.900 ms | 7,260 | 5,290 | 457 |

Against `2026-08-29-final-rereview`, every primary healthy p50 improved: SQL async local/consensus/write by 13.7%/17.0%/3.8%, SQL before-ACK by 3.1%/13.6%/24.4%, Graph async by 12.0%/24.0%/6.3%, and Graph before-ACK by 11.5%/25.0%/5.0%.

## One-peer-fault latency

Complete-matrix medians are retained as the failure-state evidence.

| Profile | local read p50 | consensus read p50 | write p50 |
|---|---:|---:|---:|
| SQL async | 1.429 ms | 1.723 ms | 11.177 ms |
| SQL before-ack | 1.178 ms | 1.657 ms | 18.508 ms |
| Graph async | 2.309 ms | 2.446 ms | 14.580 ms |
| Graph before-ack | 1.548 ms | 2.173 ms | 21.472 ms |

The parallel full matrix recorded one 5-second SQL async fault-transition probe; an isolated repeat returned quorum immediately and served 12,600 requests with zero errors. All other full-matrix transitions were immediate.

## CPU and memory

Values are aggregate CPU cores and mean per-pod cgroup memory from the complete matrix. One-fault rows observe two pods.

| Profile | State | CPU cores | memory current | memory peak |
|---|---|---:|---:|---:|
| SQL async | healthy / one fault | 0.263 / 0.170 | 41.7 / 37.1 MiB | 49.0 / 44.5 MiB |
| SQL before-ack | healthy / one fault | 0.250 / 0.175 | 45.3 / 37.6 MiB | 52.3 / 44.6 MiB |
| Graph async | healthy / one fault | 0.399 / 0.332 | 53.1 / 51.4 MiB | 60.0 / 57.9 MiB |
| Graph before-ack | healthy / one fault | 0.372 / 0.340 | 50.3 / 49.5 MiB | 56.6 / 55.4 MiB |

## Object-store calls

Exact totals cover all 18 workload samples per configuration.

| Profile | State | HTTP calls | PUT | GET | HEAD | LIST | uploaded | downloaded |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| SQL async | healthy / one fault | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| SQL before-ack | healthy / one fault | 455 / 480 | 182 / 192 | 91 / 96 | 182 / 192 | 0 / 0 | 652,113 / 537,207 B | 11,284 / 11,904 B |
| Graph async | healthy / one fault | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Graph before-ack | healthy / one fault | 560 / 530 | 224 / 212 | 112 / 106 | 224 / 212 | 0 / 0 | 550,877 / 609,906 B | 13,888 / 13,144 B |
| SQL async, checkpoint 1s | healthy / one fault | 832 / 494 | 155 / 102 | 301 / 168 | 366 / 217 | 10 / 7 | — | — |
| Graph async, checkpoint 1s | healthy / one fault | 659 / 624 | 131 / 132 | 233 / 220 | 286 / 264 | 9 / 8 | — | — |

The first parallel Graph checkpoint sample observed three unexpected 4xx responses while workload requests still succeeded. A dedicated clean-state repeat served 12,600 requests with zero unexpected 4xx, HTTP 5xx, transport, or S3 HTTP failures, so the event was not reproducible; the raw sample remains preserved rather than hidden.

## Review-fix evidence

- Notification delivery is outside the materializer write lock, globally and per-subscriber bounded, and drops before a full-queue payload copy. The allocation and subscriber-bound regressions passed 20 consecutive runs.
- QUIC 0-RTT has a shared read-only allowlist on client and server. `Record`, `Propose`, `Learned`, `StageValue`, and `PrepareCheckpoint` require handshake completion.
- Default and Graph suites, focused race suites, full `go test ./...`, Graph-tag tests, and `go vet ./...` passed.

## Evidence

- `raw/`, `summary.json`, `summary.csv`: complete 3-peer matrix
- `confirmation/`, `confirmation-summary.json`: sequential healthy comparison
- `fault-confirmation/`, `fault-confirmation-summary.json`: isolated SQL one-fault repeat
- `checkpoint-1s/`, `checkpoint-1s-summary.json`, `checkpoint-1s-object-store-totals.json`: checkpoint/archive stress
- `checkpoint-probe/`: Graph unexpected-4xx reproduction attempt
- `live-recovery/`: SQL and Graph emptyDir recovery timing and queries
- `resources/`, `resources.json`: cgroup and pod evidence
- `object-store-totals.json`: API method and byte totals
- `environment.json`: toolchain, images, cluster, and base commit

Recovery certificate signing/key rotation and peer mTLS remain intentionally excluded by user decision. These are short single-host Dory/MinIO measurements, not WAN or production-capacity claims.
