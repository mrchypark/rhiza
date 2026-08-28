# Archive refresh performance review

## Verdict

Commit `4a2260db77295758ceab1e628799c92d7565f9e1` fixes the only reproduced
foreground regression found after the WAL/archive work: a successful
before-ACK publication verified the stable head and then downloaded the extent
that the same process had just encoded, uploaded, and hash-validated.

The optimized path keeps the required `Attributes -> GET head -> Attributes`
stable-generation proof and the CAS token from that proof. When the stable head
is exactly the expected head, it installs the already validated local extent
references. If another writer advanced the head, it falls back to the complete
remote chain load. Sync, trim, and cleanup use the same path.

On paired SQL before-ACK runs, SQL write recovered from 257 to 375 ops/s
(+45.6%) and p95 fell from 120.7 to 56.2 ms (-53.5%). KV write recovered from
177 to 384 ops/s (+117.6%) and p95 fell from 159.4 to 56.5 ms (-64.5%). The
normal publication cost is now two PUTs, one GET, and two HEADs: five object
requests instead of seven, without weakening the stable-head invariant.

The broad regression claimed in `2026-08-28-wal-archive-91c3a86` was based on
non-paired local runs and is not supported by the controlled checks. Core
proposal medians are equal or faster than `b98b877`, direct server queries are
faster, and paired async writes do not reproduce the claimed 50% loss. The host
had persistent macOS Storage Management activity at roughly 40-60% host CPU,
so absolute Dory throughput is retained as evidence, not treated as a release
SLO.

## Reproduced regression and fix

| paired SQL workload | `b98b877` | pre-fix | optimized | pre-fix p95 | optimized p95 |
|---|---:|---:|---:|---:|---:|
| SQL write | 368 ops/s | 257 ops/s | 375 ops/s | 120.7 ms | 56.2 ms |
| KV write | 372 ops/s | 177 ops/s | 384 ops/s | 159.4 ms | 56.5 ms |

The pre-fix SQL write sample used 44 GET, 66 HEAD, and 154 total HTTP calls.
The optimized sample used 18 GET, 36 HEAD, and 90 total calls. Downloaded bytes
fell from 109,454 to 2,232 because the locally produced extent is no longer
downloaded after every successful publish.

## Regression review

- QuePaxa three-peer proposal median improved from about 27.9 ms to 25.1 ms.
- QuePaxa proposal with one peer down improved from about 22.1 ms to 20.3 ms.
- `ReadIndex` rose from about 1.1 us/400 B/6 allocs to 2.1 us/496 B/8 allocs.
  This is the intentional cost of canceling slower quorum RPCs. It is retained
  because it bounds work and connection pressure after quorum completion.
- Direct one-node server query median improved from about 4.1 to 2.9 us for
  local reads and from about 8.2 to 2.8 us for linearizable reads. Current
  allocation is about 850 B and 21 allocations per query.
- Checkpoint creation moved from about 718 to 751 us (+4.7%), within run noise
  and below the provisional 10% gate. No checkpoint code was changed.
- SQL and Graph memory stayed within the prior envelopes. No capacity or queue
  limit was raised to hide overload symptoms.

Raw microbenchmarks are stored beside this report. Paired Dory inputs are in
`paired/`; `paired-summary.json` is their normalized view.

## Three-peer Dory results

Values are medians of three repetitions, except Graph async healthy, which has
six repetitions because the first and second runs were strongly bimodal. Every
canonical operation succeeded.

| profile | local read | linearizable read | write | write p95 |
|---|---:|---:|---:|---:|
| SQL async healthy | 7,002 ops/s | 4,762 ops/s | 612 ops/s | 58.9 ms |
| SQL async, one peer down | 2,194 ops/s | 4,842 ops/s | 600 ops/s | 63.4 ms |
| SQL before-ACK healthy | 5,970 ops/s | 5,811 ops/s | 375 ops/s | 56.2 ms |
| SQL before-ACK, one peer down | 6,502 ops/s | 4,804 ops/s | 374 ops/s | 71.0 ms |
| SQL checkpoint every 1s | 7,498 ops/s | 5,746 ops/s | 433 ops/s | 87.8 ms |
| Graph async healthy | 3,882 ops/s | 2,238 ops/s | 616 ops/s | 192.8 ms |
| Graph async, one peer down | 3,519 ops/s | 2,083 ops/s | 538 ops/s | 56.9 ms |
| Graph before-ACK healthy | 4,704 ops/s | 3,807 ops/s | 411 ops/s | 63.0 ms |
| Graph before-ACK, one peer down | 4,366 ops/s | 1,529 ops/s | 308 ops/s | 81.8 ms |
| Graph checkpoint every 1s | 2,914 ops/s | 2,529 ops/s | 337 ops/s | 93.9 ms |

Graph async healthy ranged from 171 to 721 write ops/s across the two back-to-
back profiles while making zero object-store calls. Its p95 is therefore host-
noise dominated; the paired core and server microbenchmarks are the regression
authority for foreground code.

## CPU and memory

CPU is summed cgroup CPU divided by profile wall time. Memory is the maximum
single-pod observation.

| profile | average cores | current MiB/pod | peak MiB/pod |
|---|---:|---:|---:|
| Graph async healthy | 0.356 | 58.8 | 64.3 |
| Graph async, one peer down | 0.364 | 61.4 | 67.6 |
| Graph before-ACK healthy | 0.429 | 58.3 | 65.2 |
| Graph before-ACK, one peer down | 0.357 | 56.5 | 62.5 |
| Graph checkpoint every 1s | 0.399 | 56.1 | 62.3 |
| SQL async healthy | 0.303 | 41.4 | 47.8 |
| SQL async, one peer down | 0.273 | 42.5 | 50.9 |
| SQL before-ACK healthy | 0.277 | 42.3 | 48.5 |
| SQL before-ACK, one peer down | 0.203 | 40.8 | 47.0 |
| SQL checkpoint every 1s | 0.276 | 41.7 | 48.1 |

## Object-store API cost

Counts cover all canonical workload samples in each profile. Healthy async
profiles were kept below the publication timer and made no object-store calls.

| profile | PUT | GET | HEAD | LIST | DELETE | total HTTP | uploaded | downloaded |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Graph before-ACK healthy | 256 | 128 | 256 | 0 | 0 | 640 | 587,903 B | 15,872 B |
| Graph before-ACK, one peer down | 262 | 233 | 265 | 0 | 0 | 760 | 697,336 B | 453,966 B |
| Graph checkpoint every 1s | 198 | 368 | 386 | 0 | 0 | 951 | 6,935,232 B | 15,296,293 B |
| SQL before-ACK healthy | 220 | 110 | 220 | 0 | 0 | 550 | 541,455 B | 13,640 B |
| SQL before-ACK, one peer down | 252 | 127 | 253 | 0 | 0 | 632 | 890,794 B | 273,590 B |
| SQL checkpoint every 1s | 98 | 192 | 215 | 0 | 0 | 505 | 1,601,410 B | 2,667,004 B |

One-fault and checkpoint profiles include expected conditional-claim conflicts
and recovery reads. There were no LIST/DELETE calls in the measured write path,
no application errors in the canonical profiles, and no unexpected HTTP 5xx
responses from object storage.

## Provisional regression gates

These are local development gates for this Dory hardware, not public service
SLOs.

1. Canonical three-peer, one-peer-down, and 1s-checkpoint profiles must have
   zero application errors. Two peers down must fail writes and linearizable
   reads closed while local reads remain available.
2. A successful uncontended before-ACK publication must use at most two PUT,
   one GET, and two HEAD calls, with no remote extent re-download.
3. SQL and Graph one-peer-down write throughput must retain at least 70% of the
   matching healthy mode, and write p95 must stay below twice healthy p95.
4. Three-peer and one-peer-down core proposal medians may not regress more than
   10% against a paired baseline.
5. Direct server query median must remain below 4 us, 900 B/op, and 21
   allocations/op on this host.
6. `ReadIndex` must remain below 3 us and 9 allocations/op; the cancellation
   overhead is accepted unless network-level evidence shows it is harmful.
7. Checkpoint creation median may not regress more than 10%. SQL peak memory
   must remain below 55 MiB/pod and Graph below 75 MiB/pod in this suite.
8. Any run with host CPU interference or more than 15% back-to-back spread must
   be paired or repeated; it cannot alone reject or qualify a candidate.

## Transient failure retained for review

The first SQL 1s-checkpoint profile produced 55 HTTP 503 responses in one KV
write repetition. The exact raw profile and pod evidence are retained under
`diagnostics/`. An immediate identical full rerun and a separate 600-request,
concurrency-16 KV probe both completed with zero errors. The canonical result
uses the successful rerun, but the first sample is not discarded. A future
reproduction with stable response cause is required before changing bounded
admission or checkpoint protocol behavior.

## Verification

- `go test ./...`
- `go vet ./...`
- `GOEXPERIMENT=arenas,greenteagc go test ./...`
- `go test -race ./...`
- `go test -race -tags graph ./...`
- SQL and Graph container builds, including all graph-tag tests
- Ten Dory profiles: SQL/Graph, async/before-ACK, healthy/one-peer-down, plus
  1s-checkpoint profiles

