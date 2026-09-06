# Before-ack publication comparison

Baseline: `3b5a5a07aaf83b5ed279f75b920395bda7179577` (v0.12.0).
Measured candidate: the earlier working-tree changes that reuse encoded archive
bytes and recheck durability before returning cached mutation receipts. Production
file hashes and exact environment are in `environment.json`. These are historical
local measurements, not measurements against the current PR base. The receipt
fix has since shipped in v0.12.1; this PR contains only archive encoding and
benchmark/CI changes on top of that release.

## Archive CPU/allocated-byte measurement

Five samples per revision, Apple M3, Go 1.27.0, GOMAXPROCS=8, 500ms/sample.
The identical benchmark fixture measures `syncNow` publication using a static
source and in-memory bucket. It excludes network latency, WAL fsync, and group
delay. Baseline ran before the production edit. Raw samples transcribed from
the measurement tool output are in `archive-micro-samples.json`.

| Payload per extent | Baseline median | Candidate median | Time change | Allocated bytes/op change |
| --- | ---: | ---: | ---: | ---: |
| 1 × 4 KiB | 12.434 µs | 8.264 µs | -33.5% | -19.0% |
| 32 × 4 KiB | 267.631 µs | 171.393 µs | -36.0% | -23.5% |
| 1024 × 4 KiB | 7.414 ms | 5.388 ms | -27.3% | -22.9% |

Only duplicate serialization/validation is removed. The bytes hashed by
`buildExtent` are the bytes uploaded. Conditional writes, stable-head readback,
checksums, original object keys, and the before-ack barrier remain enabled.

## Real three-voter HTTP/QUIC measurement

Local macOS processes and Docker MinIO, three interleaved runs per revision,
3,000 SQL inserts/run, concurrency 16, fresh cluster/bucket each run, checkpoint
interval zero, background object-store sync interval one hour. Both revisions
use `before-ack` and the same updated harness. The initial 1,000-request harness
smoke is excluded. All 18,000 measured writes succeeded; each run verified the
exact row count with a linearizable query, with zero runtime failure log lines.

| Median across runs | Baseline | Candidate |
| --- | ---: | ---: |
| Successful writes/s | 252.3 | 218.1 |
| p50 | 51.7 ms | 44.2 ms |
| p95 | 127.8 ms | 199.2 ms |
| p99 | 213.1 ms | 382.4 ms |
| Object-store HTTP requests/write | 0.397 | 0.313 |

This run does **not** establish an end-to-end throughput or tail-latency win.
Candidate median throughput was 13.6% lower and p99 79.4% higher, while ranges
overlap widely: baseline 126.7–335.1 and candidate 163.7–492.5 writes/s.
This is a shared developer machine with Docker virtualization, not an isolated
performance runner; three samples cannot distinguish a regression from this
variation. Archive CPU/allocated-byte improvement must not be presented as an HTTP
performance improvement.

Object counters cover all three nodes, after schema creation through the final
linearizable verification. They are not solely timed client traffic. Changes
in requests/write reflect observed batch formation, not an algorithmic removal
of object-store calls. No object-store request failure or retry was observed.
Whole-process CPU utilization, peak RSS, cloud-store latency and faulted
before-ack performance were not measured in this experiment.

CI now runs three alternating base/candidate before-ack samples at 10,000 writes
and concurrency 16, retaining raw method counters and latency distributions.
The existing async availability scenarios remain. Remote results for the final PR revision are published by the Performance
workflow; the local measurements above remain unchanged.

## Verification of the measured working tree

- `CGO_ENABLED=0 go test ./...` passed.
- `CGO_ENABLED=0 go vet ./...` passed.
- `go test -race ./pkg/network ./pkg/recovery` passed.
- `go test ./cmd/rhiza-ffi` and `cargo test --manifest-path sdk/rust/Cargo.toml` passed.
- Cached SQL/Graph/KV/Notify receipts stay unsuccessful while the installed
  barrier is unavailable, then succeed through that same barrier at the original
  slot after recovery; retries do not advance the consensus tip.
- Uploaded archive bytes recover the original values and certificates exactly.

Reproduce the server runs with `RHIZA_SERVER_BENCH_DURABILITY=before-ack`,
`RHIZA_SERVER_BENCH_REQUESTS=3000`, `RHIZA_SERVER_BENCH_CONCURRENCY=16`, and
`benchmarks/run-server-reference.sh SOURCE_DIR OUTPUT.json`. Use separate
baseline/candidate source directories and alternate their execution order.
