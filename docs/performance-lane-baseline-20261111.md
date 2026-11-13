# SQL/KV/Graph durability lane baseline

Measured on 2026-11-11 from `origin/main` commit
`b193f1a620ad0b2207d3d90e4295bd2d6243da8b` plus the benchmark and
request-status changes in this branch. Environment:
Apple M1, Darwin 25.6.0 arm64, Go 1.27.3.

## Method

`BenchmarkDurabilityLane` uses the existing in-memory three-voter fixture, real
`qlog` WAL files, and 16 concurrent clients. Each case ran 200 logical mutations,
three times. Before-ACK attaches the production-style `EnsureDurable` plus
archive barrier to a temporary filesystem object store. Async has no foreground
archive barrier.

The measured counters are:

- `record-rpc/op`: remote Record calls through the test transport.
- `fsync/op`: successful file syncs across all three voter WALs.
- `uploads/op`, `heads/op`, `gets/op`: logical object-store calls from the
  metered bucket. The filesystem provider has no S3 HTTP transport, so
  `s3-http/op` is zero by design.
- latency percentiles: completion latency of admitted calls.

The p99 has only about two observations at 200 samples per run and is a
regression signal, not an SLO estimate.

## Results

Medians of three runs:

| durability | lane | ops/s | p50 ms | p95 ms | ns/op | allocs/op | Record RPC/op | Learned RPC/op | fsync/op | uploads/op | HEAD/op | GET/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| async | SQL | 872 | 16.92 | 24.55 | 1.15 ms | 150 | 0.13 | 0.065 | 0.26 | 0 | 0 | 0 |
| async | KV | 376 | 36.12 | 70.42 | 2.66 ms | 327 | 0.40 | 0.120 | 0.64 | 0 | 0 | 0 |
| async | Graph | 93.9 | 169.7 | 189.8 | 10.65 ms | 716 | 0.14 | 0.065 | 0.275 | 0 | 0 | 0 |
| before-ack | SQL | 640 | 22.54 | 36.63 | 1.56 ms | 177 | 0.13 | 0.065 | 0.26 | 0.26 | 0.905 | 0.385 |
| before-ack | KV | 464 | 25.57 | 63.94 | 2.16 ms | 342 | 0.22 | 0.090 | 0.405 | 0.26 | 0.905 | 0.385 |
| before-ack | Graph | 94.0 | 166.0 | 216.4 | 10.64 ms | 742 | 0.13 | 0.065 | 0.26 | 0.26 | 0.905 | 0.385 |

In this concurrent workload, one consensus slot carried a median 14.3-15.4
SQL or Graph commands and 8-10 KV commands. That batching explains why one
logical mutation paid much less than one full Record round and WAL fsync.

The fsync counts did not show a uniform Before-ACK penalty on current main:
SQL remained at 0.26 per logical mutation and Graph moved from 0.275 to 0.26.
KV varied with batch coalescing. Before-ACK did add 1.55 logical object calls per
mutation (0.26 uploads, 0.905 HEADs, 0.385 GETs), about one publication per 3.85
mutations and about five HEAD/GET metadata calls per upload.

Before-ACK p50 was higher for SQL (16.92 -> 22.54 ms), lower for KV in this run
(36.12 -> 25.57 ms), and effectively flat for Graph (169.7 -> 166.0 ms). These
are short, independently started runs; use the paired Dory runner before
treating the throughput ratios as stable.

The Graph materializer dominated that lane: about 10.6 ms/op and 716-742
allocs/op. Before-ACK therefore changed its aggregate cost much less than SQL or
KV.

Single-operation component measurements on the same host:

| component | samples | result |
| --- | --- | --- |
| `BenchmarkWALAppendSync` | 300x x3 | 15.77-17.37 ms/op, 320 B/op, 1 alloc/op |
| `BenchmarkCoreProposeThreePeersParallel` | 100x x3 | 33.04-37.11 ms/op, about 31 allocs/op |
| `BenchmarkGraphApply` | 200x x3 | 10.61-12.40 ms/op, about 409 KiB/op, 3,755 allocs/op |
| `BenchmarkSQLBatchApply/1` | 200x x3 | 0.53-1.46 ms/op, about 11.5 KiB/op, 243 allocs/op |

## Where the cost is

All three lanes share the same consensus path. A three-voter slot starts Record
calls for every member and returns after quorum (`pkg/quepaxa/core.go:1163`).
The benchmark's serialized quorum transport measured 0.13-0.40 remote Record
calls per logical mutation after batching. A single mutation without batching
would have a different, much higher ratio.

Each recorder appends a receipt and joins the 25-microsecond, 64-waiter group
commit (`pkg/quepaxa/core.go:1354`, `pkg/quepaxa/group_commit.go:9`). The
benchmark measured 0.195-0.66 successful fsyncs per logical mutation across all
three voters.

Before-ACK additionally runs `EnsureDurable` and `archive.SyncThrough` before
acknowledgement (`pkg/node/node.go:765`). The archive barrier batches callers but
currently performs up to three validated `Load` phases per publication
(`pkg/recovery/archive.go:1000`, `:1013`, `:1030`). The measured 0.905 HEAD and
0.385 GET per logical write show that publication metadata, not data uploads,
dominates the object-store call count.

## Ranked improvement candidates

1. Remove redundant archive `Load`/HEAD work before publication. Keep the
   verified load inside the publication fence, and avoid repeating an identical
   pre-fence load when the local head is still current. This directly targets
   about five HEAD/GET metadata calls per upload while preserving the CAS fence.
2. Collapse KV and Graph's duplicate request-match and receipt lookups. KV reads
   the same receipt path four times around submit (`pkg/network/server.go:1872`);
   Graph does the same (`pkg/network/graph.go:110`). One combined match+receipt
   API would reduce local LatticeDB reads from four to two without changing
   consensus semantics.
3. Evaluate the second local decision-marker fsync in Before-ACK. Quorum
   receipt durability already certifies the value, while the local marker adds
   an fsync before the archive barrier. Removing it needs an explicit recovery
   proof because Async deliberately relies on periodic decision sync
   (`pkg/quepaxa/core.go:2016`).
4. Hedge the second Record peer instead of launching both remote peers at once.
   A local plus one remote recorder can satisfy quorum; the second remote attempt
   is currently started immediately (`pkg/quepaxa/core.go:1189`). Measure p99
   before adopting this because slow-peer tail latency may worsen.
5. Treat Graph apply allocation as a separate optimization. It measured
   716-742 allocs/op in this run, larger than the consensus and archive call
   overhead visible here.

## Implemented: combined KV/Graph request status

KV and Graph now use one materializer lookup for receipt plus fingerprint match,
before and after submit. This reduces their duplicate idempotency lookups from
four to two per call. SQL already used the same combined pattern.

The focused materializer tests cover missing, exact retry, conflicting intent,
and expired receipt states. `go test ./pkg/materializer ./pkg/network`,
`go vet` for both packages, and `git diff --check` pass.

Current main already uses fingerprint-specific combined status methods for its
managed KV/Graph protocol. This change applies the same one-lookup behavior to
the remaining generic paths without changing the managed `commitUnknown` flow.
The lookup reduction from four to two is deterministic; the measured lane totals
are baseline diagnostics, not an isolated speedup claim. In this run KV used
327-342 allocs/op and Graph used 716-742 allocs/op.

The archive pre-publication `Load` was not removed. It is a fast path for a
publisher that has already advanced the shared head; deleting it would force more
lock admissions and may make the contended case worse.

## Limits

This is an in-process loopback measurement, not production QUIC or S3. Dory's
Kubernetes component remained invalid, so the existing Dory matrix was not run.
The filesystem object store proves logical call multiplicity but not request
bytes, retries, or real provider tail latency. The next qualification should use
the existing Versity/Dory runner with the same lane matrix and record
`s3_http_requests`, SDK attempts, and bytes.
