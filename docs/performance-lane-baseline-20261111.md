# SQL/KV/Graph durability lane baseline

Measured on 2026-11-11 from commit `828155d7d18687647eaf8873e42ec6df27b36458`
plus the benchmark/instrumentation changes in this worktree. Environment:
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

| durability | lane | ops/s | p50 ms | p95 ms | ns/op | Record RPC/op | Learned RPC/op | fsync/op | uploads/op | HEAD/op | GET/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| async | SQL | 1,306 | 11.42 | 17.21 | 0.77 ms | 0.13 | 0.065 | 0.195 | 0 | 0 | 0 |
| async | KV | 438 | 23.42 | 150.2 | 2.28 ms | 0.43 | 0.125 | 0.615 | 0 | 0 | 0 |
| async | Graph | 104 | 149.0 | 184.2 | 9.59 ms | 0.14 | 0.070 | 0.210 | 0 | 0 | 0 |
| before-ack | SQL | 696 | 21.75 | 32.66 | 1.44 ms | 0.13 | 0.065 | 0.26 | 0.26 | 0.905 | 0.385 |
| before-ack | KV | 398 | 31.39 | 82.28 | 2.51 ms | 0.29 | 0.100 | 0.46 | 0.26 | 0.905 | 0.385 |
| before-ack | Graph | 100 | 158.4 | 185.0 | 9.99 ms | 0.13 | 0.065 | 0.26 | 0.26 | 0.905 | 0.385 |

In this concurrent workload, one consensus slot carried a median 14.3-15.4
SQL or Graph commands and 8-10 KV commands. That batching explains why one
logical mutation paid much less than one full Record round and WAL fsync.

Before-ACK added 0.065 fsync per logical mutation for SQL and Graph, or about one
extra fsync per 15 mutations. KV did not show a stable increase because its
batch size and fsync coalescing varied across the three short runs. Before-ACK
also added 1.55 logical object calls per
mutation (0.26 uploads, 0.905 HEADs, 0.385 GETs), about one publication per 3.85
mutations and about five HEAD/GET metadata calls per upload.

Before-ACK p50 was higher for SQL and KV (11.42 -> 21.75 ms and 23.42 -> 31.39
ms), while Graph was 149.0 -> 158.4 ms. Throughput was lower for SQL (1,306 ->
696 ops/s), but KV and Graph overlapped with their Async ranges. These are short,
independently started runs; use the paired Dory runner before treating the
throughput ratios as stable.

The Graph materializer dominated that lane: 9.6-12.4 ms/op and about 324-338
KiB/op. Before-ACK therefore changed its aggregate cost much less than SQL or KV.

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
   2,388-3,755 allocs/op depending on the fixture, larger than the consensus and
   archive call overhead visible in this run.

## Implemented: combined KV/Graph request status

KV and Graph now use one materializer lookup for receipt plus fingerprint match,
before and after submit. This reduces their duplicate idempotency lookups from
four to two per call. SQL already used the same combined pattern.

The focused materializer tests cover missing, exact retry, conflicting intent,
and expired receipt states. `go test ./pkg/materializer ./pkg/network`,
`go vet` for both packages, and `git diff --check` pass.

Post-change 200x x3 benchmark, same local fixture:

| lane | allocs/op before | allocs/op after | B/op before | B/op after |
| --- | ---: | ---: | ---: | ---: |
| async KV | 457 | 308 | 23,134 | 16,841 |
| before-ack KV | 474 | 338 | 34,939 | 30,226 |
| async Graph | 2,367 | 2,355 | 324,546 | 324,554 |
| before-ack Graph | 2,387 | 2,381 | 337,638 | 337,384 |

The KV allocation reduction is repeatable. Graph still spends its time in
apply/snapshot work, so the removed lookup is below the noise floor. The host was
under elevated load during the post-change run; throughput samples are retained
as diagnostics, not as a stable speedup claim.

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
