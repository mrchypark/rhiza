# Managed SQL/KV/Graph HA — candidate

This document describes an **unreleased, incompletely qualified candidate**.
It is not a claim that the new profile has passed CI, all providers, Pod
replacement, Rust native execution, or Chaos Mesh qualification.

## Explicit profile

`rhiza-managed-request-graph/v1` selects materialization policy **10** and the
dedicated peer protocol `rhiza-peer-managed-request-graph-v1`. The same managed
Host can execute SQL, KV, and Graph operations. It uses the existing validated
bootstrap/runtime configuration; there is no new per-request CR option.

Profiles 7, 8, and 9 keep their existing semantics. Their database files,
archives, prepared handoffs, and peer protocols are **not automatically
converted** to profile10. Selecting a different profile is not a migration or
permission to relabel existing data. Unknown profiles and incompatible peers
must be refused before application effects.

The current scope is **generation0 with quorum preserved**. Whole-generation
disaster recovery and arbitrary network-partition recovery are not qualified
by this candidate.

## Go API

The existing `pkg/managedsql.Host` adds these Graph methods:

- `GraphExecute`, `GraphQuery`, and `GraphReachable`;
- `GraphChanges`, `GraphStreamRead`, and `GraphStreamOffset`;
- `SetGraphStreamOffset` and `TrimGraphStream`.

`GraphExecute` accepts `rhiza.GraphCommand` (the public alias of the actual
command type) and returns `network.GraphExecuteResponse`, including its real
`MutationReceipt`. Queries and reachable operations return actual Graph
results. Stream methods use the existing `network.GraphStreamReadRequest`,
`GraphStreamReadResponse`, `GraphStreamOffsetRequest`, and
`GraphStreamOffsetResponse` types. Offset/trim mutations also return the actual
Graph mutation receipt.

For example, with an already opened, validated profile10 Host:

```go
receipt, err := host.GraphExecute(ctx, rhiza.GraphCommand{
    RequestID: string([]byte{0xff, 1}),
    Cypher:    "CREATE (:Item {name: 'one'})",
})
```

This fragment does not construct authority, bootstrap a cluster, or substitute
for checking `err` and the receipt. Do not resubmit it to resolve an uncertain
outcome.

SQL `Execute`/`Query`, KV `KVMutate`/`KVGet`, and local `RequestStatus` retain
their existing public methods. Graph status uses
`network.RequestStatusRequest{Kind: "graph", RequestID: originalRawID}`.
Status is a local observation, not proof of object-store ACK durability.

## Rust and the C ABI

`rhizadb::ManagedDb` adds `graph_execute`, `graph_query`, `graph_reachable`,
`graph_changes`, `graph_stream_read`, `graph_stream_offset`,
`set_graph_stream_offset`, and `trim_graph_stream`.

Mutation inputs use `ManagedGraphCommand`, `ManagedGraphValue`,
`ManagedGraphEvent`, `ManagedGraphOffset`, and `ManagedGraphTrim`. Request IDs
are `Vec<u8>`; they are not coerced through UTF-8 strings. Typed values preserve
raw string/map-key bytes, number lexemes, floating-point bit patterns, and
nil-versus-empty containers in the original representation. This does not make
invalid Cypher or unsupported execution values valid: existing engine
validation still applies.

`ManagedGraphReceipt` carries the actual slot/status/applied fields. Stream
read/offset results use `ManagedGraphStreamRead`, `ManagedGraphStreamRecord`,
and `ManagedGraphOffsetRead`. Event order is retained. Offset sequences and trim
boundaries use the full `u64` range, not JavaScript-number rounding.

The C ABI transports mutations as the typed execution frame in `frame_b64`.
The ordinary JSON Graph mutation DTO is not an alternative original. Rust
read-only query DTOs continue to use the existing JSON read semantics; they
are not mutation intent fingerprints.

## Uncertain outcomes and exact recovery

A submitted mutation can return `commit_unknown` even when its effect committed.
Keep the original kind/raw ID and the error's known slot/cause context. Do not
generate a new ID, replace the payload, or submit the mutation again as a
recovery strategy.

Use Go `Host.RecoverRequest` with the exact Graph selector, or Rust
`ManagedDb::recover_request(MutationKind::Graph, &original_id)`. These resolve
the engine's retained canonical original, replay certified history, and check
the configured durability barrier. They must add **zero application proposals**.
They do not refresh retention or accept a replacement payload as the original.
Go `RecoveryResult.Graph` and Rust `ManagedRecoveryResult::Graph` return the
original Graph receipt and raw request ID.

Missing/expired originals, lookup uncertainty, and conflicting intent remain
unresolved or rejected; absence is not proof of no effect. The existing
no-argument recovery facade uses only this handle's bounded recovery hint and
can remain ambiguous. An explicit selector is required after its hint is lost.
Known submission slots bound the canonical first result; a duplicate submit
can have a later slot than the original. See [the internal request API](managed-internal-request-api.md).

## Normal requests versus the operator

After actual startup and initial admission, normal profile10 reads, writes,
status, and exact recovery use the engine runtime. They do not add CR
reservation/finalization records or a second application intent log. This is a
control-path boundary, not a measured latency or Kube-request-count claim.

CRs remain responsible for policy, bootstrap/repair authority, observation, and
disaster-recovery coordination. Initial admission, repair, and generation
changes still require their actual authority checks. Normal operation is not
permission to bypass membership, registered WAL identity, lifecycle, poison,
quorum, or object-store durability checks.

## NoPVC startup contract and remaining gaps

The required startup path is a **verified SQLite/native Graph pair plus the
contiguous, full-hash-checked certified suffix**, reaching the registered local
WAL's captured certified ACK tip. The Node must open that reconstructed pair,
not merely check a separate reconstruction and then trust old native files.

A required missing root, wrong root/full hash/certificate, changed WAL identity
or prefix, poison, and incomplete/ambiguous installation must fail closed.
Validated first genesis is distinct from a missing required recovery root.
NoPVC does not remove those checks or turn missing evidence into bootstrap.

Focused Go tests have exercised public startup through the actual pair installer
at the original database path, with certified suffix data, original receipts,
and a fresh SQLite file rather than only reopening the existing pair. A separate
public restart test leaves a real native-ahead/SQLite-pending state and verifies
authenticated same-slot reconciliation before installation. These are local
functional results, not completed NoPVC Pod or cross-provider qualification.

An actual child-process test exits during the Node installer's `after-staged`
phase. Public reopen reauthenticates the original cut and retained physical input
pair, finishes that attempt, and applies a later certified ACK suffix. Normal
and race checks cover this one Node crash phase, not every installer instruction.
Unknown input bytes remain preserved and refused; an unidentifiable old external
reconstruction temp is not automatically deleted.

After a focused Core certified-redelivery repair, changed-candidate public
publisher and suffix-restart checks pass normally and under the race detector.
Earlier root-preparation failures remain recorded; their precise historical
schedule is not established by those passes.

**Remaining gaps:** the materializer's wider crash-phase tests do not establish
all those phases through public Node startup. Final-head CI and runtime gates
below must still complete. This candidate is not a completed policy10 recovery
guarantee.

Native-ahead/SQLite-pending publication blocks local reads and status. Exact
recovery and linearizable reads first use their real certified replay boundary
and may finish the same slot before taking a clean read snapshot. Fatal
commit-unknown/poison is different and must not be cleared by a retry.
Graph long-poll reads borrow the real Node runtime; Close cancels and
drains them before native resources close, without holding the Host mutex for
the entire wait.

## Storage and resource limits

HA immutable data remains append-only with no online deletion GC. No new lease
system is introduced; the existing archive publication lock remains in use.
Do not equate Pod API deletion with termination of an isolated old process or
with completion of outstanding storage work.

Existing mutation admission, typed depth/node/count/body bounds, receipt
retention/accounting, and peer framing checks remain. Only strictly validated
Graph frames under trusted profile10 receive the enlarged 256KiB execution
frame budget; legacy values keep their existing bounds. This is not a proof of
a continuous physical-memory peak.

Existing checkpoint size and prepared-Graph aggregate artifact limits remain
32GiB. They are restore/fixture boundaries, not a claim about total database
capacity, RAM use, or production-cloud parity. Large restore resource behavior
still needs explicit qualification.

## Qualification status

Focused Go tests cover actual Core/WAL/native effects, raw-ID receipts,
proposal-free exact recovery, public fresh-pair startup, pending healing,
and long-poll Close. They do not replace final-head CI, all three provider SDK
paths (S3, GCS, Azure), native Rust/C ABI runtime, Pod replacement/restart,
and both scoped Chaos Mesh faults. A synthetic S3 wire-fixture pass is not
three-provider emulator or production-cloud parity. No release or production
readiness is asserted here.

The current public managed Host requires `before_ack` durability. An async
success ACK beyond the remote archive HEAD is outside that public contract;
an uncertain outcome or a certified-only decision must not be reported as such
an ACK. No new durability option is introduced by this candidate.
