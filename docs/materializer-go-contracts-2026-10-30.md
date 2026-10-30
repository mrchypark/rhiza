# Materializer Go Contract Appendix

This appendix is an exact API/schema companion to the materializer reverse
specification. Paths below name the canonical project checkout, never this
temporary worktree.

Review status: corrected and independently rechecked for the findings raised; see
[materializer-go-egm-review-2026-10-30.md](materializer-go-egm-review-2026-10-30.md).

## Public aliases and data API

`rhiza.go` exports aliases, not wrapper schemas:

| Public name | Underlying type |
| --- | --- |
| `SQLStatement`, `SQLStatementOutputRef`, `SQLStatementResult` | `internal/types` |
| `GraphCommand`, `GraphResult`, `GraphStreamEvent`, `GraphStreamRecord` | `internal/types` |
| `NotifyCommand`, `MutationReceipt`, `MutationStatus` | `internal/types` |
| `GraphReachableRequest`, `GraphReachableNode`, `GraphReachableResult` | `internal/types` |
| request/response aliases for execute/query/KV/graph/status | `pkg/network` |

`DB` methods are: `Execute`, `ExecuteReturning`, `ExecuteReturningOne`,
generic `ExecuteReturningMap[T]`/`ExecuteReturningMapOne[T]`, `Migrate`,
`Query`, `KVGet`, `KVPut`, `KVDelete`, `KVCAS`, `GraphExecute`, `GraphQuery`,
`GraphReachable`, `GraphChanges`/`GraphStreamRead`, `GraphStreamOffset`,
`SetGraphStreamOffset`, `TrimGraphStream`, `NotifyPublish`, `NotifySubscribe`,
`NotificationDrops`, and `RequestStatus`. Their context reaches the runtime;
they do not imply an atomic cross-call transaction.

`SQLRow.Columns` and `Values` return clones. `Value(i)` fails outside bounds;
`Named(name)` fails if absent. Mapper helpers map returned SQL rows in result
order; mapper error returns the response with no mapped success value.

Source: [rhiza.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/rhiza.go).

## JSON schemas

All shown keys use these exact tags; omitted fields use Go `omitempty`.

```text
MutationReceipt {slot:uint64, status:"committed"|"rejected",
 error_code?:string, rows_affected?:int64, last_insert_id?:int64,
 applied?:bool, retry_through_slot:uint64}
SQLCommand {request_id:string, sql?:string, args?:[]any, want_rows?:bool,
 require_one?:bool, statements?:[]SQLStatement, migration?:SQLMigration}
SQLMigration {version:int64, name:string, checksum:string}
SQLStatement {sql:string, args?:[]any, want_rows?:bool,
 expected_rows_affected?:*int64, expected_returned_rows?:*int64,
 output_refs?:[]SQLStatementOutputRef}
SQLStatementOutputRef {arg_index:int, statement_index:int,
 column_name?:string, column_index?:*int}
SQLStatementResult {rows_affected?:int64, last_insert_id?:int64,
 columns?:[]string, rows?:[][]any}
SQLCommandResult {statements:[]SQLStatementResult, error?:string}
KVCommand {request_id:string, operation:string, key:string, value?:[]byte,
 expected?:[]byte, expected_exists?:bool, ttl_ms?:int64,
 expires_at_unix_ms?:int64, observed_at_unix_ms?:int64}
KVCommandResult {applied:bool}
NotifyCommand {request_id:string, topic:string, payload:[]byte}
GraphCommand {request_id:string, cypher?:string, args?:map[string]any,
 events?:[]GraphStreamEvent, stream_offset?:GraphStreamOffsetMutation,
 stream_trim?:GraphStreamTrimMutation}
GraphStreamOffsetMutation {stream:string, consumer:string, sequence:uint64}
GraphStreamTrimMutation {stream:string, through_sequence:uint64}
GraphStreamEvent {stream:string, kind:string, payload:any}
GraphStreamRecord {sequence:uint64, kind:string, payload:any}
GraphCommandResult {columns?:[]string, rows?:[][]any, error?:string,
 applied_slot?:uint64, consensus_tip?:uint64}
```

`GraphReachableRequest` is `{start_label,start_property,start_value,edge_type,
node_label?,node_filters?,result_property,max_depth,max_results,
max_scanned_edges,max_bytes,consistency?,require_applied_slot?}`. Its response
is `{nodes:[{value,distance}],start_found,scanned_edges,applied_slot,
consensus_tip}`. A `GraphNodePropertyIndex` is a label/property pair; at most
64 are configured, and duplicates collapse while preserving first occurrence.

Source: [value.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/types/value.go), [materializer.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go).

## Values, fingerprints, and limits

`MutationKind` numeric values are SQL=1, KV=2, Notify=3, Graph=4. Statuses are
exactly `committed` and `rejected`; stable error-code strings are
`execution_failed`, `precondition_failed`, and `result_retention_full`.

`MaxRequestIDBytes=64`; default idempotency window is 65,536 slots. SQL has 64
commands per decided value, 64 statements per command, 256 KiB SQL/Cypher,
999 arguments, 2,000 result columns, 10,000 rows, 16 MiB aggregate result,
1 MiB cell/result envelope. Graph reachable limits are depth 64, result rows
10,000, scanned edges 1,000,000, and 16 MiB nodes JSON. Graph query work is
10,000,000. Graph streams return at most 1,000 records. Retained SQL results
have a 256 MiB byte quota and a receipt-count cap of `64 × window`; accounting
is validated before retention. Envelope-count exhaustion returns an apply
error. Result-byte exhaustion rolls back the command and records a
`result_retention_full` rejection.

For SQL blob arguments only, wire encoding changes a Go `[]byte` to the exact
one-key object `{"$rhiza_blob":"<standard-base64>"}`. Other singleton-looking
objects remain JSON objects. Decoders reject trailing JSON values/data.

`SQLFingerprint`, `GraphFingerprint`, `KVFingerprint`, and `NotifyFingerprint`
clear the request ID then hash a domain-separated JSON representation. KV
clears computed absolute timing before hashing. `KVRequestMatches` also
normalizes legacy absolute expiry to a TTL duration; it is deliberately
independent of first-observation time.

## Applying a decided value

```text
state = (sqlTip, graphTip, SQL state, Graph state, receipts)

new non-graph value at tip+1:
  one SQLite transaction applies/rejects command -> durable receipt -> SQL tip advances
new graph value at tip+1:
  graph tx effects/events/receipt -> graph pending+tip -> SQLite tip advances
  graph read exposure only when sqlTip == graphTip
failed deterministic command:
  command-local effects roll back; rejected receipt and slot advancement may commit
infrastructure failure/cancel before known commit:
  return error; no invented receipt
unknown graph commit outcome:
  poison reads/apply until reopen
older slot replay:
  skipped after initial execution-policy validation; no value-hash comparison
current-tip replay:
  permitted only when the slot/value hash agrees
pending graph recovery:
  replay must exactly match the pending decided graph value
```

SQL and KV state are not globally transactionally coupled to graph state: the
graph pending journal closes the cross-store crash/replay gap. Therefore the
reader must not expose graph results for a graph slot while SQLite is behind.
Receipts make only matching requests idempotent and expire after their retry
window; they are not permanent deduplication records.

## Local storage schema

SQLite creates `_rhiza_meta(key TEXT PRIMARY KEY,value TEXT)`, `_rhiza_kv(key
TEXT PRIMARY KEY,value BLOB NOT NULL,expires_at_unix_ms INTEGER NOT NULL
DEFAULT 0)`, and the partial expiry index. `_rhiza_idempotency` is `WITHOUT
ROWID`, keyed by `(kind,request_id)`, with 32-byte fingerprint, commit slot,
status/error/affected/insert/applied fields, and `sql_result BLOB NOT NULL
DEFAULT X''`; it is indexed by commit slot. `_rhiza_migrations` must exactly be
`version INTEGER PRIMARY KEY CHECK(version > 0), name TEXT NOT NULL, checksum
TEXT NOT NULL CHECK(length(checksum) = 64)` in a STRICT table.

New SQLite metadata includes current SQL policy marker, window, result byte and
count counters, `applied_slot=0`, `state_slot=0`, and all-zero 32-byte applied
hash. Existing materialization lacking these compatible records is refused,
not upgraded silently. Graph keys include idempotency by request ID and per-slot
journal/pending/tip metadata; malformed graph metadata is refused.

## Snapshot and exact-base restore state machine

```text
no journal
  --Prepare(exact authority + descriptor hashes)--> intent
intent --stage/copy verified checkpoint pair--> staged
staged --backup SQLite--> backup-sql --backup graph--> backup-graph
backup-graph --publish graph--> publish-graph --publish SQLite--> published
published --validate pair + reconcile indexes--> installed
installed --Finalize(same authority, before suffix replay)--> no journal

any interruption: preserve journal, stage, originals; resume only with exact
same authority and file inventory. Different anchor/source: error, no mutation.

Pair publication is not atomic: graph is renamed before SQLite and the journal
persists `backup-sql`, `backup-graph`, `publish-graph`, and `published` so
restart recovery can reconcile the corresponding partial filesystem state.
Only after pair validation and declared-index reconciliation does it enter
`installed`.
```

Authority fields are `StoreUUID`, `ConfigDigest`, `Index`, `ConfigID`, and
lowercase-hex SHA-256 `RootHash`, `PrefixHash`, `StateHash`. It has no default
or bootstrap interpretation: index zero/pending local state cannot authorize
replacement. `PrepareLocalRestore` must precede `OpenLocalFromBase`; the latter
requires both SQL and graph tips equal authority index. `FinalizeLocalRestore`
is the only cleanup transition and removes retained originals/stage then the
journal. `ValidateNoLocalRestore` refuses unexplained artifacts.

Source: [local_restore.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/local_restore.go).

## Intentional documentation boundary and gaps

This appendix does not recapitulate network request schemas, HTTP response
mapping, consensus certification, QLog/archive storage, or admission protocol.
Those established specifications remain dependencies. The materializer contract
requires their output to be an ordered certified slot/value stream; it specifies
the local transition, partial-apply gate, rollback, and exact replay behavior
once that stream arrives.

Known source-level gap: public stream API defaults/validation are primarily in
the network handler, which is excluded; callers of `GraphReadStream` should
pass an explicit valid limit and wait duration. Exact engine parser acceptance
is delegated to the pinned SQLite/LatticeDB engines after the deterministic
guards described above. `Snapshot`, `SnapshotAt`, `SnapshotTo`, `Restore`, and
`RestoreFile` are intentionally unavailable for the combined store; the usable
pair APIs are `CheckpointFilesAt` and `RestoreCheckpoint`.
