# Materializer Go Reverse Specification

## Scope and model

This is the behavior of `pkg/materializer`, the shared command types in
`internal/types`, and the public Go façade in `rhiza.go` at
`008f8e576ad76425182e10a69b987f10cfb48b2a`. It describes current behavior,
not a proposed protocol. HTTP admission, consensus, and archive internals are
out of scope.

Review status: corrected and independently rechecked for the findings raised; see
[materializer-go-egm-review-2026-10-30.md](materializer-go-egm-review-2026-10-30.md).

One materializer owns replayable local state in two stores: SQLite holds SQL,
KV, migrations, SQL/KV/notify receipts, and the SQL applied tip; LatticeDB
holds graph data, graph receipts, graph stream state, and a graph tip. QLog is
the durable authority; local stores may be rebuilt by replay.

Every `Apply(ctx, slot, value)` is ordered. A new slot must be the next slot
after the applied tip; replay of the current slot is a no-op only when its
SHA-256 hash agrees, while already older slots are skipped after policy
validation. A conflicting current-tip hash, a gap, an incompatible
encoded value, or a closed/poisoned store fails. `ApplyBatch` processes supplied
decisions in sequence in one SQLite transaction and returns the first error.

Source: [materializer.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go), [graph_enabled.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/graph_enabled.go).

## Opening and ownership

`materializer.Open(dbPath string, readerCount int, idempotencyWindow ...uint64)`
canonicalizes `dbPath`, rejects a local-restore journal, resumes the ordinary
restore journal, and requires `readerCount >= 1`. Window defaults to 65,536
slots and is accepted only from 1,024 through 1,048,576 inclusive. Existing
SQLite and graph files must carry the current execution-policy marker; old
policy is incompatible and must be rebuilt from certified history.

The SQLite writer is single-connection WAL mode with `synchronous=normal`,
`busy_timeout=5000`, and autocheckpoint disabled. Reader handles are read-only
and have the same timeout. `Close` stops notification/checkpoint workers,
closes graph and SQL handles, and makes subsequent operations fail. The caller
owns the materializer and must not use it after `Close`.

The public `rhiza.Open(ctx, Config) (*DB, error)` owns this materializer behind
the node runtime. `DB.Close`, `Ready`, execution/query/KV/graph/notification
methods are forwarding public data APIs; callers must pass cancellation through
their `context.Context`. A cancelled operation returns its context/driver error
unless a durable commit is already ambiguous; it never rolls back a confirmed
commit solely because the caller stopped waiting.

## Encoded decided values

`types.ValidateExecutionPolicy` recognizes exactly current SQL, graph, KV,
notify, read-barrier, checkpoint-seal, leader-schedule, and reconfiguration
encodings. Unknown data and known older policy prefixes return
`sqlpolicy.ErrIncompatible`.

SQL batches begin `QBAT<sql-policy-version>` then one JSON array; graph batches
begin `QGRB<sql-policy-version>` then one JSON array; KV batches begin
`QKVB\x00`; single KV begins `QKVC\x00`; notify begins `QNTF\x00`. Empty SQL,
graph, or KV batches fail; SQL allows at most 64 commands. SQL and graph JSON
decoders use `UseNumber` and require EOF after the array. SQL `[]byte` arguments
encode as `{"$rhiza_blob":"base64"}`; that exact singleton object decodes back
to bytes. JSON numbers are preserved for deterministic engine conversion.

A batch item fingerprint is SHA-256 of `domain + NUL + canonical JSON item`
after clearing `request_id`; domains are `rhiza/sql`, `rhiza/graph`,
`rhiza/kv`, and `rhiza/notify`. KV fingerprints additionally clear observed and
absolute expiry timestamps. Thus reusing a request ID with another intent is a
hard conflict at admission/status matching, while a matching in-window request
returns its retained outcome. Replay-time duplicate behavior differs by kind:
SQL skips existing or pending IDs without comparing fingerprints; KV and notify
likewise skip existing receipts; graph compares fingerprints and rejects a
mismatch.

Source: [value.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/types/value.go).

## SQL apply and results

`SQLCommand` accepts legacy `SQL`/`Args` or transactional `Statements`; it has
`RequestID`, `WantRows`, `RequireOne`, and optional engine-owned `Migration`.
Each `SQLStatement` has SQL, args, `WantRows`, optional exact affected/returned
counts, and output references. An output reference replaces a null argument
with a named or indexed column from exactly one row returned by an earlier
statement. All statements in one command are one SQL transaction.

The command request ID is at most 64 bytes (an empty ID is permitted but then
does not obtain a retained SQL receipt). SQL text is at most 256 KiB, one
statement, and at most 999 bound args. The writer
enforces SQLite limits: 2,000 columns, 10,000 returned rows, 16 MiB aggregate
result bytes, 1 MiB per cell, and 1 MiB serialized retained mutation result.
Failure of a command rolls back that command's state; its deterministic
execution/precondition/retention failure can still be recorded as a rejected
receipt and the slot advances. Infrastructure failures do not create a receipt.

The writer rejects attach/detach, TEMP objects or qualifiers, virtual tables,
ANALYZE, unsafe schema/catalog writes, write PRAGMAs, and nondeterministic,
connection-local, or extension functions. It reserves maximum SQLite rowid;
an effective write that reaches `math.MaxInt64` fails. One `ALTER TABLE` or
`DROP TABLE` maintenance statement has narrowly scoped native exceptions.
These engine-facing restrictions are further specified by the established SQL
policy material; this document records their replay effect rather than an HTTP
validation substitute.

SQL receipts store status (`committed` or `rejected`), error code, affected
rows, last insert ID, `Applied`, slot, and optional statement result. Successful
SQL receipt construction does not set `Applied`, so it remains false; KV and
graph receipts explicitly set it. SQL precondition/no-op outcomes are likewise
not represented by a true `Applied` bit. Receipt
lookup is valid through `commit_slot + window - 1`, then reports absent.
Retained SQL result envelopes are quota-accounted; exhaustion rejects a
row-returning command with `result_retention_full` rather than dropping a
successful retry result.

`Query`, `QueryResult`, and `QueryResultAt` are local read APIs. `QueryResultAt`
returns the SQL tip captured for the result. Results are columns plus ordered
rows with supported driver values, subject to the same output bounds.

## KV apply and expiry

`KVCommand` has request ID, operation, key, optional value/expected bytes,
`ExpectedExists`, `TTLMS`, and upstream-populated observation/absolute-expiry
fields. Operations are `put`, `delete`, and CAS. KV idempotency compares ID,
operation, key, value, expected bytes/existence, and TTL duration—not the first
admission clock. A matching receipt returns its original `Applied` bit.

The upstream boundary populates observation and absolute-expiry timestamps.
`applyKV` writes the supplied absolute expiry and uses the supplied observation
time; it does not derive expiry from `TTLMS`. `TTLMS=0` means no expiry; a
nonpositive/effectively expired TTL is not live. Reads take caller `now`, filter
expired rows, and report absent; apply opportunistically prunes up to 256
expired rows. `KVGetAt` returns `(value, exists, SQL tip, error)`;
`KVGet` omits the tip. Expiry cleanup is local apply-time state maintenance, not
a separate replicated value.

## Graph apply, query, and streams

`GraphCommand` has one required request ID and exactly one primary mutation:
nonempty one-statement Cypher, `StreamOffset`, or `StreamTrim`. Events are
allowed only with Cypher. Cypher is <=256 KiB, has <=999 named arguments, and
each name is 1..256 bytes. Graph values recursively accept nil, bool, string,
finite supported number forms, arrays, and maps; unsigned values above int64
and unsupported Go types fail. Standalone `CREATE` may create exactly one node;
relationship creation uses `MATCH ... CREATE`.

Within a graph slot, LatticeDB applies graph effects, events, and request
receipt atomically before graph metadata advances. SQLite then records the slot
as applied. Until both tips agree, graph reads are hidden/gated. A pending graph
journal prevents later slots from bypassing a partially applied slot. On
restart, only an exact matching decided graph value may complete recovery.
Unknown commit acknowledgement poisons graph reads and apply until reopen;
never infer rollback from a missing acknowledgement.

Graph query returns deterministic `Columns`, ordered `Rows`, `AppliedSlot`, and
`ConsensusTip`; deterministic parse/query errors become a retained graph
rejection, but infrastructure/unknown errors remain retryable without receipt.
Graph read snapshots wait for a writer or context cancellation and are bounded
by the 10,000-row, 16 MiB result, and 10,000,000-work limits.

Events append per-stream monotonically increasing sequence records atomically
with their Cypher mutation. `GraphReadStream(ctx, stream, after, limit, wait)`
returns `(records, appliedSlot, error)`: records are strictly after `after`,
while the second result is the materializer's applied slot captured as `m.tip`,
not a stream high-water sequence or a caller cursor.
Limit is 1..1,000 (with the implementation's default applied by its caller);
wait may long-poll but does not block graph apply. Offset mutation atomically
sets `(stream, consumer)->sequence`; `GraphStreamOffset` returns sequence,
existence, and applied slot. Trim atomically removes records through its
inclusive sequence. Reserved `__lattice_` streams are forbidden for events but
allowed for offset/trim. Stream and consumer names are valid UTF-8, 1..255 bytes.

`GraphReachable` performs bounded outgoing traversal: it finds the one start
node by label/property/value, never returns the start, returns matching nodes
and expands them, and counts every fetched outgoing edge (including duplicates
and rejected targets). The caller must provide nonzero bounded depth/results/
scanned-edges/bytes not exceeding 64/10,000/1,000,000/16 MiB. Optional exact
`RequireAppliedSlot` must equal local graph/SQL state; otherwise it fails.

## Notification and receipt lifecycle

Notify commands are fingerprinted/idempotent replicated records. A committed
notification is dispatched after apply to local subscribers of its exact topic.
There are at most 64 subscriptions, a one-item producer queue, and dispatch
depth 64; full queues increase the drop counter rather than blocking apply.
`NotifySubscribe` returns receive-only bytes, cancellation function, and error.

All receipts are retained only for the configured slot window. SQL uses SQLite
receipt rows plus a Bloom filter that only selects whether a SQLite receipt
lookup might be necessary; it is not durable deduplication or expired-ID replay
protection. KV/notify use the common SQL receipt table; graph uses graph
metadata. A duplicate outside
the window is a new request unless other admission controls reject it. Receipt
methods fail closed when graph commit state is unknown.

## Snapshot and restore boundaries

`Snapshot`, `SnapshotAt`, and `SnapshotTo` are deliberately unavailable for
the combined SQL+graph store and return an error directing callers to
`CheckpointFilesAt`. That method freezes a SQLite backup plus a graph snapshot,
requires SQL and graph tips to agree, returns fixed roles `sqlite` and
`graph-data`, their exact tip, and a caller-owned cleanup function. `Restore`
and `RestoreFile` likewise return an error; `RestoreCheckpoint` alone installs
the exact two-role set after policy and pair validation. Before the restore
journal commits, recovery attempts rollback to the original; rollback or
reopening can fail and the returned error can join those failures. After the
journal is `committed`, recovery retains the replacement, so a later cleanup
failure can return an error with the replacement selected. A mismatched pair is
never made readable.

For Local exact-base recovery, `PrepareLocalRestore` writes only a durable
journal bound to `StoreUUID`, config digest/ID, index, root/prefix/state hashes,
and source file digests. `OpenLocalFromBase` resumes that exact journal,
installs/validates matching SQL+graph tips, reconciles declared local indexes,
and leaves phase `installed`. `FinalizeLocalRestore` removes old/staged files
and journal before any WAL suffix replay. Floor-zero startup with pending or
unexplained restore artifacts fails and preserves evidence.

Source: [local_restore.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/local_restore.go).

## Known replay boundary

The materializer owns local application, receipts, and pair consistency. It
does not establish decision certification, admission-time request validation,
or archive recovery. The replay caller must provide ordered certified decisions
and must resolve external recovery authority before calling Local restore APIs.
