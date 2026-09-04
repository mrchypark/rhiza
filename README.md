# Rhiza

Rhiza is an embedded-first, leaderless Go database with SQL, Graph, KV, and
notification APIs. Any healthy voter can accept a write. QuePaxa records a
certified decision on a quorum before Rhiza acknowledges it, while local reads
remain available without a quorum. The HTTP server is an optional adapter over
the same Go API.

## Highlights

- One cgo-free runtime for SQLite, Graph, KV, and notifications.
- Fixed-membership, leaderless replication over QUIC.
- Local and linearizable read consistency.
- Idempotent mutations with bounded receipts and request-status lookup.
- Certified object-store archives and checkpoints.
- Non-voting object-store replicas and low-lag learners.
- Embedded Go API first; HTTP and the `rhiza` server binary are optional.

## Requirements and installation

Rhiza requires Go 1.27 or newer. `GOTOOLCHAIN=auto` is expected. SQLite uses
[`ncruces/go-sqlite3`](https://github.com/ncruces/go-sqlite3), and Graph uses
[`latticedb-go`](https://github.com/mrchypark/latticedb-go), both without cgo.

```bash
go get github.com/mrchypark/rhiza
```

## Quick start

`Open` starts the embedded engine and its private peer endpoint. It does not
open a public HTTP listener.

```go
db, err := rhiza.Open(ctx, rhiza.Config{
    NodeID:  "node-1",
    DataDir: "./rhiza-data",
})
if err != nil {
    return err
}
defer db.Close()

_, err = db.Execute(ctx, rhiza.ExecuteRequest{
    RequestID: "schema-1",
    SQL:       "CREATE TABLE tea (id INTEGER PRIMARY KEY, name TEXT)",
})
if err != nil {
    return err
}

rows, err := db.Query(ctx, rhiza.QueryRequest{
    SQL:         "SELECT id, name FROM tea",
    Consistency: rhiza.ConsistencyLocal,
})
if err != nil {
    return err
}
```

SQL, Graph, KV, notification, stream, and request-status methods are available
directly on `DB`. Use `db.Handler()` or `db` itself as an `http.Handler` when
the HTTP adapter is needed.

## Consistency and failure model

Local reads use the node's applied state. Linearizable reads first decide a
unique read barrier and return `ErrQuorumUnavailable` when a quorum cannot be
reached; they never fall back to stale data.

With three voters, one failed voter preserves writes and linearizable reads.
Two failed voters preserve local reads only. `DB.Ready()` means local recovery
and startup catch-up completed; it is not a live quorum probe. Use an
inexpensive linearizable query when current quorum readiness matters.

All mutations require a unique `request_id`. During the idempotency window,
retrying the same request with the same payload returns its retained result;
reusing the ID with different intent returns `ErrRequestConflict`.
`ErrCommitUnknown` means the caller must inspect the request status rather than
submit a different operation under the same ID.
Use the `RequestKind*`, `RequestState*`, and `MutationCommitted`/
`MutationRejected` constants instead of comparing wire strings directly.

## Data APIs

### SQL

Rhiza exposes SQLite's DDL, views, triggers, generated and STRICT tables,
partial and expression indexes, CTEs and recursive CTEs, joins, subqueries,
UPSERT, `RETURNING`, window functions, JSON functions, and FTS5.

Replicated execution rejects explicit transaction control, attachment, and
known nondeterministic functions. Use `Execute` for one mutation,
`ExecuteReturning` for bounded mutation rows, `ExecuteReturningOne` when the
mutation must produce exactly one row, or a prepared `Statements` array for an
atomic multi-statement transaction. Generic `ExecuteReturningMap` helpers map
rows through a typed Go callback; `SQLRow` exposes read-only `Len`, `Columns`,
`Values`, `Value`, and `Named` accessors. SQL values are `nil`, `int64`,
`float64`, `string`, or `[]byte`, and replicated BLOB arguments retain their
Go type. A later statement can bind a value
from the exactly one row of an earlier `WantRows` statement through
`OutputRefs`, by a unique column name or index. Reference targets use plain `?`
parameters and must have a matching `null` entry in `Args`. Returned rows and
idempotency receipts are retained together, so retrying the same request ID
returns the original result without re-executing the mutation.

Each statement can require an exact result with `ExpectedRowsAffected` for a
non-row-returning statement or `ExpectedReturnedRows` for a `WantRows`
statement. A mismatch rolls back the whole transaction and returns a rejected
receipt with `MutationErrorCodePreconditionFailed`. Use these checks with
version predicates and database constraints to keep validation and mutation in
the same replicated transaction.

`DB.Migrate` applies named, contiguous migration versions starting at 1 through
the same replicated transaction path. Reapplying an identical migration list
is a no-op; changing an already-applied version or leaving a gap is an error.
Migration statements are SQL-only (no arguments, returned rows, or output references). The private
`_rhiza_migrations` ledger and each migration are committed atomically, and the
reserved `_rhiza_` namespace is inaccessible through public SQL APIs.

### Graph and Cypher

Rhiza uses `latticedb-go v0.3.0` and exposes its deliberately small,
case-sensitive Cypher subset. This is not full openCypher. Structural keywords
must be uppercase.

Supported query building blocks include:

- `MATCH` with fixed-length incoming, outgoing, or undirected patterns.
- `WHERE` with comparisons, `IN`, `STARTS WITH`, `ENDS WITH`, `CONTAINS`,
  `IS NULL`, `IS NOT NULL`, `AND`, `OR`, and `NOT`.
- `RETURN`, `DISTINCT`, `count`, `ORDER BY`, `SKIP`, and `LIMIT`.
- Standalone `CREATE` of one node.
- `MATCH ... SET`, `MATCH ... CREATE` for relationships, `REMOVE`, `DELETE`,
  and `DETACH DELETE`.
- `UNWIND ... RETURN`, `UNWIND ... CREATE`, and `UNWIND ... MATCH`.
- Full-text `@@` predicates, subject to the engine's ranking restrictions.

`OPTIONAL MATCH`, `MERGE`, `WITH`, `UNION`, variable-length paths, list
literals, and backtick identifiers are not supported. A standalone relationship
creation such as `CREATE (:Person)-[:KNOWS]->(:Person)` is also unsupported;
create or match the nodes first, then use `MATCH ... CREATE`. Named parameters
accept JSON-compatible null, boolean, string, number, list, and map values.

The dependency grammar also includes vector-distance `<=>`, but Rhiza does not
currently configure LatticeDB vector mode or expose typed vector parameters, so
vector search is not part of Rhiza's supported Graph surface.

Use `GraphQuery` or `POST /graph/query` for read-only `MATCH ... RETURN` and
`UNWIND ... RETURN` queries. Use `GraphExecute` or `POST /graph/execute` for
replicated mutations. Mutation statements are applied atomically with their
request receipt and optional stream events.

The embedded `GraphReachable` API performs a bounded outgoing traversal on one
immutable local graph snapshot. Callers must provide depth, result, scanned-edge,
and encoded-byte limits. It reports `StartFound`, `AppliedSlot`, and
`ConsensusTip`, supports an optional `RequireAppliedSlot` precondition, and
orders results by distance then internal node ID. A limit failure returns
`ErrGraphResourceLimit` without partial nodes.

The exported `MaxGraphReachable*` constants expose the library ceilings.

Declare lookup indexes with `Config.LocalGraphNodePropertyIndexes`. These
indexes are node-local derived state: they are not replicated, and Rhiza
reconciles them when the node opens or installs a checkpoint.

The dependency owns the complete language contract. See the version-pinned
[`Supported Cypher Subset`](https://github.com/mrchypark/latticedb-go/blob/v0.3.0/docs/engine_conformance.md#supported-cypher-subset)
and [canonical EBNF grammar](https://github.com/mrchypark/latticedb-go/blob/v0.3.0/internal/engine/testdata/query_grammar.ebnf).

### Graph streams

`GraphChanges` reads the node-local semantic changefeed. Named graph streams
can be published atomically with a graph mutation, read by sequence, trimmed,
and tracked with replicated durable consumer offsets.

### KV and notifications

The KV API supports get, put, delete, compare-and-swap, and TTL. Notification
publication is replicated; subscriptions are bounded, live, at-most-once
streams. Slow subscribers may drop notifications, so use graph streams when a
durable cursor is required.

## Limits

- HTTP JSON bodies: 1 MiB.
- Canonically encoded consensus mutations: 128 KiB.
- SQL or Cypher text: 256 KiB.
- SQL/Cypher arguments: 999.
- Statements in one SQL transaction: 64.
- SQL query or `RETURNING` result rows: 10,000.
- SQL query result size: 16 MiB total; replicated `RETURNING` result size: 1
  MiB total. Each cell is limited to 1 MiB.
- Request IDs: 64 bytes.

An HTTP request below 1 MiB may still exceed the encoded consensus limit.
Embedded SQL callers can preflight the exact mutation with
`rhiza.ValidateExecuteRequest` and inspect `rhiza.MaxReplicatedMutationBytes`.

## HTTP adapter

The optional adapter exposes:

- SQL: `POST /sql/execute`, `/sql/execute-returning`,
  `/sql/execute-returning-one`, `/sql/transaction`, `/sql/query`.
- Graph: `POST /graph/execute`, `/graph/query`, `/graph/changes`.
- Graph streams: `POST /graph/streams/read`, `POST /graph/streams/offset`,
  `PUT /graph/streams/offset`, and `POST /graph/streams/trim`.
- KV: `POST /kv/put`, `/kv/get`, `/kv/delete`, `/kv/cas`.
- Notifications: `POST /notify/publish`, `GET /notify/subscribe`.
- Operations: `POST /request/status`, `GET /metrics/object-store`,
  `GET /replica/status`.
- Health: `GET /ready`, `GET /healthz`.

```bash
curl --fail-with-body http://127.0.0.1:8080/sql/query \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT 1","consistency":"local"}'
```

Errors use one JSON envelope: `{"code":"invalid_request","error":"..."}`.
Stable codes include `invalid_request`, `request_conflict`, `not_ready`,
`overloaded`, `durability_unavailable`, `commit_unknown`, and
`quorum_unavailable`. A `commit_unknown` response also carries `request_id`,
`slot`, and `retry_through_slot`. Unsupported methods return an `Allow` header.
Go clients can decode `rhiza.HTTPErrorResponse` and compare its code with the
`rhiza.HTTPErrorCode*` constants.
SQL BLOB parameters use `{"$rhiza_blob":"<base64>"}` in HTTP JSON; returned
BLOB values are base64 JSON strings.

Query endpoints accept `local` or `linearizable` consistency. Read replicas
serve the read routes but return HTTP 503 for mutations. The HTTP adapter has no
built-in client authentication and must not be exposed directly to an
untrusted network.

## Non-voting read replicas

Read replicas serve the SQL, Graph, KV, graph-stream, request-status, and HTTP
read APIs, but never propose, vote, acknowledge decisions, or participate in
quorum and read-index calculations. Linearizable reads return
`ErrQuorumUnavailable`; use `Status().LagSlots` to observe bounded staleness.
The HTTP status response also includes `lag_slots`.

| Mode | Source | Tradeoff |
| --- | --- | --- |
| `object-store` | Certified checkpoints and archives | Best for broad fan-out; adds object-store polling latency and requests. |
| `learner` | Voter decision streams, then object storage | Lower lag; adds one read stream per polling learner. |

```go
replica, err := rhiza.OpenReadReplica(ctx, rhiza.ReplicaConfig{
    ClusterID:        "prod",
    ReplicaID:        "read-1",
    DataDir:          "./read-1",
    Members:          []rhiza.ReplicaMember{{ID: "n1"}},
    ObjStoreProvider: rhiza.ObjectStoreProviderS3,
    ObjStoreBucket:   "rhiza",
})
```

Both modes require shared object storage for cold start and recovery. Learners
authenticate the read-only `Sync` RPC with the voter `AdminToken`, but must not
receive voter tokens. Build token-free pinned peer identities with
`rhiza.NewReplicaMember(clusterID, voter)`.

`GET /replica/status` reports the mode, applied slot, observed source tip, lag,
source, last sync time, and last error. Replica `Ready` means local recovery
completed, not that the copy is current.

## Storage, durability, and recovery

The certified QLog is the source of truth. SQLite and LatticeDB are rebuildable
materialized state. Startup replays missing decisions; unreadable local state is
quarantined and rebuilt. Checkpoints capture both engines at the same applied
slot and restore them together.

Single-voter deployments may use local filesystem storage. Multi-voter
clusters require shared S3-compatible, GCS, or Azure Blob storage. Read
replicas must reach the same published namespace; their filesystem provider is
useful only when that directory is shared or mounted. Object-store durability
modes are:

- `async`: acknowledge after quorum certification and publish in the
  background.
- `before-ack`: wait for durable object-store publication before acknowledging.

S3 supports custom endpoints and insecure HTTP for local-compatible stores.
GCS uses service-account JSON. Azure supports its standard credentials and
custom endpoint, but GCS endpoint overrides and insecure GCS/Azure transports
are rejected rather than ignored.

## Server binary and deployment

`go run ./cmd/rhiza` starts the optional HTTP server. `RHIZA_ROLE` selects the
runtime:

- `voter` (default): voting read/write node.
- `object-store`: non-voting object-store replica.
- `learner`: non-voting peer-first learner.

Common settings are:

| Variable | Default | Purpose |
| --- | --- | --- |
| `RHIZA_ROLE` | `voter` | `voter`, `object-store`, or `learner` |
| `RHIZA_CLUSTER_ID` | `cluster-a` | Stable cluster identity |
| `RHIZA_NODE_ID` | `node-1` | Unique voter or replica identity |
| `RHIZA_DATA_DIR` | `./rhiza-data` | Durable local state |
| `RHIZA_BIND_ADDR` | `127.0.0.1:8080` | HTTP listen address |
| `RHIZA_PEER_ADDR` | `127.0.0.1:9090` | Voter QUIC listen address |
| `RHIZA_CLUSTER_MEMBERS` | empty | JSON fixed voter membership |
| `RHIZA_REPLICA_MEMBERS` | empty | Learner JSON pinned voter identities |
| `RHIZA_REPLICA_SYNC_INTERVAL` | `0s` | Replica polling interval; zero selects the engine default |
| `RHIZA_ADMIN_TOKEN` | empty | Learner-to-voter sync credential |
| `RHIZA_OBJSTORE_PROVIDER` | empty | `filesystem`, `s3`, `gcs`, or `azure` |
| `RHIZA_OBJSTORE_DIR` | empty | Filesystem provider directory |

Cloud credentials and tuning use the remaining `RHIZA_OBJSTORE_*` variables.
`RHIZA_FILESYSTEM_DIR` remains an alias for `RHIZA_OBJSTORE_DIR`; conflicting
values are rejected. Invalid boolean and duration values fail startup instead
of silently changing behavior.

```bash
docker build -t rhiza:dev .
docker run --rm --name rhiza -p 8080:8080 \
  -e RHIZA_BIND_ADDR=0.0.0.0:8080 \
  -v rhiza-data:/data \
  rhiza:dev
```

Kubernetes examples live under [`deploy/k8s`](deploy/k8s), including three-peer
SQL and Graph qualification manifests and both read-replica modes.

## Peer security

Peer traffic uses QUIC over UDP with TLS 1.3, pinned identities, bounded frames,
and voter-specific membership tokens. The admin token must differ from every
voter token. Learners receive only the admin token and token-free pinned voter
identities.

This is server authentication and token authorization, not peer mTLS. Keep peer
UDP and the unauthenticated HTTP adapter on private networks, restrict them with
firewalls or Kubernetes NetworkPolicy, and add authentication or mTLS at the
deployment boundary when private-network trust is insufficient.

## Development and qualification

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/rhiza
```

For local Kubernetes qualification, preload `rhiza-e2e:dev`, apply one of the
three-peer manifests under `deploy/k8s`, and run the matching tests under
[`e2e`](e2e). Chaos Mesh scenarios live in [`e2e/chaos`](e2e/chaos).

Benchmark instructions and durable result artifacts live under
[`benchmarks`](benchmarks). Performance claims belong with the exact code,
environment, and raw measurements that produced them rather than in this
README. The last archived LatticeDB v0.2.1 qualification is attached in
[`2026-09-01-latticedb-0.2.1-current`](benchmarks/results/2026-09-01-latticedb-0.2.1-current/REPORT.md).
The same-host reproduction of Hiqlite's official three-node benchmark and its
comparison boundaries are in
[`2026-09-01-hiqlite-local-comparison`](benchmarks/results/2026-09-01-hiqlite-local-comparison/REPORT.md).

## License

[MIT](LICENSE)
