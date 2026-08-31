# rhiza

Rhiza is an embedded, leaderless Go database with SQL, Graph, and KV.
Any healthy peer can accept a write; QuePaxa records a certified decision on a
quorum before the API acknowledges it. An HTTP server is an optional adapter
over the same Go API.

## Runtime

- Go 1.27.0; `GOTOOLCHAIN=auto` is expected.
- Green Tea GC is the Go 1.27 default.
- SQLite uses cgo-free `ncruces/go-sqlite3`.
- Graph uses the pure-Go `latticedb-go` engine.

```bash
go test ./...
go vet ./...
go build ./cmd/rhiza
docker build -t rhiza:dev .
```

## Embedded Go API

```go
db, err := rhiza.Open(ctx, rhiza.Config{
    NodeID: "node-1",
    DataDir: "./rhiza-data",
})
if err != nil { return err }
defer db.Close()

_, err = db.Execute(ctx, rhiza.ExecuteRequest{
    RequestID: "schema-1",
    SQL: "CREATE TABLE tea (id INTEGER PRIMARY KEY, name TEXT)",
})
rows, err := db.Query(ctx, rhiza.QueryRequest{
    SQL: "SELECT id, name FROM tea",
    Consistency: rhiza.ConsistencyLocal,
})
```

`Open` starts the embedded engine and its private peer endpoint, but no public
HTTP listener. Use `db.Handler()` or `db` itself as an `http.Handler` when a
server endpoint is wanted. SQL, KV, Graph, and Notify methods are available
directly on `DB`.

## Non-voting read copies

Read copies use the same SQL, Graph, KV, checkpoint, and HTTP read APIs as a
voter, but never propose, vote, acknowledge decisions, or enter quorum/read
index calculations. Their `linearizable` reads fail with
`ErrQuorumUnavailable`; use local reads and inspect `Status().AppliedSlot` and
`SourceTip` when bounding staleness.

```go
replica, err := rhiza.OpenReadReplica(ctx, rhiza.ReplicaConfig{
    ClusterID: "prod", ReplicaID: "read-1", DataDir: "./read-1",
    Members: []rhiza.ReplicaMember{{ID: "n1"}}, // read-1 is not a voter
    ObjStoreProvider: "s3", ObjStoreBucket: "rhiza",
})
```

`OpenReadReplica` polls certified checkpoint/archive state and is the default
for broad fan-out (10+ copies): it adds no voter traffic or membership entries,
at the cost of object-store polling latency and requests. `OpenLearner` first
pulls certified decisions over the private peer QUIC endpoint, then falls back
to object storage after compaction or peer loss. It has lower lag but adds one
read stream per polling learner. Learners authenticate read-only `Sync` with
the voter `AdminToken`; provision token-free peer identities with
`rhiza.NewReplicaMember(clusterID, voter)`. Non-members cannot call other peer RPCs. Both modes
require shared object storage for cold start and checkpoint recovery. Tune
`SyncInterval` for the desired freshness/cost tradeoff (defaults: one second
for object-store replicas, 100 ms for learners).

The optional handler on either type registers the normal routes, but all
mutation routes return 503. `Ready` means that local recovery completed, not
that the copy is current or that voter quorum is available.

## Optional HTTP API

All mutations require a unique `request_id` and are idempotent. The optional
HTTP adapter accepts JSON bodies up to 1 MiB, while every canonically encoded
consensus mutation must fit within 128 KiB. An HTTP request below 1 MiB can
therefore still be rejected by the consensus limit. Embedded SQL callers can
preflight the exact mutation contract with `rhiza.ValidateExecuteRequest` and
inspect `rhiza.MaxReplicatedMutationBytes`; neither limit should be raised
without evaluating consensus latency and memory. SQL text is limited to 256
KiB, with at most 999 arguments and 64 statements per transaction. Queries
return at most 10,000 rows.

- `POST /sql/execute`: one mutation statement and arguments.
- `POST /sql/transaction`: an atomic `statements` array.
- `POST /sql/query`: arguments plus `local` or `linearizable` consistency.
- `POST /graph/execute`: one idempotent Cypher mutation with named arguments.
- `POST /graph/query`: read-only Cypher with `local` or `linearizable` consistency.
- `POST /kv/put`, `/get`, `/delete`, `/cas`: binary values, TTL, and CAS.
- `POST /notify/publish`: replicated notification publication.
- `GET /notify/subscribe?topic=...`: bounded, live, at-most-once SSE stream.

Replicated SQL rejects explicit transaction control, attachment, and known
nondeterministic functions. Multi-statement client transactions use the
transaction endpoint.

The SQL surface is SQLite's: DDL, views, triggers, generated and STRICT tables,
partial and expression indexes, CTEs and recursive CTEs, joins, subqueries,
UPSERT, RETURNING, window functions, JSON functions, and FTS5 are supported.
Replicated `Execute` and transaction calls never expose statement rows:
`want_rows` is rejected and their idempotent response is one bounded aggregate
`MutationReceipt`. Use `Query` for read-only SQL and observe committed state
with `linearizable` consistency. Raw unprepared SQL batches are omitted because
the prepared `statements` array covers the same database features.

## Peer transport

Peer consensus and catch-up traffic uses raw QUIC over UDP 9090 with a private
`rhiza-peer` ALPN and FlatBuffers messages. Each RPC uses an independent
bidirectional QUIC stream on a reused connection. Frames are capped at 1 MiB;
connections use TLS 1.3, keepalive, bounded stream counts, five-second RPC
deadlines, and reconnect after transport failure.

Replay-safe `Record`, certified `Learned`, and read-only `Decisions` operations
may use QUIC 0-RTT after session resumption. `Propose` waits for the handshake
because replay before a decision could consume duplicate consensus slots.
Multi-node members require voter-specific tokens, and the admin token must not
equal any voter token. Credentials are checked against fixed membership. This is
server authentication and membership-token authorization, not peer mTLS.
Deploy peer UDP and the optional HTTP adapter only on a private network, limit
them with firewall or Kubernetes NetworkPolicy, keep membership tokens secret,
and do not expose the unauthenticated HTTP adapter publicly. Add peer mTLS at a
deployment boundary where private-network and token trust are insufficient.
The public HTTP API remains TCP 8080 and contains no registered internal
consensus routes.

## Reads and failures

Local reads use the peer's applied state and remain available without a
quorum. Linearizable reads decide a unique read barrier and return HTTP 503 if
a quorum is unavailable; they never fall back to a stale read. With three
peers, one failed peer preserves reads and writes. Two failed peers preserve
only local reads and reject writes and linearizable reads with HTTP 503.

For embedded health checks, `DB.Ready()` means local recovery and startup
catch-up completed; it is not a live quorum signal, so an isolated peer may
remain locally ready. Use an inexpensive `linearizable` query when current
quorum readiness is required. Mutations and linearizable reads always enforce
quorum at operation time and fail closed.

SQLite and LatticeDB are derived from the certified QLog. Startup replays
missing decisions; unreadable local state is quarantined and rebuilt from the
log. Checkpoints capture both engines at the same applied slot and restore the
fixed SQLite and Graph files atomically.

## Docker quick start

```bash
docker build -t rhiza:dev -t rhiza-e2e:dev .
docker run --rm --name rhiza -p 8080:8080 \
  -e RHIZA_BIND_ADDR=0.0.0.0:8080 \
  -v rhiza-data:/data \
  rhiza:dev
```

For Kubernetes qualification, preload `rhiza-e2e:dev` into every node or
replace the manifest image references with published registry images. Then
apply `deploy/k8s/sql-server-3peer-e2e.yaml` or
`deploy/k8s/graph-server-3peer-e2e.yaml` with standard `kubectl`. The Chaos Mesh
manifests under `e2e/chaos` work with any compatible Kubernetes environment.

On a local Kubernetes cluster on 2026-08-24, the QUIC/FlatBuffers Chaos Mesh
scenario passed: one failed peer kept quorum writes available (31.2 ms sample),
the rebuilt peer converged, two failed peers rejected writes with 503, and
writes resumed after quorum recovery. Normal three-peer SQL benchmark medians
were 0.207 ms local read, 2.67 ms linearizable read, and 1.24 ms write. With one
failed peer they were 0.245 ms, 1.98 ms, and 9.10 ms respectively. These include
local port-forward overhead and showed substantial tail variance.

For Graph qualification, preload `rhiza-e2e:dev`, then apply
`deploy/k8s/graph-server-3peer-e2e.yaml`. Set `RHIZA_GRAPH_E2E_URL` to a
forwarded peer and run `go test ./e2e -run TestGraphServer`. The same local
Kubernetes qualification passed with a 15.2 ms one-peer-failure write, HTTP 503
with two failed peers, convergence, and a successful write after quorum
recovery. Three-peer samples were 0.29–0.35 ms local read, 4.7–21.6 ms
linearizable read, and
8.1–11.6 ms graph write, including port-forward overhead.

Every binary and node includes SQL, Graph, and KV. Graph mutations, request
receipts, and the applied slot commit atomically in LatticeDB before the SQLite
sidecar tip, so a crash replays without applying a graph mutation twice.
LatticeDB is rebuilt from the QLog when local state is missing; checkpoints
always bundle the SQLite and LatticeDB materializations.

## License

MIT
