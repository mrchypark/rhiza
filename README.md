# rhiza

Rhiza is an embedded, leaderless Go database with SQL, Graph, and KV.
Any healthy peer can accept a write; QuePaxa records a certified decision on a
quorum before the API acknowledges it. An HTTP server is an optional adapter
over the same Go API.

## Runtime

- Go 1.27.0; `GOTOOLCHAIN=auto` is expected.
- Green Tea GC is the Go 1.27 default.
- The container enables `GOEXPERIMENT=arenas` for QLog read scratch buffers.
- SQLite uses cgo-free `ncruces/go-sqlite3`.
- Graph uses the pure-Go `latticedb-go` engine.

```bash
go test ./...
go vet ./...
GOEXPERIMENT=arenas go test ./...
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

## Optional HTTP API

All mutations require a unique `request_id` and are idempotent. JSON request
bodies are limited to 1 MiB. SQL is limited to 256 KiB, 999 arguments, 64
statements per transaction, and 10,000 returned rows.

- `POST /sql/execute`: one statement, arguments, and optional returned rows.
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
Read-only statements may also appear in a replicated transaction when their
rows are needed by setting `want_rows`; raw unprepared SQL batches are omitted
because the prepared `statements` array covers the same database features.

## Peer transport

Peer consensus and catch-up traffic uses raw QUIC over UDP 9090 with a private
`rhiza-peer` ALPN and FlatBuffers messages. Each RPC uses an independent
bidirectional QUIC stream on a reused connection. Frames are capped at 1 MiB;
connections use TLS 1.3, keepalive, bounded stream counts, five-second RPC
deadlines, and reconnect after transport failure.

Replay-safe `Record`, certified `Learned`, and read-only `Decisions` operations
may use QUIC 0-RTT after session resumption. `Propose` waits for the handshake
because replay before a decision could consume duplicate consensus slots.
Peer tokens are checked against fixed membership when configured. The public
HTTP API remains TCP 8080 and contains no registered internal consensus routes.

## Reads and failures

Local reads use the peer's applied state and remain available without a
quorum. Linearizable reads decide a unique read barrier and return HTTP 503 if
a quorum is unavailable; they never fall back to a stale read. With three
peers, one failed peer preserves reads and writes. Two failed peers preserve
only local reads and reject writes and linearizable reads with HTTP 503.

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
