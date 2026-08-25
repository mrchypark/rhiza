# rhiza

Rhiza is an embedded, leaderless Go database with SQL/KV or Graph/KV builds.
Any healthy peer can accept a write; QuePaxa records a certified decision on a
quorum before the API acknowledges it. An HTTP server is an optional adapter
over the same Go API.

## Runtime

- Go 1.27.0; `GOTOOLCHAIN=auto` is expected.
- Green Tea GC is the Go 1.27 default.
- The container enables `GOEXPERIMENT=arenas` for QLog read scratch buffers.
- The default `sql-kv` image uses cgo-free `ncruces/go-sqlite3`.
- The separate `graph-kv` image uses the pure-Go GoraphDB fork.

```bash
go test ./...
go vet ./...
GOEXPERIMENT=arenas go test ./...
go build ./cmd/rhiza
CGO_ENABLED=0 GOEXPERIMENT=arenas,greenteagc go build -tags=graph ./cmd/rhiza
docker build -t rhiza-sql-kv:dev .
docker build -f Dockerfile.graph -t rhiza-graph-kv:dev .
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

- `POST /v1/sql/execute`: one statement, arguments, and optional returned rows.
- `POST /v1/sql/transaction`: an atomic `statements` array.
- `POST /v1/sql/query`: arguments plus `local` or `linearizable` consistency.
- `POST /v1/graph/execute`: one idempotent Cypher mutation with named arguments.
- `POST /v1/graph/query`: read-only Cypher with `local` or `linearizable` consistency.
- `POST /v1/kv/put`, `/get`, `/delete`, `/cas`: binary values, TTL, and CAS.
- `POST /v1/notify/publish`: replicated notification publication.
- `GET /v1/notify/subscribe?topic=...`: bounded, live, at-most-once SSE stream.

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
`rhiza-peer/1` ALPN and FlatBuffers messages. Each RPC uses an independent
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

SQLite and GoraphDB are derived from the certified QLog. Startup replays missing decisions;
an unreadable SQLite database is quarantined and rebuilt from the log. SQLite
snapshots use `VACUUM INTO`, are integrity-checked before atomic restore, and
checkpoint uploads consume those consistent bytes rather than the live file.
QLog compaction and remote object-store bootstrap are not yet enabled.

## Local Kubernetes qualification

```bash
dory build -t rhiza-sql-kv-e2e:dev .
dory save rhiza-sql-kv-e2e:dev | dory exec -i dory-k8s ctr -n k8s.io images import -
dory k8s apply -f deploy/k8s/sql-server-3peer-e2e.yaml
bash e2e/chaos/install-dory.sh
bash e2e/chaos/run-three-peer-dory.sh
```

On the local dory cluster on 2026-08-24, the QUIC/FlatBuffers Chaos Mesh scenario
passed: one failed peer kept quorum writes available (scenario sample 31.2 ms),
the rebuilt peer converged, two failed peers rejected writes with 503, and
writes resumed after quorum recovery. Normal three-peer SQL benchmark medians
were 0.207 ms local read, 2.67 ms linearizable read, and 1.24 ms write. With one
failed peer they were 0.245 ms, 1.98 ms, and 9.10 ms respectively. These include
local port-forward overhead and showed substantial tail variance.

For Graph/KV qualification, build and import `rhiza-graph-kv-e2e:dev`, then
apply `deploy/k8s/graph-server-3peer-e2e.yaml`. Set `RHIZA_GRAPH_E2E_URL` to a
forwarded peer and run `go test ./e2e -run TestGraphServer`. The same Dory
qualification passed with a 15.2 ms one-peer-failure write, HTTP 503 with two
failed peers, convergence, and a successful write after quorum recovery. Three-peer
samples were 0.29–0.35 ms local read, 4.7–21.6 ms linearizable read, and
8.1–11.6 ms graph write, including port-forward overhead.

The build profile is fixed into each binary and mismatched
`RHIZA_EXECUTION_PROFILE` values are rejected at startup. Graph mutations and
their request IDs commit atomically in GoraphDB before the SQLite sidecar tip,
so a crash replays without applying the graph mutation twice. GoraphDB is
rebuilt from the full QLog when local state is missing; graph checkpoints bundle
the SQLite and GoraphDB materializations.

## License

MIT
