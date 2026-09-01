# SQL async decision-sync fix

## Verdict

The async collapse is fixed. A remote proposal response already contains a
quorum-certified decision, but `acceptFrom` previously tried to synchronize
through that slot before installing the returned decision. Under sustained
async load this turned normal writes into redundant decision-sync traffic and
eventually produced 993 HTTP 503 responses.

`acceptFrom` now validates and installs the returned certified decision first.
Catch-up remains as the fallback for a real earlier-slot gap. The final change
is three production lines plus one regression test; experimental batching and
admission changes were rejected and reverted.

## Final measurements

Each run used a fresh three-voter `emptyDir` cluster, concurrency 16, a
500-write warm-up, 5,000 measured single-row HTTP SQL inserts, checkpointing
disabled, and a 10-minute object-sync interval. The image was
`rhiza-e2e:latticedb-0.2.1-async-fix`.

| Mode | Run | Success | Errors | ops/s | p50 | p95 |
|---|---:|---:|---:|---:|---:|---:|
| async | 1 | 5,000 | 0 | 130.22 | 91.23 ms | 301.11 ms |
| async | 2 | 5,000 | 0 | 173.31 | 57.50 ms | 302.18 ms |
| async | 3 | 5,000 | 0 | 141.61 | 46.81 ms | 471.03 ms |
| **async median** | | **5,000** | **0** | **141.61** | **57.50 ms** | **302.18 ms** |
| before-ACK | 1 | 5,000 | 0 | 78.57 | 152.01 ms | 502.27 ms |
| before-ACK | 2 | 5,000 | 0 | 169.99 | 55.69 ms | 273.44 ms |
| before-ACK | 3 | 5,000 | 0 | 216.66 | 53.20 ms | 188.14 ms |
| **before-ACK median** | | **5,000** | **0** | **169.99** | **55.69 ms** | **273.44 ms** |

The broken async probe completed only 4,007/5,000 writes at 14.75 successful
ops/s. The fixed async median is 9.6 times higher and all 15,000 measured writes
succeeded. The repeated load-time sync/deadline/QUIC error storm disappeared;
remaining isolated sync messages occurred during cluster bootstrap.

before-ACK is not slower in this local setup: its median is 20.0% above async.
The MinIO durability barrier is local and the pipeline hides much of its
latency, while run-to-run variance remains large. The data therefore does not
support a claim that async should be several times faster here.

An in-node client bypassing `kubectl port-forward` measured 150.21 ops/s async
and 190.40 ops/s before-ACK. The direct path is only modestly different from the
port-forward medians, so port-forward is not the main throughput ceiling.

## Hiqlite boundary

Hiqlite's official leader-local Rust client benchmark measured 9,340 single
inserts/s on this host. The fixed Rhiza HTTP-over-port-forward median is still
66.0 times lower. This is not an equivalent client path, but the gap is too
large to describe as comparable performance.

Closing that gap requires a separate consensus throughput project: eliminate
per-decision durable/completion round trips where the safety proof permits it,
disseminate every winning decision to the ingress voter without reactive
catch-up, and benchmark an equivalent direct client path. Increasing peer
admission or changing batch linger without that protocol work was unstable and
was not retained.

The existing serial three-peer core benchmark measured 24.2–73.6 ms per
proposal across three samples. Combined with the default 5 ms hedge delay, this
shows the next investigation boundary: normal durable proposals live long
enough to start fallback proposers. A 100 ms hedge experiment was invalidated
when the Dory k3s container exited during an in-node load run, so no hedge
default was changed from that incomplete evidence.

## Verification and evidence

- `go test ./... -count=1`
- `go test -race ./pkg/network -count=1`
- `go vet ./...`
- `long/async-{5000,repeat2-5000,repeat3-5000}.json`
- `long/before-ack-{5000,repeat2-5000,repeat3-5000}.json`
- `long/{async,before-ack}-direct-5000.json`
- Rejected experiments are retained as `long/async-{batch,peer8,peer8-final,batch2}-*.json`.
- Original failure evidence: `../2026-09-01-sql-async-diagnosis/REPORT.md`
- Hiqlite comparison: `../2026-09-01-hiqlite-local-comparison/REPORT.md`

The Dory SQL StatefulSet was left healthy in `before-ack` mode after the final
probe.
