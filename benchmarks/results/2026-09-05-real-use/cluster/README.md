# Local three-voter HTTP/QUIC validation

Built the current dirty worktree at `f6c01b19ea6957a4dbdfd5636a7fdb5ffaad2c35`
with `go1.27.0 darwin/arm64`. The runner used Docker `29.6.1 linux/arm64`,
three local Rhiza processes, and one dedicated local MinIO container.

`run-server-reference.sh` is the exact copied harness (SHA-256
`d185dcd0cb76a0e68ca4feed42a59c03412769502947ef0648e2678365de85a7`). It
used HTTP ports `18470`–`18472`, QUIC ports `19470`–`19472`, MinIO port
`19170`, a runner-owned temporary data directory, and an ephemeral MinIO
container. All were removed after each run.

Both runs used 5,000 single-row SQL writes at concurrency 16 against `n1`,
with quorum WAL sync, async object publication at one hour, and checkpointing
disabled. The harness verifies a linearizable final `COUNT(*) = 5000` and
counts application log lines containing `error`, `failed`, or `timeout`.

| run | writes | errors | successful ops/s | p50 | p95 | p99 | max | final count | runtime failure lines |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| healthy | 5,000 | 0 | 1,094.87 | 10.94 ms | 29.45 ms | 42.71 ms | 78.47 ms | 5,000 | 0 |
| SIGKILL `n0` after 1s, target `n1` | 5,000 | 0 | 1,353.34 | 7.83 ms | 28.53 ms | 43.13 ms | 66.98 ms | 5,000 | 0 |

These are a single local-machine validation, not a capacity or cross-version
comparison. The fault row passes the harness's 1,500 ms maximum-latency gate.
