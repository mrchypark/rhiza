# Hiqlite v0.15.0 reference refresh

The [CI measurement](https://github.com/mrchypark/rhiza/actions/runs/36226289455)
used Hiqlite commit `cc0c64c9e2369ad9bd8ee5aea8e6825e3d481249`, Rust 1.97.1,
`ubuntu24-20260920.314.1`, 100,000 SQL inserts and concurrency 16 in each
scenario. The patch digest is recorded in `../../hiqlite-reference.json`.

| Scenario | inserts/s |
| --- | ---: |
| 200 ms WAL sync, local client | 14,088 |
| Immediate WAL sync, local client | 1,632 |
| Immediate WAL sync, remote client | 1,471 |
| Immediate WAL sync, follower stopped before load | 1,739 |
| Immediate WAL sync, leader stopped before load | 1,739 |

The five raw logs and runner environment are retained as compressed text in
[`evidence/`](evidence/), with SHA-256 digests in
[`hiqlite-reference.json`](../../hiqlite-reference.json). The CI artifact also
contains the original logs. `bash benchmarks/verify-hiqlite-reference.sh`
checks the reference fields, digests, and parsed throughput against the retained
logs. Each process completed its 100,000
inserts successfully. The upstream split-brain check logs self-signed certificate
verification errors during the healthy remote run; the stopped-peer runs also
log expected unreachable-peer replication errors. These numbers are diagnostic
references, not a claim of error-free Hiqlite operation or a direct Rhiza speed
ratio. The stopped-peer cases measure post-election steady state, not the
interruption during failure.
