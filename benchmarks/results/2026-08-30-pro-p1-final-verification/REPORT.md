# Pro P1 final verification

## Verdict

- Correctness: PASS. Default/Graph full tests, focused race tests, vet, Docker image-internal tests, real Dory 3-peer SQL/Graph chaos, two-peer loss, checkpoint, and emptyDir object-store recovery all passed.
- Requests: 126,000 primary-matrix requests plus 50,400 same-host A/B requests; zero client errors, unexpected S3 4xx, HTTP 5xx, or transport failures.
- Recovery: SQL and Graph both recovered from shared object storage after all three peer data directories were removed.
- Performance: write medians and object-store call rates are within run-to-run batching variance. Read p50 shifts are small in absolute terms (all under 2.36 ms). Same-host CPU samples remain noisy and are reported without claiming a CPU win.

## Same-host final13 versus P1 candidate

| Image | Workload | final13 p50 | candidate p50 | final13 S3 HTTP / 300 writes | candidate S3 HTTP / 300 writes |
|---|---:|---:|---:|---:|---:|
| Graph | graph write | 18.067 ms | 15.031 ms | 75 | 55 |
| Graph | KV write | 21.275 ms | 19.015 ms | 65 | 75 |
| SQL | SQL write | 15.762 ms | 16.852 ms | 45 | 65 |
| SQL | KV write | 18.407 ms | 18.418 ms | 70 | 80 |

The S3 count changes are batching-boundary variation over short 100-write runs, not a protocol cardinality change. Every write sample remained error-free.

| Image | Workload | final13 p50 | candidate p50 |
|---|---:|---:|---:|
| Graph | graph linearizable read | 2.232 ms | 2.359 ms |
| Graph | graph local read | 1.693 ms | 1.798 ms |
| Graph | KV linearizable read | 1.856 ms | 2.064 ms |
| Graph | KV local read | 1.245 ms | 1.508 ms |
| SQL | SQL linearizable read | 1.812 ms | 1.881 ms |
| SQL | SQL local read | 1.274 ms | 1.258 ms |
| SQL | KV linearizable read | 1.790 ms | 1.971 ms |
| SQL | KV local read | 1.287 ms | 1.330 ms |

Resource snapshots over 12,600 requests per image:

| Image | final13 CPU | candidate CPU | final13 memory.current sum | candidate memory.current sum |
|---|---:|---:|---:|---:|
| Graph | 387.351 us/request | 462.276 us/request | 468,963,328 B | 403,464,192 B |
| SQL | 155.624 us/request | 205.003 us/request | 351,813,632 B | 326,692,864 B |

The candidate changes run only during checkpoint creation, GC, and recovery; checkpointing was disabled in this A/B slice. Therefore the CPU delta is not attributed to the patch without a longer interleaved run. Memory current improved in both images.

## Fault and recovery proof

- SQL: one-peer loss kept quorum writes available; two-peer loss returned 503 and resolved commit-unknown after quorum returned; all peers rebuilt from shared object storage.
- Graph: the same sequence passed with graph convergence and shared-object recovery.
- SQL one-peer quorum write: 18.203 ms.
- Graph one-peer quorum write: 25.666 ms.

## Files

- `summary.json`: workload aggregates and object-store API counts.
- `resources/`: cgroup CPU/memory and timing snapshots.
- `sql-chaos-e2e.log`, `graph-chaos-e2e.log`: fault and emptyDir recovery transcripts.
