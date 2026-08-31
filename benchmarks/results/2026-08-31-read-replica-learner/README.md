# Non-voting read copy comparison

Host: Apple M3, darwin/arm64, Go benchmark, local filesystem object store.

Command:

```bash
go test -run '^$' -bench '^BenchmarkReplicaCatchUp$' -benchtime=50x -count=3 -benchmem .
```

Each measured operation publishes one SQL decision outside the timer, then
measures one follower catch-up and materialization pass.

| source | ns/op (3 runs) | median | object GET/op | object HEAD/op |
|---|---:|---:|---:|---:|
| object-store | 3,864,815 / 3,520,864 / 3,457,612 | 3.521 ms | 3 | 3 |
| peer learner | 3,632,577 / 4,351,266 / 3,644,973 | 3.645 ms | 0 | 0 |

On this same-host filesystem setup, latency is effectively tied (learner
median +3.5%) while the learner removes six object-store metadata/read calls
per sync. Remote S3 latency was not measured, so no remote-latency claim is
made. `TestElevenObjectStoreReadReplicas` also passed with eleven independent
non-voting copies reading the same certified archive.
