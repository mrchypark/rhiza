# Issue 184 benchmark smoke results

These are local harness measurements, not evidence of a production bottleneck
or an optimization win. They validate that the bounded benchmark cases execute
and emit latency/Go allocation data. The CI workflow is the paired source of
comparison evidence: fixed `ubuntu-24.04`, `GOMAXPROCS=2`, ten interleaved
samples per revision.

Environment: Apple M1, Darwin arm64, Go 1.27.0, `GOMAXPROCS=2`,
`CGO_ENABLED=0`. The source commit was `ec516c4860e18f94ebab43b73b681c6b1221d070`
(`origin/main`); benchmark changes were uncommitted. Each command ran once on
the working tree; durability and SQL used a 100 ms calibration window, graph
pruning used one operation per matrix point.

| Measurement | Cases | Local result |
| --- | --- | --- |
| `EnsureDurableThrough` retained prefix | 1 / 64 / 1,024 slots | 39.61 / 1,859 / 36,082 ns/op; 0 B/op, 0 allocs/op |
| SQL authorizer catalog scan | 1 / 64 / 256 tables | 4,658 / 28,902 / 113,056 ns/op; 240 / 240 / 241 B/op; 9 / 9 / 9 allocs/op |
| Graph prune, gap=1, requests/slot=1 | 256 slots | 12,876,542 ns/op; 956,184 B/op; 9,773 allocs/op |
| Graph prune, gap=1, requests/slot=8 | 256 slots | 21,293,916 ns/op; 4,055,272 B/op; 42,446 allocs/op |
| Graph prune, gap=8, requests/slot=1 | 256 slots | 9,988,166 ns/op; 174,416 B/op; 1,418 allocs/op |
| Graph prune, gap=8, requests/slot=8 | 256 slots | 11,817,959 ns/op; 627,128 B/op; 6,175 allocs/op |

Exact commands:

```sh
GOMAXPROCS=2 CGO_ENABLED=0 go test ./pkg/quepaxa -run '^$' -bench '^BenchmarkEnsureDurableThroughRetainedPrefix$' -benchtime=100ms -benchmem -cpu=2
GOMAXPROCS=2 CGO_ENABLED=0 go test ./pkg/materializer -run '^$' -bench '^BenchmarkSQLAuthorizerSchemaCount$' -benchtime=100ms -benchmem -cpu=2
GOMAXPROCS=2 CGO_ENABLED=0 go test ./pkg/materializer -run '^$' -bench '^BenchmarkGraphPruneSparseSlotsAndBatchSize$' -benchtime=1x -benchmem -cpu=2
```

Raw Go output is retained in `raw/`. Graph samples are single-operation
observations with substantial per-case cost; rely on the repeated CI samples
and inspect the workload model before drawing conclusions. No production code
was changed.
