# LatticeDB v0.5.1 released-dependency validation

## Provenance

`go mod download -json github.com/mrchypark/latticedb-go@v0.5.1` recorded
the release origin in `module-download.json`:

- tag: `v0.5.1`
- origin SHA: `33293c219a2c33ec0222cebca06291013a0c14f7`
- module sum: `h1:mrXz7LlApwKlSr6fQ7vqMyXVSdW7BadgpvUBsHajD34=`

The commands below run the unchanged public repro sources from
`../2026-09-05-latticedb-0.5.0/upstream` with `repro.mod`, which instead
requires v0.5.1. `go test -modfile=... ./...` passed.

## Snapshot/checkpoint contention (#157)

Command:

```sh
cd benchmarks/results/2026-09-05-latticedb-0.5.0/upstream
go run -modfile=../../2026-09-05-latticedb-0.5.1/repro.mod .
```

The existing public-API workload makes one 2 MiB metadata update with a 1 MiB
automatic checkpoint threshold, then probes `BeginSnapshot` for 250 ms after
the application writer returned. v0.5.0 observed `ErrWriteTxActive` in 20/20
rounds. v0.5.1 observed it in 0/20 rounds; raw output is
`begin_snapshot_contention.txt`.

The released source changes `BeginSnapshot` from direct `TryLock` to
`lockWriterAfterCheckpoint`, which waits for a checkpoint attempt but preserves
`ErrWriteTxActive` if a non-checkpoint writer still owns the slot. This is
consistent with the reproducer result. It is a bounded observational check,
not a latency measurement or a proof for every scheduler interleaving.

## Application-metadata allocation (#158)

Command:

```sh
cd benchmarks/results/2026-09-05-latticedb-0.5.0/upstream
go test -modfile=../../2026-09-05-latticedb-0.5.1/repro.mod \
  -run='^$' -bench='BenchmarkAppMetadataOneKey' -benchmem -benchtime=100x -count=3
```

The unchanged benchmark updates one key after seeding a fixed metadata cohort.
The table compares medians from the retained v0.5.0 raw result with v0.5.1.
Times are intentionally omitted: these runs used a shared workstation and
different active CPU counts, while allocated bytes are the relevant scaling
signal.

| Seeded keys | v0.5.0 median B/op | v0.5.1 median B/op | Change | v0.5.0/v0.5.1 median allocs/op |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 3,280 | 8,320 | +154% | 27 / 34 |
| 4,096 | 396,605 | 9,424 | -97.6% | 44 / 36 |
| 16,384 | 1,577,270 | 13,386 | -99.2% | 92 / 36 |

Raw v0.5.1 output is `metadata-bench.txt`; the retained comparison baseline is
`../2026-09-05-latticedb-0.5.0/upstream/metadata-bench.txt`. The release source
uses `AppMetadata.Fork()` in `ensureAppMetadataWritable`, replacing the prior
whole-map copy. This removes the key-count-proportional allocation observed at
the two material cohorts, though it has a higher fixed cost for one key.

## Rhiza integration

Rhiza now requires the official v0.5.1 module without a local replacement.
The existing Rhiza `BenchmarkLatticeAppMetadataUpdate4096Keys` also completed:

```sh
CGO_ENABLED=0 GOMAXPROCS=2 go test ./pkg/materializer -run '^$' \
  -bench '^BenchmarkLatticeAppMetadataUpdate4096Keys$' -benchmem -benchtime=100x -count=3
```

Its median was 8,768 B/op and 36 allocs/op; the retained v0.5.0 samples were
approximately 396,610 B/op. See `rhiza-metadata-bench.txt`. Other build/race
processes ran concurrently, so elapsed time is not a valid before/after latency
comparison. The existing context-bounded Rhiza snapshot acquisition still
handles an actual application writer being active.

Final integration checks passed on macOS ARM64 (Go 1.27.0):
`CGO_ENABLED=0 go test ./...`, `CGO_ENABLED=1 go vet ./...`,
`CGO_ENABLED=1 GOEXPERIMENT=cgocheck2 go test -race ./...`, and the Go server build.
The first race invocation overlapped an FFI test rename and failed to compile
that package; the final invocation after edits completed passed all packages.
The Rust SDK passed fmt, clippy with warnings denied, and all three tests with
`GOEXPERIMENT=cgocheck2`. A separate Cargo consumer built in release mode and
verified SQL large integers, binary KV, Graph, and persistence after reopening.
Linux SDK checks are configured in CI but were not run locally.
One later Rust rerun hit a WAL lock collision with timestamp-only test directory
names. Test paths now include PID and an atomic sequence; the final all-target
run and 10 repeated parallel runs (30 tests) passed, as did final clippy/fmt.
