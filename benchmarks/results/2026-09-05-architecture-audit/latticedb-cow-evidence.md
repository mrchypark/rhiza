# LatticeDB v0.3.0 AppMetadata COW patch evidence

`latticedb-cow.patch` applies cleanly to an untouched v0.3.0 module copy with `git apply --check`. It changes only LatticeDB internals; the public `Tx.GetAppMetadata`, `PutAppMetadata`, and `DeleteAppMetadata` APIs and persisted snapshot/WAL records are unchanged.

The patch forks 256 string-hash shards and copies a shard only when a transaction writes it. `CloneGraphState` remains a deep metadata clone for callers that require it. The recovery snapshot serializer iterates metadata directly and does not create an extra full metadata map.

Validation used Go 1.27.0 on Apple M3:

- `go test ./...` passed in the isolated patched v0.3.0 module.
- Existing transactional rollback, snapshot serialization/reopen, and WAL recovery tests passed.
- `TestAppMetadataForkClonesOnlyTouchedShard` adds same-shard mutation isolation coverage.
- The fixed 4,096-key cohort benchmark writes `cohort/0000` repeatedly with `-benchmem -benchtime=100x -count=3` on the same local filesystem and options.

| Build | Median ns/op | Median B/op | allocs/op |
| --- | ---: | ---: | ---: |
| v0.3.0 baseline | 463372 | 412653 | 129 |
| patched COW | 376660 | 30231 | 115 |

The benchmark is evidence for the map-copy allocation reduction, not an end-to-end Rhiza latency claim. Use a temporary `replace` in an alternate modfile for Rhiza graph integration before proposing an upstream release.
