Single-key application-metadata updates allocate in proportion to the entire metadata key set. This remains present in v0.5.0; it is not a newly introduced v0.5 regression.

Rhiza stores applied positions and request receipts through this API. Even when each transaction changes only one small value, retained request metadata increases the allocation cost of subsequent transactions.

## Evidence

Public API only, Go 1.27.0, Apple M3, darwin/arm64, GOMAXPROCS=2. Each cohort is seeded outside timing; each measured transaction updates the same existing key. Default storage options. Three 100-operation samples per cohort:

| Retained keys | Median B/op | Median allocs/op | Median ns/op |
| --- | ---: | ---: | ---: |
| 1 | 3,280 | 27 | 27,349 |
| 4,096 | 396,605 | 44 | 172,812 |
| 16,384 | 1,577,270 | 92 | 527,043 |

Timing is informational on a shared workstation; the allocation scaling is the principal evidence. These are per-operation allocated bytes, not retained heap or process RSS.

## Cause

[`ensureAppMetadataWritable` at v0.5.0](https://github.com/mrchypark/latticedb-go/blob/299e3003e84f85becd504217a63e63dde784b36c/internal/engine/app_metadata.go#L56) allocates a map sized to the complete `AppMetadata` map and copies every entry on the first metadata write of each transaction. The byte values are shared here, but map entries are copied even for untouched keys.

## Reproduce

Save this as `metadata_bench_test.go` in a new directory:

```go
package main

import (
	"fmt"
	"path/filepath"
	"testing"

	lattice "github.com/mrchypark/latticedb-go"
)

func BenchmarkAppMetadataOneKey(b *testing.B) {
	for _, keys := range []int{1, 4096, 16384} {
		b.Run(fmt.Sprint(keys), func(b *testing.B) {
			db, err := lattice.Open(filepath.Join(b.TempDir(), "db"), lattice.OpenOptions{Create: true})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := db.Close(); err != nil {
					b.Error(err)
				}
			})
			if err := db.Update(func(tx *lattice.Tx) error {
				for i := range keys {
					if err := tx.PutAppMetadata(fmt.Appendf(nil, "key/%05d", i), []byte("seed")); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			key, value := []byte("key/00000"), []byte("next")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.Update(func(tx *lattice.Tx) error { return tx.PutAppMetadata(key, value) }); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if err := db.View(func(tx *lattice.Tx) error {
				got, ok, err := tx.GetAppMetadata(key)
				if err != nil {
					return err
				}
				if !ok || string(got) != "next" {
					return fmt.Errorf("value = %q, found = %v", got, ok)
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
		})
	}
}
```

```sh
go mod init metadata-repro
go get github.com/mrchypark/latticedb-go@v0.5.0
GOMAXPROCS=2 go test -run '^$' -bench '^BenchmarkAppMetadataOneKey$' -benchtime=100x -count=3 -benchmem
```

## Requested improvement

Avoid copying the whole key map for a single-key mutation, while preserving transaction rollback and immutable snapshot/read generations. A shard-level copy-on-write map or equivalent bounded mutation structure could address the cost. Validate update/delete rollback, old snapshot visibility, WAL replay, and snapshot restore alongside allocation scaling across these fixed cohorts. Do not trade isolation or durability for the allocation reduction.
