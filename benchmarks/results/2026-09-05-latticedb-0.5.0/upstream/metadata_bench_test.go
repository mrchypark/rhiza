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
