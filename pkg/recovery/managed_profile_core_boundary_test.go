//go:build rhiza_local_testhooks

package recovery

import (
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/ncruces/go-sqlite3/driver"
)

func TestManagedRecoveryBeforeCoreBase(t *testing.T) {
	for _, query := range []string{"", `UPDATE _rhiza_meta SET value='7' WHERE key='sql_execution_policy'`, `UPDATE _rhiza_meta SET value='1025' WHERE key='sql_idempotency_window'`, `DELETE FROM _rhiza_meta WHERE key='managed_application_policy'`, `UPDATE _rhiza_meta SET value='2' WHERE key='applied_slot'`} {
		t.Run(query, func(t *testing.T) {
			var alter func(string)
			if query != "" {
				alter = func(path string) {
					db, err := driver.Open(path)
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					if _, err := db.Exec(query); err != nil {
						t.Fatal(err)
					}
				}
			}
			f := newMixedRecoveryFixture(t, 1024, true, alter)
			// Real root/block verification and a same-manager cache hit succeed;
			// neither may stand in for the actual SQLite semantic check.
			if err := f.cp.Verify(f.ctx, f.root.Index, f.root.RootHash, f.root.Hash); err != nil {
				t.Fatal(err)
			}
			if err := f.cp.Verify(f.ctx, f.root.Index, f.root.RootHash, f.root.Hash); err != nil {
				t.Fatal(err)
			}
			if query != "" {
				if err := verifyMixedReadOnlyCheckpoint(f.ctx, f.cp, f.root, f.seal.Index, 1024, filepath.Join(t.TempDir(), "cache")); err == nil {
					t.Fatal("cache bypassed file inspection")
				}
			}
			var installs atomic.Int32
			restore := localtesthooks.Set(func(name string) {
				if name == "recovery:readonly:before-base-install" {
					installs.Add(1)
				}
			})
			defer restore()
			view, err := OpenReadOnlyViewForPolicy(f.ctx, f.bucket, "mixed", 1, f.bootstrap, 8, 1024)
			if query == "" {
				if err != nil || view == nil || installs.Load() < 1 {
					t.Fatalf("positive gate disabled: installs=%d error=%v", installs.Load(), err)
				}
			} else if err == nil || view != nil || installs.Load() != 0 {
				t.Fatalf("invalid file reached Core base: installs=%d error=%v", installs.Load(), err)
			}
		})
	}
}
