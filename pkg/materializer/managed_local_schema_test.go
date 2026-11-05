package materializer

import (
	"context"
	"path/filepath"
	"testing"
)

func TestManagedLocalSchemaRequestProfiles(t *testing.T) {
	for _, profile := range []struct {
		name string
		open func(string, int, ...uint64) (*Materializer, error)
	}{
		{"policy9", OpenManagedSQLKVWithAppendOnlyOriginals},
		{"policy10", OpenManagedSQLKVGraphWithAppendOnlyOriginals},
	} {
		t.Run(profile.name, func(t *testing.T) {
			ctx := context.Background()
			m, err := profile.open(filepath.Join(t.TempDir(), "sqlite.db"), 1, 1024)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := m.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := m.ValidateManagedLocalSchema(ctx); err != nil {
				t.Fatal("healthy schema rejected", err)
			}
			for _, corruption := range []struct{ name, damage, repair string }{
				{"unknown internal table", "CREATE TABLE _rhiza_corrupt_internal(value)", "DROP TABLE _rhiza_corrupt_internal"},
				{"missing internal index", "DROP INDEX _rhiza_idempotency_slot", "CREATE INDEX _rhiza_idempotency_slot ON _rhiza_idempotency(commit_slot)"},
			} {
				t.Run(corruption.name, func(t *testing.T) {
					if _, err := m.writer.ExecContext(ctx, corruption.damage); err != nil {
						t.Fatal(err)
					}
					defer func() {
						if _, err := m.writer.ExecContext(ctx, corruption.repair); err != nil {
							t.Error(err)
						}
					}()
					if err := m.ValidateManagedLocalSchema(ctx); err == nil {
						t.Fatal("corrupt internal schema accepted")
					}
				})
				if err := m.ValidateManagedLocalSchema(ctx); err != nil {
					t.Fatal("repaired schema rejected", err)
				}
			}
		})
	}
}
