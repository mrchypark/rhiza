package materializer

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
)

// BenchmarkApplicationEvidenceScan measures the existing complete inspection,
// including its private SQLite sort and cleanup, not JSON encoding in isolation.
func BenchmarkApplicationEvidenceScan(b *testing.B) {
	for _, mixed := range []bool{false, true} {
		profile := "sql"
		if mixed {
			profile = "sql_kv"
		}
		for _, rows := range []int{0, 64, 512, 4096} {
			b.Run(fmt.Sprintf("%s/rows_%d/blob_256", profile, rows), func(b *testing.B) {
				m := evidenceCostFixture(b, mixed, rows)
				want := inspectEvidenceCost(b, m, mixed)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if got := inspectEvidenceCost(b, m, mixed); got != want {
						b.Fatal("inspection changed unchanged evidence")
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(rows), "user_rows/op")
			})
		}
	}
}

func evidenceCostFixture(t testing.TB, mixed bool, rows int) *Materializer {
	t.Helper()
	file := filepath.Join(t.TempDir(), "sqlite.db")
	var m *Materializer
	var err error
	if mixed {
		m, err = OpenManagedSQLKV(file, 1, 1024)
	} else {
		m, err = Open(file, 1)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	if mixed {
		value, err := types.EncodeManagedKVCommand(types.KVCommand{
			RequestID: "evidence-cost-kv", Operation: "put", Key: "evidence-cost-key", Value: []byte{0, 255},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(context.Background(), 1, value); err != nil {
			t.Fatal(err)
		}
	}
	// Setup only: keep receipt/history size fixed to isolate user-data scaling.
	// This is not a replicated write, admission, or end-to-end Host benchmark.
	tx, err := m.writer.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`CREATE TABLE evidence_cost (id INTEGER PRIMARY KEY, text_value TEXT, blob_value BLOB)`); err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO evidence_cost VALUES (?, ?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	blob := bytes.Repeat([]byte{0, 255, 34, 92}, 64)
	for i := range rows {
		if _, err := stmt.Exec(i+1, "<>&\n\"\\한글", blob); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return m
}

func inspectEvidenceCost(t testing.TB, m *Materializer, mixed bool) SQLApplicationState {
	t.Helper()
	if mixed {
		state, err := m.InspectManagedApplicationState(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return SQLApplicationState(state)
	}
	state, err := m.InspectSQLApplicationState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestApplicationEvidenceCostFixture(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprint(mixed), func(t *testing.T) {
			m := evidenceCostFixture(t, mixed, 16)
			var rows int
			if err := m.writer.QueryRow(`SELECT COUNT(*) FROM evidence_cost`).Scan(&rows); err != nil || rows != 16 {
				t.Fatalf("fixture rows=%d err=%v", rows, err)
			}
			before := inspectEvidenceCost(t, m, mixed)
			if before.FreshEmpty || inspectEvidenceCost(t, m, mixed) != before {
				t.Fatal("fixture evidence is empty or unstable")
			}
			if _, err := m.writer.Exec(`UPDATE evidence_cost SET blob_value=x'01' WHERE id=1`); err != nil {
				t.Fatal(err)
			}
			if after := inspectEvidenceCost(t, m, mixed); after.Digest == before.Digest {
				t.Fatal("changed row did not change evidence")
			}
		})
	}
}
