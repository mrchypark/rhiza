package materializer

import (
	"context"
	"fmt"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
)

// An ordinary rowid table containing MaxInt64 enters SQLite's random rowid
// allocation path on its next implicit insert. Keep that state unrepresentable.
func TestSQLRejectsExhaustedRowID(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/state.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for i, query := range []string{
		"CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)",
		"INSERT INTO items VALUES(9223372036854775807, 'sentinel')",
	} {
		id := fmt.Sprint(i)
		value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: id, SQL: query}})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(ctx, uint64(i+1), value); err != nil {
			t.Fatal(err)
		}
		receipt, ok, err := m.MutationReceipt(ctx, types.MutationSQL, id)
		if err != nil || !ok {
			t.Fatalf("receipt=%+v found=%v err=%v", receipt, ok, err)
		}
		want := types.MutationCommitted
		if i == 1 {
			want = types.MutationRejected
		}
		if receipt.Status != want {
			t.Fatalf("%s: status=%s want=%s", query, receipt.Status, want)
		}
	}
}
