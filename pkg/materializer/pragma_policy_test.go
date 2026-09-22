package materializer

import (
	"context"
	"fmt"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
)

func TestWriterRejectsNodeLocalPragma(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/state.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for i, query := range []string{
		"CREATE TABLE probe(value TEXT)",
		"INSERT INTO probe SELECT file FROM pragma_database_list WHERE name='main'",
	} {
		id := fmt.Sprint(i)
		value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: id, SQL: query}})
		if err != nil {
			t.Fatal(err)
		}
		if err = m.Apply(ctx, uint64(i+1), value); err != nil {
			t.Fatal(err)
		}
		receipt, ok, err := m.MutationReceipt(ctx, types.MutationSQL, id)
		if err != nil || !ok {
			t.Fatal(receipt, ok, err)
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
