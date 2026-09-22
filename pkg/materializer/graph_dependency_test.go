package materializer

import (
	"context"
	"github.com/mrchypark/rhiza/internal/types"
	"testing"
)

func TestGraphListPropertyMutation(t *testing.T) {
	m, e := Open(t.TempDir()+"/db", 1)
	if e != nil {
		t.Fatal(e)
	}
	defer m.Close()
	cmd := types.GraphCommand{RequestID: "probe", Cypher: "CREATE (:ListItem {values: [1,2]})"}
	if err := ValidateGraphCommandAdmission(cmd); err != nil {
		t.Fatal(err)
	}
	v, e := types.EncodeGraphCommand(cmd)
	if e != nil {
		t.Fatal(e)
	}
	if e = m.Apply(context.Background(), 1, v); e != nil {
		t.Fatal(e)
	}
	r, f, e := m.GraphMutationReceipt(context.Background(), "probe")
	if e != nil || !f || r.Status != types.MutationCommitted {
		t.Fatalf("receipt=%+v found=%v err=%v", r, f, e)
	}
	result, e := m.GraphQuery(context.Background(), "MATCH (n:ListItem) RETURN size(n.values)", nil)
	if e != nil || len(result.Rows) != 1 || result.Rows[0][0] != int64(2) {
		t.Fatalf("result=%+v err=%v", result, e)
	}
}
