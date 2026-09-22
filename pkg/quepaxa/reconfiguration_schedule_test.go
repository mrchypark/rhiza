package quepaxa

import (
	"context"
	"testing"
)

func TestReconfigurationScheduleBoundaryAbort(t *testing.T) {
	cores, _ := reconfigCluster(t)
	c := cores["a"]
	ctx := context.Background()
	for c.Tip() < 96 {
		if _, _, err := c.Propose(ctx, []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "lost", WALIdentity: "0000000000000000000000000000000000000000000000000000000000000000"}}}
	slot, err := c.BeginReconfiguration(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("freeze=%d", slot)
	if err = c.AbortReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err = c.Propose(ctx, []byte("after")); err != nil {
		t.Fatal(err)
	}
}
