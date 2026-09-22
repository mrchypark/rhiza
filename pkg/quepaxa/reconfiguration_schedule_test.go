package quepaxa

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestReconfigurationScheduleBoundaryAbort(t *testing.T) {
	for _, tip := range []Slot{80, 95, 96, 97, 112} {
		t.Run(fmt.Sprint(tip), func(t *testing.T) {
			cores, _ := reconfigCluster(t)
			c := cores["a"]
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			for c.Tip() < tip {
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
			for c.Tip() < slot+32 {
				if _, _, err = c.Propose(ctx, []byte("after")); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
