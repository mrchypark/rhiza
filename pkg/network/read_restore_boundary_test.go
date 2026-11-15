//go:build rhiza_local_testhooks

package network

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

func TestReadRestoreBoundary(t *testing.T) {
	for _, quiesce := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "node-order"}[quiesce], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			p := newForegroundAPIPeers(t, t.TempDir())
			s := p.servers["n1"]
			if _, err := s.Execute(ctx, ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE probe(id INTEGER PRIMARY KEY)"}); err != nil {
				t.Fatal(err)
			}
			files, index, cleanup, err := p.materials["n1"].CheckpointFilesAt(ctx)
			if err != nil || index != 1 {
				t.Fatalf("checkpoint=%d err=%v", index, err)
			}
			defer cleanup()
			if _, err := s.Execute(ctx, ExecuteRequest{RequestID: "write", SQL: "INSERT INTO probe VALUES(1)"}); err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once, releaseOnce sync.Once
			releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(releaseGate)
			undo := localtesthooks.Set(func(event string) {
				if event == "network:read:after-barrier" {
					once.Do(func() { close(entered) })
					<-release
				}
			})
			t.Cleanup(undo)
			type result struct {
				response QueryResponse
				err      error
			}
			done := make(chan result, 1)
			go func() {
				response, err := s.Query(ctx, QueryRequest{SQL: "SELECT count(*) FROM probe", Consistency: "linearizable"})
				done <- result{response, err}
			}()
			select {
			case <-entered:
			case got := <-done:
				t.Fatalf("early query: %+v", got)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if quiesce {
				resume, err := s.Quiesce(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if p.materials["n1"].Tip() < index {
					err = p.materials["n1"].RestoreCheckpoint(ctx, files)
				}
				resume()
				if err != nil {
					t.Fatal(err)
				}
			} else if err := p.materials["n1"].RestoreCheckpoint(ctx, files); err != nil {
				t.Fatal(err)
			}
			releaseGate()
			select {
			case got := <-done:
				wantSlot, wantCount := uint64(1), int64(0)
				if quiesce {
					wantSlot, wantCount = 2, 1
				}
				if got.err != nil || got.response.AppliedSlot != wantSlot || len(got.response.Rows) != 1 || got.response.Rows[0][0] != wantCount {
					t.Fatalf("result=%+v", got)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}
