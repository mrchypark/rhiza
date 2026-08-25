//go:build graph

package network

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestConcurrentGraphRequestIDConflict(t *testing.T) {
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	member := quepaxa.Member{ID: "n1"}
	core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: quepaxa.Cluster{Members: []quepaxa.Member{member}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil, []quepaxa.Member{member}, 0)
	defer server.Close()

	const contenders = 32
	start := make(chan struct{})
	errs := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := range contenders {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := server.GraphExecute(context.Background(), types.GraphCommand{
				RequestID: "shared", Cypher: `CREATE (:Item {id: $id})`, Args: map[string]any{"id": float64(i)},
			})
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	succeeded, conflicted := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrRequestConflict):
			conflicted++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if succeeded != 1 || conflicted != contenders-1 {
		t.Fatalf("succeeded=%d conflicted=%d", succeeded, conflicted)
	}
}
