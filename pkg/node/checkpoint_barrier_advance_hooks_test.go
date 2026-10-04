//go:build rhiza_local_testhooks

package node

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestAdvanceCheckpointReadBarriersRetriesRealPreAdmissionRefusal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := wal.SetMaxBytes(4 << 20); err != nil {
		t.Fatal(err)
	}
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: "node", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "node"}}}, WAL: wal, LocalMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(filepath.Join(t.TempDir(), "db.sqlite"), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	if err := material.ConfigureLocalGraphNodePropertyIndexes(nil); err != nil {
		t.Fatal(err)
	}
	server := network.NewServer(core, material, "cluster", true, nil)
	defer server.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	server.SetDurabilityBarrier(func(workCtx context.Context, _ quepaxa.Slot) error {
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-workCtx.Done():
			return workCtx.Err()
		}
	})
	heldDone := make(chan error, 1)
	go func() {
		_, err := server.ProposeControl(ctx, types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{7}))
		heldDone <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("held proposal did not reach the durability barrier")
	}

	refused := make(chan struct{})
	var refusedOnce sync.Once
	restore := localtesthooks.Set(func(event string) {
		if event == "network:checkpoint-barrier:typed-pre-admission-refused" {
			refusedOnce.Do(func() { close(refused) })
		}
	})
	defer restore()
	advanceDone := make(chan error, 1)
	go func() { advanceDone <- advanceCheckpointReadBarriers(ctx, 2, material, server) }()
	select {
	case <-refused:
	case err := <-advanceDone:
		t.Fatalf("Node checkpoint advance ended before observed real refusal: %v", err)
	case <-ctx.Done():
		t.Fatal("Node checkpoint advance did not observe real pre-admission refusal")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-heldDone:
		if err != nil {
			t.Fatalf("held control proposal failed after release: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("held proposal did not finish after release")
	}
	select {
	case err := <-advanceDone:
		if err != nil {
			t.Fatalf("Node checkpoint advance failed after observed refusal: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Node checkpoint advance did not finish after release")
	}
	if got := material.Tip(); got != 2 {
		t.Fatalf("materialized tip=%d after observed refusal, want reserved tip 2", got)
	}
}
