package quepaxa

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestEnsureDurableThroughPromotesHintsWithOneSync(t *testing.T) {
	config := &Cluster{ConfigID: 1, Members: []Member{{ID: "a"}}}
	sourceWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sourceWAL.Close() })
	source := newCore("a", config, sourceWAL, nil)
	for i := 1; i <= 3; i++ {
		if _, _, err := source.Propose(context.Background(), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	values, _, err := source.DecisionsFrom(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core := newCore("a", config, wal, nil)
	if err := core.AcceptCertifiedHints(values); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := core.EnsureDurableThrough(ctx, 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled promotion: %v", err)
	}
	core.commits = newGroupCommit(func() error { return errors.New("sync failed") })
	if err := core.EnsureDurableThrough(context.Background(), 3); err == nil {
		t.Fatal("failed sync was acknowledged")
	}
	for slot := Slot(1); slot <= 3; slot++ {
		if core.durable[slot] {
			t.Fatalf("failed sync marked slot %d durable", slot)
		}
	}
	var syncs atomic.Int32
	core.commits = newGroupCommit(func() error { syncs.Add(1); return wal.Sync() })
	for i := 0; i < 2; i++ {
		if err := core.EnsureDurableThrough(context.Background(), 3); err != nil {
			t.Fatal(err)
		}
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("promotion and repeated check used %d syncs, want 1", got)
	}
	if err := core.EnsureDurableThrough(context.Background(), 4); err == nil {
		t.Fatal("missing prefix was acknowledged")
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered := newCore("a", config, reopened, nil)
	if err := recovered.recover(); err != nil {
		t.Fatal(err)
	}
	if recovered.Tip() != 3 {
		t.Fatalf("restarted prefix=%d, want 3", recovered.Tip())
	}
	for _, value := range values {
		got, ok := recovered.CertifiedValue(value.Slot)
		if !ok || got.Hash != value.Hash {
			t.Fatalf("restarted slot %d differs", value.Slot)
		}
	}
}

func TestEnsureDurableThroughConcurrentRecoveryBase(t *testing.T) {
	source, seal, decision := testCheckpointRecoveryBase(t)
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	core := newCore("n1", source.config, wal, nil)
	first, _ := source.CertifiedValue(1)
	if err := core.AcceptCertifiedHints([]DecidedValue{first}); err != nil {
		t.Fatal(err)
	}
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	entered, release := make(chan struct{}), make(chan struct{})
	core.commits = newGroupCommit(func() error {
		close(entered)
		<-release
		return wal.Sync()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- core.EnsureDurableThrough(ctx, 1) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("prefix did not reach sync")
	}
	err = core.RestoreCheckpointBase(ctx, seal, decision)
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if core.durable[1] {
		t.Fatal("sync resurrected a compacted durability entry")
	}
	if err := core.EnsureDurableThrough(ctx, seal.Index); err != nil {
		t.Fatalf("covered prefix: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered := newCore("n1", source.config, reopened, nil)
	if err := recovered.recover(); err != nil {
		t.Fatal(err)
	}
	if recovered.Tip() != seal.Index || recovered.CompactionFloor() != seal.Index {
		t.Fatal("checkpoint coverage lost across restart")
	}
}
