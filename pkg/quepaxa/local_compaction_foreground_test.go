//go:build rhiza_local_testhooks

package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestLocalCompactionAllowsForegroundDuringBuild(t *testing.T) {
	dir := t.TempDir()
	core, wal := localReserveCore(t, dir, "local")
	defer wal.Close()
	if _, _, err := core.Propose(context.Background(), []byte("before")); err != nil {
		t.Fatal(err)
	}
	core.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	prefix, _ := core.PrefixHash(1)
	next, following, err := core.CheckpointLeaderOrders(1)
	if err != nil {
		t.Fatal(err)
	}
	root := sha256.Sum256([]byte("root"))
	seal := CheckpointSeal{ConfigID: 1, Index: 1, RootHash: root, StateHash: sha256.Sum256([]byte("state")), PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	if err := core.PrepareCheckpoint(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(context.Background(), encoded); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	reset := localtesthooks.Set(func(name string) {
		if name == "qlog:after-compaction-build-synced-before-manifest" {
			close(started)
			<-release
		}
	})
	defer reset()
	compacted := make(chan error, 1)
	go func() { compacted <- core.CompactThrough(1, root) }()
	select {
	case <-started:
	case err := <-compacted:
		t.Fatalf("compaction before build fence: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("compaction did not reach build fence")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	slot, _, err := core.Propose(ctx, []byte("during"))
	if err != nil {
		t.Errorf("foreground proposal while compaction is paused: %v", err)
	}
	once.Do(func() { close(release) })
	if err := <-compacted; err != nil {
		t.Fatalf("compaction: %v", err)
	}
	if err != nil {
		return
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.SetMaxBytes(math.MaxInt64); err != nil {
		t.Fatal(err)
	}
	recovered, err := New(Config{NodeID: "local", Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: "local"}}}, WAL: reopened, LocalMode: true})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := recovered.CertifiedValue(slot)
	if !ok || !bytes.Equal(got.Value, []byte("during")) {
		t.Fatalf("recovered slot %d: %+v ok=%v", slot, got, ok)
	}
}
