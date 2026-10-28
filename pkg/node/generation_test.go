package node

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestInstallGenerationAnchorWithEmptyArchiveSnapshot(t *testing.T) {
	for _, anchoredWAL := range []bool{false, true} {
		name := "fresh WAL accepts empty archive"
		if anchoredWAL {
			name = "anchored WAL rejects empty archive"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			bucket := objstore.NewInMemBucket()
			archive := recovery.NewManager(bucket, "cluster", 1)
			defer archive.Close()

			wal, err := qlog.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer wal.Close()
			core, err := quepaxa.New(quepaxa.Config{
				NodeID:  "n1",
				Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}},
				WAL:     wal,
			})
			if err != nil {
				t.Fatal(err)
			}
			var localAnchor [32]byte
			if anchoredWAL {
				localAnchor = sha256.Sum256([]byte("local-generation-anchor"))
				if err := core.InstallFencedGenerationBase(ctx, quepaxa.FencedGenerationBase{
					ConfigID: 1, Index: 1,
					PrefixHash: sha256.Sum256([]byte("prefix")),
					RootHash:   sha256.Sum256([]byte("root")),
					AnchorHash: localAnchor,
				}); err != nil {
					t.Fatal(err)
				}
			}

			guard, err := newStartupRecoveryGuard(ctx, archive, "empty-archive-test")
			if err != nil {
				t.Fatal(err)
			}
			defer guard.Close()
			snapshot := guard.Snapshot()
			if snapshot == nil {
				t.Fatal("expected the real startup snapshot, including for an empty archive")
			}
			if _, err := snapshot.CaptureCertifiedView(); err == nil {
				t.Fatal("empty archive unexpectedly produced a certified archive view")
			}
			if _, anchored := snapshot.GenerationAnchor(); anchored {
				t.Fatal("empty archive unexpectedly contained a generation anchor")
			}
			if _, _, hasBase := snapshot.RecoveryBase(); hasBase {
				t.Fatal("empty archive unexpectedly contained a regular recovery base")
			}

			n := &Node{core: core, checkpoints: checkpoint.NewManager(bucket, "cluster", t.TempDir(), 1)}
			_, err = n.installGenerationAnchor(guard)
			if anchoredWAL {
				if err == nil {
					t.Fatal("anchored WAL accepted an empty archive without its shared generation evidence")
				}
				if core.Tip() != 1 || core.GenerationAnchorHash() != localAnchor {
					t.Fatalf("refusal changed anchored WAL state: tip=%d anchor=%x", core.Tip(), core.GenerationAnchorHash())
				}
			} else {
				if err != nil {
					t.Fatalf("fresh WAL rejected empty generation-0 archive snapshot: %v", err)
				}
				if core.Tip() != 0 || core.GenerationAnchorHash() != ([32]byte{}) {
					t.Fatalf("empty archive startup changed fresh WAL state: tip=%d anchor=%x", core.Tip(), core.GenerationAnchorHash())
				}
			}
			if _, err := snapshot.CaptureCertifiedView(); err == nil {
				t.Fatal("Node empty-archive exception changed generation-0 certified-view behavior")
			}
		})
	}
}
