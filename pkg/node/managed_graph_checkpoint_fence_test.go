package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

func TestManagedGraphCapturedCutReplaysHigherPreparedFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peers := graphReviewCores(t, false)
	core := peers["n1"]
	open := func() *materializer.Materializer {
		m, err := materializer.OpenManagedSQLKVGraphWithOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = m.Close() })
		return m
	}
	local, future := open(), open()
	value, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "fence-origin", Cypher: "CREATE (:FenceCI {n:1})"})
	if err != nil {
		t.Fatal(err)
	}
	if slot, _, err := core.Propose(ctx, value); err != nil || slot != 1 {
		t.Fatal("actual original proposal", slot, err)
	}
	if err := local.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	if err := future.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	cp, err := checkpoint.NewAppendOnlyManagerForPolicy(objstore.NewInMemBucket(), "higher-fence", t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	capture := func(m *materializer.Materializer) (*checkpoint.Checkpoint, quepaxa.CheckpointSeal) {
		files, cut, release, err := m.CheckpointFilesAt(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		sources := make([]checkpoint.Source, len(files))
		for i, f := range files {
			sources[i] = checkpoint.Source{Role: string(f.Role), Path: f.Path}
		}
		root, err := cp.CreateManagedGraphCandidate(ctx, sources, cut)
		if err != nil {
			t.Fatal(err)
		}
		prefix, _ := core.PrefixHash(quepaxa.Slot(cut))
		next, following, err := core.CheckpointLeaderOrders(quepaxa.Slot(cut))
		if err != nil {
			t.Fatal(err)
		}
		return root, quepaxa.CheckpointSeal{ConfigID: 1, Index: quepaxa.Slot(cut), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	}
	_, captured := capture(local)
	var nonce [types.ReadBarrierNonceSize]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	control := types.EncodeReadBarrier(nonce)
	if slot, _, err := core.Propose(ctx, control); err != nil || slot != 2 {
		t.Fatal("actual certified control", slot, err)
	}
	if err := future.Apply(ctx, 2, control); err != nil {
		t.Fatal(err)
	}
	higherRoot, higher := capture(future)
	core.SetCheckpointValidator(func(ctx context.Context, s quepaxa.CheckpointSeal) error {
		return cp.Verify(ctx, uint64(s.Index), s.RootHash, s.StateHash)
	})
	if err := core.PrepareCheckpoint(ctx, higher); err != nil {
		t.Fatal(err)
	}
	if local.Tip() != 1 || core.Tip() != 2 {
		t.Fatal("actual certified/materializer gap absent")
	}
	prepareErr := core.PrepareCheckpoint(ctx, captured)
	if prepareErr == nil {
		t.Fatal("stale cut accepted below actual prepared fence")
	}
	n := New(&types.ExecutionConfig{NodeID: "n1"})
	n.core, n.material = core, local
	if err := n.reconcileManagedGraphCapturedFence(ctx, captured.Index, prepareErr); !errors.Is(err, prepareErr) {
		t.Fatal("stale capture error was lost", err)
	}
	if local.Tip() < uint64(higher.Index) {
		t.Fatalf("stale capture never replayed actual higher prepared fence: material=%d certified=%d fence=%d", local.Tip(), core.Tip(), higher.Index)
	}
	index, hash, ok := core.LatestPreparedCheckpoint()
	if !ok || index != higher.Index || hash != higherRoot.RootHash {
		t.Fatal("actual higher prepared record was removed or changed")
	}
	if core.Tip() != 2 {
		t.Fatal("reconciliation fabricated a missing control/application decision")
	}
	_, fresh := capture(local)
	if fresh.Index < higher.Index {
		t.Fatal("fresh actual snapshot is still below prepared fence")
	}
}

func TestManagedGraphHigherFenceWithOlderRecordedSealMakesCertifiedProgress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peers := graphReviewCores(t, false)
	core := peers["n1"]
	mat, err := materializer.OpenManagedSQLKVGraphWithOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer mat.Close()
	cp, err := checkpoint.NewAppendOnlyManagerForPolicy(objstore.NewInMemBucket(), "recorded-fence", t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	capture := func() quepaxa.CheckpointSeal {
		files, cut, release, err := mat.CheckpointFilesAt(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		sources := make([]checkpoint.Source, len(files))
		for i, f := range files {
			sources[i] = checkpoint.Source{Role: string(f.Role), Path: f.Path}
		}
		root, err := cp.CreateManagedGraphCandidate(ctx, sources, cut)
		if err != nil {
			t.Fatal(err)
		}
		prefix, _ := core.PrefixHash(quepaxa.Slot(cut))
		next, following, err := core.CheckpointLeaderOrders(quepaxa.Slot(cut))
		if err != nil {
			t.Fatal(err)
		}
		return quepaxa.CheckpointSeal{ConfigID: 1, Index: quepaxa.Slot(cut), RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	}
	for _, peer := range peers {
		peer.SetCheckpointValidator(func(ctx context.Context, s quepaxa.CheckpointSeal) error {
			return cp.Verify(ctx, uint64(s.Index), s.RootHash, s.StateHash)
		})
	}
	for i := 0; i < 3; i++ {
		var nonce [types.ReadBarrierNonceSize]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			t.Fatal(err)
		}
		value := types.EncodeReadBarrier(nonce)
		slot, _, err := core.Propose(ctx, value)
		if err != nil {
			t.Fatal(err)
		}
		if err := mat.Apply(ctx, uint64(slot), value); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			// Keep the first actual physical root available for the older seal.
			old := capture()
			for _, peer := range peers {
				if err := peer.PrepareCheckpoint(ctx, old); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	// The original root at cut1 is immutable; reload its prepared identity.
	oldCut, oldRoot, _ := core.LatestPreparedCheckpoint()
	root, err := cp.OpenRoot(ctx, uint64(oldCut), oldRoot)
	if err != nil {
		t.Fatal(err)
	}
	prefix, _ := core.PrefixHash(oldCut)
	next, following, err := core.CheckpointLeaderOrders(oldCut)
	if err != nil {
		t.Fatal(err)
	}
	old := quepaxa.CheckpointSeal{ConfigID: 1, Index: oldCut, RootHash: oldRoot, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	encoded, err := quepaxa.EncodeCheckpointSeal(old)
	if err != nil {
		t.Fatal(err)
	}
	proposal := quepaxa.Proposal{Priority: quepaxa.Priority{31: 1}, ProposerID: "n2", Hash: sha256.Sum256(encoded), Value: encoded}
	for _, id := range []quepaxa.NodeID{"n1", "n2"} {
		if _, err := peers[id].Record(ctx, quepaxa.RecordRequest{Slot: 4, Step: 4, ConfigID: 1, Proposal: proposal}); err != nil {
			t.Fatal("real older seal record", err)
		}
	}
	// Certify this old-cut seal before another valid higher candidate is
	// prepared. A subsequent durability delivery must remain idempotent.
	certifiedSlot, _, err := core.Propose(ctx, encoded)
	if err != nil {
		t.Fatal("actual old seal certification", err)
	}
	if err := mat.Apply(ctx, uint64(certifiedSlot), encoded); err != nil {
		t.Fatal(err)
	}
	higher := capture()
	for _, peer := range peers {
		if err := peer.PrepareCheckpoint(ctx, higher); err != nil {
			t.Fatal(err)
		}
	}
	before, ok := core.CertifiedValue(certifiedSlot)
	if !ok {
		t.Fatal("actual original certificate absent")
	}
	request := quepaxa.RecordRequest{Slot: certifiedSlot, Step: 40, ConfigID: 1, Proposal: proposal}
	request.Proposal.ProposerID = "n3"
	request.Proposal.Priority = quepaxa.Priority{31: 99}
	summary, err := core.Record(ctx, request)
	if err != nil {
		t.Fatalf("already certified old seal durability delivery rejected by higher fence: slot=%d oldcut=%d higher=%d error=%v", certifiedSlot, old.Index, higher.Index, err)
	}
	if summary.Step < request.Step || summary.FirstCurrent == nil || summary.FirstCurrent.Hash != before.Hash || !bytes.Equal(summary.FirstCurrent.Value, before.Value) || summary.FirstCurrent.ProposerID == request.Proposal.ProposerID && summary.FirstCurrent.Priority == request.Proposal.Priority {
		t.Fatal("redelivery replaced certified proposal or max step")
	}
	if err := core.RequirePreparedCheckpoint(old); err == nil {
		t.Fatal("old root became eligible for new recording")
	}
	if err := core.PrepareCheckpoint(ctx, old); err == nil {
		t.Fatal("old preparation fence was weakened")
	}
	for _, kind := range []string{"unknown-slot", "hash-only", "bad-hash", "oversize", "zero-slot", "bad-step"} {
		t.Run(kind, func(t *testing.T) {
			r := request
			switch kind {
			case "unknown-slot":
				r.Slot = certifiedSlot + 1
			case "hash-only":
				r.Proposal.Value = nil
			case "bad-hash":
				r.Proposal.Value = append(bytes.Clone(encoded), 1)
			case "oversize":
				r.Proposal.Value = make([]byte, quepaxa.MaxReplicatedValueBytes+1)
			case "zero-slot":
				r.Slot = 0
			case "bad-step":
				r.Step = 3
			}
			if _, err := core.Record(ctx, r); err == nil {
				t.Fatal("ineligible redelivery bypassed original guards")
			}
		})
	}
	after, exists := core.CertifiedValue(certifiedSlot)
	if !exists || before.Hash != after.Hash || !bytes.Equal(before.Value, after.Value) || !bytes.Equal(before.Certificate, after.Certificate) || mat.Tip() != uint64(certifiedSlot) {
		t.Fatal("redelivery changed certified value/certificate/apply")
	}
	fence, hash, exists := core.LatestPreparedCheckpoint()
	if !exists || fence != higher.Index || hash != higher.RootHash {
		t.Fatal("higher prepared promise changed")
	}
	n := New(&types.ExecutionConfig{})
	n.core, n.material = core, mat
	n.server, err = network.NewServerForPolicy(core, mat, "recorded-fence", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer n.server.Close()
	if err := n.advanceManagedGraphCheckpointCut(ctx); err != nil {
		t.Fatalf("actual older recorded seal blocked certified control progress: tip=%d recorder=%d fence=%d error=%v", core.Tip(), core.RecorderTip(), higher.Index, err)
	}
	if core.Tip() <= higher.Index || mat.Tip() != uint64(core.Tip()) {
		t.Fatal("no higher actual certified cut", core.Tip(), mat.Tip())
	}
}
