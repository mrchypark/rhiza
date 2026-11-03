//go:build rhiza_local_testhooks

package quepaxa

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func newManagedNoPVCReconstructionCutFixture(t *testing.T) (*Core, SealedCheckpoint, [32]byte) {
	t.Helper()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	identity := make([]byte, 32)
	for i := range identity {
		identity[i] = byte(i + 1)
	}
	if err := wal.BindIdentity(identity); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	var identityArray [32]byte
	copy(identityArray[:], identity)
	cluster := Cluster{ConfigID: 1, Members: []Member{{ID: "n1", WALIdentity: hex.EncodeToString(identity)}}}
	core, err := New(Config{
		NodeID: "n1", Cluster: cluster, WAL: wal,
		ManagedAppendOnly: &ManagedAppendOnlyConfig{Binding: ManagedAppendOnlyBinding{
			ApplicationProtocol: managedGraphRequestAppendOnlyApplicationProtocol,
			BootstrapPlanDigest: hex.EncodeToString(make([]byte, 32)),
			MemberConfigDigest:  hex.EncodeToString(identity),
			Generation:          7,
			WALIdentity:         identityArray,
		}, Verify: func(context.Context, ManagedCheckpointCandidate) error { return nil }},
	})
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := wal.Close(); err != nil {
			t.Errorf("close reconstruction-cut WAL: %v", err)
		}
	})
	if slot, _, err := core.Propose(context.Background(), []byte("actual-certified-prefix")); err != nil || slot != 1 {
		t.Fatalf("certify reconstruction prefix: slot=%d err=%v", slot, err)
	}
	prefix, ok := core.PrefixHash(1)
	if !ok {
		t.Fatal("actual certified prefix hash unavailable")
	}
	next, following, err := core.CheckpointLeaderOrders(1)
	if err != nil {
		t.Fatal(err)
	}
	seal := CheckpointSeal{
		ConfigID: 1, Index: 1,
		RootHash:   sha256.Sum256([]byte("reconstruction-root-one")),
		StateHash:  sha256.Sum256([]byte("reconstruction-state-one")),
		PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following,
	}
	sealBytes, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if slot, _, err := core.Propose(context.Background(), sealBytes); err != nil || slot != 2 {
		t.Fatalf("certify actual checkpoint producer: slot=%d err=%v", slot, err)
	}
	selected, ok, err := core.LatestManagedCheckpointSeal()
	if err != nil || !ok || selected.Index != 1 || selected.DecisionSlot != 2 {
		t.Fatalf("select actual checkpoint certificate: selected=%+v found=%t err=%v", selected, ok, err)
	}
	prefix, ok = core.PrefixHash(2)
	if !ok {
		t.Fatal("captured certified cut prefix unavailable")
	}
	return core, selected, prefix
}

func TestManagedNoPVCReconstructionCutCapturesCertifiedExtension(t *testing.T) {
	core, selected, prefix := newManagedNoPVCReconstructionCutFixture(t)
	cut, err := core.CaptureManagedReconstructionCut(selected, 0, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if cut.CertifiedTip() != 2 || cut.ProducerDecisionSlot() != 2 || cut.PrefixHash() != prefix {
		t.Fatalf("captured cut=%d producer=%d prefix=%x", cut.CertifiedTip(), cut.ProducerDecisionSlot(), cut.PrefixHash())
	}
	if slot, _, err := core.Propose(context.Background(), []byte("actual-certified-extension")); err != nil || slot != 3 {
		t.Fatalf("certify monotone suffix: slot=%d err=%v", slot, err)
	}
	if got, ok := core.PrefixHash(cut.CertifiedTip()); !ok || got != prefix {
		t.Fatal("certified suffix changed the captured prefix")
	}
	through, err := core.RecheckManagedReconstructionCut(context.Background(), cut)
	if err != nil || through != 3 {
		t.Fatalf("same-authority certified extension recheck through=%d err=%v", through, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := core.RecheckManagedReconstructionCut(canceled, cut); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled cut recheck error=%v", err)
	}
}

func TestManagedNoPVCReconstructionCutRejectsUninitializedAndWrongCore(t *testing.T) {
	var uninitialized *Core
	if _, err := uninitialized.CaptureManagedReconstructionCut(SealedCheckpoint{}, 0, [32]byte{}); err == nil {
		t.Fatal("nil Core captured a reconstruction cut")
	}
	if _, err := uninitialized.RecheckManagedReconstructionCut(context.Background(), nil); err == nil {
		t.Fatal("nil Core rechecked a reconstruction cut")
	}
	core, selected, prefix := newManagedNoPVCReconstructionCutFixture(t)
	cut, err := core.CaptureManagedReconstructionCut(selected, 0, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	other, _, _ := newManagedNoPVCReconstructionCutFixture(t)
	if _, err := other.RecheckManagedReconstructionCut(context.Background(), cut); err == nil {
		t.Fatal("different Core accepted the captured cut")
	}
	if _, err := core.CaptureManagedReconstructionCut(selected, 2, sha256.Sum256([]byte("wrong-prefix"))); err == nil {
		t.Fatal("Core captured a cut with a mismatched expected prefix")
	}
	if cut.PrefixHash() != prefix {
		t.Fatal("wrong-prefix negative case mutated the original captured cut")
	}
}

func TestManagedNoPVCReconstructionCutRejectsProducerAfterRetainedK(t *testing.T) {
	core, original, prefix := newManagedNoPVCReconstructionCutFixture(t)
	retained, err := core.CaptureManagedReconstructionCut(original, 2, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if retained.CertifiedTip() != 2 || retained.ProducerDecisionSlot() != original.DecisionSlot || retained.PrefixHash() != prefix || retained.seal.RootHash != original.RootHash {
		t.Fatalf("initial retained cut did not preserve journal root/K: cut=%+v", retained)
	}
	if slot, _, err := core.Propose(context.Background(), []byte("certified-before-late-root")); err != nil || slot != 3 {
		t.Fatalf("certify pre-root suffix: slot=%d err=%v", slot, err)
	}
	late := original.CheckpointSeal
	late.RootHash = sha256.Sum256([]byte("reconstruction-root-late"))
	late.StateHash = sha256.Sum256([]byte("reconstruction-state-late"))
	lateBytes, err := EncodeCheckpointSeal(late)
	if err != nil {
		t.Fatal(err)
	}
	if slot, _, err := core.Propose(context.Background(), lateBytes); err != nil || slot != 4 {
		t.Fatalf("certify later producer: slot=%d err=%v", slot, err)
	}
	latest, ok, err := core.LatestManagedCheckpointSeal()
	if err != nil || !ok || latest.DecisionSlot != 4 || latest.RootHash != late.RootHash {
		t.Fatalf("latest producer=%+v found=%t err=%v", latest, ok, err)
	}
	if _, err := core.CaptureManagedReconstructionCut(latest, 2, prefix); err == nil {
		t.Fatal("late producer certificate was allowed outside retained K")
	}
	// A retained install keeps the original journal's producer and K; a later
	// same-index root cannot replace its historical source.
	retainedAgain, err := core.CaptureManagedReconstructionCut(original, 2, prefix)
	if err != nil || retainedAgain.CertifiedTip() != 2 || retainedAgain.ProducerDecisionSlot() != 2 || retainedAgain.seal.RootHash != original.RootHash {
		t.Fatalf("retained source was not kept fixed: cut=%+v err=%v", retainedAgain, err)
	}
	through, err := core.RecheckManagedReconstructionCut(context.Background(), retainedAgain)
	if err != nil || through != 4 || retainedAgain.CertifiedTip() != 2 {
		t.Fatalf("retained original K/root was not rechecked against bounded later tip: K=%d through=%d err=%v", retainedAgain.CertifiedTip(), through, err)
	}
}

func TestManagedNoPVCReconstructionCutRejectsAuthorityAndRecoveryDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Core, *ManagedReconstructionCut)
	}{
		{name: "identity", mutate: func(core *Core, _ *ManagedReconstructionCut) { core.nodeID = "other" }},
		{name: "wal_nonce", mutate: func(core *Core, _ *ManagedReconstructionCut) { core.walIdentity = "other-wal" }},
		{name: "generation_anchor", mutate: func(core *Core, _ *ManagedReconstructionCut) { core.generationAnchorHash[0] ^= 1 }},
		{name: "profile_generation", mutate: func(core *Core, _ *ManagedReconstructionCut) { core.managedAppendOnly.Binding.Generation++ }},
		{name: "profile_pointer", mutate: func(core *Core, _ *ManagedReconstructionCut) {
			replacement := *core.managedAppendOnly
			core.managedAppendOnly = &replacement
		}},
		{name: "config_membership", mutate: func(core *Core, _ *ManagedReconstructionCut) { core.configHistory[0].cluster.ConfigID++ }},
		{name: "membership_version", mutate: func(core *Core, _ *ManagedReconstructionCut) { core.membershipVersion++ }},
		{name: "floor", mutate: func(core *Core, cut *ManagedReconstructionCut) { core.floor = cut.floor + 1 }},
		{name: "floor_root", mutate: func(core *Core, _ *ManagedReconstructionCut) { core.floorRoot[0] ^= 1 }},
		{name: "captured_prefix", mutate: func(core *Core, cut *ManagedReconstructionCut) {
			prefix := core.prefixes[cut.tip]
			prefix[0] ^= 1
			core.prefixes[cut.tip] = prefix
		}},
		{name: "root_prefix", mutate: func(core *Core, cut *ManagedReconstructionCut) {
			prefix := core.prefixes[cut.seal.Index]
			prefix[0] ^= 1
			core.prefixes[cut.seal.Index] = prefix
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			core, selected, _ := newManagedNoPVCReconstructionCutFixture(t)
			cut, err := core.CaptureManagedReconstructionCut(selected, 0, [32]byte{})
			if err != nil {
				t.Fatal(err)
			}
			core.mu.Lock()
			test.mutate(core, cut)
			core.mu.Unlock()
			if _, err := core.RecheckManagedReconstructionCut(context.Background(), cut); err == nil {
				t.Fatal("recheck accepted injected authority/recovery drift")
			}
		})
	}
}
