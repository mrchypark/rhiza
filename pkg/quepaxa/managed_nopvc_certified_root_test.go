package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestManagedNoPVCLearnerSelectsAlternateCertifiedRootAtPhysicalFloor(t *testing.T) {
	ctx := context.Background()
	ids := []NodeID{"a", "b", "c"}
	cluster := Cluster{ConfigID: 1, Members: make([]Member, len(ids))}
	transport := &clusterTransport{cores: make(map[NodeID]*Core), down: make(map[NodeID]bool), dropDecision: make(map[NodeID]bool)}
	var voters []*Core
	var wals []*qlog.WAL
	for i, id := range ids {
		identity := bytes.Repeat([]byte{byte(i + 1)}, 32)
		cluster.Members[i] = Member{ID: id, WALIdentity: hex.EncodeToString(identity)}
	}
	for i, id := range ids {
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		wals = append(wals, wal)
		identity, _ := hex.DecodeString(cluster.Members[i].WALIdentity)
		if err := wal.BindIdentity(identity); err != nil {
			t.Fatal(err)
		}
		var walIdentity [32]byte
		copy(walIdentity[:], identity)
		config := Config{NodeID: id, Cluster: cluster, WAL: wal, Transport: transport, ManagedAppendOnly: &ManagedAppendOnlyConfig{
			Binding: ManagedAppendOnlyBinding{ApplicationProtocol: managedRequestAppendOnlyApplicationProtocol,
				BootstrapPlanDigest: hex.EncodeToString(bytes.Repeat([]byte{0x11}, 32)),
				MemberConfigDigest:  hex.EncodeToString(bytes.Repeat([]byte{byte(i + 10)}, 32)),
				Generation:          7, WALIdentity: walIdentity},
			Verify: func(context.Context, ManagedCheckpointCandidate) error { return nil },
		}}
		core, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		voters = append(voters, core)
		transport.cores[id] = core
	}
	defer func() {
		for _, wal := range wals {
			_ = wal.Close()
		}
	}()
	if _, _, err := voters[0].Propose(ctx, []byte("certified prefix")); err != nil {
		t.Fatal(err)
	}
	sealA := managedAppendOnlyTestSeal()
	sealA.GenerationAnchorHash = voters[0].GenerationAnchorHash()
	sealA.PrefixHash, _ = voters[0].PrefixHash(1)
	sealA.NextLeaderOrder, sealA.FollowingLeaderOrder, _ = voters[0].CheckpointLeaderOrders(1)
	valueA, err := EncodeCheckpointSeal(sealA)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := voters[0].Propose(ctx, valueA); err != nil {
		t.Fatalf("certify physical-floor root A: %v", err)
	}
	sealB := sealA
	sealB.RootHash = sha256.Sum256([]byte("alternate root B at the same cut"))
	sealB.StateHash = sha256.Sum256([]byte("alternate state B"))
	valueB, err := EncodeCheckpointSeal(sealB)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := voters[0].Propose(ctx, valueB); err != nil {
		t.Fatalf("certify same-cut alternate root B: %v", err)
	}
	decisionA, ok := voters[0].CertifiedValue(2)
	if !ok {
		t.Fatal("root A quorum certificate missing")
	}
	decisionB, ok := voters[0].CertifiedValue(3)
	if !ok {
		t.Fatal("root B quorum certificate missing")
	}

	identity := bytes.Repeat([]byte{0x6d}, 32)
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := wal.BindIdentity(identity); err != nil {
		t.Fatal(err)
	}
	var walIdentity [32]byte
	copy(walIdentity[:], identity)
	var verifierCalls int
	learner, err := NewLearner(Config{NodeID: "d", Cluster: cluster, WAL: wal, Transport: transport, ManagedAppendOnly: &ManagedAppendOnlyConfig{
		Binding: ManagedAppendOnlyBinding{ApplicationProtocol: managedRequestAppendOnlyApplicationProtocol,
			BootstrapPlanDigest: hex.EncodeToString(bytes.Repeat([]byte{0x11}, 32)),
			MemberConfigDigest:  hex.EncodeToString(bytes.Repeat([]byte{0x44}, 32)),
			Generation:          7, WALIdentity: walIdentity},
		Verify: func(_ context.Context, candidate ManagedCheckpointCandidate) error {
			verifierCalls++
			if !bytes.Equal(candidate.Value, valueB) || candidate.Hash != ValueHash(sha256.Sum256(valueB)) {
				t.Fatal("root verifier did not receive exact B bytes")
			}
			return nil
		},
	}})
	if err != nil {
		t.Fatalf("create managed learner D: %v", err)
	}
	learner.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := learner.RestoreCheckpointBase(ctx, sealA, decisionA); err != nil {
		t.Fatalf("install actual A physical recovery floor: %v", err)
	}
	if err := learner.AcceptCertifiedValues([]DecidedValue{decisionA, decisionB}); err != nil {
		t.Fatalf("replay actual A/B certified suffix: %v", err)
	}
	selected, ok, err := learner.LatestManagedCheckpointSeal()
	if err != nil || !ok || selected.RootHash != sealB.RootHash || selected.Index != sealA.Index {
		t.Fatalf("same-cut certified B was not selected above physical A floor: %+v ok=%t err=%v", selected, ok, err)
	}
	if err := learner.ValidateCheckpointBase(ctx, sealB, decisionB); err == nil {
		t.Fatal("legacy recovery-base equality unexpectedly accepted B over physical A")
	}
	candidate := ManagedCheckpointCandidate{Value: valueB, Hash: decisionB.Hash, Seal: sealB, DecisionSlot: 3, ConfigID: 1}
	if err := learner.ValidateManagedCheckpointRoot(ctx, candidate); err != nil {
		t.Fatalf("read-only root selection rejected actual quorum-certified B at floor A: %v", err)
	}
	tamperedPrefix := candidate
	tamperedPrefix.Seal.PrefixHash[0] ^= 1
	if err := learner.ValidateManagedCheckpointRoot(ctx, tamperedPrefix); err == nil {
		t.Fatal("tampered checkpoint prefix proof was accepted")
	}
	tamperedHistory := candidate
	tamperedHistory.Seal.Membership = &MembershipRecord{}
	if err := learner.ValidateManagedCheckpointRoot(ctx, tamperedHistory); err == nil {
		t.Fatal("tampered checkpoint history proof was accepted")
	}
	if verifierCalls != 1 {
		t.Fatalf("root verifier calls=%d, want one", verifierCalls)
	}
	learner.mu.Lock()
	corrupt := learner.decided[3]
	corrupt.Certificate = bytes.Clone(corrupt.Certificate)
	corrupt.Certificate[len(corrupt.Certificate)-1] ^= 1
	learner.decided[3] = corrupt
	learner.mu.Unlock()
	if err := learner.ValidateManagedCheckpointRoot(ctx, candidate); err == nil {
		t.Fatal("tampered B quorum certificate was accepted")
	}
	if learner.IsVoter() {
		t.Fatal("historic A/B certificate verification promoted learner D")
	}
}
