package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func newManagedAppendOnlyTestCore(t *testing.T, verify ManagedCheckpointCandidateVerifier) (*Core, *qlog.WAL) {
	t.Helper()
	config, wal := managedAppendOnlyTestConfig(t, "rhiza-managed-request-append-only/v1", verify)
	core, err := New(config)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	return core, wal
}

func managedAppendOnlyTestConfig(t *testing.T, protocol string, verify ManagedCheckpointCandidateVerifier) (Config, *qlog.WAL) {
	t.Helper()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	identity := bytes.Repeat([]byte{0x5a}, 32)
	if err := wal.BindIdentity(identity); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	config := Config{
		NodeID:  "n1",
		Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: "n1", WALIdentity: hex.EncodeToString(identity)}}},
		WAL:     wal,
		ManagedAppendOnly: &ManagedAppendOnlyConfig{
			Binding: ManagedAppendOnlyBinding{
				ApplicationProtocol: protocol,
				BootstrapPlanDigest: hex.EncodeToString(bytes.Repeat([]byte{0x11}, 32)),
				MemberConfigDigest:  hex.EncodeToString(bytes.Repeat([]byte{0x22}, 32)),
				Generation:          0,
				WALIdentity:         [32]byte(identity),
			},
			Verify: verify,
		},
	}
	return config, wal
}

func managedAppendOnlyTestSeal() CheckpointSeal {
	return CheckpointSeal{
		ConfigID: 1, Index: 1,
		RootHash: sha256.Sum256([]byte("managed-root")), StateHash: sha256.Sum256([]byte("managed-state")),
		PrefixHash: sha256.Sum256([]byte("managed-prefix")), NextLeaderOrder: []NodeID{"n1"},
	}
}

func TestManagedAppendOnlyRecordValidatesOriginalCandidateOutsideLocksWithoutPrepare(t *testing.T) {
	seal := managedAppendOnlyTestSeal()
	value, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	wantHash := ValueHash(sha256.Sum256(value))
	var core *Core
	calls := 0
	core, _ = newManagedAppendOnlyTestCore(t, func(ctx context.Context, candidate ManagedCheckpointCandidate) error {
		calls++
		if err := ctx.Err(); err != nil {
			return err
		}
		if !bytes.Equal(candidate.Value, value) || candidate.Hash != wantHash || candidate.DecisionSlot != Slot(calls+1) || candidate.ConfigID != 1 || !sameManagedCheckpointSeal(candidate.Seal, seal) {
			t.Fatal("verifier did not receive the exact QCKP bytes and decision identity")
		}
		if !core.mu.TryLock() {
			t.Fatal("verifier ran while Core.mu was held")
		}
		core.mu.Unlock()
		stripe := &core.recordLocks[uint64(candidate.DecisionSlot)%uint64(len(core.recordLocks))]
		if !stripe.TryLock() {
			t.Fatal("verifier ran while the decision-slot record stripe was held")
		}
		stripe.Unlock()
		return nil
	})
	proposal := Proposal{Priority: highestPriority, ProposerID: "n1", Hash: wantHash, Value: bytes.Clone(value)}
	if _, err := core.Record(context.Background(), RecordRequest{Slot: 2, Step: 4, ConfigID: 1, Proposal: proposal}); err != nil {
		t.Fatalf("managed direct Record failed without a prepared fence: %v", err)
	}
	if calls != 1 {
		t.Fatalf("managed candidate verifier calls=%d, want 1", calls)
	}
	if _, prepared := core.preparedCheckpoints[seal.Index]; prepared {
		t.Fatal("managed Record manufactured a prepared fence")
	}
	core.mu.RLock()
	got := core.recorders[2].FirstCurrent
	core.mu.RUnlock()
	if got == nil || got.Hash != wantHash {
		t.Fatal("managed direct Record did not append the expected recorder receipt")
	}
	if _, err := core.Record(context.Background(), RecordRequest{Slot: 3, Step: 4, ConfigID: 1, Proposal: Proposal{Priority: highestPriority, ProposerID: "n1", Hash: wantHash}}); err != nil {
		t.Fatalf("hash-only managed Record did not resolve the original staged QCKP: %v", err)
	}
	if calls != 2 {
		t.Fatalf("hash-only managed candidate verifier calls=%d, want 2", calls)
	}
}

func TestManagedAppendOnlyRecordRefusesUnverifiedCandidateBeforeReceipt(t *testing.T) {
	denied := errors.New("candidate proof refused")
	core, _ := newManagedAppendOnlyTestCore(t, func(context.Context, ManagedCheckpointCandidate) error { return denied })
	value, err := EncodeCheckpointSeal(managedAppendOnlyTestSeal())
	if err != nil {
		t.Fatal(err)
	}
	hash := ValueHash(sha256.Sum256(value))
	proposal := Proposal{Priority: highestPriority, ProposerID: "n1", Hash: hash, Value: value}
	if _, err := core.Record(context.Background(), RecordRequest{Slot: 2, Step: 4, ConfigID: 1, Proposal: proposal}); !errors.Is(err, denied) {
		t.Fatalf("Record error=%v, want candidate verifier refusal", err)
	}
	core.mu.RLock()
	_, hasReceipt := core.recorders[2]
	core.mu.RUnlock()
	if hasReceipt {
		t.Fatal("unverified candidate advanced recorder state")
	}
}

func TestManagedAppendOnlyHashOnlyKnownRecorderRequiresOriginalBytes(t *testing.T) {
	core, _ := newManagedAppendOnlyTestCore(t, func(context.Context, ManagedCheckpointCandidate) error {
		t.Fatal("candidate verifier was called without original bytes")
		return nil
	})
	value, err := EncodeCheckpointSeal(managedAppendOnlyTestSeal())
	if err != nil {
		t.Fatal(err)
	}
	hash := ValueHash(sha256.Sum256(value))
	proposal := Proposal{Priority: highestPriority, ProposerID: "n1", Hash: hash}
	core.mu.Lock()
	core.recorders[2] = ISR{Step: 4, FirstCurrent: cloneProposal(&proposal), AggregateCurrent: cloneProposal(&proposal)}
	delete(core.values, hash)
	core.mu.Unlock()
	if _, err := core.Record(context.Background(), RecordRequest{Slot: 2, Step: 4, ConfigID: 1, Proposal: proposal}); !errors.Is(err, ErrManagedAppendOnlyCandidateBytesUnavailable) {
		t.Fatalf("hash-only known record error=%v, want missing managed candidate bytes", err)
	}
	core.mu.RLock()
	state := core.recorders[2]
	_, staged := core.values[hash]
	core.mu.RUnlock()
	if state.Step != 4 || !sameProposal(state.FirstCurrent, &proposal) || staged {
		t.Fatal("missing-byte request mutated or staged recorder state")
	}
}

func TestManagedAppendOnlyRejectsUnsupportedApplicationProtocol(t *testing.T) {
	config, wal := managedAppendOnlyTestConfig(t, "rhiza-managed-request/v1", func(context.Context, ManagedCheckpointCandidate) error { return nil })
	defer wal.Close()
	if _, err := New(config); err == nil {
		t.Fatal("accepted an application protocol that does not enable append-only V1")
	}
}

func TestManagedAppendOnlyAcceptsOnlyTwoV1Profiles(t *testing.T) {
	protocols := []string{
		"rhiza-managed-request-append-only/v1",
		"rhiza-managed-request-graph-append-only/v1",
	}
	for _, protocol := range protocols {
		t.Run(protocol, func(t *testing.T) {
			config, wal := managedAppendOnlyTestConfig(t, protocol, func(context.Context, ManagedCheckpointCandidate) error { return nil })
			defer wal.Close()
			if _, err := New(config); err != nil {
				t.Fatalf("approved protocol rejected: %v", err)
			}
		})
	}
}

func TestManagedAppendOnlyRejectsLocallyWrongCutConfig(t *testing.T) {
	core, _ := newManagedAppendOnlyTestCore(t, func(context.Context, ManagedCheckpointCandidate) error { return nil })
	if _, _, err := core.Propose(context.Background(), []byte("local-history")); err != nil {
		t.Fatal(err)
	}
	cut := core.Tip()
	prefix, _ := core.PrefixHash(cut)
	next, following, err := core.CheckpointLeaderOrders(cut)
	if err != nil {
		t.Fatal(err)
	}
	seal := managedAppendOnlyTestSeal()
	seal.Index = cut
	seal.ConfigID = 2 // locally available history says config 1 at this cut.
	seal.PrefixHash = prefix
	seal.NextLeaderOrder, seal.FollowingLeaderOrder = next, following
	value, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	candidate := ManagedCheckpointCandidate{Value: value, Hash: ValueHash(sha256.Sum256(value)), Seal: seal, DecisionSlot: cut + 1, ConfigID: 1}
	if err := core.ValidateManagedCheckpointCandidate(context.Background(), candidate); err == nil {
		t.Fatal("accepted a checkpoint whose cut config conflicts with local history")
	}
}

func TestManagedAppendOnlyRejectsLocallyWrongCutMembership(t *testing.T) {
	config, wal := managedAppendOnlyTestConfig(t, "rhiza-managed-request-append-only/v1", func(context.Context, ManagedCheckpointCandidate) error { return nil })
	config.EnableReconfiguration = true
	core, err := New(config)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	if _, _, err := core.Propose(context.Background(), []byte("local-history")); err != nil {
		t.Fatal(err)
	}
	cut := core.Tip()
	prefix, _ := core.PrefixHash(cut)
	next, following, err := core.CheckpointLeaderOrders(cut)
	if err != nil {
		t.Fatal(err)
	}
	membership, err := core.CheckpointMembership(cut)
	if err != nil {
		t.Fatal(err)
	}
	membership.Genesis.ConfigID = 2 // differs from the local recovered membership history.
	seal := managedAppendOnlyTestSeal()
	seal.Index, seal.PrefixHash = cut, prefix
	seal.NextLeaderOrder, seal.FollowingLeaderOrder = next, following
	seal.Membership = &membership
	value, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	candidate := ManagedCheckpointCandidate{Value: value, Hash: ValueHash(sha256.Sum256(value)), Seal: seal, DecisionSlot: cut + 1, ConfigID: 1}
	if err := core.ValidateManagedCheckpointCandidate(context.Background(), candidate); err == nil {
		t.Fatal("accepted checkpoint membership that conflicts with local history")
	}
}

func TestManagedRecoveryDoesNotPromoteLegacyPreparedMarker(t *testing.T) {
	dir := t.TempDir()
	identity := bytes.Repeat([]byte{0x5a}, 32)
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.BindIdentity(identity); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	cluster := Cluster{ConfigID: 1, Members: []Member{{ID: "n1", WALIdentity: hex.EncodeToString(identity)}}}
	legacy, err := New(Config{NodeID: "n1", Cluster: cluster, WAL: wal})
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	if _, _, err := legacy.Propose(context.Background(), []byte("certified-prefix")); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	cut := legacy.Tip()
	prefix, _ := legacy.PrefixHash(cut)
	next, following, err := legacy.CheckpointLeaderOrders(cut)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	seal := managedAppendOnlyTestSeal()
	seal.Index, seal.PrefixHash = cut, prefix
	seal.NextLeaderOrder, seal.FollowingLeaderOrder = next, following
	legacy.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
	if err := legacy.PrepareCheckpoint(context.Background(), seal); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{
		NodeID: "n1", Cluster: cluster, WAL: reopened,
		ManagedAppendOnly: &ManagedAppendOnlyConfig{
			Binding: ManagedAppendOnlyBinding{
				ApplicationProtocol: "rhiza-managed-request-append-only/v1",
				BootstrapPlanDigest: hex.EncodeToString(bytes.Repeat([]byte{0x11}, 32)),
				MemberConfigDigest:  hex.EncodeToString(bytes.Repeat([]byte{0x22}, 32)),
				WALIdentity:         [32]byte(identity),
			},
			Verify: func(context.Context, ManagedCheckpointCandidate) error { return nil },
		},
	}
	managed, err := New(config)
	if err != nil {
		_ = reopened.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, ok := managed.preparedCheckpoints[seal.Index]; ok {
		t.Fatal("managed recovery promoted a legacy prepared marker into V1 vote authority")
	}
	markerCount := 0
	if err := reopened.Scan(func(entry qlog.Entry) error {
		if entry.Type == qlog.EntryCheckpointVerified && entry.Slot == uint64(seal.Index) && entry.Hash == seal.RootHash {
			markerCount++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if markerCount != 1 {
		t.Fatalf("managed recovery preserved %d legacy marker entries, want exactly 1", markerCount)
	}
	value, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	proposal := Proposal{Priority: highestPriority, ProposerID: "n1", Hash: ValueHash(sha256.Sum256(value)), Value: value}
	if _, err := managed.Record(context.Background(), RecordRequest{Slot: cut + 1, Step: 4, ConfigID: 1, Proposal: proposal}); err != nil {
		t.Fatalf("V1 candidate Record incorrectly required legacy marker promotion: %v", err)
	}
}

func TestManagedRecoveryChecksLegacyMarkerOrderWithoutPromotingIt(t *testing.T) {
	config, wal := managedAppendOnlyTestConfig(t, "rhiza-managed-request-append-only/v1", func(context.Context, ManagedCheckpointCandidate) error { return nil })
	seal2 := managedAppendOnlyTestSeal()
	seal2.Index = 2
	value2, err := EncodeCheckpointSeal(seal2)
	if err != nil {
		t.Fatal(err)
	}
	seal1 := managedAppendOnlyTestSeal()
	value1, err := EncodeCheckpointSeal(seal1)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range []qlog.Entry{
		{Slot: uint64(seal2.Index), Hash: seal2.RootHash, Type: qlog.EntryCheckpointVerified, Payload: value2},
		{Slot: uint64(seal1.Index), Hash: seal1.RootHash, Type: qlog.EntryCheckpointVerified, Payload: value1},
	} {
		if err := wal.Append(entry); err != nil {
			_ = wal.Close()
			t.Fatal(err)
		}
	}
	if err := wal.Sync(); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	_, err = New(config)
	_ = wal.Close()
	if err == nil || !strings.Contains(err.Error(), "managed marker ordering") {
		t.Fatalf("managed recovery error=%v, want scan-time marker-order refusal", err)
	}
}

func TestLatestManagedCheckpointSealRanksActualDecisionSlot(t *testing.T) {
	core, _ := newManagedAppendOnlyTestCore(t, func(context.Context, ManagedCheckpointCandidate) error { return nil })
	ctx := context.Background()
	if slot, _, err := core.Propose(ctx, []byte("certified-prefix")); err != nil || slot != 1 {
		t.Fatalf("prefix proposal slot=%d err=%v", slot, err)
	}
	cut := core.Tip()
	prefix, ok := core.PrefixHash(cut)
	if !ok {
		t.Fatal("missing actual certified prefix")
	}
	next, following, err := core.CheckpointLeaderOrders(cut)
	if err != nil {
		t.Fatal(err)
	}
	seal := CheckpointSeal{
		ConfigID: 1, Index: cut,
		RootHash: sha256.Sum256([]byte("same-managed-root")), StateHash: sha256.Sum256([]byte("same-managed-state")),
		PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following,
	}
	value, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := core.Propose(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := core.Propose(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	if second <= first {
		t.Fatalf("same original QCKP decision slots=%d,%d", first, second)
	}
	selected, ok, err := core.LatestManagedCheckpointSeal()
	if err != nil || !ok {
		t.Fatalf("managed selector result ok=%v err=%v", ok, err)
	}
	if selected.Index != cut || selected.DecisionSlot != second || selected.RootHash != seal.RootHash {
		t.Fatalf("selector chose cut=%d slot=%d root=%x, want cut=%d slot=%d root=%x", selected.Index, selected.DecisionSlot, selected.RootHash, cut, second, seal.RootHash)
	}
}
