//go:build rhiza_local_testhooks

package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/qlog"
)

func verifyUnchangedISRMembershipDrift(t *testing.T, ctx context.Context, lagger *Core, freezeSlot Slot, left, right MembershipRecord) {
	t.Helper()
	if observation, err := lagger.ObserveManagedAppendOnlyControl(ctx); err != nil || !observation.BelongsTo(lagger) {
		t.Fatalf("unchanged ISR baseline: %v", err)
	}
	before, err := lagger.managedControlSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	hit := false
	restore := localtesthooks.Set(func(event string) {
		if event != "core:managed-control:before-snapshot-recheck" || hit {
			return
		}
		hit = true
		lagger.mu.Lock()
		v := lagger.decided[freezeSlot]
		v.Certificate = bytes.Clone(left.Transitions[0].Freeze.Certificate)
		lagger.decided[freezeSlot] = v
		lagger.mu.Unlock()
	})
	_, observeErr := lagger.ObserveManagedAppendOnlyControl(ctx)
	restore()
	after, snapshotErr := lagger.managedControlSnapshot()
	if snapshotErr != nil || !sameManagedControlSnapshotValues(before, after) {
		t.Fatalf("test changed ISR/authority, err=%v", snapshotErr)
	}
	if !hit || observeErr == nil {
		t.Fatalf("unchanged ISR accepted changed membership proof: hook=%t err=%v", hit, observeErr)
	}
	lagger.mu.Lock()
	v := lagger.decided[freezeSlot]
	v.Certificate = bytes.Clone(right.Transitions[0].Freeze.Certificate)
	lagger.decided[freezeSlot] = v
	lagger.mu.Unlock()
}

// Synthetic floor and durability markers isolate this final recheck. They are
// deliberately not claimed as a reproducible physical compaction state.
func verifyUnchangedISRUnavailableBecomesAvailable(t *testing.T, ctx context.Context, leader *Core, initial Cluster, freeze Slot) {
	t.Helper()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	reader := newCore("n9", &initial, wal, nil)
	reader.reconfigEnabled = true
	reader.walIdentity = hex.EncodeToString(make([]byte, 32))
	reader.managedAppendOnly = &ManagedAppendOnlyConfig{Binding: ManagedAppendOnlyBinding{ApplicationProtocol: managedRequestAppendOnlyApplicationProtocol}, Verify: func(context.Context, ManagedCheckpointCandidate) error { return nil }}
	for slot := Slot(1); slot <= leader.Tip(); slot++ {
		if err := reader.AcceptCertifiedValue(mustDecision(t, leader, slot)); err != nil {
			t.Fatal(err)
		}
	}
	cut := reader.Tip() - 1
	prefix, ok := reader.PrefixHash(cut)
	if !ok {
		t.Fatal("missing old cut")
	}
	next, following, err := reader.CheckpointLeaderOrders(cut)
	if err != nil {
		t.Fatal(err)
	}
	history, err := reader.CheckpointMembership(cut)
	if err != nil {
		t.Fatal(err)
	}
	seal := CheckpointSeal{Index: cut, ConfigID: reader.ConfigIDForSlot(cut + 1), PrefixHash: prefix, RootHash: sha256.Sum256([]byte("old root")), StateHash: sha256.Sum256([]byte("old state")), NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &history}
	value, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	proposal := Proposal{Priority: highestPriority, ProposerID: "n9", Hash: ValueHash(sha256.Sum256(value)), Value: value}
	reader.mu.Lock()
	reader.floor = cut + 1
	reader.durable[freeze] = false
	reader.mu.Unlock()
	if _, err := reader.Record(ctx, RecordRequest{Slot: reader.Tip() + 1, Step: 4, ConfigID: reader.ConfigID(), Proposal: proposal}); err != nil {
		t.Fatalf("install pending QCKP with unavailable local proof: %v", err)
	}
	if _, err := reader.ObserveManagedAppendOnlyControl(ctx); err != nil {
		t.Fatalf("unchanged unavailable baseline: %v", err)
	}
	before, err := reader.managedControlSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	hit := false
	restore := localtesthooks.Set(func(event string) {
		if event != "core:managed-control:before-snapshot-recheck" || hit {
			return
		}
		hit = true
		reader.mu.Lock()
		reader.durable[freeze] = true
		reader.mu.Unlock()
	})
	_, err = reader.ObserveManagedAppendOnlyControl(ctx)
	restore()
	after, afterErr := reader.managedControlSnapshot()
	if afterErr != nil || !sameManagedControlSnapshotValues(before, after) {
		t.Fatalf("ISR changed during proof availability gate: %v", afterErr)
	}
	if !hit || err == nil {
		t.Fatalf("unavailable-to-available transition accepted: hook=%t err=%v", hit, err)
	}
}
