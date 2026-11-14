package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

// A second, equally valid proof of a decision can reach a lagging voter first.
// This records the observed exact-certificate mismatch without relaxing it.
func TestTenVoterEquivalentMembershipCertificates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var members []Member
	for i := 0; i < 10; i++ {
		members = append(members, Member{ID: NodeID(fmt.Sprintf("n%d", i))})
	}
	initial := Cluster{ConfigID: 1, Members: members}
	transport := &clusterTransport{cores: map[NodeID]*Core{}, down: map[NodeID]bool{}, dropDecision: map[NodeID]bool{}}
	for _, member := range members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), string(member.ID)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core := newCore(member.ID, &initial, wal, transport)
		core.reconfigEnabled = true
		core.reconfigAdmission = func(context.Context, Cluster, Slot, [32]byte) error { return nil }
		transport.cores[member.ID] = core
	}
	leader, lagger := transport.cores["n0"], transport.cores["n9"]
	transport.fail("n9")
	target := Cluster{ConfigID: 2, Members: append(slices.Clone(members[:8]), members[9])}
	if _, err := leader.BeginReconfiguration(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := leader.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := leader.Propose(ctx, []byte("after-reconfiguration")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := leader.Propose(ctx, []byte("additional-certified-slot")); err != nil {
		t.Fatal(err)
	}
	if lagger.Tip() != 0 {
		t.Fatalf("lagger unexpectedly received history: tip=%d", lagger.Tip())
	}
	for slot := Slot(1); slot <= leader.Tip(); slot++ {
		value, ok := leader.CertifiedValue(slot)
		if !ok {
			t.Fatalf("missing leader slot %d", slot)
		}
		if slot == 1 {
			config, decision, err := decodeCertificate(value.Certificate)
			if err != nil {
				t.Fatal(err)
			}
			if len(decision.Summaries) < 2 {
				t.Fatal("missing recorder quorum")
			}
			slices.Reverse(decision.Summaries)
			value.Certificate, err = encodeCertificate(config, decision)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(value.Certificate, mustDecision(t, leader, slot).Certificate) {
				t.Fatal("certificate did not change")
			}
		}
		if err := lagger.AcceptCertifiedValue(value); err != nil {
			t.Fatalf("lagger rejected slot %d: %v", slot, err)
		}
	}
	if err := lagger.EnsureDurableThrough(ctx, leader.Tip()); err != nil {
		t.Fatal(err)
	}
	left, err := leader.CheckpointMembership(leader.Tip())
	if err != nil {
		t.Fatal(err)
	}
	right, err := lagger.CheckpointMembership(leader.Tip())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validateMembershipHistory(initial, cloneMembershipRecord(left)); err != nil {
		t.Fatal("leader proof invalid", err)
	}
	if _, err := validateMembershipHistory(initial, cloneMembershipRecord(right)); err != nil {
		t.Fatal("lagger proof invalid", err)
	}
	if sameMembershipRecord(left, right) {
		t.Fatal("expected exact-byte comparison to reject alternate valid proof")
	}
	if !sameCluster(leader.CurrentCluster(), lagger.CurrentCluster()) {
		t.Fatal("voters disagree on committed membership")
	}
	prefix, ok := leader.PrefixHash(leader.Tip())
	if !ok {
		t.Fatal("leader prefix unavailable")
	}
	next, following, err := leader.CheckpointLeaderOrders(leader.Tip())
	if err != nil {
		t.Fatal(err)
	}
	seal := CheckpointSeal{Index: leader.Tip(), ConfigID: target.ConfigID, RootHash: sha256.Sum256([]byte("test root")), StateHash: sha256.Sum256([]byte("test state")), PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &left}
	lagger.walIdentity = hex.EncodeToString(make([]byte, 32))
	lagger.managedAppendOnly = &ManagedAppendOnlyConfig{Binding: ManagedAppendOnlyBinding{ApplicationProtocol: managedRequestAppendOnlyApplicationProtocol}, Verify: func(context.Context, ManagedCheckpointCandidate) error { return nil }}
	if _, err := lagger.checkpointMembershipForSeal(seal); err != nil {
		t.Fatal("candidate membership proof invalid", err)
	}
	value, err := EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	candidate := ManagedCheckpointCandidate{Value: value, Hash: ValueHash(sha256.Sum256(value)), DecisionSlot: seal.Index + 1, ConfigID: target.ConfigID, Seal: seal}
	if err := lagger.ValidateManagedCheckpointCandidate(ctx, candidate); err != nil {
		t.Fatalf("valid alternative proof rejected: %v", err)
	}
	freezeSlot := right.Transitions[0].Freeze.Slot
	t.Run("unavailable history becomes retained during verification", func(t *testing.T) {
		lagger.mu.Lock()
		oldTip, oldDurable := lagger.tip, lagger.durable[freezeSlot]
		lagger.tip, lagger.durable[freezeSlot] = seal.Index-1, false
		lagger.mu.Unlock()
		defer func() {
			lagger.mu.Lock()
			lagger.tip, lagger.durable[freezeSlot] = oldTip, oldDurable
			lagger.mu.Unlock()
		}()
		lagger.managedAppendOnly.Verify = func(context.Context, ManagedCheckpointCandidate) error { return nil }
		if err := lagger.ValidateManagedCheckpointCandidate(ctx, candidate); err != nil {
			t.Fatalf("stable unavailable state unexpectedly rejected: %v", err)
		}
		called := false
		lagger.managedAppendOnly.Verify = func(context.Context, ManagedCheckpointCandidate) error {
			called = true
			lagger.mu.Lock()
			lagger.tip = seal.Index // exact same nondurable-freeze error remains
			lagger.mu.Unlock()
			return nil
		}
		err := lagger.ValidateManagedCheckpointCandidate(ctx, candidate)
		if !called {
			t.Fatalf("test did not reach unlocked object verification: %v", err)
		}
		if err == nil {
			t.Fatal("accepted unavailable membership after cut became retained")
		}
	})
	lagger.managedAppendOnly.Verify = func(context.Context, ManagedCheckpointCandidate) error {
		lagger.mu.Lock()
		v := lagger.decided[freezeSlot]
		v.Certificate = bytes.Clone(left.Transitions[0].Freeze.Certificate)
		lagger.decided[freezeSlot] = v
		lagger.mu.Unlock()
		return nil
	}
	if err := lagger.ValidateManagedCheckpointCandidate(ctx, candidate); err == nil {
		t.Fatal("accepted local proof drift during object verification")
	}
	lagger.mu.Lock()
	v := lagger.decided[freezeSlot]
	v.Certificate = bytes.Clone(right.Transitions[0].Freeze.Certificate)
	lagger.decided[freezeSlot] = v
	lagger.mu.Unlock()
	lagger.managedAppendOnly.Verify = func(context.Context, ManagedCheckpointCandidate) error { return nil }
	proposal := Proposal{Priority: highestPriority, ProposerID: "n9", Hash: candidate.Hash, Value: bytes.Clone(candidate.Value)}
	countReceipts := func(entries []qlog.Entry) int {
		count := 0
		for _, e := range entries {
			if e.Type == qlog.EntryReceipt {
				count++
			}
		}
		return count
	}
	beforeRecord, err := lagger.wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	lagger.managedAppendOnly.Verify = func(context.Context, ManagedCheckpointCandidate) error {
		lagger.mu.Lock()
		v := lagger.decided[freezeSlot]
		v.Certificate = bytes.Clone(left.Transitions[0].Freeze.Certificate)
		lagger.decided[freezeSlot] = v
		lagger.mu.Unlock()
		return nil
	}
	if _, err := lagger.Record(ctx, RecordRequest{Slot: candidate.DecisionSlot, Step: 4, ConfigID: target.ConfigID, Proposal: proposal}); err == nil {
		t.Fatal("Record accepted membership snapshot drift")
	}
	afterRecord, err := lagger.wal.Read()
	if err != nil {
		t.Fatal(err)
	}
	if countReceipts(beforeRecord) != countReceipts(afterRecord) {
		t.Fatal("rejected drift wrote a receipt")
	}
	lagger.mu.Lock()
	v = lagger.decided[freezeSlot]
	v.Certificate = bytes.Clone(right.Transitions[0].Freeze.Certificate)
	lagger.decided[freezeSlot] = v
	lagger.mu.Unlock()
	lagger.managedAppendOnly.Verify = func(context.Context, ManagedCheckpointCandidate) error { return nil }
	t.Run("post-validation pre-receipt membership drift", func(t *testing.T) {
		var verified, boundary bool
		lagger.mu.RLock()
		_, hadRecorder := lagger.recorders[candidate.DecisionSlot]
		_, hadDecision := lagger.decided[candidate.DecisionSlot]
		lagger.mu.RUnlock()
		if hadRecorder || hadDecision {
			t.Fatal("fixture already has accepted slot state")
		}
		before, err := lagger.wal.Read()
		if err != nil {
			t.Fatal(err)
		}
		lagger.managedAppendOnly.Verify = func(context.Context, ManagedCheckpointCandidate) error { verified = true; return nil }
		lagger.recordBeforeAppend = func() {
			boundary = true
			lagger.mu.Lock()
			v := lagger.decided[freezeSlot]
			v.Certificate = bytes.Clone(left.Transitions[0].Freeze.Certificate)
			lagger.decided[freezeSlot] = v
			lagger.mu.Unlock()
		}
		defer func() {
			lagger.recordBeforeAppend = nil
			lagger.managedAppendOnly.Verify = func(context.Context, ManagedCheckpointCandidate) error { return nil }
			lagger.mu.Lock()
			v := lagger.decided[freezeSlot]
			v.Certificate = bytes.Clone(right.Transitions[0].Freeze.Certificate)
			lagger.decided[freezeSlot] = v
			lagger.mu.Unlock()
		}()
		summary, recordErr := lagger.Record(ctx, RecordRequest{Slot: candidate.DecisionSlot, Step: 4, ConfigID: target.ConfigID, Proposal: proposal})
		if !verified || !boundary || recordErr == nil || recordErr.Error() != "managed checkpoint membership differs from available local history" || summary != (Summary{}) {
			t.Fatalf("final boundary verified=%t reached=%t summary=%+v result=%v", verified, boundary, summary, recordErr)
		}
		after, err := lagger.wal.Read()
		if err != nil {
			t.Fatal(err)
		}
		if countReceipts(before) != countReceipts(after) {
			t.Fatal("final guard refusal wrote a receipt")
		}
		lagger.mu.RLock()
		_, recorder := lagger.recorders[candidate.DecisionSlot]
		_, decided := lagger.decided[candidate.DecisionSlot]
		lagger.mu.RUnlock()
		if recorder || decided {
			t.Fatalf("final guard refusal installed state: recorder=%t decision=%t", recorder, decided)
		}
	})
	if _, err := lagger.Record(ctx, RecordRequest{Slot: candidate.DecisionSlot, Step: 4, ConfigID: target.ConfigID, Proposal: proposal}); err != nil {
		t.Fatalf("real Record rejected verified alternative proof: %v", err)
	}
	verifyUnchangedISRMembershipDrift(t, ctx, lagger, freezeSlot, left, right)
	verifyUnchangedISRUnavailableBecomesAvailable(t, ctx, leader, initial, freezeSlot)
	for i := 0; i < 8; i++ {
		peer := transport.cores[NodeID(fmt.Sprintf("n%d", i))]
		if err := peer.WaitTip(ctx, seal.Index); err != nil {
			t.Fatalf("peer %d catchup: %v", i, err)
		}
		peer.SetCheckpointValidator(func(context.Context, CheckpointSeal) error { return nil })
		if err := peer.PrepareCheckpoint(ctx, seal); err != nil {
			t.Fatalf("peer %d prepare: %v", i, err)
		}
	}
	if _, _, err := leader.Propose(ctx, value); err != nil {
		t.Fatalf("certify exact checkpoint: %v", err)
	}
	outer, ok := leader.CertifiedValue(candidate.DecisionSlot)
	if !ok || !bytes.Equal(outer.Value, value) || outer.Hash != candidate.Hash {
		t.Fatal("producer did not certify original QCKP")
	}
	if err := lagger.AcceptCertifiedValue(outer); err != nil {
		t.Fatalf("install certified QCKP: %v", err)
	}
	if err := lagger.ValidateManagedCheckpointRoot(ctx, candidate); err != nil {
		t.Fatalf("certified root rejected verified alternative proof: %v", err)
	}
	t.Run("later terminal nondurable does not invalidate older root", func(t *testing.T) {
		transport.recover("n9")
		later := Cluster{ConfigID: 3, Members: append(slices.Clone(members[:7]), members[9])}
		if _, err := leader.BeginReconfiguration(ctx, later); err != nil {
			t.Fatal(err)
		}
		if err := leader.FinishReconfiguration(ctx); err != nil {
			t.Fatal(err)
		}
		terminal := leader.Tip()
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer wal.Close()
		reader := newCore("n9", &initial, wal, transport)
		reader.reconfigEnabled = true
		reader.walIdentity = hex.EncodeToString(make([]byte, 32))
		reader.managedAppendOnly = &ManagedAppendOnlyConfig{Verify: func(context.Context, ManagedCheckpointCandidate) error { return nil }}
		reached, release := make(chan struct{}), make(chan struct{})
		var releaseOnce, reachedOnce sync.Once
		defer releaseOnce.Do(func() { close(release) })
		reader.commits = newGroupCommit(func() error {
			reader.mu.RLock()
			_, found := reader.decided[terminal]
			reader.mu.RUnlock()
			if found {
				reachedOnce.Do(func() { close(reached) })
				<-release
			}
			return wal.Sync()
		})
		for slot := Slot(1); slot < terminal; slot++ {
			if err := reader.AcceptCertifiedValue(mustDecision(t, leader, slot)); err != nil {
				t.Fatalf("replay slot %d: %v", slot, err)
			}
		}
		terminalDone := make(chan error, 1)
		terminalValue := mustDecision(t, leader, terminal)
		go func() { terminalDone <- reader.AcceptCertifiedValue(terminalValue) }()
		select {
		case <-reached:
		case <-ctx.Done():
			t.Fatal("terminal WAL sync gate not reached", ctx.Err())
		}
		reader.mu.RLock()
		advanced, terminalDurable := reader.tip >= terminal, reader.durable[terminal]
		reader.mu.RUnlock()
		if !advanced || terminalDurable || reader.ConfigID() != later.ConfigID {
			t.Fatal("later terminal did not reach accepted, nondurable state")
		}
		if err := reader.ValidateManagedCheckpointRoot(ctx, candidate); err != nil {
			t.Fatalf("valid earlier root rejected by later undurable transition: %v", err)
		}
		reader.mu.Lock()
		wasDurable := reader.durable[freezeSlot]
		reader.durable[freezeSlot] = false
		reader.mu.Unlock()
		if err := reader.ValidateManagedCheckpointRoot(ctx, candidate); err == nil {
			t.Fatal("accepted root with required freeze nondurable")
		}
		reader.mu.Lock()
		reader.durable[freezeSlot] = wasDurable
		requiredDecision := reader.decided[freezeSlot]
		delete(reader.decided, freezeSlot)
		reader.mu.Unlock()
		if err := reader.ValidateManagedCheckpointRoot(ctx, candidate); err == nil {
			t.Fatal("accepted root with missing required freeze")
		}
		reader.mu.Lock()
		reader.decided[freezeSlot] = requiredDecision
		reader.mu.Unlock()
		releaseOnce.Do(func() { close(release) })
		select {
		case err := <-terminalDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("terminal did not drain", ctx.Err())
		}
		reader.mu.Lock()
		invalid := reader.decided[freezeSlot]
		oldCert := bytes.Clone(invalid.Certificate)
		invalid.Certificate = []byte("invalid certificate")
		reader.decided[freezeSlot] = invalid
		reader.mu.Unlock()
		defer func() {
			reader.mu.Lock()
			v := reader.decided[freezeSlot]
			v.Certificate = oldCert
			reader.decided[freezeSlot] = v
			reader.mu.Unlock()
		}()
		if err := reader.ValidateManagedCheckpointRoot(ctx, candidate); err == nil {
			t.Fatal("accepted root with invalid required history")
		}
		before, err := reader.wal.Read()
		if err != nil {
			t.Fatal(err)
		}
		_, err = reader.Record(ctx, RecordRequest{Slot: reader.Tip() + 1, Step: 4, ConfigID: later.ConfigID, Proposal: proposal})
		if err == nil {
			t.Fatal("Record accepted invalid required history")
		}
		after, err := reader.wal.Read()
		if err != nil {
			t.Fatal(err)
		}
		if countReceipts(after) != countReceipts(before) {
			t.Fatal("rejected Record wrote a receipt")
		}
	})
	// Replace one old-configuration recorder in both freeze and terminal so
	// each proof remains independently bound to its own freeze quorum.
	alternate := cloneMembershipRecord(left)
	fc, freezeProof, err := decodeCertificate(alternate.Transitions[0].Freeze.Certificate)
	if err != nil {
		t.Fatal(err)
	}
	tc, terminalProof, err := decodeCertificate(alternate.Transitions[0].Terminal.Certificate)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[NodeID]bool)
	for _, s := range freezeProof.Summaries {
		seen[s.RecorderID] = true
	}
	var outsider NodeID
	for _, member := range initial.Members {
		if !seen[member.ID] {
			outsider = member.ID
			break
		}
	}
	if outsider == "" {
		t.Fatal("freeze proof has no alternative recorder")
	}
	replaced := terminalProof.Summaries[0].RecorderID
	found := false
	for i := range freezeProof.Summaries {
		if freezeProof.Summaries[i].RecorderID == replaced {
			freezeProof.Summaries[i].RecorderID = outsider
			found = true
			break
		}
	}
	if !found {
		t.Fatal("terminal recorder not in freeze proof")
	}
	terminalProof.Summaries[0].RecorderID = outsider
	alternate.Transitions[0].Freeze.Certificate, err = encodeCertificate(fc, freezeProof)
	if err != nil {
		t.Fatal(err)
	}
	alternate.Transitions[0].Terminal.Certificate, err = encodeCertificate(tc, terminalProof)
	if err != nil {
		t.Fatal(err)
	}
	if equal, err := EquivalentVerifiedMembershipHistories(ctx, initial, right, alternate); err != nil || !equal {
		t.Fatalf("independently valid alternate quorum rejected: %v", err)
	}
	// The separately supplied decoded seal must remain bound to the exact
	// QCKP bytes, even if the alternate history is independently valid.
	unbound := candidate
	unbound.Seal.Membership = &right
	if err := lagger.ValidateManagedCheckpointCandidate(ctx, unbound); err == nil {
		t.Fatal("accepted semantically equal but byte-different QCKP seal")
	}
	_, decision, err := decodeCertificate(left.Transitions[0].Freeze.Certificate)
	if err != nil {
		t.Fatal(err)
	}
	decision.Summaries = decision.Summaries[:5]
	bad := cloneMembershipRecord(left)
	bad.Transitions[0].Freeze.Certificate, err = encodeCertificate(1, decision)
	if err != nil {
		t.Fatal(err)
	}
	if equal, err := EquivalentVerifiedMembershipHistories(ctx, initial, right, bad); err == nil || equal {
		t.Fatal("old ten-voter five-recorder proof was accepted")
	}
	for _, kind := range []string{"wrong-config", "duplicate", "wrong-step", "wrong-fast-leader", "wrong-phase2-max", "terminal-outside-freeze"} {
		invalid := cloneMembershipRecord(left)
		cert := &invalid.Transitions[0].Freeze.Certificate
		if kind == "terminal-outside-freeze" {
			cert = &invalid.Transitions[0].Terminal.Certificate
		}
		config, proof, err := decodeCertificate(*cert)
		if err != nil {
			t.Fatal(err)
		}
		switch kind {
		case "wrong-config":
			config++
		case "duplicate":
			proof.Summaries[1] = proof.Summaries[0]
		case "wrong-step":
			proof.Step = 0
		case "wrong-phase2-max":
			proof.Step = 6
			for i := range proof.Summaries {
				proof.Summaries[i].Step = 6
				proof.Summaries[i].AggregatePrior = cloneProposal(&proof.Proposal)
			}
			*cert, err = encodeCertificate(config, proof)
			if err != nil {
				t.Fatal(err)
			}
			if equal, err := EquivalentVerifiedMembershipHistories(ctx, initial, right, invalid); err != nil || !equal {
				t.Fatalf("valid phase 2 witness rejected: %v", err)
			}
			higher := cloneProposal(&proof.Proposal)
			higher.Priority = highestPriority
			higher.ProposerID = "zzzz"
			proof.Summaries[0].AggregatePrior = higher
		case "wrong-fast-leader":
			order, e := leader.LeaderOrder(proof.Slot)
			if e != nil || len(order) == 0 {
				t.Fatalf("freeze leader unavailable: %v", e)
			}
			proof.Step = 4
			proof.Proposal.Priority = highestPriority
			proof.Proposal.ProposerID = order[0]
			for i := range proof.Summaries {
				proof.Summaries[i].Step = 4
				proof.Summaries[i].FirstCurrent = cloneProposal(&proof.Proposal)
				proof.Summaries[i].AggregatePrior = nil
			}
			*cert, err = encodeCertificate(config, proof)
			if err != nil {
				t.Fatal(err)
			}
			if equal, err := EquivalentVerifiedMembershipHistories(ctx, initial, right, invalid); err != nil || !equal {
				t.Fatalf("valid fast witness rejected: %v", err)
			}
			old := proof.Proposal.ProposerID
			wrong := NodeID("n9")
			if old == wrong {
				wrong = "n8"
			}
			proof.Proposal.ProposerID = wrong
			for i := range proof.Summaries {
				if proof.Summaries[i].FirstCurrent != nil && proof.Summaries[i].FirstCurrent.ProposerID == old {
					proof.Summaries[i].FirstCurrent.ProposerID = wrong
				}
			}
		case "terminal-outside-freeze":
			_, freezeProof, e := decodeCertificate(invalid.Transitions[0].Freeze.Certificate)
			if e != nil {
				t.Fatal(e)
			}
			seen := make(map[NodeID]bool)
			for _, s := range freezeProof.Summaries {
				seen[s.RecorderID] = true
			}
			var outsider NodeID
			for _, member := range initial.Members {
				if !seen[member.ID] {
					outsider = member.ID
					break
				}
			}
			if outsider == "" {
				t.Fatal("fixture lacks recorder outside freeze quorum")
			}
			proof.Summaries[0].RecorderID = outsider
		}
		*cert, err = encodeCertificate(config, proof)
		if err != nil {
			t.Fatal(err)
		}
		if equal, err := EquivalentVerifiedMembershipHistories(ctx, initial, right, invalid); err == nil || equal {
			t.Fatalf("invalid %s proof accepted", kind)
		}
	}
	badSeal := seal
	badSeal.Membership = &bad
	badValue, err := EncodeCheckpointSeal(badSeal)
	if err != nil {
		t.Fatal(err)
	}
	badCandidate := ManagedCheckpointCandidate{Value: badValue, Hash: ValueHash(sha256.Sum256(badValue)), DecisionSlot: candidate.DecisionSlot, ConfigID: candidate.ConfigID, Seal: badSeal}
	if err := lagger.ValidateManagedCheckpointCandidate(ctx, badCandidate); err == nil {
		t.Fatal("candidate with five-recorder old quorum accepted")
	}
	lagger.mu.Lock()
	v = lagger.decided[freezeSlot]
	v.Certificate = bytes.Clone(bad.Transitions[0].Freeze.Certificate)
	lagger.decided[freezeSlot] = v
	lagger.mu.Unlock()
	if err := lagger.ValidateManagedCheckpointCandidate(ctx, candidate); err == nil {
		t.Fatal("independently invalid local history accepted")
	}
	lagger.mu.Lock()
	v = lagger.decided[freezeSlot]
	v.Certificate = bytes.Clone(right.Transitions[0].Freeze.Certificate)
	lagger.decided[freezeSlot] = v
	lagger.mu.Unlock()
	fork := cloneMembershipRecord(left)
	fork.Genesis.Members[0], fork.Genesis.Members[1] = fork.Genesis.Members[1], fork.Genesis.Members[0]
	if equal, err := EquivalentVerifiedMembershipHistories(ctx, initial, right, fork); err == nil || equal {
		t.Fatal("changed ordered genesis was accepted")
	}
	abort := cloneMembershipRecord(left)
	abort.Abort = &ConfigTransition{}
	if equal, err := EquivalentVerifiedMembershipHistories(ctx, initial, right, abort); err == nil || equal {
		t.Fatal("invalid abort accepted")
	}
	t.Log("same committed 10→9 transition and valid proofs compare unequal because certificate summary order differs")
}

func mustDecision(t *testing.T, c *Core, slot Slot) DecidedValue {
	t.Helper()
	v, ok := c.CertifiedValue(slot)
	if !ok {
		t.Fatalf("missing decision %d", slot)
	}
	return v
}

func TestVerifiedMembershipEquivalentAbortProof(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	initial := core.CurrentCluster()
	target := Cluster{ConfigID: 2, Members: append(slices.Clone(initial.Members), Member{ID: "lost", WALIdentity: strings.Repeat("0", 64)})}
	if _, err := core.BeginReconfiguration(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := core.AbortReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	history, err := core.MembershipHistory()
	if err != nil || history.Abort == nil {
		t.Fatalf("actual abort history missing: %v", err)
	}
	alternate := cloneMembershipRecord(history)
	config, decision, err := decodeCertificate(alternate.Abort.Freeze.Certificate)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Summaries) < 2 {
		t.Fatal("abort freeze lacks quorum")
	}
	slices.Reverse(decision.Summaries)
	alternate.Abort.Freeze.Certificate, err = encodeCertificate(config, decision)
	if err != nil {
		t.Fatal(err)
	}
	if sameMembershipRecord(history, alternate) {
		t.Fatal("abort certificate bytes unexpectedly unchanged")
	}
	if equal, err := EquivalentVerifiedMembershipHistories(ctx, initial, history, alternate); err != nil || !equal {
		t.Fatalf("valid alternate abort proof rejected: %v", err)
	}
	withoutAbort := cloneMembershipRecord(history)
	withoutAbort.Abort = nil
	if equal, err := EquivalentVerifiedMembershipHistories(ctx, initial, history, withoutAbort); err != nil || equal {
		t.Fatalf("different abort revision considered equivalent: equal=%t err=%v", equal, err)
	}
	broken := cloneMembershipRecord(alternate)
	broken.Abort.Terminal.Value[0] ^= 1
	if equal, err := EquivalentVerifiedMembershipHistories(ctx, initial, history, broken); err == nil || equal {
		t.Fatal("invalid abort decision accepted")
	}
}

func TestManagedMembershipUnavailableSnapshotRequiresExactState(t *testing.T) {
	snapshot := &managedMembershipSnapshot{floor: 2, tip: 5, unavailableReason: "history unavailable"}
	valid := errors.New("history unavailable")
	if !managedMembershipSnapshotMatches(snapshot, MembershipRecord{}, valid, 2, 5, 7, 0) {
		t.Fatal("unchanged unavailable snapshot was rejected")
	}
	for _, tc := range []struct {
		floor, tip, cut Slot
		err             error
		version         uint64
	}{
		{2, 7, 7, valid, 0},
		{3, 5, 7, valid, 0},
		{2, 5, 7, errors.New("different failure"), 0},
		{2, 5, 7, nil, 0},
		{2, 5, 7, valid, 1},
	} {
		if managedMembershipSnapshotMatches(snapshot, MembershipRecord{}, tc.err, tc.floor, tc.tip, tc.cut, tc.version) {
			t.Fatal("changed unavailable state was accepted")
		}
	}
}
