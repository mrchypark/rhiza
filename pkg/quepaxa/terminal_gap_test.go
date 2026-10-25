package quepaxa

import (
	"context"
	"crypto/sha256"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func terminalGapFixture(t *testing.T) (*Core, Decision, []DecidedValue, []DecidedValue, TerminalGapKey) {
	t.Helper()
	sourceCores, _ := reconfigCluster(t)
	source := sourceCores["a"]
	initial := source.CurrentCluster()
	target := Cluster{ConfigID: initial.ConfigID + 1, Members: append([]Member(nil), initial.Members[:2]...)}
	freeze, err := source.BeginReconfiguration(t.Context(), target)
	if err != nil {
		t.Fatalf("begin reconfiguration: %v", err)
	}
	freezeValues, _, err := source.DecisionsFrom(freeze, 1)
	if err != nil || len(freezeValues) != 1 {
		t.Fatalf("read freeze decision: values=%d err=%v", len(freezeValues), err)
	}

	wal, err := qlog.Open(filepath.Join(t.TempDir(), "pending.qlog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	pending := newCore("a", &initial, wal, nil)
	pending.reconfigEnabled = true
	if err := pending.AcceptCertifiedHints(freezeValues); err != nil {
		t.Fatalf("install active freeze on pending replica: %v", err)
	}
	pending.mu.RLock()
	if pending.reconfiguration == nil {
		pending.mu.RUnlock()
		t.Fatal("freeze hint did not retain active reconfiguration state")
	}
	pending.mu.RUnlock()

	if err := source.FinishReconfiguration(t.Context()); err != nil {
		t.Fatalf("finish source transition: %v", err)
	}
	terminal := freeze + leaderEpochSize
	terminalValues, _, err := source.DecisionsFrom(terminal, 1)
	if err != nil || len(terminalValues) != 1 {
		t.Fatalf("read terminal decision: values=%d err=%v", len(terminalValues), err)
	}
	configID, decision, err := decodeCertificate(terminalValues[0].Certificate)
	if err != nil {
		t.Fatalf("decode terminal certificate: %v", err)
	}
	decision.ConfigID = configID
	decision.Proposal.Value = append([]byte(nil), terminalValues[0].Value...)
	key, ok := pending.KnownTerminalGap(decision)
	if !ok {
		t.Fatal("matching certified terminal did not identify the active local gap")
	}
	prefixValues, _, err := source.DecisionsFrom(freeze+1, int(terminal-freeze-1))
	if err != nil || len(prefixValues) != int(terminal-freeze-1) {
		t.Fatalf("read certified pre-terminal prefix: values=%d err=%v", len(prefixValues), err)
	}
	return pending, decision, prefixValues, terminalValues, key
}

func TestKnownTerminalGapRequiresExactActiveFreezeAndTerminal(t *testing.T) {
	pending, decision, _, _, key := terminalGapFixture(t)
	if !pending.PendingTerminalGap(key) {
		t.Fatal("fresh terminal gap key did not revalidate")
	}

	tests := map[string]func(*Decision){
		"wrong slot":          func(d *Decision) { d.Slot++ },
		"wrong config":        func(d *Decision) { d.ConfigID++ },
		"wrong proposal hash": func(d *Decision) { d.Proposal.Hash[0] ^= 0xff },
		"wrong freeze": func(d *Decision) {
			control, _, _ := decodeReconfiguration(d.Proposal.Value)
			control.Freeze++
			d.Proposal.Value, _ = encodeReconfiguration(control)
			d.Proposal.Hash = ValueHash(sha256.Sum256(d.Proposal.Value))
		},
		"wrong target": func(d *Decision) {
			control, _, _ := decodeReconfiguration(d.Proposal.Value)
			control.Target.ConfigID++
			d.Proposal.Value, _ = encodeReconfiguration(control)
			d.Proposal.Hash = ValueHash(sha256.Sum256(d.Proposal.Value))
		},
		"abort terminal": func(d *Decision) {
			control, _, _ := decodeReconfiguration(d.Proposal.Value)
			control.Abort = true
			d.Proposal.Value, _ = encodeReconfiguration(control)
			d.Proposal.Hash = ValueHash(sha256.Sum256(d.Proposal.Value))
		},
		"non-terminal value": func(d *Decision) {
			d.Proposal.Value = []byte("ordinary")
			d.Proposal.Hash = ValueHash(sha256.Sum256(d.Proposal.Value))
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := decision
			candidate.Proposal.Value = append([]byte(nil), decision.Proposal.Value...)
			mutate(&candidate)
			if _, ok := pending.KnownTerminalGap(candidate); ok {
				t.Fatal("mismatched terminal identified as the active gap")
			}
		})
	}
}

func TestKnownEngineRemovalTerminalGapRequiresExactStorageBoundTerminal(t *testing.T) {
	cores, _, target := engineOwnedRemovalCluster(t)
	ctx := t.Context()
	for _, core := range cores {
		core.reconfigAdmission = func(context.Context, ReconfigurationAdmission) error { return nil }
	}
	operation, err := cores["a"].BeginEngineOwnedRemovalAt(ctx, target, "terminal-gap-engine-removal", 0)
	if err != nil {
		t.Fatalf("begin engine-owned removal: %v", err)
	}
	freezeValues, _, err := cores["a"].DecisionsFrom(operation.Freeze, 1)
	if err != nil || len(freezeValues) != 1 {
		t.Fatalf("read engine-owned freeze: values=%d err=%v", len(freezeValues), err)
	}

	wal, err := qlog.Open(filepath.Join(t.TempDir(), "engine-removal-pending.qlog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	initial := cores["a"].clusterForSlot(operation.Freeze)
	pending := newCore("a", &initial, wal, nil)
	pending.reconfigEnabled = true
	pending.engineOwnedRemovals = true
	if err := pending.AcceptCertifiedHints(freezeValues); err != nil {
		t.Fatalf("install engine-owned freeze: %v", err)
	}

	operation.StorageEffectsExcludedHash = ValueHash{0x7a}
	if err := cores["a"].FinishEngineOwnedRemovalAt(ctx, operation); err != nil {
		t.Fatalf("finish engine-owned removal: %v", err)
	}
	terminalSlot := operation.Freeze + 16
	terminalValues, _, err := cores["a"].DecisionsFrom(terminalSlot, 1)
	if err != nil || len(terminalValues) != 1 {
		t.Fatalf("read engine-owned terminal: values=%d err=%v", len(terminalValues), err)
	}
	configID, decision, err := decodeCertificate(terminalValues[0].Certificate)
	if err != nil {
		t.Fatalf("decode engine-owned terminal certificate: %v", err)
	}
	decision.ConfigID = configID
	decision.Proposal.Value = append([]byte(nil), terminalValues[0].Value...)
	key, ok := pending.KnownTerminalGap(decision)
	if !ok || key.Freeze != operation.Freeze || key.Terminal != terminalSlot || key.FreezeHash != operation.FreezeHash {
		t.Fatalf("exact engine-owned terminal did not identify pending gap: key=%+v ok=%v", key, ok)
	}
	if !pending.PendingTerminalGap(key) {
		t.Fatal("engine-owned terminal gap key did not revalidate")
	}
}

func TestPendingTerminalGapSurvivesPrefixProgressAndExpiresAtTerminal(t *testing.T) {
	pending, _, prefixValues, terminalValues, key := terminalGapFixture(t)
	for name, mutate := range map[string]func(*TerminalGapKey){
		"config":      func(candidate *TerminalGapKey) { candidate.ConfigID++ },
		"freeze":      func(candidate *TerminalGapKey) { candidate.Freeze++ },
		"terminal":    func(candidate *TerminalGapKey) { candidate.Terminal++ },
		"freeze hash": func(candidate *TerminalGapKey) { candidate.FreezeHash[0] ^= 0xff },
	} {
		t.Run("reject changed key/"+name, func(t *testing.T) {
			candidate := key
			mutate(&candidate)
			if pending.PendingTerminalGap(candidate) {
				t.Fatal("changed terminal gap key revalidated")
			}
		})
	}
	freeze := prefixValues[0].Slot - 1
	terminal := terminalValues[0].Slot
	if got := pending.Tip(); got != freeze {
		t.Fatalf("pending tip=%d, want freeze=%d", got, freeze)
	}

	// The archive worker may fill the prefix before this key is revalidated.
	// That progress does not change the still-pending transition.
	if err := pending.AcceptCertifiedHints(prefixValues); err != nil {
		t.Fatalf("apply certified prefix before terminal: %v", err)
	}
	if pending.Tip() != terminal-1 {
		t.Fatalf("prefix tip=%d, want %d", pending.Tip(), terminal-1)
	}
	if !pending.PendingTerminalGap(key) {
		t.Fatal("pending gap expired before the terminal was installed")
	}
	if err := pending.AcceptCertifiedHints(terminalValues); err != nil {
		t.Fatalf("install exact terminal after prefix progress: %v", err)
	}
	if pending.PendingTerminalGap(key) {
		t.Fatal("completed terminal remained pending")
	}
}

func TestStartupTerminalGapSnapshotNeedsNoIncomingTerminalAndAllowsTMinusOne(t *testing.T) {
	pending, _, prefixValues, terminalValues, want := terminalGapFixture(t)
	if _, exists := pending.CertifiedValue(want.Terminal); exists {
		t.Fatal("fixture unexpectedly contains the incoming terminal")
	}
	got, ok := pending.StartupTerminalGapSnapshot()
	if !ok || got != want {
		t.Fatalf("startup terminal snapshot=%+v ok=%t, want %+v", got, ok, want)
	}
	if pending.StartupCatchUpRoundEligible(pending.CurrentCluster()) {
		t.Fatal("startup quorum round was eligible while the terminal remained pending")
	}

	if err := pending.AcceptCertifiedHints(prefixValues); err != nil {
		t.Fatalf("apply certified prefix through T-1: %v", err)
	}
	if pending.Tip() != want.Terminal-1 {
		t.Fatalf("tip=%d, want T-1=%d", pending.Tip(), want.Terminal-1)
	}
	if got, ok := pending.StartupTerminalGapSnapshot(); !ok || got != want {
		t.Fatalf("T-1 startup snapshot=%+v ok=%t, want %+v", got, ok, want)
	}
	if err := pending.AcceptCertifiedHints(terminalValues); err != nil {
		t.Fatalf("apply certified terminal: %v", err)
	}
	if _, ok := pending.StartupTerminalGapSnapshot(); ok {
		t.Fatal("startup terminal snapshot remained after the terminal was installed")
	}
	if current := pending.CurrentCluster(); !pending.StartupCatchUpRoundEligible(current) {
		t.Fatal("current voter was not eligible after the pending transition completed")
	}
}

func TestStartupCatchUpRoundEligibilityRequiresCurrentVoterAndNoPendingTransition(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	current := core.CurrentCluster()
	if !core.StartupCatchUpRoundEligible(current) {
		t.Fatal("matching current voter round was rejected")
	}
	wrongConfig := current
	wrongConfig.ConfigID++
	if core.StartupCatchUpRoundEligible(wrongConfig) {
		t.Fatal("round with stale configuration ID was eligible")
	}
	wrongMembers := current
	wrongMembers.Members = append([]Member(nil), current.Members[:len(current.Members)-1]...)
	if core.StartupCatchUpRoundEligible(wrongMembers) {
		t.Fatal("round with stale membership was eligible")
	}

	wal, err := qlog.Open(filepath.Join(t.TempDir(), "learner.qlog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	learner := newCore("a", &current, wal, nil)
	learner.learner = true
	if learner.StartupCatchUpRoundEligible(current) {
		t.Fatal("learner was eligible for a voter quorum round")
	}
	observer := newCore("a", &current, wal, nil)
	observer.observer = true
	if observer.StartupCatchUpRoundEligible(current) {
		t.Fatal("observer was eligible for a voter quorum round")
	}

	pending, _, _, _, _ := terminalGapFixture(t)
	if pending.StartupCatchUpRoundEligible(pending.CurrentCluster()) {
		t.Fatal("round was eligible while a reconfiguration terminal was pending")
	}
}
