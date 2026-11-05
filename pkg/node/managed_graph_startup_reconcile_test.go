package node

import (
	"context"
	"os"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedGraphReconcileRefusesUncertifiedNativeStateBeforeOpen(t *testing.T) {
	ctx := context.Background()
	core := graphReviewCores(t, false)["n1"]
	value, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "certified-original", Cypher: "CREATE (:CertifiedOriginal {n:1})"})
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := core.Propose(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	decision, found := core.CertifiedValue(slot)
	if !found {
		t.Fatal("actual three-Core certificate missing")
	}
	wrong := decision.Hash
	wrong[0] ^= 1
	for _, tc := range []struct {
		name  string
		state materializer.ManagedGraphTargetState
	}{
		{"wrong-full-hash", materializer.ManagedGraphTargetState{Replay: []materializer.ManagedGraphReplayRecord{{Slot: uint64(slot), DecisionHash: wrong}}}},
		{"missing-certificate", materializer.ManagedGraphTargetState{Replay: []materializer.ManagedGraphReplayRecord{{Slot: uint64(slot + 1), DecisionHash: decision.Hash}}}},
		{"already-folded-slot", materializer.ManagedGraphTargetState{SQLiteTip: uint64(slot), Replay: []materializer.ManagedGraphReplayRecord{{Slot: uint64(slot), DecisionHash: decision.Hash}}}},
		{"partial-install-not-live-replay", materializer.ManagedGraphTargetState{InstallAttempt: &materializer.ManagedGraphInstallAttemptObservation{AttemptID: "observed-only"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			n := &Node{config: &types.ExecutionConfig{DataDir: dir}}
			if err := n.reconcileManagedGraphTarget(ctx, core, &managedGraphStartupProjection{tip: quepaxa.Slot(slot)}, tc.state); err == nil {
				t.Fatal("untrusted native observation authorized a writer")
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 0 {
				t.Fatal("negative observation created target/staging effects", entries, err)
			}
		})
	}
}
