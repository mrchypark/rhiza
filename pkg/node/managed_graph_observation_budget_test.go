//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type managedGraphAdmissionLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *managedGraphAdmissionLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *managedGraphAdmissionLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func TestManagedGraphAdmissionResolvesOriginalPendingApplicationBeforeBarrier(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	fixture.transport.releaseHeld()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := types.GraphCommand{RequestID: "pending-before-barrier", Cypher: "CREATE (:PendingBeforeBarrier {id:'once'})"}
	value, err := types.EncodeManagedGraphExecution(command)
	if err != nil {
		t.Fatal(err)
	}
	hash := quepaxa.ValueHash(sha256.Sum256(value))
	for id, core := range fixture.transport.graphReviewTransport {
		if _, err := core.Record(ctx, quepaxa.RecordRequest{Slot: 9, Step: 5, ConfigID: 1, Proposal: quepaxa.Proposal{ProposerID: "n2", Hash: hash, Value: bytes.Clone(value)}}); err != nil {
			t.Fatalf("record original pending value on %s: %v", id, err)
		}
	}
	if _, ok := fixture.node.core.CertifiedValue(9); ok {
		t.Fatal("fixture application was already certified")
	}
	var applicationProposals atomic.Int32
	var ownBarrier []byte
	restore := localtesthooks.SetProposalEntry(func(offered []byte) {
		if _, application, err := types.DecodeManagedGraphExecution(offered); err == nil && application {
			applicationProposals.Add(1)
		}
		if barrier, err := types.DecodeReadBarrier(offered); err == nil && barrier {
			ownBarrier = bytes.Clone(offered)
		}
	})
	defer restore()
	if err := fixture.node.admitInternalApplication(ctx); err != nil {
		t.Fatalf("resolve original pending application: %v", err)
	}
	decision, ok := fixture.node.core.CertifiedValue(9)
	if !ok || decision.Hash != hash || !bytes.Equal(decision.Value, value) || len(decision.Certificate) == 0 {
		t.Fatalf("original slot certificate differs: ok=%t decision=%+v", ok, decision)
	}
	if applicationProposals.Load() != 0 {
		t.Fatalf("admission resubmitted %d application proposals", applicationProposals.Load())
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if progress.RecorderTip != progress.CertifiedTip || fixture.node.material.Tip() != uint64(progress.CertifiedTip) {
		t.Fatalf("admission returned before contiguous certified replay: progress=%+v applied=%d", progress, fixture.node.material.Tip())
	}
	if len(ownBarrier) == 0 {
		t.Fatal("admission did not offer its own barrier")
	}
	slot, found := fixture.node.core.DecidedSlot(ownBarrier)
	if !found || slot <= 9 {
		t.Fatalf("admission lacks its own later barrier: slot=%d found=%t", slot, found)
	}
	barrier, found := fixture.node.core.CertifiedValue(slot)
	if !found || !bytes.Equal(barrier.Value, ownBarrier) || len(barrier.Certificate) == 0 {
		t.Fatal("admission lacks its own exact barrier certificate")
	}
	fingerprint, err := types.ManagedGraphIntentFingerprint(command)
	if err != nil {
		t.Fatal(err)
	}
	receipt, found, matches, err := fixture.node.material.GraphRequestStatusFingerprint(ctx, command.RequestID, fingerprint)
	if err != nil || !found || !matches || !receipt.Applied || receipt.Slot != 9 {
		t.Fatalf("original receipt differs: receipt=%+v found=%t matches=%t err=%v", receipt, found, matches, err)
	}
	rows, err := fixture.node.material.GraphQuery(ctx, "MATCH (n:PendingBeforeBarrier) RETURN n.id", nil)
	if err != nil || len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || rows.Rows[0][0] != "once" {
		t.Fatalf("original application was not applied once: rows=%v err=%v", rows.Rows, err)
	}
	decisions, _, err := fixture.node.core.DecisionsFrom(9, int(progress.CertifiedTip-9)+1)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, retained := range decisions {
		if bytes.Equal(retained.Value, value) {
			count++
			if retained.Slot != 9 {
				t.Fatalf("original bytes moved to slot %d", retained.Slot)
			}
		}
	}
	if count != 1 {
		t.Fatalf("original bytes appeared at %d slots", count)
	}
	if _, found, _, err := fixture.node.material.GraphRequestStatusFingerprint(ctx, "never-submitted", fingerprint); err != nil || found {
		t.Fatalf("admission fabricated another request receipt: found=%t err=%v", found, err)
	}
}

func TestManagedGraphLearnerAdmissionCannotProposePendingRecovery(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.NewLearner(quepaxa.Config{NodeID: "new-learner", Cluster: fixture.node.core.CurrentCluster(), WAL: wal, Transport: fixture.transport})
	if err != nil {
		t.Fatal(err)
	}
	binding := fixture.node.internalApplication.Load()
	originalCore := fixture.node.core
	defer func() { binding.core, fixture.node.core = originalCore, originalCore }()
	binding.core = core
	fixture.node.core = core
	var proposals atomic.Int32
	restore := localtesthooks.SetProposalEntry(func([]byte) { proposals.Add(1) })
	defer restore()
	if err := fixture.node.waitManagedAppendOnlyControl(context.Background(), binding); err == nil {
		t.Fatal("learner received voter recovery authority")
	}
	if proposals.Load() != 0 || core.Tip() != 0 || core.RecorderTip() != 0 {
		t.Fatalf("learner proposed or certified history: proposals=%d C=%d R=%d", proposals.Load(), core.Tip(), core.RecorderTip())
	}
}
