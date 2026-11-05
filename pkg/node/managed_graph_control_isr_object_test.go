//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type objectISRRecord struct {
	to      quepaxa.NodeID
	request quepaxa.RecordRequest
	summary quepaxa.Summary
}

type objectISRStepGate struct {
	graphReviewTransport
	step4Records chan objectISRRecord
	step5Records chan objectISRRecord
	release4     chan struct{}
	release5     chan struct{}
	release4Once sync.Once
	release5Once sync.Once
}

func (t *objectISRStepGate) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	summary, err := t.graphReviewTransport[to].Record(ctx, request)
	if err != nil || request.Slot != 9 {
		return summary, err
	}
	var records chan objectISRRecord
	var release chan struct{}
	switch request.Step {
	case 4:
		records, release = t.step4Records, t.release4
	case 5:
		records, release = t.step5Records, t.release5
	default:
		return summary, nil
	}
	records <- objectISRRecord{to: to, request: request, summary: summary}
	select {
	case <-release:
		return summary, nil
	case <-ctx.Done():
		return quepaxa.Summary{}, ctx.Err()
	}
}

func (t *objectISRStepGate) releaseStep4() { t.release4Once.Do(func() { close(t.release4) }) }
func (t *objectISRStepGate) releaseStep5() { t.release5Once.Do(func() { close(t.release5) }) }

func TestManagedGraphAdmissionAfterObjectCheckpointSameISRStepAdvance(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	t.Cleanup(fixture.transport.releaseHeld)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	oldCore := fixture.node.core
	if err := fixture.node.archive.SyncThrough(ctx, oldCore, 8); err != nil {
		t.Fatalf("archive real certified prefix: %v", err)
	}
	files, cut, releaseFiles, err := fixture.node.material.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseFiles()
	if cut != 8 || len(files) != 2 || files[0].Role != checkpoint.RoleSQLite || files[1].Role != checkpoint.RoleGraphData {
		t.Fatalf("fixture lacks exact paired Graph cut: cut=%d files=%+v", cut, files)
	}
	sources := make([]checkpoint.Source, len(files))
	for i, file := range files {
		sources[i] = checkpoint.Source{Role: string(file.Role), Path: file.Path}
	}
	root, err := fixture.node.checkpoints.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
	if err != nil {
		t.Fatalf("upload real immutable Graph pair: %v", err)
	}
	prefix, ok := oldCore.PrefixHash(8)
	if !ok {
		t.Fatal("actual cut-8 certified prefix is missing")
	}
	next, following, err := oldCore.CheckpointLeaderOrders(8)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: 8, RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	hash := quepaxa.ValueHash(sha256.Sum256(value))
	gate := &objectISRStepGate{
		graphReviewTransport: make(graphReviewTransport),
		step4Records:         make(chan objectISRRecord, 2), step5Records: make(chan objectISRRecord, 2),
		release4: make(chan struct{}), release5: make(chan struct{}),
	}
	t.Cleanup(func() { gate.releaseStep4(); gate.releaseStep5() })
	cores, observerVerifications := newObjectISRStepCoreCluster(t, fixture, oldCore, gate)
	observer := cores["n1"]
	proposer := cores["n2"]
	if err := proposer.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatalf("persist real prepare marker on actual proposer Core: %v", err)
	}
	if observer.Tip() != 8 || proposer.Tip() != 8 {
		t.Fatalf("observer and proposer do not share actual prefix 8: observer=%d proposer=%d", observer.Tip(), proposer.Tip())
	}
	leaders, err := proposer.LeaderOrder(9)
	if err != nil || len(leaders) == 0 || leaders[0] == "n2" {
		t.Fatalf("slot 9 proposal is not the actual non-leader path: leaders=%v err=%v", leaders, err)
	}

	proposalCtx, cancelProposal := context.WithCancel(ctx)
	ownerDone := make(chan error, 1)
	go func() {
		defer close(ownerDone)
		slot, _, proposeErr := proposer.Propose(proposalCtx, value)
		if proposeErr == nil && slot != 9 {
			proposeErr = fmt.Errorf("QCKP proposal used slot %d, want 9", slot)
		}
		ownerDone <- proposeErr
	}()
	t.Cleanup(func() {
		cancelProposal()
		gate.releaseStep4()
		gate.releaseStep5()
		select {
		case <-ownerDone:
		case <-time.After(5 * time.Second):
			t.Error("actual QCKP proposer did not terminate during cleanup")
		}
	})
	step4 := waitObjectISRStepRecords(t, ctx, gate.step4Records, "Step-4")
	if len(step4) != 2 {
		t.Fatalf("got %d actual Step-4 peer Records, want 2", len(step4))
	}
	for _, record := range step4 {
		if record.request.Proposal.Hash != hash || !bytes.Equal(record.request.Proposal.Value, value) || record.summary.Step != 4 {
			t.Fatalf("Step-4 peer did not durably Record the exact QCKP: %+v", record)
		}
	}
	progress := observer.RuntimeProgressSnapshot()
	if progress.CertifiedTip != 8 || progress.RecorderTip != 9 || progress.RecorderHash != hash || progress.RecorderHashIsCertified {
		t.Fatalf("observer lacks exact uncertified QCKP after actual Step-4 Records: %+v", progress)
	}
	if _, certified := observer.CertifiedValue(9); certified {
		t.Fatal("QCKP certified before observer validation")
	}
	verifiedBeforeObserver := observerVerifications.Load()

	var hookHit atomic.Bool
	var observed quepaxa.ManagedAppendOnlyControlObservation
	var observationErr error
	var step5 []objectISRRecord
	restore := localtesthooks.Set(func(event string) {
		if event != "core:managed-control:before-snapshot-recheck" || !hookHit.CompareAndSwap(false, true) {
			return
		}
		if calls := observerVerifications.Load(); calls < verifiedBeforeObserver+2 {
			t.Errorf("full observer did not run the real object/history verifier for both Step-4 QCKP references: before=%d now=%d", verifiedBeforeObserver, calls)
		}
		gate.releaseStep4()
		step5 = waitObjectISRStepRecords(t, ctx, gate.step5Records, "Step-5")
		for _, record := range step5 {
			if record.request.Proposal.Hash != hash || !bytes.Equal(record.request.Proposal.Value, value) || record.summary.Step != 5 || record.summary.FirstCurrent == nil || record.summary.FirstCurrent.Hash != hash || record.summary.AggregatePrior == nil || record.summary.AggregatePrior.Hash != hash {
				t.Errorf("Step-5 peer did not durably Record exact same QCKP ISR: %+v", record)
			}
		}
		progress := observer.RuntimeProgressSnapshot()
		if progress.CertifiedTip != 8 || progress.RecorderTip != 9 || progress.RecorderHash != hash || progress.RecorderHashIsCertified {
			t.Errorf("observer prefix or QCKP changed while Step-5 responses held: %+v", progress)
		}
		if _, certified := observer.CertifiedValue(9); certified {
			t.Error("slot 9 certified while both actual Step-5 responses were held")
		}
	})
	defer restore()
	observed, observationErr = observer.ObserveManagedAppendOnlyControl(ctx)
	if !hookHit.Load() {
		t.Fatal("observer did not reach the final recheck after object/history validation")
	}
	if observationErr != nil || observed.CommittedTip != 8 || observed.ObservedSlot != 9 || observed.Step != 5 || len(observed.Proposals) != 3 {
		t.Fatalf("fresh object-backed Step-5 observation=%+v err=%v", observed, observationErr)
	}
	if len(step5) != 2 {
		t.Fatalf("captured Step-5 peer Records=%d, want 2", len(step5))
	}
	var observerRecord *objectISRRecord
	for i := range step5 {
		if step5[i].to == "n1" {
			observerRecord = &step5[i]
		}
	}
	if observerRecord == nil {
		t.Fatal("no actual Step-5 Record reached the remote observer")
	}
	wantRoles := []quepaxa.ManagedAppendOnlyControlProposalRole{quepaxa.ManagedAppendOnlyFirstCurrent, quepaxa.ManagedAppendOnlyAggregateCurrent, quepaxa.ManagedAppendOnlyAggregatePrior}
	for i, proposal := range observed.Proposals {
		if proposal.Role != wantRoles[i] || proposal.Kind != quepaxa.ManagedAppendOnlyCheckpointSeal || proposal.Hash != hash || proposal.Priority != observerRecord.request.Proposal.Priority || proposal.ProposerID != observerRecord.request.Proposal.ProposerID || observerRecord.request.Proposal.ProposerID != "n2" || observerRecord.request.Proposal.Hash != hash || !bytes.Equal(observerRecord.request.Proposal.Value, value) {
			t.Fatalf("fresh QCKP reference %d differs from actual Step-5 ISR: got=%+v request=%+v", i, proposal, observerRecord.request)
		}
	}

	gate.releaseStep5()
	select {
	case err := <-ownerDone:
		if err != nil {
			t.Fatalf("actual non-leader QCKP proposal did not finish after gates released: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("QCKP proposal did not finish: %v", ctx.Err())
	}
	certified, ok := observer.CertifiedValue(9)
	if !ok || certified.Hash != hash || !bytes.Equal(certified.Value, value) {
		t.Fatalf("slot 9 lacks the actual exact QCKP certificate: found=%v hash=%x", ok, certified.Hash)
	}
}

func waitObjectISRStepRecords(t *testing.T, ctx context.Context, records <-chan objectISRRecord, step string) []objectISRRecord {
	t.Helper()
	got := make([]objectISRRecord, 0, 2)
	for len(got) < 2 {
		select {
		case record := <-records:
			got = append(got, record)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for two actual %s peer Records: %v", step, ctx.Err())
		}
	}
	return got
}

func newObjectISRStepCoreCluster(t *testing.T, fixture *managedGraphAdmissionRaceFixture, oldCore *quepaxa.Core, transport *objectISRStepGate) (map[quepaxa.NodeID]*quepaxa.Core, *atomic.Int32) {
	t.Helper()
	cluster := oldCore.CurrentCluster()
	cores := make(map[quepaxa.NodeID]*quepaxa.Core, len(cluster.Members))
	var observerVerifications atomic.Int32
	protocol := sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
	for _, member := range cluster.Members {
		config := *fixture.node.config
		config.NodeID = member.ID
		config.Members = cluster.Members
		identity, err := hex.DecodeString(member.WALIdentity)
		if err != nil || len(identity) != 32 {
			t.Fatalf("member %s WAL identity is invalid: %v", member.ID, err)
		}
		walDir := filepath.Join(t.TempDir(), "qlog")
		wal, err := qlog.Open(walDir)
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.BindIdentity(identity); err != nil {
			_ = wal.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		profileNode := New(&config)
		source, memberDigest, err := profileNode.PreparedStartupBindingDigestsForProtocol(protocol)
		if err != nil {
			t.Fatal(err)
		}
		planDigest, err := profileNode.ManagedBootstrapPlanDigestForProtocol(source, protocol, protocol)
		if err != nil {
			t.Fatal(err)
		}
		var identityArray [32]byte
		copy(identityArray[:], identity)
		verify := quepaxa.ManagedCheckpointCandidateVerifier(func(ctx context.Context, candidate quepaxa.ManagedCheckpointCandidate) error {
			if member.ID == "n1" {
				observerVerifications.Add(1)
			}
			return fixture.node.verifyManagedAppendOnlyCandidate(ctx, candidate)
		})
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: cluster, WAL: wal, Transport: transport,
			ValidateValue: types.ValidateManagedGraphExecutionPolicy,
			ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{Binding: quepaxa.ManagedAppendOnlyBinding{
				ApplicationProtocol: protocol, BootstrapPlanDigest: planDigest, MemberConfigDigest: memberDigest,
				Generation: 0, WALIdentity: identityArray,
			}, Verify: verify},
		})
		if err != nil {
			t.Fatal(err)
		}
		core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
			return fixture.node.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
		})
		transport.graphReviewTransport[member.ID] = core
		cores[member.ID] = core
	}
	fixture.node.core = cores["n1"]
	decisions, _, err := oldCore.DecisionsFrom(1, 8)
	if err != nil || len(decisions) != 8 {
		t.Fatalf("read actual certified setup prefix: count=%d err=%v", len(decisions), err)
	}
	for _, core := range cores {
		if err := core.AcceptCertifiedHints(decisions); err != nil {
			t.Fatalf("replay actual certified setup prefix to %s: %v", core.NodeID(), err)
		}
		if core.Tip() != 8 {
			t.Fatalf("Core %s did not replay exact certified prefix: %d", core.NodeID(), core.Tip())
		}
	}
	return cores, &observerVerifications
}
