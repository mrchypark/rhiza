package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// A control proposal is observed at R before a higher-Step application arrives
// at that same slot. Existing QuePaxa consensus may adopt the original
// application at R; Node must neither resubmit it nor treat it as the barrier.
func TestManagedGraphAppendOnlyAdmissionAdoptsSameSlotApplication(t *testing.T) {
	fixture := newManagedGraphAppendOnlyAdmissionFixture(t)
	defer fixture.transport.releaseAll()
	controlled := installManagedAppendOnlyPreRecordGate(t, fixture)
	defer controlled.release()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	fixture.transport.addGate(10)

	remoteOwnerCtx, cancelRemoteOwner := context.WithCancel(ctx)
	defer cancelRemoteOwner()
	remoteBarrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{0x31, 0x32, 0x33})
	ownerResult := make(chan struct {
		slot quepaxa.Slot
		err  error
	}, 1)
	go func() {
		slot, _, err := fixture.node.core.Propose(remoteOwnerCtx, remoteBarrier)
		ownerResult <- struct {
			slot quepaxa.Slot
			err  error
		}{slot: slot, err: err}
	}()
	if !controlled.waitEntered(ctx) {
		t.Fatal("actual slot-9 pure control did not reach the pre-record gate")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		progress := fixture.node.core.RuntimeProgressSnapshot()
		if progress.RecorderTip == 9 && progress.CertifiedTip == 8 && progress.RecorderHashPresent && progress.RecorderHash == quepaxa.ValueHash(sha256.Sum256(remoteBarrier)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("publisher did not retain the exact pending read barrier: %+v", progress)
		}
		time.Sleep(time.Millisecond)
	}

	admission := make(chan error, 1)
	go func() { admission <- fixture.node.admitInternalApplication(ctx) }()
	if !fixture.transport.waitEntered(ctx, 10) {
		select {
		case err := <-admission:
			t.Fatalf("admission returned before a slot-10 control barrier: %v", err)
		default:
		}
		progress := fixture.node.core.RuntimeProgressSnapshot()
		fixture.node.managedGraphControlMu.Lock()
		operation := fixture.node.managedGraphControl
		fixture.node.managedGraphControlMu.Unlock()
		t.Fatalf("admission did not start its separate slot-10 control barrier: progress=%+v operation=%+v", progress, operation)
	}
	var admissionOwner *managedGraphControlOperation
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		fixture.node.managedGraphControlMu.Lock()
		candidate := fixture.node.managedGraphControl
		fixture.node.managedGraphControlMu.Unlock()
		if candidate != nil && candidate.observation != nil && candidate.observation.ObservedSlot == 9 {
			admissionOwner = candidate
			break
		}
		time.Sleep(time.Millisecond)
	}
	if admissionOwner == nil {
		t.Fatal("admission did not retain its exact R=9 observation")
	}
	if fixture.node.core.Tip() != 8 || fixture.node.material.Tip() != 8 {
		t.Fatalf("foreground passed before R and S controls completed: certified=%d applied=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}

	command := types.GraphCommand{RequestID: "remote-app-at-observed-slot", Cypher: "CREATE (:RemoteControlAdopt {id:'once'})"}
	application, err := types.EncodeManagedGraphExecution(command)
	if err != nil {
		t.Fatal(err)
	}
	applicationHash := quepaxa.ValueHash(sha256.Sum256(application))
	var priority quepaxa.Priority
	for index := range priority {
		priority[index] = 0xff
	}
	proposal := quepaxa.Proposal{Priority: priority, ProposerID: "n2", Hash: applicationHash, Value: bytes.Clone(application)}
	for _, peerID := range []quepaxa.NodeID{"n1", "n2", "n3"} {
		summary, err := fixture.transport.graphReviewTransport[peerID].Record(ctx, quepaxa.RecordRequest{Slot: 9, Step: 5, ConfigID: 1, Proposal: proposal})
		if err != nil {
			t.Fatalf("record actual higher-Step application on %s: %v", peerID, err)
		}
		value, retained := fixture.transport.graphReviewTransport[peerID].Value(applicationHash)
		if summary.Step != 5 || summary.FirstCurrent == nil || summary.FirstCurrent.Hash != applicationHash || !retained || !bytes.Equal(value, application) {
			t.Fatalf("peer %s did not retain exact Step-5 application receipt: %+v", peerID, summary)
		}
	}

	releaseManagedAppendOnlyAdmissionGate(t, fixture.transport, 9)
	controlled.release()
	releaseManagedAppendOnlyAdmissionGate(t, fixture.transport, 10)

	select {
	case result := <-ownerResult:
		if result.err != nil || result.slot <= 9 {
			t.Fatalf("remote owner did not carry its barrier to a fresh slot after same-slot application adoption: slot=%d err=%v", result.slot, result.err)
		}
		barrier, ok := fixture.node.core.CertifiedValue(result.slot)
		if !ok || !bytes.Equal(barrier.Value, remoteBarrier) {
			t.Fatalf("remote owner retry did not certify its exact barrier: slot=%d ok=%t", result.slot, ok)
		}
	case <-ctx.Done():
		fixture.node.managedGraphControlMu.Lock()
		operation := fixture.node.managedGraphControl
		fixture.node.managedGraphControlMu.Unlock()
		decision, decided := fixture.node.core.CertifiedValue(9)
		var admissionErr error
		select {
		case admissionErr = <-admission:
		default:
		}
		t.Fatalf("remote owner did not finish after same-slot application: n1=%+v n2=%+v n3=%+v decision9=%+v decided=%t operation=%+v admission=%v",
			fixture.transport.graphReviewTransport["n1"].RuntimeProgressSnapshot(),
			fixture.transport.graphReviewTransport["n2"].RuntimeProgressSnapshot(),
			fixture.transport.graphReviewTransport["n3"].RuntimeProgressSnapshot(), decision, decided, operation, admissionErr)
	}
	cancelRemoteOwner()
	decision, ok := waitManagedAppendOnlyCertifiedValue(ctx, fixture.node.core, 9)
	if !ok || decision.Hash != applicationHash || !bytes.Equal(decision.Value, application) {
		t.Fatalf("slot 9 did not certify the exact original application: ok=%t slot=%d hash=%x", ok, decision.Slot, decision.Hash)
	}
	select {
	case err := <-admission:
		if err != nil {
			t.Fatalf("admission did not complete after same-slot application and its own barrier: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("admission did not complete after control release")
	}

	progress := fixture.node.core.RuntimeProgressSnapshot()
	if progress.RecorderTip != progress.CertifiedTip || progress.CertifiedTip < admissionOwner.observation.ObservedSlot || fixture.node.material.Tip() < uint64(progress.CertifiedTip) {
		t.Fatalf("admission returned before certified prefix replay: progress=%+v materialized=%d", progress, fixture.node.material.Tip())
	}
	barrierDecision, ok := fixture.node.core.CertifiedValue(admissionOwner.slot)
	if !ok || admissionOwner.slot < 9 || barrierDecision.Hash != admissionOwner.hash || !bytes.Equal(barrierDecision.Value, admissionOwner.value) {
		t.Fatalf("admission owner did not certify its own S>=R barrier: slot=%d ok=%t hash=%x", admissionOwner.slot, ok, barrierDecision.Hash)
	}

	decision, ok = fixture.node.core.CertifiedValue(9)
	if !ok || decision.Hash != applicationHash || !bytes.Equal(decision.Value, application) {
		t.Fatal("same-slot consensus did not preserve the original application certificate")
	}
	fingerprint, err := types.ManagedGraphIntentFingerprint(command)
	if err != nil {
		t.Fatal(err)
	}
	receipt, found, matches, err := fixture.node.material.GraphRequestStatusFingerprint(ctx, command.RequestID, fingerprint)
	if err != nil || !found || !matches || !receipt.Applied || receipt.Slot != 9 {
		t.Fatalf("original same-slot application receipt differs: receipt=%+v found=%t matches=%t err=%v", receipt, found, matches, err)
	}
	query, err := fixture.node.material.GraphQuery(ctx, "MATCH (n:RemoteControlAdopt) RETURN n.id", nil)
	if err != nil || len(query.Rows) != 1 || len(query.Rows[0]) != 1 || query.Rows[0][0] != "once" {
		t.Fatalf("original application effect was not applied exactly once: rows=%v err=%v", query.Rows, err)
	}
	decisions, _, err := fixture.node.core.DecisionsFrom(9, 16)
	if err != nil {
		t.Fatal(err)
	}
	applications := 0
	for _, decided := range decisions {
		if bytes.Equal(decided.Value, application) {
			applications++
		}
	}
	if applications != 1 {
		t.Fatalf("original application was decided at %d slots, want exactly once", applications)
	}
}

func TestManagedGraphAppendOnlyAdmissionWaitsForRemoteQCKPAndReadBarrier(t *testing.T) {
	fixture := newManagedGraphAppendOnlyAdmissionFixture(t)
	defer fixture.transport.releaseAll()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Create and certify a real immutable Graph pair at the actual applied cut.
	core := fixture.node.core
	if err := fixture.node.archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal("archive actual Graph prefix", err)
	}
	files, cut, releaseFiles, err := fixture.node.material.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal("capture actual Graph pair", err)
	}
	defer releaseFiles()
	if cut != 8 || len(files) != 2 || files[0].Role != materializer.CheckpointSQLite || files[1].Role != materializer.CheckpointGraphData {
		t.Fatalf("fixture did not capture the exact Graph pair at cut 8: cut=%d files=%+v", cut, files)
	}
	sources := make([]checkpoint.Source, len(files))
	for i, file := range files {
		sources[i] = checkpoint.Source{Role: string(file.Role), Path: file.Path}
	}
	root, err := fixture.node.checkpoints.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
	if err != nil {
		t.Fatal("upload actual immutable Graph pair", err)
	}
	prefix, ok := core.PrefixHash(quepaxa.Slot(cut))
	if !ok {
		t.Fatal("actual Graph pair cut has no certified prefix")
	}
	next, following, err := core.CheckpointLeaderOrders(quepaxa.Slot(cut))
	if err != nil {
		t.Fatal("derive actual Graph pair checkpoint leader order", err)
	}
	seal := quepaxa.CheckpointSeal{
		ConfigID: core.ConfigIDForSlot(quepaxa.Slot(cut)), Index: quepaxa.Slot(cut),
		RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix,
		NextLeaderOrder: next, FollowingLeaderOrder: following,
		GenerationAnchorHash: core.GenerationAnchorHash(),
	}
	value, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal("encode actual Graph QCKP", err)
	}
	initialQCKP := make(chan struct {
		slot quepaxa.Slot
		err  error
	}, 1)
	go func() {
		slot, _, proposeErr := core.Propose(ctx, value)
		initialQCKP <- struct {
			slot quepaxa.Slot
			err  error
		}{slot: slot, err: proposeErr}
	}()
	if !fixture.transport.waitEntered(ctx, 9) {
		t.Fatal("initial actual Graph QCKP did not reach the held slot-9 recorder")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		initialProgress := core.RuntimeProgressSnapshot()
		if initialProgress.RecorderTip == 9 && initialProgress.CertifiedTip == 8 && initialProgress.RecorderHash == quepaxa.ValueHash(sha256.Sum256(value)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("initial Graph QCKP did not create exact R=9: %+v", initialProgress)
		}
		time.Sleep(time.Millisecond)
	}
	releaseManagedAppendOnlyAdmissionGate(t, fixture.transport, 9)
	select {
	case result := <-initialQCKP:
		if result.err != nil || result.slot != 9 {
			t.Fatalf("initial actual Graph QCKP failed after R=9 gate release: slot=%d err=%v", result.slot, result.err)
		}
	case <-ctx.Done():
		t.Fatal("initial actual Graph QCKP did not finish after R=9 gate release")
	}
	if err := fixture.node.replayLocalDecisions(ctx); err != nil {
		t.Fatal("replay initial actual Graph QCKP", err)
	}
	if err := fixture.node.archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal("archive initial actual Graph QCKP", err)
	}
	selected, exists, err := core.LatestManagedCheckpointSeal()
	if err != nil || !exists || selected.Index != seal.Index || selected.RootHash != seal.RootHash {
		t.Fatalf("actual immutable Graph QCKP was not selected: selected=%+v exists=%t err=%v", selected, exists, err)
	}
	// Revalidate through a Core-less Node so the verifier must cold-read the
	// actual immutable pair and archived certified history, not a local cut.
	coldNode := New(fixture.node.config)
	coldNode.managedStartup = fixture.node.managedStartup
	coldNode.checkpoints = fixture.node.checkpoints
	coldNode.archive = fixture.node.archive
	candidate := quepaxa.ManagedCheckpointCandidate{
		Value: bytes.Clone(value), Hash: quepaxa.ValueHash(sha256.Sum256(value)),
		Seal: seal, DecisionSlot: selected.DecisionSlot,
		ConfigID: core.ConfigIDForSlot(selected.DecisionSlot),
	}
	if err := coldNode.verifyManagedAppendOnlyCandidate(ctx, candidate); err != nil {
		t.Fatal("cold verifier failed actual immutable pair/archive read", err)
	}

	// Make a second actual QCKP at R=10 from a distinct Core. Its record on n1
	// is real and durable, while the remote proposer waits for the held response
	// and the third voter is held before Record. This is the precise pending-R
	// shape that admission must classify and wait through.
	fixture.transport.addGate(10)
	fixture.transport.addGate(11)
	remoteGate := installManagedAppendOnlyRemoteProposerGate(t, fixture, "n1", 10)
	defer remoteGate.release()
	remoteCore := installManagedAppendOnlyRemoteQCKPCore(t, fixture, remoteGate)
	remoteOwner := make(chan struct {
		slot quepaxa.Slot
		err  error
	}, 1)
	go func() {
		slot, _, proposeErr := remoteCore.Propose(ctx, value)
		remoteOwner <- struct {
			slot quepaxa.Slot
			err  error
		}{slot: slot, err: proposeErr}
	}()
	if !remoteGate.waitEntered(ctx) {
		t.Fatal("remote actual QCKP did not reach held n1 record response")
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		progress := core.RuntimeProgressSnapshot()
		if progress.RecorderTip == 10 && progress.CertifiedTip == 9 && progress.RecorderHashPresent && progress.RecorderHash == candidate.Hash {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("n1 did not retain exact pending remote QCKP R=10: %+v", progress)
		}
		time.Sleep(time.Millisecond)
	}
	for {
		secondProgress := remoteCore.RuntimeProgressSnapshot()
		secondValue, secondValuePresent := remoteCore.Value(candidate.Hash)
		if secondProgress.RecorderTip == 10 && secondProgress.CertifiedTip == 9 && secondProgress.RecorderHashPresent && secondProgress.RecorderHash == candidate.Hash && secondValuePresent && bytes.Equal(secondValue, value) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("actual QCKP proposer did not retain exact uncertified self-record at R=10: progress=%+v value_present=%t value_matches=%t", secondProgress, secondValuePresent, bytes.Equal(secondValue, value))
		}
		time.Sleep(time.Millisecond)
	}
	// Put the exact QCKP at Step-5 FirstCurrent on every real recorder before
	// admission can race its own barrier into R=10. These are actual durable
	// Record calls, not a synthesized certificate; the remote proposer below
	// still has to drive the QCKP through normal consensus at R=10.
	var qckpPriority quepaxa.Priority
	for index := range qckpPriority {
		qckpPriority[index] = 0xff
	}
	for id, peer := range fixture.transport.graphReviewTransport {
		summary, err := peer.Record(ctx, quepaxa.RecordRequest{
			Slot: 10, Step: 5, ConfigID: 1,
			Proposal: quepaxa.Proposal{Priority: qckpPriority, ProposerID: "n2", Hash: candidate.Hash, Value: bytes.Clone(value)},
		})
		stored, present := peer.Value(candidate.Hash)
		if err != nil || summary.Step != 5 || summary.FirstCurrent == nil || summary.FirstCurrent.Hash != candidate.Hash || !present || !bytes.Equal(stored, value) {
			t.Fatalf("recorder %s did not durably retain the exact QCKP as Step-5 FirstCurrent: summary=%+v err=%v", id, summary, err)
		}
	}
	admission := make(chan error, 1)
	go func() { admission <- fixture.node.admitInternalApplication(ctx) }()
	var owned *managedGraphControlOperation
	observationWaitStart := time.Now()
	for owned == nil || owned.observation == nil {
		select {
		case err := <-admission:
			t.Fatalf("admission returned before the held R=10 owner completed: %v", err)
		case <-ctx.Done():
			t.Fatalf("admission did not retain its R=10 observation before caller context ended: progress=%+v", core.RuntimeProgressSnapshot())
		default:
		}
		fixture.node.managedGraphControlMu.Lock()
		owned = fixture.node.managedGraphControl
		fixture.node.managedGraphControlMu.Unlock()
		if owned == nil || owned.observation == nil {
			progress := core.RuntimeProgressSnapshot()
			if progress.RecorderTip != 10 || progress.CertifiedTip != 9 || !progress.RecorderHashPresent || progress.RecorderHash != candidate.Hash {
				t.Fatalf("R=10 observation window drifted before the owned operation was created: progress=%+v operation=%+v", progress, owned)
			}
			select {
			case err := <-admission:
				t.Fatalf("admission returned before the held R=10 owner completed: %v", err)
			case <-ctx.Done():
				t.Fatalf("admission did not retain its R=10 observation before caller context ended: progress=%+v", core.RuntimeProgressSnapshot())
			case <-time.After(time.Millisecond):
			}
		}
	}
	t.Logf("phase=node_admission_qckp_observation_ready elapsed_ms=%d", time.Since(observationWaitStart).Milliseconds())
	if owned == nil || owned.observation == nil || owned.observation.ObservedSlot != 10 || !owned.observation.BelongsTo(core) || owned.slot != 0 {
		t.Fatalf("admission did not retain the exact R=10 observation: %+v", owned)
	}
	sealObserved := false
	for _, proposal := range owned.observation.Proposals {
		if proposal.Hash == candidate.Hash && proposal.Kind == quepaxa.ManagedAppendOnlyCheckpointSeal {
			sealObserved = true
		}
	}
	if !sealObserved {
		t.Fatalf("actual QCKP R=10 did not pass the cold-control object verifier: %+v", owned.observation.Proposals)
	}
	if decision, decided := core.CertifiedValue(10); decided {
		t.Fatalf("R=10 QCKP unexpectedly certified while its exact remote owner response is held: %+v", decision)
	}
	if decision, decided := core.CertifiedValue(11); decided {
		t.Fatalf("S=11 barrier unexpectedly certified before R=10 completed: %+v", decision)
	}
	select {
	case err := <-admission:
		t.Fatalf("foreground admission escaped before R certification: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, ok := core.CertifiedValue(10); ok {
		t.Fatal("R=10 QCKP became certified before its owner gate was released")
	}
	remoteGate.release()
	releaseManagedAppendOnlyAdmissionGate(t, fixture.transport, 10)
	select {
	case result := <-remoteOwner:
		if result.err != nil || result.slot != 10 {
			t.Fatalf("remote actual QCKP owner did not certify exact R=10: slot=%d err=%v", result.slot, result.err)
		}
	case <-ctx.Done():
		t.Fatal("remote QCKP owner did not finish after gate release")
	}
	decision, ok := waitManagedAppendOnlyCertifiedValue(ctx, core, 10)
	if !ok || decision.Hash != candidate.Hash || !bytes.Equal(decision.Value, value) {
		t.Fatalf("remote R=10 QCKP certificate differs from the immutable pair: ok=%t decision=%+v", ok, decision)
	}
	if err := fixture.node.archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal("archive actual R=10 QCKP certificate", err)
	}
	if err := coldNode.verifyManagedAppendOnlyCandidate(ctx, quepaxa.ManagedCheckpointCandidate{
		Value: bytes.Clone(value), Hash: candidate.Hash, Seal: seal,
		DecisionSlot: 10, ConfigID: core.ConfigIDForSlot(10),
	}); err != nil {
		t.Fatal("cold verifier rejected exact R=10 QCKP from actual archived history", err)
	}
	if !fixture.transport.waitEntered(ctx, 11) {
		select {
		case err := <-admission:
			t.Fatalf("admission returned before its S=11 barrier: %v", err)
		default:
		}
		t.Fatalf("admission did not submit its separate S=11 control after R=10 certification: progress=%+v", core.RuntimeProgressSnapshot())
	}
	if _, ok := core.CertifiedValue(11); ok {
		t.Fatal("S=11 barrier unexpectedly certified while its transport gate is held")
	}
	select {
	case err := <-admission:
		t.Fatalf("foreground admission escaped before S=11 and replay: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	fixture.transport.releaseAll() // release S=11 only after R=10 is certified.
	select {
	case err := <-admission:
		if err != nil {
			t.Fatal("Graph admission failed after exact QCKP, barrier, and replay completion", err)
		}
	case <-ctx.Done():
		t.Fatal("Graph admission did not finish after R/S gates were released")
	}
	barrier, ok := core.CertifiedValue(owned.slot)
	if !ok || owned.slot < 10 || quepaxa.ValueHash(sha256.Sum256(owned.value)) != owned.hash || barrier.Hash != owned.hash || !bytes.Equal(barrier.Value, owned.value) {
		t.Fatalf("admission did not certify its exact S>=R barrier: op=%+v ok=%t barrier=%+v", owned, ok, barrier)
	}
	progress := core.RuntimeProgressSnapshot()
	if progress.RecorderTip != progress.CertifiedTip || uint64(progress.CertifiedTip) > fixture.node.material.Tip() {
		t.Fatalf("admission returned before the certified suffix was replayed: progress=%+v applied=%d", progress, fixture.node.material.Tip())
	}
	decisions, _, err := core.DecisionsFrom(9, 16)
	if err != nil {
		t.Fatal("read exact post-cut certified history", err)
	}
	for _, decided := range decisions {
		if _, checkpointValue, decodeErr := quepaxa.DecodeCheckpointSeal(decided.Value); checkpointValue && decodeErr == nil {
			continue
		}
		if barrierValue, decodeErr := types.DecodeReadBarrier(decided.Value); decodeErr == nil && barrierValue {
			continue
		}
		t.Fatalf("foreground application intent was certified before/through admission controls: slot=%d hash=%x", decided.Slot, decided.Hash)
	}
}

type managedAppendOnlyRemoteRecordGate struct {
	entered     chan struct{}
	releaseCh   chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
	transport   *managedAppendOnlyRemoteQCKPTransport
}

type managedAppendOnlyRemoteQCKPTransport struct {
	*managedAppendOnlyAdmissionTransport
	gate   *managedAppendOnlyRemoteRecordGate
	target quepaxa.NodeID
	slot   quepaxa.Slot
}

func (transport *managedAppendOnlyRemoteQCKPTransport) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	if request.Slot == transport.slot && to == "n3" {
		select {
		case <-transport.gate.releaseCh:
		case <-ctx.Done():
			return quepaxa.Summary{}, ctx.Err()
		}
	}
	summary, err := transport.managedAppendOnlyAdmissionTransport.SendRecord(ctx, to, request)
	if err != nil {
		return quepaxa.Summary{}, err
	}
	if request.Slot == transport.slot && to == transport.target {
		transport.gate.enteredOnce.Do(func() { close(transport.gate.entered) })
		select {
		case <-transport.gate.releaseCh:
		case <-ctx.Done():
			return quepaxa.Summary{}, ctx.Err()
		}
	}
	return summary, nil
}

func (gate *managedAppendOnlyRemoteRecordGate) waitEntered(ctx context.Context) bool {
	select {
	case <-gate.entered:
		return true
	case <-ctx.Done():
		return false
	case <-time.After(2 * time.Second):
		return false
	}
}

func (gate *managedAppendOnlyRemoteRecordGate) release() {
	gate.releaseOnce.Do(func() { close(gate.releaseCh) })
}

func installManagedAppendOnlyRemoteProposerGate(t *testing.T, fixture *managedGraphAppendOnlyAdmissionFixture, target quepaxa.NodeID, slot quepaxa.Slot) *managedAppendOnlyRemoteRecordGate {
	t.Helper()
	gate := &managedAppendOnlyRemoteRecordGate{entered: make(chan struct{}), releaseCh: make(chan struct{})}
	gate.transport = &managedAppendOnlyRemoteQCKPTransport{managedAppendOnlyAdmissionTransport: fixture.transport, gate: gate, target: target, slot: slot}
	return gate
}

func installManagedAppendOnlyRemoteQCKPCore(t *testing.T, fixture *managedGraphAppendOnlyAdmissionFixture, gate *managedAppendOnlyRemoteRecordGate) *quepaxa.Core {
	t.Helper()
	node1 := fixture.node
	protocol := sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
	config := &types.ExecutionConfig{
		NodeID: "n2", ClusterID: node1.config.ClusterID, Members: node1.config.Members,
		DataDir: t.TempDir(), ObjStorePrefix: "managed-graph-append-only-admission",
		ObjStoreProvider: "memory", ObjStoreBucket: "test", ObjStoreRegion: "local",
	}
	node2 := New(config)
	node2.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedGraphRequestMaterializationPolicy, Identity: ManagedStartupIdentity{
		ApplicationProtocol: protocol, EvidenceFormat: protocol,
		Member: recovery.PreparedStartupIdentity{ClusterUID: string(config.ClusterID), NodeID: "n2"},
	}}
	source, memberDigest, err := node2.preparedBindingDigestsForProtocol(protocol)
	if err != nil {
		t.Fatal(err)
	}
	planDigest, err := node2.ManagedBootstrapPlanDigestForProtocol(source, protocol, protocol)
	if err != nil {
		t.Fatal(err)
	}
	node2.managedStartup.Identity.PhysicalBinding = source
	node2.managedStartup.Identity.BootstrapPlanDigest = planDigest
	node2.managedStartup.Identity.Member.SourceBinding = source
	node2.managedStartup.Identity.Member.ConfigDigest = memberDigest
	node2.checkpoints, node2.archive = node1.checkpoints, node1.archive

	prior := fixture.transport.graphReviewTransport["n2"]
	identityBytes, err := hex.DecodeString(prior.WALIdentity())
	if err != nil || len(identityBytes) != 32 {
		t.Fatalf("actual n2 WAL identity unavailable: %v", err)
	}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	if err := wal.BindIdentity(identityBytes); err != nil {
		t.Fatal(err)
	}
	var walIdentity [32]byte
	copy(walIdentity[:], identityBytes)
	transport := gate.transport
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: "n2", Cluster: node1.core.CurrentCluster(), WAL: wal, Transport: transport,
		ValidateValue: types.ValidateManagedGraphExecutionPolicy,
		ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{Binding: quepaxa.ManagedAppendOnlyBinding{
			ApplicationProtocol: protocol, BootstrapPlanDigest: planDigest,
			MemberConfigDigest: memberDigest, Generation: 0, WALIdentity: walIdentity,
		}, Verify: node2.verifyManagedAppendOnlyCandidate},
	})
	if err != nil {
		t.Fatalf("construct actual n2 remote QCKP proposer: %v", err)
	}
	node2.core = core
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return node2.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	page, tip, err := node1.core.DecisionsFrom(1, 16)
	if err != nil || tip < node1.core.Tip() || len(page) != int(node1.core.Tip()) {
		t.Fatalf("actual certified n1 prefix unavailable to n2 proposer: len=%d tip=%d err=%v", len(page), tip, err)
	}
	if err := core.AcceptCertifiedValues(page); err != nil {
		t.Fatalf("install exact actual certified prefix on n2 proposer: %v", err)
	}
	fixture.transport.graphReviewTransport["n2"] = core
	if core.Tip() != node1.core.Tip() || core.RecorderTip() != core.Tip() {
		t.Fatalf("n2 proposer did not recover the exact prefix: n1=%d n2=%d recorded=%d", node1.core.Tip(), core.Tip(), core.RecorderTip())
	}
	return core
}

type managedAppendOnlyPreRecordGate struct {
	entered     chan struct{}
	releaseCh   chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

type managedAppendOnlyPreRecordTransport struct {
	*managedAppendOnlyAdmissionTransport
	gate *managedAppendOnlyPreRecordGate
}

func (transport *managedAppendOnlyPreRecordTransport) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	if to == "n2" && request.Slot == 9 {
		if barrier, err := types.DecodeReadBarrier(request.Proposal.Value); err == nil && barrier {
			transport.gate.enteredOnce.Do(func() { close(transport.gate.entered) })
			select {
			case <-transport.gate.releaseCh:
			case <-ctx.Done():
				return quepaxa.Summary{}, ctx.Err()
			}
		}
	}
	return transport.managedAppendOnlyAdmissionTransport.SendRecord(ctx, to, request)
}

func (gate *managedAppendOnlyPreRecordGate) waitEntered(ctx context.Context) bool {
	select {
	case <-gate.entered:
		return true
	case <-ctx.Done():
		return false
	case <-time.After(2 * time.Second):
		return false
	}
}

func (gate *managedAppendOnlyPreRecordGate) release() {
	gate.releaseOnce.Do(func() { close(gate.releaseCh) })
}

func installManagedAppendOnlyPreRecordGate(t *testing.T, fixture *managedGraphAppendOnlyAdmissionFixture) *managedAppendOnlyPreRecordGate {
	t.Helper()
	node := fixture.node
	base := node.core
	walIdentity, registered := node.wal.Identity()
	if !registered || len(walIdentity) != 32 {
		t.Fatalf("test fixture WAL identity unavailable: registered=%t bytes=%d", registered, len(walIdentity))
	}
	var identity [32]byte
	copy(identity[:], walIdentity)
	managedConfig := &quepaxa.ManagedAppendOnlyConfig{Binding: quepaxa.ManagedAppendOnlyBinding{
		ApplicationProtocol: node.managedStartup.Identity.ApplicationProtocol,
		BootstrapPlanDigest: node.managedStartup.Identity.BootstrapPlanDigest,
		MemberConfigDigest:  node.managedStartup.Identity.Member.ConfigDigest,
		Generation:          node.managedStartup.Identity.Member.Generation,
		WALIdentity:         identity,
	}, Verify: node.verifyManagedAppendOnlyCandidate}
	gate := &managedAppendOnlyPreRecordGate{entered: make(chan struct{}), releaseCh: make(chan struct{})}
	transport := &managedAppendOnlyPreRecordTransport{managedAppendOnlyAdmissionTransport: fixture.transport, gate: gate}
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: node.config.NodeID, Cluster: base.CurrentCluster(), WAL: node.wal, Transport: transport,
		ValidateValue: types.ValidateManagedGraphExecutionPolicy, ManagedAppendOnly: managedConfig,
	})
	if err != nil {
		t.Fatalf("reopen actual Node core with test-only peer gate: %v", err)
	}
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return node.checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	oldServer := node.server
	if oldServer != nil {
		oldServer.Close()
	}
	profile, err := node.managedAppendOnlyProfile()
	if err != nil || profile == nil {
		t.Fatalf("load trusted test fixture profile: profile=%+v err=%v", profile, err)
	}
	server, err := network.NewServerForManagedProfile(core, node.material, node.config.ClusterID, true, nil, *profile)
	if err != nil {
		t.Fatalf("rebind actual Node server to test-gated Core: %v", err)
	}
	node.core = core
	node.server = server
	binding := node.internalApplication.Load()
	if binding == nil {
		t.Fatal("test fixture has no internal application binding")
	}
	binding.core = core
	fixture.transport.graphReviewTransport[node.config.NodeID] = core
	t.Cleanup(server.Close)
	return gate
}

func releaseManagedAppendOnlyAdmissionGate(t *testing.T, transport *managedAppendOnlyAdmissionTransport, slot quepaxa.Slot) {
	t.Helper()
	transport.mu.Lock()
	gate := transport.gates[slot]
	transport.mu.Unlock()
	if gate == nil {
		t.Fatalf("missing transport gate for slot %d", slot)
	}
	gate.releaseOnce.Do(func() { close(gate.release) })
}

func waitManagedAppendOnlyCertifiedValue(ctx context.Context, core *quepaxa.Core, slot quepaxa.Slot) (quepaxa.DecidedValue, bool) {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if decision, ok := core.CertifiedValue(slot); ok {
			return decision, true
		}
		select {
		case <-ctx.Done():
			return quepaxa.DecidedValue{}, false
		case <-ticker.C:
		}
	}
}
