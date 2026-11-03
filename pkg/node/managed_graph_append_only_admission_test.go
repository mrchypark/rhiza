package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	thanosobjstore "github.com/thanos-io/objstore"
)

func TestManagedGraphAppendOnlyAdmissionCancellationBeforeIntent(t *testing.T) {
	fixture := newManagedGraphAppendOnlyAdmissionFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := fixture.node.admitInternalApplication(ctx); err == nil {
		t.Fatal("canceled caller was admitted")
	}
	if fixture.node.core.Tip() != 8 || fixture.node.core.RecorderTip() != 8 || fixture.node.managedGraphControl != nil {
		t.Fatalf("cancellation before admission intent changed consensus: certified=%d recorded=%d operation=%+v", fixture.node.core.Tip(), fixture.node.core.RecorderTip(), fixture.node.managedGraphControl)
	}
}

func TestManagedGraphAppendOnlyAdmissionJoinsActivePublisherBarrier(t *testing.T) {
	fixture := newManagedGraphAppendOnlyAdmissionFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	publisher := make(chan error, 1)
	go func() { publisher <- fixture.node.advanceManagedGraphCheckpointCut(ctx) }()
	if !fixture.transport.waitEntered(ctx, 9) {
		t.Fatal("actual publisher barrier did not reach the controlled recorder")
	}
	deadline := time.Now().Add(2 * time.Second)
	for fixture.node.core.RecorderTip() != 9 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	fixture.node.managedGraphControlMu.Lock()
	owned := fixture.node.managedGraphControl
	fixture.node.managedGraphControlMu.Unlock()
	if owned == nil || owned.observation != nil || owned.binding != fixture.node.internalApplication.Load() {
		t.Fatal("publisher operation did not retain its exact local binding")
	}
	admission := make(chan error, 1)
	go func() { admission <- fixture.node.admitInternalApplication(ctx) }()
	select {
	case err := <-admission:
		t.Fatalf("admission bypassed active publisher barrier: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	fixture.transport.releaseAll()
	if err := awaitProposal(t, publisher); err != nil {
		t.Fatalf("actual publisher barrier failed: %v", err)
	}
	select {
	case err := <-admission:
		if err != nil {
			t.Fatalf("admission did not join exact publisher barrier: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("admission did not finish after publisher barrier", ctx.Err())
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if progress.RecorderTip != 9 || progress.CertifiedTip != 9 || fixture.node.material.Tip() != 9 {
		t.Fatalf("publisher join did not finish exact certified prefix: progress=%+v applied=%d", progress, fixture.node.material.Tip())
	}
}

func TestManagedGraphAppendOnlyAdmissionCancellationAfterOwnedBarrierStarts(t *testing.T) {
	fixture := newManagedGraphAppendOnlyAdmissionFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	remoteOwner := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	fixture.transport.addGate(10)
	ownerCtx, cancelOwner := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() { result <- fixture.node.admitInternalApplication(ownerCtx) }()
	if !fixture.transport.waitEntered(ctx, 10) {
		t.Fatal("owned barrier did not reach the actual slot-10 recorder")
	}
	deadline := time.Now().Add(2 * time.Second)
	for fixture.node.core.RecorderTip() != 10 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	fixture.node.managedGraphControlMu.Lock()
	owned := fixture.node.managedGraphControl
	fixture.node.managedGraphControlMu.Unlock()
	if owned == nil || owned.observation == nil || progress.RecorderTip != 10 || progress.RecorderHash != owned.hash {
		t.Fatalf("owned barrier was not the actual pending value before cancel: progress=%+v operation=%+v", progress, owned)
	}
	cancelOwner()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled barrier owner returned %v", err)
		}
	case <-ctx.Done():
		t.Fatal("admission waiter did not stop after caller cancellation")
	}
	select {
	case <-owned.done:
		if owned.err == nil {
			t.Fatal("owned barrier worker did not retain its cancellation error")
		}
	case <-ctx.Done():
		t.Fatal("owned barrier worker outlived its caller cancellation")
	}
	if err := fixture.node.admitInternalApplication(ctx); err == nil || !strings.Contains(err.Error(), "target=10 recorded=10 certified=8") {
		t.Fatalf("fresh caller retried or accepted the still-unresolved canceled control: %v", err)
	}
	fixture.node.managedGraphControlMu.Lock()
	if fixture.node.managedGraphControl != nil {
		fixture.node.managedGraphControlMu.Unlock()
		t.Fatal("settled failed operation was not reaped for the fresh caller")
	}
	fixture.node.managedGraphControlMu.Unlock()
	if fixture.node.core.RecorderTip() != 10 {
		t.Fatalf("fresh unresolved-state caller created another intent: recorder=%d", fixture.node.core.RecorderTip())
	}
	fixture.transport.releaseAll()
	if err := awaitProposal(t, remoteOwner); err != nil {
		t.Fatalf("remote control did not cleanly finish after release: %v", err)
	}
	if fixture.node.core.RecorderTip() > 10 {
		t.Fatalf("canceled owner retried or submitted another intent: recorder tip=%d", fixture.node.core.RecorderTip())
	}
	settled := fixture.node.core.RuntimeProgressSnapshot()
	if settled.RecorderTip != settled.CertifiedTip {
		t.Fatalf("test requires settled recorder state before a fresh admission: %+v", settled)
	}
	if err := fixture.node.replayLocalDecisions(ctx); err != nil {
		t.Fatalf("replay the now-certified control before fresh admission: %v", err)
	}
	if err := fixture.node.admitInternalApplication(ctx); err != nil {
		t.Fatalf("later fresh admission remained poisoned by the settled canceled owner: %v", err)
	}
	fixture.node.managedGraphControlMu.Lock()
	remaining := fixture.node.managedGraphControl
	fixture.node.managedGraphControlMu.Unlock()
	if remaining != nil {
		t.Fatal("fresh admission left a stale operation pointer")
	}
	if !errors.Is(owned.err, context.Canceled) {
		t.Fatalf("fresh admission rewrote original owner's cancellation result: %v", owned.err)
	}
	for slot := quepaxa.Slot(9); slot <= fixture.node.core.RecorderTip(); slot++ {
		var value []byte
		if decision, ok := fixture.node.core.CertifiedValue(slot); ok {
			value = decision.Value
		} else if progress := fixture.node.core.RuntimeProgressSnapshot(); progress.RecorderTip == slot && progress.RecorderHashPresent {
			value, _ = fixture.node.core.Value(progress.RecorderHash)
		}
		if len(value) == 0 {
			t.Fatalf("slot %d has no exact retained value for cancellation outcome", slot)
		}
		if isBarrier, err := types.DecodeReadBarrier(value); err != nil || !isBarrier {
			t.Fatalf("cancellation introduced a non-control application value at slot %d: barrier=%t error=%v", slot, isBarrier, err)
		}
	}
}

func TestManagedGraphAppendOnlyAdmissionStillRefusesPendingApplication(t *testing.T) {
	fixture := newManagedGraphAppendOnlyAdmissionFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	value, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "pending-unknown-admission", Cypher: "CREATE (:PendingUnknown {ok:true})"})
	if err != nil {
		t.Fatal(err)
	}
	proposal := make(chan error, 1)
	go func() {
		_, _, err := fixture.node.core.Propose(ctx, value)
		proposal <- err
	}()
	if !fixture.transport.waitEntered(ctx, 9) {
		t.Fatal("actual application proposal did not reach the controlled recorder")
	}
	deadline := time.Now().Add(2 * time.Second)
	for fixture.node.core.RecorderTip() != 9 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if progress.RecorderTip != 9 || progress.CertifiedTip != 8 || progress.RecorderHash != quepaxa.ValueHash(sha256.Sum256(value)) {
		t.Fatalf("pending application value was not the actual recorded unknown: %+v", progress)
	}
	err = fixture.node.admitInternalApplication(ctx)
	if err == nil || !strings.Contains(err.Error(), "target=9 recorded=9 certified=8 applied=8") {
		t.Fatalf("pending application was not refused under strict admission: %v", err)
	}
	fixture.node.managedGraphControlMu.Lock()
	operation := fixture.node.managedGraphControl
	fixture.node.managedGraphControlMu.Unlock()
	if operation != nil {
		t.Fatal("admission created a barrier owner over application UNKNOWN")
	}
	fixture.transport.releaseAll()
	if err := awaitProposal(t, proposal); err != nil {
		t.Fatalf("actual application proposal did not complete after release: %v", err)
	}
	decision, ok := fixture.node.core.CertifiedValue(9)
	if !ok || !bytes.Equal(decision.Value, value) || fixture.node.core.RecorderTip() != 9 {
		t.Fatalf("admission changed or resubmitted the existing application: decision=%+v ok=%t recorder=%d", decision, ok, fixture.node.core.RecorderTip())
	}
}

type managedGraphAppendOnlyAdmissionFixture struct {
	node      *Node
	transport *managedAppendOnlyAdmissionTransport
}

func newManagedGraphAppendOnlyAdmissionFixture(t *testing.T) *managedGraphAppendOnlyAdmissionFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	transport := newManagedAppendOnlyAdmissionTransport()
	transport.addGate(9)
	protocol := sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
	clusterID := types.ClusterID("managed-graph-append-only-admission")
	cluster := quepaxa.Cluster{ConfigID: 1}
	wals := make(map[quepaxa.NodeID]*qlog.WAL, 3)
	for i, id := range []quepaxa.NodeID{"n1", "n2", "n3"} {
		identity := []byte(strings.Repeat(fmt.Sprintf("%d", i), 32))
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal-"+string(id)))
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.BindIdentity(identity); err != nil {
			_ = wal.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		wals[id] = wal
		member := peerMember(string(clusterID), id, string(id)+"-token")
		member.PeerURL = "quic://" + string(id) + ":9090"
		member.WALIdentity = hex.EncodeToString(identity)
		cluster.Members = append(cluster.Members, member)
	}
	bucket := thanosobjstore.NewInMemBucket()
	var node1 *Node
	for _, member := range cluster.Members {
		config := &types.ExecutionConfig{NodeID: member.ID, ClusterID: clusterID, Members: cluster.Members, DataDir: t.TempDir(), ObjStorePrefix: "managed-graph-append-only-admission", ObjStoreProvider: "memory", ObjStoreBucket: "test", ObjStoreRegion: "local"}
		n := New(config)
		n.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedGraphRequestMaterializationPolicy, Identity: ManagedStartupIdentity{ApplicationProtocol: protocol, EvidenceFormat: protocol, Member: recovery.PreparedStartupIdentity{ClusterUID: string(clusterID), NodeID: string(member.ID)}}}
		source, memberDigest, err := n.preparedBindingDigestsForProtocol(protocol)
		if err != nil {
			t.Fatal(err)
		}
		planDigest, err := n.ManagedBootstrapPlanDigestForProtocol(source, protocol, protocol)
		if err != nil {
			t.Fatal(err)
		}
		n.managedStartup.Identity.PhysicalBinding = source
		n.managedStartup.Identity.BootstrapPlanDigest = planDigest
		n.managedStartup.Identity.Member.SourceBinding = source
		n.managedStartup.Identity.Member.ConfigDigest = memberDigest
		manager, err := checkpoint.NewAppendOnlyManagerForPolicy(bucket, string(clusterID), t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
		if err != nil {
			t.Fatal(err)
		}
		archive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, string(clusterID), 1, types.ValidateManagedGraphExecutionPolicy)
		if err != nil {
			t.Fatal(err)
		}
		if err := archive.Load(ctx); err != nil {
			t.Fatal(err)
		}
		n.checkpoints, n.archive = manager, archive
		walIdentity, err := hex.DecodeString(member.WALIdentity)
		if err != nil || len(walIdentity) != 32 {
			t.Fatalf("invalid fixture WAL identity: %v", err)
		}
		var identity [32]byte
		copy(identity[:], walIdentity)
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: cluster, WAL: wals[member.ID], Transport: transport,
			ValidateValue: types.ValidateManagedGraphExecutionPolicy,
			ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{Binding: quepaxa.ManagedAppendOnlyBinding{
				ApplicationProtocol: protocol, BootstrapPlanDigest: planDigest, MemberConfigDigest: memberDigest, Generation: 0, WALIdentity: identity,
			}, Verify: n.verifyManagedAppendOnlyCandidate},
		})
		if err != nil {
			t.Fatal(err)
		}
		core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
			return manager.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
		})
		n.core, n.wal = core, wals[member.ID]
		transport.graphReviewTransport[member.ID] = core
		if member.ID == "n1" {
			node1 = n
		}
	}
	mat, err := node1.openMaterializer(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	node1.material = mat
	t.Cleanup(func() { _ = mat.Close() })
	node1.opened.Store(true)
	node1.managedPeerVerified.Store(true)
	node1.managedInitiallyAdmitted.Store(true)
	node1.managedGraphInitialRootDone = make(chan struct{})
	close(node1.managedGraphInitialRootDone)
	for slot := uint64(1); slot <= 8; slot++ {
		var nonce [types.ReadBarrierNonceSize]byte
		for i := range nonce {
			nonce[i] = byte(slot + uint64(i))
		}
		value := types.EncodeReadBarrier(nonce)
		got, _, err := node1.core.Propose(ctx, value)
		if err != nil || uint64(got) != slot {
			t.Fatalf("actual setup barrier slot=%d got=%d err=%v", slot, got, err)
		}
		if err := mat.Apply(ctx, slot, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := node1.installInternalApplicationBinding(ctx); err != nil {
		t.Fatal(err)
	}
	profile := network.ManagedPeerProfile{ApplicationProtocol: protocol, BootstrapPlanDigest: node1.managedStartup.Identity.BootstrapPlanDigest, Generation: 0}
	server, err := network.NewServerForManagedProfile(node1.core, mat, clusterID, true, nil, profile)
	if err != nil {
		t.Fatal(err)
	}
	node1.server = server
	t.Cleanup(server.Close)
	if node1.core.Tip() != 8 || node1.core.RecorderTip() != 8 || mat.Tip() != 8 {
		t.Fatalf("fixture did not establish aligned actual prefix: certified=%d recorded=%d applied=%d", node1.core.Tip(), node1.core.RecorderTip(), mat.Tip())
	}
	return &managedGraphAppendOnlyAdmissionFixture{node: node1, transport: transport}
}

func (f *managedGraphAppendOnlyAdmissionFixture) startHeldControl(ctx context.Context) <-chan error {
	result := make(chan error, 1)
	go func() {
		var nonce [types.ReadBarrierNonceSize]byte
		for i := range nonce {
			nonce[i] = byte(200 + i)
		}
		slot, _, err := f.node.core.Propose(ctx, types.EncodeReadBarrier(nonce))
		if err == nil && slot != 9 {
			err = fmt.Errorf("remote control used slot %d, want 9", slot)
		}
		result <- err
	}()
	if !f.transport.waitEntered(ctx, 9) {
		return result
	}
	return result
}

func (f *managedGraphAppendOnlyAdmissionFixture) awaitPendingControl(t *testing.T) {
	t.Helper()
	if !f.transport.waitEntered(context.Background(), 9) {
		t.Fatal("actual slot-9 barrier did not reach the controlled recorder")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.node.core.RecorderTip() == 9 && f.node.core.Tip() == 8 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expected real pending recorder state: recorded=%d certified=%d", f.node.core.RecorderTip(), f.node.core.Tip())
}

type managedAppendOnlyAdmissionGate struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

type managedAppendOnlyAdmissionTransport struct {
	graphReviewTransport
	mu    sync.Mutex
	gates map[quepaxa.Slot]*managedAppendOnlyAdmissionGate
}

func newManagedAppendOnlyAdmissionTransport() *managedAppendOnlyAdmissionTransport {
	return &managedAppendOnlyAdmissionTransport{graphReviewTransport: graphReviewTransport{}, gates: make(map[quepaxa.Slot]*managedAppendOnlyAdmissionGate)}
}

func (p *managedAppendOnlyAdmissionTransport) addGate(slot quepaxa.Slot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.gates[slot] == nil {
		p.gates[slot] = &managedAppendOnlyAdmissionGate{entered: make(chan struct{}), release: make(chan struct{})}
	}
}

func (p *managedAppendOnlyAdmissionTransport) releaseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, gate := range p.gates {
		gate.releaseOnce.Do(func() { close(gate.release) })
	}
}

func (p *managedAppendOnlyAdmissionTransport) waitEntered(ctx context.Context, slot quepaxa.Slot) bool {
	p.mu.Lock()
	gate := p.gates[slot]
	p.mu.Unlock()
	if gate == nil {
		return false
	}
	select {
	case <-gate.entered:
		return true
	case <-ctx.Done():
		return false
	case <-time.After(2 * time.Second):
		return false
	}
}

func (p *managedAppendOnlyAdmissionTransport) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	p.mu.Lock()
	gate := p.gates[request.Slot]
	p.mu.Unlock()
	if gate == nil {
		return p.graphReviewTransport[to].Record(ctx, request)
	}
	if to == "n3" {
		select {
		case <-gate.release:
		case <-ctx.Done():
			return quepaxa.Summary{}, ctx.Err()
		}
	}
	summary, err := p.graphReviewTransport[to].Record(ctx, request)
	if err != nil {
		return quepaxa.Summary{}, err
	}
	if to == "n2" {
		gate.enteredOnce.Do(func() { close(gate.entered) })
		select {
		case <-gate.release:
		case <-ctx.Done():
			return quepaxa.Summary{}, ctx.Err()
		}
	}
	return summary, nil
}
