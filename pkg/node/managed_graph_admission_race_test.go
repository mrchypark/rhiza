package node

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

// graphAdmissionRaceTransport uses real Core.Record calls. For the actual
// slot-9 barrier only, n2's durable recorder response is held and n3 is held
// before recording, leaving n1 with a real pending recorder entry but no
// recorder quorum/certificate. No decision state is injected by the test.
type graphAdmissionRaceTransport struct {
	graphReviewTransport
	held         chan struct{}
	release      chan struct{}
	uncertified  bool
	heldOnce     sync.Once
	releaseOnce  sync.Once
	readTipCalls atomic.Int32
}

func (p *graphAdmissionRaceTransport) ReadTip(ctx context.Context, id quepaxa.NodeID) (quepaxa.Slot, error) {
	p.readTipCalls.Add(1)
	return p.graphReviewTransport.ReadTip(ctx, id)
}

func (p *graphAdmissionRaceTransport) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	isHeldBarrier, err := types.DecodeReadBarrier(request.Proposal.Value)
	if err != nil {
		return quepaxa.Summary{}, err
	}
	_, isHeldSeal, sealErr := quepaxa.DecodeCheckpointSeal(request.Proposal.Value)
	if sealErr != nil {
		return quepaxa.Summary{}, sealErr
	}
	if request.Slot == 9 && (isHeldBarrier || isHeldSeal) {
		if to == "n3" {
			select {
			case <-p.release:
			case <-ctx.Done():
				return quepaxa.Summary{}, ctx.Err()
			}
			if p.uncertified {
				return quepaxa.Summary{}, fmt.Errorf("test transport withheld recorder quorum")
			}
		}
		summary, err := p.graphReviewTransport[to].Record(ctx, request)
		if err != nil {
			return quepaxa.Summary{}, err
		}
		if to == "n2" {
			p.heldOnce.Do(func() { close(p.held) })
			select {
			case <-p.release:
			case <-ctx.Done():
				return quepaxa.Summary{}, ctx.Err()
			}
			if p.uncertified {
				return quepaxa.Summary{}, fmt.Errorf("test transport withheld recorder quorum")
			}
		}
		return summary, nil
	}
	return p.graphReviewTransport[to].Record(ctx, request)
}

func (p *graphAdmissionRaceTransport) releaseHeld() {
	p.releaseOnce.Do(func() { close(p.release) })
}

func (p *graphAdmissionRaceTransport) releaseHeldUncertified() {
	p.uncertified = true
	p.releaseHeld()
}

func TestManagedGraphAdmissionJoinsStableActualPendingControl(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	proposal := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	wantControl := managedGraphRaceControlValue()
	gate := make(chan error, 1)
	go func() { gate <- fixture.node.admitInternalApplication(ctx) }()
	awaitManagedGraphAdmissionObservation(t, fixture, gate, 9)

	fixture.transport.releaseHeld()
	if err := awaitProposal(t, proposal); err != nil {
		t.Fatalf("actual control did not certify after release: %v", err)
	}
	select {
	case err := <-gate:
		if err != nil {
			t.Fatalf("admission did not complete after the exact owned barrier: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("admission did not complete after barrier release", ctx.Err())
	}
	assertManagedGraphRaceBarrierSuffix(t, fixture, wantControl)
}

func TestManagedGraphAdmissionPendingControlCompletesSeparateBarrier(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	proposal := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	wantControl := managedGraphRaceControlValue()
	gate := make(chan error, 1)
	go func() { gate <- fixture.node.admitInternalApplication(ctx) }()
	awaitManagedGraphAdmissionObservation(t, fixture, gate, 9)
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, proposal); err != nil {
		t.Fatalf("actual control did not certify after cleanup release: %v", err)
	}
	select {
	case err := <-gate:
		if err != nil {
			t.Fatalf("admission failed after pending barrier completed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("admission did not finish", ctx.Err())
	}
	assertManagedGraphRaceBarrierSuffix(t, fixture, wantControl)
}

func TestManagedGraphAdmissionReplaysStableCertifiedApplyGap(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	proposal := fixture.startHeldControl(ctx)
	fixture.awaitPendingControl(t)
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, proposal); err != nil {
		t.Fatalf("actual control did not certify: %v", err)
	}
	if fixture.node.core.Tip() != 9 || fixture.node.material.Tip() != 8 {
		t.Fatalf("fixture lacks a stable certified/apply gap: certified=%d applied=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}
	if err := fixture.node.admitInternalApplication(ctx); err != nil {
		t.Fatalf("admission did not recover the actual certified suffix: %v", err)
	}
	if fixture.node.core.Tip() != 9 || fixture.node.material.Tip() != 9 {
		t.Fatalf("certified suffix was not replayed: certified=%d applied=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}
}

func TestManagedGraphAdmissionWaitsForExactOwnedCheckpointControl(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	server, err := network.NewServerForPolicy(fixture.node.core, fixture.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node.server = server
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	owner := make(chan error, 1)
	go func() { owner <- fixture.node.advanceManagedGraphCheckpointCut(ctx) }()
	fixture.awaitPendingControl(t)

	foreground := make(chan error, 1)
	go func() {
		if err := fixture.node.admitInternalApplication(ctx); err != nil {
			foreground <- err
			return
		}
		_, err := fixture.node.server.GraphExecute(ctx, types.GraphCommand{RequestID: "owned-control-foreground", Cypher: "CREATE (:OwnedControlAdmission {ok: true})"})
		foreground <- err
	}()
	select {
	case err := <-foreground:
		t.Fatalf("admission completed before its matching owned control: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if fixture.node.core.Tip() != 8 || fixture.node.material.Tip() != 8 {
		t.Fatalf("foreground advanced before owner completion: certified=%d applied=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}

	fixture.transport.releaseHeld()
	if err := awaitProposal(t, owner); err != nil {
		t.Fatalf("owned checkpoint control did not finish: %v", err)
	}
	select {
	case err := <-foreground:
		if err != nil {
			t.Fatalf("foreground application did not proceed after exact control certification: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("foreground remained blocked after exact control certification")
	}
	if fixture.node.core.Tip() != 10 || fixture.node.material.Tip() != 10 {
		t.Fatalf("foreground did not submit exactly once after control replay: certified=%d applied=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}
}

func TestManagedGraphAdmissionWaitsForOwnedControlDurabilityCompletion(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	server, err := network.NewServerForPolicy(fixture.node.core, fixture.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node.server = server
	t.Cleanup(server.Close)
	durable := make(chan struct{})
	releaseDurable := make(chan struct{})
	var held sync.Once
	server.SetDurabilityBarrier(func(ctx context.Context, slot quepaxa.Slot) error {
		if slot != 9 {
			return nil
		}
		held.Do(func() { close(durable) })
		select {
		case <-releaseDurable:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	owner := make(chan error, 1)
	go func() { owner <- fixture.node.advanceManagedGraphCheckpointCut(ctx) }()
	fixture.awaitPendingControl(t)
	fixture.transport.releaseHeld()
	select {
	case <-durable:
	case <-ctx.Done():
		t.Fatal("owned control did not reach the actual durability boundary")
	}
	if fixture.node.core.Tip() != 9 || fixture.node.material.Tip() != 9 {
		t.Fatalf("durability hold is not post-certificate/apply: certified=%d applied=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}

	foreground := make(chan error, 1)
	go func() {
		if err := fixture.node.admitInternalApplication(ctx); err != nil {
			foreground <- err
			return
		}
		_, err := fixture.node.server.GraphExecute(ctx, types.GraphCommand{RequestID: "durable-owned-control-foreground", Cypher: "CREATE (:DurableOwnedControl {ok: true})"})
		foreground <- err
	}()
	select {
	case err := <-foreground:
		t.Fatalf("foreground passed before owner durability completion: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if fixture.node.core.Tip() != 9 || fixture.node.material.Tip() != 9 {
		t.Fatalf("foreground advanced while owner durability was held: certified=%d applied=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}
	close(releaseDurable)
	if err := awaitProposal(t, owner); err != nil {
		t.Fatalf("owned control did not complete after durability release: %v", err)
	}
	select {
	case err := <-foreground:
		if err != nil {
			t.Fatalf("foreground failed after owner durability completion: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("foreground remained blocked after owner durability completion")
	}
	if fixture.node.core.Tip() != 10 || fixture.node.material.Tip() != 10 {
		t.Fatalf("foreground did not submit once after durability completion: certified=%d applied=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}
}

func TestManagedGraphAdmissionConcurrentWaitersUseLocalObservedSlot(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	server, err := network.NewServerForPolicy(fixture.node.core, fixture.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node.server = server
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	owner := make(chan error, 1)
	go func() { owner <- fixture.node.advanceManagedGraphCheckpointCut(ctx) }()
	fixture.awaitPendingControl(t)
	waiters := make(chan error, 2)
	for range 2 {
		go func() { waiters <- fixture.node.admitInternalApplication(ctx) }()
	}
	select {
	case err := <-waiters:
		t.Fatalf("concurrent waiter escaped before the fixed owned slot completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, owner); err != nil {
		t.Fatalf("owner did not complete: %v", err)
	}
	for range 2 {
		select {
		case err := <-waiters:
			if err != nil {
				t.Fatalf("concurrent waiter failed after exact certificate: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent waiter remained blocked")
		}
	}
}

func TestManagedGraphAdmissionRejectsCertifiedControlWithDurabilityError(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	server, err := network.NewServerForPolicy(fixture.node.core, fixture.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node.server = server
	t.Cleanup(server.Close)
	durable := make(chan struct{})
	releaseDurable := make(chan struct{})
	var durableOnce sync.Once
	server.SetDurabilityBarrier(func(ctx context.Context, slot quepaxa.Slot) error {
		if slot == 9 {
			durableOnce.Do(func() { close(durable) })
			select {
			case <-releaseDurable:
			case <-ctx.Done():
				return ctx.Err()
			}
			return fmt.Errorf("test durability failure")
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	owner := make(chan error, 1)
	go func() { owner <- fixture.node.advanceManagedGraphCheckpointCut(ctx) }()
	fixture.awaitPendingControl(t)
	fixture.transport.releaseHeld()
	select {
	case <-durable:
	case <-ctx.Done():
		t.Fatal("owner did not reach durability failure boundary")
	}
	if fixture.node.core.Tip() != 9 || fixture.node.material.Tip() != 9 {
		t.Fatalf("expected post-certificate/apply state: certified=%d applied=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}
	waiter := make(chan error, 1)
	go func() { waiter <- fixture.node.admitInternalApplication(ctx) }()
	select {
	case err := <-waiter:
		t.Fatalf("waiter escaped before owner durability result: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseDurable)
	if err := awaitProposal(t, owner); err == nil {
		t.Fatal("owner reported success despite durability failure")
	}
	select {
	case err := <-waiter:
		if err == nil {
			t.Fatal("waiting admission accepted owner durability error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waiting admission did not return after owner durability error")
	}
	if _, ok := fixture.node.core.CertifiedValue(9); !ok || fixture.node.core.Tip() != 9 || fixture.node.material.Tip() != 9 {
		t.Fatalf("test failed to create certified/applied control with owner error: certified=%d applied=%d", fixture.node.core.Tip(), fixture.node.material.Tip())
	}
}

func TestManagedGraphAdmissionOwnedControlWaitHonorsCallerCancellation(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	server, err := network.NewServerForPolicy(fixture.node.core, fixture.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node.server = server
	t.Cleanup(server.Close)
	ownerCtx, ownerCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ownerCancel()
	owner := make(chan error, 1)
	go func() { owner <- fixture.node.advanceManagedGraphCheckpointCut(ownerCtx) }()
	fixture.awaitPendingControl(t)
	gateCtx, cancelGate := context.WithCancel(context.Background())
	gate := make(chan error, 1)
	go func() { gate <- fixture.node.admitInternalApplication(gateCtx) }()
	select {
	case err := <-gate:
		t.Fatalf("admission did not wait for owned control: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancelGate()
	select {
	case err := <-gate:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("caller cancellation was not preserved: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("caller cancellation did not release owned-control wait")
	}
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, owner); err != nil {
		t.Fatalf("owner cleanup failed after caller cancellation: %v", err)
	}
	if fixture.node.core.Tip() != 9 {
		t.Fatalf("canceled caller submitted a foreground proposal: tip=%d", fixture.node.core.Tip())
	}
}

func TestManagedGraphAdmissionOwnedControlWaitCancelsWithBindingLifetime(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	server, err := network.NewServerForPolicy(fixture.node.core, fixture.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node.server = server
	t.Cleanup(server.Close)
	ownerCtx, ownerCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ownerCancel()
	owner := make(chan error, 1)
	go func() { owner <- fixture.node.advanceManagedGraphCheckpointCut(ownerCtx) }()
	fixture.awaitPendingControl(t)

	admitted := make(chan error, 1)
	go func() { admitted <- fixture.node.admitInternalApplication(context.Background()) }()
	select {
	case err := <-admitted:
		t.Fatalf("admission did not wait for matching owned control: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	_ = fixture.node.stopInternalApplication()
	select {
	case err := <-admitted:
		if err == nil {
			t.Fatal("binding lifetime cancellation unexpectedly admitted the operation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Host/binding lifetime did not cancel owned-control wait")
	}
	fixture.transport.releaseHeld()
	if err := awaitProposal(t, owner); err != nil {
		t.Fatalf("owned control cleanup failed: %v", err)
	}
	if fixture.node.core.Tip() != 9 {
		t.Fatalf("canceled admission submitted a foreground proposal: tip=%d", fixture.node.core.Tip())
	}
}

func TestManagedGraphAdmissionOwnedControlWithoutCertificateRemainsFailClosed(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	server, err := network.NewServerForPolicy(fixture.node.core, fixture.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node.server = server
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	owner := make(chan error, 1)
	go func() { owner <- fixture.node.advanceManagedGraphCheckpointCut(ctx) }()
	fixture.awaitPendingControl(t)
	fixture.transport.releaseHeldUncertified()
	if err := awaitProposal(t, owner); err == nil {
		t.Fatal("owned control without a recorder quorum was reported successful")
	}
	if _, ok := fixture.node.core.CertifiedValue(9); ok {
		t.Fatal("test unexpectedly certified the pending control")
	}
	if err := fixture.node.admitInternalApplication(ctx); err == nil || !strings.Contains(err.Error(), "target=9 recorded=9 certified=8 applied=8") {
		t.Fatalf("uncertified owned control did not preserve fail-closed refusal: %v", err)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if fixture.node.core.Tip() != 8 || progress.RecorderTip != 9 || progress.CertifiedTip != 8 || fixture.node.material.Tip() != 8 {
		t.Fatalf("admission changed uncertified control state: progress=%+v applied=%d", progress, fixture.node.material.Tip())
	}
}

func TestManagedGraphAdmissionRejectsEarlyUnknownOwnerReturn(t *testing.T) {
	fixture := newManagedGraphAdmissionRaceFixture(t)
	server, err := network.NewServerForPolicy(fixture.node.core, fixture.node.material, "managed-graph-race", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	fixture.node.server = server
	t.Cleanup(server.Close)
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	owner := make(chan error, 1)
	go func() { owner <- fixture.node.advanceManagedGraphCheckpointCut(ownerCtx) }()
	fixture.awaitPendingControl(t)
	fixture.node.managedGraphControlMu.Lock()
	owned := fixture.node.managedGraphControl
	if owned == nil {
		fixture.node.managedGraphControlMu.Unlock()
		t.Fatal("owner did not publish its actual control operation")
	}
	wantControl := append([]byte(nil), owned.value...)
	fixture.node.managedGraphControlMu.Unlock()
	cancelOwner()
	ownerErr := awaitProposal(t, owner)
	if !errors.Is(ownerErr, network.ErrCommitUnknown) {
		t.Fatalf("canceled proposer did not report the early unknown outcome: %v", ownerErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if fixture.node.core.Tip() != 8 || progress.RecorderTip != 9 || progress.CertifiedTip != 8 || fixture.node.material.Tip() != 8 {
		t.Fatalf("foreground advanced before worker completion: progress=%+v applied=%d", progress, fixture.node.material.Tip())
	}
	gate := make(chan error, 1)
	go func() { gate <- fixture.node.admitInternalApplication(ctx) }()
	awaitManagedGraphAdmissionObservation(t, fixture, gate, 9)
	fixture.transport.releaseHeld()
	select {
	case err := <-gate:
		if err != nil {
			t.Fatalf("admission did not complete after actual owner recovery: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("admission did not finish after release", ctx.Err())
	}
	assertManagedGraphRaceBarrierSuffix(t, fixture, wantControl)
}

type managedGraphAdmissionRaceFixture struct {
	node      *Node
	transport *graphAdmissionRaceTransport
}

func newManagedGraphAdmissionRaceFixture(t *testing.T) *managedGraphAdmissionRaceFixture {
	t.Helper()
	transport := &graphAdmissionRaceTransport{graphReviewTransport: graphReviewTransport{}, held: make(chan struct{}), release: make(chan struct{})}
	protocol := sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
	clusterID := types.ClusterID("managed-graph-race")
	cluster := quepaxa.Cluster{ConfigID: 1}
	bucket := thanosobjstore.NewInMemBucket()
	var node1 *Node
	walByNode := make(map[quepaxa.NodeID]*qlog.WAL, 3)
	for i, id := range []quepaxa.NodeID{"n1", "n2", "n3"} {
		identity := []byte(strings.Repeat(fmt.Sprintf("%d", i), 32))
		if len(identity) != 32 {
			t.Fatalf("test WAL identity length=%d, want 32", len(identity))
		}
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal-"+string(id)))
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.BindIdentity(identity); err != nil {
			_ = wal.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		walByNode[id] = wal
		member := peerMember(string(clusterID), id, string(id)+"-token")
		member.PeerURL = "quic://" + string(id) + ":9090"
		member.WALIdentity = hex.EncodeToString(identity)
		cluster.Members = append(cluster.Members, member)
	}
	for _, member := range cluster.Members {
		config := &types.ExecutionConfig{NodeID: member.ID, ClusterID: clusterID, Members: cluster.Members, DataDir: t.TempDir(), ObjStorePrefix: "managed-graph-race", ObjStoreProvider: "memory", ObjStoreBucket: "test", ObjStoreRegion: "local"}
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
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := archive.Load(ctx); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		n.checkpoints, n.archive = manager, archive
		walIdentity, err := hex.DecodeString(member.WALIdentity)
		if err != nil || len(walIdentity) != 32 {
			t.Fatalf("invalid fixture WAL identity: %v", err)
		}
		var identity [32]byte
		copy(identity[:], walIdentity)
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: cluster, WAL: walByNode[member.ID], Transport: transport,
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
		transport.graphReviewTransport[member.ID] = core
		n.core, n.wal = core, walByNode[member.ID]
		if member.ID == "n1" {
			node1 = n
		}
	}
	n := node1
	if n == nil {
		t.Fatal("managed Graph test node missing")
	}
	mat, err := n.openMaterializer(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	n.material = mat
	t.Cleanup(func() { _ = n.material.Close() })

	for slot := uint64(1); slot <= 8; slot++ {
		var nonce [types.ReadBarrierNonceSize]byte
		for i := range nonce {
			nonce[i] = byte(slot + uint64(i))
		}
		value := types.EncodeReadBarrier(nonce)
		got, _, err := n.core.Propose(context.Background(), value)
		if err != nil || uint64(got) != slot {
			t.Fatalf("actual setup barrier slot=%d, got=%d err=%v", slot, got, err)
		}
		if err := n.material.Apply(context.Background(), slot, value); err != nil {
			t.Fatal(err)
		}
	}
	if n.core.Tip() != 8 || n.core.RecorderTip() != 8 || n.material.Tip() != 8 {
		t.Fatalf("fixture did not establish actual aligned prefix: certified=%d recorded=%d applied=%d", n.core.Tip(), n.core.RecorderTip(), n.material.Tip())
	}

	n.opened.Store(true)
	n.managedPeerVerified.Store(true)
	if err := n.installInternalApplicationBinding(context.Background()); err != nil {
		t.Fatal(err)
	}
	profile := network.ManagedPeerProfile{ApplicationProtocol: protocol, BootstrapPlanDigest: n.managedStartup.Identity.BootstrapPlanDigest, Generation: 0}
	server, err := network.NewServerForManagedProfile(n.core, mat, clusterID, true, nil, profile)
	if err != nil {
		t.Fatal(err)
	}
	n.server = server
	t.Cleanup(server.Close)
	n.managedInitiallyAdmitted.Store(true)
	n.managedGraphInitialRootDone = make(chan struct{})
	close(n.managedGraphInitialRootDone)

	return &managedGraphAdmissionRaceFixture{node: n, transport: transport}
}

func (f *managedGraphAdmissionRaceFixture) startHeldControl(ctx context.Context) <-chan error {
	return f.startHeldValue(ctx, managedGraphRaceControlValue())
}

func (f *managedGraphAdmissionRaceFixture) startHeldValue(ctx context.Context, value []byte) <-chan error {
	result := make(chan error, 1)
	go func() {
		slot, _, err := f.node.core.Propose(ctx, value)
		if err == nil && slot != 9 {
			err = fmt.Errorf("control proposal used slot %d, want 9", slot)
		}
		result <- err
	}()
	select {
	case <-f.transport.held:
	case <-ctx.Done():
		return result
	case <-time.After(2 * time.Second):
		return result
	}
	return result
}

func managedGraphRaceControlValue() []byte {
	var nonce [types.ReadBarrierNonceSize]byte
	for i := range nonce {
		nonce[i] = byte(200 + i)
	}
	return types.EncodeReadBarrier(nonce)
}

func awaitManagedGraphAdmissionObservation(t *testing.T, fixture *managedGraphAdmissionRaceFixture, result <-chan error, observedSlot uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-result:
			t.Fatalf("admission returned before the exact observed barrier completed: %v", err)
		default:
		}
		fixture.node.managedGraphControlMu.Lock()
		operation := fixture.node.managedGraphControl
		observed := operation != nil && operation.observation != nil && operation.observation.ObservedSlot == quepaxa.Slot(observedSlot) && operation.observation.BelongsTo(fixture.node.core)
		fixture.node.managedGraphControlMu.Unlock()
		if observed {
			return
		}
		time.Sleep(time.Millisecond) // Poll for the concrete operation state; silence alone is never evidence.
	}
	fixture.node.managedGraphControlMu.Lock()
	operation := fixture.node.managedGraphControl
	fixture.node.managedGraphControlMu.Unlock()
	progress := fixture.node.core.RuntimeProgressSnapshot()
	t.Fatalf("admission did not retain the exact R=%d observation: operation=%+v progress=%+v", observedSlot, operation, progress)
}

func assertManagedGraphRaceBarrierSuffix(t *testing.T, fixture *managedGraphAdmissionRaceFixture, original []byte) {
	t.Helper()
	if isBarrier, err := types.DecodeReadBarrier(original); err != nil || !isBarrier {
		t.Fatalf("original owner value is not a pure read barrier: %x, %v", original, err)
	}
	decision, ok := fixture.node.core.CertifiedValue(9)
	if !ok || !bytes.Equal(decision.Value, original) {
		t.Fatalf("slot 9 did not preserve the exact original control: found=%t value=%x want=%x", ok, decision.Value, original)
	}
	if isBarrier, err := types.DecodeReadBarrier(decision.Value); err != nil || !isBarrier {
		t.Fatalf("slot 9 is not a pure read barrier: %v", err)
	}
	second, ok := fixture.node.core.CertifiedValue(10)
	if !ok || bytes.Equal(second.Value, original) {
		t.Fatalf("admission did not use a distinct certified slot-10 barrier: found=%t value=%x", ok, second.Value)
	}
	if isBarrier, err := types.DecodeReadBarrier(second.Value); err != nil || !isBarrier {
		t.Fatalf("slot 10 is not a pure read barrier: %v", err)
	}
	progress := fixture.node.core.RuntimeProgressSnapshot()
	if fixture.node.core.Tip() != 10 || progress.RecorderTip != 10 || fixture.node.material.Tip() != 10 {
		t.Fatalf("expected exact original-plus-fresh-barrier suffix, progress=%+v applied=%d", progress, fixture.node.material.Tip())
	}
}

func (f *managedGraphAdmissionRaceFixture) awaitPendingControl(t *testing.T) {
	t.Helper()
	select {
	case <-f.transport.held:
	case <-time.After(2 * time.Second):
		t.Fatal("actual slot-9 control was not recorded by n2")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f.node.core.RecorderTip() == 9 && f.node.core.Tip() == 8 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("held control did not create the expected real pending state: recorded=%d certified=%d", f.node.core.RecorderTip(), f.node.core.Tip())
}

func awaitProposal(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("actual control proposal did not finish after release")
		return nil
	}
}
