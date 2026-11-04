//go:build rhiza_local_testhooks

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

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type managedGraphInitialGapTransport struct {
	cores             map[quepaxa.NodeID]*quepaxa.Core
	local             quepaxa.NodeID
	heldDecision      chan quepaxa.Decision
	readTipCalls      atomic.Int32
	maxReadTip        atomic.Uint64
	allowLocalRecords atomic.Bool
	readIndexMu       sync.Mutex
	readIndexRounds   map[context.Context]struct{}
}

func (p *managedGraphInitialGapTransport) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	if to == p.local && !p.allowLocalRecords.Load() {
		return quepaxa.Summary{}, quepaxa.ErrQuorumUnavailable
	}
	core := p.cores[to]
	if core == nil {
		return quepaxa.Summary{}, quepaxa.ErrQuorumUnavailable
	}
	return core.Record(ctx, request)
}

func (p *managedGraphInitialGapTransport) SendDecision(_ context.Context, decision quepaxa.Decision) error {
	accepted := 0
	for id, core := range p.cores {
		if id == p.local {
			continue
		}
		if err := core.AcceptDecision(decision); err == nil {
			accepted++
		}
	}
	if accepted < 2 {
		return quepaxa.ErrQuorumUnavailable
	}
	select {
	case p.heldDecision <- decision:
	default:
		return fmt.Errorf("initial-gap decision gate was already occupied")
	}
	return nil
}

func (p *managedGraphInitialGapTransport) releaseLocalDecision() (quepaxa.Decision, error) {
	decision := <-p.heldDecision
	return decision, p.cores[p.local].AcceptDecision(decision)
}

func (p *managedGraphInitialGapTransport) ReadTip(ctx context.Context, to quepaxa.NodeID) (quepaxa.Slot, error) {
	if ctx.Done() != nil {
		// Core.ReadIndex shares one derived context across its ReadTip fanout;
		// another ReadIndex call creates a distinct context.
		p.readIndexMu.Lock()
		p.readIndexRounds[ctx] = struct{}{}
		p.readIndexMu.Unlock()
	}
	p.readTipCalls.Add(1)
	core := p.cores[to]
	if core == nil {
		return 0, quepaxa.ErrQuorumUnavailable
	}
	tip := core.Tip()
	for current := p.maxReadTip.Load(); uint64(tip) > current; current = p.maxReadTip.Load() {
		if p.maxReadTip.CompareAndSwap(current, uint64(tip)) {
			break
		}
	}
	return tip, nil
}

func (p *managedGraphInitialGapTransport) readIndexRoundCount() int {
	p.readIndexMu.Lock()
	defer p.readIndexMu.Unlock()
	return len(p.readIndexRounds)
}

func (p *managedGraphInitialGapTransport) StageValue(_ context.Context, to quepaxa.NodeID, hash quepaxa.ValueHash, value []byte) error {
	core := p.cores[to]
	if core == nil {
		return quepaxa.ErrQuorumUnavailable
	}
	return core.StageValue(hash, value)
}

func (p *managedGraphInitialGapTransport) FetchValue(_ context.Context, from quepaxa.NodeID, hash quepaxa.ValueHash) ([]byte, error) {
	core := p.cores[from]
	if core == nil {
		return nil, quepaxa.ErrQuorumUnavailable
	}
	value, ok := core.Value(hash)
	if !ok {
		return nil, fmt.Errorf("actual proposal value unavailable")
	}
	return value, nil
}

type managedGraphInitialGapFixture struct {
	transport *managedGraphInitialGapTransport
	nodes     map[quepaxa.NodeID]*Node
	node      *Node
	material  *materializer.Materializer
	checker   *managedTestChecker
	barrier   []byte
}

func newManagedGraphInitialGapFixture(t *testing.T) *managedGraphInitialGapFixture {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	transport := &managedGraphInitialGapTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), local: "n1", heldDecision: make(chan quepaxa.Decision, 1), readIndexRounds: make(map[context.Context]struct{})}
	clusterID := types.ClusterID("managed-graph-initial-readindex-gap")
	protocol := sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
	cluster := quepaxa.Cluster{ConfigID: 1}
	wals := make(map[quepaxa.NodeID]*qlog.WAL, 3)
	nodes := make(map[quepaxa.NodeID]*Node, 3)
	bucket := objstore.NewInMemBucket()
	t.Cleanup(func() {
		if err := bucket.Close(); err != nil {
			t.Errorf("close initial-admission object bucket: %v", err)
		}
	})

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
		wals[id] = wal
		t.Cleanup(func() {
			if err := wal.Close(); err != nil {
				t.Errorf("close initial-admission WAL: %v", err)
			}
		})
		member := peerMember(string(clusterID), id, string(id)+"-token")
		member.PeerURL = "quic://" + string(id) + ":9090"
		member.WALIdentity = hex.EncodeToString(identity)
		cluster.Members = append(cluster.Members, member)
	}

	for _, member := range cluster.Members {
		config := &types.ExecutionConfig{NodeID: member.ID, ClusterID: clusterID, Members: cluster.Members, DataDir: t.TempDir(), ObjStorePrefix: "managed-graph-initial-readindex-gap", ObjStoreProvider: "memory", ObjStoreBucket: "test", ObjStoreRegion: "local"}
		n := New(config)
		n.managedStartup = &ManagedStartupConfig{Checker: &managedTestChecker{}, expectedPolicy: sqlpolicy.ManagedGraphRequestMaterializationPolicy, Identity: ManagedStartupIdentity{ApplicationProtocol: protocol, EvidenceFormat: protocol, Member: recovery.PreparedStartupIdentity{ClusterUID: string(clusterID), NodeID: string(member.ID)}}}
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
		t.Cleanup(archive.Close)
		if err := archive.Load(ctx); err != nil {
			t.Fatal(err)
		}
		n.checkpoints, n.archive = manager, archive
		var walIdentity [32]byte
		copy(walIdentity[:], mustDecodeInitialGapIdentity(t, member.WALIdentity))
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: member.ID, Cluster: cluster, WAL: wals[member.ID], Transport: transport,
			ValidateValue: types.ValidateManagedGraphExecutionPolicy,
			ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{Binding: quepaxa.ManagedAppendOnlyBinding{
				ApplicationProtocol: protocol, BootstrapPlanDigest: planDigest, MemberConfigDigest: memberDigest, Generation: 0, WALIdentity: walIdentity,
			}, Verify: n.verifyManagedAppendOnlyCandidate},
		})
		if err != nil {
			t.Fatal(err)
		}
		core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
			return manager.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
		})
		n.core, n.wal = core, wals[member.ID]
		transport.cores[member.ID] = core
		nodes[member.ID] = n
	}

	n := nodes[transport.local]
	checker := n.managedStartup.Checker.(*managedTestChecker)
	mat, err := materializer.OpenManagedSQLKVGraphWithAppendOnlyOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1, types.DefaultIdempotencyWindowSlots)
	if err != nil {
		t.Fatal(err)
	}
	n.material = mat
	t.Cleanup(func() {
		if err := mat.Close(); err != nil {
			t.Errorf("close initial-admission materializer: %v", err)
		}
	})
	n.opened.Store(true)
	n.managedPeerVerified.Store(true)
	n.managedGraphInitialRootDone = make(chan struct{})
	close(n.managedGraphInitialRootDone)
	if err := n.installInternalApplicationBinding(ctx); err != nil {
		t.Fatal(err)
	}
	profile := network.ManagedPeerProfile{ApplicationProtocol: protocol, BootstrapPlanDigest: n.managedStartup.Identity.BootstrapPlanDigest, Generation: 0}
	server, err := network.NewServerForManagedProfile(n.core, mat, clusterID, true, nil, profile)
	if err != nil {
		t.Fatal(err)
	}
	n.server = server
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		if binding := n.stopInternalApplication(); binding != nil {
			binding.active.Wait()
		}
		n.wg.Wait()
	})

	proposal := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{10, 11, 12})
	proposed := make(chan error, 1)
	go func() {
		slot, _, err := nodes["n2"].core.Propose(ctx, proposal)
		if err == nil && slot != 1 {
			err = fmt.Errorf("actual control used slot %d, want 1", slot)
		}
		proposed <- err
	}()
	select {
	case err := <-proposed:
		if err != nil {
			t.Fatalf("actual two-peer control certification failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("actual control proposal did not finish: %v", ctx.Err())
	}
	if nodes["n2"].core.Tip() != 1 || nodes["n3"].core.Tip() != 1 || n.core.Tip() != 0 || n.core.RecorderTip() != 0 {
		t.Fatalf("fixture lacks a real quorum-certified remote/local-delivery gap: peer tips=(%d,%d) local certified=%d recorded=%d", nodes["n2"].core.Tip(), nodes["n3"].core.Tip(), n.core.Tip(), n.core.RecorderTip())
	}

	return &managedGraphInitialGapFixture{transport: transport, nodes: nodes, node: n, material: mat, checker: checker, barrier: proposal}
}

func TestManagedGraphInitialAdmissionWaitsForReadIndexLocalDelivery(t *testing.T) {
	fixture := newManagedGraphInitialGapFixture(t)
	n, transport, mat, checker := fixture.node, fixture.transport, fixture.material, fixture.checker
	readTipsBefore := transport.readTipCalls.Load()
	transport.maxReadTip.Store(0)
	admitCtx, cancelAdmit := context.WithTimeout(context.Background(), 300*time.Millisecond)
	admitErr := n.EnsureInitialManagedAdmission(admitCtx)
	cancelAdmit()
	readTipsAfter := transport.readTipCalls.Load()
	if readTipsAfter <= readTipsBefore || transport.maxReadTip.Load() != 1 || checker.admissionCalls.Load() != 0 {
		t.Fatalf("initial path did not observe quorum T=1 while local admission remained unapproved: read-tip calls=%d->%d max-tip=%d checker calls=%d", readTipsBefore, readTipsAfter, transport.maxReadTip.Load(), checker.admissionCalls.Load())
	}
	if n.core.Tip() != 0 || n.core.RecorderTip() != 0 || mat.Tip() != 0 {
		t.Fatalf("bounded local wait changed application state: certified=%d recorded=%d applied=%d", n.core.Tip(), n.core.RecorderTip(), mat.Tip())
	}
	if !errors.Is(admitErr, context.DeadlineExceeded) {
		t.Fatalf("initial admission did not preserve caller deadline while waiting for local certification: T=1 R=C=A=0 err=%v", admitErr)
	}
	released, err := transport.releaseLocalDecision()
	if err != nil {
		t.Fatalf("deliver exact held certified decision to local Core: %v", err)
	}
	if !bytes.Equal(released.Proposal.Value, fixture.barrier) {
		t.Fatal("retry delivered a different decision than the original ReadIndex barrier")
	}
	retryCtx, cancelRetry := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRetry()
	if err := n.EnsureInitialManagedAdmission(retryCtx); err != nil {
		t.Fatalf("initial admission did not complete after actual local delivery: %v", err)
	}
	if n.core.Tip() < 1 || mat.Tip() < 1 || checker.admissionCalls.Load() != 1 || !n.managedInitiallyAdmitted.Load() {
		t.Fatalf("actual local decision was not replayed/admitted exactly once: certified=%d recorded=%d applied=%d checker_calls=%d", n.core.Tip(), n.core.RecorderTip(), mat.Tip(), checker.admissionCalls.Load())
	}
}

func TestManagedGraphInitialAdmissionContinuesSameCallAfterLocalDelivery(t *testing.T) {
	runManagedGraphInitialAdmissionDelivery(t, false)
}

func TestManagedGraphInitialAdmissionStillChecksAuthorityAfterLocalDelivery(t *testing.T) {
	runManagedGraphInitialAdmissionDelivery(t, true)
}

func TestManagedGraphInitialAdmissionRejectsNewOrdinaryRecorderGapAfterFixedTarget(t *testing.T) {
	fixture := newManagedGraphInitialGapFixture(t)
	n, transport, mat, checker := fixture.node, fixture.transport, fixture.material, fixture.checker
	waitEntered, resumeWait := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(resumeWait) }) }
	t.Cleanup(resume)
	restore := localtesthooks.Set(func(event string) {
		if event == "core:wait-tip:waiting" {
			close(waitEntered)
			<-resumeWait
		}
	})
	t.Cleanup(restore)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	completed := make(chan error, 1)
	go func() { completed <- n.EnsureInitialManagedAdmission(ctx) }()
	select {
	case <-waitEntered:
	case err := <-completed:
		t.Fatalf("initial admission returned before fixed-target wait: %v", err)
	case <-ctx.Done():
		t.Fatal("initial admission did not reach fixed-target wait")
	}
	if _, err := transport.releaseLocalDecision(); err != nil {
		t.Fatalf("deliver target certificate to local Core: %v", err)
	}
	value, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "initial-wait-tip-gap", Cypher: "CREATE (:WaitTipGap {n:2})"})
	if err != nil {
		t.Fatal(err)
	}
	transport.allowLocalRecords.Store(true)
	slot, _, err := fixture.nodes["n2"].core.Propose(ctx, value)
	if err != nil || slot != 2 {
		t.Fatalf("create actual later ordinary Graph recorder/certificate after target capture: slot=%d err=%v", slot, err)
	}
	if n.core.Tip() != 1 || n.core.RecorderTip() != 2 || mat.Tip() != 0 || fixture.nodes["n2"].core.Tip() != 2 || fixture.nodes["n3"].core.Tip() != 2 {
		t.Fatalf("fixture did not preserve new ordinary R>C gap: local=(certified=%d recorded=%d applied=%d) peer_tips=(%d,%d)", n.core.Tip(), n.core.RecorderTip(), mat.Tip(), fixture.nodes["n2"].core.Tip(), fixture.nodes["n3"].core.Tip())
	}
	resume()
	select {
	case err := <-completed:
		if err == nil {
			t.Fatal("fixed-target wait incorrectly admitted new ordinary recorder gap")
		}
	case <-ctx.Done():
		t.Fatal("initial admission did not reject the ordinary recorder gap")
	}
	if n.core.Tip() != 1 || n.core.RecorderTip() != 2 || mat.Tip() != 1 || checker.admissionCalls.Load() != 0 || n.managedInitiallyAdmitted.Load() || n.managedAdmitted.Load() {
		t.Fatalf("ordinary gap crossed admission gate: certified=%d recorded=%d applied=%d checker=%d initially_admitted=%t admitted=%t", n.core.Tip(), n.core.RecorderTip(), mat.Tip(), checker.admissionCalls.Load(), n.managedInitiallyAdmitted.Load(), n.managedAdmitted.Load())
	}
}

func TestManagedGraphInitialAdmissionLifetimeCloseCancelsAndDrainsFixedWait(t *testing.T) {
	fixture := newManagedGraphInitialGapFixture(t)
	n := fixture.node
	waitEntered, resumeWait := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(resumeWait) }) }
	t.Cleanup(resume)
	restore := localtesthooks.Set(func(event string) {
		if event == "core:wait-tip:waiting" {
			close(waitEntered)
			<-resumeWait
		}
	})
	t.Cleanup(restore)
	completed := make(chan error, 1)
	go func() { completed <- n.EnsureInitialManagedAdmission(context.Background()) }()
	select {
	case <-waitEntered:
	case err := <-completed:
		t.Fatalf("initial admission returned before lifetime wait: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("initial admission did not reach fixed-target wait")
	}
	resume()
	binding := n.stopInternalApplication()
	if binding == nil {
		t.Fatal("expected actual operation binding to close")
	}
	select {
	case err := <-completed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("binding close did not cancel the fixed-target wait: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fixed-target wait did not drain after binding close")
	}
	drained := make(chan struct{})
	go func() { binding.active.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("binding close returned before the operation borrower drained")
	}
	if n.core.Tip() != 0 || n.core.RecorderTip() != 0 || fixture.material.Tip() != 0 || fixture.checker.admissionCalls.Load() != 0 {
		t.Fatalf("canceled wait changed runtime state: certified=%d recorded=%d applied=%d checker=%d", n.core.Tip(), n.core.RecorderTip(), fixture.material.Tip(), fixture.checker.admissionCalls.Load())
	}
}

func runManagedGraphInitialAdmissionDelivery(t *testing.T, denyAuthority bool) {
	fixture := newManagedGraphInitialGapFixture(t)
	n, transport, mat, checker := fixture.node, fixture.transport, fixture.material, fixture.checker
	waitEntered, resumeWait := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(resumeWait) }) }
	t.Cleanup(resume)
	restore := localtesthooks.Set(func(event string) {
		if event == "core:wait-tip:waiting" {
			close(waitEntered)
			<-resumeWait
		}
	})
	t.Cleanup(restore)

	before := transport.readTipCalls.Load()
	roundsBefore := transport.readIndexRoundCount()
	transport.maxReadTip.Store(0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	completed := make(chan error, 1)
	go func() { completed <- n.EnsureInitialManagedAdmission(ctx) }()
	select {
	case <-waitEntered:
	case err := <-completed:
		t.Fatalf("initial admission returned before reaching the fixed-target local wait: %v", err)
	case <-ctx.Done():
		t.Fatal("initial admission did not reach the local wait")
	}
	afterReadIndex := transport.readTipCalls.Load()
	roundsAfterReadIndex := transport.readIndexRoundCount()
	if afterReadIndex <= before || roundsAfterReadIndex != roundsBefore+1 || transport.maxReadTip.Load() != 1 || n.core.Tip() != 0 || n.core.RecorderTip() != 0 || mat.Tip() != 0 || checker.admissionCalls.Load() != 0 {
		t.Fatalf("wait boundary did not preserve one logical ReadIndex round at T=1 R=C=A=0: RPCs=%d->%d rounds=%d->%d max-tip=%d state=(%d,%d,%d) checker=%d", before, afterReadIndex, roundsBefore, roundsAfterReadIndex, transport.maxReadTip.Load(), n.core.RecorderTip(), n.core.Tip(), mat.Tip(), checker.admissionCalls.Load())
	}
	checker.denyAdmission.Store(denyAuthority)
	resume()
	released, err := transport.releaseLocalDecision()
	if err != nil {
		t.Fatalf("deliver exact held certified decision to same waiting call: %v", err)
	}
	if !bytes.Equal(released.Proposal.Value, fixture.barrier) {
		t.Fatal("same-call delivery used a different decision than the original ReadIndex barrier")
	}
	select {
	case err := <-completed:
		if denyAuthority && err == nil {
			t.Fatal("initial admission ignored the live authority rejection after local certification")
		}
		if !denyAuthority && err != nil {
			t.Fatalf("same initial admission call failed after local certification: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("same initial admission call did not complete after local delivery")
	}
	if transport.readIndexRoundCount() != roundsAfterReadIndex || n.core.Tip() < 1 || mat.Tip() < 1 || checker.admissionCalls.Load() != 1 || n.managedInitiallyAdmitted.Load() == denyAuthority || n.managedAdmitted.Load() == denyAuthority || (!denyAuthority && !n.managedGraphPublisherStarted.Load()) {
		t.Fatalf("initial call did not preserve one fixed-target ReadIndex and live authority: rounds=%d->%d certified=%d recorded=%d applied=%d checker=%d admitted=%t deny=%t", roundsAfterReadIndex, transport.readIndexRoundCount(), n.core.Tip(), n.core.RecorderTip(), mat.Tip(), checker.admissionCalls.Load(), n.managedInitiallyAdmitted.Load(), denyAuthority)
	}
}

func mustDecodeInitialGapIdentity(t *testing.T, raw string) []byte {
	t.Helper()
	identity, err := hex.DecodeString(raw)
	if err != nil || len(identity) != 32 {
		t.Fatalf("invalid fixture WAL identity: %v", err)
	}
	return identity
}
