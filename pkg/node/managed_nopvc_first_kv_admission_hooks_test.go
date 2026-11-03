//go:build rhiza_local_testhooks

package node

import (
	"context"
	"encoding/hex"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type managedNoPVCFirstKVCase struct {
	name        string
	application bool
	budgetScope bool
	advance     bool
	pending     bool
	cancel      bool
	binding     bool
	nodeClose   bool
}

func TestManagedNoPVCFirstKVAdmissionReplaysFixedCertifiedPrefix(t *testing.T) {
	for _, test := range []managedNoPVCFirstKVCase{
		{name: "pure-control"},
		{name: "direct-entry-budget-scope", budgetScope: true},
		{name: "certified-application", application: true},
		{name: "reject-growth-after-fixed-prefix", advance: true},
		{name: "reject-new-ordinary-recorder-gap", pending: true},
		{name: "caller-cancel-after-fixed-prefix", cancel: true},
		{name: "binding-close-after-fixed-prefix", binding: true},
		{name: "node-close-after-fixed-prefix", nodeClose: true},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) { testManagedNoPVCFirstKVAdmissionFixedPrefix(t, test) })
	}
}

func testManagedNoPVCFirstKVAdmissionFixedPrefix(t *testing.T, test managedNoPVCFirstKVCase) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const protocol = sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	const clusterID = types.ClusterID("managed-nopvc-first-kv-admission")
	transport := &managedAppendOnlyTestTransport{peers: make(map[quepaxa.NodeID]*quepaxa.Core), down: make(map[quepaxa.NodeID]bool)}
	cluster := quepaxa.Cluster{ConfigID: 1}
	walByID := make(map[quepaxa.NodeID]*qlog.WAL)
	nodes := make(map[quepaxa.NodeID]*Node)
	for i, id := range []quepaxa.NodeID{"n1", "n2", "n3"} {
		var nonce [32]byte
		for j := range nonce {
			nonce[j] = byte((i+1)*31 + j + 1)
		}
		member := peerMember(string(clusterID), id, string(id)+"-token")
		member.PeerURL = "quic://" + string(id) + ":9090"
		member.WALIdentity = hex.EncodeToString(nonce[:])
		cluster.Members = append(cluster.Members, member)
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal-"+string(id)))
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.BindIdentity(nonce[:]); err != nil {
			_ = wal.Close()
			t.Fatal(err)
		}
		walByID[id] = wal
		t.Cleanup(func() { _ = wal.Close() })
		config := &types.ExecutionConfig{NodeID: id, ClusterID: clusterID, Members: nil, DataDir: t.TempDir(), ObjStoreProvider: "memory", ObjStoreBucket: "test", ObjStorePrefix: "first-kv"}
		n := New(config)
		nodes[id] = n
	}
	for _, n := range nodes {
		n.config.Members = append([]quepaxa.Member(nil), cluster.Members...)
	}
	bucket := objstore.NewInMemBucket()
	manager, err := checkpoint.NewAppendOnlyManagerForPolicy(bucket, string(clusterID), t.TempDir(), sqlpolicy.ManagedRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, string(clusterID), 1, types.ValidateManagedKVExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	for i, id := range []quepaxa.NodeID{"n1", "n2", "n3"} {
		n := nodes[id]
		n.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedRequestMaterializationPolicy}
		source, memberDigest, err := n.preparedBindingDigestsForProtocol(protocol)
		if err != nil {
			t.Fatal(err)
		}
		planDigest, err := n.ManagedBootstrapPlanDigestForProtocol(source, protocol, protocol)
		if err != nil {
			t.Fatal(err)
		}
		member := recovery.PreparedStartupIdentity{ClusterUID: string(clusterID), NodeID: string(id), SourceBinding: source, ConfigDigest: memberDigest}
		n.managedStartup.Identity = ManagedStartupIdentity{ApplicationProtocol: protocol, EvidenceFormat: protocol, PhysicalBinding: source, BootstrapPlanDigest: planDigest, Member: member}
		var walIdentity [32]byte
		decoded, err := hex.DecodeString(cluster.Members[i].WALIdentity)
		if err != nil || len(decoded) != len(walIdentity) {
			t.Fatalf("invalid fixture WAL identity: %v", err)
		}
		copy(walIdentity[:], decoded)
		core, err := quepaxa.New(quepaxa.Config{
			NodeID: id, Cluster: cluster, WAL: walByID[id], Transport: transport, ValidateValue: types.ValidateManagedKVExecutionPolicy,
			ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{Binding: quepaxa.ManagedAppendOnlyBinding{ApplicationProtocol: protocol, BootstrapPlanDigest: planDigest, MemberConfigDigest: memberDigest, Generation: 0, WALIdentity: walIdentity}, Verify: n.verifyManagedAppendOnlyCandidate},
		})
		if err != nil {
			t.Fatal(err)
		}
		n.core, n.wal = core, walByID[id]
		transport.peers[id] = core
	}
	n := nodes["n1"]
	n.checkpoints, n.archive = manager, archive
	mat, err := n.openMaterializer(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	n.material = mat
	var nodeShutdown atomic.Bool
	t.Cleanup(func() {
		if !nodeShutdown.Load() {
			if err := mat.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	n.opened.Store(true)
	n.managedInitiallyAdmitted.Store(true)
	bindingCtx, bindingCancel := context.WithCancel(context.Background())
	t.Cleanup(bindingCancel)
	member := cluster.Members[0]
	binding := &internalApplicationBinding{identity: n.managedStartup.Identity, policy: sqlpolicy.ManagedRequestMaterializationPolicy, window: types.DefaultIdempotencyWindowSlots, wal: n.core.WALIdentity(), walStore: n.wal, member: member, core: n.core, material: mat, lifetime: bindingCtx, cancel: bindingCancel}
	n.internalApplication.Store(binding)
	profile := network.ManagedPeerProfile{ApplicationProtocol: protocol, BootstrapPlanDigest: n.managedStartup.Identity.BootstrapPlanDigest, Generation: 0}
	server, err := network.NewServerForManagedProfile(n.core, mat, clusterID, true, nil, profile)
	if err != nil {
		t.Fatal(err)
	}
	n.server = server
	t.Cleanup(server.Close)
	if test.budgetScope {
		marker := &struct{}{}
		type markerKey struct{}
		ctx = context.WithValue(ctx, markerKey{}, marker)
		var observations atomic.Int32
		var budgetSeen atomic.Bool
		restoreContext := localtesthooks.SetManagedRuntimeStateContext(func(observed context.Context) {
			if observed.Value(markerKey{}) != marker {
				return
			}
			observations.Add(1)
			if managedControlObservationBudgetFromContext(observed) != nil {
				budgetSeen.Store(true)
			}
		})
		_, err := n.ExecuteManagedKV(ctx, "put", network.KVMutationRequest{})
		restoreContext()
		if err == nil || observations.Load() == 0 || !budgetSeen.Load() {
			t.Fatalf("direct KV public entry did not carry its append-only operation budget: err=%v observations=%d budget=%t", err, observations.Load(), budgetSeen.Load())
		}
		return
	}

	entered, resume := make(chan struct{}), make(chan struct{})
	var stateCalls atomic.Int32
	closeDone := make(chan error, 1)
	restore := localtesthooks.Set(func(event string) {
		if event == "node:managed-runtime-state:after-certified-tip" && stateCalls.Add(1) == 2 {
			close(entered)
			<-resume
		}
		if event == "node:replay-local-decisions-through:applied" {
			switch {
			case test.advance:
				var next [types.ReadBarrierNonceSize]byte
				for i := range next {
					next[i] = byte(151 + i)
				}
				if slot, _, err := n.core.Propose(ctx, types.EncodeReadBarrier(next)); err != nil || slot != 2 {
					t.Errorf("certify actual post-bound control: slot=%d error=%v", slot, err)
				}
			case test.pending:
				transport.down["n2"], transport.down["n3"] = true, true
				ordinary, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: "unquorate-ordinary", Operation: "put", Key: "unquorate-key", Value: []byte("unquorate-value")})
				if err != nil {
					t.Errorf("encode ordinary unquorate request: %v", err)
					return
				}
				if slot, _, err := n.core.Propose(ctx, ordinary); err == nil || slot != 2 {
					t.Errorf("ordinary next-slot proposal unexpectedly certified: slot=%d error=%v", slot, err)
				}
			case test.cancel:
				cancel()
			case test.binding:
				n.stopInternalApplication()
			case test.nodeClose:
				nodeShutdown.Store(true)
				go func() { closeDone <- n.Shutdown() }()
				select {
				case <-binding.lifetime.Done():
				case <-ctx.Done():
					t.Errorf("Node.Close did not cancel the borrowed application binding before releasing the replay hook: %v", ctx.Err())
				}
			}
		}
	})
	t.Cleanup(restore)
	var resumeOnce sync.Once
	releaseResume := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(releaseResume)
	result := make(chan error, 1)
	go func() { result <- n.admitInternalApplication(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("admission did not reach the post-ReadIndex runtime observation", ctx.Err())
	}
	if n.core.Tip() != 0 || n.core.RecorderTip() != 0 || mat.Tip() != 0 {
		t.Fatalf("first quorum/replay phase was not at the empty prefix: certified=%d recorded=%d applied=%d", n.core.Tip(), n.core.RecorderTip(), mat.Tip())
	}
	var value []byte
	if test.application {
		value, err = types.EncodeManagedKVCommand(types.KVCommand{RequestID: "certified-original", Operation: "put", Key: "original-key", Value: []byte("original-value")})
		if err != nil {
			t.Fatal(err)
		}
	} else {
		var nonce [types.ReadBarrierNonceSize]byte
		for i := range nonce {
			nonce[i] = byte(91 + i)
		}
		value = types.EncodeReadBarrier(nonce)
	}
	slot, _, err := n.core.Propose(ctx, value)
	if err != nil || slot != 1 {
		t.Fatalf("certify actual intervening slot-1 value: slot=%d error=%v", slot, err)
	}
	decision, ok := n.core.CertifiedValue(slot)
	if !ok || string(decision.Value) != string(value) || n.core.RecorderTip() != 1 || mat.Tip() != 0 {
		t.Fatalf("interleaving did not establish the real C=R=1/A=0 state: decision=%t recorded=%d certified=%d applied=%d", ok, n.core.RecorderTip(), n.core.Tip(), mat.Tip())
	}
	releaseResume()
	var admissionErr error
	if test.cancel {
		select {
		case admissionErr = <-result:
		case <-time.After(5 * time.Second):
			t.Fatal("canceled admission did not return after the fixed-prefix apply")
		}
	} else {
		select {
		case admissionErr = <-result:
		case <-ctx.Done():
			t.Fatal("admission did not finish after certified-prefix interleaving", ctx.Err())
		}
	}
	if test.advance || test.pending || test.cancel || test.binding || test.nodeClose {
		if admissionErr == nil {
			t.Fatal("admission accepted cancellation, binding drift, or progress beyond the single fixed certified prefix")
		}
		if test.cancel && !errors.Is(admissionErr, context.Canceled) {
			t.Fatalf("caller cancellation was not preserved after the fixed-prefix apply: %v", admissionErr)
		}
	} else if admissionErr != nil {
		t.Fatalf("admission failed to replay the exact fixed certified prefix: %v", admissionErr)
	}
	if test.nodeClose {
		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatalf("Node.Close failed after admission borrower returned: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Node.Close did not drain the fixed-prefix admission borrower")
		}
	}
	wantCertified, wantRecorded := quepaxa.Slot(1), quepaxa.Slot(1)
	if test.advance {
		wantCertified = 2
		wantRecorded = 2
	}
	if test.pending {
		wantRecorded = 2
	}
	progress := n.core.RuntimeProgressSnapshot()
	if progress.CertifiedTip != wantCertified || progress.RecorderTip != wantRecorded || (!test.nodeClose && mat.Tip() != 1) {
		t.Fatalf("fixed replay changed the certified/apply prefix unexpectedly: progress=%+v applied=%d want_certified=%d want_recorded=%d", progress, mat.Tip(), wantCertified, wantRecorded)
	}
	if !test.advance {
		if _, exists := n.core.CertifiedValue(2); exists {
			t.Fatal("admission submitted an application decision beyond the fixed prefix")
		}
	}
	if test.application {
		original, originalOK := n.core.CertifiedValue(1)
		status, statusErr := server.RequestStatus(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: "certified-original"})
		if !originalOK || string(original.Value) != string(value) || original.Hash != decision.Hash || statusErr != nil || status.Receipt == nil || status.Receipt.Slot != 1 || status.Receipt.Status != types.MutationCommitted {
			t.Fatalf("certified original request changed during bounded replay: decision=%+v status=%+v error=%v", original, status, statusErr)
		}
	}
	if !test.advance && !test.pending && !test.cancel && !test.binding && !test.nodeClose {
		response, err := server.KVPut(ctx, network.KVMutationRequest{RequestID: "first-kv-after-certified-prefix", Key: "first-key", Value: []byte("first-value")})
		if err != nil || response.Status != types.MutationCommitted || !response.Applied || response.Slot != 2 {
			t.Fatalf("first KV did not commit after admission: response=%+v error=%v", response, err)
		}
		if readback, err := server.KVGet(ctx, network.KVGetRequest{Key: "first-key", Consistency: "local"}); err != nil || !readback.Found || string(readback.Value) != "first-value" || readback.AppliedSlot != response.Slot {
			t.Fatalf("first KV exact readback: response=%+v error=%v", readback, err)
		}
	}
	if got := n.core.WALIdentity(); got != binding.wal {
		t.Fatalf("first KV changed original WAL binding: got=%q want=%q", got, binding.wal)
	}
}
