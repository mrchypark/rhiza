package node

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"path"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	localobj "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// These legacy reservation interfaces are tripwires, not execution authority.
// Normal 9/10 mutations must use the captured engine binding and lifetime.
type kvWrapperChecker struct {
	managedTestChecker
	calls atomic.Int64
}

func (c *kvWrapperChecker) ValidateManagedKVWrite(context.Context, ManagedStartupIdentity, ManagedRuntimeState, string, network.KVMutationRequest) error {
	c.calls.Add(1)
	return errors.New("retired CR write reservation invoked")
}
func (c *kvWrapperChecker) ValidateManagedKVRecovery(context.Context, ManagedStartupIdentity, string, network.KVMutationRequest) error {
	c.calls.Add(1)
	return errors.New("retired CR recovery reservation invoked")
}
func (c *kvWrapperChecker) ValidateManagedSQLRead(context.Context, ManagedStartupIdentity, ManagedRuntimeState) error {
	c.calls.Add(1)
	return errors.New("retired CR read authority invoked")
}

func newKVWrapperFixture(t *testing.T) (*Node, *kvWrapperChecker, *managedGraphAdmissionRaceFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const protocol = sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	const clusterID = "kv-wrapper"
	ns, err := ManagedNamespaceFor("root", "uid", 0, "assignment", clusterID)
	if err != nil {
		t.Fatal(err)
	}
	storeDir := t.TempDir()
	bucket, err := localobj.NewBucketForNamespace(ctx, localobj.Config{Provider: localobj.ProviderFilesystem, FilesystemDir: storeDir}, ns.FinalPrefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := bucket.Close(); err != nil {
			t.Error(err)
		}
	})
	transport := &graphAdmissionRaceTransport{graphReviewTransport: make(graphReviewTransport), held: make(chan struct{}), release: make(chan struct{})}
	transport.releaseHeld()
	cluster := quepaxa.Cluster{ConfigID: 1}
	for i, id := range []quepaxa.NodeID{"n1", "n2", "n3"} {
		member := peerMember(clusterID, id, string(id)+"-token")
		member.PeerURL = "quic://" + string(id) + ":9090"
		member.WALIdentity = hex.EncodeToString(bytes.Repeat([]byte{byte(i + 1)}, 32))
		cluster.Members = append(cluster.Members, member)
	}
	c := &kvWrapperChecker{}
	var owned *Node
	for _, member := range cluster.Members {
		cfg := &types.ExecutionConfig{NodeID: member.ID, ClusterID: clusterID, Members: cluster.Members, DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: storeDir, ObjStorePrefix: ns.ConfigPrefix, ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck}
		n := New(cfg)
		physical, memberDigest, err := n.PreparedStartupBindingDigestsForProtocol(protocol)
		if err != nil {
			t.Fatal(err)
		}
		common, err := n.ManagedBootstrapPlanDigestForProtocol(physical, protocol, protocol)
		if err != nil {
			t.Fatal(err)
		}
		identity := ManagedStartupIdentity{ApplicationProtocol: protocol, EvidenceFormat: protocol, AssignmentID: "assignment", IntentHash: "intent", StorageID: "storage", PhysicalBinding: physical, BootstrapPlanDigest: common, Member: recovery.PreparedStartupIdentity{ClusterUID: "uid", DeploymentUID: "deployment", PodUID: string(member.ID), NodeID: string(member.ID), SourceBinding: physical, ConfigDigest: memberDigest}}
		n.managedStartup = &ManagedStartupConfig{Identity: identity, Checker: c, expectedPolicy: sqlpolicy.ManagedRequestMaterializationPolicy}
		// Match public startup ordering: claim exact birth intent before any
		// voter registration consumes the shared namespace's empty boundary.
		if fresh, err := n.PrepareManagedSQLSource(ctx, identity, true); err != nil || !fresh {
			t.Fatalf("prepare exact source before voter registration: fresh=%t err=%v", fresh, err)
		}
		wal, err := qlog.Open(filepath.Join(cfg.DataDir, "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := wal.Close(); err != nil {
				t.Error(err)
			}
		})
		nonce, err := hex.DecodeString(member.WALIdentity)
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.BindIdentity(nonce); err != nil {
			t.Fatal(err)
		}
		profile, err := n.managedVoterProfileFor(cfg)
		if err != nil {
			t.Fatal(err)
		}
		pending := voterIdentityForProfile(cfg, profile)
		pending.Nonce = member.WALIdentity
		if err := writeVoterIdentity(cfg.DataDir, pending); err != nil {
			t.Fatal(err)
		}
		local, err := loadVoterIdentity(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := ensureVoterIdentityWithProfile(ctx, cfg, bucket, wal, local, true, false, profile); err != nil {
			t.Fatal(err)
		}
		registered, err := loadVoterIdentity(cfg)
		if err != nil || registered.identity == nil || !registered.identity.Registered || registered.identity.Nonce != member.WALIdentity {
			t.Fatalf("actual voter registration missing: %+v %v", registered.identity, err)
		}
		manager, err := checkpoint.NewAppendOnlyManagerForPolicy(bucket, ns.FinalPrefix, t.TempDir(), sqlpolicy.ManagedRequestMaterializationPolicy, 1)
		if err != nil {
			t.Fatal(err)
		}
		archive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, ns.FinalPrefix, 1, types.ValidateManagedKVExecutionPolicy)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(archive.Close)
		if err := archive.Load(ctx); err != nil {
			t.Fatal(err)
		}
		n.bucket, n.checkpoints, n.archive, n.wal = bucket, manager, archive, wal
		var walIdentity [32]byte
		copy(walIdentity[:], nonce)
		core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: cluster, WAL: wal, Transport: transport, ValidateValue: types.ValidateManagedKVExecutionPolicy, ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{Binding: quepaxa.ManagedAppendOnlyBinding{ApplicationProtocol: protocol, BootstrapPlanDigest: common, MemberConfigDigest: memberDigest, WALIdentity: walIdentity}, Verify: n.verifyManagedAppendOnlyCandidate}})
		if err != nil {
			t.Fatal(err)
		}
		n.core = core
		transport.graphReviewTransport[member.ID] = core
		if member.ID == "n1" {
			owned = n
		}
	}
	n := owned
	if n == nil {
		t.Fatal("owned runtime missing")
	}
	mat, err := n.openMaterializer(filepath.Join(n.config.DataDir, "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mat.Close(); err != nil {
			t.Error(err)
		}
	})
	n.material = mat
	n.opened.Store(true)
	if err := n.installInternalApplicationBinding(ctx); err != nil {
		t.Fatal(err)
	}
	server, err := network.NewServerForManagedProfile(n.core, mat, clusterID, true, nil, network.ManagedPeerProfile{ApplicationProtocol: protocol, BootstrapPlanDigest: n.managedStartup.Identity.BootstrapPlanDigest})
	if err != nil {
		t.Fatal(err)
	}
	n.server = server
	t.Cleanup(server.Close)
	// Isolate wrapper/application assertions from the real publisher's first
	// checkpoint round; cancel it before releasing this maintenance lock.
	n.compactionMu.Lock()
	t.Cleanup(func() {
		if binding := n.stopInternalApplication(); binding != nil {
			binding.active.Wait()
		}
		n.compactionMu.Unlock()
		n.wg.Wait()
	})
	if err := n.AdmitManaged(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if got := c.calls.Load(); got != 0 {
			t.Errorf("normal engine wrapper used %d retired CR reservation callbacks", got)
		}
	})
	return n, c, &managedGraphAdmissionRaceFixture{node: n, transport: transport}
}

func TestManagedKVWrappersExactAuthorityAndObservation(t *testing.T) {
	n, _, _ := newKVWrapperFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := network.KVMutationRequest{RequestID: string([]byte{0xff}), Key: string([]byte{0xfe}), Value: []byte{}, TTLMS: 60000}
	response, err := n.ExecuteManagedKV(ctx, "put", req)
	if err != nil || response.Status != types.MutationCommitted {
		t.Fatal(response, err)
	}
	wantStart, prefix, err := n.managedSQLScope(n.managedStartup.Identity)
	if err != nil {
		t.Fatal(err)
	}
	start, err := readVoterRegistration(ctx, n.bucket, path.Join(prefix, "voters/managed-sql-application-start.json"))
	if err != nil || !bytes.Equal(start, wantStart) {
		t.Fatal("actual application start was not conditionally recorded", err)
	}
	original, found, err := n.RetainedRequest(ctx, "kv", req.RequestID)
	if err != nil || !found || original.Slot != response.Slot {
		t.Fatal("actual retained original missing", found, err)
	}
	duplicate, err := n.ExecuteManagedKV(ctx, "put", req)
	retained, found, retainedErr := n.RetainedRequest(ctx, "kv", req.RequestID)
	if err != nil || duplicate.Slot != response.Slot || !found || retainedErr != nil || retained.Fingerprint != original.Fingerprint || retained.Receipt != original.Receipt || !bytes.Equal(retained.ExecutionItem, original.ExecutionItem) {
		t.Fatal("duplicate changed exact original/result", duplicate, err, retainedErr)
	}
	executions := 0
	for slot := quepaxa.Slot(1); slot <= n.core.Tip(); slot++ {
		decision, ok := n.core.CertifiedValue(slot)
		if !ok {
			continue
		}
		commands, kv, err := types.DecodeManagedKVBatch(decision.Value)
		if err != nil {
			t.Fatal(err)
		}
		if !kv {
			command, single, err := types.DecodeManagedKVCommand(decision.Value)
			if err != nil {
				t.Fatal(err)
			}
			if single {
				commands, kv = []types.KVCommand{command}, true
			}
		}
		if kv {
			for _, command := range commands {
				if command.RequestID == req.RequestID {
					executions++
				}
			}
		}
	}
	if executions != 1 {
		t.Fatalf("original appeared in %d certified application values", executions)
	}
	changed := req
	changed.Value = []byte("different original")
	if _, err := n.ExecuteManagedKV(ctx, "put", changed); err == nil {
		t.Fatal("changed original reused the request identity")
	}
	for _, kind := range []string{"graph", "notify", "unknown"} {
		if _, err := n.RequestStatusManaged(ctx, network.RequestStatusRequest{Kind: kind, RequestID: req.RequestID}); !errors.Is(err, network.ErrInvalidRequest) {
			t.Fatal(kind, err)
		}
	}
	var barriers atomic.Int64
	n.server.SetDurabilityBarrier(func(context.Context, quepaxa.Slot) error { barriers.Add(1); return nil })
	if _, err := n.RequestStatusManaged(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: req.RequestID}); err != nil || barriers.Load() != 0 {
		t.Fatal("status invoked durability", err)
	}
	n.managedAdmitted.Store(false)
	if value, err := n.KVGetManaged(ctx, network.KVGetRequest{Key: req.Key, Consistency: "local"}); err != nil || !value.Found || len(value.Value) != 0 {
		t.Fatal("closed write admission blocked certified local read", value, err)
	}
	if status, err := n.RequestStatusManaged(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: req.RequestID}); err != nil || status.Receipt == nil || status.Receipt.Slot != response.Slot {
		t.Fatal("closed write admission blocked original status", status, err)
	}
	resolved, err := n.ResolveManagedKV(ctx, "put", req)
	if err != nil || resolved.Receipt == nil || resolved.Receipt.Slot != response.Slot || resolved.Durability != network.ResolveKVDurabilityConfirmed || barriers.Load() != 1 {
		t.Fatal("actual retained resolution", resolved, err)
	}
	if _, err := n.API(); !errors.Is(err, network.ErrNotReady) {
		t.Fatal("raw API exposed", err)
	}
	if _, err := n.Handler(); !errors.Is(err, network.ErrNotReady) {
		t.Fatal("raw Handler exposed", err)
	}
	identity := n.managedStartup.Identity
	n.managedStartup.Identity.Member.PodUID = "different-runtime"
	beforeMismatch := n.core.Tip()
	if _, err := n.ExecuteManagedKV(ctx, "put", network.KVMutationRequest{RequestID: "wrong-binding", Key: req.Key}); err == nil || n.core.Tip() != beforeMismatch {
		t.Fatal("changed captured identity admitted mutation", err)
	}
	n.managedStartup.Identity = identity

	binding := n.internalApplication.Load()
	n.internalApplication.Store(nil)
	defer n.internalApplication.Store(binding)
	before := n.core.Tip()
	if _, err := n.ExecuteManagedKV(ctx, "put", network.KVMutationRequest{RequestID: "unbound", Key: req.Key}); err == nil {
		t.Fatal("missing runtime binding admitted a mutation")
	}
	if _, err := n.ResolveManagedKV(ctx, "put", req); !errors.Is(err, network.ErrCommitUnknown) {
		t.Fatal("missing binding fabricated a recovery ACK", err)
	}
	if _, err := n.KVGetManaged(ctx, network.KVGetRequest{Key: req.Key}); err == nil {
		t.Fatal("missing binding admitted a read")
	}
	if _, err := n.RequestStatusManaged(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: req.RequestID}); err == nil {
		t.Fatal("missing binding admitted status")
	}
	if n.core.Tip() != before || barriers.Load() != 1 {
		t.Fatal("binding rejection proposed or reached durability")
	}
}

func TestManagedKVWrappersControlledCertifiedApplyLag(t *testing.T) {
	n, _, fixture := newKVWrapperFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := network.KVMutationRequest{RequestID: "certified-unapplied", Key: "key", Value: []byte("original")}
	value, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: req.RequestID, Operation: "put", Key: req.Key, Value: req.Value})
	if err != nil {
		t.Fatal(err)
	}
	beforeApplied := n.material.Tip()
	slot, _, err := fixture.transport.graphReviewTransport["n2"].Propose(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	if n.material.Tip() != beforeApplied || uint64(slot) <= beforeApplied {
		t.Fatal("fixture lost controlled certified-but-unapplied value")
	}
	result, err := n.ResolveManagedKV(ctx, "put", req)
	if err != nil || result.Receipt == nil || result.Receipt.Slot != uint64(slot) || n.material.Tip() < uint64(slot) {
		t.Fatal("certified recovery lost original", result, err)
	}
	original, found, err := n.RetainedRequest(ctx, "kv", req.RequestID)
	if err != nil || !found || original.Slot != uint64(slot) {
		t.Fatal("recovery resubmitted original at another slot", original, found, err)
	}
}

func TestManagedKVWrappersFalseCASAndUnknown(t *testing.T) {
	n, _, _ := newKVWrapperFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := network.KVMutationRequest{RequestID: "false", Key: "absent", ExpectedExists: true, Expected: []byte{}, Value: nil}
	response, err := n.ExecuteManagedKV(ctx, "cas", req)
	if err != nil || response.Applied || response.Status != types.MutationCommitted {
		t.Fatal(response, err)
	}
	original, found, err := n.RetainedRequest(ctx, "kv", req.RequestID)
	if err != nil || !found || original.Receipt.Applied {
		t.Fatal(original, found, err)
	}
	n.server.SetDurabilityBarrier(func(context.Context, quepaxa.Slot) error { return context.DeadlineExceeded })
	if _, err := n.ResolveManagedKV(ctx, "cas", req); !errors.Is(err, network.ErrCommitUnknown) {
		t.Fatal("unconfirmed false CAS fabricated ACK", err)
	}
	retained, found, err := n.RetainedRequest(ctx, "kv", req.RequestID)
	if err != nil || !found || retained.Slot != original.Slot || retained.Receipt != original.Receipt || !bytes.Equal(retained.ExecutionItem, original.ExecutionItem) {
		t.Fatal("UNKNOWN lost false-CAS original", retained, found, err)
	}
	if _, err := n.ResolveManagedKV(ctx, "cas", network.KVMutationRequest{RequestID: "missing", Key: req.Key}); !errors.Is(err, network.ErrCommitUnknown) {
		t.Fatal("missing original fabricated recovery", err)
	}
	n.server.SetDurabilityBarrier(nil)
	binding := n.internalApplication.Load()
	binding.cancel()
	before := n.core.Tip()
	if _, err := n.ExecuteManagedKV(ctx, "put", network.KVMutationRequest{RequestID: "after-lifetime", Key: req.Key}); err == nil {
		t.Fatal("expired binding lifetime admitted mutation")
	}
	if _, err := n.ResolveManagedKV(ctx, "cas", req); !errors.Is(err, network.ErrCommitUnknown) || n.core.Tip() != before {
		t.Fatal("expired lifetime fabricated recovery or proposed", err)
	}
}
