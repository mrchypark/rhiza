package node

import (
	"bytes"
	"context"
	"errors"
	"path"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

type kvWrapperChecker struct {
	identity      ManagedStartupIdentity
	operation     string
	request       network.KVMutationRequest
	deny          atomic.Bool
	writeCalls    atomic.Int64
	recoveryCalls atomic.Int64
	readCalls     atomic.Int64
}

func (c *kvWrapperChecker) ValidateManagedStartup(_ context.Context, i ManagedStartupIdentity) error {
	if i != c.identity || c.deny.Load() {
		return errors.New("synthetic exact authority refused")
	}
	return nil
}
func (c *kvWrapperChecker) ValidateManagedAdmission(ctx context.Context, i ManagedStartupIdentity, state ManagedRuntimeState) error {
	if state.WALIdentity == "" || state.AppliedTip != state.CertifiedTip || state.RecorderTip > state.CertifiedTip {
		return errors.New("synthetic runtime unresolved")
	}
	return c.ValidateManagedStartup(ctx, i)
}
func (c *kvWrapperChecker) ValidateManagedSQLRead(ctx context.Context, i ManagedStartupIdentity, state ManagedRuntimeState) error {
	c.readCalls.Add(1)
	return c.ValidateManagedStartup(ctx, i)
}
func (c *kvWrapperChecker) exact(ctx context.Context, i ManagedStartupIdentity, operation string, req network.KVMutationRequest) error {
	if err := c.ValidateManagedStartup(ctx, i); err != nil {
		return err
	}
	want := types.KVCommand{RequestID: c.request.RequestID, Operation: c.operation, Key: c.request.Key, Value: c.request.Value, Expected: c.request.Expected, ExpectedExists: c.request.ExpectedExists, TTLMS: c.request.TTLMS}
	got := types.KVCommand{RequestID: req.RequestID, Operation: operation, Key: req.Key, Value: req.Value, Expected: req.Expected, ExpectedExists: req.ExpectedExists, TTLMS: req.TTLMS}
	wf, we := types.ManagedKVFingerprint(want)
	gf, ge := types.ManagedKVFingerprint(got)
	if we != nil || ge != nil || want.RequestID != got.RequestID || wf != gf {
		return errors.New("synthetic exact original refused")
	}
	return nil
}
func (c *kvWrapperChecker) ValidateManagedKVWrite(ctx context.Context, i ManagedStartupIdentity, _ ManagedRuntimeState, operation string, req network.KVMutationRequest) error {
	c.writeCalls.Add(1)
	return c.exact(ctx, i, operation, req)
}
func (c *kvWrapperChecker) ValidateManagedKVRecovery(ctx context.Context, i ManagedStartupIdentity, operation string, req network.KVMutationRequest) error {
	c.recoveryCalls.Add(1)
	return c.exact(ctx, i, operation, req)
}

// Actual local Core/WAL/SQLite/filesystem source proof. This is neither public
// OpenManaged nor a remote registry/provider qualification.
func newKVWrapperFixture(t *testing.T) (*Node, *kvWrapperChecker) {
	t.Helper()
	ctx := context.Background()
	ns, err := ManagedNamespaceFor("root", "uid", 0, "assignment", "cluster")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &types.ExecutionConfig{ClusterID: "cluster", NodeID: "member", DataDir: t.TempDir(), ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), ObjStorePrefix: ns.ConfigPrefix}
	n := New(cfg)
	physical, member, err := n.PreparedStartupBindingDigestsForProtocol(types.ManagedApplicationProtocolSQLKV)
	if err != nil {
		t.Fatal(err)
	}
	common, err := n.ManagedBootstrapPlanDigestForProtocol(physical, types.ManagedApplicationProtocolSQLKV, types.ManagedApplicationProtocolSQLKV)
	if err != nil {
		t.Fatal(err)
	}
	i := ManagedStartupIdentity{ApplicationProtocol: types.ManagedApplicationProtocolSQLKV, EvidenceFormat: types.ManagedApplicationProtocolSQLKV, Member: recovery.PreparedStartupIdentity{ClusterUID: "uid", DeploymentUID: "deployment", PodUID: "pod", NodeID: "member", SourceBinding: physical, ConfigDigest: member}, AssignmentID: "assignment", IntentHash: "intent", StorageID: "storage", PhysicalBinding: physical, BootstrapPlanDigest: common}
	checker := &kvWrapperChecker{identity: i}
	if _, err := n.PrepareManagedSQLSource(ctx, i, true); err != nil {
		t.Fatal(err)
	}
	bucket, err := objectstore.NewBucketForNamespace(ctx, objectstore.Config{Provider: objectstore.ProviderFilesystem, FilesystemDir: cfg.ObjStoreDir}, ns.FinalPrefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	wal, err := qlog.Open(filepath.Join(cfg.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	if err := wal.BindIdentity([]byte("01234567890123456789012345678901")); err != nil {
		t.Fatal(err)
	}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "member", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "member"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	m, err := materializer.OpenManagedSQLKV(filepath.Join(cfg.DataDir, "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	n.managedStartup = &ManagedStartupConfig{Identity: i, Checker: checker, expectedPolicy: 8, idempotencyWindow: types.DefaultIdempotencyWindowSlots}
	n.core, n.material, n.bucket, n.wal = core, m, bucket, wal
	n.opened.Store(true)
	n.ready.Store(true)
	n.server, err = network.NewServerForPolicy(core, m, "cluster", true, nil, 8, n.Ready)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.server.Close)
	if err := n.AdmitManaged(ctx); err != nil {
		t.Fatal(err)
	}
	return n, checker
}

func TestManagedKVWrappersExactAuthorityAndObservation(t *testing.T) {
	n, c := newKVWrapperFixture(t)
	ctx := context.Background()
	req := network.KVMutationRequest{RequestID: string([]byte{0xff}), Key: string([]byte{0xfe}), Value: []byte{}, TTLMS: 60000}
	c.operation, c.request = "put", req
	response, err := n.ExecuteManagedKV(ctx, "put", req)
	if err != nil || response.Status != types.MutationCommitted {
		t.Fatal(response, err)
	}
	tip := n.core.Tip()
	first, ok := n.core.CertifiedValue(quepaxa.Slot(response.Slot))
	if !ok {
		t.Fatal("missing actual first execution")
	}
	duplicate, err := n.ExecuteManagedKV(ctx, "put", req)
	second, secondOK := n.core.CertifiedValue(quepaxa.Slot(response.Slot))
	if err != nil || duplicate.Slot != response.Slot || n.core.Tip() != tip || !secondOK || !bytes.Equal(first.Value, second.Value) {
		t.Fatal("duplicate reexecuted or refreshed TTL", duplicate, err)
	}
	for _, kind := range []string{"graph", "notify", "unknown"} {
		if _, err := n.RequestStatusManaged(ctx, network.RequestStatusRequest{Kind: kind, RequestID: req.RequestID}); !errors.Is(err, network.ErrInvalidRequest) {
			t.Fatal(kind, err)
		}
	}
	status, err := n.RequestStatusManaged(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: req.RequestID})
	if err != nil || status.Receipt == nil || status.Receipt.Slot != response.Slot {
		t.Fatal(status, err)
	}
	value, err := n.KVGetManaged(ctx, network.KVGetRequest{Key: req.Key, Consistency: "local"})
	if err != nil || !value.Found || len(value.Value) != 0 {
		t.Fatal(value, err)
	}
	resolved, err := n.ResolveManagedKV(ctx, "put", req)
	if err != nil || resolved.Receipt == nil || resolved.Receipt.Slot != response.Slot || n.core.Tip() != tip {
		t.Fatal(resolved, err)
	}
	changed := req
	changed.Expected = []byte("ignored but bound")
	if _, err := n.ExecuteManagedKV(ctx, "put", changed); err == nil || n.core.Tip() != tip {
		t.Fatal("changed original bypassed exact checker", err)
	}
	var barriers atomic.Int64
	n.server.SetDurabilityBarrier(func(context.Context, quepaxa.Slot) error { barriers.Add(1); return nil })
	if _, err := n.RequestStatusManaged(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: req.RequestID}); err != nil || barriers.Load() != 0 {
		t.Fatal("status invoked durability", err)
	}
	// Previously admitted local reads do not inherit closed write admission.
	n.managedAdmitted.Store(false)
	if _, err := n.KVGetManaged(ctx, network.KVGetRequest{Key: req.Key, Consistency: "local"}); err != nil {
		t.Fatal("write gate blocked local read", err)
	}
	if _, err := n.RequestStatusManaged(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: req.RequestID}); err != nil {
		t.Fatal("write gate blocked status", err)
	}
	n.managedAdmitted.Store(true)
	c.deny.Store(true)
	if _, err := n.ResolveManagedKV(ctx, "put", req); err == nil || barriers.Load() != 0 {
		t.Fatal("denied recovery reached barrier", err)
	}
	if _, err := n.ExecuteManagedKV(ctx, "put", req); err == nil || n.core.Tip() != tip {
		t.Fatal("denied write submitted", err)
	}
	if _, err := n.KVGetManaged(ctx, network.KVGetRequest{Key: req.Key}); err == nil {
		t.Fatal("stale authority read")
	}
	if _, err := n.RequestStatusManaged(ctx, network.RequestStatusRequest{Kind: "kv", RequestID: req.RequestID}); err == nil {
		t.Fatal("stale authority status")
	}
	c.deny.Store(false)
	resolved, err = n.ResolveManagedKV(ctx, "put", req)
	if err != nil || resolved.Durability != network.ResolveKVDurabilityConfirmed || barriers.Load() != 1 {
		t.Fatal(resolved, err)
	}
	n.server.SetDurabilityBarrier(func(context.Context, quepaxa.Slot) error { c.deny.Store(true); return nil })
	resolved, err = n.ResolveManagedKV(ctx, "put", req)
	if !errors.Is(err, network.ErrCommitUnknown) || resolved.Receipt == nil || resolved.Durability != network.ResolveKVDurabilityUnknown {
		t.Fatal("barrier authority change ACKed", resolved, err)
	}
	c.deny.Store(false)
	if _, err := n.API(); !errors.Is(err, network.ErrNotReady) {
		t.Fatal("raw API exposed", err)
	}
	if _, err := n.Handler(); !errors.Is(err, network.ErrNotReady) {
		t.Fatal("raw Handler exposed", err)
	}
	if _, _, err := n.inspectApplicationFile(ctx, path.Join(n.config.DataDir, "sqlite.db")); err != nil {
		t.Fatal(err)
	}
	if c.writeCalls.Load() == 0 || c.recoveryCalls.Load() == 0 || c.readCalls.Load() == 0 {
		t.Fatal("exact permission paths untested")
	}
}

func TestManagedKVWrappersControlledCertifiedApplyLag(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n, _, peers, _ := terminalRetryFixture(t, nil)
	m, err := materializer.OpenManagedSQLKV(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	req := network.KVMutationRequest{RequestID: string([]byte{0xff}), Key: string([]byte{0xfe}), Value: []byte("original")}
	c := &kvWrapperChecker{operation: "put", request: req}
	n.material = m
	n.managedStartup = &ManagedStartupConfig{Checker: c, expectedPolicy: 8, idempotencyWindow: types.DefaultIdempotencyWindowSlots}
	n.opened.Store(true)
	n.server, err = network.NewServerForPolicy(n.core, m, "cluster", false, nil, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.server.Close)
	value, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: req.RequestID, Operation: "put", Key: req.Key, Value: req.Value})
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := peers.cores["b"].Propose(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	if m.Tip() != 0 {
		t.Fatal("fixture lost controlled unapplied state")
	}
	before := peers.cores["b"].Tip()
	result, err := n.ResolveManagedKV(ctx, "put", req)
	if err != nil || result.Receipt == nil || result.Receipt.Slot != uint64(slot) || m.Tip() != uint64(slot) || peers.cores["b"].Tip() != before {
		t.Fatal("certified recovery submitted or lost original", result, err)
	}
}

func TestManagedKVWrappersFalseCASAndUnknown(t *testing.T) {
	n, c := newKVWrapperFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req := network.KVMutationRequest{RequestID: "false", Key: "absent", ExpectedExists: true, Expected: []byte{}, Value: nil}
	c.operation, c.request = "cas", req
	response, err := n.ExecuteManagedKV(ctx, "cas", req)
	if err != nil || response.Applied || response.Status != types.MutationCommitted {
		t.Fatal(response, err)
	}
	tip := n.core.Tip()
	n.server.SetDurabilityBarrier(func(context.Context, quepaxa.Slot) error { return context.DeadlineExceeded })
	result, err := n.ResolveManagedKV(ctx, "cas", req)
	var unknown *network.CommitUnknownError
	if !errors.As(err, &unknown) || result.Receipt == nil || result.Receipt.Applied || unknown.Slot != quepaxa.Slot(response.Slot) || unknown.RequestID != req.RequestID {
		t.Fatal(result, err)
	}
	c.request.RequestID = "missing"
	missing, err := n.ResolveManagedKV(ctx, "cas", c.request)
	if err != nil || missing.State != network.ResolveKVUnknownOrExpired || missing.Receipt != nil || n.core.Tip() != tip {
		t.Fatal(missing, err)
	}
	c.operation, c.request = "put", network.KVMutationRequest{RequestID: "old-large-pending", Key: "k", Value: bytes.Repeat([]byte{1}, 100000)}
	if _, err := types.ManagedKVExecutionSizeBound(types.KVCommand{RequestID: c.request.RequestID, Operation: "put", Key: "k", Value: c.request.Value}); err == nil {
		t.Fatal("fixture must exceed new-reservation bound")
	}
	missing, err = n.ResolveManagedKV(ctx, "put", c.request)
	if err != nil || missing.State != network.ResolveKVUnknownOrExpired || n.core.Tip() != tip {
		t.Fatal("retroactive reservation cap or resubmit", missing, err)
	}
}
