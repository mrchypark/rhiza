package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

type managedTestChecker struct {
	denyStartup    bool
	denyAdmission  atomic.Bool
	admissionCalls atomic.Int64
}

func (c *managedTestChecker) ValidateManagedStartup(context.Context, ManagedStartupIdentity) error {
	if c.denyStartup {
		return errors.New("assignment denied")
	}
	return nil
}

func (c *managedTestChecker) ValidateManagedAdmission(context.Context, ManagedStartupIdentity, ManagedRuntimeState) error {
	c.admissionCalls.Add(1)
	if c.denyAdmission.Load() {
		return errors.New("admission denied")
	}
	return nil
}

func TestManagedPausedRuntimeOwnsResourcesAndDoesNotConfuseCatchUpWithAdmission(t *testing.T) {
	// Exercise the shared resource-owning phase without an external storage
	// emulator. The public OpenManaged assignment/store checks are tested below.
	c := &managedTestChecker{}
	c.denyAdmission.Store(true)
	n := New(&types.ExecutionConfig{NodeID: "managed", ClusterID: "cluster", DataDir: t.TempDir(), PeerAddr: "127.0.0.1:0"})
	n.managedStartup = &ManagedStartupConfig{Checker: c}
	if err := n.open(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := n.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	if n.wal == nil || n.lock == nil || n.core == nil || n.material == nil || n.peer == nil {
		t.Fatal("paused runtime did not retain real owned resources")
	}
	n.observeCatchUp(nil)
	if !n.ready.Load() || n.Ready() {
		t.Fatal("control readiness opened application admission")
	}
	if n.peerControl == nil || !n.peerControl.Ready() || n.server.Ready() {
		t.Fatal("private control readiness was coupled to application admission")
	}
	if _, err := n.API(); !errors.Is(err, network.ErrNotReady) {
		t.Fatalf("paused application API exposed: %v", err)
	}
	if _, err := n.Handler(); !errors.Is(err, network.ErrNotReady) {
		t.Fatalf("paused HTTP API exposed: %v", err)
	}
	api := n.server
	if _, err := api.Execute(context.Background(), network.ExecuteRequest{RequestID: "paused", SQL: "CREATE TABLE forbidden(value)"}); !errors.Is(err, network.ErrNotReady) {
		t.Fatalf("paused Execute: %v", err)
	}
	if err := n.AdmitManaged(context.Background()); err == nil {
		t.Fatal("denied authority admitted runtime")
	}
	if !n.opened.Load() || n.material == nil {
		t.Fatal("admission failure destroyed paused runtime")
	}
	c.denyAdmission.Store(false)
	if err := n.AdmitManaged(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !n.Ready() {
		t.Fatal("live admitted runtime is not ready")
	}
	if _, err := n.API(); !errors.Is(err, network.ErrNotReady) {
		t.Fatal("admitted managed raw API exposed")
	}
	if _, err := n.Handler(); !errors.Is(err, network.ErrNotReady) {
		t.Fatal("admitted managed raw handler exposed")
	}
	if _, err := n.ExecuteManagedSQL(context.Background(), network.ExecuteRequest{RequestID: "unreserved", SQL: "CREATE TABLE forbidden(value)"}); err == nil {
		t.Fatal("managed SQL executed without exact reservation checker")
	}
	c.denyAdmission.Store(true)
	if n.Ready() {
		t.Fatal("cached admission bypassed current authority")
	}
	if _, err := api.Execute(context.Background(), network.ExecuteRequest{RequestID: "revoked", SQL: "CREATE TABLE forbidden(value)"}); !errors.Is(err, network.ErrNotReady) {
		t.Fatalf("revoked Execute: %v", err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if n.wal != nil || n.lock != nil || n.material != nil || n.peer != nil || n.Ready() {
		t.Fatal("managed resource cleanup incomplete")
	}
}

func TestManagedAssignmentDenialPrecedesLocalMutation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "must-remain-absent")
	n := New(&types.ExecutionConfig{NodeID: "managed", ClusterID: "cluster", DataDir: dir, PeerToken: "managed-token", Members: []types.NodeConfig{peerMember("cluster", "managed", "managed-token"), peerMember("cluster", "other", "other-token")}, ObjStoreProvider: "s3", ObjStoreBucket: "test", ObjStoreEndpoint: "127.0.0.1:1", ObjStoreInsecure: true, ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck})
	n.config.Members[0].PeerURL = "quic://127.0.0.1:1"
	n.config.Members[1].PeerURL = "quic://127.0.0.1:2"
	namespace, err := ManagedNamespaceFor("", "uid", 0, "assignment", "cluster")
	if err != nil {
		t.Fatal(err)
	}
	n.config.ObjStorePrefix = namespace.ConfigPrefix
	source, config, err := n.preparedBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := n.ManagedBootstrapPlanDigest(source, "sql-v1")
	if err != nil {
		t.Fatal(err)
	}
	i := ManagedStartupIdentity{Member: recovery.PreparedStartupIdentity{ClusterUID: "uid", DeploymentUID: "deployment", PodUID: "pod", NodeID: "managed", SourceBinding: source, ConfigDigest: config}, AssignmentID: "assignment", IntentHash: "intent", StorageID: "storage", PhysicalBinding: source, BootstrapPlanDigest: bootstrap, EvidenceFormat: "sql-v1", Fresh: true}
	if err := n.OpenManaged(context.Background(), ManagedStartupConfig{Identity: i, Checker: &managedTestChecker{denyStartup: true}}); err == nil {
		t.Fatal("denied assignment opened")
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("denied assignment touched data directory: %v", err)
	}
	if n.opened.Load() || n.peer != nil || n.wal != nil {
		t.Fatal("denied assignment retained runtime")
	}
}

func TestManagedBootstrapPlanIsCommonButBindsActualStorageAndMembership(t *testing.T) {
	a := New(&types.ExecutionConfig{NodeID: "a", ClusterID: "cluster", ObjStoreProvider: "s3", ObjStoreBucket: "bucket", Members: []types.NodeConfig{peerMember("cluster", "a", "a-token"), peerMember("cluster", "b", "b-token")}})
	a.config.Members[0].PeerURL = "quic://127.0.0.1:1"
	a.config.Members[1].PeerURL = "quic://127.0.0.1:2"
	source, _, err := a.PreparedStartupBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	want, err := a.ManagedBootstrapPlanDigest(source, "sql-v1")
	if err != nil {
		t.Fatal(err)
	}
	other := *a.config
	other.NodeID = "b"
	learner := peerMember("cluster", "later", "later-token")
	other.Learner = &learner
	other.Members = []types.NodeConfig{a.config.Members[1], a.config.Members[0]}
	other.Members[0].WALIdentity = "member-local-nonce"
	b := New(&other)
	got, err := b.ManagedBootstrapPlanDigest(source, "sql-v1")
	if err != nil || got != want {
		t.Fatalf("member-local fields changed common plan: %s %v", got, err)
	}
	if _, err := b.ManagedBootstrapPlanDigest("untrusted-physical", "sql-v1"); err == nil {
		t.Fatal("untrusted physical binding accepted")
	}
	got, err = b.ManagedBootstrapPlanDigest(source, "sql-v2")
	if err != nil || got == want {
		t.Fatal("evidence format not bound")
	}
	other.Members[0].PeerURL = "different:1234"
	got, err = b.ManagedBootstrapPlanDigest(source, "sql-v1")
	if err != nil || got == want {
		t.Fatal("actual bootstrap member address not bound")
	}
}
