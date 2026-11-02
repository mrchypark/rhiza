package node

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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

// This owns the actual Node resource phase, not a Kubernetes/Host assignment or
// an object-storage qualification. Public OpenManaged negatives are below.
func mixedResourceNode(t *testing.T, dir string) *Node {
	t.Helper()
	n := New(&types.ExecutionConfig{NodeID: "mixed", ClusterID: "cluster", DataDir: dir, PeerAddr: "127.0.0.1:0"})
	n.managedStartup = &ManagedStartupConfig{Checker: &managedTestChecker{}, expectedPolicy: sqlpolicy.ManagedSQLKVMaterializationPolicy, idempotencyWindow: types.DefaultIdempotencyWindowSlots}
	if err := n.open(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := n.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	return n
}

type mixedPreambleChecker struct {
	managedTestChecker
	calls int
}

func (c *mixedPreambleChecker) ValidateManagedStartup(context.Context, ManagedStartupIdentity) error {
	c.calls++
	return errors.New("synthetic authority refusal")
}

func TestManagedKVNodePreambleBeforeEffects(t *testing.T) {
	for _, fault := range []string{"unknown", "evidence", "generation", "binding", "digest", "authority"} {
		t.Run(fault, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "untouched")
			cfg := &types.ExecutionConfig{NodeID: "member", ClusterID: "cluster", DataDir: dir, PeerToken: "peer", AdminToken: "admin", Members: []types.NodeConfig{peerMember("cluster", "member", "peer"), peerMember("cluster", "other", "other")}, ObjStoreProvider: "s3", ObjStoreBucket: "test", ObjStoreEndpoint: "127.0.0.1:1", ObjStoreInsecure: true, ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck}
			cfg.Members[0].PeerURL, cfg.Members[1].PeerURL = "quic://127.0.0.1:1", "quic://127.0.0.1:2"
			ns, err := ManagedNamespaceFor("", "uid", 0, "assignment", "cluster")
			if err != nil {
				t.Fatal(err)
			}
			cfg.ObjStorePrefix = ns.ConfigPrefix
			n := New(cfg)
			protocol := types.ManagedApplicationProtocolSQLKV
			source, config, err := n.PreparedStartupBindingDigestsForProtocol(protocol)
			if err != nil {
				t.Fatal(err)
			}
			common, err := n.ManagedBootstrapPlanDigestForProtocol(source, protocol, protocol)
			if err != nil {
				t.Fatal(err)
			}
			i := ManagedStartupIdentity{ApplicationProtocol: protocol, EvidenceFormat: protocol, Member: recovery.PreparedStartupIdentity{ClusterUID: "uid", DeploymentUID: "deployment", PodUID: "pod", NodeID: "member", SourceBinding: source, ConfigDigest: config}, AssignmentID: "assignment", IntentHash: "intent", StorageID: "storage", PhysicalBinding: source, BootstrapPlanDigest: common}
			if _, _, err := n.managedSQLScope(i); err != nil {
				t.Fatal("mixed source birth binding", err)
			}
			switch fault {
			case "unknown":
				i.ApplicationProtocol = "unknown"
			case "evidence":
				i.EvidenceFormat = "legacy"
			case "generation":
				i.Member.Generation = 1
			case "binding":
				i.Member.SourceBinding = "wrong"
			case "digest":
				i.BootstrapPlanDigest = "wrong"
			}
			checker := &mixedPreambleChecker{}
			if err := n.OpenManaged(context.Background(), ManagedStartupConfig{Identity: i, Checker: checker}); err == nil {
				t.Fatal("invalid identity admitted")
			}
			wantCalls := 0
			if fault == "authority" {
				wantCalls = 1
			}
			if checker.calls != wantCalls {
				t.Fatalf("wrong rejection boundary: calls=%d", checker.calls)
			}
			if _, err := os.Lstat(dir); !os.IsNotExist(err) || n.opened.Load() || n.wal != nil || n.material != nil || n.peer != nil {
				t.Fatal("preamble rejection mutated runtime", err)
			}
		})
	}
}

func TestManagedKVNodeCertifiedInspection(t *testing.T) {
	// A real local Core/WAL/materializer inspection fixture. Its explicit test
	// WAL identity is not a remote registration or application authority proof.
	ctx := context.Background()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	if err := wal.BindIdentity([]byte("01234567890123456789012345678901")); err != nil {
		t.Fatal(err)
	}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "member", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "member"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	m, err := materializer.OpenManagedSQLKV(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	n := New(&types.ExecutionConfig{NodeID: "member", ClusterID: "cluster"})
	n.managedStartup = &ManagedStartupConfig{expectedPolicy: 8, idempotencyWindow: types.DefaultIdempotencyWindowSlots}
	n.core, n.material, n.wal = core, m, wal
	n.opened.Store(true)
	initial, err := n.InspectManagedKV(ctx, true)
	if err != nil || !initial.Application.FreshEmpty {
		t.Fatal("fresh actual inspection", err)
	}
	command, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: string([]byte{0xff}), Operation: "put", Key: string([]byte{0xfe}), Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := core.Propose(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	decision, ok := core.CertifiedValue(slot)
	if !ok {
		t.Fatal("missing real decision")
	}
	if err := m.ApplyBatch(ctx, []quepaxa.DecidedValue{decision}); err != nil {
		t.Fatal(err)
	}
	after, err := n.InspectManagedKV(ctx, false)
	want, stateErr := m.InspectManagedApplicationState(ctx)
	if err != nil || stateErr != nil || after.Application != want || after.Runtime.AppliedTip != uint64(slot) {
		t.Fatal("certified mixed inspection", err, stateErr)
	}
	if _, err := n.InspectManagedKV(ctx, true); err == nil {
		t.Fatal("KV history considered fresh")
	}
}

func TestManagedKVNodeResourceProfileAndRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	n := mixedResourceNode(t, dir)
	if n.material.MaterializationPolicy() != sqlpolicy.ManagedSQLKVMaterializationPolicy || n.peerControl == nil || n.peer == nil {
		t.Fatal("mixed Node resources not connected")
	}
	if _, err := n.API(); !errors.Is(err, network.ErrNotReady) {
		t.Fatal("raw API exposed", err)
	}
	if _, err := n.Handler(); !errors.Is(err, network.ErrNotReady) {
		t.Fatal("raw HTTP exposed", err)
	}
	if err := n.AdmitManaged(ctx); err != nil {
		t.Fatal(err)
	}
	id, key := string([]byte{0xff}), string([]byte{0xfe})
	command, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: id, Operation: "put", Key: key, Value: []byte{}, ObservedAtUnixMS: 1})
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := n.core.Propose(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		t.Fatal(err)
	}
	value, found, err := n.material.KVGet(ctx, key, time.UnixMilli(2))
	if err != nil || !found || len(value) != 0 {
		t.Fatalf("empty raw-key value: %v %t %v", value, found, err)
	}
	falseCAS, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: "false", Operation: "cas", Key: "missing", ExpectedExists: true, Expected: []byte{}, Value: nil})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := n.core.Propose(ctx, falseCAS); err != nil {
		t.Fatal(err)
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		t.Fatal(err)
	}
	sql, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "sql", SQL: "CREATE TABLE application(v INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := n.core.Propose(ctx, sql); err != nil {
		t.Fatal(err)
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := n.material.InspectManagedApplicationState(ctx)
	if err != nil || before.FreshEmpty {
		t.Fatal("mixed evidence", err)
	}
	// Resource ownership is not voter registration. This fixture intentionally
	// lacks a registered identity and must not produce managed authority evidence.
	if _, err := n.InspectManagedKV(ctx, false); err == nil {
		t.Fatal("unregistered WAL became managed evidence")
	}
	if _, err := n.InspectManagedSQL(ctx, false); err == nil {
		t.Fatal("mixed evidence relabeled SQL")
	}
	if _, err := n.InspectManagedKV(ctx, true); err == nil {
		t.Fatal("application history accepted as fresh")
	}
	receipt, found, err := n.material.MutationReceipt(ctx, types.MutationKV, id)
	if err != nil || !found || receipt.Slot != uint64(slot) {
		t.Fatal("raw receipt missing", err)
	}
	falseReceipt, found, err := n.material.MutationReceipt(ctx, types.MutationKV, "false")
	if err != nil || !found || falseReceipt.Applied {
		t.Fatal("false CAS receipt", err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	reopened := mixedResourceNode(t, dir)
	after, err := reopened.material.InspectManagedApplicationState(ctx)
	if err != nil || after != before {
		t.Fatal("same-Pod evidence changed", err)
	}
	again, found, err := reopened.material.MutationReceipt(ctx, types.MutationKV, id)
	if err != nil || !found || !reflect.DeepEqual(again, receipt) {
		t.Fatal("original receipt changed", err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "latticedb")); !os.IsNotExist(err) {
		t.Fatal("mixed created Graph", err)
	}
}

func TestManagedKVNodeActualCheckpointAndReconstruction(t *testing.T) {
	ctx := context.Background()
	n := mixedResourceNode(t, t.TempDir())
	command, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: "original", Operation: "put", Key: string([]byte{0xff}), Value: []byte("kept")})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := n.core.Propose(ctx, command); err != nil {
		t.Fatal(err)
	}
	if err := n.replayLocalDecisions(ctx); err != nil {
		t.Fatal(err)
	}
	files, tip, cleanup, err := n.material.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	// Inspect the standalone downloaded layout, not a live materializer's
	// deliberately named .rhiza-checkpoint-* source artifact.
	data, err := os.ReadFile(files[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "sqlite.db")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	files[0].Path = path
	actual := []checkpoint.File{{Role: checkpoint.RoleSQLite, Path: files[0].Path}}
	if err := n.validateCheckpointFiles(ctx, actual, tip); err != nil {
		t.Fatal(err)
	}
	if err := n.validateCheckpointFiles(ctx, actual, tip+1); err == nil {
		t.Fatal("wrong checkpoint tip accepted")
	}
	if err := n.validateCheckpointFiles(ctx, append(actual, checkpoint.File{Role: "graph"}), tip); err == nil {
		t.Fatal("extra checkpoint role accepted")
	}
	digest, actualTip, err := n.inspectApplicationFile(ctx, files[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.compareManagedFileAtTip(ctx, digest, actualTip, files, nil, filepath.Join(t.TempDir(), "expected.db")); err != nil {
		t.Fatal(err)
	}
	for _, window := range []uint64{1024, 131072} {
		path := filepath.Join(t.TempDir(), "sqlite.db")
		m, err := materializer.OpenManagedSQLKV(path, 1, window)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := n.inspectApplicationFile(ctx, path); err == nil {
			t.Fatal("wrong persisted window accepted")
		}
	}
	legacyPath := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := materializer.Open(legacyPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	if err := n.validateCheckpointFiles(ctx, []checkpoint.File{{Role: checkpoint.RoleSQLite, Path: legacyPath}}, 0); err == nil {
		t.Fatal("legacy file accepted")
	}
	if !bytes.Equal(command[:5], []byte("QKVC\x01")) {
		t.Fatal("not actual mixed wire")
	}
}
