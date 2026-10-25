package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/quic-go/quic-go"
)

type terminalResumeAuthority struct {
	record fencing.EngineRepairRecord
	proof  fencing.StorageEffectsExcluded
	cas    int
}

func (a *terminalResumeAuthority) CurrentEngineRepair(context.Context) (fencing.EngineRepairRecord, bool, error) {
	return a.record, true, nil
}

func (a *terminalResumeAuthority) ListPendingEngineRepairs(context.Context) ([]fencing.PendingEngineRepairRequest, error) {
	return nil, nil
}

func (a *terminalResumeAuthority) AdoptPendingEngineRepair(context.Context, string, string, fencing.EngineRepairRecord) (fencing.EngineRepairRecord, error) {
	return fencing.EngineRepairRecord{}, fmt.Errorf("unexpected pending repair adoption")
}

func (a *terminalResumeAuthority) CompareAndSwapEngineRepair(_ context.Context, expected string, next fencing.EngineRepairRecord) (fencing.EngineRepairRecord, error) {
	if a.record.Digest() != expected || next.Phase != fencing.RepairRemoved || next.RemoveTerminalDigest == "" {
		return fencing.EngineRepairRecord{}, fmt.Errorf("unexpected engine repair resume CAS")
	}
	if err := next.Validate(); err != nil {
		return fencing.EngineRepairRecord{}, fmt.Errorf("invalid engine repair resume candidate: %w", err)
	}
	a.record = next
	a.cas++
	return a.record, nil
}

func (a *terminalResumeAuthority) CloseEngineRepairStorageEffects(context.Context, string) (fencing.StorageEffectsExcluded, error) {
	return a.proof, nil
}

func (a *terminalResumeAuthority) ConfirmEngineRepair(context.Context, string) (fencing.EngineRepairRecord, error) {
	return fencing.EngineRepairRecord{}, fmt.Errorf("unexpected engine repair confirmation")
}

func TestEngineRepairResumeUsesDurableTerminalAfterStoragePhaseCASLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	clusterID := types.ClusterID("engine-repair-terminal-resume")
	ids := []quepaxa.NodeID{"a", "b", "c"}
	tokens := map[quepaxa.NodeID]string{"a": "peer-a", "b": "peer-b", "c": "peer-c"}
	config := quepaxa.Cluster{ConfigID: 7}
	packets := make([]net.PacketConn, 0, len(ids))
	listeners := make([]*quic.Transport, 0, len(ids))
	transports := make(map[quepaxa.NodeID]*network.Transport, len(ids))
	cores := make(map[quepaxa.NodeID]*quepaxa.Core, len(ids))
	for _, id := range ids {
		packet, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		packets = append(packets, packet)
		t.Cleanup(func() { _ = packet.Close() })
		listeners = append(listeners, &quic.Transport{Conn: packet})
		member := peerMember(string(clusterID), id, tokens[id])
		member.URL = fmt.Sprintf("http://%s:8080", id)
		member.LogURL = fmt.Sprintf("http://%s:8081", id)
		member.PeerURL = "quic://" + packet.LocalAddr().String()
		member.WALIdentity = stringsOf('a'+byte(len(config.Members)), 64)
		config.Members = append(config.Members, member)
	}
	for _, listener := range listeners {
		listener := listener
		t.Cleanup(func() { _ = listener.Close() })
	}
	servers := make(map[quepaxa.NodeID]*network.Server, len(ids))
	for _, member := range config.Members {
		transport := network.NewTransport(clusterID, member.ID, &config, tokens[member.ID])
		transports[member.ID] = transport
		t.Cleanup(func() { _ = transport.Close() })
		wal, err := qlog.Open(filepath.Join(t.TempDir(), string(member.ID)+".qlog"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		identity, err := hex.DecodeString(member.WALIdentity)
		if err != nil {
			t.Fatal(err)
		}
		if err := wal.BindIdentity(identity); err != nil {
			t.Fatalf("bind %s WAL: %v", member.ID, err)
		}
		core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: config, WAL: wal, Transport: transport,
			EnableReconfiguration: true, EngineOwnedRemovals: true,
			ReconfigurationAdmission: func(context.Context, quepaxa.ReconfigurationAdmission) error { return nil }})
		if err != nil {
			t.Fatal(err)
		}
		transport.BindCore(core)
		cores[member.ID] = core
		material, err := materializer.Open(filepath.Join(t.TempDir(), string(member.ID)+".sqlite"), 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = material.Close() })
		server := network.NewServer(core, material, clusterID, true, transport)
		servers[member.ID] = server
		t.Cleanup(server.Close)
	}
	for i, member := range config.Members {
		peer, err := network.StartPeerServerOnTransport(ctx, listeners[i], servers[member.ID], config.Members, tokens[member.ID], "admin-token")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = peer.Close() })
	}

	target := quepaxa.Cluster{ConfigID: config.ConfigID + 1, Members: append([]quepaxa.Member(nil), config.Members[:2]...)}
	operation, err := cores["a"].BeginEngineOwnedRemovalAt(ctx, target, "repair-terminal-cas-lost", 0)
	if err != nil {
		t.Fatalf("begin exact engine-owned REMOVE: %v", err)
	}
	freezeOperation := operation
	repair := terminalResumeRepairRecord(config, freezeOperation)
	if err := repair.Validate(); err != nil {
		t.Fatalf("construct valid storage-closing record before lost phase CAS: %v", err)
	}
	proof := terminalResumeStorageProof(repair)
	repair.StorageProofDigest = proof.ProofDigest
	if err := proof.Validate(repair); err != nil {
		t.Fatalf("construct valid storage exclusion proof: %v", err)
	}
	if repair.StorageProofDigest != proof.ProofDigest {
		t.Fatal("repair does not pin the exact storage proof")
	}
	storageHash, err := repairProofValueHash(proof.ProofDigest)
	if err != nil {
		t.Fatal(err)
	}
	operation.StorageEffectsExcludedHash = storageHash
	if err := cores["b"].FinishEngineOwnedRemovalAt(ctx, operation); err != nil {
		t.Fatalf("commit durable engine-owned REMOVE terminal: %v", err)
	}
	store := &terminalResumeAuthority{record: repair, proof: proof}
	bucket, err := objstore.NewBucket(objstore.Config{Provider: objstore.ProviderFilesystem, FilesystemDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	archive := recovery.NewManager(bucket, string(clusterID), uint(config.ConfigID))
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { archive.Close() })
	nodeConfig := &types.ExecutionConfig{ClusterID: clusterID, NodeID: "a", EngineOwnedRemovals: true,
		ObjStoreProvider: string(objstore.ProviderFilesystem), ObjStoreDir: "unused"}
	node := &Node{config: nodeConfig, core: cores["a"], transport: transports["a"], archive: archive}
	completed, err := node.resumeEngineRepairTerminal(ctx, store, repair, proof)
	if err != nil || !completed {
		t.Fatalf("resume lost canonical phase CAS from exact durable terminal: completed=%t err=%v", completed, err)
	}
	if store.cas != 1 || store.record.Phase != fencing.RepairRemoved || store.record.RemoveTerminalDigest == "" {
		t.Fatalf("terminal recovery did not perform exactly one canonical phase CAS: cas=%d phase=%s digestPresent=%t", store.cas, store.record.Phase, store.record.RemoveTerminalDigest != "")
	}
	terminal, ok := cores["a"].CertifiedValue(operation.Freeze + 16)
	if !ok || fmt.Sprintf("%x", terminal.Hash[:]) != store.record.RemoveTerminalDigest {
		t.Fatal("canonical repair phase did not pin the exact certified REMOVE terminal")
	}
	if _, err := node.EngineRemovalFreezeEvidence(ctx, *repair.RemovalOperation); err == nil {
		t.Fatal("recovery incorrectly depended on the no-longer-active freeze")
	}
}

func terminalResumeRepairRecord(source quepaxa.Cluster, operation quepaxa.EngineRemoval) fencing.EngineRepairRecord {
	old := operation.Removed
	incarnations := make([]fencing.RepairIncarnation, 0, len(source.Members))
	for i, member := range source.Members {
		incarnations = append(incarnations, fencing.RepairIncarnation{Identity: fencing.Identity{
			SourceClusterID: "engine-repair-terminal-resume", NodeID: string(member.ID), PodName: fmt.Sprintf("rhiza-%d", i),
			PodUID: fmt.Sprintf("pod-%d", i), RunID: fmt.Sprintf("run-%d", i), WALIdentity: member.WALIdentity}, Role: fencing.StartupRoleVoterActive})
	}
	candidate := quepaxa.Member{ID: "candidate", URL: "http://candidate:8080", PeerURL: "quic://candidate:9090", LogURL: "http://candidate:8081",
		WALIdentity: strings.Repeat("d", 64), PublicKey: quepaxa.PublicKey{9}}
	candidateID := fencing.Identity{SourceClusterID: "engine-repair-terminal-resume", NodeID: string(candidate.ID), PodName: "rhiza-2",
		PodUID: "candidate-pod", RunID: "candidate-run", WALIdentity: candidate.WALIdentity, AssignmentID: repairTestDigest("candidate-assignment")}
	runtime := fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "namespace", BindingUID: "binding",
		StatefulSetUID: "statefulset", SourceClusterID: candidateID.SourceClusterID, NodeID: candidateID.NodeID,
		PodName: candidateID.PodName, PodUID: candidateID.PodUID, RunID: candidateID.RunID, AssignmentID: candidateID.AssignmentID,
		NodeName: "worker", NodeUID: "worker-uid", MachineID: "machine", BootID: "boot", RuntimeID: "runtime",
		SandboxID: "sandbox", ContainerID: "container", ImageID: "image"}
	requestHash := repairTestDigest("remove-request")
	record := fencing.EngineRepairRecord{Version: fencing.EngineRepairRecordVersion,
		Binding: fencing.EngineRemovalBinding{Version: fencing.EngineRemovalBindingVersion, Mode: fencing.EngineRemovalMode,
			OperationID: operation.OperationID, RequestHash: requestHash, SourceClusterID: candidateID.SourceClusterID,
			StatefulSetUID: "statefulset", OldConfigID: uint64(source.ConfigID), ExpectedAbortSlot: uint64(operation.ExpectedAbortSlot),
			OldMemberID: string(old.ID), OldWALIdentity: old.WALIdentity, TargetMemberID: candidateID.NodeID,
			FreezeSlot: uint64(operation.Freeze), FreezeHash: fmt.Sprintf("%x", operation.FreezeHash[:])},
		SourceConfig: source, SourceAdminCredential: fencing.EngineRepairAdminCredential{Name: "source-admin", UID: "source-admin-uid",
			AdminTokenDigest: fencing.EngineRepairAdminTokenDigest([]byte("admin-token"))},
		RemovalOperation: &operation, Incarnations: incarnations, CandidateIdentity: candidateID, CandidateMember: candidate,
		CandidateCredential: fencing.EngineRepairCredential{Name: "candidate-peer", UID: "candidate-secret-uid",
			PeerTokenDigest: fencing.EngineRepairPeerTokenDigest([]byte("candidate-token"))},
		CandidateRuntimeBinding: &runtime, CandidateRuntimeHash: fencing.RuntimeBindingDigest(runtime),
		CandidateMemberDigest: fencing.EngineRepairMemberDigest(candidate), RemoveRequestHash: requestHash,
		Phase: fencing.RepairStorageClosing}
	return record
}

func terminalResumeStorageProof(record fencing.EngineRepairRecord) fencing.StorageEffectsExcluded {
	settlements := make([]fencing.StorageRunSettlement, 0)
	for _, incarnation := range record.Incarnations {
		if incarnation.Identity.NodeID != record.Binding.OldMemberID {
			continue
		}
		settlements = append(settlements, fencing.StorageRunSettlement{RunID: incarnation.Identity.RunID,
			PodUID: incarnation.Identity.PodUID, WALIdentity: incarnation.Identity.WALIdentity,
			ClosedThrough: 0, SettledThrough: 0, ProducerStateHash: repairTestDigest("producer-state"),
			SettlementDigest: repairTestDigest("settlement"), NoProducerDigest: repairTestDigest("no-producer")})
	}
	proof := fencing.StorageEffectsExcluded{Version: fencing.StorageEffectsProofVersion, Binding: record.Binding,
		RepairRecordHash: record.IdentityDigest(), RunSettlements: settlements}
	proof.ProofDigest = proof.Digest()
	return proof
}

func repairTestDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func stringsOf(first byte, count int) string {
	return strings.Repeat(string([]byte{first}), count)
}
