package network

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

func validEngineRemovalFreezeProof(t *testing.T) (EngineRemovalFreezeProof, string, quepaxa.Cluster, quepaxa.EngineRemoval, []ed25519.PrivateKey) {
	t.Helper()
	clusterID := "freeze-source"
	config := quepaxa.Cluster{ConfigID: 7}
	privateKeys := make([]ed25519.PrivateKey, 3)
	for i, id := range []quepaxa.NodeID{"a", "b", "c"} {
		publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		var fixed quepaxa.PublicKey
		copy(fixed[:], publicKey)
		privateKeys[i] = privateKey
		config.Members = append(config.Members, quepaxa.Member{ID: id, URL: fmt.Sprintf("https://%s.example:8080", id),
			PeerURL: fmt.Sprintf("quic://%s.example:9090", id), LogURL: fmt.Sprintf("https://%s.example:8081", id),
			PublicKey: fixed, WALIdentity: "wal-" + string(id)})
	}
	removed := config.Members[2]
	target := quepaxa.Cluster{ConfigID: 8, Members: append([]quepaxa.Member(nil), config.Members[:2]...)}
	operation := quepaxa.EngineRemoval{Version: 1, Mode: "engine-owned-remove", OperationID: "remove-c", ExpectedConfigID: 7,
		ExpectedAbortSlot: 2, Removed: removed, Target: target, Freeze: 44}
	// The freeze decision intentionally carries the pre-freeze operation with
	// terminal-only hashes unset. The returned operation binds its decision hash.
	decisionOperation := operation
	decisionOperation.FreezeHash = quepaxa.ValueHash{}
	decisionOperation.StorageEffectsExcludedHash = quepaxa.ValueHash{}
	decisionValue, err := json.Marshal(struct {
		Freeze        quepaxa.Slot          `json:"freeze"`
		TerminalSlot  quepaxa.Slot          `json:"terminal_slot"`
		Target        quepaxa.Cluster       `json:"target"`
		EngineRemoval quepaxa.EngineRemoval `json:"engine_removal"`
	}{Freeze: operation.Freeze, TerminalSlot: operation.Freeze + 16, Target: target, EngineRemoval: decisionOperation})
	if err != nil {
		t.Fatal(err)
	}
	decisionValue = append([]byte("QRCF\x00"), decisionValue...)
	decisionHash := sha256.Sum256(decisionValue)
	copy(operation.FreezeHash[:], decisionHash[:])
	prefix := sha256.Sum256([]byte("durable-prefix-through-freeze"))
	proof := EngineRemovalFreezeProof{Config: config, Snapshot: quepaxa.EngineRemovalFreezeSnapshot{
		ConfigID: 7, Operation: operation, Decision: quepaxa.DecidedValue{Slot: operation.Freeze, Hash: decisionHash, Value: decisionValue},
		PrefixHash: prefix,
	}}
	for i, member := range config.Members[:2] {
		message := EngineRemovalFreezeAttestationMessage(clusterID, member.ID, config.ConfigID, operation, decisionHash, prefix)
		proof.Attestations = append(proof.Attestations, EngineRemovalFreezeAttestation{PeerID: member.ID, Signature: ed25519.Sign(privateKeys[i], message)})
	}
	return proof, clusterID, config, operation, privateKeys
}

func TestVerifyEngineRemovalFreezeProofBindsOriginalQuorumAndExactOperation(t *testing.T) {
	proof, clusterID, config, operation, keys := validEngineRemovalFreezeProof(t)
	if err := VerifyEngineRemovalFreezeProof(proof, clusterID, config, operation); err != nil {
		t.Fatalf("valid original-quorum proof rejected: %v", err)
	}

	tests := map[string]func(*EngineRemovalFreezeProof, *string, *quepaxa.Cluster, *quepaxa.EngineRemoval){
		"wrong peer signing key": func(p *EngineRemovalFreezeProof, _ *string, _ *quepaxa.Cluster, _ *quepaxa.EngineRemoval) {
			message := EngineRemovalFreezeAttestationMessage(clusterID, p.Attestations[0].PeerID, config.ConfigID, operation, p.Snapshot.Decision.Hash, p.Snapshot.PrefixHash)
			p.Attestations[0].Signature = ed25519.Sign(keys[2], message)
		},
		"wrong source cluster": func(_ *EngineRemovalFreezeProof, id *string, _ *quepaxa.Cluster, _ *quepaxa.EngineRemoval) {
			*id = "other-source"
		},
		"wrong source configuration": func(p *EngineRemovalFreezeProof, _ *string, expected *quepaxa.Cluster, _ *quepaxa.EngineRemoval) {
			expected.Members[0].WALIdentity = "replaced-wal"
			p.Config = *expected
		},
		"wrong old WAL": func(_ *EngineRemovalFreezeProof, _ *string, expected *quepaxa.Cluster, _ *quepaxa.EngineRemoval) {
			expected.Members[2].WALIdentity = "recreated-old-wal"
		},
		"wrong operation ID": func(_ *EngineRemovalFreezeProof, _ *string, _ *quepaxa.Cluster, expected *quepaxa.EngineRemoval) {
			expected.OperationID = "other-operation"
		},
		"wrong abort slot": func(_ *EngineRemovalFreezeProof, _ *string, _ *quepaxa.Cluster, expected *quepaxa.EngineRemoval) {
			expected.ExpectedAbortSlot++
		},
		"wrong freeze slot": func(_ *EngineRemovalFreezeProof, _ *string, _ *quepaxa.Cluster, expected *quepaxa.EngineRemoval) {
			expected.Freeze++
		},
		"tampered decision hash": func(p *EngineRemovalFreezeProof, _ *string, _ *quepaxa.Cluster, _ *quepaxa.EngineRemoval) {
			p.Snapshot.Decision.Hash[0] ^= 0xff
		},
		"quorum missing": func(p *EngineRemovalFreezeProof, _ *string, _ *quepaxa.Cluster, _ *quepaxa.EngineRemoval) {
			p.Attestations = p.Attestations[:1]
		},
		"duplicate signer": func(p *EngineRemovalFreezeProof, _ *string, _ *quepaxa.Cluster, _ *quepaxa.EngineRemoval) {
			p.Attestations[1].PeerID = p.Attestations[0].PeerID
		},
		"unknown signer": func(p *EngineRemovalFreezeProof, _ *string, _ *quepaxa.Cluster, _ *quepaxa.EngineRemoval) {
			p.Attestations[1].PeerID = "not-in-source-config"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := proof
			candidate.Config.Members = append([]quepaxa.Member(nil), proof.Config.Members...)
			candidate.Snapshot = proof.Snapshot
			candidate.Snapshot.Decision.Value = append([]byte(nil), proof.Snapshot.Decision.Value...)
			candidate.Attestations = append([]EngineRemovalFreezeAttestation(nil), proof.Attestations...)
			candidate.Attestations[0].Signature = append([]byte(nil), proof.Attestations[0].Signature...)
			candidate.Attestations[1].Signature = append([]byte(nil), proof.Attestations[1].Signature...)
			id, expectedConfig, expectedOperation := clusterID, config, operation
			mutate(&candidate, &id, &expectedConfig, &expectedOperation)
			if err := VerifyEngineRemovalFreezeProof(candidate, id, expectedConfig, expectedOperation); err == nil {
				t.Fatal("mismatched or unauthenticated freeze proof accepted")
			}
		})
	}
}

func TestVerifyEngineRemovalFreezeProofRejectsRemovedVoterAsQuorumSigner(t *testing.T) {
	proof, clusterID, config, operation, keys := validEngineRemovalFreezeProof(t)
	removed := operation.Removed.ID
	message := EngineRemovalFreezeAttestationMessage(clusterID, removed, config.ConfigID, operation, proof.Snapshot.Decision.Hash, proof.Snapshot.PrefixHash)
	proof.Attestations = []EngineRemovalFreezeAttestation{
		proof.Attestations[0],
		{PeerID: removed, Signature: ed25519.Sign(keys[2], message)},
	}
	if len(proof.Attestations) != len(config.Members)/2+1 {
		t.Fatal("test must meet the original configuration threshold using the removed voter")
	}
	if err := VerifyEngineRemovalFreezeProof(proof, clusterID, config, operation); err == nil {
		t.Fatal("removed voter was counted toward the original-survivor freeze quorum")
	}
}

func TestVerifyEngineRemovalTerminalProofIsDistinctAndBindsExactTerminal(t *testing.T) {
	freezeProof, clusterID, config, operation, keys := validEngineRemovalFreezeProof(t)
	operation.FreezeHash = freezeProof.Snapshot.Decision.Hash
	operation.StorageEffectsExcludedHash = quepaxa.ValueHash(sha256.Sum256([]byte("canonical-storage-exclusion-proof")))
	terminalSlot := operation.Freeze + 16
	prefix := quepaxa.ValueHash(sha256.Sum256([]byte("durable-prefix-through-terminal")))
	terminalValue, err := json.Marshal(struct {
		Terminal      bool                   `json:"terminal,omitempty"`
		Abort         bool                   `json:"abort,omitempty"`
		Freeze        quepaxa.Slot           `json:"freeze"`
		TerminalSlot  quepaxa.Slot           `json:"terminal_slot"`
		Target        quepaxa.Cluster        `json:"target"`
		PrefixHash    quepaxa.ValueHash      `json:"prefix_hash"`
		EngineRemoval *quepaxa.EngineRemoval `json:"engine_removal,omitempty"`
	}{Terminal: true, Freeze: operation.Freeze, TerminalSlot: terminalSlot, Target: operation.Target, PrefixHash: prefix, EngineRemoval: &operation})
	if err != nil {
		t.Fatal(err)
	}
	terminalValue = append([]byte("QRCF\x00"), terminalValue...)
	decisionHash := quepaxa.ValueHash(sha256.Sum256(terminalValue))
	proof := EngineRemovalTerminalProof{Config: config, Snapshot: quepaxa.EngineRemovalTerminalSnapshot{
		ConfigID: config.ConfigID, Operation: operation,
		Decision: quepaxa.DecidedValue{Slot: terminalSlot, Hash: decisionHash, Value: terminalValue}, PrefixHash: prefix,
	}}
	for i, member := range config.Members[:2] {
		message := EngineRemovalTerminalAttestationMessage(clusterID, member.ID, config.ConfigID, operation, terminalSlot, decisionHash, prefix)
		proof.Attestations = append(proof.Attestations, EngineRemovalFreezeAttestation{PeerID: member.ID, Signature: ed25519.Sign(keys[i], message)})
	}
	if err := VerifyEngineRemovalTerminalProof(proof, clusterID, config, operation); err != nil {
		t.Fatalf("valid original-quorum terminal proof rejected: %v", err)
	}

	wrongOperation := operation
	wrongOperation.OperationID += "-other"
	if err := VerifyEngineRemovalTerminalProof(proof, clusterID, config, wrongOperation); err == nil {
		t.Fatal("accepted terminal for another removal operation")
	}
	wrongStorage := operation
	wrongStorage.StorageEffectsExcludedHash[0] ^= 0xff
	if err := VerifyEngineRemovalTerminalProof(proof, clusterID, config, wrongStorage); err == nil {
		t.Fatal("accepted terminal referencing another storage-exclusion proof")
	}
	wrongSource := config
	wrongSource.Members = append([]quepaxa.Member(nil), config.Members...)
	wrongSource.Members[0].WALIdentity = "replaced-wal"
	wrongSourceProof := proof
	wrongSourceProof.Config = wrongSource
	if err := VerifyEngineRemovalTerminalProof(wrongSourceProof, clusterID, wrongSource, operation); err == nil {
		t.Fatal("accepted terminal whose source configuration disagrees with retained target members")
	}
	freezeSignature := EngineRemovalFreezeAttestationMessage(clusterID, proof.Attestations[0].PeerID, config.ConfigID, operation, decisionHash, prefix)
	if ed25519.Verify(ed25519.PublicKey(config.Members[0].PublicKey[:]), freezeSignature, proof.Attestations[0].Signature) {
		t.Fatal("terminal attestation was interchangeable with an active-freeze attestation")
	}
	missingQuorum := proof
	missingQuorum.Attestations = missingQuorum.Attestations[:1]
	if err := VerifyEngineRemovalTerminalProof(missingQuorum, clusterID, config, operation); err == nil {
		t.Fatal("accepted terminal proof without original-config quorum")
	}
}

func TestReadEngineRemovalProofRetriesFromActiveFreezeToCommittedTerminal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	clusterID := types.ClusterID("engine-removal-retry")
	ids := []quepaxa.NodeID{"a", "b", "c"}
	tokens := map[quepaxa.NodeID]string{"a": "peer-a", "b": "peer-b", "c": "peer-c"}
	config := quepaxa.Cluster{ConfigID: 7}
	packets := make([]net.PacketConn, 0, len(ids))
	listeners := make([]*quic.Transport, 0, len(ids))
	transports := make(map[quepaxa.NodeID]*Transport, len(ids))
	cores := make(map[quepaxa.NodeID]*quepaxa.Core, len(ids))
	servers := make(map[quepaxa.NodeID]*Server, len(ids))
	for _, id := range ids {
		packet, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		packets = append(packets, packet)
		t.Cleanup(func() { _ = packet.Close() })
		listeners = append(listeners, &quic.Transport{Conn: packet})
		member := testMember(clusterID, id, tokens[id])
		member.PeerURL = "quic://" + packet.LocalAddr().String()
		member.WALIdentity = strings.Repeat(string(id), 64)
		config.Members = append(config.Members, member)
	}
	for _, listener := range listeners {
		listener := listener
		t.Cleanup(func() { _ = listener.Close() })
	}
	for _, member := range config.Members {
		transport := NewTransport(clusterID, member.ID, &config, tokens[member.ID])
		transports[member.ID] = transport
		t.Cleanup(func() { _ = transport.Close() })
		wal, err := qlog.Open(filepath.Join(t.TempDir(), string(member.ID)+".qlog"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		walIdentity, err := hex.DecodeString(member.WALIdentity)
		if err != nil {
			t.Fatalf("decode voter WAL identity: %v", err)
		}
		if err := wal.BindIdentity(walIdentity); err != nil {
			t.Fatalf("bind voter WAL identity: %v", err)
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
		server := NewServer(core, material, clusterID, true, transport)
		servers[member.ID] = server
		t.Cleanup(server.Close)
	}
	for i, member := range config.Members {
		peer, err := StartPeerServerOnTransport(ctx, listeners[i], servers[member.ID], config.Members, tokens[member.ID], "admin-token")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = peer.Close() })
	}
	admin := NewTransport(clusterID, "candidate", &config, "")
	admin.SetAdminToken("admin-token")
	t.Cleanup(func() { admin.SetAdminToken(""); _ = admin.Close() })

	removeTarget := quepaxa.Cluster{ConfigID: config.ConfigID + 1, Members: append([]quepaxa.Member(nil), config.Members[:2]...)}
	operation, err := cores["a"].BeginEngineOwnedRemovalAt(ctx, removeTarget, "remove-retry-after-terminal", 0)
	if err != nil {
		t.Fatalf("begin engine-owned REMOVE: %v", err)
	}
	if _, err := admin.ReadEngineRemovalFreezeQuorum(ctx, operation); err != nil {
		t.Fatalf("read active original-quorum freeze: %v", err)
	}
	operation.StorageEffectsExcludedHash = quepaxa.ValueHash{1}
	if err := cores["a"].FinishEngineOwnedRemovalAt(ctx, operation); err != nil {
		t.Fatalf("commit engine-owned REMOVE terminal: %v", err)
	}
	if _, err := admin.ReadEngineRemovalFreezeQuorum(ctx, *freezeOperation(operation)); err == nil {
		t.Fatal("active-freeze query unexpectedly succeeded after terminal completion")
	}
	if _, err := admin.ReadEngineRemovalTerminalQuorum(ctx, operation); err != nil {
		t.Fatalf("retry from exact original-quorum terminal proof: %v", err)
	}
	wrongStorage := operation
	wrongStorage.StorageEffectsExcludedHash[0] ^= 0xff
	if _, err := admin.ReadEngineRemovalTerminalQuorum(ctx, wrongStorage); err == nil {
		t.Fatal("terminal retry accepted a different storage-exclusion digest")
	}
}

func freezeOperation(operation quepaxa.EngineRemoval) *quepaxa.EngineRemoval {
	operation.StorageEffectsExcludedHash = quepaxa.ValueHash{}
	return &operation
}
