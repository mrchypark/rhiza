package network

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// EngineRemovalFreezeAttestation is a peer's signed statement that it has
// durably retained the same exact freeze decision and prefix.
type EngineRemovalFreezeAttestation struct {
	PeerID    quepaxa.NodeID `json:"peerID"`
	Signature []byte         `json:"signature"`
}

// EngineRemovalFreezeProof is formed only from responses received over the
// pinned peer TLS transport and independently signed by original-config
// voters. The configuration must still be compared with canonical authority
// state by the consumer; this structure alone is not an authority source.
type EngineRemovalFreezeProof struct {
	Config       quepaxa.Cluster                     `json:"config"`
	Snapshot     quepaxa.EngineRemovalFreezeSnapshot `json:"snapshot"`
	Attestations []EngineRemovalFreezeAttestation    `json:"attestations"`
}

// EngineRemovalTerminalProof proves that the exact storage-exclusion terminal
// was durably committed by a quorum of the original configuration. It is a
// distinct proof kind from an active freeze; terminal completion is never
// represented as an active freeze snapshot.
type EngineRemovalTerminalProof struct {
	Config       quepaxa.Cluster                       `json:"config"`
	Snapshot     quepaxa.EngineRemovalTerminalSnapshot `json:"snapshot"`
	Attestations []EngineRemovalFreezeAttestation      `json:"attestations"`
}

type engineRemovalProofRequest struct {
	Version   int                   `json:"version"`
	Kind      string                `json:"kind"`
	Operation quepaxa.EngineRemoval `json:"operation"`
}

const (
	engineRemovalProofRequestVersion = 1
	engineRemovalProofFreeze         = "freeze"
	engineRemovalProofTerminal       = "terminal"
)

// EngineRemovalFreezeAttestationMessage returns the domain-separated bytes
// signed by the peer that attests to its retained freeze and durable prefix.
func EngineRemovalFreezeAttestationMessage(clusterID string, peerID quepaxa.NodeID, configID uint, operation quepaxa.EngineRemoval, decisionHash, prefixHash quepaxa.ValueHash) []byte {
	return engineRemovalAttestationMessage("rhiza-engine-removal-freeze-attestation-v1\x00", clusterID, peerID, configID, operation, operation.Freeze, decisionHash, prefixHash)
}

// EngineRemovalTerminalAttestationMessage uses a separate domain and commits
// to the terminal slot so a freeze signature cannot be reinterpreted as a
// successful REMOVE terminal.
func EngineRemovalTerminalAttestationMessage(clusterID string, peerID quepaxa.NodeID, configID uint, operation quepaxa.EngineRemoval, terminalSlot quepaxa.Slot, decisionHash, prefixHash quepaxa.ValueHash) []byte {
	return engineRemovalAttestationMessage("rhiza-engine-removal-terminal-attestation-v1\x00", clusterID, peerID, configID, operation, terminalSlot, decisionHash, prefixHash)
}

func engineRemovalAttestationMessage(domain, clusterID string, peerID quepaxa.NodeID, configID uint, operation quepaxa.EngineRemoval, slot quepaxa.Slot, decisionHash, prefixHash quepaxa.ValueHash) []byte {
	operationBytes, _ := json.Marshal(operation)
	operationDigest := sha256.Sum256(operationBytes)
	message := make([]byte, 0, 32+len(clusterID)+len(peerID)+8*2+32*3)
	message = append(message, []byte(domain)...)
	message = append(message, clusterID...)
	message = append(message, 0)
	message = append(message, peerID...)
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], uint64(configID))
	message = append(message, number[:]...)
	binary.BigEndian.PutUint64(number[:], uint64(slot))
	message = append(message, number[:]...)
	message = append(message, operationDigest[:]...)
	message = append(message, decisionHash[:]...)
	message = append(message, prefixHash[:]...)
	return message
}

// VerifyEngineRemovalTerminalProof validates the exact certified terminal
// value and independent original-voter signatures against the canonical
// original configuration and operation tuple.
func VerifyEngineRemovalTerminalProof(proof EngineRemovalTerminalProof, sourceClusterID string, expectedConfig quepaxa.Cluster, expected quepaxa.EngineRemoval) error {
	if !samePeerCluster(proof.Config, expectedConfig) || expectedConfig.ConfigID != expected.ExpectedConfigID ||
		proof.Snapshot.ConfigID != expected.ExpectedConfigID || !sameEngineRemovalOperation(proof.Snapshot.Operation, expected) ||
		!engineRemovalSourceMatchesTarget(expectedConfig, expected.Target, expected.Removed) ||
		expected.StorageEffectsExcludedHash == (quepaxa.ValueHash{}) || expected.FreezeHash == (quepaxa.ValueHash{}) ||
		proof.Snapshot.Decision.Hash == (quepaxa.ValueHash{}) || sha256.Sum256(proof.Snapshot.Decision.Value) != proof.Snapshot.Decision.Hash {
		return fmt.Errorf("engine removal terminal proof differs from canonical operation")
	}
	terminalSlot := expected.Freeze + 16
	if terminalSlot < expected.Freeze || proof.Snapshot.Decision.Slot != terminalSlot || proof.Snapshot.PrefixHash == (quepaxa.ValueHash{}) {
		return fmt.Errorf("engine removal terminal proof has an invalid durable position")
	}
	terminal, ok, err := quepaxa.DecodeEngineRemovalTerminal(proof.Snapshot.Decision.Value)
	if err != nil || !ok || terminal.Freeze != expected.Freeze || terminal.TerminalSlot != terminalSlot ||
		!samePeerCluster(terminal.Target, expected.Target) || !sameEngineRemovalOperation(terminal.Operation, expected) ||
		terminal.Operation.StorageEffectsExcludedHash != expected.StorageEffectsExcludedHash {
		return fmt.Errorf("engine removal terminal proof contains another terminal payload")
	}
	members := expectedConfig.MemberSet()
	seen := make(map[quepaxa.NodeID]struct{}, len(proof.Attestations))
	for _, attestation := range proof.Attestations {
		member, exists := members[attestation.PeerID]
		if !exists || len(attestation.Signature) != ed25519.SignatureSize {
			return fmt.Errorf("engine removal terminal proof has an unknown or malformed peer attestation")
		}
		if _, duplicate := seen[attestation.PeerID]; duplicate {
			return fmt.Errorf("engine removal terminal proof duplicates a peer")
		}
		seen[attestation.PeerID] = struct{}{}
		message := EngineRemovalTerminalAttestationMessage(sourceClusterID, attestation.PeerID, expectedConfig.ConfigID, expected, terminalSlot, proof.Snapshot.Decision.Hash, proof.Snapshot.PrefixHash)
		if !ed25519.Verify(ed25519.PublicKey(member.PublicKey[:]), message, attestation.Signature) {
			return fmt.Errorf("engine removal terminal peer attestation is invalid")
		}
	}
	if len(seen) < len(expectedConfig.Members)/2+1 {
		return quepaxa.ErrQuorumUnavailable
	}
	return nil
}

// VerifyEngineRemovalFreezeProof checks the complete attestation set against
// an independently trusted original configuration and exact operation tuple.

func VerifyEngineRemovalFreezeProof(proof EngineRemovalFreezeProof, sourceClusterID string, expectedConfig quepaxa.Cluster, expected quepaxa.EngineRemoval) error {
	if !samePeerCluster(proof.Config, expectedConfig) || expectedConfig.ConfigID != expected.ExpectedConfigID ||
		proof.Snapshot.ConfigID != expected.ExpectedConfigID || !sameEngineRemovalOperation(proof.Snapshot.Operation, expected) ||
		!engineRemovalSourceMatchesTarget(expectedConfig, expected.Target, expected.Removed) ||
		proof.Snapshot.Decision.Slot != expected.Freeze || proof.Snapshot.Decision.Hash != expected.FreezeHash ||
		sha256.Sum256(proof.Snapshot.Decision.Value) != proof.Snapshot.Decision.Hash || len(expectedConfig.Members) == 0 {
		return fmt.Errorf("engine removal freeze proof differs from canonical operation")
	}
	decision, ok, err := quepaxa.DecodeEngineRemovalFreeze(proof.Snapshot.Decision.Value)
	if err != nil || !ok || decision.Freeze != expected.Freeze || decision.TerminalSlot != expected.Freeze+16 ||
		decision.Target.ConfigID != expected.Target.ConfigID || !samePeerCluster(decision.Target, expected.Target) {
		return fmt.Errorf("engine removal freeze proof contains another control value")
	}
	if !sameEngineRemovalOperation(decision.Operation, quepaxa.EngineRemoval{Version: expected.Version, Mode: expected.Mode,
		OperationID: expected.OperationID, ExpectedConfigID: expected.ExpectedConfigID, ExpectedAbortSlot: expected.ExpectedAbortSlot,
		Removed: expected.Removed, Target: expected.Target, Freeze: expected.Freeze}) ||
		decision.Operation.FreezeHash != (quepaxa.ValueHash{}) || decision.Operation.StorageEffectsExcludedHash != (quepaxa.ValueHash{}) {
		return fmt.Errorf("engine removal freeze control does not match the operation")
	}
	if proof.Snapshot.PrefixHash == (quepaxa.ValueHash{}) {
		return fmt.Errorf("engine removal freeze prefix is incomplete")
	}
	members := expectedConfig.MemberSet()
	seen := make(map[quepaxa.NodeID]struct{}, len(proof.Attestations))
	for _, attestation := range proof.Attestations {
		member, exists := members[attestation.PeerID]
		if !exists || attestation.PeerID == expected.Removed.ID || len(attestation.Signature) != ed25519.SignatureSize {
			return fmt.Errorf("engine removal freeze proof has an unknown or malformed peer attestation")
		}
		if _, duplicate := seen[attestation.PeerID]; duplicate {
			return fmt.Errorf("engine removal freeze proof duplicates a peer")
		}
		seen[attestation.PeerID] = struct{}{}
		message := EngineRemovalFreezeAttestationMessage(sourceClusterID, attestation.PeerID, expectedConfig.ConfigID, expected, proof.Snapshot.Decision.Hash, proof.Snapshot.PrefixHash)
		if !ed25519.Verify(ed25519.PublicKey(member.PublicKey[:]), message, attestation.Signature) {
			return fmt.Errorf("engine removal freeze peer attestation is invalid")
		}
	}
	if len(seen) < len(expectedConfig.Members)/2+1 {
		return quepaxa.ErrQuorumUnavailable
	}
	return nil
}

// engineRemovalSourceMatchesTarget binds the caller-provided original source
// configuration to the exact one-member removal. A proof and expected config
// must not be allowed to agree with each other while disagreeing with the
// operation's retained target membership.
func engineRemovalSourceMatchesTarget(source, target quepaxa.Cluster, removed quepaxa.Member) bool {
	if source.ConfigID == 0 || target.ConfigID != source.ConfigID+1 || len(source.Members) < 2 || len(source.Members) > 64 ||
		len(target.Members) != len(source.Members)-1 || removed.ID == "" || removed.WALIdentity == "" {
		return false
	}
	sourceMembers := source.MemberSet()
	targetMembers := target.MemberSet()
	if len(sourceMembers) != len(source.Members) || len(targetMembers) != len(target.Members) {
		return false
	}
	var omitted quepaxa.Member
	for _, member := range source.Members {
		if member.ID == "" {
			return false
		}
		candidate, ok := targetMembers[member.ID]
		if !ok {
			if omitted.ID != "" {
				return false
			}
			omitted = member
			continue
		}
		if candidate != member {
			return false
		}
	}
	if omitted != removed {
		return false
	}
	for _, member := range target.Members {
		if _, exists := sourceMembers[member.ID]; !exists {
			return false
		}
	}
	return true
}

func sameEngineRemovalOperation(left, right quepaxa.EngineRemoval) bool {
	return left.Version == right.Version && left.Mode == right.Mode && left.OperationID == right.OperationID &&
		left.ExpectedConfigID == right.ExpectedConfigID && left.ExpectedAbortSlot == right.ExpectedAbortSlot &&
		left.Removed == right.Removed && left.Freeze == right.Freeze && left.FreezeHash == right.FreezeHash &&
		left.StorageEffectsExcludedHash == right.StorageEffectsExcludedHash && samePeerCluster(left.Target, right.Target)
}

func samePeerCluster(left, right quepaxa.Cluster) bool {
	if left.ConfigID != right.ConfigID || len(left.Members) != len(right.Members) {
		return false
	}
	leftMembers, rightMembers := left.MemberSet(), right.MemberSet()
	if len(leftMembers) != len(rightMembers) {
		return false
	}
	for id, member := range leftMembers {
		if rightMembers[id] != member {
			return false
		}
	}
	return true
}

type freezeReply struct {
	member quepaxa.Member
	resp   *peerfb.ResponseT
	err    error
}

func decodeStrictPeerJSON(data []byte, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("trailing peer JSON")
	}
	return nil
}

// ReadEngineRemovalFreezeQuorum obtains the exact freeze and local durable
// prefix from an original-configuration quorum. Each response is pinned by
// peer TLS and carries a signature checked against that member's canonical key.
func (t *Transport) ReadEngineRemovalFreezeQuorum(ctx context.Context, operation quepaxa.EngineRemoval) (EngineRemovalFreezeProof, error) {
	if ctx == nil || t == nil || operation.Freeze == 0 || operation.ExpectedConfigID == 0 {
		return EngineRemovalFreezeProof{}, fmt.Errorf("engine removal freeze peer query is incomplete")
	}
	peer, err := t.transportForSlot(operation.Freeze)
	if err != nil {
		return EngineRemovalFreezeProof{}, err
	}
	if peer != t {
		return peer.ReadEngineRemovalFreezeQuorum(ctx, operation)
	}
	// transportForSlot returns an immutable per-configuration transport. That
	// transport intentionally has no live resolver; its copied membership is
	// the slot-pinned source configuration selected by the parent transport.
	config := peer.clusterForSlot(operation.Freeze)
	if config.ConfigID != operation.ExpectedConfigID || len(config.Members) < 2 || len(config.Members) > 64 {
		return EngineRemovalFreezeProof{}, fmt.Errorf("engine removal freeze source configuration differs")
	}
	body, err := json.Marshal(engineRemovalProofRequest{Version: engineRemovalProofRequestVersion, Kind: engineRemovalProofFreeze, Operation: operation})
	if err != nil || len(body) == 0 || len(body) > 128<<10 {
		return EngineRemovalFreezeProof{}, fmt.Errorf("engine removal freeze request is invalid")
	}
	request := peer.request(peerfb.OperationEngineRemovalFreeze)
	request.From, request.Value = uint64(operation.Freeze), body
	replies := make(chan freezeReply, len(config.Members))
	var wg sync.WaitGroup
	for _, member := range config.Members {
		if member.ID == operation.Removed.ID {
			continue
		}
		member := member
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, callErr := peer.call(ctx, member.ID, request)
			replies <- freezeReply{member: member, resp: response, err: callErr}
		}()
	}
	go func() { wg.Wait(); close(replies) }()
	var snapshot *quepaxa.EngineRemovalFreezeSnapshot
	proof := EngineRemovalFreezeProof{Config: config}
	for reply := range replies {
		if reply.err != nil || reply.resp == nil || reply.resp.ClusterId != string(peer.clusterID) || reply.resp.ProposerId != string(reply.member.ID) ||
			reply.resp.ConfigId != uint64(config.ConfigID) || reply.resp.Tip != uint64(operation.Freeze) || len(reply.resp.Value) != 32+ed25519.SignatureSize {
			continue
		}
		decision, decodeErr := decidedFromWire(reply.resp.Decided)
		if decodeErr != nil || decision.Slot != operation.Freeze || decision.Hash != operation.FreezeHash || sha256.Sum256(decision.Value) != decision.Hash {
			continue
		}
		var prefix quepaxa.ValueHash
		copy(prefix[:], reply.resp.Value[:32])
		message := EngineRemovalFreezeAttestationMessage(string(peer.clusterID), reply.member.ID, config.ConfigID, operation, decision.Hash, prefix)
		signature := reply.resp.Value[32:]
		if !ed25519.Verify(ed25519.PublicKey(reply.member.PublicKey[:]), message, signature) {
			continue
		}
		candidate := quepaxa.EngineRemovalFreezeSnapshot{ConfigID: config.ConfigID, Operation: operation, Decision: decision, PrefixHash: prefix}
		if snapshot == nil {
			snapshot = &candidate
		} else if snapshot.Decision.Hash != candidate.Decision.Hash || !bytes.Equal(snapshot.Decision.Value, candidate.Decision.Value) ||
			!bytes.Equal(snapshot.Decision.Certificate, candidate.Decision.Certificate) || snapshot.PrefixHash != candidate.PrefixHash {
			return EngineRemovalFreezeProof{}, fmt.Errorf("original voters disagree on durable engine removal freeze")
		}
		proof.Attestations = append(proof.Attestations, EngineRemovalFreezeAttestation{PeerID: reply.member.ID, Signature: append([]byte(nil), signature...)})
	}
	if snapshot == nil || len(proof.Attestations) < len(config.Members)/2+1 {
		return EngineRemovalFreezeProof{}, quepaxa.ErrQuorumUnavailable
	}
	proof.Snapshot = *snapshot
	if err := VerifyEngineRemovalFreezeProof(proof, string(peer.clusterID), config, operation); err != nil {
		return EngineRemovalFreezeProof{}, err
	}
	return proof, nil
}

// ReadEngineRemovalTerminalQuorum obtains the retained exact terminal and
// durable prefix from a quorum of the operation's original configuration.
func (t *Transport) ReadEngineRemovalTerminalQuorum(ctx context.Context, operation quepaxa.EngineRemoval) (EngineRemovalTerminalProof, error) {
	var empty EngineRemovalTerminalProof
	terminalSlot := operation.Freeze + 16
	if ctx == nil || t == nil || operation.Freeze == 0 || operation.ExpectedConfigID == 0 || terminalSlot < operation.Freeze ||
		operation.StorageEffectsExcludedHash == (quepaxa.ValueHash{}) || operation.FreezeHash == (quepaxa.ValueHash{}) {
		return empty, fmt.Errorf("engine removal terminal peer query is incomplete")
	}
	peer, err := t.transportForSlot(operation.Freeze)
	if err != nil {
		return empty, err
	}
	if peer != t {
		return peer.ReadEngineRemovalTerminalQuorum(ctx, operation)
	}
	config := peer.clusterForSlot(operation.Freeze)
	if config.ConfigID != operation.ExpectedConfigID || len(config.Members) < 2 || len(config.Members) > 64 {
		return empty, fmt.Errorf("engine removal terminal source configuration differs")
	}
	body, err := json.Marshal(engineRemovalProofRequest{Version: engineRemovalProofRequestVersion, Kind: engineRemovalProofTerminal, Operation: operation})
	if err != nil || len(body) == 0 || len(body) > 128<<10 {
		return empty, fmt.Errorf("engine removal terminal request is invalid")
	}
	request := peer.request(peerfb.OperationEngineRemovalFreeze)
	request.From, request.Value = uint64(terminalSlot), body
	replies := make(chan freezeReply, len(config.Members))
	var wg sync.WaitGroup
	for _, member := range config.Members {
		member := member
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, callErr := peer.call(ctx, member.ID, request)
			replies <- freezeReply{member: member, resp: response, err: callErr}
		}()
	}
	go func() { wg.Wait(); close(replies) }()
	var snapshot *quepaxa.EngineRemovalTerminalSnapshot
	proof := EngineRemovalTerminalProof{Config: config}
	for reply := range replies {
		if reply.err != nil || reply.resp == nil || reply.resp.ClusterId != string(peer.clusterID) || reply.resp.ProposerId != string(reply.member.ID) ||
			reply.resp.ConfigId != uint64(config.ConfigID) || reply.resp.Tip != uint64(terminalSlot) || len(reply.resp.Value) != 32+ed25519.SignatureSize {
			continue
		}
		decision, decodeErr := decidedFromWire(reply.resp.Decided)
		if decodeErr != nil || decision.Slot != terminalSlot || sha256.Sum256(decision.Value) != decision.Hash {
			continue
		}
		var prefix quepaxa.ValueHash
		copy(prefix[:], reply.resp.Value[:32])
		message := EngineRemovalTerminalAttestationMessage(string(peer.clusterID), reply.member.ID, config.ConfigID, operation, terminalSlot, decision.Hash, prefix)
		signature := reply.resp.Value[32:]
		if !ed25519.Verify(ed25519.PublicKey(reply.member.PublicKey[:]), message, signature) {
			continue
		}
		candidate := quepaxa.EngineRemovalTerminalSnapshot{ConfigID: config.ConfigID, Operation: operation, Decision: decision, PrefixHash: prefix}
		if snapshot == nil {
			snapshot = &candidate
		} else if snapshot.Decision.Hash != candidate.Decision.Hash || !bytes.Equal(snapshot.Decision.Value, candidate.Decision.Value) ||
			!bytes.Equal(snapshot.Decision.Certificate, candidate.Decision.Certificate) || snapshot.PrefixHash != candidate.PrefixHash {
			return empty, fmt.Errorf("original voters disagree on durable engine removal terminal")
		}
		proof.Attestations = append(proof.Attestations, EngineRemovalFreezeAttestation{PeerID: reply.member.ID, Signature: append([]byte(nil), signature...)})
	}
	if snapshot == nil || len(proof.Attestations) < len(config.Members)/2+1 {
		return empty, quepaxa.ErrQuorumUnavailable
	}
	proof.Snapshot = *snapshot
	if err := VerifyEngineRemovalTerminalProof(proof, string(peer.clusterID), config, operation); err != nil {
		return empty, err
	}
	return proof, nil
}
