package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// EngineRemovalFreezeEvidence returns an independently authenticated proof
// that an original-configuration quorum durably retains the exact active
// engine-owned REMOVE freeze. Local Core state alone is not treated as quorum
// evidence.
func (n *Node) EngineRemovalFreezeEvidence(ctx context.Context, operation quepaxa.EngineRemoval) (network.EngineRemovalFreezeProof, error) {
	if n == nil || ctx == nil || n.core == nil || n.transport == nil || n.config == nil || !n.core.IsVoter() {
		return network.EngineRemovalFreezeProof{}, fmt.Errorf("engine removal freeze verifier is unavailable on this node")
	}
	local, err := n.core.EngineRemovalFreezeSnapshot(ctx, operation)
	if err != nil {
		return network.EngineRemovalFreezeProof{}, err
	}
	proof, err := n.transport.ReadEngineRemovalFreezeQuorum(ctx, operation)
	if err != nil {
		return network.EngineRemovalFreezeProof{}, err
	}
	config := n.core.ClusterForSlot(operation.Freeze)
	if proof.Snapshot.ConfigID != local.ConfigID || proof.Snapshot.Decision.Hash != local.Decision.Hash ||
		!bytes.Equal(proof.Snapshot.Decision.Value, local.Decision.Value) ||
		!bytes.Equal(proof.Snapshot.Decision.Certificate, local.Decision.Certificate) || proof.Snapshot.PrefixHash != local.PrefixHash {
		return network.EngineRemovalFreezeProof{}, fmt.Errorf("original quorum freeze evidence differs from this node's retained freeze")
	}
	if err := network.VerifyEngineRemovalFreezeProof(proof, string(n.config.ClusterID), config, operation); err != nil {
		return network.EngineRemovalFreezeProof{}, err
	}
	return proof, nil
}

// EngineRemovalTerminalEvidence proves that the exact storage-exclusion
// terminal is retained by the original voter quorum and by this Core. A
// current membership view alone is not terminal evidence: the old source
// quorum and the certified decision/prefix must both match the pinned
// operation.
func (n *Node) EngineRemovalTerminalEvidence(ctx context.Context, operation quepaxa.EngineRemoval) (network.EngineRemovalTerminalProof, error) {
	if n == nil || ctx == nil || n.core == nil || n.transport == nil || n.config == nil || !n.core.IsVoter() {
		return network.EngineRemovalTerminalProof{}, fmt.Errorf("engine removal terminal verifier is unavailable on this node")
	}
	if operation.Version != 1 || operation.Mode != fencing.EngineRemovalMode || operation.OperationID == "" ||
		operation.Freeze == 0 || operation.FreezeHash == (quepaxa.ValueHash{}) ||
		operation.StorageEffectsExcludedHash == (quepaxa.ValueHash{}) {
		return network.EngineRemovalTerminalProof{}, fmt.Errorf("engine removal terminal operation is incomplete")
	}
	proof, err := n.transport.ReadEngineRemovalTerminalQuorum(ctx, operation)
	if err != nil {
		return network.EngineRemovalTerminalProof{}, err
	}
	config := n.core.ClusterForSlot(operation.Freeze)
	if err := network.VerifyEngineRemovalTerminalProof(proof, string(n.config.ClusterID), config, operation); err != nil {
		return network.EngineRemovalTerminalProof{}, err
	}
	local, err := n.core.EngineRemovalTerminalSnapshot(ctx, operation)
	if err != nil {
		return network.EngineRemovalTerminalProof{}, err
	}
	if local.ConfigID != proof.Snapshot.ConfigID || local.Decision.Hash != proof.Snapshot.Decision.Hash ||
		!bytes.Equal(local.Decision.Value, proof.Snapshot.Decision.Value) ||
		!bytes.Equal(local.Decision.Certificate, proof.Snapshot.Decision.Certificate) || local.PrefixHash != proof.Snapshot.PrefixHash {
		return network.EngineRemovalTerminalProof{}, errors.New("original-quorum terminal differs from locally retained certified history")
	}
	return proof, nil
}
