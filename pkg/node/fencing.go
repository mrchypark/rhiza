package node

import (
	"context"
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza/pkg/network"
)

// TerminalDrainResult identifies the live engine incarnation whose work joined.
// It is not a process termination or Kubernetes recreation-barrier proof.
type TerminalDrainResult struct {
	ClusterID    string `json:"clusterID"`
	NodeID       string `json:"nodeID"`
	WALIdentity  string `json:"walIdentity"`
	StorageKnown bool   `json:"storageKnown"`
}

// TerminalDrain irreversibly shuts down the engine, then verifies every tracked
// store mutation completed with a known outcome. The supervisor must bind this
// result to its registered Pod/process incarnation before recording a receipt.
func (n *Node) TerminalDrain(ctx context.Context, expected network.TerminalDrainRequest) (TerminalDrainResult, error) {
	if err := ctx.Err(); err != nil {
		return TerminalDrainResult{}, err
	}
	n.terminalMu.Lock()
	identity := n.terminalIdentity
	if n.terminalDone == nil {
		if !n.shutdownMu.TryLock() {
			n.terminalMu.Unlock()
			return TerminalDrainResult{}, fmt.Errorf("node shutdown is already in progress")
		}
		if !n.opened.Load() || n.core == nil || n.bucket == nil || n.config == nil {
			n.shutdownMu.Unlock()
			n.terminalMu.Unlock()
			return TerminalDrainResult{}, fmt.Errorf("terminal drain requires a live engine with a lifetime mutation tracker")
		}
		identity = network.TerminalDrainRequest{ClusterID: string(n.config.ClusterID), NodeID: string(n.config.NodeID), WALIdentity: n.core.WALIdentity()}
	}
	if expected.ClusterID == "" || expected.NodeID == "" || expected.WALIdentity == "" || expected != identity {
		if n.terminalDone == nil {
			n.shutdownMu.Unlock()
		}
		n.terminalMu.Unlock()
		return TerminalDrainResult{}, fmt.Errorf("terminal drain identity mismatch")
	}
	if n.terminalDone == nil {
		n.terminalRequested.Store(true)
		n.terminalIdentity = identity
		n.terminalResult = TerminalDrainResult{ClusterID: identity.ClusterID, NodeID: identity.NodeID, WALIdentity: identity.WALIdentity}
		n.terminalDone = make(chan struct{})
		go func() {
			defer close(n.terminalDone)
			defer n.shutdownMu.Unlock()
			bucket := n.bucket
			n.terminalErr = n.shutdownEngine(false)
			// Shutdown may flush certified decisions. Stop mutation admission only
			// after those writers join, before closing the provider transport.
			drainErr := bucket.DrainMutations(context.Background())
			n.terminalErr = errors.Join(n.terminalErr, drainErr, bucket.Close())
			n.bucket = nil
			n.terminalResult.StorageKnown = n.terminalErr == nil
		}()
	}
	done := n.terminalDone
	n.terminalMu.Unlock()
	select {
	case <-ctx.Done():
		return TerminalDrainResult{}, ctx.Err()
	case <-done:
		return n.terminalResult, n.terminalErr
	}
}
