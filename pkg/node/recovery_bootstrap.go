package node

import (
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
)

// prepareEngineRepairLearnerBootstrap applies only the candidate configuration
// already pinned into the authenticated startup grant. It runs before any new
// WAL or local voter identity can be created.
func (n *Node) prepareEngineRepairLearnerBootstrap(local localVoterState, loadErr error) (localVoterState, error) {
	if n == nil || n.config == nil || n.fenceSession == nil || n.fenceSession.StartupRole() != fencing.StartupRoleLearnerPendingRemove {
		return local, loadErr
	}
	expectedWAL, replacedNode, replacedWAL := n.fenceSession.ExpectedWALIdentity()
	if expectedWAL == "" {
		return local, loadErr
	}
	if !n.config.EngineOwnedRemovals || n.config.Learner == nil || n.config.Learner.ID != n.config.NodeID ||
		!validWALIdentity(expectedWAL) || replacedNode == "" || replacedWAL == "" || !validWALIdentity(replacedWAL) || replacedNode == string(n.config.NodeID) {
		return localVoterState{}, fmt.Errorf("fencing: incomplete engine-repair learner bootstrap")
	}
	if loadErr != nil && !errors.Is(loadErr, ErrVoterWALLost) {
		return localVoterState{}, loadErr
	}
	if local.identity == nil {
		if local.hadWAL || loadErr != nil {
			return localVoterState{}, fmt.Errorf("%w: local identity is missing while WAL data remains", ErrVoterStateLost)
		}
		n.recoveryWALIdentity = expectedWAL
		return local, nil
	}
	if local.identity.Node == string(n.config.NodeID) {
		if loadErr != nil || !local.identity.Registered || !local.hadWAL || local.identity.Nonce != expectedWAL ||
			!sameVoterIdentityConfiguration(*local.identity, voterIdentityFor(n.config)) {
			return localVoterState{}, fmt.Errorf("%w: candidate identity/WAL is not intact for its assigned startup", ErrVoterStateLost)
		}
		return local, nil
	}
	if loadErr != nil && !errors.Is(loadErr, ErrVoterWALLost) || local.hadWAL || !local.identity.Registered ||
		local.identity.Node != replacedNode || local.identity.Nonce != replacedWAL {
		return localVoterState{}, fmt.Errorf("%w: local voter state is neither empty nor the exact replaced identity", ErrVoterStateLost)
	}
	oldConfig := *n.config
	oldConfig.NodeID = fencingNodeID(replacedNode)
	oldConfig.Learner = nil
	if !sameVoterIdentityConfiguration(*local.identity, voterIdentityFor(&oldConfig)) {
		return localVoterState{}, fmt.Errorf("%w: replaced local identity differs from its immutable source configuration", ErrVoterStateLost)
	}
	n.recoveryWALIdentity = expectedWAL
	return localVoterState{hadWAL: false}, nil
}

func sameVoterIdentityConfiguration(got, want voterIdentity) bool {
	got.Nonce, got.Registered = "", false
	want.Nonce, want.Registered = "", false
	return got == want
}

func validWALIdentity(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func fencingNodeID(value string) types.NodeID { return types.NodeID(value) }
