package node

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path"
	"reflect"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// ManagedRepairSnapshot is read from the live voter runtime and immutable
// voter-registration record. An empty LearnerWALIdentity means registration
// has not appeared yet; malformed or mismatched registration is an error.
type ManagedRepairSnapshot struct {
	Current            quepaxa.Cluster
	AbortSlot          quepaxa.Slot
	LocalWALIdentity   string
	LearnerWALIdentity string
}

// ManagedRepairSnapshot reads only state needed to bind a managed replacement
// plan. It does not infer learner identity from a Pod name or public key.
func (n *Node) ManagedRepairSnapshot(ctx context.Context, learnerID quepaxa.NodeID, learnerKey quepaxa.PublicKey) (ManagedRepairSnapshot, error) {
	if n == nil {
		return ManagedRepairSnapshot{}, network.ErrNotReady
	}
	n.shutdownMu.Lock()
	defer n.shutdownMu.Unlock()
	if err := n.managedRepairReady(); err != nil {
		return ManagedRepairSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return ManagedRepairSnapshot{}, err
	}
	current := n.core.CurrentCluster()
	current.Members = append([]quepaxa.Member(nil), current.Members...)
	abort, _, _ := n.core.LastReconfigurationAbort()
	result := ManagedRepairSnapshot{Current: current, AbortSlot: abort, LocalWALIdentity: n.core.WALIdentity()}
	if learnerID == "" {
		return result, nil
	}
	if learnerKey == (quepaxa.PublicKey{}) || learnerID == n.config.NodeID {
		return ManagedRepairSnapshot{}, network.ErrInvalidRequest
	}
	key := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID), "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(learnerID))))
	data, err := readVoterRegistration(ctx, n.bucket, key)
	if err != nil {
		return result, err
	}
	if data == nil {
		return result, nil
	}
	var identity voterIdentity
	if json.Unmarshal(data, &identity) != nil || identity.Version != voterIdentityVersion || !identity.Registered || identity.Cluster != string(n.config.ClusterID) || identity.Node != string(learnerID) || identity.Nonce == "" || identity.LearnerPublicKey != fmt.Sprintf("%x", learnerKey[:]) {
		return ManagedRepairSnapshot{}, network.ErrInvalidRequest
	}
	result.LearnerWALIdentity = identity.Nonce
	return result, nil
}

// ManagedRepairCompletion is returned only after the exact journal terminal
// has been retained, the runtime configuration matches its target, and
// CompleteDecision has re-established delivery/ACK completion.
type ManagedRepairCompletion struct {
	Current  quepaxa.Cluster
	Terminal quepaxa.DecidedValue
}

// VerifyManagedRepairCompletion proves an already-applied managed ADD. Status
// records and locally retained certificates alone are not completion proof.
func (n *Node) VerifyManagedRepairCompletion(ctx context.Context, request ManagedMembershipRequest) (ManagedRepairCompletion, error) {
	if n == nil || request.Action != ManagedMembershipAdd || request.Addition == nil || request.Addition.WALIdentity == "" || request.HandoffHeadHash == "" {
		return ManagedRepairCompletion{}, network.ErrInvalidRequest
	}
	n.shutdownMu.Lock()
	defer n.shutdownMu.Unlock()
	if err := n.managedRepairReady(); err != nil {
		return ManagedRepairCompletion{}, err
	}
	if err := ctx.Err(); err != nil {
		return ManagedRepairCompletion{}, err
	}
	change := request.change(string(n.config.ClusterID))
	data, err := readVoterRegistration(ctx, n.bucket, n.membershipOperationKey(change))
	if err != nil {
		return ManagedRepairCompletion{}, fmt.Errorf("read managed ADD journal: %w", err)
	}
	if len(data) == 0 {
		return ManagedRepairCompletion{}, fmt.Errorf("managed ADD journal unavailable")
	}
	var operation membershipOperation
	if json.Unmarshal(data, &operation) != nil || !reflect.DeepEqual(operation.Request, change) || operation.Request.Add == nil || *operation.Request.Add != *request.Addition {
		return ManagedRepairCompletion{}, network.ErrRequestConflict
	}
	current := n.core.CurrentCluster()
	if !reflect.DeepEqual(current, operation.Target) {
		return ManagedRepairCompletion{}, network.ErrRequestConflict
	}
	terminal, err := n.retainedMembershipTerminal(operation)
	if err != nil {
		return ManagedRepairCompletion{}, err
	}
	completed, err := n.core.CompleteDecision(ctx, terminal.Slot)
	if err != nil {
		return ManagedRepairCompletion{}, fmt.Errorf("managed ADD terminal completion: %w", err)
	}
	if completed.Slot != terminal.Slot || completed.Hash != terminal.Hash || !reflect.DeepEqual(completed.Value, terminal.Value) || !reflect.DeepEqual(completed.Certificate, terminal.Certificate) || !reflect.DeepEqual(n.core.CurrentCluster(), operation.Target) {
		return ManagedRepairCompletion{}, fmt.Errorf("managed ADD terminal or target changed")
	}
	current.Members = append([]quepaxa.Member(nil), current.Members...)
	return ManagedRepairCompletion{Current: current, Terminal: completed}, nil
}

func (n *Node) managedRepairReady() error {
	if n.config == nil || n.core == nil || n.bucket == nil || !n.opened.Load() || !n.config.EnableReconfiguration || !n.core.IsVoter() || n.managedStartup == nil {
		return network.ErrNotReady
	}
	if _, ok := n.managedStartup.Checker.(ManagedMembershipChecker); !ok {
		return network.ErrNotReady
	}
	return nil
}
