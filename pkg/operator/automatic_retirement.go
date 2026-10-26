package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"reflect"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// A retired learner may already be a voter. Confirm the immutable operation
// and authenticated surviving configuration without contacting that learner.
func (c *Controller) confirmRetiredMembershipCommit(ctx context.Context, r *Resource, statuses map[quepaxa.NodeID]network.MembershipStatus, current network.MembershipStatus) error {
	add := r.Status.Membership.Add
	request, err := c.retiringMembershipRequest(ctx, r)
	if err != nil {
		return err
	}
	member := *request.Add
	if r.Status.Membership.RemoveRequest == nil || r.Status.Membership.RemoveRequest.ExpectedConfigID+1 != add.ExpectedConfigID {
		return fmt.Errorf("committed addition does not follow the journaled removal")
	}
	var voters []quepaxa.NodeID
	for _, ref := range r.Spec.Membership.VoterPods {
		voters = append(voters, ref.NodeID)
	}
	voters = append(voters, member.ID)
	confirmed := 0
	for id, status := range statuses {
		if id != r.Spec.Membership.Remove && status.NodeID == id && status.ClusterID == r.Spec.SourceClusterID && status.Voting && !status.Pending && status.AbortSlot == 0 && status.ConfigID == add.ExpectedConfigID+1 && containsID(voters, id) && sameIDs(status.Voters, voters) {
			confirmed++
		}
	}
	if confirmed < len(voters)/2+1 || current.Pending || current.AbortSlot != 0 || !sameIDs(current.Voters, voters) {
		return fmt.Errorf("committed addition is not confirmed by the surviving configuration")
	}
	name := fmt.Sprintf("%d.json", add.ExpectedConfigID)
	if add.ExpectedAbortSlot != 0 {
		name = fmt.Sprintf("%d-abort-%d.json", add.ExpectedConfigID, add.ExpectedAbortSlot)
	}
	reader, err := c.Bucket.Get(ctx, path.Join(c.Prefix, r.Spec.SourceClusterID, "membership", name))
	if err != nil {
		return fmt.Errorf("committed addition operation is unavailable")
	}
	data, readErr := io.ReadAll(io.LimitReader(reader, 64<<10))
	closeErr := reader.Close()
	var operation struct {
		Request network.MembershipChange `json:"request"`
		Target  quepaxa.Cluster          `json:"target"`
	}
	if readErr != nil || closeErr != nil || len(data) == 64<<10 || json.Unmarshal(data, &operation) != nil || !reflect.DeepEqual(operation.Request, request) || operation.Target.ConfigID != current.ConfigID {
		return fmt.Errorf("committed addition operation differs from the journal")
	}
	var targetIDs []quepaxa.NodeID
	matched := false
	for _, target := range operation.Target.Members {
		targetIDs = append(targetIDs, target.ID)
		if reflect.DeepEqual(target, member) {
			matched = true
		}
	}
	if !matched || !sameIDs(targetIDs, voters) {
		return fmt.Errorf("committed addition target differs from the surviving configuration")
	}
	return nil
}

func (c *Controller) retiringMembershipRequest(ctx context.Context, r *Resource) (network.MembershipChange, error) {
	add := r.Status.Membership.Add
	member, uid, version, err := c.replacementMember(ctx, r.Spec.Membership.ReplacementSecret)
	if err != nil || member.ID != add.MemberID || uid != add.SecretUID || version != add.SecretResourceVer {
		return network.MembershipChange{}, fmt.Errorf("retiring addition credentials differ from the journal")
	}
	member.WALIdentity = add.WALIdentity
	request := network.MembershipChange{OperationID: membershipOperationID(r, "add"), ClusterID: r.Spec.SourceClusterID, ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot, Add: &member}
	if add.OperationID != request.OperationID || add.WALIdentity == "" || add.RequestHash != hashJSON(request) {
		return network.MembershipChange{}, fmt.Errorf("retiring addition does not match the immutable request")
	}
	return request, nil
}

// historicalRetiringMembershipRequest validates an ADD journal against the
// immutable credential and retained retirement evidence of an older learner.
// It never dispatches the ADD; callers may only use the returned request for
// an idempotent /membership/abort confirmation.
func (c *Controller) historicalRetiringMembershipRequest(ctx context.Context, r *Resource, state *autoControl) (network.MembershipChange, error) {
	if state == nil || state.Intent == nil || r.Status.Membership == nil || r.Status.Membership.Add == nil {
		return network.MembershipChange{}, fmt.Errorf("historical addition context is unavailable")
	}
	add := r.Status.Membership.Add
	if add.OperationID != membershipOperationID(r, "add") || add.MemberID == "" || add.WALIdentity == "" || add.RequestHash == "" || add.SecretUID == "" || add.SecretResourceVer == "" {
		return network.MembershipChange{}, fmt.Errorf("historical addition journal is incomplete")
	}
	if err := validateLearnerHistory(state.Learners); err != nil {
		return network.MembershipChange{}, fmt.Errorf("historical learner inventory is invalid")
	}
	var entry *learnerIncarnation
	for i := range state.Learners {
		candidate := &state.Learners[i]
		if candidate.SourceClusterID != state.ActiveClusterID || candidate.Target.NodeID != string(add.MemberID) {
			continue
		}
		if entry != nil {
			return network.MembershipChange{}, fmt.Errorf("historical addition has ambiguous learner history")
		}
		entry = candidate
	}
	if entry == nil || entry.OperationID != state.Intent.Request.OperationID || entry.CredentialName == "" || entry.CredentialUID != add.SecretUID || entry.Target.WALIdentity != add.WALIdentity || entry.Retirement == nil {
		return network.MembershipChange{}, fmt.Errorf("historical addition does not match retained learner history")
	}
	retirement := entry.Retirement
	req, proof := retirement.Request, &retirement.Proof
	retirementOperationMatches := false
	for priorAttempt := 0; priorAttempt < state.Intent.LearnerAttempt && priorAttempt < 3; priorAttempt++ {
		if req.OperationID == fmt.Sprintf("%s-learner-fence-%d", state.Intent.RecoveryName, priorAttempt) {
			retirementOperationMatches = true
			break
		}
	}
	if req.LogicalID != state.Intent.Request.LogicalID || req.BindingUID != state.BindingUID || req.Namespace != c.Kube.Namespace ||
		req.StatefulSet != state.Intent.Request.StatefulSet || req.StatefulSetUID != state.StatefulSetUID ||
		req.SourceClusterID != entry.SourceClusterID || !retirementOperationMatches || req.Scope != ScopeVoter || len(req.Targets) != 1 || req.Targets[0] != entry.Target ||
		validateFenceProof(req, proof) != nil || proofCoversLearner(req, proof, entry.Target) != nil {
		return network.MembershipChange{}, fmt.Errorf("historical learner retirement proof does not bind the journaled addition")
	}
	var secret object
	if err := c.Kube.Get(ctx, c.corePath("secrets", entry.CredentialName), &secret); err != nil {
		return network.MembershipChange{}, fmt.Errorf("historical learner credentials are unavailable")
	}
	member, uid, version, err := replacementMemberFromSecret(secret)
	if err != nil || uid != add.SecretUID || version != add.SecretResourceVer ||
		str(nested(secret, "metadata", "name")) != entry.CredentialName ||
		str(nested(secret, "metadata", "annotations", ownerAnnotation)) != entry.OperationID ||
		string(member.ID) != string(add.MemberID) || member.PublicKey == (quepaxa.PublicKey{}) {
		return network.MembershipChange{}, fmt.Errorf("historical learner credentials differ from the journal")
	}
	member.WALIdentity = add.WALIdentity
	request := network.MembershipChange{OperationID: add.OperationID, ClusterID: r.Spec.SourceClusterID, ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot, Add: &member}
	if request.ClusterID != entry.SourceClusterID || add.RequestHash != hashJSON(request) {
		return network.MembershipChange{}, fmt.Errorf("historical addition request hash differs from immutable credentials")
	}
	return request, nil
}
