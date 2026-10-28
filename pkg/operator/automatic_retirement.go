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
	retained, managed, err := c.retainedMembershipLearner(ctx, r)
	if err != nil {
		return network.MembershipChange{}, err
	}
	member, uid, version, err := c.replacementMember(ctx, r.Spec.Membership.ReplacementSecret)
	if err != nil || member.ID != add.MemberID || uid != add.SecretUID || version != add.SecretResourceVer {
		return network.MembershipChange{}, fmt.Errorf("retiring addition credentials differ from the journal")
	}
	if managed && (r.Spec.Membership.ReplacementSecret != retained.CredentialName || uid != retained.CredentialUID || string(member.ID) != retained.Target.NodeID) {
		return network.MembershipChange{}, fmt.Errorf("retiring addition credential differs from retained learner enrollment")
	}
	member.WALIdentity = add.WALIdentity
	request := network.MembershipChange{OperationID: membershipOperationID(r, "add"), ClusterID: r.Spec.SourceClusterID, ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot, Add: &member}
	if add.OperationID != request.OperationID || add.WALIdentity == "" || add.RequestHash != hashJSON(request) {
		return network.MembershipChange{}, fmt.Errorf("retiring addition does not match the immutable request")
	}
	return request, nil
}
