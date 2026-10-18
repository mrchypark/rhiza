package operator

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// Completion authenticates the executor's durable result, not current health.
// A later outage must be evaluated against the installed generation/inventory.
func (c *Controller) verifyAutomaticCompletion(ctx context.Context, r *ClusterResource, state *autoControl, child *Resource, sts object) error {
	i := state.Intent
	if i.RecoveryUID == "" || resourceName(child) != i.RecoveryName || str(child.Metadata["uid"]) != i.RecoveryUID || hashJSON(child.Spec) != i.SpecHash || str(nested(child.Metadata, "annotations", automaticOwner)) != state.BindingUID || str(nested(child.Metadata, "annotations", automaticLogical)) != r.Spec.LogicalID {
		return fmt.Errorf("child identity or authorized specification differs")
	}
	if i.Failed != "" {
		m, journal := child.Spec.Membership, child.Status.Membership
		if m == nil || journal == nil || journal.Phase != "Complete" || journal.Add == nil || journal.RemoveRequest == nil || i.LearnerUID == "" || i.LearnerWAL == "" {
			return fmt.Errorf("committed membership journal or learner identity is missing")
		}
		remove := journal.RemoveRequest
		if remove.OperationID != membershipOperationID(child, "remove") || remove.ClusterID != state.ActiveClusterID || remove.Remove != i.Failed || !sameFence(remove.Fence, m.Fence) {
			return fmt.Errorf("committed removal differs from fenced intent")
		}
		member, uid, rv, err := c.replacementMember(ctx, m.ReplacementSecret)
		if err != nil {
			return err
		}
		member.WALIdentity = i.LearnerWAL
		add := journal.Add
		request := network.MembershipChange{OperationID: membershipOperationID(child, "add"), ClusterID: state.ActiveClusterID, ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot, Add: &member}
		if string(member.ID) != i.LearnerName || add.MemberID != member.ID || add.WALIdentity != i.LearnerWAL || add.OperationID != request.OperationID || add.RequestHash != hashJSON(request) || add.SecretUID != uid || add.SecretResourceVer != rv {
			return fmt.Errorf("committed addition differs from authenticated learner")
		}
		return nil
	}
	if child.Status.Stage != "Complete" || child.Status.Source != state.ActiveClusterID || child.Status.StatefulSetUID != state.StatefulSetUID || child.Status.RecoveryID != child.Spec.RecoveryID || child.Status.SpecHash != immutableSpecHash(child.Spec) || child.Status.Target != "rhiza-r-"+shortID(c.operationID(child)) || child.Status.SecretName != "rhiza-recovery-"+shortID(c.operationID(child)) || child.Status.RecoveredTip == 0 || child.Status.ManifestHash == "" {
		return fmt.Errorf("generation completion journal differs from reserved operation")
	}
	if err := c.verifyReservation(ctx, child); err != nil {
		return err
	}
	members, err := c.targetCredentials(ctx, child, child.Status.SecretName, nil)
	if err != nil {
		return err
	}
	registered, err := c.sourceMembership(ctx, path.Join(c.Prefix, child.Status.Target))
	if err != nil || registered != recovery.NewMembershipRecord(child.Status.Target, members, child.Spec.Durability) {
		return fmt.Errorf("installed target membership differs from reserved credentials")
	}
	var lineage struct {
		Version          int                       `json:"version"`
		OperationID      string                    `json:"operation_id"`
		Source           string                    `json:"source"`
		SourceDurability string                    `json:"source_durability"`
		SourceMembership string                    `json:"source_membership"`
		TargetMembership recovery.MembershipRecord `json:"target_membership"`
		Fork             recovery.ForkResult       `json:"fork"`
	}
	reader, err := c.Bucket.Get(ctx, path.Join(c.Prefix, child.Status.Target, "recovery/generation.json"))
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(io.LimitReader(reader, 1<<20))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&lineage)
	trailingErr := decoder.Decode(new(any))
	closeErr := reader.Close()
	if decodeErr != nil || trailingErr != io.EOF || closeErr != nil || lineage.Version != 1 || lineage.OperationID != c.operationID(child) || lineage.Source != child.Spec.SourceClusterID || lineage.SourceDurability != child.Status.SourceDurability || lineage.SourceMembership != child.Status.SourceMembership || lineage.TargetMembership != registered || lineage.Fork.Tip != child.Status.RecoveredTip || lineage.Fork.ManifestHash != child.Status.ManifestHash || lineage.Fork.PrefixHash == "" {
		return fmt.Errorf("immutable generation lineage differs from completed fork")
	}
	source := recovery.NewManager(c.Bucket, path.Join(c.Prefix, child.Spec.SourceClusterID), 1)
	defer source.Close()
	if err := source.Load(ctx); err != nil || !source.Sealed() || uint64(source.Tip()) != child.Status.RecoveredTip {
		return fmt.Errorf("source is not sealed at the completed fork tip")
	}
	if child.Spec.AnchorID != "" {
		targetPrefix := path.Join(c.Prefix, child.Status.Target)
		anchor, anchorHash, err := recovery.ReadGenerationAnchor(ctx, c.Bucket, targetPrefix)
		if err != nil {
			return err
		}
		if _, err := recovery.VerifyGenerationAnchor(ctx, c.Bucket, targetPrefix, registered, anchorHash); err != nil {
			return err
		}
		if anchor.SourcePrefix != path.Join(c.Prefix, child.Spec.SourceClusterID) || anchor.OperationID != c.operationID(child) || anchor.SourceTip != child.Status.RecoveredTip || anchor.SourceManifest != child.Status.ManifestHash {
			return fmt.Errorf("completed generation anchor differs from reserved operation")
		}
		result := recovery.ForkResult{Tip: anchor.SourceTip, ManifestHash: anchor.SourceManifest, PrefixHash: anchor.SourcePrefixHash}
		if err := c.verifyAnchorRequest(ctx, child, sourceState{prefix: anchor.SourcePrefix}, registered, result, hex.EncodeToString(anchorHash[:])); err != nil {
			return err
		}
	}
	ctr, err := container(sts, r.Spec.Container)
	if err != nil {
		return err
	}
	env, err := c.environment(ctx, ctr)
	if err != nil {
		return err
	}
	actual, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
	if err != nil || env["RHIZA_CLUSTER_ID"] != child.Status.Target || env["RHIZA_OBJSTORE_DURABILITY"] != child.Spec.Durability || recovery.NewMembershipRecord(child.Status.Target, actual, child.Spec.Durability) != registered {
		return fmt.Errorf("installed workload differs from completed generation")
	}
	if replicas := int(number(nested(sts, "spec", "replicas"))); replicas != len(members) {
		return fmt.Errorf("installed target StatefulSet has %d desired replicas but the recovered generation has %d voters", replicas, len(members))
	}
	allowed, ready := map[string]bool{}, map[string]bool{}
	for _, member := range members {
		allowed[string(member.ID)] = true
	}
	for _, peer := range child.Status.Peers {
		if allowed[peer.NodeID] && peer.ClusterID == child.Status.Target && peer.Durability == child.Spec.Durability && peer.Ready && peer.Quorum && peer.CertifiedTip >= child.Status.RecoveredTip && peer.AppliedTip >= child.Status.RecoveredTip {
			ready[peer.NodeID] = true
		}
	}
	if len(ready) < len(members)/2+1 {
		return fmt.Errorf("executor did not record the recovered target quorum")
	}
	inventory, podNames := map[string]string{}, map[string]bool{}
	for _, ref := range r.Spec.VoterPods {
		id := string(ref.NodeID)
		if !allowed[id] || inventory[id] != "" || !identifier.MatchString(ref.Pod) || podNames[ref.Pod] {
			return fmt.Errorf("installed target voter inventory differs from frozen binding")
		}
		inventory[id], podNames[ref.Pod] = ref.Pod, true
	}
	if len(inventory) != len(members) {
		return fmt.Errorf("installed target voter inventory is incomplete")
	}
	seen, pods, uids := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, identity := range child.Status.TargetIdentities {
		if !allowed[identity.NodeID] || inventory[identity.NodeID] != identity.Pod || seen[identity.NodeID] || !identifier.MatchString(identity.Pod) || pods[identity.Pod] || identity.PodUID == "" || uids[identity.PodUID] || identity.WALIdentity == "" {
			return fmt.Errorf("installed target identity inventory is incomplete or conflicting")
		}
		seen[identity.NodeID], pods[identity.Pod], uids[identity.PodUID] = true, true, true
	}
	// A durable record written before capture required every voter may name only
	// the identities it authenticated. Such a deficit is accepted here and
	// answered by the escalation rule: an unauthenticated voter can never be
	// removed one-for-one, so the next incident must fence the whole generation.
	return nil
}

func (c *Controller) captureAutomaticTargetIdentities(ctx context.Context, child *Resource, sts object, members []quepaxa.Member, admin string) ([]FenceTarget, error) {
	if admin == "" || str(nested(sts, "metadata", "uid")) != child.Status.StatefulSetUID {
		return nil, fmt.Errorf("target workload or admin credential is unavailable")
	}
	pods, err := c.ownedPods(ctx, child.Status.StatefulSetUID)
	if err != nil {
		return nil, err
	}
	allowed := map[quepaxa.NodeID]bool{}
	memberIDs := make([]quepaxa.NodeID, 0, len(members))
	for _, member := range members {
		allowed[member.ID] = true
		memberIDs = append(memberIDs, member.ID)
	}
	identities := map[quepaxa.NodeID]FenceTarget{}
	for _, pod := range pods {
		if nested(pod, "metadata", "deletionTimestamp") != nil {
			continue
		}
		endpoint, err := podEndpoint(pod, child.Spec.Container, "/membership/status")
		if err != nil {
			continue
		}
		status, err := c.membershipStatus(ctx, endpoint, admin)
		if err != nil {
			if errors.Is(err, ErrMembershipUnauthorized) || ctx.Err() != nil {
				return nil, err
			}
			continue
		}
		if !allowed[status.NodeID] || status.ClusterID != child.Status.Target || !status.Voting || status.Pending || status.WALIdentity == "" || !sameIDs(status.Voters, memberIDs) || identities[status.NodeID].NodeID != "" {
			return nil, fmt.Errorf("target voter identity differs from installed membership")
		}
		name, uid := str(nested(pod, "metadata", "name")), str(nested(pod, "metadata", "uid"))
		var current object
		if err := c.Kube.Get(ctx, c.corePath("pods", name), &current); err != nil {
			return nil, err
		}
		if !identifier.MatchString(name) || uid == "" || uid != str(nested(current, "metadata", "uid")) || nested(pod, "status", "podIP") != nested(current, "status", "podIP") || nested(current, "metadata", "deletionTimestamp") != nil || !ownedBy(current, child.Status.StatefulSetUID) {
			return nil, fmt.Errorf("target Pod changed during identity authentication")
		}
		identities[status.NodeID] = FenceTarget{NodeID: string(status.NodeID), Pod: name, PodUID: uid, WALIdentity: status.WALIdentity}
	}
	result := make([]FenceTarget, 0, len(members))
	for _, member := range members {
		if identity, ok := identities[member.ID]; ok {
			result = append(result, identity)
		}
	}
	// A partial inventory would become the next generation's fencing scope and
	// could never name the missing configured voters again.
	if len(result) != len(members) {
		return nil, fmt.Errorf("target identity inventory covers %d of %d configured voters", len(result), len(members))
	}
	return result, nil
}
