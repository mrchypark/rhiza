package operator

import (
	"context"
	"errors"
	"fmt"

	"github.com/thanos-io/objstore"
)

var errPrunedLearnerReappeared = errors.New("terminated learner identity reappeared")

func rememberLearner(state *autoControl, entry learnerIncarnation) error {
	if err := validateLearnerHistory(state.Learners); err != nil {
		return err
	}
	current := false
	for i, old := range state.Learners {
		if old.SourceClusterID != entry.SourceClusterID || old.Target.Pod != entry.Target.Pod {
			continue
		}
		if old.Target.PodUID != entry.Target.PodUID || old.OperationID != entry.OperationID || old.CredentialUID != entry.CredentialUID || old.Target.NodeID != entry.Target.NodeID || (old.Target.WALIdentity != "" && old.Target.WALIdentity != entry.Target.WALIdentity) {
			return fmt.Errorf("learner incarnation history conflicts")
		}
		state.Learners[i] = entry
		current = true
		break
	}
	for _, old := range state.PrunedLearners {
		if learnerIdentityMatches(old, entry) {
			if !current {
				state.Learners = append(state.Learners, entry)
			}
			return errPrunedLearnerReappeared
		}
	}
	if current {
		return nil
	}
	state.Learners = append(state.Learners, entry)
	return nil
}

func learnerIdentityMatches(old, entry learnerIncarnation) bool {
	return (old.CredentialUID != "" && old.CredentialUID == entry.CredentialUID) ||
		(old.Target.PodUID != "" && old.Target.PodUID == entry.Target.PodUID) ||
		(old.SourceClusterID == entry.SourceClusterID && old.Target.Pod == entry.Target.Pod)
}

func validateLearnerHistory(entries []learnerIncarnation) error {
	for i, entry := range entries {
		if entry.SourceClusterID == "" || entry.OperationID == "" || entry.Target.PodUID == "" || entry.Target.NodeID != entry.Target.Pod || !identifier.MatchString(entry.Target.Pod) {
			return fmt.Errorf("invalid learner history")
		}
		for _, old := range entries[:i] {
			if old.SourceClusterID == entry.SourceClusterID && (old.Target.Pod == entry.Target.Pod || old.Target.PodUID == entry.Target.PodUID) {
				return fmt.Errorf("duplicate learner history")
			}
		}
	}
	return nil
}

func includeLearnerHistory(state *autoControl, request *FenceRequest) error {
	if err := validateLearnerHistory(state.Learners); err != nil {
		return err
	}
	for _, entry := range state.Learners {
		if entry.SourceClusterID != request.SourceClusterID {
			continue
		}
		found := false
		for _, target := range request.Targets {
			if target.Pod == entry.Target.Pod {
				if target != entry.Target {
					return fmt.Errorf("learner generation inventory conflicts")
				}
				found = true
			}
		}
		if !found {
			request.Targets = append(request.Targets, entry.Target)
		}
	}
	return nil
}

// pruneTerminatedLearners mutates the completed control record only. The caller
// persists it with the same CAS version as the completion, so a stale snapshot
// cannot resurrect a pruned incarnation. Only an already-completed request is
// used as proof; an in-flight intent leaves every entry untouched.
func (c *Controller) pruneTerminatedLearners(ctx context.Context, state *autoControl, completed *autoIntent) error {
	if state.Intent != nil || completed == nil || completed.Proof == nil {
		return nil
	}
	if err := validateFenceProof(completed.Request, completed.Proof); err != nil {
		return nil
	}
	if err := validateLearnerHistory(state.Learners); err != nil {
		return err
	}
	if err := validateLearnerHistory(state.PrunedLearners); err != nil {
		return err
	}
	protected := make(map[string]bool)
	for _, voters := range [][]VoterPod{state.Voters, completed.Voters} {
		for _, voter := range voters {
			protected[string(voter.NodeID)] = true
			protected[voter.Pod] = true
		}
	}
	var retained, pruned []learnerIncarnation
	for _, entry := range state.Learners {
		if protected[string(entry.Target.NodeID)] || protected[entry.Target.Pod] || !learnerCoveredByRequest(entry.Target, completed.Request.Targets) {
			retained = append(retained, entry)
			continue
		}
		if !c.learnerPodUIDGone(ctx, entry.Target.Pod, entry.Target.PodUID) || !c.learnerCredentialUnreusable(ctx, state.ActiveClusterID, completed.Proof, entry) {
			retained = append(retained, entry)
			continue
		}
		if entry.Target.WALIdentity == "" {
			if err := c.verifyNeverBoundCleanup(ctx, completed.Request, completed.Proof, entry.Target); err != nil {
				retained = append(retained, entry)
				continue
			}
		}
		pruned = append(pruned, entry)
	}
	state.Learners = retained
	for _, entry := range pruned {
		rememberPrunedLearner(state, entry)
	}
	return nil
}

func rememberPrunedLearner(state *autoControl, entry learnerIncarnation) {
	for _, old := range state.PrunedLearners {
		if old == entry {
			return
		}
	}
	state.PrunedLearners = append(state.PrunedLearners, entry)
}

func learnerCoveredByRequest(target FenceTarget, targets []FenceTarget) bool {
	for _, covered := range targets {
		if covered == target {
			return true
		}
	}
	return false
}

func (c *Controller) learnerPodUIDGone(ctx context.Context, name, uid string) bool {
	if name == "" || uid == "" {
		return false
	}
	var pod object
	err := c.Kube.Get(ctx, c.corePath("pods", name), &pod)
	if apiNotFound(err) {
		return true
	}
	return err == nil && str(nested(pod, "metadata", "uid")) != uid
}

func (c *Controller) learnerCredentialUnreusable(ctx context.Context, activeCluster string, proof *FenceProof, entry learnerIncarnation) bool {
	if entry.CredentialUID == "" {
		return false
	}
	var secret object
	err := c.Kube.Get(ctx, c.corePath("secrets", entry.Target.Pod+"-credentials"), &secret)
	if apiNotFound(err) {
		return true
	}
	if err != nil {
		return false
	}
	if str(nested(secret, "metadata", "uid")) != entry.CredentialUID {
		return true
	}
	// A completed generation replacement supersedes credentials from the prior
	// cluster while its recreation barrier remains in force.
	return activeCluster != entry.SourceClusterID && proof.ProcessesTerminated && proof.RecreationBlocked
}

func (c *Controller) learnerInventoryEntry(ctx context.Context, state *autoControl, sts object, pod object) (learnerIncarnation, error) {
	i := state.Intent
	name, uid := str(nested(pod, "metadata", "name")), str(nested(pod, "metadata", "uid"))
	entry := learnerIncarnation{SourceClusterID: state.ActiveClusterID, OperationID: i.Request.OperationID, Target: FenceTarget{NodeID: name, Pod: name, PodUID: uid, WALIdentity: i.LearnerWAL}}
	if uid == "" || name != i.LearnerName || (i.LearnerUID != "" && uid != i.LearnerUID) {
		return entry, fmt.Errorf("learner Pod identity differs")
	}
	if str(nested(sts, "spec", "template", "metadata", "annotations", fencingProtocolAnnotation)) != "1" {
		return entry, nil
	}
	refs := list(nested(pod, "metadata", "ownerReferences"))
	if len(refs) != 1 {
		return entry, fmt.Errorf("learner owner differs")
	}
	ref := asObject(refs[0])
	if ref["controller"] != false || ref["apiVersion"] != "apps/v1" || ref["kind"] != "StatefulSet" || ref["name"] != nested(sts, "metadata", "name") || ref["uid"] != state.StatefulSetUID || nested(sts, "metadata", "uid") != state.StatefulSetUID || nested(pod, "metadata", "annotations", fencingRootAnnotation) != state.BindingUID || nested(pod, "metadata", "annotations", ownerAnnotation) != i.Request.OperationID || nested(pod, "metadata", "deletionTimestamp") != nil {
		return entry, fmt.Errorf("learner creation binding differs")
	}
	if err := validateSupervisedPod(pod); err != nil {
		return entry, err
	}
	if err := (&FencingExecutor{Kube: c.Kube}).validateLearnerBinding(ctx, pod, str(nested(sts, "metadata", "name"))); err != nil {
		return entry, err
	}
	entry.CredentialUID = str(nested(pod, "metadata", "annotations", fencingCredentialAnnotation))
	if entry.CredentialUID == "" {
		return entry, fmt.Errorf("learner credential UID unavailable")
	}
	return entry, nil
}

// A Pod creation can commit before the control CAS response. Recover that exact
// incarnation before escalation reserves an immutable generation request.
func (c *Controller) recoverLearnerInventory(ctx context.Context, r *ClusterResource, state *autoControl, version *objstore.ObjectVersion, sts object) (bool, error) {
	i := state.Intent
	if i.LearnerName == "" {
		return false, nil
	}
	seen := false
	for _, old := range state.Learners {
		if old.SourceClusterID != state.ActiveClusterID || old.Target.Pod != i.LearnerName {
			continue
		}
		if seen || old.Target.PodUID != i.LearnerUID || old.Target.WALIdentity != i.LearnerWAL || old.OperationID != i.Request.OperationID {
			return true, c.autoStatus(ctx, r, state, "Blocked", "learner history conflicts")
		}
		seen = true
	}
	if seen || (i.LearnerUID != "" && str(nested(sts, "spec", "template", "metadata", "annotations", fencingProtocolAnnotation)) != "1") {
		return false, nil
	}
	var pod object
	if err := c.Kube.Get(ctx, c.corePath("pods", i.LearnerName), &pod); err != nil {
		if apiNotFound(err) {
			return false, nil
		}
		return true, err
	}
	entry, err := c.learnerInventoryEntry(ctx, state, sts, pod)
	if err != nil {
		return true, c.autoStatus(ctx, r, state, "Blocked", err.Error())
	}
	if err := rememberLearner(state, entry); err != nil {
		if errors.Is(err, errPrunedLearnerReappeared) {
			if saveErr := c.saveAuto(ctx, r, state, version); saveErr != nil {
				return true, saveErr
			}
			return true, c.autoStatus(ctx, r, state, "Blocked", err.Error())
		}
		return true, err
	}
	i.LearnerUID = entry.Target.PodUID
	return true, c.saveAuto(ctx, r, state, version)
}

func (c *Controller) verifyNeverBoundCleanup(ctx context.Context, request FenceRequest, proof *FenceProof, target FenceTarget) error {
	state, err := (&FencingExecutor{Kube: c.Kube}).read(ctx, request.StatefulSetUID, request.SourceClusterID)
	if err != nil {
		return err
	}
	hash, err := requestHash(&request)
	if err != nil {
		return err
	}
	if proof.ProofID != "cooperative-"+hashJSON(state) || !state.Ledger.CompleteNodes(request.OperationID, hash, nil, []string{target.PodUID}) {
		return fmt.Errorf("never-bound cleanup lacks exact complete ledger proof")
	}
	found := false
	for _, run := range state.Ledger.Runs {
		if run.Identity.PodUID != target.PodUID {
			continue
		}
		if run.Identity.NodeID != target.NodeID || run.Identity.SourceClusterID != request.SourceClusterID || run.Identity.WALIdentity != "" || run.NeverBoundExit == nil {
			return fmt.Errorf("learner was bound or has unknown startup history")
		}
		found = true
	}
	if !found {
		return fmt.Errorf("never-bound learner is absent from ledger")
	}
	return nil
}
