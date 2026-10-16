package operator

import (
	"context"
	"fmt"

	"github.com/thanos-io/objstore"
)

func rememberLearner(state *autoControl, entry learnerIncarnation) error {
	if err := validateLearnerHistory(state.Learners); err != nil {
		return err
	}
	for i, old := range state.Learners {
		if old.SourceClusterID != entry.SourceClusterID || old.Target.Pod != entry.Target.Pod {
			continue
		}
		if old.Target.PodUID != entry.Target.PodUID || old.OperationID != entry.OperationID || old.CredentialUID != entry.CredentialUID || old.Target.NodeID != entry.Target.NodeID || (old.Target.WALIdentity != "" && old.Target.WALIdentity != entry.Target.WALIdentity) {
			return fmt.Errorf("learner incarnation history conflicts")
		}
		state.Learners[i] = entry
		return nil
	}
	state.Learners = append(state.Learners, entry)
	return nil
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
