package operator

import (
	"context"
	"fmt"

	"github.com/thanos-io/objstore"
)

const maxLearnerHistory = 64

// learnerFenceTarget is the journaled incarnation of one learner. Identity that
// was captured when the learner was provisioned is never re-derived from its Pod
// name, and a caller that rebuilt the target without it would make an unequal
// FenceTarget that blocks generation-wide fencing during an outage.
func learnerFenceTarget(state *autoControl, name, uid, wal string) FenceTarget {
	target := FenceTarget{NodeID: name, Pod: name, PodUID: uid, WALIdentity: wal}
	for _, entry := range state.Learners {
		if entry.SourceClusterID == state.ActiveClusterID && entry.Target.Pod == name && entry.Target.PodUID == uid {
			target.NodeName, target.NodeUID, target.ContainerID, target.ImageID = entry.Target.NodeName, entry.Target.NodeUID, entry.Target.ContainerID, entry.Target.ImageID
		}
	}
	return target
}

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
	if len(state.Learners) >= maxLearnerHistory {
		return fmt.Errorf("learner history capacity %d reached; reconcile must compact proven terminated learners", maxLearnerHistory)
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

func learnerTargetCovered(target FenceTarget, targets []FenceTarget) bool {
	for _, covered := range targets {
		if covered == target {
			return true
		}
	}
	return false
}

// compactLearnerHistory retries every retained incarnation on each reconcile.
// An in-flight intent only protects its own targets, active voters, and current
// learner; unrelated entries remain eligible for ledger-backed compaction.
func (c *Controller) compactLearnerHistory(ctx context.Context, state *autoControl) (bool, error) {
	if err := validateLearnerHistory(state.Learners); err != nil {
		return false, err
	}
	protected := make(map[string]bool)
	for _, voter := range state.Voters {
		protected[string(voter.NodeID)] = true
		protected[voter.Pod] = true
	}
	if intent := state.Intent; intent != nil {
		for _, voter := range intent.Voters {
			protected[string(voter.NodeID)] = true
			protected[voter.Pod] = true
		}
		for _, target := range intent.Request.Targets {
			protected[target.NodeID] = true
			protected[target.Pod] = true
		}
		protected[intent.LearnerName] = true
	}
	retained := make([]learnerIncarnation, 0, len(state.Learners))
	for _, entry := range state.Learners {
		if protected[string(entry.Target.NodeID)] || protected[entry.Target.Pod] {
			retained = append(retained, entry)
			continue
		}
		if !c.learnerPodUIDGone(ctx, entry.Target.Pod, entry.Target.PodUID) {
			retained = append(retained, entry)
			continue
		}
		proven := false
		if entry.Retirement != nil && learnerTargetCovered(entry.Target, entry.Retirement.Request.Targets) && validateFenceProof(entry.Retirement.Request, &entry.Retirement.Proof) == nil {
			proven = true
		}
		if !proven && c.verifyTerminatedCleanup(ctx, state.StatefulSetUID, entry) != nil {
			retained = append(retained, entry)
			continue
		}
		if !c.retireLearnerCredential(ctx, entry) {
			retained = append(retained, entry)
			continue
		}
	}
	changed := len(retained) != len(state.Learners)
	state.Learners = retained
	if len(state.Learners) > maxLearnerHistory {
		return changed, fmt.Errorf("learner history has %d entries and cannot be compacted; restore the fencing ledger for the affected generations or remove verified completed generation learners", len(state.Learners))
	}
	return changed, nil
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

func (c *Controller) retireLearnerCredential(ctx context.Context, entry learnerIncarnation) bool {
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
	if err := c.Kube.DeleteSecret(ctx, c.Kube.Namespace, entry.Target.Pod+"-credentials", entry.CredentialUID); err != nil {
		return apiNotFound(err)
	}
	return true
}

func (c *Controller) verifyTerminatedCleanup(ctx context.Context, stsUID string, entry learnerIncarnation) error {
	ledger, err := (&FencingExecutor{Kube: c.Kube}).read(ctx, stsUID, entry.SourceClusterID)
	if err != nil {
		return err
	}
	found := false
	for _, run := range ledger.Ledger.Runs {
		if run.Identity.PodUID != entry.Target.PodUID {
			continue
		}
		found = true
		if run.Identity.NodeID != entry.Target.NodeID || run.Identity.SourceClusterID != entry.SourceClusterID || run.Stop == nil {
			return fmt.Errorf("learner run identity differs")
		}
		if entry.Target.WALIdentity == "" {
			if run.Identity.WALIdentity != "" || run.NeverBoundExit == nil || *run.NeverBoundExit != *run.Stop {
				return fmt.Errorf("learner never-bound exit is not proven")
			}
		} else if run.Identity.WALIdentity != entry.Target.WALIdentity || !run.Exited || run.Drain == nil || run.Drain.Request != *run.Stop || !run.Drain.Clean() {
			return fmt.Errorf("learner termination proof is incomplete")
		}
	}
	if !found {
		return fmt.Errorf("learner run is absent from the fencing ledger")
	}
	return nil
}

func (c *Controller) prepareLearnerProvisioning(ctx context.Context, r *ClusterResource, state *autoControl, version *objstore.ObjectVersion) (bool, error) {
	changed, err := c.compactLearnerHistory(ctx, state)
	if err != nil {
		return true, c.autoStatus(ctx, r, state, "Blocked", err.Error())
	}
	if changed {
		if err := c.saveAuto(ctx, r, state, version); err != nil {
			return true, err
		}
		return true, c.autoStatus(ctx, r, state, "Recovering", "learner history compacted before replacement provisioning")
	}
	for _, old := range state.Learners {
		if old.SourceClusterID == state.ActiveClusterID && old.Target.Pod == state.Intent.LearnerName {
			return false, nil
		}
	}
	worst := worstCaseGenerationTargets(state)
	if err := fenceCapacityError(worst); err != nil {
		return true, c.autoStatus(ctx, r, state, "Blocked", "online learner replacement cannot reserve its fallback generation fence: "+err.Error()+"; use whole-generation recovery instead")
	}
	if len(state.Learners) >= maxLearnerHistory {
		return true, c.autoStatus(ctx, r, state, "Blocked", fmt.Sprintf("learner history is full (%d) and no ledger proof can compact it; restore the fencing ledger or remove verified completed-generation learners", maxLearnerHistory))
	}
	return false, nil
}

func worstCaseGenerationTargets(state *autoControl) int {
	ids := make(map[string]bool)
	for _, voter := range state.Voters {
		ids[string(voter.NodeID)] = true
	}
	for _, entry := range state.Learners {
		if entry.SourceClusterID == state.ActiveClusterID {
			ids[string(entry.Target.NodeID)] = true
		}
	}
	if state.Intent != nil && state.Intent.LearnerName != "" {
		ids[state.Intent.LearnerName] = true
	}
	return len(ids)
}

func (c *Controller) learnerInventoryEntry(ctx context.Context, state *autoControl, sts object, pod object) (learnerIncarnation, error) {
	i := state.Intent
	name, uid := str(nested(pod, "metadata", "name")), str(nested(pod, "metadata", "uid"))
	entry := learnerIncarnation{SourceClusterID: state.ActiveClusterID, OperationID: i.Request.OperationID, Target: capturedFenceIdentity(FenceTarget{NodeID: name, Pod: name, PodUID: uid, WALIdentity: i.LearnerWAL}, pod)}
	if uid == "" || name != i.LearnerName || (i.LearnerUID != "" && uid != i.LearnerUID) {
		return entry, fmt.Errorf("learner Pod identity differs")
	}
	supervised := str(nested(sts, "spec", "template", "metadata", "annotations", fencingProtocolAnnotation)) == "1"
	if supervised {
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
	}
	var credential object
	if err := c.Kube.Get(ctx, c.corePath("secrets", name+"-credentials"), &credential); err != nil {
		return entry, err
	}
	entry.CredentialUID = str(nested(credential, "metadata", "uid"))
	if supervised && entry.CredentialUID != str(nested(pod, "metadata", "annotations", fencingCredentialAnnotation)) {
		return entry, fmt.Errorf("learner credential UID differs")
	}
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
