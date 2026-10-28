package operator

import (
	"context"
	"fmt"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
)

const maxLearnerHistory = 64

// learnerFenceTarget is the journaled incarnation of one learner. Identity that
// was captured when the learner was provisioned is never re-derived from its Pod
// name, and a caller that rebuilt the target without it would make an unequal
// FenceTarget that blocks generation-wide fencing during an outage.
func learnerFenceTarget(state *autoControl, name, uid, wal string) (FenceTarget, error) {
	for _, entry := range state.Learners {
		if entry.SourceClusterID == state.ActiveClusterID && entry.Target.Pod == name && entry.Target.PodUID == uid {
			target := entry.Target
			target.WALIdentity = wal
			return target, nil
		}
	}
	for _, target := range state.Identities {
		if target.Pod == name && target.PodUID == uid {
			target.WALIdentity = wal
			return target, nil
		}
	}
	for _, entry := range state.Learners {
		if entry.SourceClusterID == state.ActiveClusterID && entry.Target.Pod == name {
			nodeID, err := fencing.ReplacementNodeID(uid)
			if err != nil {
				return FenceTarget{}, err
			}
			return FenceTarget{NodeID: nodeID, Pod: name, PodUID: uid, WALIdentity: wal}, nil
		}
	}
	return FenceTarget{NodeID: name, Pod: name, PodUID: uid, WALIdentity: wal}, nil
}

// learnerNodeID returns the consensus identity captured for this exact learner
// incarnation. The Pod name remains the Kubernetes address; it is not a
// substitute for a UID-derived supervised NodeID.
func learnerNodeID(state *autoControl, name, uid string) (string, error) {
	target, err := learnerFenceTarget(state, name, uid, "")
	if err != nil {
		return "", err
	}
	if target.NodeID == "" || target.Pod != name || target.PodUID != uid {
		return "", fmt.Errorf("learner consensus identity is incomplete")
	}
	return target.NodeID, nil
}

func capturedLearnerNodeID(state *autoControl, name, uid string) (string, error) {
	var found string
	for _, entry := range state.Learners {
		if entry.SourceClusterID == state.ActiveClusterID && entry.Target.Pod == name && entry.Target.PodUID == uid {
			if found != "" && found != entry.Target.NodeID {
				return "", fmt.Errorf("learner incarnation has conflicting consensus identities")
			}
			found = entry.Target.NodeID
		}
	}
	if found == "" {
		return "", fmt.Errorf("learner incarnation consensus identity was not persisted")
	}
	return found, nil
}

func rememberLearner(state *autoControl, entry learnerIncarnation) error {
	if err := validateLearnerHistory(state.Learners); err != nil {
		return err
	}
	for i, old := range state.Learners {
		if old.SourceClusterID != entry.SourceClusterID || old.Target.NodeID != entry.Target.NodeID || old.Target.PodUID != entry.Target.PodUID {
			continue
		}
		if old.Target.Pod != entry.Target.Pod || old.OperationID != entry.OperationID || old.CredentialUID != entry.CredentialUID || old.CredentialName != entry.CredentialName || (old.Target.WALIdentity != "" && old.Target.WALIdentity != entry.Target.WALIdentity) || old.LedgerUID != entry.LedgerUID {
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
		if entry.SourceClusterID == "" || entry.OperationID == "" || !identifier.MatchString(entry.Target.Pod) || fencing.ValidatePodBinding(entry.Target.NodeID, entry.Target.Pod, entry.Target.PodUID) != nil {
			return fmt.Errorf("invalid learner history")
		}
		for _, old := range entries[:i] {
			if old.SourceClusterID == entry.SourceClusterID && (old.Target.NodeID == entry.Target.NodeID || old.Target.PodUID == entry.Target.PodUID) {
				return fmt.Errorf("duplicate learner history")
			}
		}
	}
	return nil
}

func retainedLearnerCredential(state *autoControl, source, operation, pod, podUID string) (learnerIncarnation, error) {
	var found *learnerIncarnation
	for _, entry := range state.Learners {
		if entry.SourceClusterID != source || entry.OperationID != operation || entry.Target.Pod != pod || entry.Target.PodUID != podUID {
			continue
		}
		if found != nil || entry.CredentialName == "" || entry.CredentialUID == "" || entry.Target.NodeID == "" {
			return learnerIncarnation{}, fmt.Errorf("learner credential history is missing or ambiguous")
		}
		copy := entry
		found = &copy
	}
	if found == nil {
		return learnerIncarnation{}, fmt.Errorf("learner credential is not in retained incarnation history")
	}
	return *found, nil
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
			if target.NodeID == entry.Target.NodeID && target.PodUID == entry.Target.PodUID {
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
		// A ledger UID is captured at the authoritative live binding. Never
		// adopt the current object UID while deciding whether old history can be
		// removed: that would bless a recreated ledger with copied contents.
		if entry.LedgerUID == "" {
			retained = append(retained, entry)
			continue
		}
		if !c.learnerPodUIDGone(ctx, entry.Target.Pod, entry.Target.PodUID) {
			retained = append(retained, entry)
			continue
		}
		if c.verifyTerminatedCleanup(ctx, state.StatefulSetUID, entry) != nil {
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
	if entry.CredentialUID == "" || entry.CredentialName == "" {
		return false
	}
	var secret object
	err := c.Kube.Get(ctx, c.corePath("secrets", entry.CredentialName), &secret)
	if apiNotFound(err) {
		return true
	}
	if err != nil {
		return false
	}
	if str(nested(secret, "metadata", "uid")) != entry.CredentialUID {
		return true
	}
	if err := c.Kube.DeleteSecret(ctx, c.Kube.Namespace, entry.CredentialName, entry.CredentialUID); err != nil {
		return apiNotFound(err)
	}
	return true
}

func (c *Controller) verifyTerminatedCleanup(ctx context.Context, stsUID string, entry learnerIncarnation) error {
	if entry.LedgerUID == "" {
		return fmt.Errorf("learner cleanup has no retained ledger UID")
	}
	executor := &FencingExecutor{Kube: c.Kube}
	ledger, ledgerUID, ledgerObject, err := executor.readWithUID(ctx, stsUID, entry.SourceClusterID)
	if err != nil {
		return err
	}
	verifiedUID, err := executor.registrationLedgerObjectUID(ledgerObject, stsUID, entry.SourceClusterID)
	if err != nil || verifiedUID != ledgerUID || ledgerUID != entry.LedgerUID {
		return fmt.Errorf("fencing: learner cleanup ledger identity changed")
	}
	return verifyLearnerTerminalRuns(ledger.Ledger, entry, nil, nil)
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
		id := state.Intent.LearnerName
		if state.Intent.LearnerUID != "" {
			if captured, err := learnerNodeID(state, state.Intent.LearnerName, state.Intent.LearnerUID); err == nil {
				id = captured
			}
		}
		ids[id] = true
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
	var registeredCredentialName, registeredCredentialUID, registeredLedgerUID string
	if supervised {
		nodeID, err := fencing.ReplacementNodeID(uid)
		if err != nil {
			return entry, err
		}
		entry.Target.NodeID = nodeID
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
		registeredCredentialName, registeredCredentialUID, registeredLedgerUID, err = (&FencingExecutor{Kube: c.Kube}).learnerCredentialBindingWithLedgerUID(ctx, pod, str(nested(sts, "metadata", "name")))
		if err != nil {
			return entry, err
		}
	}
	if supervised {
		entry.CredentialName, entry.CredentialUID, entry.LedgerUID = registeredCredentialName, registeredCredentialUID, registeredLedgerUID
	} else {
		var credential object
		if err := c.Kube.Get(ctx, c.corePath("secrets", name+"-credentials"), &credential); err != nil {
			return entry, err
		}
		entry.CredentialUID = str(nested(credential, "metadata", "uid"))
		entry.CredentialName = name + "-credentials"
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

func (c *Controller) verifyNeverBoundCleanup(ctx context.Context, request FenceRequest, proof *FenceProof, target FenceTarget, expectedLedgerUID string) error {
	if expectedLedgerUID == "" {
		return fmt.Errorf("never-bound cleanup has no retained ledger UID")
	}
	executor := &FencingExecutor{Kube: c.Kube}
	state, ledgerUID, ledgerObject, err := executor.readWithUID(ctx, request.StatefulSetUID, request.SourceClusterID)
	if err != nil {
		return err
	}
	verifiedUID, err := executor.registrationLedgerObjectUID(ledgerObject, request.StatefulSetUID, request.SourceClusterID)
	if err != nil || verifiedUID != ledgerUID || ledgerUID != expectedLedgerUID {
		return fmt.Errorf("never-bound cleanup ledger identity changed")
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
