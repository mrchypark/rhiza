package operator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

const automaticOwner = "rhiza.mrchypark.dev/automatic-owner"
const automaticLogical = "rhiza.mrchypark.dev/automatic-logical"

func (c *Controller) advanceAutomatic(ctx context.Context, r *ClusterResource, state *autoControl, version *objstore.ObjectVersion, sts object) error {
	intent := state.Intent
	block := func(message string) error { return c.autoStatus(ctx, r, state, "Blocked", message) }
	if c.Fencer == nil {
		return block("fencing backend is unavailable")
	}
	if intent.Request.BindingUID != state.BindingUID || intent.Request.SourceClusterID != state.ActiveClusterID || intent.Request.StatefulSetUID != state.StatefulSetUID {
		return block("recovery intent differs from durable binding")
	}
	if intent.Proof == nil {
		// This intent is already durably reserved. An ambiguous provider result is
		// retried with the same operation, never replaced with a different decision.
		proof, err := c.fence(ctx, intent.Request)
		if err != nil {
			if errors.Is(err, ErrFencePending) {
				return c.autoStatus(ctx, r, state, "Fencing", "waiting for verified termination and recreation barrier")
			}
			return block("fencing backend did not return a valid completion proof")
		}
		intent.Proof = proof
		if err := c.saveAuto(ctx, r, state, version); err != nil {
			return err
		}
		return c.autoStatus(ctx, r, state, "Fenced", "fencing backend proof persisted")
	}
	if err := validateFenceProof(intent.Request, intent.Proof); err != nil {
		return block("persisted fencing proof is invalid")
	}
	if intent.Failed != "" && (intent.Request.Scope != ScopeVoter || len(intent.Request.Targets) != 1 || intent.Request.Targets[0].NodeID != string(intent.Failed)) {
		return block("persisted voter fencing scope is invalid")
	}
	var existing Resource
	existingErr := c.Kube.Get(ctx, c.resourcePath(intent.RecoveryName), &existing)
	if existingErr != nil && !apiNotFound(existingErr) {
		return existingErr
	}
	if intent.RecoveryUID != "" && (existingErr != nil || str(existing.Metadata["uid"]) != intent.RecoveryUID) {
		return block("journaled recovery resource was deleted or recreated")
	}
	if intent.Failed != "" && existing.Status.Phase != "Complete" {
		if handled, err := c.recoverLearnerInventory(ctx, r, state, version, sts); handled || err != nil {
			return err
		}
		handled := false
		if intent.LearnerUID != "" {
			before := hashJSON(state)
			var err error
			handled, err = c.handleAutomaticLearner(ctx, r, state, version, sts, &existing)
			if err != nil || hashJSON(state) != before {
				return err
			}
		}
		abortRevisionAttempted := false
		var abortRevisionErr error
		if intent.Aborting {
			revisionHandled, attempted, err := c.advancePendingAbortRevision(ctx, r, state, version, sts, &existing)
			if revisionHandled && !attempted {
				return err
			}
			if err != nil && !attempted {
				return err
			}
			handled = handled || revisionHandled
			abortRevisionErr = err
			abortRevisionAttempted = attempted
		}
		disposition, err := c.assessAutomaticEscalation(ctx, r, state, version, sts, &existing)
		if err != nil || disposition == automaticEscalationStop || handled {
			return errors.Join(abortRevisionErr, err)
		}
		if abortRevisionAttempted {
			return errors.Join(abortRevisionErr, c.autoStatus(ctx, r, state, "Recovering", "exact abort submission failed; bounded escalation remains active"))
		}
		if disposition == automaticEscalationWait && !intent.Aborting {
			return nil
		}
		if intent.LearnerUID == "" {
			handled, err = c.handleAutomaticLearner(ctx, r, state, version, sts, &existing)
			if handled || err != nil {
				return err
			}
		}
	}
	desired, err := automaticRecoveryDesired(r, state, intent, sts)
	if err != nil {
		return block(err.Error())
	}
	var child Resource
	err = c.Kube.Get(ctx, c.resourcePath(intent.RecoveryName), &child)
	if err != nil {
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
			return err
		}
		if intent.RecoveryUID != "" {
			return block("journaled recovery resource was deleted; refusing new execution authority")
		}
		child = Resource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaRecovery", Metadata: object{"name": intent.RecoveryName, "namespace": c.Kube.Namespace, "annotations": object{automaticOwner: state.BindingUID, automaticLogical: r.Spec.LogicalID}}, Spec: desired}
		// Automatic children cannot dispatch until their exact UID and specification
		// have been committed to the remote control record.
		if err := c.Kube.Post(ctx, c.resourcePath(""), &child, &child); err != nil {
			return err
		}
	}
	if str(nested(child.Metadata, "annotations", automaticOwner)) != state.BindingUID || str(nested(child.Metadata, "annotations", automaticLogical)) != r.Spec.LogicalID {
		return block("recovery resource differs from frozen automatic intent")
	}
	if intent.RecoveryUID == "" {
		intent.RecoveryUID = str(child.Metadata["uid"])
		intent.SpecHash = hashJSON(desired)
		if intent.RecoveryUID == "" {
			return block("recovery resource UID is unavailable")
		}
		if err := c.saveAuto(ctx, r, state, version); err != nil {
			return err
		}
		return c.autoStatus(ctx, r, state, "Recovering", "recovery child authorized by durable UID and specification")
	}
	if intent.SpecHash != hashJSON(desired) {
		if hashJSON(child.Spec) != intent.SpecHash {
			return block("recovery specification changed outside the durable intent")
		}
		intent.PreviousSpecHash = intent.SpecHash
		intent.SpecHash = hashJSON(desired)
		if err := c.saveAuto(ctx, r, state, version); err != nil {
			return err
		}
		return c.autoStatus(ctx, r, state, "Recovering", "recovery specification revision reserved")
	}
	if hashJSON(child.Spec) != intent.SpecHash {
		if hashJSON(child.Spec) != intent.PreviousSpecHash {
			return block("recovery specification differs from authorized revisions")
		}
		child.Spec = desired
		if err := c.Kube.Put(ctx, c.resourcePath(intent.RecoveryName), &child, &child); err != nil {
			return err
		}
		return c.autoStatus(ctx, r, state, "Recovering", "recovery specification revision submitted")
	}
	if str(child.Metadata["uid"]) != intent.RecoveryUID {
		return block("recovery resource UID changed")
	}
	if child.Status.Phase != "Complete" {
		return c.autoStatus(ctx, r, state, "Recovering", child.Status.Phase+": "+child.Status.Message)
	}
	var installed installedGeneration
	if err := c.verifyAutomaticCompletion(ctx, r, state, &child, sts, &installed); err != nil {
		return block("completed recovery outcome is invalid: " + err.Error())
	}
	if intent.Failed != "" {
		supervised := str(nested(sts, "spec", "template", "metadata", "annotations", fencingProtocolAnnotation)) == "1"
		if err := promoteCompletedMembershipLearner(state, intent, supervised); err != nil {
			return block("completed learner identity is unavailable: " + err.Error())
		}
	} else {
		if child.Status.Target == "" || child.Status.Stage != "Complete" {
			return block("target generation is not confirmed complete")
		}
		if err := c.releaseCompleted(ctx, &child); err != nil {
			return err
		}
		state.ActiveClusterID = installed.ClusterID
		state.Voters = append([]VoterPod(nil), installed.Voters...)
		state.Identities = installed.Identities
	}
	if intent.Proof != nil {
		for idx := range state.Learners {
			entry := &state.Learners[idx]
			if entry.SourceClusterID == intent.Request.SourceClusterID && learnerTargetCovered(entry.Target, intent.Request.Targets) {
				entry.Retirement = &learnerRetirement{Request: intent.Request, Proof: *intent.Proof}
			}
		}
	}
	now := time.Now().UTC()
	recent := []time.Time{now}
	for _, completion := range state.Completions {
		if completion.After(now.Add(-24 * time.Hour)) {
			recent = append(recent, completion)
		}
	}
	state.Completions = recent
	state.Completed = &autoCompletion{RecoveryName: intent.RecoveryName, RecoveryUID: intent.RecoveryUID, SpecHash: intent.SpecHash, FenceProofID: intent.Proof.ProofID, FenceRequestHash: intent.Proof.RequestHash, ClusterID: state.ActiveClusterID, Voters: append([]VoterPod(nil), state.Voters...), At: now}
	state.SuspectedSince = time.Time{}
	state.Intent = nil
	if err := c.saveAuto(ctx, r, state, version); err != nil {
		return err
	}
	return c.autoStatus(ctx, r, state, "Stabilizing", "recovery completed; verifying the active quorum")
}

func promoteCompletedMembershipLearner(state *autoControl, intent *autoIntent, supervised bool) error {
	entry, err := retainedLearnerCredential(state, state.ActiveClusterID, intent.Request.OperationID, intent.LearnerName, intent.LearnerUID)
	if err != nil {
		return err
	}
	learnerID, err := learnerNodeID(state, intent.LearnerName, intent.LearnerUID)
	if supervised {
		learnerID, err = capturedLearnerNodeID(state, intent.LearnerName, intent.LearnerUID)
	}
	if err != nil {
		return err
	}
	target := entry.Target
	if target.NodeID != learnerID || target.Pod != intent.LearnerName || target.PodUID != intent.LearnerUID || target.WALIdentity != intent.LearnerWAL {
		return fmt.Errorf("retained learner target differs from completed incarnation")
	}
	state.Voters = nil
	for _, ref := range intent.Voters {
		if ref.NodeID != intent.Failed {
			state.Voters = append(state.Voters, ref)
		}
	}
	state.Voters = append(state.Voters, VoterPod{NodeID: quepaxa.NodeID(learnerID), Pod: intent.LearnerName})
	delete(state.Identities, intent.Failed)
	state.Identities[quepaxa.NodeID(learnerID)] = target
	return nil
}

func automaticRecoveryDesired(r *ClusterResource, state *autoControl, intent *autoIntent, sts object) (Spec, error) {
	desired := Spec{StatefulSet: r.Spec.StatefulSet, Container: r.Spec.Container, SourceClusterID: state.ActiveClusterID, Durability: intent.Durability, RecoveryID: intent.RecoveryName, AllowDataLoss: intent.Policy.AllowDataLoss, AnchorID: r.Spec.AnchorID, Fence: Fence{RecoveryID: intent.RecoveryName, ClusterID: state.ActiveClusterID, StatefulSetUID: state.StatefulSetUID, Confirmed: true, Evidence: intent.Proof.ProofID}}
	if intent.Failed == "" {
		return desired, nil
	}
	var voters []VoterPod
	for _, ref := range intent.Voters {
		if ref.NodeID != intent.Failed {
			voters = append(voters, ref)
		}
	}
	target := intent.Request.Targets[0]
	desired.RecoveryID = ""
	desired.Fence = Fence{}
	replacementSecret := intent.LearnerName + "-credentials"
	if str(nested(sts, "spec", "template", "metadata", "annotations", fencingProtocolAnnotation)) == "1" {
		entry, err := retainedLearnerCredential(state, state.ActiveClusterID, intent.Request.OperationID, intent.LearnerName, intent.LearnerUID)
		if err != nil {
			return Spec{}, fmt.Errorf("replacement learner credential is not durably enrolled: %w", err)
		}
		replacementSecret = entry.CredentialName
	}
	desired.Membership = &MembershipSpec{AbortAddition: intent.Aborting, OperationID: intent.RecoveryName, Remove: intent.Failed, RemovedPod: target.Pod, VoterPods: voters, ReplacementPod: intent.LearnerName, ReplacementSecret: replacementSecret, Fence: MembershipFence{NodeID: intent.Failed, WALIdentity: target.WALIdentity, WorkloadUID: target.PodUID, Confirmed: true, Evidence: intent.Proof.ProofID}}
	return desired, nil
}

// advancePendingAbortRevision prioritizes the already-proven voter's exact
// metadata-CAS and child PUT before generation escalation. A failed PUT is
// observed by resource UID/spec readback, then returns to bounded escalation.
func (c *Controller) advancePendingAbortRevision(ctx context.Context, r *ClusterResource, state *autoControl, version *objstore.ObjectVersion, sts object, child *Resource) (handled, attempted bool, resultErr error) {
	intent := state.Intent
	if intent == nil || !intent.Aborting || intent.Failed == "" || child == nil {
		return false, false, nil
	}
	block := func(message string) (bool, bool, error) {
		return true, false, c.autoStatus(ctx, r, state, "Blocked", message)
	}
	if intent.RecoveryUID == "" || str(child.Metadata["uid"]) != intent.RecoveryUID {
		return block("journaled recovery resource was deleted or recreated")
	}
	if str(nested(child.Metadata, "annotations", automaticOwner)) != state.BindingUID || str(nested(child.Metadata, "annotations", automaticLogical)) != r.Spec.LogicalID {
		return block("recovery resource differs from frozen automatic intent")
	}
	desired, err := automaticRecoveryDesired(r, state, intent, sts)
	if err != nil {
		return block(err.Error())
	}
	if desired.Membership == nil || desired.Membership.OperationID != intent.RecoveryName || desired.Membership.Remove != intent.Failed || desired.Membership.ReplacementPod != intent.LearnerName || desired.Membership.Fence.Evidence != intent.Proof.ProofID {
		return block("abort revision differs from the proven voter intent")
	}
	currentHash, desiredHash := hashJSON(child.Spec), hashJSON(desired)
	if currentHash == desiredHash && intent.SpecHash == desiredHash {
		return false, false, nil
	}
	if intent.SpecHash != desiredHash {
		if currentHash != intent.SpecHash {
			return block("recovery specification changed outside the durable intent")
		}
		intent.PreviousSpecHash, intent.SpecHash = intent.SpecHash, desiredHash
		if err := c.saveAuto(ctx, r, state, version); err != nil {
			return true, false, err
		}
		return true, false, c.autoStatus(ctx, r, state, "Recovering", "exact abort specification revision reserved")
	}
	if currentHash != intent.PreviousSpecHash || intent.PreviousSpecHash == "" {
		return block("recovery specification differs from authorized abort revisions")
	}
	child.Spec = desired
	if err := c.Kube.Put(ctx, c.resourcePath(intent.RecoveryName), child, child); err != nil {
		putErr := err
		var observed Resource
		if readErr := c.Kube.Get(ctx, c.resourcePath(intent.RecoveryName), &observed); readErr != nil {
			return true, false, errors.Join(putErr, readErr)
		} else if str(observed.Metadata["uid"]) != intent.RecoveryUID || str(nested(observed.Metadata, "annotations", automaticOwner)) != state.BindingUID || str(nested(observed.Metadata, "annotations", automaticLogical)) != r.Spec.LogicalID {
			return block("recovery resource changed after abort submission")
		} else if hashJSON(observed.Spec) == desiredHash {
			return true, false, c.autoStatus(ctx, r, state, "Recovering", "abort specification accepted despite a lost response")
		} else if hashJSON(observed.Spec) != currentHash {
			return block("recovery specification changed during abort submission")
		}
		return false, true, putErr
	}
	return true, false, c.autoStatus(ctx, r, state, "Recovering", "exact abort specification revision submitted")
}

// authorizeAutomaticChild is checked by the existing executor before any side
// effect. A stale or recreated Kubernetes child cannot bypass the remote intent.
func (c *Controller) authorizeAutomaticChild(ctx context.Context, r *Resource) (bool, error) {
	owner := str(nested(r.Metadata, "annotations", automaticOwner))
	if owner == "" {
		if c.Automatic {
			var sts object
			if err := c.Kube.Get(ctx, c.stsPath(r.Spec.StatefulSet), &sts); err != nil {
				return false, err
			}
			uid := str(nested(sts, "metadata", "uid"))
			if uid == "" {
				return false, fmt.Errorf("workload UID unavailable")
			}
			bound, err := c.Bucket.Exists(ctx, path.Join(c.Prefix, "automatic-workloads", c.Kube.Namespace, uid, "binding.json"))
			if err != nil {
				return false, err
			}
			if bound {
				return false, fmt.Errorf("automatic workload requires a journal-authorized recovery child")
			}
		}
		return true, nil
	}
	logical := str(nested(r.Metadata, "annotations", automaticLogical))
	if !identifier.MatchString(logical) {
		return false, fmt.Errorf("invalid automatic recovery owner")
	}
	state, _, err := c.loadAuto(ctx, path.Join(c.Prefix, "automatic", c.Kube.Namespace, logical, "control.json"))
	if err != nil {
		return false, err
	}
	intent := state.Intent
	if state.BindingUID != owner || intent == nil || intent.Proof == nil || intent.RecoveryUID != str(r.Metadata["uid"]) || intent.RecoveryName != resourceName(r) || intent.SpecHash != hashJSON(r.Spec) {
		return false, nil
	}
	if err := validateFenceProof(intent.Request, intent.Proof); err != nil {
		return false, err
	}
	return true, nil
}

// retainedMembershipLearner returns the exact credential captured for an
// automatically authorized learner. The child spec carries the Secret name;
// this retained control record supplies the immutable Secret UID and canonical
// consensus identity that the membership executor must match.
func (c *Controller) retainedMembershipLearner(ctx context.Context, r *Resource) (learnerIncarnation, bool, error) {
	owner := str(nested(r.Metadata, "annotations", automaticOwner))
	logical := str(nested(r.Metadata, "annotations", automaticLogical))
	if owner == "" && logical == "" {
		return learnerIncarnation{}, false, nil
	}
	if owner == "" || !identifier.MatchString(logical) || r.Spec.Membership == nil {
		return learnerIncarnation{}, true, fmt.Errorf("automatic learner authority binding is incomplete")
	}
	state, _, err := c.loadAuto(ctx, path.Join(c.Prefix, "automatic", c.Kube.Namespace, logical, "control.json"))
	if err != nil {
		return learnerIncarnation{}, true, fmt.Errorf("automatic learner authority is unavailable: %w", err)
	}
	intent, membership := state.Intent, r.Spec.Membership
	if state.BindingUID != owner || state.StatefulSetUID == "" || state.ActiveClusterID != r.Spec.SourceClusterID || intent == nil || intent.Proof == nil || intent.RecoveryName != resourceName(r) || intent.RecoveryUID != str(r.Metadata["uid"]) || intent.SpecHash != hashJSON(r.Spec) || intent.Request.OperationID == "" || intent.Failed != membership.Remove || intent.LearnerName != membership.ReplacementPod || intent.LearnerUID == "" {
		return learnerIncarnation{}, true, fmt.Errorf("automatic learner authority differs from the immutable child")
	}
	entry, err := retainedLearnerCredential(state, state.ActiveClusterID, intent.Request.OperationID, intent.LearnerName, intent.LearnerUID)
	if err != nil || entry.Target.NodeID == "" || entry.Target.WALIdentity != intent.LearnerWAL || entry.CredentialName != membership.ReplacementSecret {
		return learnerIncarnation{}, true, fmt.Errorf("automatic learner credential differs from retained enrollment")
	}
	return entry, true, nil
}
