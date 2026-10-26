package operator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// BootstrapGenesisFenceConfirmation is returned only by the protected
// read-only fencer confirmation route. It carries the persisted fence request
// alongside its canonical proof so the DATA-side authorization cannot treat a
// locally stored proof-shaped object as authority.
type BootstrapGenesisFenceConfirmation struct {
	Request FenceRequest `json:"request"`
	Proof   FenceProof   `json:"proof"`
}

// BootstrapGenesisFenceConfirmer confirms an already-persisted bootstrap
// fence. Implementations must not reconcile, dispatch, settle, or otherwise
// mutate source state while answering this query.
type BootstrapGenesisFenceConfirmer interface {
	ConfirmBootstrapGenesisFence(context.Context, FenceRequest) (BootstrapGenesisFenceConfirmation, error)
}

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
	if state.BootstrapGenesis != nil {
		if intent.Request.BootstrapGenesis == nil || *intent.Request.BootstrapGenesis != *state.BootstrapGenesis ||
			intent.Request.Scope != ScopeGeneration || intent.Durability != "async" || !intent.Policy.AllowDataLoss {
			return block("bootstrap genesis intent differs from its protected binding")
		}
	} else if intent.Request.BootstrapGenesis != nil {
		return block("bootstrap genesis request has no durable automatic binding")
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
	desired := automaticRecoveryChildSpec(r, state, intent)
	desiredHash := hashJSON(desired)
	if intent.RecoveryUID == "" && existingErr == nil &&
		(intent.SpecHash == "" || intent.SpecHash == desiredHash) &&
		resourceName(&existing) == intent.RecoveryName && str(existing.Metadata["uid"]) != "" &&
		str(nested(existing.Metadata, "annotations", automaticOwner)) == state.BindingUID &&
		str(nested(existing.Metadata, "annotations", automaticLogical)) == r.Spec.LogicalID &&
		hashJSON(existing.Spec) == desiredHash {
		intent.RecoveryUID = str(existing.Metadata["uid"])
		intent.SpecHash = desiredHash
		if intent.LearnerPodName != "" {
			assignment, err := sameOrdinalAssignmentDigest(state, intent)
			if err != nil {
				return block("same-ordinal assignment binding is incomplete")
			}
			intent.LearnerAssignmentID = assignment
		}
		if err := c.saveAuto(ctx, r, state, version); err != nil {
			return err
		}
		return c.autoStatus(ctx, r, state, "Recovering", "existing exact recovery child authorized by durable UID and specification")
	}
	if intent.Failed != "" && existing.Status.Phase != "Complete" {
		if handled, err := c.recoverLearnerInventory(ctx, r, state, version, sts); handled || err != nil {
			return err
		}
		// A live learner with a reserved UID cannot pass startup until its exact
		// child revision is persisted below. Keep quorum assessment ahead of child
		// execution, but defer its health gate.
		deferLearnerHealth := intent.LearnerUID != "" && !intent.Aborting && hashJSON(existing.Spec) != hashJSON(desired)
		if deferLearnerHealth {
			var learnerPod object
			err := c.Kube.Get(ctx, c.corePath("pods", learnerPhysicalPod(intent)), &learnerPod)
			if err != nil && !apiNotFound(err) {
				return err
			}
			deferLearnerHealth = err == nil && str(nested(learnerPod, "metadata", "uid")) == intent.LearnerUID && nested(learnerPod, "metadata", "deletionTimestamp") == nil
		}
		handled := false
		if !deferLearnerHealth && intent.LearnerUID != "" {
			before := hashJSON(state)
			var err error
			handled, err = c.handleAutomaticLearner(ctx, r, state, version, sts, &existing)
			if err != nil || hashJSON(state) != before {
				return err
			}
		}
		disposition, err := c.assessAutomaticEscalation(ctx, r, state, version, sts)
		if err != nil || disposition == automaticEscalationStop || handled {
			return err
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
	var child Resource
	err := c.Kube.Get(ctx, c.resourcePath(intent.RecoveryName), &child)
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
	// The earlier read is only a preflight. The child can be deleted and
	// recreated before this revision read, so re-check its durable UID before
	// reserving or writing any specification revision.
	if intent.RecoveryUID != "" && str(child.Metadata["uid"]) != intent.RecoveryUID {
		return block("recovery resource UID changed")
	}
	if str(nested(child.Metadata, "annotations", automaticOwner)) != state.BindingUID || str(nested(child.Metadata, "annotations", automaticLogical)) != r.Spec.LogicalID {
		return block("recovery resource differs from frozen automatic intent")
	}
	if intent.RecoveryUID == "" {
		if resourceName(&child) != intent.RecoveryName || str(child.Metadata["uid"]) == "" || hashJSON(child.Spec) != desiredHash {
			return block("uncheckpointed recovery resource differs from the desired specification")
		}
		intent.RecoveryUID = str(child.Metadata["uid"])
		intent.SpecHash = desiredHash
		if intent.LearnerPodName != "" {
			assignment, err := sameOrdinalAssignmentDigest(state, intent)
			if err != nil {
				return block("same-ordinal assignment binding is incomplete")
			}
			intent.LearnerAssignmentID = assignment
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
		if intent.LearnerPodName != "" {
			assignment, err := sameOrdinalAssignmentDigest(state, intent)
			if err != nil {
				return block("same-ordinal assignment binding is incomplete")
			}
			intent.LearnerAssignmentID = assignment
		}
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
	if intent.Failed != "" && child.Status.Phase != "Complete" && intent.LearnerUID != "" && !intent.Aborting {
		before := hashJSON(state)
		handled, err := c.handleAutomaticLearner(ctx, r, state, version, sts, &child)
		if err != nil || handled || hashJSON(state) != before {
			return err
		}
	}
	if child.Status.Phase != "Complete" {
		return c.autoStatus(ctx, r, state, "Recovering", child.Status.Phase+": "+child.Status.Message)
	}
	var installed installedGeneration
	if err := c.verifyAutomaticCompletion(ctx, r, state, &child, sts, &installed); err != nil {
		return block("completed recovery outcome is invalid: " + err.Error())
	}
	if intent.Failed != "" {
		state.Voters = nil
		for _, ref := range intent.Voters {
			if ref.NodeID != intent.Failed {
				state.Voters = append(state.Voters, ref)
			}
		}
		state.Voters = append(state.Voters, VoterPod{NodeID: quepaxa.NodeID(intent.LearnerName), Pod: learnerPhysicalPod(intent)})
		delete(state.Identities, intent.Failed)
		state.Identities[quepaxa.NodeID(intent.LearnerName)] = FenceTarget{NodeID: intent.LearnerName, Pod: learnerPhysicalPod(intent), PodUID: intent.LearnerUID, WALIdentity: intent.LearnerWAL}
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
	completion := &autoCompletion{RecoveryName: intent.RecoveryName, RecoveryUID: intent.RecoveryUID, SpecHash: intent.SpecHash, FenceProofID: intent.Proof.ProofID, FenceRequestHash: intent.Proof.RequestHash, ClusterID: state.ActiveClusterID, Voters: append([]VoterPod(nil), state.Voters...), At: now}
	if intent.LearnerPodName != "" {
		copy := *intent
		completion.LearnerIntent = &copy
	}
	state.Completed = completion
	state.BootstrapGenesis = nil
	state.SuspectedSince = time.Time{}
	state.Intent = nil
	if err := c.saveAuto(ctx, r, state, version); err != nil {
		return err
	}
	return c.autoStatus(ctx, r, state, "Stabilizing", "recovery completed; verifying the active quorum")
}

// automaticRecoveryChildSpec is the single production builder for the exact
// child spec pinned into automatic CONTROL. Membership children bind fencing
// evidence in Membership.Fence; the top-level Fence is intentionally empty for
// that distinct recovery mode.
func automaticRecoveryChildSpec(r *ClusterResource, state *autoControl, intent *autoIntent) Spec {
	desired := Spec{StatefulSet: r.Spec.StatefulSet, Container: r.Spec.Container, SourceClusterID: state.ActiveClusterID, Durability: intent.Durability, RecoveryID: intent.RecoveryName, AllowDataLoss: intent.Policy.AllowDataLoss, AnchorID: r.Spec.AnchorID, Fence: Fence{RecoveryID: intent.RecoveryName, ClusterID: state.ActiveClusterID, StatefulSetUID: state.StatefulSetUID, Confirmed: true, Evidence: intent.Proof.ProofID}}
	if intent.Failed == "" {
		return desired
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
	desired.Membership = &MembershipSpec{AbortAddition: intent.Aborting, OperationID: intent.RecoveryName, Remove: intent.Failed, RemovedPod: target.Pod, VoterPods: voters, ReplacementPod: learnerPhysicalPod(intent), ReplacementSecret: learnerCredentialName(intent), Fence: MembershipFence{NodeID: intent.Failed, WALIdentity: target.WALIdentity, WorkloadUID: target.PodUID, Confirmed: true, Evidence: intent.Proof.ProofID}}
	return desired
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
	if state.BootstrapGenesis != nil {
		if intent.Request.BootstrapGenesis == nil || *intent.Request.BootstrapGenesis != *state.BootstrapGenesis ||
			r.Spec.SourceClusterID != state.ActiveClusterID || r.Spec.Durability != "async" || !r.Spec.AllowDataLoss {
			return false, nil
		}
		if _, err := c.confirmBootstrapFence(ctx, intent.Request, *intent.Proof); err != nil {
			if errors.Is(err, errBootstrapFenceConfirmationUnavailable) {
				return false, nil
			}
			return false, err
		}
	} else if intent.Request.BootstrapGenesis != nil {
		return false, nil
	}
	if err := validateFenceProof(intent.Request, intent.Proof); err != nil {
		return false, err
	}
	return true, nil
}

var errBootstrapFenceConfirmationUnavailable = errors.New("canonical bootstrap fence confirmation is unavailable")

func (c *Controller) confirmBootstrapFence(ctx context.Context, request FenceRequest, dataProof FenceProof) (BootstrapGenesisFenceConfirmation, error) {
	confirmer, ok := c.Fencer.(BootstrapGenesisFenceConfirmer)
	if !ok || confirmer == nil {
		return BootstrapGenesisFenceConfirmation{}, errBootstrapFenceConfirmationUnavailable
	}
	confirmation, err := confirmer.ConfirmBootstrapGenesisFence(ctx, request)
	if err != nil {
		return BootstrapGenesisFenceConfirmation{}, err
	}
	if hashJSON(confirmation.Request) != hashJSON(request) || hashJSON(confirmation.Proof) != hashJSON(dataProof) ||
		confirmation.Proof.IndependentV2 == nil || !strings.HasPrefix(confirmation.Proof.ProofID, "independent-v2-") {
		return BootstrapGenesisFenceConfirmation{}, fmt.Errorf("bootstrap recovery child has no matching canonical CONTROL fence proof")
	}
	if err := validateFenceProof(request, &confirmation.Proof); err != nil {
		return BootstrapGenesisFenceConfirmation{}, err
	}
	return confirmation, nil
}

// confirmedBootstrapFenceForChild recovers the protected source cohort used
// for the one initial-genesis case where the archive membership object itself
// was never written. The DATA control record only supplies a lookup request;
// confirmBootstrapFence verifies that request and proof with canonical CONTROL.
func (c *Controller) confirmedBootstrapFenceForChild(ctx context.Context, r *Resource) (*BootstrapGenesisFenceConfirmation, error) {
	owner := str(nested(r.Metadata, "annotations", automaticOwner))
	logical := str(nested(r.Metadata, "annotations", automaticLogical))
	if owner == "" || !identifier.MatchString(logical) {
		return nil, nil
	}
	state, _, err := c.loadAuto(ctx, path.Join(c.Prefix, "automatic", c.Kube.Namespace, logical, "control.json"))
	if err != nil {
		return nil, err
	}
	intent := state.Intent
	if state.BootstrapGenesis == nil || intent == nil || intent.Proof == nil || state.BindingUID != owner ||
		intent.RecoveryUID != str(r.Metadata["uid"]) || intent.RecoveryName != resourceName(r) || intent.SpecHash != hashJSON(r.Spec) ||
		intent.Request.BootstrapGenesis == nil || *intent.Request.BootstrapGenesis != *state.BootstrapGenesis ||
		r.Spec.SourceClusterID != state.ActiveClusterID || r.Spec.Durability != "async" || !r.Spec.AllowDataLoss {
		return nil, nil
	}
	confirmation, err := c.confirmBootstrapFence(ctx, intent.Request, *intent.Proof)
	if err != nil {
		return nil, err
	}
	return &confirmation, nil
}
