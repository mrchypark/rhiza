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
		return block("external fencing service is unavailable")
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
				return c.autoStatus(ctx, r, state, "Fencing", "waiting for external termination and recreation barrier")
			}
			return block("external fencing did not return a valid completion proof")
		}
		intent.Proof = proof
		if err := c.saveAuto(ctx, r, state, version); err != nil {
			return err
		}
		return c.autoStatus(ctx, r, state, "Fenced", "external fencing proof persisted")
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
		handled, err := c.maybeEscalateAutomatic(ctx, r, state, version, sts)
		if handled || err != nil {
			return err
		}
		handled, err = c.handleAutomaticLearner(ctx, r, state, version, sts, &existing)
		if handled || err != nil {
			return err
		}
	}
	desired := Spec{StatefulSet: r.Spec.StatefulSet, Container: r.Spec.Container, SourceClusterID: state.ActiveClusterID, Durability: intent.Durability, RecoveryID: intent.RecoveryName, AllowDataLoss: intent.Policy.AllowDataLoss, Fence: Fence{RecoveryID: intent.RecoveryName, ClusterID: state.ActiveClusterID, StatefulSetUID: state.StatefulSetUID, Confirmed: true, Evidence: intent.Proof.ProofID}}
	if intent.Failed != "" {
		var voters []VoterPod
		for _, ref := range intent.Voters {
			if ref.NodeID != intent.Failed {
				voters = append(voters, ref)
			}
		}
		target := intent.Request.Targets[0]
		desired.RecoveryID = ""
		desired.Fence = Fence{}
		desired.Membership = &MembershipSpec{AbortAddition: intent.Aborting, OperationID: intent.RecoveryName, Remove: intent.Failed, RemovedPod: target.Pod, VoterPods: voters, ReplacementPod: intent.LearnerName, ReplacementSecret: intent.LearnerName + "-credentials", Fence: MembershipFence{NodeID: intent.Failed, WALIdentity: target.WALIdentity, WorkloadUID: target.PodUID, Confirmed: true, Evidence: intent.Proof.ProofID}}
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
	if intent.Failed != "" {
		state.Voters = nil
		for _, ref := range intent.Voters {
			if ref.NodeID != intent.Failed {
				state.Voters = append(state.Voters, ref)
			}
		}
		state.Voters = append(state.Voters, VoterPod{NodeID: quepaxa.NodeID(intent.LearnerName), Pod: intent.LearnerName})
		delete(state.Identities, intent.Failed)
	} else {
		if child.Status.Target == "" || child.Status.Stage != "Complete" {
			return block("target generation is not confirmed complete")
		}
		if err := c.releaseCompleted(ctx, &child); err != nil {
			return err
		}
		state.ActiveClusterID = child.Status.Target
		state.Voters = append([]VoterPod(nil), r.Spec.VoterPods...)
		state.Identities = map[quepaxa.NodeID]FenceTarget{}
	}
	obs, _, err := c.observeAutomatic(ctx, r, state, sts)
	if err != nil || !obs.QuorumVerified || obs.Reachable < 2 {
		return block("authenticated target quorum is not verified")
	}
	now := time.Now().UTC()
	recent := []time.Time{now}
	for _, completion := range state.Completions {
		if completion.After(now.Add(-24 * time.Hour)) {
			recent = append(recent, completion)
		}
	}
	state.Completions = recent
	state.SuspectedSince = time.Time{}
	state.Intent = nil
	if err := c.saveAuto(ctx, r, state, version); err != nil {
		return err
	}
	return c.autoStatus(ctx, r, state, "Stabilizing", "recovery completed; verifying the active quorum")
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
