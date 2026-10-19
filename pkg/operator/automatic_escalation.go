package operator

import (
	"context"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// Losing the surviving quorum during an online incident never reuses the
// single-voter proof for DR. Reserve a new generation-wide fence first and
// revoke the old child's execution authority in the same CAS.
func (c *Controller) maybeEscalateAutomatic(ctx context.Context, r *ClusterResource, state *autoControl, version *objstore.ObjectVersion, sts object) (bool, error) {
	disposition, err := c.assessAutomaticEscalation(ctx, r, state, version, sts)
	return disposition != automaticEscalationContinue, err
}

type automaticEscalationDisposition uint8

const (
	automaticEscalationContinue automaticEscalationDisposition = iota
	automaticEscalationWait
	automaticEscalationStop
)

func (c *Controller) assessAutomaticEscalation(ctx context.Context, r *ClusterResource, state *autoControl, version *objstore.ObjectVersion, sts object) (automaticEscalationDisposition, error) {
	intent := state.Intent
	ctr, err := container(sts, r.Spec.Container)
	if err != nil {
		return automaticEscalationStop, err
	}
	env, err := c.environment(ctx, ctr)
	if err != nil {
		return automaticEscalationStop, err
	}
	statuses := map[quepaxa.NodeID]network.MembershipStatus{}
	pods := map[quepaxa.NodeID]object{}
	fencePods := map[quepaxa.NodeID]string{}
	for _, ref := range state.Voters {
		fencePods[ref.NodeID] = ref.Pod
	}
	voters := append([]VoterPod(nil), intent.Voters...)
	for _, ref := range voters {
		if fencePods[ref.NodeID] != ref.Pod {
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "intent voter is outside the generation fencing scope")
		}
	}
	if intent.LearnerName != "" {
		voters = append(voters, VoterPod{NodeID: quepaxa.NodeID(intent.LearnerName), Pod: intent.LearnerName})
		if intent.LearnerUID != "" && intent.LearnerWAL != "" {
			fencePods[quepaxa.NodeID(intent.LearnerName)] = intent.LearnerName
		}
	}
	for _, ref := range voters {
		if ref.NodeID == intent.Failed {
			continue
		}
		var pod object
		err := c.Kube.Get(ctx, c.corePath("pods", ref.Pod), &pod)
		if err != nil {
			if apiNotFound(err) {
				continue
			}
			return automaticEscalationStop, err
		}
		if string(ref.NodeID) == intent.LearnerName && intent.LearnerUID != "" && str(nested(pod, "metadata", "uid")) != intent.LearnerUID {
			return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "learner Pod differs from the generation fencing scope")
		}
		endpoint, err := podEndpoint(pod, r.Spec.Container, "/membership/status")
		if err != nil {
			continue
		}
		status, err := c.membershipStatus(ctx, endpoint, env["RHIZA_ADMIN_TOKEN"])
		if errors.Is(err, ErrMembershipUnauthorized) {
			return automaticEscalationStop, err
		}
		if err == nil {
			if string(ref.NodeID) == intent.LearnerName && status.NodeID == ref.NodeID && status.ClusterID == state.ActiveClusterID && intent.LearnerWAL != "" && status.WALIdentity != "" && status.WALIdentity != intent.LearnerWAL {
				return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "learner WAL differs from the generation fencing scope")
			}
			statuses[ref.NodeID] = status
			pods[ref.NodeID] = pod
		}
	}
	for id, status := range statuses {
		if status.NodeID != id || status.ClusterID != state.ActiveClusterID {
			continue
		}
		for _, voter := range status.Voters {
			if _, known := fencePods[voter]; !known {
				return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "observed membership is outside the generation fencing scope")
			}
		}
	}
	quorum := false
	// Local configurations may differ while promotion propagates. A fresh
	// ReadIndex proves source-cluster liveness, not the earlier snapshot's config.
	for id, status := range statuses {
		if status.NodeID != id || status.ClusterID != state.ActiveClusterID || !status.Voting || status.ConfigID == 0 || !containsID(status.Voters, id) || !uniqueIDs(status.Voters) {
			continue
		}
		endpoint, err := podEndpoint(pods[id], r.Spec.Container, "/recovery/probe")
		if err == nil && c.automaticQuorumProbe(ctx, endpoint, env["RHIZA_ADMIN_TOKEN"], state.ActiveClusterID, string(id), string(intent.Failed)) {
			quorum = true
			break
		}
	}
	if quorum {
		if intent.NoQuorumSince.IsZero() {
			return automaticEscalationContinue, nil
		}
		intent.NoQuorumSince = time.Time{}
		return automaticEscalationStop, c.saveAuto(ctx, r, state, version)
	}
	// A frozen Add can itself disable ReadIndex. Give its exact retirement round
	// one finite incident-wide window, without treating snapshots as live quorum.
	if intent.Aborting && !intent.RetirementSince.IsZero() && time.Since(intent.RetirementSince) < time.Duration(intent.Policy.FailureGraceSeconds)*time.Second {
		return automaticEscalationWait, nil
	}
	if intent.NoQuorumSince.IsZero() {
		intent.NoQuorumSince = time.Now().UTC()
		return automaticEscalationStop, c.saveAuto(ctx, r, state, version)
	}
	if time.Since(intent.NoQuorumSince) < time.Duration(intent.Policy.FailureGraceSeconds)*time.Second {
		return automaticEscalationWait, c.autoStatus(ctx, r, state, "Recovering", "surviving quorum unavailable; waiting before generation-wide fencing")
	}
	if intent.Durability == "async" && !intent.Policy.AllowDataLoss {
		return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "async generation escalation requires allowDataLoss")
	}
	archive := recovery.NewManager(c.Bucket, path.Join(c.Prefix, state.ActiveClusterID), 1)
	err = archive.Load(ctx)
	tip := archive.Tip()
	archive.Close()
	if err != nil || tip == 0 {
		return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", "certified archive unavailable for escalation")
	}
	state.Sequence++
	if err := validateLearnerHistory(state.Learners); err != nil {
		return automaticEscalationStop, err
	}
	name := "auto-" + shortID(state.BindingUID+":"+fmt.Sprint(state.Sequence))
	request := intent.Request
	request.OperationID = name
	request.Scope = ScopeGeneration
	request.Targets = nil
	for _, ref := range state.Voters {
		target := state.Identities[ref.NodeID]
		target.NodeID = string(ref.NodeID)
		target.Pod = ref.Pod
		request.Targets = append(request.Targets, target)
	}
	if intent.LearnerUID != "" {
		request.Targets = append(request.Targets, learnerFenceTarget(state, intent.LearnerName, intent.LearnerUID, intent.LearnerWAL))
	}
	if err := includeLearnerHistory(state, &request); err != nil {
		return automaticEscalationStop, err
	}
	// Reserve nothing the fencing backend cannot execute during the outage.
	if err := fenceCapacityError(len(request.Targets)); err != nil {
		return automaticEscalationStop, c.autoStatus(ctx, r, state, "Blocked", err.Error())
	}
	superseded := intent.RecoveryName + "-" + hashJSON(intent)
	if err := c.immutable(ctx, path.Join(c.Prefix, "automatic", c.Kube.Namespace, r.Spec.LogicalID, "superseded", superseded+".json"), jsonBytes(intent)); err != nil {
		return automaticEscalationStop, err
	}
	state.Intent = &autoIntent{Supersedes: superseded, Request: request, Policy: intent.Policy, Durability: intent.Durability, Voters: intent.Voters, RecoveryName: name}
	if err := c.saveAuto(ctx, r, state, version); err != nil {
		return automaticEscalationStop, err
	}
	return automaticEscalationStop, c.autoStatus(ctx, r, state, "Fencing", "online quorum lost; generation-wide fencing reserved")
}
