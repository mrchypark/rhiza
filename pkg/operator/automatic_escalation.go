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
	intent := state.Intent
	ctr, err := container(sts, r.Spec.Container)
	if err != nil {
		return true, err
	}
	env, err := c.environment(ctx, ctr)
	if err != nil {
		return true, err
	}
	statuses := map[quepaxa.NodeID]network.MembershipStatus{}
	for _, ref := range intent.Voters {
		if ref.NodeID == intent.Failed {
			continue
		}
		var pod object
		err := c.Kube.Get(ctx, c.corePath("pods", ref.Pod), &pod)
		if err != nil {
			if apiNotFound(err) {
				continue
			}
			return true, err
		}
		endpoint, err := podEndpoint(pod, r.Spec.Container, "/membership/status")
		if err != nil {
			continue
		}
		status, err := c.membershipStatus(ctx, endpoint, env["RHIZA_ADMIN_TOKEN"])
		if errors.Is(err, ErrMembershipUnauthorized) {
			return true, err
		}
		if err == nil {
			statuses[ref.NodeID] = status
		}
	}
	quorum, _, err := membershipQuorum(statuses, state.ActiveClusterID, intent.Failed)
	if err != nil {
		return true, c.autoStatus(ctx, r, state, "Blocked", "surviving membership observations conflict")
	}
	if quorum {
		if intent.NoQuorumSince.IsZero() {
			return false, nil
		}
		intent.NoQuorumSince = time.Time{}
		return true, c.saveAuto(ctx, r, state, version)
	}
	if intent.NoQuorumSince.IsZero() {
		intent.NoQuorumSince = time.Now().UTC()
		return true, c.saveAuto(ctx, r, state, version)
	}
	if time.Since(intent.NoQuorumSince) < time.Duration(intent.Policy.FailureGraceSeconds)*time.Second {
		return true, c.autoStatus(ctx, r, state, "Recovering", "surviving quorum unavailable; waiting before generation-wide fencing")
	}
	if intent.Durability == "async" && !intent.Policy.AllowDataLoss {
		return true, c.autoStatus(ctx, r, state, "Blocked", "async generation escalation requires allowDataLoss")
	}
	archive := recovery.NewManager(c.Bucket, path.Join(c.Prefix, state.ActiveClusterID), 1)
	err = archive.Load(ctx)
	tip := archive.Tip()
	archive.Close()
	if err != nil || tip == 0 {
		return true, c.autoStatus(ctx, r, state, "Blocked", "certified archive unavailable for escalation")
	}
	state.Sequence++
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
		request.Targets = append(request.Targets, FenceTarget{NodeID: intent.LearnerName, Pod: intent.LearnerName, PodUID: intent.LearnerUID, WALIdentity: intent.LearnerWAL})
	}
	superseded := intent.RecoveryName + "-" + hashJSON(intent)
	if err := c.immutable(ctx, path.Join(c.Prefix, "automatic", c.Kube.Namespace, r.Spec.LogicalID, "superseded", superseded+".json"), jsonBytes(intent)); err != nil {
		return true, err
	}
	state.Intent = &autoIntent{Supersedes: superseded, Request: request, Policy: intent.Policy, Durability: intent.Durability, Voters: intent.Voters, RecoveryName: name}
	if err := c.saveAuto(ctx, r, state, version); err != nil {
		return true, err
	}
	return true, c.autoStatus(ctx, r, state, "Fencing", "online quorum lost; generation-wide fencing reserved")
}
