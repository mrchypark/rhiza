package operator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type quorumProbeState uint8

const (
	quorumProbeUnknown quorumProbeState = iota
	quorumProbeAvailable
	quorumProbeUnavailable
	quorumProbeNoResponse
)

// automaticQuorumProbe accepts only a nonce-authenticated membership snapshot
// whose fresh ReadIndex result is bound to that same configuration. A failed,
// malformed, stale, or unstable probe is unknown, never evidence of death.
func (c *Controller) automaticQuorumProbe(ctx context.Context, endpoint, token, cluster, node string, excluded ...string) (network.VoterRecoveryStatus, quorumProbeState, error) {
	return c.automaticQuorumProbeWithTimeout(ctx, endpoint, token, cluster, node, 3*time.Second, excluded...)
}

func (c *Controller) automaticQuorumProbeWithTimeout(ctx context.Context, endpoint, token, cluster, node string, timeout time.Duration, excluded ...string) (network.VoterRecoveryStatus, quorumProbeState, error) {
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return network.VoterRecoveryStatus{}, quorumProbeUnknown, fmt.Errorf("create recovery probe challenge: %w", err)
	}
	challenge := hex.EncodeToString(nonce[:])
	exclude := ""
	if len(excluded) > 0 {
		exclude = excluded[0]
	}
	call, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(call, http.MethodGet, endpoint+"?version=1&nonce="+challenge+"&exclude="+url.QueryEscape(exclude), nil)
	if err != nil {
		return network.VoterRecoveryStatus{}, quorumProbeUnknown, fmt.Errorf("build recovery probe request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := c.client().Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return network.VoterRecoveryStatus{}, quorumProbeUnknown, ctx.Err()
		}
		return network.VoterRecoveryStatus{}, quorumProbeNoResponse, nil
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return network.VoterRecoveryStatus{}, quorumProbeUnknown, ErrMembershipUnauthorized
	}
	if response.StatusCode != http.StatusOK {
		data, readErr := io.ReadAll(io.LimitReader(response.Body, 16<<10))
		if ctx.Err() != nil {
			return network.VoterRecoveryStatus{}, quorumProbeUnknown, ctx.Err()
		}
		if readErr != nil {
			return network.VoterRecoveryStatus{}, quorumProbeNoResponse, nil
		}
		var apiErr network.ErrorResponse
		if response.StatusCode >= http.StatusInternalServerError {
			if json.Unmarshal(data, &apiErr) == nil && (apiErr.Code == "invalid_recovery_observation" || apiErr.Code == "recovery_observation_unavailable") {
				return network.VoterRecoveryStatus{}, quorumProbeUnknown, network.ErrRecoveryProbeSchema
			}
			return network.VoterRecoveryStatus{}, quorumProbeNoResponse, nil
		}
		if json.Unmarshal(data, &apiErr) == nil && apiErr.Code == "unsupported_recovery_probe_version" {
			return network.VoterRecoveryStatus{}, quorumProbeUnknown, network.ErrRecoveryProbeVersion
		}
		return network.VoterRecoveryStatus{}, quorumProbeUnknown, fmt.Errorf("recovery probe HTTP status %d: %w", response.StatusCode, network.ErrRecoveryProbeSchema)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (16<<10)+1))
	if err != nil {
		if ctx.Err() != nil {
			return network.VoterRecoveryStatus{}, quorumProbeUnknown, ctx.Err()
		}
		return network.VoterRecoveryStatus{}, quorumProbeNoResponse, nil
	}
	if err := call.Err(); err != nil {
		if ctx.Err() != nil {
			return network.VoterRecoveryStatus{}, quorumProbeUnknown, ctx.Err()
		}
		return network.VoterRecoveryStatus{}, quorumProbeNoResponse, nil
	}
	if len(data) > 16<<10 {
		return network.VoterRecoveryStatus{}, quorumProbeUnknown, network.ErrRecoveryProbeSchema
	}
	probe, err := network.StrictDecodeRecoveryProbe(data, challenge, exclude, token)
	if err != nil {
		return network.VoterRecoveryStatus{}, quorumProbeUnknown, err
	}
	if err := call.Err(); err != nil {
		if ctx.Err() != nil {
			return network.VoterRecoveryStatus{}, quorumProbeUnknown, ctx.Err()
		}
		return network.VoterRecoveryStatus{}, quorumProbeNoResponse, nil
	}
	status := probe.Status
	if status.ClusterID != cluster || status.NodeID != node || status.ConfigID == 0 || status.WALIdentity == "" {
		return status, quorumProbeUnknown, network.ErrRecoveryProbeIdentity
	}
	if !uniqueIDs(nodeIDs(status.Voters)) {
		return status, quorumProbeUnknown, network.ErrRecoveryProbeSchema
	}
	if status.Voting && !containsID(nodeIDs(status.Voters), quepaxa.NodeID(node)) {
		return status, quorumProbeUnknown, network.ErrRecoveryProbeIdentity
	}
	if !status.Voting || status.Pending || (!status.QuorumKnown && !status.QuorumUnavailable) {
		return status, quorumProbeUnknown, nil
	}
	if status.QuorumKnown {
		// A successful ReadIndex is the only positive quorum proof. Conflicting
		// flags or a mismatched configuration invalidate the whole observation.
		if status.QuorumUnavailable || !status.Quorum || status.QuorumConfigID != status.ConfigID {
			return status, quorumProbeUnknown, network.ErrRecoveryProbeSchema
		}
		return status, quorumProbeAvailable, nil
	}
	if status.QuorumUnavailable {
		// Explicit no-quorum is a timer input only. It must be the stable,
		// authenticated negative ReadIndex result; it carries no positive
		// quorum configuration or slot evidence.
		if status.Quorum || status.QuorumConfigID != 0 || status.ReadIndexSlot != 0 {
			return status, quorumProbeUnknown, network.ErrRecoveryProbeSchema
		}
		return status, quorumProbeUnavailable, nil
	}
	// An unknown observation cannot carry stale positive/negative metadata.
	if status.Quorum || status.QuorumConfigID != 0 || status.ReadIndexSlot != 0 {
		return status, quorumProbeUnknown, network.ErrRecoveryProbeSchema
	}
	return status, quorumProbeUnknown, nil
}

func (c *Controller) automaticSurvivingQuorum(ctx context.Context, r *ClusterResource, state *autoControl, sts object, failed quepaxa.NodeID) (quorumProbeState, error) {
	ctr, err := container(sts, r.Spec.Container)
	if err != nil {
		return quorumProbeUnknown, err
	}
	env, err := c.environment(ctx, ctr)
	if err != nil {
		return quorumProbeUnknown, err
	}
	unknown := false
	available := false
	for _, ref := range state.Voters {
		if ref.NodeID == failed {
			continue
		}
		expected, ok := state.Identities[ref.NodeID]
		if !ok || expected.NodeID != string(ref.NodeID) || expected.Pod != ref.Pod || expected.PodUID == "" || expected.WALIdentity == "" {
			return quorumProbeUnknown, fmt.Errorf("surviving voter %s is outside the captured fencing inventory", ref.NodeID)
		}
		var pod object
		if err := c.Kube.Get(ctx, c.corePath("pods", ref.Pod), &pod); err != nil {
			return quorumProbeUnknown, fmt.Errorf("read surviving voter Pod %s: %w", ref.Pod, err)
		}
		if str(nested(pod, "metadata", "uid")) != expected.PodUID || str(nested(pod, "status", "podIP")) == "" {
			return quorumProbeUnknown, fmt.Errorf("surviving voter Pod %s differs from the captured fencing inventory", ref.Pod)
		}
		endpoint, err := podEndpoint(pod, r.Spec.Container, "/recovery/probe")
		if err != nil {
			return quorumProbeUnknown, err
		}
		status, result, err := c.automaticQuorumProbe(ctx, endpoint, env["RHIZA_ADMIN_TOKEN"], state.ActiveClusterID, string(ref.NodeID), string(failed))
		if err != nil {
			return quorumProbeUnknown, err
		}
		var current object
		if err := c.Kube.Get(ctx, c.corePath("pods", ref.Pod), &current); err != nil {
			return quorumProbeUnknown, fmt.Errorf("recheck surviving voter Pod %s: %w", ref.Pod, err)
		}
		if err := validateProbePodIncarnation(pod, current, expected); err != nil {
			return quorumProbeUnknown, err
		}
		if status.NodeID != "" {
			if status.WALIdentity != expected.WALIdentity {
				return quorumProbeUnknown, fmt.Errorf("surviving voter %s WAL identity differs from captured fencing inventory", ref.NodeID)
			}
			if !sameIDs(nodeIDs(status.Voters), voterPodIDs(state.Voters)) {
				return quorumProbeUnknown, fmt.Errorf("surviving voter %s reports membership outside the active fencing inventory", ref.NodeID)
			}
		}
		if result == quorumProbeNoResponse || result == quorumProbeUnknown {
			unknown = true
			continue
		}
		if result == quorumProbeAvailable {
			available = true
		}
	}
	if available {
		return quorumProbeAvailable, nil
	}
	if unknown {
		return quorumProbeUnknown, nil
	}
	return quorumProbeUnavailable, nil
}

func voterPodIDs(voters []VoterPod) []quepaxa.NodeID {
	ids := make([]quepaxa.NodeID, len(voters))
	for i, voter := range voters {
		ids[i] = voter.NodeID
	}
	return ids
}

func validateProbePodIncarnation(before, after object, expected FenceTarget) error {
	beforeUID := str(nested(before, "metadata", "uid"))
	beforeIP := str(nested(before, "status", "podIP"))
	if expected.PodUID == "" || beforeUID != expected.PodUID || beforeIP == "" ||
		str(nested(after, "metadata", "uid")) != beforeUID || str(nested(after, "status", "podIP")) != beforeIP {
		return fmt.Errorf("Pod incarnation changed during recovery probe")
	}
	return nil
}
