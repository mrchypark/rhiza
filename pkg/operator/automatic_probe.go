package operator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"io"
	"net/http"
	"net/url"
	"time"
)

func (c *Controller) automaticQuorumProbe(ctx context.Context, endpoint, token, cluster, node string, excluded ...string) bool {
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return false
	}
	challenge := hex.EncodeToString(nonce[:])
	exclude := ""
	if len(excluded) > 0 {
		exclude = excluded[0]
	}
	call, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(call, http.MethodGet, endpoint+"?nonce="+challenge+"&exclude="+url.QueryEscape(exclude), nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := c.client().Do(req)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	var probe network.RecoveryProbe
	if response.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(response.Body, 16<<10)).Decode(&probe) != nil {
		return false
	}
	return probe.Excluded == exclude && network.VerifyRecoveryProbe(probe, challenge, token) && probe.Status.ClusterID == cluster && probe.Status.NodeID == node && probe.Status.Quorum
}

func (c *Controller) automaticSurvivingQuorum(ctx context.Context, r *ClusterResource, state *autoControl, sts object, failed quepaxa.NodeID) bool {
	ctr, err := container(sts, r.Spec.Container)
	if err != nil {
		return false
	}
	env, err := c.environment(ctx, ctr)
	if err != nil {
		return false
	}
	for _, ref := range state.Voters {
		if ref.NodeID == failed {
			continue
		}
		var pod object
		if c.Kube.Get(ctx, c.corePath("pods", ref.Pod), &pod) != nil {
			continue
		}
		endpoint, err := podEndpoint(pod, r.Spec.Container, "/recovery/probe")
		if err != nil {
			continue
		}
		if c.automaticQuorumProbe(ctx, endpoint, env["RHIZA_ADMIN_TOKEN"], state.ActiveClusterID, string(ref.NodeID), string(failed)) {
			return true
		}
	}
	return false
}
