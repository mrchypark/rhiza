package managedbootstrap

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/operator"
)

// RunReplacementLoop reconciles certified membership changes until ctx is cancelled.
// The caller must wait for it to return before closing host.
func RunReplacementLoop(ctx context.Context, host *managedsql.Host, cfg managedsql.Config) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	last := ""
	for {
		if ctx.Err() != nil {
			return
		}
		attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
		stage, err := reconcileReplacement(attempt, host, cfg)
		cancel()
		outcome := stage + ":ok"
		if err != nil {
			outcome = fmt.Sprintf("%s:%T", stage, err)
		}
		if outcome != last && ctx.Err() == nil {
			log.Printf("managed replacement %s", outcome)
			last = outcome
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func reconcileReplacement(ctx context.Context, host *managedsql.Host, cfg managedsql.Config) (string, error) {
	stage, err := host.ReconcileExternalReplicas(ctx)
	if err != nil || stage != "disabled" && stage != "idle" && stage != "ordinary-repair" {
		return stage, err
	}
	a, err := operator.LoadV1Alpha2Admission(ctx, cfg.Store, cfg.Ref)
	if err != nil {
		return "authority", err
	}
	if r, ok := a.Repair(); ok && r.State == "Closed" {
		plan, err := host.PlanVoterReplacement(ctx, operator.V1Alpha2RepairPodRequest{}, nil)
		if err != nil {
			return "closed-candidate-blocked", err
		}
		stage := "closed-remove"
		if r.PreparedHandoff != nil {
			stage = "closed-learner-add"
		}
		return stage, host.ReplaceVoter(ctx, *plan)
	}
	_, port, err := net.SplitHostPort(cfg.Runtime.PeerAddr)
	if err != nil {
		return "discovery", err
	}
	peerPort, err := strconv.Atoi(port)
	if err != nil {
		return "discovery", err
	}
	pods, err := operator.ObserveV1Alpha2ManagedPods(ctx, cfg.Store, cfg.Ref, peerPort)
	if err != nil {
		return "discovery", err
	}
	present := make([]types.NodeID, 0, len(pods))
	for _, pod := range pods {
		present = append(present, pod.Member.NodeID)
	}
	for _, pod := range pods {
		if !pod.Candidate {
			continue
		}
		plan, err := host.PlanVoterReplacement(ctx, pod.Member, present)
		if err != nil {
			return "plan", err
		}
		if plan != nil {
			return "replace", host.ReplaceVoter(ctx, *plan)
		}
	}
	return "idle", nil
}
