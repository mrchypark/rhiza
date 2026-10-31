package operator

import (
	"context"
	"strings"
	"testing"
)

func TestRepairPodBindingReturnsValidatedOwnerChain(t *testing.T) {
	f, old := repairTestSetup(t)
	status := f.pod["status"].(map[string]any)
	status["containerStatuses"] = []any{map[string]any{"name": "app", "restartCount": 1, "state": map[string]any{"running": map[string]any{"startedAt": "2026-10-30T00:00:00Z"}}}}
	binding, err := ValidateV1Alpha2RepairPodBinding(context.Background(), f.store, phase2Ref, f.plan)
	if err != nil {
		t.Fatal(err)
	}
	if binding.Namespace != phase2Ref.Namespace || binding.PodName != f.plan.Replacement.PodName || binding.PodUID != f.plan.Replacement.PodUID || binding.ReplicaSetUID != "rsuid" || binding.DeploymentUID != f.plan.DeploymentUID || binding.ClusterUID != phase2Ref.UID || binding.Generation != f.plan.Generation {
		t.Fatalf("unexpected repair binding: %+v", binding)
	}
	if _, _, err := BeginV1Alpha2Repair(context.Background(), f.store, old, old.Authority(), f.plan); err == nil || !strings.Contains(err.Error(), "repair candidate main container has already started") {
		t.Fatalf("first repair authorization did not reject the already-started candidate for the phase-gate reason: %v", err)
	}
}
