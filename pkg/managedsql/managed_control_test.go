package managedsql

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedSQLPreparationRequiresLiveExactRepair(t *testing.T) {
	ctx := context.Background()
	configs, hosts, _ := openSQLQuorum(t)
	h := hosts[0]
	writer, plan := hostRepairWriter(t, configs, hosts)
	plan.Sequence = operator.V1Alpha2RepairAddThenRemove
	r := node.ManagedMembershipRequest{RepairOperationID: plan.OperationID, Action: node.ManagedMembershipAdd, ExpectedConfigID: uint(plan.ExpectedConfigID), OldNodeID: quepaxa.NodeID(plan.OldNodeID), OldWALIdentity: plan.OldWALIdentity}
	current := quepaxa.Cluster{ConfigID: 1, Members: configs[0].Runtime.Members}
	if err := h.ValidateManagedRepairPreparation(ctx, h.identity, r, current, 0); err == nil {
		t.Fatal("missing repair accepted")
	}
	a, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	closed, _, err := operator.BeginV1Alpha2Repair(ctx, writer, a, a.Authority(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.ValidateManagedMembership(ctx, h.identity, r, current, 0); err != nil {
		t.Fatalf("exact live ADD-first source preparation: %v", err)
	}
	wrong := r
	wrong.OldWALIdentity = "wrong"
	if err := h.ValidateManagedMembership(ctx, h.identity, wrong, current, 0); err == nil {
		t.Fatal("wrong old WAL accepted")
	}
	if err := h.ValidateManagedMembership(ctx, h.identity, r, current, 1); err == nil {
		t.Fatal("wrong abort revision accepted")
	}
	i := h.identity
	i.Member.Generation++
	if err := h.ValidateManagedMembership(ctx, i, r, current, 0); err == nil {
		t.Fatal("wrong generation accepted")
	}
	completion := operator.V1Alpha2RepairCompletion{OperationID: plan.OperationID, PlanDigest: plan.Digest(), ExpectedConfigID: plan.ExpectedConfigID, ExpectedAbortSlot: plan.ExpectedAbortSlot, Kind: "Abort", ConfigID: 1, Slot: 1, Hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if _, _, err := operator.FinishV1Alpha2Repair(ctx, writer, closed, closed.Authority(), completion); err != nil {
		t.Fatal(err)
	}
	if err := h.ValidateManagedMembership(ctx, h.identity, r, current, 0); err == nil {
		t.Fatal("terminal repair reused for preparation")
	}
}

func TestManagedSQLDirectMembershipRejectsRemoveFirstIntent(t *testing.T) {
	configs, fixture := sqlCluster(t)
	ctx := context.Background()
	// Snapshot assertions require an actual reconfiguration-enabled runtime,
	// including the matching committed bootstrap digest, not the SQL-only fixture.
	for i := range configs {
		configs[i].Runtime.EnableReconfiguration = true
	}
	bootstrapNode := node.New(&configs[0].Runtime)
	plan := configs[0].Plan
	digest, err := bootstrapNode.ManagedBootstrapPlanDigestForProtocol(plan.PhysicalBinding, plan.EvidenceFormat, plan.ApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	plan.BootstrapPlanDigest = digest
	var status map[string]json.RawMessage
	if err := json.Unmarshal(fixture.obj["status"], &status); err != nil {
		t.Fatal(err)
	}
	var assignment operator.V1Alpha2ApplicationInitAssignment
	var bootstrap operator.V1Alpha2BootstrapPlan
	if err := json.Unmarshal(status["applicationInit"], &assignment); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(status["bootstrapPlan"], &bootstrap); err != nil {
		t.Fatal(err)
	}
	assignment.Plan = plan
	bootstrap.Reconfiguration = true
	status["applicationInit"], _ = json.Marshal(assignment)
	status["bootstrapPlan"], _ = json.Marshal(bootstrap)
	fixture.obj["status"], _ = json.Marshal(status)
	hosts := make([]*Host, len(configs))
	for i := range configs {
		configs[i].Plan = plan
		h, err := fixture.open(ctx, configs[i])
		if h != nil {
			t.Cleanup(func() {
				if err := h.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = h
	}
	for _, h := range hosts {
		readySQL(t, h)
	}
	readyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()
	for _, h := range hosts {
		for !h.node.Ready() {
			select {
			case <-readyCtx.Done():
				t.Fatal("fixture voter did not reach actual peer readiness")
			case <-ticker.C:
			}
		}
	}
	h := hosts[0]
	writer, repairPlan := hostRepairWriter(t, configs, hosts)
	repairPlan.Sequence = ""
	fixture.mu.Lock()
	_ = json.Unmarshal(fixture.obj["status"], &status)
	status["repair"], _ = json.Marshal(operator.V1Alpha2RepairRecord{Plan: repairPlan, State: "Closed"})
	fixture.obj["status"], _ = json.Marshal(status)
	fixture.mu.Unlock()
	closed, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	state, err := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []struct {
		name string
		run  func() error
	}{
		{"remove", func() error { return h.RemoveRepairVoter(ctx, repairPlan.OperationID) }},
		{"capture", func() error { _, err := h.CaptureRepairHandoff(ctx, repairPlan.OperationID); return err }},
		{"add", func() error { return h.AddRepairVoter(ctx, repairPlan.OperationID, "must-not-authorize") }},
	} {
		if err := action.run(); err == nil || !strings.Contains(err.Error(), "append-only replacement requires ADD-first") {
			t.Fatalf("%s accepted a retired REMOVE-first intent: %v", action.name, err)
		}
	}
	after, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	afterState, stateErr := h.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil || stateErr != nil || after.ResourceVersion() != closed.ResourceVersion() || !reflect.DeepEqual(state.Current, afterState.Current) || state.AbortSlot != afterState.AbortSlot {
		t.Fatalf("rejected direct membership changed CR/runtime: err=%v state_error=%v", err, stateErr)
	}
}
