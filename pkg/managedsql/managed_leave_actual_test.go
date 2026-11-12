//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func openManagedLeaveHosts(t *testing.T, configs []Config, fixture *sqlStatusFixture) []*Host {
	t.Helper()
	hosts := make([]*Host, len(configs))
	for i := range configs {
		h, err := fixture.open(context.Background(), configs[i])
		if h != nil {
			hosts[i] = h
			t.Cleanup(func() {
				if err := fixture.closeHost(h); err != nil {
					t.Error(err)
				}
			})
		}
		if err != nil {
			t.Fatalf("open voter %d: %v", i, err)
		}
	}
	for _, h := range hosts {
		readySQL(t, h)
	}
	return hosts
}

func managedLeaveFixture(t *testing.T) ([]Config, *sqlStatusFixture, []*Host) {
	t.Helper()
	configs, fixture := sqlClusterForProtocolVoters(t, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, 4, true)
	return configs, fixture, openManagedLeaveHosts(t, configs, fixture)
}

func setManagedTarget(t *testing.T, fixture *sqlStatusFixture, target int) {
	t.Helper()
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	var spec map[string]json.RawMessage
	if json.Unmarshal(fixture.obj["spec"], &spec) != nil {
		t.Fatal("invalid fixture spec")
	}
	spec["voterCount"], _ = json.Marshal(target)
	fixture.obj["spec"], _ = json.Marshal(spec)
}

func TestManagedExplicitLeaveReplaysAndRejectsRetiredRunningVoter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	configs, fixture, hosts := managedLeaveFixture(t)
	if err := hosts[0].LeaveVoter(ctx, LeaveVoterRequest{NodeID: configs[3].Runtime.NodeID}); err == nil {
		t.Fatal("leave was accepted without a desired-count reduction")
	}
	setManagedTarget(t, fixture, 3)
	want := []quepaxa.NodeID{configs[0].Runtime.NodeID, configs[1].Runtime.NodeID, configs[2].Runtime.NodeID}
	slices.Sort(want)
	if err := hosts[0].LeaveVoter(ctx, LeaveVoterRequest{NodeID: configs[3].Runtime.NodeID}); err != nil {
		t.Fatalf("explicit leave: %v", err)
	}
	stateBefore, err := hosts[0].node.ManagedState()
	if err != nil {
		t.Fatal(err)
	}
	before, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := hosts[0].LeaveVoter(ctx, LeaveVoterRequest{NodeID: configs[3].Runtime.NodeID}); err != nil {
		t.Fatalf("exact leave replay: %v", err)
	}
	after, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	stateAfter, stateErr := hosts[0].node.ManagedState()
	if err != nil || stateErr != nil || stateAfter.CertifiedTip != stateBefore.CertifiedTip || after.ResourceVersion() != before.ResourceVersion() {
		t.Fatalf("replay changed consensus/status: tip=%d/%d rv=%s/%s err=%v state_err=%v", stateBefore.CertifiedTip, stateAfter.CertifiedTip, before.ResourceVersion(), after.ResourceVersion(), err, stateErr)
	}
	record, ok := after.Repair()
	if !ok || record.Plan.Sequence != operator.V1Alpha2RepairLeave || record.State != "Completed" || !slices.Equal(record.Plan.FinalVoterIDs, want) {
		t.Fatalf("missing completed leave journal: %+v", record)
	}
	for i := 0; i < 3; i++ {
		snapshot, err := hosts[i].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
		if err != nil || snapshot.Current.ConfigID != 2 || len(snapshot.Current.Members) != 3 {
			t.Fatalf("survivor %d did not install target: %+v err=%v", i, snapshot.Current, err)
		}
	}
	retired := operator.V1Alpha2RepairPodRequest{PodName: "retired", PodUID: "retired-uid", NodeID: configs[3].Runtime.NodeID, PublicKey: configs[3].Runtime.Members[3].PublicKey, PeerURL: "quic://" + configs[3].Runtime.PeerAddr}
	if plan, err := hosts[0].PlanVoterReplacement(ctx, retired, append(slices.Clone(want), retired.NodeID)); err == nil || plan != nil {
		t.Fatalf("still-running retired voter was accepted: plan=%+v err=%v", plan, err)
	}
	setManagedTarget(t, fixture, 4)
	afterScale, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := hosts[0].LeaveVoter(ctx, LeaveVoterRequest{NodeID: configs[3].Runtime.NodeID}); err != nil {
		t.Fatalf("completed leave replay after desired-count increase: %v", err)
	}
	replayed, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil || replayed.ResourceVersion() != afterScale.ResourceVersion() {
		t.Fatalf("completed leave replay rewrote status after scale-up: rv=%s/%s err=%v", afterScale.ResourceVersion(), replayed.ResourceVersion(), err)
	}
	retiredID, err := hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil || len(retiredID.Current.Members) != 3 {
		t.Fatalf("retired identity observation changed roster: %+v err=%v", retiredID.Current, err)
	}
}

func TestManagedExplicitLeaveUsesSurvivingOldAndNewQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	configs, fixture, hosts := managedLeaveFixture(t)
	setManagedTarget(t, fixture, 3)
	if err := fixture.closeHost(hosts[3]); err != nil {
		t.Fatal(err)
	}
	if err := hosts[0].LeaveVoter(ctx, LeaveVoterRequest{NodeID: configs[3].Runtime.NodeID}); err != nil {
		t.Fatalf("leave with unavailable retiree: %v", err)
	}
	for i := 0; i < 3; i++ {
		snapshot, err := hosts[i].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
		if err != nil || snapshot.Current.ConfigID != 2 || len(snapshot.Current.Members) != 3 {
			t.Fatalf("survivor %d did not certify old/new quorum: %+v err=%v", i, snapshot.Current, err)
		}
	}
}

func TestManagedExplicitLeaveRequiresOldQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	configs, fixture, hosts := managedLeaveFixture(t)
	setManagedTarget(t, fixture, 3)
	if err := fixture.closeHost(hosts[2]); err != nil {
		t.Fatal(err)
	}
	if err := fixture.closeHost(hosts[3]); err != nil {
		t.Fatal(err)
	}
	if err := hosts[0].LeaveVoter(ctx, LeaveVoterRequest{NodeID: configs[2].Runtime.NodeID}); err == nil {
		t.Fatal("leave committed without the old quorum")
	}
	snapshot, err := hosts[0].node.ManagedRepairSnapshot(context.Background(), "", quepaxa.PublicKey{})
	if err != nil || snapshot.Current.ConfigID != 1 || len(snapshot.Current.Members) != 4 {
		t.Fatalf("failed leave changed membership: %+v err=%v", snapshot.Current, err)
	}
}

func TestManagedExplicitLeaveClosedPlanSurvivesDesiredCountChange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	configs, fixture, hosts := managedLeaveFixture(t)
	setManagedTarget(t, fixture, 3)
	a, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	target, err := a.TargetVoterCount()
	if err != nil {
		t.Fatal(err)
	}
	record, err := hosts[0].beginLeave(ctx, a, LeaveVoterRequest{NodeID: configs[3].Runtime.NodeID}, target)
	if err != nil || record.State != "Closed" || record.Plan.Sequence != operator.V1Alpha2RepairLeave {
		t.Fatalf("begin leave did not retain exact closed intent: %+v err=%v", record, err)
	}
	waitCurrent := func(h *Host, configID, members int) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		var last error
		for {
			snapshot, err := h.node.ManagedRepairSnapshot(context.Background(), "", quepaxa.PublicKey{})
			last = err
			if err == nil && snapshot.Current.ConfigID == uint(configID) && len(snapshot.Current.Members) == members {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("membership did not reach config=%d voters=%d: %+v err=%v", configID, members, snapshot.Current, last)
			}
			time.Sleep(30 * time.Millisecond)
		}
	}
	if err := hosts[0].RemoveRepairVoter(ctx, record.Plan.OperationID); err != nil {
		t.Fatalf("remove after retained closed intent: %v", err)
	}
	removed, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	record, ok := removed.Repair()
	if !ok || record.State != "Closed" || record.PreparedHandoff != nil || record.Completion != nil {
		t.Fatalf("REMOVE crash point did not retain closed uncaptured intent: %+v", record)
	}
	if err := fixture.closeHost(hosts[0]); err != nil {
		t.Fatal(err)
	}
	reopened, err := fixture.open(ctx, configs[0])
	if reopened != nil {
		hosts[0] = reopened
		t.Cleanup(func() {
			if err := fixture.closeHost(reopened); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatalf("reopen leave coordinator after REMOVE: %v", err)
	}
	for _, i := range []int{0, 1, 2} {
		waitCurrent(hosts[i], 2, 3)
	}
	setManagedTarget(t, fixture, 4)
	if err := hosts[0].LeaveVoter(ctx, LeaveVoterRequest{NodeID: configs[3].Runtime.NodeID}); err != nil {
		t.Fatalf("resume post-REMOVE leave after desired-count change: %v", err)
	}
	snapshot, err := hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil || snapshot.Current.ConfigID != 2 || len(snapshot.Current.Members) != 3 {
		t.Fatalf("closed-plan resume did not remove the exact voter: %+v err=%v", snapshot.Current, err)
	}
	completed, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	record, ok = completed.Repair()
	if !ok || record.State != "Completed" || record.PreparedHandoff == nil || record.Completion == nil {
		t.Fatalf("post-REMOVE resume did not complete the retained leave: %+v", record)
	}
}
