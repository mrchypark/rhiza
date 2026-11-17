//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestPendingMembershipCannotCloseReplacementReservations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	configs, fixture, hosts := managedLeaveFixture(t)
	setManagedTarget(t, fixture, 3)
	before, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if before.SQLReservationsClosed() {
		t.Fatal("fixture unexpectedly started with closed SQL reservations")
	}
	settled, err := hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		t.Fatal(err)
	}
	retired := settled.Current.Members[3]
	wal, err := hosts[0].node.ManagedVoterWALIdentity(ctx, settled.Current.ConfigID, settled.AbortSlot, retired)
	if err != nil {
		t.Fatal(err)
	}
	final := make([]types.NodeID, 0, len(settled.Current.Members)-1)
	for _, member := range settled.Current.Members {
		if member.ID != retired.ID {
			final = append(final, types.NodeID(member.ID))
		}
	}
	slices.Sort(final)
	plan := operator.V1Alpha2RepairPlan{
		Sequence: operator.V1Alpha2RepairLeave, OperationID: managedLeaveOperationID(hosts[0].identity, settled.Current.ConfigID, settled.AbortSlot, retired.ID, 3),
		ClusterUID: configs[0].Ref.UID, Generation: hosts[0].cfg.Plan.Generation, DeploymentUID: hosts[0].cfg.Plan.DeploymentUID, StorageID: hosts[0].cfg.Plan.StorageID,
		ExpectedConfigID: uint64(settled.Current.ConfigID), ExpectedAbortSlot: uint64(settled.AbortSlot), OldNodeID: retired.ID, OldWALIdentity: wal,
		Replacement:   operator.V1Alpha2RepairMember{PodName: string(retired.ID), PodUID: string(retired.ID), NodeID: retired.ID, PublicKey: retired.PublicKey, PeerURL: retired.PeerURL},
		FinalVoterIDs: final, BootstrapPlanDigest: hosts[0].cfg.Plan.BootstrapPlanDigest,
	}
	pending := settled
	pending.Pending = quepaxa.Cluster{ConfigID: settled.Current.ConfigID + 1, Members: settled.Current.Members[:3]}
	pending.PendingOK = true
	prepared, observation, err := operator.PrepareV1Alpha2Repair(ctx, configs[0].Store, before, before.Authority(), plan)
	if err != nil || observation != operator.V1Alpha2CASApplied {
		t.Fatalf("prepare full repair intent: %s %v", observation, err)
	}
	before = prepared

	_, _, err = closeReplacementAfterValidation(ctx, configs[0].Store, before, before.Authority(), plan, settled, func(context.Context) (node.ManagedRepairSnapshot, error) {
		return pending, nil
	})
	if err == nil || !strings.Contains(err.Error(), "membership changed before repair reservation close") {
		t.Fatalf("stale settled snapshot did not block reservation close: %v", err)
	}
	after, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if after.ResourceVersion() != before.ResourceVersion() || after.SQLReservationsClosed() {
		t.Fatalf("pre-CAS pending changed admission: rv=%s/%s closed=%t", before.ResourceVersion(), after.ResourceVersion(), after.SQLReservationsClosed())
	}

	calls := 0
	closed, observation, err := closeReplacementAfterValidation(ctx, configs[0].Store, before, before.Authority(), plan, settled, func(context.Context) (node.ManagedRepairSnapshot, error) {
		calls++
		if calls == 1 {
			return settled, nil
		}
		return pending, nil
	})
	if err == nil || !strings.Contains(err.Error(), "membership changed after repair reservation close") {
		t.Fatalf("post-CAS pending was not retained as an owned failure: %v", err)
	}
	if observation != operator.V1Alpha2CASApplied {
		t.Fatalf("post-CAS reproduction did not apply the close CAS: %s", observation)
	}
	record, ok := closed.Repair()
	if !ok || record.State != "Closed" || record.Plan.OperationID != plan.OperationID || record.PreparedHandoff != nil || record.Completion != nil {
		t.Fatalf("post-CAS pending did not retain exact unused repair ownership: %+v", record)
	}
}

func TestPreparingReservationSurvivesHostRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	configs, fixture := sqlCluster(t)
	fixture.mu.Lock()
	fixture.obj["status"] = json.RawMessage(`{}`)
	fixture.mu.Unlock()
	for i := range configs {
		configs[i].Runtime.EnableReconfiguration = true
	}
	n := node.New(&configs[0].Runtime)
	physical, _, err := n.PreparedStartupBindingDigestsForProtocol(configs[0].Plan.ApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := n.ManagedBootstrapPlanDigestForProtocol(physical, configs[0].Plan.EvidenceFormat, configs[0].Plan.ApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	init, err := operator.NewV1Alpha2ApplicationInitPlan(configs[0].Ref, "logical", "deployment", "assignment", physical, digest, configs[0].Plan.EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	init.ApplicationProtocol = configs[0].Plan.ApplicationProtocol
	bootstrap := operator.V1Alpha2BootstrapPlan{ClusterID: configs[0].Runtime.ClusterID, PhysicalBinding: physical, EvidenceFormat: configs[0].Plan.EvidenceFormat, Durability: types.ObjectStoreDurabilityBeforeAck, Reconfiguration: true}
	for i, member := range configs[0].Runtime.Members {
		bootstrap.Members = append(bootstrap.Members, operator.V1Alpha2BootstrapMember{PodUID: configs[i].PodUID, Member: member})
	}
	deployment, _ := operator.V1Alpha2GenesisDeploymentName(configs[0].Ref.UID)
	if _, err := operator.InitializeV1Alpha2AuthorityWithBootstrap(ctx, configs[0].Store, configs[0].Ref, deployment, init, bootstrap); err != nil {
		t.Fatal(err)
	}
	hosts := make([]*Host, 3)
	for _, i := range []int{1, 2} {
		configs[i].Plan = init
		hosts[i], err = fixture.open(ctx, configs[i])
		if err != nil {
			t.Fatal(err)
		}
		opened := hosts[i]
		t.Cleanup(func() { _ = fixture.closeHost(opened) })
	}
	configs[0].Plan = init
	writer, plan := hostRepairWriter(t, configs, hosts)
	configs[0].Store = writer
	hosts[0], err = fixture.open(ctx, configs[0])
	if err != nil {
		t.Fatal(err)
	}
	opened := hosts[0]
	t.Cleanup(func() { _ = fixture.closeHost(opened) })
	readySQL(t, hosts[0])
	if _, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "preparing-write", SQL: "CREATE TABLE preparing_write (id INTEGER)"}); err != nil {
		t.Fatal(err)
	}
	present := []types.NodeID{configs[0].Runtime.NodeID, configs[2].Runtime.NodeID, plan.Replacement.NodeID}
	planned, err := hosts[0].PlanVoterReplacement(ctx, plan.Replacement, present)
	if err != nil || planned == nil || planned.Sequence != operator.V1Alpha2RepairAddThenRemove {
		t.Fatalf("original plan: %+v err=%v", planned, err)
	}
	original := *planned
	admission, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	prepared, observation, err := operator.PrepareV1Alpha2Repair(ctx, writer, admission, admission.Authority(), original)
	if err != nil || observation != operator.V1Alpha2CASApplied || prepared.SQLReservationsClosed() {
		t.Fatalf("prepare: %s %v closed=%t", observation, err, prepared.SQLReservationsClosed())
	}
	if err := hosts[0].reserveReplacementAdd(ctx, original); err != nil {
		t.Fatal(err)
	}
	if _, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "preparing-still-open", SQL: "INSERT INTO preparing_write VALUES (1)"}); err != nil {
		t.Fatalf("write blocked while merely preparing: %v", err)
	}
	if err := fixture.closeHost(hosts[0]); err != nil {
		t.Fatal(err)
	}
	hosts[0], err = fixture.open(ctx, configs[0])
	if err != nil {
		t.Fatal(err)
	}
	readySQL(t, hosts[0])
	if _, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "preparing-restart-open", SQL: "INSERT INTO preparing_write VALUES (2)"}); err != nil {
		t.Fatalf("restart write blocked before reservation close: %v", err)
	}
	if stage, err := hosts[0].ReconcilePreparingRepairAbort(ctx); err != nil || stage != "resume" {
		t.Fatalf("exact preparing repair did not resume: stage=%s err=%v", stage, err)
	}
	transport := writer.Kube.Client.Transport
	writer.Kube.Client.Transport = repairGateTransport(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/api/v1/namespaces/ns/pods/replacement" {
			return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
		}
		return transport.RoundTrip(request)
	})
	if _, err := hosts[0].PlanVoterReplacement(ctx, operator.V1Alpha2RepairPodRequest{}, nil); err == nil || !strings.Contains(err.Error(), "candidate unavailable") {
		t.Fatalf("stale Preparing did not fail closed on missing exact candidate: %v", err)
	}
	if stage, err := hosts[0].ReconcilePreparingRepairAbort(ctx); err == nil || stage != "manual-recovery-required" {
		t.Fatalf("candidate mismatch was not exposed as manual recovery: stage=%s err=%v", stage, err)
	}
	stale, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	staleRecord, ok := stale.Repair()
	if !ok || staleRecord.State != "Preparing" || stale.SQLReservationsClosed() {
		t.Fatalf("stale Preparing changed ownership or closed writes: %+v", staleRecord)
	}
	writer.Kube.Client.Transport = transport
	resumed, err := hosts[0].PlanVoterReplacement(ctx, operator.V1Alpha2RepairPodRequest{}, nil)
	if err != nil || resumed == nil || !reflect.DeepEqual(*resumed, original) {
		t.Fatalf("restarted plan changed: %+v err=%v", resumed, err)
	}
	if err := hosts[0].reserveReplacementAdd(ctx, *resumed); err != nil {
		t.Fatalf("restart reservation was not idempotent: %v", err)
	}
	competing := *resumed
	competing.OperationID = "competing-after-restart"
	if err := hosts[0].reserveReplacementAdd(ctx, competing); err == nil {
		t.Fatal("competing operation claimed restart reservation")
	}
	snapshot, err := hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil {
		t.Fatal(err)
	}
	closed, observation, err := closeReplacementAfterValidation(ctx, writer, prepared, prepared.Authority(), *resumed, snapshot, func(ctx context.Context) (node.ManagedRepairSnapshot, error) {
		return hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	})
	if err != nil || observation != operator.V1Alpha2CASApplied || !closed.SQLReservationsClosed() {
		t.Fatalf("restart close: %s %v", observation, err)
	}
	record, ok := closed.Repair()
	if !ok || record.State != "Closed" || !reflect.DeepEqual(record.Plan, original) {
		t.Fatalf("restart changed original plan: %+v", record)
	}
	writer.Kube.Client.Transport = repairGateTransport(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/api/v1/namespaces/ns/pods/replacement" {
			return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
		}
		return transport.RoundTrip(request)
	})
	if stage, err := hosts[0].ReconcilePreparingRepairAbort(ctx); err == nil || stage != "manual-recovery-required" {
		t.Fatalf("closed no-handoff mismatch was not exposed as manual recovery: stage=%s err=%v", stage, err)
	}
	closedStale, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := closedStale.Repair(); !ok || !closedStale.SQLReservationsClosed() {
		t.Fatal("closed mismatch lost repair ownership or reopened SQL")
	}
	writer.Kube.Client.Transport = transport
}
