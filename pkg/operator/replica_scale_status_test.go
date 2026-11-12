package operator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
)

func replicaStatusObject(t *testing.T, proposal int32, step *V1Alpha2ReplicaStep, applied *int32) map[string]json.RawMessage {
	t.Helper()
	status := map[string]any{
		"authority": map[string]any{
			"owner":                   map[string]any{"namespace": "ns", "name": "cluster", "uid": "uid", "logicalID": "logical"},
			"generation":              2,
			"binding":                 map[string]any{"deploymentUID": "deployment", "storageID": "storage"},
			"everApplicationReserved": false,
			"pendingWrite":            false,
			"evidence":                map[string]any{},
		},
		"replicas":  3,
		"selector":  replicaSelector(V1Alpha2AuthorityView{Owner: V1Alpha2AuthorityOwner{Namespace: "ns", Name: "cluster", UID: "uid", LogicalID: "logical"}, Generation: 2, Binding: V1Alpha2AuthorityBinding{DeploymentUID: "deployment", StorageID: "storage"}}),
		"unrelated": map[string]any{"keep": true},
	}
	if step != nil {
		status["replicaStep"] = step
	}
	if applied != nil {
		status["appliedVoterCount"] = *applied
	}
	return map[string]json.RawMessage{
		"apiVersion": mustManagedJSON(V1Alpha2APIVersion),
		"kind":       mustManagedJSON("RhizaCluster"),
		"metadata":   mustManagedJSON(map[string]any{"name": "cluster", "namespace": "ns", "uid": "uid", "resourceVersion": "1", "labels": map[string]string{"keep": "yes"}}),
		"spec":       mustManagedJSON(map[string]any{"logicalID": "logical", "voterCount": 3, "replicas": proposal, "keep": true}),
		"status":     mustManagedJSON(status),
	}
}

func replicaCASSetup(t *testing.T, object map[string]json.RawMessage) (*V1Alpha2StatusStore, map[string]json.RawMessage, V1Alpha2AdmissionSnapshot) {
	t.Helper()
	f := &struct {
		object map[string]json.RawMessage
		puts   int
	}{object: object}
	store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet {
			return phase2Response(200, string(mustManagedJSON(f.object))), nil
		}
		if r.Method != http.MethodPut || !strings.HasSuffix(r.URL.Path, "/status") {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		f.puts++
		var next map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
			return nil, err
		}
		if !sameJSON(next["metadata"], f.object["metadata"]) || !sameJSON(next["spec"], f.object["spec"]) || !sameJSON(next["apiVersion"], f.object["apiVersion"]) || !sameJSON(next["kind"], f.object["kind"]) {
			return phase2Response(http.StatusConflict, `{}`), nil
		}
		var meta map[string]any
		if err := json.Unmarshal(next["metadata"], &meta); err != nil {
			return nil, err
		}
		rv, _ := strconv.Atoi(meta["resourceVersion"].(string))
		meta["resourceVersion"] = strconv.Itoa(rv + 1)
		next["metadata"] = mustManagedJSON(meta)
		f.object = next
		return phase2Response(200, string(mustManagedJSON(f.object))), nil
	})
	old, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	return store, f.object, old
}

func TestV1Alpha2ReplicaStepCASPreservesAndReapproves(t *testing.T) {
	ctx := context.Background()
	store, _, old := replicaCASSetup(t, replicaStatusObject(t, 4, nil, nil))
	if proposal, ok, err := old.ReplicaProposal(); err != nil || !ok || proposal != 4 {
		t.Fatalf("proposal=%d present=%t err=%v", proposal, ok, err)
	}
	if target, err := old.TargetVoterCount(); err != nil || target != 3 {
		t.Fatalf("initial target=%d err=%v", target, err)
	}
	step := V1Alpha2ReplicaStep{Generation: 2, DeploymentUID: "deployment", DeploymentResourceVersion: "91", ConfigID: 5, From: 3, To: 4, ExpiresAt: time.Now().Add(time.Minute)}
	committed, observation, err := CommitV1Alpha2ReplicaStep(ctx, store, old, old.Authority(), &step)
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("commit step: %s %v", observation, err)
	}
	if got, ok, err := committed.ReplicaStep(); err != nil || !ok || !got.ExpiresAt.Equal(step.ExpiresAt) || got.Generation != step.Generation || got.DeploymentUID != step.DeploymentUID || got.DeploymentResourceVersion != step.DeploymentResourceVersion || got.ConfigID != step.ConfigID || got.From != step.From || got.To != step.To {
		t.Fatalf("step readback=%+v present=%t err=%v", got, ok, err)
	}
	if target, err := committed.TargetVoterCount(); err != nil || target != 4 {
		t.Fatalf("step target=%d err=%v", target, err)
	}
	if _, _, err := ReportV1Alpha2ReplicaScale(ctx, store, old, old.Authority(), 3, replicaSelector(old.Authority())); !errors.Is(err, ErrV1Alpha2StatusConflict) {
		t.Fatalf("stale report: %v", err)
	}
	jsonStatus, _ := jsonObject(committed.status.object["status"])
	if !sameJSON(jsonStatus["unrelated"], mustManagedJSON(map[string]any{"keep": true})) {
		t.Fatal("unrelated status changed")
	}
	changed := step
	changed.To = 5
	if _, _, err := CommitV1Alpha2ReplicaStep(ctx, store, committed, committed.Authority(), &changed); err == nil {
		t.Fatal("changed the From/To pair midstep")
	}
	reapproved := step
	reapproved.DeploymentResourceVersion = "92"
	reapproved.ExpiresAt = step.ExpiresAt.Add(time.Minute)
	if _, observation, err := CommitV1Alpha2ReplicaStep(ctx, store, committed, committed.Authority(), &reapproved); err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("reapprove: %s %v", observation, err)
	}
}

func TestV1Alpha2ReplicaGenesisStartsAtVoterCount(t *testing.T) {
	valid, invalid, manual := phase1Cluster(), phase1Cluster(), phase1Cluster()
	valid.Spec.Replicas = phase1Int32(3)
	invalid.Spec.Replicas = phase1Int32(4)
	if err := validateV1Alpha2ReplicaGenesis(valid); err != nil {
		t.Fatal(err)
	}
	if err := validateV1Alpha2ReplicaGenesis(invalid); err == nil {
		t.Fatal("accepted an external target different from the initial voter count")
	}
	if err := validateV1Alpha2ReplicaGenesis(manual); err != nil {
		t.Fatalf("manual cluster changed: %v", err)
	}
}

func TestV1Alpha2ReplicaStepDirectionOnlyAtCreation(t *testing.T) {
	ctx := context.Background()
	// HPA has reversed the proposal while the 3->4 pair remains active.
	step := V1Alpha2ReplicaStep{Generation: 2, DeploymentUID: "deployment", DeploymentResourceVersion: "91", ConfigID: 5, From: 3, To: 4, ExpiresAt: time.Now().Add(time.Minute)}
	store, _, committed := replicaCASSetup(t, replicaStatusObject(t, 3, &step, nil))
	reapproved := step
	reapproved.ExpiresAt = step.ExpiresAt.Add(time.Minute)
	if _, observation, err := CommitV1Alpha2ReplicaStep(ctx, store, committed, committed.Authority(), &reapproved); err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("reapproval after proposal reversal: %s %v", observation, err)
	}
}

func TestV1Alpha2ReplicaDownCheckIsFreshAndOneUse(t *testing.T) {
	ctx := context.Background()
	oldStep := V1Alpha2ReplicaStep{Generation: 2, DeploymentUID: "deployment", DeploymentResourceVersion: "90", ConfigID: 5, From: 4, To: 3, ExpiresAt: time.Now().Add(-time.Minute)}
	store, _, old := replicaCASSetup(t, replicaStatusObject(t, 3, &oldStep, nil))
	check := V1Alpha2ReplicaCheck{ID: "fresh-check", DeploymentResourceVersion: "91", ExpiresAt: time.Now().Add(time.Minute)}
	withCheck, observation, err := BeginV1Alpha2ReplicaCheck(ctx, store, old, old.Authority(), check)
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("begin check: %s %v", observation, err)
	}
	gotCheck, ok, err := withCheck.ReplicaCheck()
	if err != nil || !ok || gotCheck.ID != check.ID || gotCheck.DeploymentResourceVersion != check.DeploymentResourceVersion {
		t.Fatalf("check readback=%+v present=%t err=%v", gotCheck, ok, err)
	}
	gotStep, ok, err := withCheck.ReplicaStep()
	if err != nil || !ok || gotStep.CheckID != "" {
		t.Fatalf("old proof was not cleared: step=%+v present=%t err=%v", gotStep, ok, err)
	}
	bad := oldStep
	bad.CheckID = "old-check"
	bad.DeploymentResourceVersion = "91"
	bad.ExpiresAt = time.Now().Add(time.Minute)
	if _, _, err := CommitV1Alpha2ReplicaStep(ctx, store, withCheck, withCheck.Authority(), &bad); err == nil {
		t.Fatal("mismatched proof was accepted")
	}
	proof := oldStep
	proof.CheckID = check.ID
	proof.DeploymentResourceVersion = check.DeploymentResourceVersion
	proof.ExpiresAt = time.Now().Add(time.Minute)
	proven, observation, err := CommitV1Alpha2ReplicaStep(ctx, store, withCheck, withCheck.Authority(), &proof)
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("fresh proof: %s %v", observation, err)
	}
	abandoned, observation, err := AbandonV1Alpha2ReplicaCheck(ctx, store, proven, proven.Authority(), check.ID)
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("abandon old check: %s %v", observation, err)
	}
	nextCheck := V1Alpha2ReplicaCheck{ID: "next-check", DeploymentResourceVersion: "92", ExpiresAt: time.Now().Add(time.Minute)}
	replaced, observation, err := BeginV1Alpha2ReplicaCheck(ctx, store, abandoned, abandoned.Authority(), nextCheck)
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("replace check: %s %v", observation, err)
	}
	if _, _, err := CommitV1Alpha2ReplicaStep(ctx, store, replaced, replaced.Authority(), &proof); err == nil {
		t.Fatal("old proof replayed after challenge replacement")
	}
}

func TestV1Alpha2ReplicaCheckConcurrentBeginKeepsFirst(t *testing.T) {
	ctx := context.Background()
	step := V1Alpha2ReplicaStep{Generation: 2, DeploymentUID: "deployment", DeploymentResourceVersion: "91", ConfigID: 5, From: 4, To: 3, ExpiresAt: time.Now().Add(-time.Minute)}
	store, _, old := replicaCASSetup(t, replicaStatusObject(t, 3, &step, nil))
	first := V1Alpha2ReplicaCheck{ID: "first-owner", DeploymentResourceVersion: "91", ExpiresAt: time.Now().Add(time.Minute)}
	withFirst, observation, err := BeginV1Alpha2ReplicaCheck(ctx, store, old, old.Authority(), first)
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("first begin: %s %v", observation, err)
	}
	before := withFirst.ResourceVersion()
	second := V1Alpha2ReplicaCheck{ID: "second-owner", DeploymentResourceVersion: "91", ExpiresAt: time.Now().Add(time.Minute)}
	if _, _, err := BeginV1Alpha2ReplicaCheck(ctx, store, withFirst, withFirst.Authority(), second); err == nil {
		t.Fatal("second active challenge replaced the first")
	}
	live, err := LoadV1Alpha2Admission(ctx, store, phase2Ref)
	if err != nil || live.ResourceVersion() != before {
		t.Fatalf("denied begin wrote status: rv=%s want=%s err=%v", live.ResourceVersion(), before, err)
	}
	check, ok, err := live.ReplicaCheck()
	if err != nil || !ok || check.ID != first.ID {
		t.Fatalf("first check lost: %+v present=%t err=%v", check, ok, err)
	}
}

func TestV1Alpha2ReplicaRepairStepGuard(t *testing.T) {
	guard := func(proposal *int32, step *V1Alpha2ReplicaStep) V1Alpha2AdmissionSnapshot {
		spec := map[string]any{"logicalID": "logical", "voterCount": 3}
		status := map[string]any{}
		if proposal != nil {
			spec["replicas"] = *proposal
		}
		if step != nil {
			status["replicaStep"] = step
		}
		view := V1Alpha2AuthorityView{Owner: V1Alpha2AuthorityOwner{Namespace: "ns", Name: "cluster", UID: "uid", LogicalID: "logical"}, Generation: 2, Binding: V1Alpha2AuthorityBinding{DeploymentUID: "deployment", StorageID: "storage"}}
		return V1Alpha2AdmissionSnapshot{status: V1Alpha2StatusSnapshot{authority: view, object: map[string]json.RawMessage{"spec": mustManagedJSON(spec), "status": mustManagedJSON(status)}}}
	}
	up := V1Alpha2ReplicaStep{Generation: 2, DeploymentUID: "deployment", DeploymentResourceVersion: "91", ConfigID: 5, From: 3, To: 4, ExpiresAt: time.Now().Add(time.Minute)}
	down := V1Alpha2ReplicaStep{Generation: 2, DeploymentUID: "deployment", DeploymentResourceVersion: "91", ConfigID: 5, From: 4, To: 3, ExpiresAt: time.Now().Add(time.Minute)}
	plan := func(sequence V1Alpha2RepairSequence, config uint64, final []types.NodeID) V1Alpha2RepairPlan {
		return V1Alpha2RepairPlan{Sequence: sequence, ExpectedConfigID: config, FinalVoterIDs: final}
	}
	for _, tt := range []struct {
		name     string
		snapshot V1Alpha2AdmissionSnapshot
		plan     V1Alpha2RepairPlan
		wantErr  bool
	}{
		{"manual add", guard(nil, nil), plan(V1Alpha2RepairAdd, 5, make([]types.NodeID, 4)), false},
		{"no step add", guard(phase1Int32(4), nil), plan(V1Alpha2RepairAdd, 5, make([]types.NodeID, 4)), true},
		{"no step leave", guard(phase1Int32(3), nil), plan(V1Alpha2RepairLeave, 5, make([]types.NodeID, 3)), true},
		{"no step replacement", guard(phase1Int32(3), nil), plan(V1Alpha2RepairAddThenRemove, 5, make([]types.NodeID, 3)), false},
		{"up exact", guard(phase1Int32(5), &up), plan(V1Alpha2RepairAdd, 5, make([]types.NodeID, 4)), false},
		{"up wrong sequence", guard(phase1Int32(5), &up), plan(V1Alpha2RepairLeave, 5, make([]types.NodeID, 4)), true},
		{"up wrong config", guard(phase1Int32(5), &up), plan(V1Alpha2RepairAdd, 6, make([]types.NodeID, 4)), true},
		{"down exact", guard(phase1Int32(3), &down), plan(V1Alpha2RepairLeave, 5, make([]types.NodeID, 3)), false},
		{"down wrong final", guard(phase1Int32(3), &down), plan(V1Alpha2RepairLeave, 5, make([]types.NodeID, 4)), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateV1Alpha2ReplicaRepairStep(tt.snapshot, tt.plan); (err != nil) != tt.wantErr {
				t.Fatalf("error=%v wantErr=%t", err, tt.wantErr)
			}
		})
	}
	completed := guard(phase1Int32(5), &up)
	completed.status.repair = &V1Alpha2RepairRecord{Plan: upPlan(up), State: "Completed", Completion: &V1Alpha2RepairCompletion{ConfigID: up.ConfigID + 1}}
	if err := validateV1Alpha2ReplicaRepairStep(completed, plan(V1Alpha2RepairAddThenRemove, up.ConfigID, make([]types.NodeID, 3))); err == nil {
		t.Fatal("completed step allowed another plan before Host clear")
	}
}

func upPlan(step V1Alpha2ReplicaStep) V1Alpha2RepairPlan {
	return V1Alpha2RepairPlan{Sequence: V1Alpha2RepairAdd, Generation: step.Generation, DeploymentUID: step.DeploymentUID, ExpectedConfigID: step.ConfigID, FinalVoterIDs: make([]types.NodeID, step.To)}
}

func TestV1Alpha2ReplicaStepClearRequiresExactTerminal(t *testing.T) {
	ctx := context.Background()
	f, old := repairTestSetup(t)
	spec, _ := jsonObject(f.cluster["spec"])
	spec["replicas"] = mustManagedJSON(4)
	f.cluster["spec"] = mustManagedJSON(spec)
	old, err := LoadV1Alpha2Admission(ctx, f.store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	reported, observation, err := ReportV1Alpha2ReplicaScale(ctx, f.store, old, old.Authority(), 3, replicaSelector(old.Authority()))
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("report scale: %s %v", observation, err)
	}
	step := V1Alpha2ReplicaStep{Generation: old.Authority().Generation, DeploymentUID: old.Authority().Binding.DeploymentUID, DeploymentResourceVersion: "91", ConfigID: f.plan.ExpectedConfigID, From: 3, To: 4, ExpiresAt: time.Now().Add(time.Minute)}
	begun, observation, err := CommitV1Alpha2ReplicaStep(ctx, f.store, reported, reported.Authority(), &step)
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("commit external step: %s %v", observation, err)
	}
	if _, _, err := BeginV1Alpha2Repair(ctx, f.store, begun, begun.Authority(), f.plan); err == nil {
		t.Fatal("up step accepted an ADD-first replacement instead of exact ADD")
	}
	bootstrap, err := begun.status.BootstrapPlan()
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Sequence = V1Alpha2RepairAdd
	f.plan.FinalVoterIDs = nil
	for _, member := range bootstrap.Members {
		f.plan.FinalVoterIDs = append(f.plan.FinalVoterIDs, types.NodeID(member.Member.ID))
	}
	f.plan.FinalVoterIDs = append(f.plan.FinalVoterIDs, f.plan.Replacement.NodeID)
	slices.Sort(f.plan.FinalVoterIDs)
	closed, observation, err := BeginV1Alpha2Repair(ctx, f.store, begun, begun.Authority(), f.plan)
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("begin exact up step: %s %v", observation, err)
	}
	reapproved := step
	reapproved.DeploymentResourceVersion = "92"
	reapproved.ExpiresAt = step.ExpiresAt.Add(time.Minute)
	reapprovedSnapshot, observation, err := CommitV1Alpha2ReplicaStep(ctx, f.store, closed, closed.Authority(), &reapproved)
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("persist step through exact closed repair: %s %v", observation, err)
	}
	handoff := addFirstHandoff(t, f, reapprovedSnapshot)
	captured, observation, err := CaptureV1Alpha2RepairHandoff(ctx, f.store, reapprovedSnapshot, reapprovedSnapshot.Authority(), handoff)
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("capture: %s %v", observation, err)
	}
	completion := addFirstCompletion(f.plan, handoff)
	finished, observation, err := FinishV1Alpha2Repair(ctx, f.store, captured, captured.Authority(), completion)
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("finish exact terminal: %s %v", observation, err)
	}
	cleared, observation, err := CommitV1Alpha2ReplicaStep(ctx, f.store, finished, finished.Authority(), nil)
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("clear exact terminal: %s %v", observation, err)
	}
	if _, ok, err := cleared.ReplicaStep(); err != nil || ok {
		t.Fatalf("step remained: present=%t err=%v", ok, err)
	}
	if target, err := cleared.TargetVoterCount(); err != nil || target != 4 {
		t.Fatalf("applied target=%d err=%v", target, err)
	}
}
