package operator

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func addFirstHandoff(t *testing.T, f *repairFixture, closed V1Alpha2AdmissionSnapshot) V1Alpha2RepairHandoff {
	t.Helper()
	init, err := LoadV1Alpha2ApplicationInit(context.Background(), f.store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := closed.status.BootstrapPlan()
	if err != nil {
		t.Fatal(err)
	}
	_, prefix, err := objstore.ManagedNamespacePrefixes("", f.plan.ClusterUID, f.plan.Generation, init.Assignment().Plan.AssignmentID, string(bootstrap.ClusterID))
	if err != nil {
		t.Fatal(err)
	}
	source := quepaxa.Cluster{ConfigID: uint(f.plan.ExpectedConfigID)}
	for _, member := range bootstrap.Members {
		m := member.Member
		if types.NodeID(m.ID) == f.plan.OldNodeID {
			m.WALIdentity = f.plan.OldWALIdentity
		}
		source.Members = append(source.Members, m)
	}
	head := []byte("fixed ADD-first prepared source")
	sourceHash := sha256.Sum256([]byte("certified prepared source anchor"))
	return V1Alpha2RepairHandoff{
		PlanDigest:      f.plan.Digest(),
		SourcePrefix:    prefix,
		PhysicalBinding: init.Assignment().Plan.PhysicalBinding,
		Head:            head,
		HeadHash:        fmt.Sprintf("%x", sha256.Sum256(head)),
		PreparedConfig:  &source,
		PreparedSlot:    f.plan.ExpectedAbortSlot + 5,
		PreparedHash:    fmt.Sprintf("%x", sourceHash),
	}
}

func addFirstCompletion(plan V1Alpha2RepairPlan, handoff V1Alpha2RepairHandoff) V1Alpha2RepairCompletion {
	ids := make([]types.NodeID, 0, len(handoff.PreparedConfig.Members)+1)
	for _, member := range handoff.PreparedConfig.Members {
		ids = append(ids, types.NodeID(member.ID))
	}
	ids = append(ids, plan.Replacement.NodeID)
	slices.Sort(ids)
	return V1Alpha2RepairCompletion{
		OperationID:       plan.OperationID,
		PlanDigest:        plan.Digest(),
		ExpectedConfigID:  plan.ExpectedConfigID,
		ExpectedAbortSlot: plan.ExpectedAbortSlot,
		Kind:              "Terminal",
		ConfigID:          plan.ExpectedConfigID + 1,
		Slot:              handoff.PreparedSlot + 2,
		Hash:              strings.Repeat("c", 64),
		FinalVoterIDs:     ids,
	}
}

func TestRepairV1Alpha2AddFirstPersistsPreparedSourceAndADDPhase(t *testing.T) {
	ctx := context.Background()
	f, old := repairTestSetup(t)
	f.plan.Sequence = V1Alpha2RepairAddThenRemove
	closed, obs, err := BeginV1Alpha2Repair(ctx, f.store, old, old.Authority(), f.plan)
	if err != nil || obs != V1Alpha2CASApplied || !closed.SQLReservationsClosed() {
		t.Fatalf("begin ADD-first repair: observation=%s error=%v", obs, err)
	}

	handoff := addFirstHandoff(t, f, closed)
	for name, mutate := range map[string]func(*V1Alpha2RepairHandoff){
		"wrong plan digest": func(h *V1Alpha2RepairHandoff) { h.PlanDigest = strings.Repeat("d", 64) },
		"wrong source binding": func(h *V1Alpha2RepairHandoff) { h.PhysicalBinding = strings.Repeat("e", 64) },
		"stale source config": func(h *V1Alpha2RepairHandoff) { h.PreparedConfig.ConfigID-- },
		"old voter absent": func(h *V1Alpha2RepairHandoff) {
			h.PreparedConfig.Members = slices.DeleteFunc(h.PreparedConfig.Members, func(m quepaxa.Member) bool { return types.NodeID(m.ID) == f.plan.OldNodeID })
		},
		"old WAL changed": func(h *V1Alpha2RepairHandoff) {
			for i := range h.PreparedConfig.Members {
				if types.NodeID(h.PreparedConfig.Members[i].ID) == f.plan.OldNodeID {
					h.PreparedConfig.Members[i].WALIdentity = "different-registered-wal"
				}
			}
		},
		"malformed prepared hash": func(h *V1Alpha2RepairHandoff) { h.PreparedHash = "not-a-digest" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := handoff
			config := *handoff.PreparedConfig
			config.Members = slices.Clone(handoff.PreparedConfig.Members)
			bad.PreparedConfig = &config
			mutate(&bad)
			before := f.puts
			if _, _, err := CaptureV1Alpha2RepairHandoff(ctx, f.store, closed, closed.Authority(), bad); err == nil || f.puts != before {
				t.Fatalf("invalid prepared source wrote status: %v puts=%d", err, f.puts-before)
			}
		})
	}
	captured, obs, err := CaptureV1Alpha2RepairHandoff(ctx, f.store, closed, closed.Authority(), handoff)
	if err != nil || obs != V1Alpha2CASApplied {
		t.Fatalf("capture prepared source: observation=%s error=%v", obs, err)
	}
	if _, _, err := CaptureV1Alpha2RepairHandoff(ctx, f.store, closed, closed.Authority(), handoff); err == nil {
		t.Fatal("stale snapshot overwrote the prepared source")
	}

	final := repairCompletion(f.plan, "Terminal")
	if _, _, err := FinishV1Alpha2Repair(ctx, f.store, captured, captured.Authority(), final); err == nil {
		t.Fatal("final repair completed before the ADD terminal was journaled")
	}
	add := addFirstCompletion(f.plan, handoff)
	for name, mutate := range map[string]func(*V1Alpha2RepairCompletion){
		"wrong config": func(c *V1Alpha2RepairCompletion) { c.ConfigID++ },
		"wrong target": func(c *V1Alpha2RepairCompletion) { c.FinalVoterIDs = slices.Clone(c.FinalVoterIDs[:2]) },
		"wrong operation": func(c *V1Alpha2RepairCompletion) { c.OperationID = "other-operation" },
		"wrong slot": func(c *V1Alpha2RepairCompletion) { c.Slot = handoff.PreparedSlot },
		"wrong plan digest": func(c *V1Alpha2RepairCompletion) { c.PlanDigest = strings.Repeat("0", 64) },
	} {
		t.Run("add-proof-"+name, func(t *testing.T) {
			bad := add
			bad.FinalVoterIDs = slices.Clone(add.FinalVoterIDs)
			mutate(&bad)
			before := f.puts
			if _, _, err := RecordV1Alpha2RepairAddition(ctx, f.store, captured, captured.Authority(), bad); err == nil || f.puts != before {
				t.Fatalf("invalid ADD terminal wrote status: %v puts=%d", err, f.puts-before)
			}
		})
	}

	added, obs, err := RecordV1Alpha2RepairAddition(ctx, f.store, captured, captured.Authority(), add)
	if err != nil || obs != V1Alpha2CASApplied {
		t.Fatalf("record ADD terminal: observation=%s error=%v", obs, err)
	}
	beforeRepeat := f.puts
	if _, _, err := RecordV1Alpha2RepairAddition(ctx, f.store, added, added.Authority(), add); err == nil || f.puts != beforeRepeat {
		t.Fatalf("duplicate ADD proof should resume from the retained record without a write: %v", err)
	}
	live, err := LoadV1Alpha2Admission(ctx, f.store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	record, ok := live.Repair()
	if !ok || record.State != "Closed" || record.AddCompletion == nil || !reflect.DeepEqual(*record.AddCompletion, add) || !live.SQLReservationsClosed() {
		t.Fatal("ADD phase was not durably retained under the same closed operation")
	}
	changed := add
	changed.FinalVoterIDs = slices.Clone(add.FinalVoterIDs)
	changed.Slot++
	before := f.puts
	if _, _, err := RecordV1Alpha2RepairAddition(ctx, f.store, added, added.Authority(), changed); err == nil || f.puts != before {
		t.Fatalf("changed ADD proof replaced the persisted terminal: %v", err)
	}

	for name, slot := range map[string]uint64{"equal": add.Slot, "before": add.Slot - 1} {
		t.Run("final-slot-"+name, func(t *testing.T) {
			bad := final
			bad.Slot = slot
			before := f.puts
			if _, _, err := FinishV1Alpha2Repair(ctx, f.store, added, added.Authority(), bad); err == nil || f.puts != before {
				t.Fatalf("out-of-order final terminal wrote status: %v puts=%d", err, f.puts-before)
			}
			malformedRecord := record
			malformedRecord.State = "Completed"
			malformedRecord.Completion = &bad
			status, err := jsonObject(added.status.object["status"])
			if err != nil {
				t.Fatal(err)
			}
			status["repair"], err = json.Marshal(malformedRecord)
			if err != nil {
				t.Fatal(err)
			}
			malformed := added.status
			malformed.object = make(map[string]json.RawMessage, len(added.status.object))
			for key, value := range added.status.object {
				malformed.object[key] = value
			}
			malformed.object["status"], err = json.Marshal(status)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := parseV1Alpha2Repair(malformed); err == nil || f.puts != before {
				t.Fatalf("parser accepted out-of-order final terminal or wrote status: %v", err)
			}
		})
	}
	finished, obs, err := FinishV1Alpha2Repair(ctx, f.store, added, added.Authority(), final)
	if err != nil || obs != V1Alpha2CASApplied || finished.SQLReservationsClosed() {
		t.Fatalf("finish after verified ADD: observation=%s error=%v", obs, err)
	}
	finalRecord, ok := finished.Repair()
	if !ok || finalRecord.State != "Completed" || finalRecord.AddCompletion == nil || !reflect.DeepEqual(*finalRecord.AddCompletion, add) || finalRecord.Completion == nil || !reflect.DeepEqual(*finalRecord.Completion, final) {
		t.Fatal("completed record lost its ADD phase proof")
	}
}

func TestRepairV1Alpha2CompletedAddFirstRejectsNextRemoveFirstOperation(t *testing.T) {
	ctx := context.Background()
	f, old := repairTestSetup(t)
	f.plan.Sequence = V1Alpha2RepairAddThenRemove
	closed, _, err := BeginV1Alpha2Repair(ctx, f.store, old, old.Authority(), f.plan)
	if err != nil {
		t.Fatal(err)
	}
	handoff := addFirstHandoff(t, f, closed)
	captured, _, err := CaptureV1Alpha2RepairHandoff(ctx, f.store, closed, closed.Authority(), handoff)
	if err != nil {
		t.Fatal(err)
	}
	added, _, err := RecordV1Alpha2RepairAddition(ctx, f.store, captured, captured.Authority(), addFirstCompletion(f.plan, handoff))
	if err != nil {
		t.Fatal(err)
	}
	completed, _, err := FinishV1Alpha2Repair(ctx, f.store, added, added.Authority(), repairCompletion(f.plan, "Terminal"))
	if err != nil {
		t.Fatal(err)
	}
	prior, ok := completed.Repair()
	if !ok || prior.State != "Completed" || prior.AddCompletion == nil || prior.Completion == nil {
		t.Fatal("first ADD-first repair did not retain its terminal evidence")
	}

	bootstrap, err := completed.status.BootstrapPlan()
	if err != nil {
		t.Fatal(err)
	}
	next := f.plan
	next.OperationID = "repair-2"
	next.Sequence = ""
	next.ExpectedConfigID = prior.Completion.ConfigID
	next.ExpectedAbortSlot = prior.Completion.Slot
	next.OldNodeID = bootstrap.Members[1].Member.ID
	next.OldWALIdentity = "next-registered-old-wal"
	meta := f.pod["metadata"].(map[string]any)
	meta["name"] = "replacement-2"
	meta["uid"] = "replacement-uid-2"
	podStatus := f.pod["status"].(map[string]any)
	podStatus["podIP"] = "10.0.0.2"
	var nextKey quepaxa.PublicKey = next.Replacement.PublicKey
	nextKey[0] ^= 0xff
	nextNodeID, err := types.ManagedNodeID("replacement-uid-2")
	if err != nil {
		t.Fatal(err)
	}
	initStatuses := podStatus["initContainerStatuses"].([]any)
	initState := initStatuses[0].(map[string]any)["state"].(map[string]any)
	initState["terminated"].(map[string]any)["message"] = fmt.Sprintf(`{"version":1,"publicKey":%q}`, base64.StdEncoding.EncodeToString(nextKey[:]))
	next.Replacement = V1Alpha2RepairMember{
		PodName: "replacement-2", PodUID: "replacement-uid-2", NodeID: nextNodeID,
		PublicKey: nextKey, PeerURL: "quic://10.0.0.2:9090",
	}
	next.FinalVoterIDs = slices.Clone(f.plan.FinalVoterIDs)
	next.FinalVoterIDs = slices.DeleteFunc(next.FinalVoterIDs, func(id types.NodeID) bool { return id == next.OldNodeID })
	next.FinalVoterIDs = append(next.FinalVoterIDs, next.Replacement.NodeID)
	slices.Sort(next.FinalVoterIDs)

	beforePuts := f.puts
	if _, _, err := BeginV1Alpha2Repair(ctx, f.store, completed, completed.Authority(), next); err == nil || !strings.Contains(err.Error(), "requires ADD-first") || f.puts != beforePuts {
		t.Fatalf("new REMOVE-first operation was admitted after terminal ADD-first: %v puts=%d", err, f.puts-beforePuts)
	}
	retained, err := LoadV1Alpha2Admission(ctx, f.store, phase2Ref)
	if err != nil { t.Fatal(err) }
	record, ok := retained.Repair()
	if !ok || !reflect.DeepEqual(record, prior) { t.Fatal("rejected successor changed prior terminal/ADD proof") }
	next.Sequence = V1Alpha2RepairAddThenRemove
	nextClosed, _, err := BeginV1Alpha2Repair(ctx, f.store, completed, completed.Authority(), next)
	if err != nil { t.Fatalf("new ADD-first successor rejected: %v", err) }
	nextRecord, ok := nextClosed.Repair()
	if !ok || nextRecord.State != "Closed" || nextRecord.Plan.Sequence != V1Alpha2RepairAddThenRemove || nextRecord.AddCompletion != nil || nextRecord.Plan.OperationID != next.OperationID {
		t.Fatal("new ADD-first successor inherited prior proof or wrong sequence")
	}

}

func TestRepairV1Alpha2HistoricalEmptySequenceRemainsReadableButNewBeginRefuses(t *testing.T) {
	f, old := repairTestSetup(t)
	f.plan.Sequence = ""
	if repairSequence(f.plan) != V1Alpha2RepairRemoveThenAdd { t.Fatal("historical empty sequence changed interpretation") }
	for _, sequence := range []V1Alpha2RepairSequence{"", V1Alpha2RepairRemoveThenAdd} {
		f.plan.Sequence = sequence
		if _, _, err := BeginV1Alpha2Repair(context.Background(), f.store, old, old.Authority(), f.plan); err == nil || !strings.Contains(err.Error(), "requires ADD-first") || f.puts != 0 {
			t.Fatalf("new sequence %q wrote status: %v puts=%d", sequence, err, f.puts)
		}
	}
	f.plan.Sequence = ""
	closed := historicalClosedRepair(t, f, old)
	record, exists := closed.Repair()
	if !exists || record.Plan.Sequence != "" || repairSequence(record.Plan) != V1Alpha2RepairRemoveThenAdd {
		t.Fatal("historical Closed record became unreadable or was silently converted")
	}
	if _, _, err := RecordV1Alpha2RepairAddition(context.Background(), f.store, closed, closed.Authority(), V1Alpha2RepairCompletion{}); err == nil {
		t.Fatal("historical REMOVE-first accepted ADD-first proof")
	}
}

func TestRepairV1Alpha2RejectsUnknownSequenceWithoutWrite(t *testing.T) {
	f, old := repairTestSetup(t)
	f.plan.Sequence = V1Alpha2RepairSequence("removeAndReplaceSomehow")
	if _, _, err := BeginV1Alpha2Repair(context.Background(), f.store, old, old.Authority(), f.plan); err == nil || f.puts != 0 {
		t.Fatalf("unknown sequence was accepted or persisted: %v puts=%d", err, f.puts)
	}
}
