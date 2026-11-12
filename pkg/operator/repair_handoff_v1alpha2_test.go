package operator

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"gopkg.in/yaml.v3"
)

func TestRepairV1Alpha2RemoveFirstJSONRemainsStable(t *testing.T) {
	plan := V1Alpha2RepairPlan{
		OperationID: "op", ClusterUID: "cluster", Generation: 1, DeploymentUID: "dep", StorageID: "store",
		ExpectedConfigID: 2, ExpectedAbortSlot: 3, OldNodeID: "old", OldWALIdentity: "wal",
		Replacement:         V1Alpha2RepairMember{PodName: "pod", PodUID: "uid", NodeID: "node", PeerURL: "quic://127.0.0.1:1"},
		BootstrapPlanDigest: "digest", FinalVoterIDs: []types.NodeID{"new"},
	}
	planJSON, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	wantPlan := `{"operationID":"op","clusterUID":"cluster","generation":1,"deploymentUID":"dep","storageID":"store","expectedConfigID":2,"expectedAbortSlot":3,"oldNodeID":"old","oldWALIdentity":"wal","replacement":{"podName":"pod","podUID":"uid","nodeID":"node","publicKey":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","peerURL":"quic://127.0.0.1:1"},"bootstrapPlanDigest":"digest","finalVoterIDs":["new"]}`
	if string(planJSON) != wantPlan {
		t.Fatalf("legacy plan JSON changed:\n got %s\nwant %s", planJSON, wantPlan)
	}
	recordJSON, err := json.Marshal(V1Alpha2RepairRecord{Plan: plan, State: "Closed"})
	if err != nil {
		t.Fatal(err)
	}
	var legacyRecord map[string]json.RawMessage
	if err := json.Unmarshal(recordJSON, &legacyRecord); err != nil {
		t.Fatal(err)
	}
	if _, exists := legacyRecord["addCompletion"]; exists {
		t.Fatal("legacy REMOVE-first record gained an empty ADD completion field")
	}

	handoff := V1Alpha2RepairHandoff{
		PlanDigest: "d", SourceRoot: "/src", SourcePrefix: "prefix", PhysicalBinding: "p",
		Head: []byte("H"), HeadHash: "h", RemoveTerminalSlot: 9, RemoveTerminalHash: "r",
		Survivors: quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{}},
	}
	handoffJSON, err := json.Marshal(handoff)
	if err != nil {
		t.Fatal(err)
	}
	wantHandoff := `{"planDigest":"d","sourceRoot":"/src","sourcePrefix":"prefix","physicalBinding":"p","head":"SA==","headHash":"h","removeTerminalSlot":9,"removeTerminalHash":"r","survivors":{"config_id":2,"members":[]}}`
	if string(handoffJSON) != wantHandoff {
		t.Fatalf("legacy handoff JSON changed:\n got %s\nwant %s", handoffJSON, wantHandoff)
	}

	plan.Sequence = V1Alpha2RepairAddThenRemove
	addPlanJSON, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	var addPlan map[string]json.RawMessage
	if err := json.Unmarshal(addPlanJSON, &addPlan); err != nil {
		t.Fatal(err)
	}
	if string(addPlan["sequence"]) != `"addThenRemove"` {
		t.Fatal("ADD-first sequence was omitted")
	}
	addHandoff := V1Alpha2RepairHandoff{PlanDigest: "d", SourceRoot: "/src", SourcePrefix: "prefix", PhysicalBinding: "p", Head: []byte("H"), HeadHash: "h", PreparedConfig: &quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{{ID: "a", PublicKey: quepaxa.PublicKey{1}}, {ID: "b", PublicKey: quepaxa.PublicKey{2}}}}, PreparedSlot: 4, PreparedHash: strings.Repeat("a", 64)}
	addHandoffJSON, err := json.Marshal(addHandoff)
	if err != nil {
		t.Fatal(err)
	}
	var addHandoffFields map[string]json.RawMessage
	if err := json.Unmarshal(addHandoffJSON, &addHandoffFields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"removeTerminalSlot", "removeTerminalHash", "survivors"} {
		if _, exists := addHandoffFields[field]; exists {
			t.Fatalf("ADD-first JSON included REMOVE-only field %q", field)
		}
	}
	for _, field := range []string{"preparedConfig", "preparedSlot", "preparedHash"} {
		if _, exists := addHandoffFields[field]; !exists {
			t.Fatalf("ADD-first JSON omitted prepared-source field %q", field)
		}
	}
}

// Status fixtures below are trusted-caller assertions, not consensus proof.
// The managed Host test separately captures and verifies a real archived REMOVE.
func TestRepairV1Alpha2HandoffWriteOnceAndBindings(t *testing.T) {
	ctx := context.Background()
	f, old := repairTestSetup(t)
	f.plan.Sequence = ""
	closed := historicalClosedRepair(t, f, old)
	init, err := LoadV1Alpha2ApplicationInit(ctx, f.store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := closed.status.BootstrapPlan()
	if err != nil {
		t.Fatal(err)
	}
	_, prefix, err := objectstore.ManagedNamespacePrefixes("", f.plan.ClusterUID, f.plan.Generation, init.Assignment().Plan.AssignmentID, string(bootstrap.ClusterID))
	if err != nil {
		t.Fatal(err)
	}
	head := []byte("trusted-fixture-head")
	h := V1Alpha2RepairHandoff{PlanDigest: f.plan.Digest(), SourcePrefix: prefix, PhysicalBinding: init.Assignment().Plan.PhysicalBinding, Head: head, HeadHash: fmt.Sprintf("%x", sha256.Sum256(head)), RemoveTerminalSlot: 17, RemoveTerminalHash: strings.Repeat("a", 64), Survivors: quepaxa.Cluster{ConfigID: 2}}
	for _, m := range bootstrap.Members {
		if m.Member.ID != f.plan.OldNodeID {
			h.Survivors.Members = append(h.Survivors.Members, m.Member)
		}
	}
	for _, name := range []string{"prefix", "physical", "digest", "head-size", "record-size", "hash", "survivors", "terminal", "config"} {
		t.Run(name, func(t *testing.T) {
			bad := h
			bad.Survivors.Members = slices.Clone(h.Survivors.Members)
			switch name {
			case "prefix":
				bad.SourcePrefix = "other/" + prefix
			case "physical":
				bad.PhysicalBinding = strings.Repeat("f", 64)
			case "digest":
				bad.PlanDigest = strings.Repeat("f", 64)
			case "head-size":
				bad.Head = make([]byte, V1Alpha2RepairHeadLimit+1)
				bad.HeadHash = fmt.Sprintf("%x", sha256.Sum256(bad.Head))
			case "record-size":
				bad.Survivors.Members[0].URL = strings.Repeat("x", V1Alpha2RepairRecordLimit)
			case "config":
				bad.Survivors.ConfigID++
			case "hash":
				bad.HeadHash = strings.Repeat("f", 64)
			case "survivors":
				bad.Survivors.Members[0].ID = f.plan.OldNodeID
			case "terminal":
				bad.RemoveTerminalSlot = 0
			}
			before := f.puts
			if _, _, err := CaptureV1Alpha2RepairHandoff(ctx, f.store, closed, closed.Authority(), bad); err == nil || f.puts != before {
				t.Fatalf("invalid capture wrote status: %v", err)
			}
		})
	}
	if _, _, err := CaptureV1Alpha2RepairHandoff(ctx, f.store, old, old.Authority(), h); err == nil {
		t.Fatal("capture before closed repair accepted")
	}
	captured, obs, err := CaptureV1Alpha2RepairHandoff(ctx, f.store, closed, closed.Authority(), h)
	if err != nil || obs != V1Alpha2CASApplied {
		t.Fatalf("capture: %s %v", obs, err)
	}
	if _, _, err := CaptureV1Alpha2RepairHandoff(ctx, f.store, captured, captured.Authority(), h); err == nil {
		t.Fatal("capture overwritten")
	}
	r, _ := captured.Repair()
	r.PreparedHandoff.Head[0] ^= 1
	r2, _ := captured.Repair()
	if r2.PreparedHandoff.HeadHash != fmt.Sprintf("%x", sha256.Sum256(r2.PreparedHandoff.Head)) {
		t.Fatal("mutable snapshot alias")
	}
	if _, _, err := CaptureV1Alpha2RepairHandoff(ctx, f.store, closed, closed.Authority(), h); err == nil {
		t.Fatal("stale capture snapshot succeeded")
	}
}

func TestAddFirstPreparedRosterPreservesOptionalOldWALPin(t *testing.T) {
	f, old := repairTestSetup(t)
	plan := f.plan
	plan.Sequence = V1Alpha2RepairAddThenRemove
	plan.ExpectedConfigID = 2
	bootstrap, err := old.status.BootstrapPlan()
	if err != nil {
		t.Fatal(err)
	}
	source := quepaxa.Cluster{ConfigID: uint(plan.ExpectedConfigID)}
	for _, member := range bootstrap.Members {
		source.Members = append(source.Members, member.Member)
	}
	foundOld := false
	for i := range source.Members {
		if source.Members[i].ID == plan.OldNodeID {
			source.Members[i].WALIdentity = ""
			foundOld = true
		}
	}
	if !foundOld {
		t.Fatal("fixture lacks the planned old voter")
	}
	init, err := LoadV1Alpha2ApplicationInit(context.Background(), f.store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	_, prefix, err := objectstore.ManagedNamespacePrefixes("", plan.ClusterUID, plan.Generation, init.Assignment().Plan.AssignmentID, string(bootstrap.ClusterID))
	if err != nil {
		t.Fatal(err)
	}
	head := []byte("prepared source with preserved bootstrap roster")
	handoff := V1Alpha2RepairHandoff{
		PlanDigest: plan.Digest(), SourcePrefix: prefix, PhysicalBinding: init.Assignment().Plan.PhysicalBinding,
		Head: head, HeadHash: fmt.Sprintf("%x", sha256.Sum256(head)), PreparedConfig: &source,
		PreparedSlot: 1, PreparedHash: strings.Repeat("a", 64),
	}
	if err := validateRepairHandoff(old.status, plan, handoff); err != nil {
		t.Fatalf("config>1 source with untouched optional bootstrap WAL pin rejected: %v", err)
	}
	for i := range source.Members {
		if source.Members[i].ID == plan.OldNodeID {
			source.Members[i].WALIdentity = "different-nonempty-pin"
		}
	}
	handoff.PlanDigest = plan.Digest()
	handoff.PreparedConfig = &source
	if err := validateRepairHandoff(old.status, plan, handoff); err == nil {
		t.Fatal("conflicting nonempty old WAL pin accepted")
	}
}

// Structural/boundary check only, not a replacement CEL evaluator. Actual
// write-once and malformed-input admission must still run against the API.
func TestRepairV1Alpha2HandoffSchema(t *testing.T) {
	data, err := os.ReadFile("../../deploy/operator/cluster-crd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	versions := doc["spec"].(map[string]any)["versions"].([]any)
	var status map[string]any
	for _, item := range versions {
		version := item.(map[string]any)
		if version["name"] == "v1alpha2" {
			status = version["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any)["properties"].(map[string]any)["status"].(map[string]any)
		}
	}
	if status == nil {
		t.Fatal("v1alpha2 status schema missing")
	}
	repair := status["properties"].(map[string]any)["repair"].(map[string]any)
	handoff := repair["properties"].(map[string]any)["preparedHandoff"].(map[string]any)
	properties := handoff["properties"].(map[string]any)
	for _, field := range []string{"planDigest", "sourceRoot", "sourcePrefix", "physicalBinding", "head", "headHash", "removeTerminalSlot", "removeTerminalHash", "survivors", "preparedConfig", "preparedSlot", "preparedHash"} {
		if properties[field] == nil {
			t.Fatalf("handoff schema would prune %s", field)
		}
	}
	handoffRequired := handoff["required"].([]any)
	for _, legacyOnly := range []string{"removeTerminalSlot", "removeTerminalHash", "survivors"} {
		if slices.ContainsFunc(handoffRequired, func(value any) bool {
			field, ok := value.(string)
			return ok && field == legacyOnly
		}) {
			t.Fatalf("ADD-first handoff cannot omit REMOVE-only field %s", legacyOnly)
		}
	}
	planSchema := repair["properties"].(map[string]any)["plan"].(map[string]any)
	planProperties := planSchema["properties"].(map[string]any)
	var finalTargetRule string
	for _, validation := range planSchema["x-kubernetes-validations"].([]any) {
		entry := validation.(map[string]any)
		if strings.Contains(entry["message"].(string), "final target") {
			finalTargetRule = entry["rule"].(string)
			break
		}
	}
	const wantFinalTargetRule = `(has(self.sequence) && self.sequence == "leave") ? (self.replacement.nodeID == self.oldNodeID && !(self.oldNodeID in self.finalVoterIDs)) : (self.replacement.nodeID in self.finalVoterIDs && (self.oldNodeID in self.finalVoterIDs) == (has(self.sequence) && self.sequence == "add"))`
	if finalTargetRule != wantFinalTargetRule {
		t.Fatalf("add-only final target must retain its old-voter witness: %q", finalTargetRule)
	}
	sequence := planProperties["sequence"].(map[string]any)
	if !slices.Equal(sequence["enum"].([]any), []any{"removeThenAdd", "addThenRemove", "add", "leave"}) || slices.ContainsFunc(planSchema["required"].([]any), func(value any) bool {
		field, ok := value.(string)
		return ok && field == "sequence"
	}) {
		t.Fatal("repair sequence must be optional and limited to the two supported orders")
	}
	preparedConfig := properties["preparedConfig"].(map[string]any)
	preparedMembers := preparedConfig["properties"].(map[string]any)["members"].(map[string]any)
	if preparedMembers["maxItems"] != 63 || preparedMembers["x-kubernetes-list-type"] != "map" {
		t.Fatal("prepared roster lost its existing bound or node-ID uniqueness")
	}
	legacyMembers := properties["survivors"].(map[string]any)["properties"].(map[string]any)["members"].(map[string]any)
	if legacyMembers["minItems"] != 1 || legacyMembers["maxItems"] != 63 || properties["survivors"].(map[string]any)["nullable"] == true {
		t.Fatal("REMOVE-first survivors schema was weakened to accept empty or null data")
	}
	addCompletion := repair["properties"].(map[string]any)["addCompletion"].(map[string]any)
	if addCompletion["type"] != "object" || addCompletion["properties"].(map[string]any)["finalVoterIDs"].(map[string]any)["maxItems"] != 64 {
		t.Fatal("ADD proof schema missing or exceeds the existing voter bound")
	}
	head := properties["head"].(map[string]any)
	pattern := regexp.MustCompile(head["pattern"].(string))
	max := head["maxLength"].(int)
	for _, size := range []int{V1Alpha2RepairHeadLimit - 1, V1Alpha2RepairHeadLimit, V1Alpha2RepairHeadLimit + 1, V1Alpha2RepairHeadLimit + 2} {
		encoded := base64.StdEncoding.EncodeToString(make([]byte, size))
		bounded := pattern.MatchString(encoded) && len(encoded) <= max && (len(encoded) < 87384 || strings.HasSuffix(encoded, "=="))
		if bounded != (size <= V1Alpha2RepairHeadLimit) {
			t.Fatalf("base64 schema boundary incorrect for decoded bytes=%d", size)
		}
	}
	joined := string(data)
	for _, message := range []string{"handoff capture requires an existing closed repair operation", "handoff physical binding must match immutable initialization", "captured handoff is immutable within an operation", "ADD terminal proof is immutable once recorded", "handoff must bind the post-REMOVE configuration and terminal position", "handoff survivor set must exactly precede the intended ADD", "ADD-first prepared source must bind the current roster and exact old WAL", "ADD terminal proof must bind the exact intermediate configuration", "REMOVE alone is not final repair completion", "m.node_id == self.plan.oldNodeID && (!has(m.wal_identity) || m.wal_identity == self.plan.oldWALIdentity)"} {
		if !strings.Contains(joined, message) {
			t.Fatalf("missing handoff invariant %s", message)
		}
	}
	for _, fragment := range []string{
		`(self.plan.sequence == "addThenRemove" && self.preparedHandoff.preparedConfig.members.filter(m, m.node_id != self.plan.oldNodeID).size() + 1 == self.plan.finalVoterIDs.size()`,
		`self.preparedHandoff.preparedConfig.members.all(m, m.node_id == self.plan.oldNodeID || m.node_id in self.plan.finalVoterIDs)`,
	} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("ADD-first replacement prepared source must retain its old voter: %s", fragment)
		}
	}
	var addProofImmutableRule string
	for _, validation := range status["x-kubernetes-validations"].([]any) {
		entry := validation.(map[string]any)
		if entry["message"] == "ADD terminal proof is immutable once recorded" {
			addProofImmutableRule = entry["rule"].(string)
			break
		}
	}
	const wantAddProofImmutableRule = `!has(oldSelf.repair) || !has(oldSelf.repair.addCompletion) || (has(self.repair) && (self.repair.plan.operationID != oldSelf.repair.plan.operationID || (has(self.repair.addCompletion) && self.repair.addCompletion == oldSelf.repair.addCompletion)))`
	if addProofImmutableRule != wantAddProofImmutableRule {
		t.Fatalf("ADD proof CEL must allow its initial write but preserve any recorded proof unchanged; got %q", addProofImmutableRule)
	}
	var addProofRule string
	for _, validation := range repair["x-kubernetes-validations"].([]any) {
		entry := validation.(map[string]any)
		if entry["message"] == "ADD terminal proof must bind the exact intermediate configuration" {
			addProofRule = entry["rule"].(string)
		}
	}
	if !strings.Contains(addProofRule, `((self.state == "Closed" && !has(self.completion)) || (self.state == "Completed" && has(self.completion)))`) {
		t.Fatalf("ADD proof must remain valid in both Closed and Completed repair records: %q", addProofRule)
	}
	for _, operationScopedRule := range []string{
		`!has(oldSelf.repair) || !has(self.repair) || self.repair.plan.operationID != oldSelf.repair.plan.operationID || self.repair.plan == oldSelf.repair.plan`,
		`!has(oldSelf.repair) || !has(self.repair) || oldSelf.repair.state == "Closed" || self.repair.plan.operationID != oldSelf.repair.plan.operationID || self.repair == oldSelf.repair`,
		`self.repair.plan.operationID == oldSelf.repair.plan.operationID || (oldSelf.repair.state != "Closed" && self.repair.state == "Closed"`,
	} {
		if !strings.Contains(joined, operationScopedRule) {
			t.Fatalf("existing operation boundary or terminal predecessor guard changed: %s", operationScopedRule)
		}
	}
	for _, fragment := range []string{
		`!has(self.plan.sequence) || self.plan.sequence != "addThenRemove"`,
		`self.preparedHandoff.preparedConfig.config_id == self.plan.expectedConfigID`,
		`self.addCompletion.configID == self.plan.expectedConfigID + 1`,
		`self.completion.configID == self.plan.expectedConfigID + 2`,
	} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("missing sequence-specific CEL contract %s", fragment)
		}
	}
	for _, guard := range []string{"has(self.authority.owner.uid)", "has(self.authority.binding.deploymentUID)", "has(self.authority.binding.storageID)"} {
		if !strings.Contains(joined, guard) {
			t.Fatalf("existing leaf guard lost: %s", guard)
		}
	}
	for _, validation := range status["x-kubernetes-validations"].([]any) {
		rule := validation.(map[string]any)["rule"].(string)
		if strings.Contains(rule, "self.spec") {
			t.Fatalf("status-scoped rule cannot reference the parent spec: %s", rule)
		}
	}
	var leaveMinimum, leaveTerminal, leaveCompletion, leaveSurvivors string
	for _, validation := range planSchema["x-kubernetes-validations"].([]any) {
		entry := validation.(map[string]any)
		if entry["message"] == "leave final roster must retain at least three voters" {
			leaveMinimum = entry["rule"].(string)
		}
	}
	for _, validation := range repair["x-kubernetes-validations"].([]any) {
		entry := validation.(map[string]any)
		switch entry["message"] {
		case "completed leave must bind the exact REMOVE terminal":
			leaveTerminal = entry["rule"].(string)
		case "REMOVE alone is not final repair completion":
			leaveCompletion = entry["rule"].(string)
		case "handoff survivor set must exactly precede the intended ADD":
			leaveSurvivors = entry["rule"].(string)
		}
	}
	if !strings.Contains(leaveMinimum, `self.finalVoterIDs.size() >= 3`) {
		t.Fatalf("leave plan lost its three-voter floor: %q", leaveMinimum)
	}
	for _, fragment := range []string{
		`self.completion.planDigest == self.preparedHandoff.planDigest`,
		`self.completion.configID == self.preparedHandoff.survivors.config_id`,
		`self.completion.slot == self.preparedHandoff.removeTerminalSlot`,
		`self.completion.hash == self.preparedHandoff.removeTerminalHash`,
	} {
		if !strings.Contains(leaveTerminal, fragment) {
			t.Fatalf("leave terminal proof lost exact REMOVE binding %s: %q", fragment, leaveTerminal)
		}
	}
	for _, fragment := range []string{
		`self.completion.configID == self.plan.expectedConfigID + 1`,
		`self.completion.finalVoterIDs == self.plan.finalVoterIDs`,
	} {
		if !strings.Contains(leaveCompletion, fragment) {
			t.Fatalf("leave completion lost exact target binding %s: %q", fragment, leaveCompletion)
		}
	}
	for _, fragment := range []string{
		`self.plan.sequence == "leave" && self.preparedHandoff.survivors.members.size() == self.plan.finalVoterIDs.size()`,
		`m.node_id in self.plan.finalVoterIDs`,
		`self.preparedHandoff.survivors.members.filter(other, other.node_id == m.node_id).size() == 1`,
	} {
		if !strings.Contains(leaveSurvivors, fragment) {
			t.Fatalf("leave survivor handoff lost exact roster binding %s: %q", fragment, leaveSurvivors)
		}
	}
}
