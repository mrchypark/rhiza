package operator

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"gopkg.in/yaml.v3"
)

// Status fixtures below are trusted-caller assertions, not consensus proof.
// The managed Host test separately captures and verifies a real archived REMOVE.
func TestRepairV1Alpha2HandoffWriteOnceAndBindings(t *testing.T) {
	ctx := context.Background()
	f, old := repairTestSetup(t)
	closed, _, err := BeginV1Alpha2Repair(ctx, f.store, old, old.Authority(), f.plan)
	if err != nil {
		t.Fatal(err)
	}
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
	for _, field := range []string{"planDigest", "sourceRoot", "sourcePrefix", "physicalBinding", "head", "headHash", "removeTerminalSlot", "removeTerminalHash", "survivors"} {
		if properties[field] == nil {
			t.Fatalf("handoff schema would prune %s", field)
		}
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
	for _, message := range []string{"handoff capture requires an existing closed repair operation", "handoff physical binding must match immutable initialization", "captured handoff is immutable within an operation", "handoff must bind the post-REMOVE configuration and terminal position", "handoff survivor set must exactly precede the intended ADD"} {
		if !strings.Contains(joined, message) {
			t.Fatalf("missing handoff invariant %s", message)
		}
	}
	for _, guard := range []string{"has(self.authority.owner.uid)", "has(self.authority.binding.deploymentUID)", "has(self.authority.binding.storageID)"} {
		if !strings.Contains(joined, guard) {
			t.Fatalf("existing leaf guard lost: %s", guard)
		}
	}
}
