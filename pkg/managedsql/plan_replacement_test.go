package managedsql

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestPlanVoterReplacementActualBootstrapNonceAndClosedPrecedence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	configs, fixture := sqlCluster(t)
	fixture.mu.Lock()
	fixture.obj["status"] = json.RawMessage(`{}`)
	fixture.mu.Unlock()
	for i := range configs {
		configs[i].Runtime.EnableReconfiguration = true
	}
	n := node.New(&configs[0].Runtime)
	physical, _, err := n.PreparedStartupBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := n.ManagedBootstrapPlanDigest(physical, EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	init, err := operator.NewV1Alpha2ApplicationInitPlan(configs[0].Ref, "logical", "deployment", "assignment", physical, digest, EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := operator.V1Alpha2BootstrapPlan{ClusterID: configs[0].Runtime.ClusterID, PhysicalBinding: physical, EvidenceFormat: EvidenceFormat, Durability: types.ObjectStoreDurabilityBeforeAck, Reconfiguration: true}
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
		hosts[i], err = Open(ctx, configs[i])
		if hosts[i] != nil {
			h := hosts[i]
			t.Cleanup(func() {
				if err := h.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	configs[0].Plan = init
	writer, expected := hostRepairWriter(t, configs, hosts)
	configs[0].Store = writer
	hosts[0], err = Open(ctx, configs[0])
	if hosts[0] != nil {
		h := hosts[0]
		t.Cleanup(func() {
			if err := h.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	h := hosts[0]
	present := []types.NodeID{configs[0].Runtime.NodeID, configs[2].Runtime.NodeID, expected.Replacement.NodeID}
	plan, err := h.PlanVoterReplacement(ctx, expected.Replacement, present)
	if err != nil || plan == nil {
		t.Fatalf("actual current bootstrap voter plan: %v", err)
	}
	if plan.OldWALIdentity != expected.OldWALIdentity || plan.OldNodeID != expected.OldNodeID || plan.ExpectedConfigID != 1 || plan.ExpectedAbortSlot != 0 || !reflect.DeepEqual(plan.FinalVoterIDs, expected.FinalVoterIDs) {
		t.Fatal("plan did not bind actual current membership/registered WAL")
	}
	again, err := h.PlanVoterReplacement(ctx, expected.Replacement, present)
	if err != nil || !reflect.DeepEqual(plan, again) {
		t.Fatalf("plan was not deterministic: %v", err)
	}
	healthy := append(append([]types.NodeID(nil), present...), expected.OldNodeID)
	if p, err := h.PlanVoterReplacement(ctx, expected.Replacement, healthy); err != nil || p != nil {
		t.Fatalf("healthy voter selected: %v", err)
	}
	if _, err := h.PlanVoterReplacement(ctx, expected.Replacement, present[:1]); err == nil {
		t.Fatal("selected repair with two missing voters")
	}
	a, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := operator.BeginV1Alpha2Repair(ctx, writer, a, a.Authority(), *plan); err != nil {
		t.Fatal(err)
	}
	other := expected.Replacement
	other.PodUID = "different-candidate"
	closed, err := h.PlanVoterReplacement(ctx, other, nil)
	if err != nil || !reflect.DeepEqual(closed, plan) {
		t.Fatalf("closed plan was retargeted: %v", err)
	}
	transport := writer.Kube.Client.Transport
	writer.Kube.Client.Transport = repairGateTransport(func(request *http.Request) (*http.Response, error) {
		if strings.Contains(request.URL.Path, "/pods/") {
			return &http.Response{StatusCode: 404, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
		}
		return transport.RoundTrip(request)
	})
	if _, err := h.PlanVoterReplacement(ctx, other, nil); err == nil {
		t.Fatal("lost Closed candidate was silently retargeted")
	}
	live, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	retained, ok := live.Repair()
	if !ok || retained.State != "Closed" || !reflect.DeepEqual(retained.Plan, *plan) {
		t.Fatal("candidate loss cleared or replaced exact Closed plan")
	}
}
