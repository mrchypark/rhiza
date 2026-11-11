package managedsql

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
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
	if err := h.ValidateManagedRepairPreparation(ctx, h.identity, r, current, 0); err != nil {
		t.Fatalf("exact live ADD-first source preparation: %v", err)
	}
	wrong := r
	wrong.OldWALIdentity = "wrong"
	if err := h.ValidateManagedRepairPreparation(ctx, h.identity, wrong, current, 0); err == nil {
		t.Fatal("wrong old WAL accepted")
	}
	if err := h.ValidateManagedRepairPreparation(ctx, h.identity, r, current, 1); err == nil {
		t.Fatal("wrong abort revision accepted")
	}
	i := h.identity
	i.Member.Generation++
	if err := h.ValidateManagedRepairPreparation(ctx, i, r, current, 0); err == nil {
		t.Fatal("wrong generation accepted")
	}
	completion := operator.V1Alpha2RepairCompletion{OperationID: plan.OperationID, PlanDigest: plan.Digest(), ExpectedConfigID: plan.ExpectedConfigID, ExpectedAbortSlot: plan.ExpectedAbortSlot, Kind: "Abort", ConfigID: 1, Slot: 1, Hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if _, _, err := operator.FinishV1Alpha2Repair(ctx, writer, closed, closed.Authority(), completion); err != nil {
		t.Fatal(err)
	}
	if err := h.ValidateManagedRepairPreparation(ctx, h.identity, r, current, 0); err == nil {
		t.Fatal("terminal repair reused for preparation")
	}
}

func TestManagedAddOnlyTerminalRequiresExactMemberIdentity(t *testing.T) {
	old := quepaxa.Member{ID: "a", URL: "https://a", PeerURL: "quic://a:1", PublicKey: quepaxa.PublicKey{1}, WALIdentity: "wal-a"}
	retained := quepaxa.Member{ID: "b", URL: "https://b", PeerURL: "quic://b:1", PublicKey: quepaxa.PublicKey{2}, WALIdentity: "wal-b"}
	addition := quepaxa.Member{ID: "c", PeerURL: "quic://c:1", PublicKey: quepaxa.PublicKey{3}, WALIdentity: strings.Repeat("3", 64)}
	prepared := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{old, retained}}
	plan := operator.V1Alpha2RepairPlan{Sequence: operator.V1Alpha2RepairAdd, ExpectedConfigID: 1, OldNodeID: old.ID, OldWALIdentity: old.WALIdentity, Replacement: operator.V1Alpha2RepairMember{NodeID: addition.ID, PublicKey: addition.PublicKey, PeerURL: addition.PeerURL}, FinalVoterIDs: []quepaxa.NodeID{old.ID, retained.ID, addition.ID}}
	record := operator.V1Alpha2RepairRecord{Plan: plan, PreparedHandoff: &operator.V1Alpha2RepairHandoff{HeadHash: "handoff", PreparedConfig: &prepared, PreparedSlot: 1, PreparedHash: strings.Repeat("a", 64)}}
	request := node.ManagedMembershipRequest{Action: node.ManagedMembershipAdd, AddOnly: true, HandoffHeadHash: "handoff", ExpectedConfigID: 1, OldNodeID: old.ID, OldWALIdentity: old.WALIdentity, Addition: &addition}
	exact := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{old, retained, addition}}
	h := &Host{}
	if err := h.validateAddOnlyEnrollment(record, request, exact, 0); err != nil {
		t.Fatalf("exact terminal identity: %v", err)
	}
	for _, tc := range []struct {
		name string
		edit func(*quepaxa.Member)
	}{
		{"public key", func(member *quepaxa.Member) { member.PublicKey[0] ^= 0xff }},
		{"peer URL", func(member *quepaxa.Member) { member.PeerURL = "quic://changed:1" }},
		{"WAL identity", func(member *quepaxa.Member) { member.WALIdentity = "different-wal" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := exact
			changed.Members = append([]quepaxa.Member(nil), exact.Members...)
			tc.edit(&changed.Members[2])
			if err := h.validateAddOnlyEnrollment(record, request, changed, 0); err == nil {
				t.Fatal("terminal identity change accepted")
			}
		})
	}
}

func TestManagedSQLWireCloseLifecycleRejectsUnexpectedPUTFailure(t *testing.T) {
	for _, tc := range []struct {
		name, method, path string
		err                error
		closing, canceled  bool
		want               bool
	}{
		{"owned-close", "PUT", "/bucket/owned/archive/head.bin", io.ErrUnexpectedEOF, true, true, true},
		{"before-close", "PUT", "/bucket/owned/archive/head.bin", io.ErrUnexpectedEOF, false, true, false},
		{"live-context", "PUT", "/bucket/owned/archive/head.bin", io.ErrUnexpectedEOF, true, false, false},
		{"wrong-path", "PUT", "/bucket/other/archive/head.bin", io.ErrUnexpectedEOF, true, true, false},
		{"prefix-only", "PUT", "/bucket/owned/", io.ErrUnexpectedEOF, true, true, false},
		{"wrong-method", "GET", "/bucket/owned/archive/head.bin", io.ErrUnexpectedEOF, true, true, false},
		{"other-io", "PUT", "/bucket/owned/archive/head.bin", io.ErrClosedPipe, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			r, err := http.NewRequestWithContext(ctx, tc.method, "http://fixture"+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			lifecycle := &sqlWireCloseLifecycle{}
			lifecycle.closing.Store(tc.closing)
			if got := lifecycle.expectedCanceledPUT("/bucket/owned/", r, tc.err); got != tc.want {
				t.Fatalf("classification got=%t want=%t", got, tc.want)
			}
		})
	}
	server, control := sqlWireStore(t)
	lifecycle := &sqlWireCloseLifecycle{}
	observed := make(chan bool, 1)
	control.putReadFailure = func(r *http.Request, err error) {
		observed <- errors.Is(err, io.ErrUnexpectedEOF) && !lifecycle.expectedCanceledPUT("/bucket/owned/", r, err)
	}
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(server.URL, "http://"), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := io.WriteString(conn, "PUT /bucket/owned/archive/head.bin HTTP/1.1\r\nHost: fixture\r\nContent-Length: 10\r\n\r\nx")
	closeErr := conn.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatal(writeErr, closeErr)
	}
	select {
	case rejected := <-observed:
		if !rejected {
			t.Fatal("actual truncated PUT before owned Close was not rejected")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("actual truncated PUT did not reach the observer")
	}
}

func TestManagedSQLWireCloseLifecycleActualCanceledPUTAfterOwnedClose(t *testing.T) {
	configs, hosts, fixture := openSQLQuorum(t)
	lifecycle := fixture.closeLifecycle
	if lifecycle == nil || fixture.source.putReadFailure == nil || !fixture.closeObserverAtOpen.Load() || lifecycle.closing.Load() {
		t.Fatal("default quorum did not install a strict observer before opening its Hosts")
	}
	path := "/bucket/" + fixture.source.finalPrefix + "/archive/close-observer-test.bin"
	for _, host := range hosts {
		if err := fixture.closeHost(host); err != nil {
			t.Fatal(err)
		}
	}
	// A real TCP disconnect produces the HTTP request cancellation and truncated
	// body. This validates classification after actual owned Close, not the
	// historical publisher's unobserved timing or the source of its EOF.
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(configs[0].Runtime.ObjStoreEndpoint, "http://"), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := io.WriteString(conn, "PUT "+path+" HTTP/1.1\r\nHost: fixture\r\nContent-Length: 10\r\n\r\nx")
	closeErr := conn.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatal(writeErr, closeErr)
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case acceptedPath := <-lifecycle.acceptedPUT:
			if acceptedPath == path {
				return
			}
		case <-deadline.C:
			t.Fatal("real canceled PUT did not reach the installed observer")
		}
	}
}

func TestManagedSQLDirectMembershipRejectsRemoveFirstIntent(t *testing.T) {
	configs, fixture := sqlCluster(t)
	lifecycle := &sqlWireCloseLifecycle{}
	lifecycle.observePUTFailures(t, fixture)
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
				if err := fixture.closeHost(h); err != nil {
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
