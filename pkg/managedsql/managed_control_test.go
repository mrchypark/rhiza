package managedsql

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path"
	"path/filepath"
	"strings"
	"testing"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestManagedSQLHostRemoveRepairVoterTerminal(t *testing.T) {
	ctx := context.Background()
	configs, seed := sqlCluster(t)
	// Build a separate, fresh CR fixture with reconfiguration enabled before
	// any Host opens; never rewrite a committed live assignment.
	f := &sqlStatusFixture{obj: make(map[string]json.RawMessage), source: seed.source}
	for k, v := range seed.obj {
		if k != "status" {
			f.obj[k] = append(json.RawMessage(nil), v...)
		}
	}
	f.obj["status"] = json.RawMessage(`{}`)
	api := httptest.NewTLSServer(http.HandlerFunc(f.serve))
	t.Cleanup(api.Close)
	k := *configs[0].Store.Kube
	k.BaseURL, k.Client = api.URL, api.Client()
	store := &operator.V1Alpha2StatusStore{Kube: &k}
	for i := range configs {
		configs[i].Runtime.EnableReconfiguration = true
		configs[i].Store = store
	}
	n := node.New(&configs[0].Runtime)
	source, _, err := n.PreparedStartupBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := n.ManagedBootstrapPlanDigest(source, EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := operator.NewV1Alpha2ApplicationInitPlan(configs[0].Ref, "logical", "deployment", "assignment", source, digest, EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := operator.V1Alpha2BootstrapPlan{ClusterID: configs[0].Runtime.ClusterID, PhysicalBinding: source, EvidenceFormat: EvidenceFormat, Durability: types.ObjectStoreDurabilityBeforeAck, Reconfiguration: true}
	for i, m := range configs[0].Runtime.Members {
		bootstrap.Members = append(bootstrap.Members, operator.V1Alpha2BootstrapMember{PodUID: configs[i].PodUID, Member: m})
	}
	name, _ := operator.V1Alpha2GenesisDeploymentName(configs[0].Ref.UID)
	if _, err := operator.InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, configs[0].Ref, name, plan, bootstrap); err != nil {
		t.Fatal(err)
	}
	hosts := make([]*Host, 3)
	for _, i := range []int{1, 2} {
		configs[i].Plan = plan
		h, err := Open(ctx, configs[i])
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
	configs[0].Plan = plan
	writer, repair := hostRepairWriter(t, configs, hosts)
	peer, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peerAddr := peer.LocalAddr().String()
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	repair.Replacement.PeerURL = "quic://" + peerAddr
	originalTransport := writer.Kube.Client.Transport
	writer.Kube.Client.Transport = repairGateTransport(func(req *http.Request) (*http.Response, error) {
		response, err := originalTransport.RoundTrip(req)
		if err != nil || !strings.Contains(req.URL.Path, "/pods/") {
			return response, err
		}
		defer response.Body.Close()
		var pod map[string]any
		if err := json.NewDecoder(response.Body).Decode(&pod); err != nil {
			return nil, err
		}
		pod["status"].(map[string]any)["podIP"] = "127.0.0.1"
		return repairGateJSON(pod), nil
	})
	configs[0].Store = writer
	hosts[0], err = Open(ctx, configs[0])
	if hosts[0] != nil {
		t.Cleanup(func() {
			if err := hosts[0].Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hosts {
		readySQL(t, h)
	}
	a, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := operator.BeginV1Alpha2Repair(ctx, writer, a, a.Authority(), repair); err != nil {
		t.Fatal(err)
	}
	logControlState := func(stage string) {
		for i, h := range hosts {
			state, stateErr := h.node.ManagedState()
			t.Logf("control %s member=%s certified=%d applied=%d recorder=%d inspect-error=%t", stage, configs[i].Runtime.NodeID, state.CertifiedTip, state.AppliedTip, state.RecorderTip, stateErr != nil)
		}
	}
	logControlState("before-remove")
	if err := hosts[0].RemoveRepairVoter(ctx, repair.OperationID); err != nil {
		logControlState("remove-failed")
		t.Fatal(err)
	}
	// The same Host entry must retry the actual terminal, not produce a second
	// operation or reopen SQL after the REMOVE-only intermediate configuration.
	before, err := hosts[0].node.ManagedState()
	if err != nil {
		t.Fatal(err)
	}
	cfg := configs[0].Runtime
	bucket, err := objectstore.NewBucketWithoutProvisioning(ctx, objectstore.Config{Provider: objectstore.ProviderS3, Endpoint: cfg.ObjStoreEndpoint, Bucket: cfg.ObjStoreBucket, Region: cfg.ObjStoreRegion, Insecure: true, AccessKey: cfg.ObjStoreAccessKey, SecretKey: cfg.ObjStoreSecretKey})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	archive := recovery.NewManager(bucket, path.Join(cfg.ObjStorePrefix, string(cfg.ClusterID)), 1)
	t.Cleanup(archive.Close)
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	values, _, err := archive.DecisionsFrom(ctx, quepaxa.Slot(before.CertifiedTip), 1)
	if err != nil || len(values) != 1 {
		t.Fatalf("actual terminal archive: %v", err)
	}
	terminal := values[0]
	if control, err := quepaxa.DecodeReconfiguration(terminal.Value); err != nil || !control || len(terminal.Certificate) == 0 {
		t.Fatalf("certified terminal: %v", err)
	}
	var record struct {
		Terminal bool            `json:"terminal"`
		Abort    bool            `json:"abort"`
		Target   quepaxa.Cluster `json:"target"`
	}
	if err := json.Unmarshal(bytes.TrimPrefix(terminal.Value, []byte("QRCF\x00")), &record); err != nil || !record.Terminal || record.Abort || record.Target.ConfigID != 2 || len(record.Target.Members) != 2 {
		t.Fatalf("REMOVE terminal: %+v %v", record, err)
	}
	for _, member := range record.Target.Members {
		if member.ID != configs[0].Runtime.NodeID && member.ID != configs[2].Runtime.NodeID {
			t.Fatal("terminal is not exact survivor-only intermediate membership")
		}
	}
	if err := hosts[0].RemoveRepairVoter(ctx, repair.OperationID); err != nil {
		t.Fatal(err)
	}
	after, err := hosts[0].node.ManagedState()
	if err != nil || after.CertifiedTip != before.CertifiedTip {
		t.Fatalf("terminal retry: %v", err)
	}
	live, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil || !live.SQLReservationsClosed() {
		t.Fatalf("REMOVE reopened repair: %v", err)
	}
	capture, err := hosts[0].CaptureRepairHandoff(ctx, repair.OperationID)
	if err != nil {
		t.Fatalf("real post-REMOVE capture: %v", err)
	}
	recordSize, _ := json.Marshal(capture)
	t.Logf("actual post-REMOVE HEAD bytes=%d handoff JSON bytes=%d", len(capture.Head), len(recordSize))
	if len(capture.Head) > operator.V1Alpha2RepairHeadLimit {
		t.Fatal("actual HEAD exceeded bounded inline contract")
	}
	live, err = operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	repairRecord, ok := live.Repair()
	if !ok || repairRecord.PreparedHandoff == nil || repairRecord.PreparedHandoff.HeadHash != capture.HeadHash {
		t.Fatal("captured handoff not read back")
	}
	encodedRecord, _ := json.Marshal(repairRecord)
	t.Logf("actual bounded repair record JSON bytes=%d", len(encodedRecord))
	if len(encodedRecord) > operator.V1Alpha2RepairRecordLimit {
		t.Fatal("actual post-REMOVE record exceeds budget")
	}
	captureAgain, err := hosts[0].CaptureRepairHandoff(ctx, repair.OperationID)
	if err != nil || !bytes.Equal(captureAgain.Head, capture.Head) {
		t.Fatalf("capture retry retargeted HEAD: %v", err)
	}
	if _, _, err := operator.CaptureV1Alpha2RepairHandoff(ctx, writer, live, live.Authority(), capture); err == nil {
		t.Fatal("write-once capture overwritten")
	}
	learnerCfg := configs[0]
	learnerCfg.Store = writer
	learnerCfg.PodUID = repair.Replacement.PodUID
	learnerCfg.Runtime = node.CloneManagedRuntimeConfig(learnerCfg.Runtime)
	learnerCfg.Runtime.NodeID = repair.Replacement.NodeID
	learnerCfg.Runtime.PeerAddr, learnerCfg.Runtime.PeerToken = peerAddr, "replacement-test-token"
	learnerCfg.Runtime.Learner = &quepaxa.Member{ID: repair.Replacement.NodeID, PublicKey: repair.Replacement.PublicKey, PeerURL: repair.Replacement.PeerURL}
	learnerCfg.Runtime.DataDir = filepath.Join(t.TempDir(), "verified")
	learnerNode := node.New(&learnerCfg.Runtime)
	physical, configDigest, err := learnerNode.PreparedStartupBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	identity := recovery.PreparedStartupIdentity{ClusterUID: learnerCfg.Ref.UID, Generation: learnerCfg.Plan.Generation, DeploymentUID: learnerCfg.Plan.DeploymentUID, PodUID: learnerCfg.PodUID, NodeID: string(learnerCfg.Runtime.NodeID), SourceBinding: physical, ConfigDigest: configDigest}
	trustedBootstrap := quepaxa.Cluster{ConfigID: 1, Members: learnerCfg.Runtime.Members}
	view, err := recovery.OpenReadOnlyViewAtHead(ctx, bucket, capture.SourcePrefix, 1, trustedBootstrap, capture.Head)
	if err != nil {
		t.Fatal(err)
	}
	handoffDir := filepath.Join(t.TempDir(), "handoff")
	if err := recovery.PublishPreparedStartup(ctx, view, handoffDir, identity); err != nil {
		t.Fatal(err)
	}
	prepared, err := recovery.LoadPreparedStartup(ctx, bucket, capture.SourcePrefix, 1, trustedBootstrap, handoffDir, learnerCfg.Runtime.DataDir, identity)
	if err != nil {
		t.Fatal(err)
	}
	learnerCfg.Prepared = prepared
	learner, err := Open(ctx, learnerCfg)
	if learner != nil {
		t.Cleanup(func() {
			if err := learner.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatalf("open actual prepared learner: %v", err)
	}
	learnerState, err := learner.node.ManagedState()
	if err != nil || learnerState.WALIdentity == "" {
		t.Fatalf("actual learner registration: %v", err)
	}
	if err := hosts[0].AddRepairVoter(ctx, repair.OperationID, "not-a-registered-nonce"); err == nil {
		t.Fatal("fake learner nonce accepted")
	}
	if err := hosts[0].AddRepairVoter(ctx, repair.OperationID, learnerState.WALIdentity); err != nil {
		t.Fatalf("private actual learner ADD: %v", err)
	}
	if err := hosts[0].AddRepairVoter(ctx, repair.OperationID, learnerState.WALIdentity); err != nil {
		t.Fatalf("exact ADD terminal retry: %v", err)
	}
	readySQL(t, learner)
	if _, err := learner.node.API(); err == nil {
		t.Fatal("prepared learner exposed raw API")
	}
	if _, err := learner.node.Handler(); err == nil {
		t.Fatal("prepared learner exposed raw Handler")
	}
	finalState, err := hosts[0].node.ManagedState()
	if err != nil {
		t.Fatal(err)
	}
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	values, _, err = archive.DecisionsFrom(ctx, quepaxa.Slot(finalState.CertifiedTip), 1)
	if err != nil || len(values) != 1 {
		t.Fatalf("actual ADD terminal archive: %v", err)
	}
	terminal = values[0]
	if control, err := quepaxa.DecodeReconfiguration(terminal.Value); err != nil || !control || len(terminal.Certificate) == 0 {
		t.Fatalf("ADD certificate: %v", err)
	}
	if err := json.Unmarshal(bytes.TrimPrefix(terminal.Value, []byte("QRCF\x00")), &record); err != nil || !record.Terminal || record.Abort || record.Target.ConfigID != 3 || len(record.Target.Members) != 3 {
		t.Fatalf("ADD terminal: %+v %v", record, err)
	}
	foundLearner := false
	for _, member := range record.Target.Members {
		if member.ID == repair.OldNodeID {
			t.Fatal("old voter resurrected")
		}
		if member.ID == repair.Replacement.NodeID {
			foundLearner = member.WALIdentity == learnerState.WALIdentity && member.PublicKey == repair.Replacement.PublicKey && member.PeerURL == repair.Replacement.PeerURL
		}
	}
	if !foundLearner {
		t.Fatal("ADD terminal lacks actual registered learner identity")
	}
	retained, err := hosts[0].CaptureRepairHandoff(ctx, repair.OperationID)
	if err != nil || !bytes.Equal(retained.Head, capture.Head) {
		t.Fatalf("later HEAD retargeted capture: %v", err)
	}
	fixed, err := recovery.OpenReadOnlyViewAtHead(ctx, bucket, capture.SourcePrefix, 1, trustedBootstrap, capture.Head)
	if err != nil || fixed.Tip() >= archive.Tip() {
		t.Fatalf("fixed HEAD did not retain earlier cut: %v", err)
	}
	if err := learner.Close(); err != nil {
		t.Fatal(err)
	}
	learnerCfg.Prepared = nil
	restarted, err := Open(ctx, learnerCfg)
	if restarted != nil {
		t.Cleanup(func() {
			if err := restarted.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatalf("same-Pod promoted adopted restart: %v", err)
	}
	readySQL(t, restarted)
	restartedState, err := restarted.node.ManagedState()
	if err != nil || restartedState.WALIdentity != learnerState.WALIdentity {
		t.Fatalf("adopted restart replaced WAL: %v", err)
	}
	live, err = operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil || !live.SQLReservationsClosed() {
		t.Fatalf("membership alone reopened SQL: %v", err)
	}
}

func TestManagedSQLMembershipRequiresLiveExactRepair(t *testing.T) {
	ctx := context.Background()
	configs, hosts, _ := openSQLQuorum(t)
	h := hosts[0]
	writer, plan := hostRepairWriter(t, configs, hosts)
	r := node.ManagedMembershipRequest{RepairOperationID: plan.OperationID, Action: node.ManagedMembershipRemove, ExpectedConfigID: uint(plan.ExpectedConfigID), OldNodeID: quepaxa.NodeID(plan.OldNodeID), OldWALIdentity: plan.OldWALIdentity}
	current := quepaxa.Cluster{ConfigID: 1, Members: configs[0].Runtime.Members}
	if err := h.ValidateManagedMembership(ctx, h.identity, r, current, 0); err == nil {
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
		t.Fatalf("exact live repair: %v", err)
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
		t.Fatal("terminal repair reused for membership")
	}
}
