package managedsql

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestReplaceVoterCoordinatesActualPreparedLearner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	configs, seed := sqlCluster(t)

	fresh := &sqlStatusFixture{obj: make(map[string]json.RawMessage)}
	for key, value := range seed.obj {
		if key != "status" {
			fresh.obj[key] = append(json.RawMessage(nil), value...)
		}
	}
	fresh.obj["status"] = json.RawMessage(`{}`)
	api := httptest.NewTLSServer(http.HandlerFunc(fresh.serve))
	t.Cleanup(api.Close)
	kube := *configs[0].Store.Kube
	kube.BaseURL, kube.Client = api.URL, api.Client()
	store := &operator.V1Alpha2StatusStore{Kube: &kube}
	for i := range configs {
		configs[i].Runtime.EnableReconfiguration = true
		configs[i].Store = store
	}
	bootstrapNode := node.New(&configs[0].Runtime)
	source, _, err := bootstrapNode.PreparedStartupBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := bootstrapNode.ManagedBootstrapPlanDigest(source, EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	initPlan, err := operator.NewV1Alpha2ApplicationInitPlan(configs[0].Ref, "logical", "deployment", "assignment", source, digest, EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := operator.V1Alpha2BootstrapPlan{ClusterID: configs[0].Runtime.ClusterID, PhysicalBinding: source, EvidenceFormat: EvidenceFormat, Durability: types.ObjectStoreDurabilityBeforeAck, Reconfiguration: true}
	for i, member := range configs[0].Runtime.Members {
		bootstrap.Members = append(bootstrap.Members, operator.V1Alpha2BootstrapMember{PodUID: configs[i].PodUID, Member: member})
	}
	deployment, _ := operator.V1Alpha2GenesisDeploymentName(configs[0].Ref.UID)
	if _, err := operator.InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, configs[0].Ref, deployment, initPlan, bootstrap); err != nil {
		t.Fatal(err)
	}
	hosts := make([]*Host, 3)
	for _, i := range []int{1, 2} {
		configs[i].Plan = initPlan
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
	configs[0].Plan = initPlan
	writer, plan := hostRepairWriter(t, configs, hosts)
	peer, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peerAddr := peer.LocalAddr().String()
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	plan.Replacement.PeerURL = "quic://" + peerAddr
	originalTransport := writer.Kube.Client.Transport
	writer.Kube.Client.Transport = repairGateTransport(func(request *http.Request) (*http.Response, error) {
		response, err := originalTransport.RoundTrip(request)
		if err != nil || !strings.Contains(request.URL.Path, "/pods/") {
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
	for _, h := range hosts {
		readySQL(t, h)
	}

	// Keep an unresolved original SQL reservation across the control-only repair.
	if _, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "repair-schema", SQL: "CREATE TABLE app(value TEXT)"}); err != nil {
		t.Fatal(err)
	}
	fresh.mu.Lock()
	fresh.failFinalize = true
	fresh.mu.Unlock()
	if _, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "repair-pending", SQL: "INSERT INTO app VALUES ('retained')"}); err == nil {
		t.Fatal("fixture did not retain an uncertain SQL request")
	}
	fresh.mu.Lock()
	fresh.failFinalize = false
	fresh.mu.Unlock()
	before, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	pending, hasPending := before.Pending()
	pendingSQL, hasSQL, err := before.PendingSQL()
	if err != nil || !hasPending || !hasSQL {
		t.Fatalf("fixture pending request: %v", err)
	}

	coordination := make(chan error, 1)
	go func() { coordination <- hosts[0].ReplaceVoter(ctx, plan) }()
	var handoff operator.V1Alpha2RepairHandoff
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for handoff.Head == nil {
		live, loadErr := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
		if loadErr == nil {
			if repair, ok := live.Repair(); ok && repair.Plan.OperationID == plan.OperationID && repair.PreparedHandoff != nil {
				handoff = *repair.PreparedHandoff
				break
			}
		}
		select {
		case err := <-coordination:
			t.Fatalf("coordinator ended before publishing prepared handoff: %v", err)
		case <-deadline.C:
			t.Fatal("coordinator did not publish prepared handoff")
		case <-time.After(20 * time.Millisecond):
		}
	}
	originAdmission, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	originRepair, ok := originAdmission.Repair()
	if !ok || originRepair.State != "Closed" || originRepair.PreparedHandoff == nil || !reflect.DeepEqual(*originRepair.PreparedHandoff, handoff) {
		t.Fatalf("captured handoff is not the exact live closed repair: %+v", originRepair)
	}
	originBinding, err := operator.ValidateV1Alpha2RepairPodBinding(ctx, writer, configs[0].Ref, originRepair.Plan)
	if err != nil {
		t.Fatalf("capture live repair Pod owner chain: %v", err)
	}
	repairOrigin, err := json.Marshal(managedRepairOriginFile{Version: 1, Repair: originRepair, Binding: originBinding})
	if err != nil {
		t.Fatal(err)
	}

	learnerCfg := configs[0]
	learnerCfg.Store = writer
	learnerCfg.PodUID = plan.Replacement.PodUID
	learnerCfg.Runtime = node.CloneManagedRuntimeConfig(learnerCfg.Runtime)
	learnerCfg.Runtime.NodeID = plan.Replacement.NodeID
	learnerCfg.Runtime.PeerAddr, learnerCfg.Runtime.PeerToken = peerAddr, "replacement-test-token"
	learnerCfg.Runtime.Learner = &quepaxa.Member{ID: plan.Replacement.NodeID, PublicKey: plan.Replacement.PublicKey, PeerURL: plan.Replacement.PeerURL}
	learnerCfg.Runtime.DataDir = filepath.Join(t.TempDir(), "verified")
	learnerCfg.RepairOrigin = repairOrigin
	learnerNode := node.New(&learnerCfg.Runtime)
	physical, configDigest, err := learnerNode.PreparedStartupBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	identity := recovery.PreparedStartupIdentity{ClusterUID: learnerCfg.Ref.UID, Generation: learnerCfg.Plan.Generation, DeploymentUID: learnerCfg.Plan.DeploymentUID, PodUID: learnerCfg.PodUID, NodeID: string(learnerCfg.Runtime.NodeID), SourceBinding: physical, ConfigDigest: configDigest}
	bucket, err := objstore.NewBucketWithoutProvisioning(ctx, objstore.Config{Provider: objstore.ProviderS3, Endpoint: learnerCfg.Runtime.ObjStoreEndpoint, Bucket: learnerCfg.Runtime.ObjStoreBucket, Region: learnerCfg.Runtime.ObjStoreRegion, Insecure: true, AccessKey: learnerCfg.Runtime.ObjStoreAccessKey, SecretKey: learnerCfg.Runtime.ObjStoreSecretKey})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := bucket.Close(); err != nil {
			t.Error(err)
		}
	})
	trustedBootstrap := quepaxa.Cluster{ConfigID: 1, Members: learnerCfg.Runtime.Members}
	view, err := recovery.OpenReadOnlyViewAtHead(ctx, bucket, handoff.SourcePrefix, 1, trustedBootstrap, handoff.Head)
	if err != nil {
		t.Fatal(err)
	}
	handoffDir := filepath.Join(t.TempDir(), "handoff")
	if err := recovery.PublishPreparedStartup(ctx, view, handoffDir, identity); err != nil {
		t.Fatal(err)
	}
	learnerCfg.Prepared, err = recovery.LoadPreparedStartup(ctx, bucket, handoff.SourcePrefix, 1, trustedBootstrap, handoffDir, learnerCfg.Runtime.DataDir, identity)
	if err != nil {
		t.Fatal(err)
	}
	badHandoffOrigin := originRepair
	badHandoff := *originRepair.PreparedHandoff
	badHandoff.RemoveTerminalSlot++
	badHandoffOrigin.PreparedHandoff = &badHandoff
	badHandoffBytes, err := json.Marshal(managedRepairOriginFile{Version: 1, Repair: badHandoffOrigin, Binding: originBinding})
	if err != nil {
		t.Fatal(err)
	}
	badHandoffCfg := learnerCfg
	badHandoffCfg.RepairOrigin = badHandoffBytes
	if badLearner, err := Open(ctx, badHandoffCfg); err == nil {
		_ = badLearner.Close()
		t.Fatal("prepared adoption accepted an origin whose REMOVE terminal differed from the full live handoff")
	} else if !strings.Contains(err.Error(), "prepared learner no longer matches the closed repair intent") {
		t.Fatalf("prepared handoff mismatch failed at an unexpected boundary: %v", err)
	}
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
	select {
	case err := <-coordination:
		if err != nil {
			t.Fatalf("actual managed replacement: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("replacement coordinator timed out: %v", ctx.Err())
	}
	after, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	record, ok := after.Repair()
	if !ok || record.State != "Completed" || record.Completion == nil {
		t.Fatalf("actual repair not completed: %+v", record)
	}
	afterPending, hasPending := after.Pending()
	afterSQL, hasSQL, err := after.PendingSQL()
	if err != nil || !hasPending || !hasSQL || afterPending != pending || !reflect.DeepEqual(afterSQL, pendingSQL) {
		t.Fatalf("replacement changed uncertain SQL: request=%+v sql=%+v err=%v", afterPending, afterSQL, err)
	}
	snapshot, err := hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil || uint64(snapshot.Current.ConfigID) != plan.ExpectedConfigID+2 {
		t.Fatalf("actual final voter configuration: %+v err=%v", snapshot.Current, err)
	}
	dSnapshot, err := learner.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil || dSnapshot.LocalWALIdentity == "" {
		t.Fatalf("read adopted D WAL identity: %+v err=%v", dSnapshot, err)
	}
	if err := learner.Close(); err != nil {
		t.Fatal(err)
	}
	completedWithoutOrigin := learnerCfg
	completedWithoutOrigin.RepairOrigin = nil
	if _, err := Open(ctx, completedWithoutOrigin); err == nil || !strings.Contains(err.Error(), "prepared repair learner requires the exact current closed repair") {
		t.Fatalf("completed D record accepted a fresh prepared adoption without its closed origin: %v", err)
	}
	if err := hosts[0].RecoverPending(ctx); err != nil {
		t.Fatalf("resolve retained original request before restart write check: %v", err)
	}
	postRecovery, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, pending := postRecovery.Pending(); pending {
		t.Fatal("original pending request remained after exact recovery")
	}
	historicalCfg := learnerCfg
	historicalCfg.Prepared = nil
	historical, err := Open(ctx, historicalCfg)
	if historical != nil {
		t.Cleanup(func() {
			if err := historical.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatalf("same-Pod D restart from immutable origin after completion: %v", err)
	}
	firstDRestart, err := historical.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil || firstDRestart.LocalWALIdentity != dSnapshot.LocalWALIdentity {
		t.Fatalf("same-Pod D restart changed its durable WAL nonce: before=%q after=%q err=%v", dSnapshot.LocalWALIdentity, firstDRestart.LocalWALIdentity, err)
	}
	if err := historical.Ready(ctx); err != nil {
		t.Fatalf("same-Pod D restart did not recover before readiness: %v", err)
	}
	if _, err := historical.Execute(ctx, ExecuteRequest{RequestID: "after-d-restart", SQL: "INSERT INTO app VALUES ('after-d-restart')"}); err != nil {
		t.Fatalf("same-Pod D restart could not admit SQL while quorum was available: %v", err)
	}
	if err := historical.Close(); err != nil {
		t.Fatal(err)
	}

	// Roll the live repair record forward to E while retaining D's exact origin
	// and adopted WAL. D's next restart must recover locally under E's closed
	// reservation gate without making SQL writable.
	rolloverRuntime := node.CloneManagedRuntimeConfig(configs[0].Runtime)
	eNodeID, err := types.ManagedNodeID("replacement-e-uid")
	if err != nil {
		t.Fatal(err)
	}
	eToken := "replacement-e-token"
	eKey := quepaxa.PublicKey(network.PeerPublicKey(rolloverRuntime.ClusterID, eNodeID, eToken))
	eConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	eAddr := eConn.LocalAddr().String()
	if err := eConn.Close(); err != nil {
		t.Fatal(err)
	}
	ePeerURL := "quic://" + eAddr
	eIdentityMessage, _ := json.Marshal(map[string]any{"version": 1, "publicKey": eKey})
	baseTransport := writer.Kube.Client.Transport
	deploymentName, err := operator.V1Alpha2GenesisDeploymentName(configs[0].Ref.UID)
	if err != nil {
		t.Fatal(err)
	}
	ePodPath := "/api/v1/namespaces/" + configs[0].Ref.Namespace + "/pods/replacement-e"
	eRSPath := "/apis/apps/v1/namespaces/" + configs[0].Ref.Namespace + "/replicasets/replacement-e-rs"
	writer.Kube.Client.Transport = repairGateTransport(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case ePodPath:
			return repairGateJSON(map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "replacement-e", "namespace": configs[0].Ref.Namespace, "uid": "replacement-e-uid", "resourceVersion": "11", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "replacement-e-rs", "uid": "replacement-e-rs-uid", "controller": true}}}, "status": map[string]any{"podIP": "127.0.0.1", "initContainerStatuses": []any{map[string]any{"name": operator.V1Alpha2PrepareIdentityContainer, "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": string(eIdentityMessage)}}}}, "containerStatuses": []any{map[string]any{"name": "app", "restartCount": 0, "state": map[string]any{"waiting": map[string]any{"reason": "ContainerCreating"}}}}}}), nil
		case eRSPath:
			return repairGateJSON(map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": map[string]any{"name": "replacement-e-rs", "namespace": configs[0].Ref.Namespace, "uid": "replacement-e-rs-uid", "resourceVersion": "12", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": deploymentName, "uid": configs[0].Plan.DeploymentUID, "controller": true}}}}), nil
		default:
			response, err := baseTransport.RoundTrip(request)
			if err != nil || request.URL.Path != "/api/v1/namespaces/"+configs[0].Ref.Namespace+"/pods/replacement" {
				return response, err
			}
			defer response.Body.Close()
			var pod map[string]any
			if err := json.NewDecoder(response.Body).Decode(&pod); err != nil {
				return nil, err
			}
			pod["status"].(map[string]any)["containerStatuses"] = []any{map[string]any{"name": "app", "restartCount": 1, "state": map[string]any{"running": map[string]any{"startedAt": "2026-10-30T00:00:00Z"}}}}
			return repairGateJSON(pod), nil
		}
	})
	current, err := hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil || current.Current.ConfigID != snapshot.Current.ConfigID {
		t.Fatalf("read current D membership before E close: %+v err=%v", current, err)
	}
	ePlan := plan
	ePlan.OperationID = "origin-rollover-e"
	ePlan.ExpectedConfigID = uint64(current.Current.ConfigID)
	ePlan.ExpectedAbortSlot = uint64(current.AbortSlot)
	ePlan.OldNodeID = plan.Replacement.NodeID
	ePlan.OldWALIdentity = dSnapshot.LocalWALIdentity
	ePlan.Replacement = operator.V1Alpha2RepairMember{PodName: "replacement-e", PodUID: "replacement-e-uid", NodeID: eNodeID, PublicKey: eKey, PeerURL: ePeerURL}
	ePlan.FinalVoterIDs = make([]types.NodeID, 0, len(current.Current.Members))
	for _, member := range current.Current.Members {
		if member.ID != ePlan.OldNodeID {
			ePlan.FinalVoterIDs = append(ePlan.FinalVoterIDs, member.ID)
		}
	}
	ePlan.FinalVoterIDs = append(ePlan.FinalVoterIDs, eNodeID)
	slices.Sort(ePlan.FinalVoterIDs)
	eAdmission, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	eClosed, observation, err := operator.BeginV1Alpha2Repair(ctx, writer, eAdmission, eAdmission.Authority(), ePlan)
	if err != nil || observation != operator.V1Alpha2CASApplied || !eClosed.SQLReservationsClosed() {
		t.Fatalf("roll current repair record from D to E: observation=%s err=%v", observation, err)
	}
	eClosedVersion := eClosed.ResourceVersion()
	historicalE, err := Open(ctx, historicalCfg)
	if historicalE != nil {
		t.Cleanup(func() {
			if err := historicalE.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatalf("same-Pod D restart with live E record: %v", err)
	}
	secondDRestart, err := historicalE.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil || secondDRestart.LocalWALIdentity != dSnapshot.LocalWALIdentity {
		t.Fatalf("same-Pod D restart under E changed its durable WAL nonce: before=%q after=%q err=%v", dSnapshot.LocalWALIdentity, secondDRestart.LocalWALIdentity, err)
	}
	if err := historicalE.Ready(ctx); err != nil {
		t.Fatalf("same-Pod D restart under E did not recover before readiness: %v", err)
	}
	if _, err := historicalE.Execute(ctx, ExecuteRequest{RequestID: "must-not-reserve-under-e", SQL: "INSERT INTO app VALUES ('blocked')"}); err == nil || !strings.Contains(err.Error(), "reservations closed") {
		t.Fatalf("E closed gate did not reject fresh SQL: %v", err)
	}
	eAfter, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if eAfter.ResourceVersion() != eClosedVersion {
		t.Fatalf("rejected SQL changed status resourceVersion: before=%s after=%s", eClosedVersion, eAfter.ResourceVersion())
	}
	if _, pending := eAfter.Pending(); pending {
		t.Fatal("rejected SQL created a pending reservation")
	}
	if currentRepair, ok := eAfter.Repair(); !ok || currentRepair.State != "Closed" || !reflect.DeepEqual(currentRepair.Plan, ePlan) {
		t.Fatalf("D restart changed live E repair: %+v", currentRepair)
	}
}
