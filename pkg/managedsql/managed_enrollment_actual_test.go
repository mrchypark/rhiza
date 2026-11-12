//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// TestManagedScaleEnrollsLearnersSerially3To4To5 drives the real QuePaxa
// reconfiguration path twice. A candidate Pod becoming ready is not enough:
// the coordinator captures a prepared source, waits for the learner's actual
// WAL registration, and records only the retained old-to-new terminal.
func TestManagedScaleEnrollsLearnersSerially3To4To5(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	const protocol = sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	configs, fixture := sqlClusterForProtocol(t, protocol)
	for i := range configs {
		configs[i].Runtime.EnableReconfiguration = true
	}
	fixture.mu.Lock()
	fixture.obj["status"] = json.RawMessage(`{}`)
	var meta map[string]json.RawMessage
	if json.Unmarshal(fixture.obj["metadata"], &meta) == nil {
		meta["resourceVersion"] = json.RawMessage(`"1"`)
		fixture.obj["metadata"], _ = json.Marshal(meta)
	}
	fixture.mu.Unlock()
	bootstrapNode := node.New(&configs[0].Runtime)
	source, _, err := bootstrapNode.PreparedStartupBindingDigestsForProtocol(protocol)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := bootstrapNode.ManagedBootstrapPlanDigestForProtocol(source, protocol, protocol)
	if err != nil {
		t.Fatal(err)
	}
	initPlan, err := operator.NewV1Alpha2ApplicationInitPlan(configs[0].Ref, "logical", "deployment", "assignment", source, digest, protocol)
	if err != nil {
		t.Fatal(err)
	}
	initPlan.ApplicationProtocol = protocol
	bootstrap := operator.V1Alpha2BootstrapPlan{ClusterID: configs[0].Runtime.ClusterID, PhysicalBinding: source, EvidenceFormat: protocol, Durability: types.ObjectStoreDurabilityBeforeAck, Reconfiguration: true}
	for i, member := range configs[0].Runtime.Members {
		bootstrap.Members = append(bootstrap.Members, operator.V1Alpha2BootstrapMember{PodUID: configs[i].PodUID, Member: member})
	}
	deployment, err := operator.V1Alpha2GenesisDeploymentName(configs[0].Ref.UID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := operator.InitializeV1Alpha2AuthorityWithBootstrap(ctx, configs[0].Store, configs[0].Ref, deployment, initPlan, bootstrap); err != nil {
		t.Fatal(err)
	}
	for i := range configs {
		configs[i].Plan = initPlan
	}

	base := configs[0].Store.Kube
	kube := *base
	client := *kube.Client
	kube.Client = &client
	writerStore := &operator.V1Alpha2StatusStore{Kube: &kube}
	baseTransport := client.Transport
	var candidatePath, candidateRSPath string
	var candidatePod map[string]any
	deploymentName, err := operator.V1Alpha2GenesisDeploymentName(configs[0].Ref.UID)
	if err != nil {
		t.Fatal(err)
	}
	client.Transport = repairGateTransport(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case candidatePath:
			return repairGateJSON(candidatePod), nil
		case candidateRSPath:
			return repairGateJSON(map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": map[string]any{"name": strings.TrimPrefix(candidateRSPath, "/apis/apps/v1/namespaces/ns/replicasets/"), "namespace": "ns", "uid": "scale-rs-uid", "resourceVersion": "8", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": deploymentName, "uid": configs[0].Plan.DeploymentUID, "controller": true}}}}), nil
		default:
			return baseTransport.RoundTrip(request)
		}
	})
	for i := range configs {
		configs[i].Store = writerStore
	}
	hosts := make([]*Host, 3)
	for i := 0; i < 3; i++ {
		host, err := fixture.open(ctx, configs[i])
		if host != nil {
			hosts[i] = host
			t.Cleanup(func() { _ = fixture.closeHost(host) })
		}
		if err != nil {
			t.Fatalf("open original voter %d: %v", i, err)
		}
	}
	writer := hosts[0]
	readySQL(t, writer)
	if _, err := writer.Execute(ctx, ExecuteRequest{RequestID: "scale-base", SQL: "CREATE TABLE scale_base (id INTEGER)"}); err != nil {
		t.Fatalf("establish enrollment source tip: %v", err)
	}
	setManagedScaleTarget(t, fixture, 5)

	present := make([]types.NodeID, 0, 5)
	for _, member := range configs[0].Runtime.Members {
		present = append(present, member.ID)
	}
	for _, ordinal := range []int{4, 5} {
		podName, podUID := fmt.Sprintf("scale-%d", ordinal), fmt.Sprintf("scale-%d-uid", ordinal)
		nodeID, err := types.ManagedNodeID(podUID)
		if err != nil {
			t.Fatal(err)
		}
		token := fmt.Sprintf("scale-%d-token", ordinal)
		addr := fixture.reservePeer(t, nodeID)
		peerURL := "quic://" + addr
		key := quepaxa.PublicKey(network.PeerPublicKey(configs[0].Runtime.ClusterID, nodeID, token))
		keyMessage, _ := json.Marshal(map[string]any{"version": 1, "publicKey": key})
		candidatePath = "/api/v1/namespaces/ns/pods/" + podName
		candidateRSPath = "/apis/apps/v1/namespaces/ns/replicasets/" + podName + "-rs"
		candidatePod = map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": podName, "namespace": "ns", "uid": podUID, "resourceVersion": fmt.Sprint(ordinal), "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": podName + "-rs", "uid": "scale-rs-uid", "controller": true}}}, "status": map[string]any{"podIP": strings.Split(addr, ":")[0], "initContainerStatuses": []any{map[string]any{"name": operator.V1Alpha2PrepareIdentityContainer, "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": string(keyMessage)}}}}, "containerStatuses": []any{map[string]any{"name": "app", "restartCount": 0, "state": map[string]any{"waiting": map[string]any{"reason": "ContainerCreating"}}}}}}
		replacement := operator.V1Alpha2RepairPodRequest{PodName: podName, PodUID: podUID, NodeID: nodeID, PublicKey: key, PeerURL: peerURL}
		plan, err := writer.PlanVoterReplacement(ctx, replacement, append(slices.Clone(present), nodeID))
		if err != nil || plan == nil || plan.Sequence != operator.V1Alpha2RepairAdd {
			t.Fatalf("plan enrollment %d: plan=%+v err=%v", ordinal, plan, err)
		}
		admission, err := operator.LoadV1Alpha2Admission(ctx, writerStore, configs[0].Ref)
		if err != nil {
			t.Fatal(err)
		}
		closed, observation, err := operator.BeginV1Alpha2Repair(ctx, writerStore, admission, admission.Authority(), *plan)
		if err != nil || observation != operator.V1Alpha2CASApplied {
			t.Fatalf("close enrollment %d: observation=%s err=%v", ordinal, observation, err)
		}
		record, _ := closed.Repair()
		handoff, err := writer.CaptureRepairHandoff(ctx, plan.OperationID)
		if err != nil {
			t.Fatalf("capture enrollment %d: %v", ordinal, err)
		}
		blockedCtx, cancelBlocked := context.WithTimeout(ctx, 100*time.Millisecond)
		blockedErr := writer.ReplaceVoter(blockedCtx, *plan)
		cancelBlocked()
		if blockedErr == nil {
			t.Fatalf("enrollment %d advanced without a registered learner admission proof", ordinal)
		}
		unchanged, err := writer.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
		if err != nil || unchanged.Current.ConfigID != uint(ordinal-3) {
			t.Fatalf("blocked enrollment %d changed membership: %+v err=%v", ordinal, unchanged.Current, err)
		}
		binding, err := operator.ValidateV1Alpha2RepairPodBinding(ctx, writerStore, configs[0].Ref, *plan)
		if err != nil {
			t.Fatal(err)
		}
		origin, _ := json.Marshal(managedRepairOriginFile{Version: 1, Repair: record, Binding: binding})
		learnerCfg := configs[0]
		learnerCfg.PodUID = podUID
		learnerCfg.Runtime = node.CloneManagedRuntimeConfig(configs[0].Runtime)
		learnerCfg.Runtime.NodeID = nodeID
		learnerCfg.Runtime.PeerAddr, learnerCfg.Runtime.PeerToken = addr, token
		learnerCfg.Runtime.Learner = &quepaxa.Member{ID: nodeID, PublicKey: key, PeerURL: peerURL}
		learnerCfg.Runtime.DataDir = filepath.Join(t.TempDir(), "enrolled")
		learnerCfg.RepairOrigin = origin
		prepareManagedEnrollment(t, ctx, &learnerCfg, handoff, binding)
		learner, err := fixture.open(ctx, learnerCfg)
		if learner != nil {
			t.Cleanup(func() { _ = fixture.closeHost(learner) })
		}
		if err != nil {
			t.Fatalf("open prepared learner %d: %v", ordinal, err)
		}
		waitManagedRegistration(t, ctx, writer, nodeID, key)
		if err := writer.ReplaceVoter(ctx, *plan); err != nil {
			t.Fatalf("enroll %d: %v", ordinal, err)
		}
		state, err := writer.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
		if err != nil || state.Current.ConfigID != uint(ordinal-2) || len(state.Current.Members) != ordinal {
			t.Fatalf("membership after %d: config=%d voters=%d err=%v", ordinal, state.Current.ConfigID, len(state.Current.Members), err)
		}
		completed, err := operator.LoadV1Alpha2Admission(ctx, writerStore, configs[0].Ref)
		if err != nil {
			t.Fatal(err)
		}
		record, ok := completed.Repair()
		if !ok || record.State != "Completed" || record.Completion == nil || record.Completion.ConfigID != uint64(ordinal-2) || !slices.Equal(record.Completion.FinalVoterIDs, plan.FinalVoterIDs) {
			t.Fatalf("completed enrollment %d: %+v", ordinal, record)
		}
		present = append(present, nodeID)
	}
	state, err := writer.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil || state.Current.ConfigID != 3 || len(state.Current.Members) != 5 {
		t.Fatalf("final membership: config=%d voters=%d err=%v", state.Current.ConfigID, len(state.Current.Members), err)
	}
}

func setManagedScaleTarget(t *testing.T, fixture *sqlStatusFixture, target int) {
	t.Helper()
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	var spec map[string]json.RawMessage
	if json.Unmarshal(fixture.obj["spec"], &spec) != nil {
		t.Fatal("invalid cluster spec")
	}
	spec["voterCount"], _ = json.Marshal(target)
	fixture.obj["spec"], _ = json.Marshal(spec)
}

func prepareManagedEnrollment(t *testing.T, ctx context.Context, cfg *Config, handoff operator.V1Alpha2RepairHandoff, binding operator.V1Alpha2RepairPodBinding) {
	t.Helper()
	n := node.New(&cfg.Runtime)
	physical, member, err := n.PreparedStartupBindingDigestsForProtocol(cfg.Plan.ApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	identity := recovery.PreparedStartupIdentity{ClusterUID: cfg.Ref.UID, Generation: cfg.Plan.Generation, DeploymentUID: cfg.Plan.DeploymentUID, PodUID: cfg.PodUID, NodeID: string(cfg.Runtime.NodeID), SourceBinding: physical, ConfigDigest: member}
	bucket, err := objstore.NewBucketWithoutProvisioning(ctx, objstore.Config{Provider: objstore.ProviderS3, Endpoint: cfg.Runtime.ObjStoreEndpoint, Bucket: cfg.Runtime.ObjStoreBucket, Region: cfg.Runtime.ObjStoreRegion, Insecure: true, AccessKey: cfg.Runtime.ObjStoreAccessKey, SecretKey: cfg.Runtime.ObjStoreSecretKey})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	trusted := quepaxa.Cluster{ConfigID: 1, Members: cfg.Runtime.Members}
	policy, window, err := types.ManagedApplicationPolicy(cfg.Plan.ApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	view, err := recovery.OpenReadOnlyViewAtHeadForApplicationProtocol(ctx, bucket, handoff.SourcePrefix, 1, trusted, handoff.Head, policy, window, cfg.Plan.ApplicationProtocol, true)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "prepared")
	if err := recovery.PublishPreparedStartupForPolicy(ctx, view, dir, identity, policy, window); err != nil {
		t.Fatal(err)
	}
	cfg.Prepared, err = recovery.LoadPreparedStartupForApplicationProtocol(ctx, bucket, handoff.SourcePrefix, 1, trusted, dir, cfg.Runtime.DataDir, identity, policy, window, cfg.Plan.ApplicationProtocol, true)
	if err != nil {
		t.Fatal(err)
	}
	record, err := operator.LoadV1Alpha2Admission(ctx, cfg.Store, cfg.Ref)
	if err != nil {
		t.Fatal(err)
	}
	repair, _ := record.Repair()
	cfg.RepairOrigin, _ = json.Marshal(managedRepairOriginFile{Version: 1, Repair: repair, Binding: binding})
}

func waitManagedRegistration(t *testing.T, ctx context.Context, host *Host, id quepaxa.NodeID, key quepaxa.PublicKey) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		snapshot, err := host.node.ManagedRepairSnapshot(ctx, id, key)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.LearnerWALIdentity != "" {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-deadline.C:
			t.Fatal("learner registration did not appear")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
