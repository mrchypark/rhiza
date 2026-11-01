package managedsql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

type repairOriginRoundTripper struct {
	next      http.RoundTripper
	resources map[string]any
}

func (r repairOriginRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	for path, body := range r.resources {
		if req.URL.Path == path {
			data, err := json.Marshal(body)
			if err != nil {
				return nil, err
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(data))}, nil
		}
	}
	return r.next.RoundTrip(req)
}

func TestManagedRepairOriginUsesCurrentPodOwnerChainAfterSamePodRestart(t *testing.T) {
	configs, hosts, _ := openSQLQuorum(t)
	ctx := context.Background()
	cfg := configs[0]
	runtime := node.CloneManagedRuntimeConfig(cfg.Runtime)
	const podUID, peerToken, peerURL = "replacement-uid", "replacement-peer-token", "quic://10.0.0.9:9090"
	cfg.PodUID = podUID
	nodeID, err := types.ManagedNodeID(podUID)
	if err != nil {
		t.Fatal(err)
	}
	key := quepaxa.PublicKey(network.PeerPublicKey(runtime.ClusterID, nodeID, peerToken))
	learner := quepaxa.Member{ID: nodeID, PublicKey: key, PeerURL: peerURL}
	runtime.NodeID, runtime.PeerToken, runtime.PeerAddr, runtime.Learner = nodeID, peerToken, "10.0.0.9:9090", &learner
	preparedNode := node.New(&runtime)
	physical, configDigest, err := preparedNode.PreparedStartupBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	identity := node.ManagedStartupIdentity{Member: recovery.PreparedStartupIdentity{ClusterUID: cfg.Ref.UID, Generation: cfg.Plan.Generation, DeploymentUID: cfg.Plan.DeploymentUID, PodUID: podUID, NodeID: string(nodeID), SourceBinding: physical, ConfigDigest: configDigest}, AssignmentID: cfg.Plan.AssignmentID, IntentHash: cfg.Plan.IntentHash, StorageID: cfg.Plan.StorageID, PhysicalBinding: physical, BootstrapPlanDigest: cfg.Plan.BootstrapPlanDigest, EvidenceFormat: EvidenceFormat}
	plan := operator.V1Alpha2RepairPlan{OperationID: "origin-operation", ClusterUID: cfg.Ref.UID, Generation: cfg.Plan.Generation, DeploymentUID: cfg.Plan.DeploymentUID, StorageID: cfg.Plan.StorageID, ExpectedConfigID: 1, OldNodeID: cfg.Runtime.NodeID, OldWALIdentity: strings.Repeat("b", 64), Replacement: operator.V1Alpha2RepairMember{PodName: "replacement", PodUID: podUID, NodeID: nodeID, PublicKey: key, PeerURL: peerURL}, BootstrapPlanDigest: cfg.Plan.BootstrapPlanDigest}
	plan.FinalVoterIDs = []types.NodeID{cfg.Runtime.Members[1].ID, cfg.Runtime.Members[2].ID, nodeID}
	slices.Sort(plan.FinalVoterIDs)
	deploymentName, err := operator.V1Alpha2GenesisDeploymentName(cfg.Ref.UID)
	if err != nil {
		t.Fatal(err)
	}
	identityMessage, _ := json.Marshal(map[string]any{"version": 1, "publicKey": key})
	resources := map[string]any{
		"/api/v1/namespaces/" + cfg.Ref.Namespace + "/pods/replacement":                    map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "replacement", "namespace": cfg.Ref.Namespace, "uid": podUID, "resourceVersion": "7", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "replacement-rs", "uid": "replacement-rs-uid", "controller": true}}}, "status": map[string]any{"podIP": "10.0.0.9", "initContainerStatuses": []any{map[string]any{"name": operator.V1Alpha2PrepareIdentityContainer, "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": string(identityMessage)}}}}, "containerStatuses": []any{map[string]any{"name": "app", "restartCount": 1, "state": map[string]any{"running": map[string]any{"startedAt": "2026-10-30T00:00:00Z"}}}}}},
		"/apis/apps/v1/namespaces/" + cfg.Ref.Namespace + "/replicasets/replacement-rs":    map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": map[string]any{"name": "replacement-rs", "namespace": cfg.Ref.Namespace, "uid": "replacement-rs-uid", "resourceVersion": "8", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": deploymentName, "uid": cfg.Plan.DeploymentUID, "controller": true}}}},
		"/apis/apps/v1/namespaces/" + cfg.Ref.Namespace + "/deployments/" + deploymentName: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": deploymentName, "namespace": cfg.Ref.Namespace, "uid": cfg.Plan.DeploymentUID, "resourceVersion": "9", "ownerReferences": []any{map[string]any{"apiVersion": operator.V1Alpha2APIVersion, "kind": "RhizaCluster", "name": cfg.Ref.Name, "uid": cfg.Ref.UID, "controller": true}}}},
	}
	kube := *cfg.Store.Kube
	client := *kube.Client
	client.Transport = repairOriginRoundTripper{next: client.Transport, resources: resources}
	kube.Client = &client
	cfg.Store = &operator.V1Alpha2StatusStore{Kube: &kube}
	space, err := node.ManagedNamespaceFor(cfg.StorageRoot, cfg.Ref.UID, cfg.Plan.Generation, cfg.Plan.AssignmentID, string(runtime.ClusterID))
	if err != nil {
		t.Fatal(err)
	}
	head := []byte("immutable-captured-head")
	headHash := sha256.Sum256(head)
	handoff := operator.V1Alpha2RepairHandoff{PlanDigest: plan.Digest(), SourceRoot: cfg.StorageRoot, SourcePrefix: space.FinalPrefix, PhysicalBinding: physical, Head: head, HeadHash: fmt.Sprintf("%x", headHash), RemoveTerminalSlot: 10, RemoveTerminalHash: strings.Repeat("a", 64), Survivors: quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{cfg.Runtime.Members[1], cfg.Runtime.Members[2]}}}
	binding, err := operator.ValidateV1Alpha2RepairPodBinding(ctx, cfg.Store, cfg.Ref, plan)
	if err != nil {
		t.Fatal(err)
	}
	origin := managedRepairOriginFile{Version: 1, Repair: operator.V1Alpha2RepairRecord{Plan: plan, State: "Closed", PreparedHandoff: &handoff}, Binding: binding}
	encoded, err := json.Marshal(origin)
	if err != nil {
		t.Fatal(err)
	}
	host := &Host{cfg: cfg, identity: identity, authority: hosts[0].authority}
	host.cfg.Runtime = runtime
	if err := host.ValidateManagedRepairOrigin(ctx, encoded); err != nil {
		t.Fatalf("same-Pod restart origin rejected after main container restart: %v", err)
	}
	origin.Binding.ReplicaSetUID = "different-replicaset"
	wrong, _ := json.Marshal(origin)
	if err := host.ValidateManagedRepairOrigin(ctx, wrong); err == nil {
		t.Fatal("accepted repair origin from a different owner chain")
	}
}
