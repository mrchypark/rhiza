package managedsql

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type repairGateTransport func(*http.Request) (*http.Response, error)

func (f repairGateTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func repairGateJSON(v any) *http.Response {
	b, _ := json.Marshal(v)
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(b))}
}

func hostRepairWriter(t *testing.T, configs []Config, hosts []*Host) (*operator.V1Alpha2StatusStore, operator.V1Alpha2RepairPlan) {
	t.Helper()
	base := configs[0].Store
	kube := *base.Kube
	client := *kube.Client
	transport := client.Transport
	kube.Client = &client
	store := &operator.V1Alpha2StatusStore{Kube: &kube}
	nodeID, err := types.ManagedNodeID("replacement-uid")
	if err != nil {
		t.Fatal(err)
	}
	key := quepaxa.PublicKey(network.PeerPublicKey(configs[0].Runtime.ClusterID, nodeID, "replacement-test-token"))
	message, _ := json.Marshal(map[string]any{"version": 1, "publicKey": key})
	depName, _ := operator.V1Alpha2GenesisDeploymentName(configs[0].Ref.UID)
	client.Transport = repairGateTransport(func(r *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(r.URL.Path, "/pods/"):
			return repairGateJSON(map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "replacement", "namespace": "ns", "uid": "replacement-uid", "resourceVersion": "1", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "rs", "uid": "rs-uid", "controller": true}}}, "status": map[string]any{"podIP": "127.0.0.9", "initContainerStatuses": []any{map[string]any{"name": operator.V1Alpha2PrepareIdentityContainer, "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": string(message)}}}}}}), nil
		case strings.Contains(r.URL.Path, "/replicasets/"):
			return repairGateJSON(map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": map[string]any{"name": "rs", "namespace": "ns", "uid": "rs-uid", "resourceVersion": "1", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": depName, "uid": "deployment", "controller": true}}}}), nil
		default:
			return transport.RoundTrip(r)
		}
	})
	a, err := operator.LoadV1Alpha2Admission(context.Background(), store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	old, err := hosts[1].node.ManagedState()
	if err != nil {
		t.Fatal(err)
	}
	plan := operator.V1Alpha2RepairPlan{Sequence: replacementSequenceForProtocol(configs[0].Plan.ApplicationProtocol), OperationID: "host-rv-barrier", ClusterUID: configs[0].Ref.UID, Generation: a.Authority().Generation, DeploymentUID: a.Authority().Binding.DeploymentUID, StorageID: a.Authority().Binding.StorageID, ExpectedConfigID: 1, OldNodeID: configs[1].Runtime.NodeID, OldWALIdentity: old.WALIdentity, Replacement: operator.V1Alpha2RepairMember{PodName: "replacement", PodUID: "replacement-uid", NodeID: nodeID, PublicKey: key, PeerURL: "quic://127.0.0.9:9090"}, BootstrapPlanDigest: configs[0].Plan.BootstrapPlanDigest, FinalVoterIDs: []types.NodeID{configs[0].Runtime.NodeID, configs[2].Runtime.NodeID, nodeID}}
	slices.Sort(plan.FinalVoterIDs)
	return store, plan
}
