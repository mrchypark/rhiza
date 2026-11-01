package managedsql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
	plan := operator.V1Alpha2RepairPlan{OperationID: "host-rv-barrier", ClusterUID: configs[0].Ref.UID, Generation: a.Authority().Generation, DeploymentUID: a.Authority().Binding.DeploymentUID, StorageID: a.Authority().Binding.StorageID, ExpectedConfigID: 1, OldNodeID: configs[1].Runtime.NodeID, OldWALIdentity: old.WALIdentity, Replacement: operator.V1Alpha2RepairMember{PodName: "replacement", PodUID: "replacement-uid", NodeID: nodeID, PublicKey: key, PeerURL: "quic://127.0.0.9:9090"}, BootstrapPlanDigest: configs[0].Plan.BootstrapPlanDigest, FinalVoterIDs: []types.NodeID{configs[0].Runtime.NodeID, configs[2].Runtime.NodeID, nodeID}}
	slices.Sort(plan.FinalVoterIDs)
	return store, plan
}

func TestManagedSQLRepairClosedFailsBeforeQuorum(t *testing.T) {
	configs, hosts, _ := openSQLQuorum(t)
	h := hosts[0]
	writer, plan := hostRepairWriter(t, configs, hosts)
	a, err := operator.LoadV1Alpha2Admission(context.Background(), writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := operator.BeginV1Alpha2Repair(context.Background(), writer, a, a.Authority(), plan); err != nil {
		t.Fatal(err)
	}
	base := h.cfg.Store
	kube := *base.Kube
	client := *kube.Client
	transport := client.Transport
	kube.Client = &client
	h.cfg.Store = &operator.V1Alpha2StatusStore{Kube: &kube}
	var gets atomic.Int64
	client.Transport = repairGateTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/rhizaclusters/") {
			gets.Add(1)
		}
		return transport.RoundTrip(r)
	})
	before, err := h.node.ManagedState()
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Execute(context.Background(), ExecuteRequest{RequestID: "closed-repair", SQL: "CREATE TABLE must_not_execute (v TEXT)"})
	if err == nil || !strings.Contains(err.Error(), "repair") || gets.Load() != 2 {
		t.Fatalf("closed repair was not checked before quorum: err=%v GETs=%d", err, gets.Load())
	}
	after, err := h.node.ManagedState()
	if err != nil || before.CertifiedTip != after.CertifiedTip {
		t.Fatalf("closed gate submitted SQL: %v", err)
	}
	if err := h.Ready(context.Background()); err != nil {
		t.Fatalf("repair removed safe local readiness: %v", err)
	}
}

func TestManagedSQLRepairABAAtQuorumBoundaryRejectsOldSnapshot(t *testing.T) {
	for _, kind := range []string{"Terminal", "Abort"} {
		t.Run(kind, func(t *testing.T) {
			configs, hosts, _ := openSQLQuorum(t)
			h := hosts[0]
			writer, plan := hostRepairWriter(t, configs, hosts)
			base := h.cfg.Store
			kube := *base.Kube
			client := *kube.Client
			transport := client.Transport
			kube.Client = &client
			h.cfg.Store = &operator.V1Alpha2StatusStore{Kube: &kube}
			var gets atomic.Int64
			var interleaved atomic.Bool
			var boundaryErr error
			client.Transport = repairGateTransport(func(r *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(r)
				if err != nil || r.Method != http.MethodGet || !strings.Contains(r.URL.Path, "/rhizaclusters/") || gets.Add(1) != 4 {
					return response, err
				}
				body, err := io.ReadAll(response.Body)
				_ = response.Body.Close()
				if err != nil {
					return nil, err
				}
				response.Body = io.NopCloser(bytes.NewReader(body))
				// This is the existing live checker/Ready boundary after ReadIndex.
				// Use actual repair/status CAS APIs, not a production test hook. The
				// completion below is a trusted status-fixture assertion; this test
				// does NOT claim that membership or certificate verification occurred.
				a, e := operator.LoadV1Alpha2Admission(r.Context(), writer, configs[0].Ref)
				if e != nil {
					boundaryErr = e
					return response, nil
				}
				closed, _, e := operator.BeginV1Alpha2Repair(r.Context(), writer, a, a.Authority(), plan)
				if e != nil {
					boundaryErr = e
					return response, nil
				}
				completion := operator.V1Alpha2RepairCompletion{OperationID: plan.OperationID, PlanDigest: plan.Digest(), ExpectedConfigID: plan.ExpectedConfigID, ExpectedAbortSlot: plan.ExpectedAbortSlot, Kind: kind, ConfigID: 3, Slot: 10, Hash: strings.Repeat("a", 64), FinalVoterIDs: slices.Clone(plan.FinalVoterIDs)}
				if kind == "Abort" {
					completion.ConfigID = 1
					completion.FinalVoterIDs = nil
				}
				_, _, boundaryErr = operator.FinishV1Alpha2Repair(r.Context(), writer, closed, closed.Authority(), completion)
				interleaved.Store(boundaryErr == nil)
				return response, nil
			})
			before, err := h.node.ManagedState()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err = h.Execute(ctx, ExecuteRequest{RequestID: fmt.Sprintf("ABA-%s", kind), SQL: "CREATE TABLE must_not_execute (v TEXT)"})
			if boundaryErr != nil || !interleaved.Load() {
				t.Fatalf("actual repair interleave did not run: %v GETs=%d", boundaryErr, gets.Load())
			}
			if !errors.Is(err, operator.ErrV1Alpha2StatusConflict) {
				t.Fatalf("stale quorum refreshed authority after repair ABA: %v", err)
			}
			a, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
			if err != nil {
				t.Fatal(err)
			}
			if _, pending := a.Pending(); pending || a.EverApplicationReserved() || a.SQLReservationsClosed() {
				t.Fatal("stale Execute reserved pending or changed repair gate")
			}
			after, err := h.node.ManagedState()
			if err != nil || before.CertifiedTip != after.CertifiedTip {
				t.Fatalf("stale SQL was submitted: %v", err)
			}
			if err := h.Ready(ctx); err != nil {
				t.Fatalf("reservation conflict revoked certified local readiness: %v", err)
			}
		})
	}
}
