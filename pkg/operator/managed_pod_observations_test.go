package operator

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestManagedPodObservationsNativeListAndLiveChain(t *testing.T) {
	f, _ := repairTestSetup(t)
	original := f.store.Kube.Client.Transport
	listKind, listUID := "PodList", "replacement-uid"
	var calls int
	f.store.Kube.Client.Transport = escalationProbeTransport(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/pods") {
			calls++
			if r.URL.Query().Get("labelSelector") != "rhiza.mrchypark.dev/cluster-uid="+phase2Ref.UID {
				t.Error("unscoped Pod discovery")
			}
			// Native list items intentionally omit kind/apiVersion.
			return phase2Response(200, string(mustManagedJSON(map[string]any{"apiVersion": "v1", "kind": listKind, "metadata": map[string]any{"resourceVersion": "9"}, "items": []any{map[string]any{"metadata": map[string]any{"name": "replacement", "namespace": phase2Ref.Namespace, "uid": listUID}}}}))), nil
		}
		return original.RoundTrip(r)
	})
	result, err := ObserveV1Alpha2ManagedPods(context.Background(), f.store, phase2Ref, 9090)
	if err != nil || len(result) != 1 || result[0].Member != f.plan.Replacement {
		t.Fatalf("actual owner-chain observation: %v, %v", result, err)
	}
	listUID = "recreated"
	if result, err := ObserveV1Alpha2ManagedPods(context.Background(), f.store, phase2Ref, 9090); err != nil || len(result) != 0 {
		t.Fatalf("recreated Pod blocked healthy observation: %v, %v", result, err)
	}
	listUID, listKind = "replacement-uid", "Pod"
	if _, err := ObserveV1Alpha2ManagedPods(context.Background(), f.store, phase2Ref, 9090); err == nil {
		t.Fatal("accepted malformed list")
	}
	listKind = "PodList"
	f.pod["metadata"].(map[string]any)["deletionTimestamp"] = "2026-10-30T00:00:00Z"
	result, err = ObserveV1Alpha2ManagedPods(context.Background(), f.store, phase2Ref, 9090)
	if err != nil || len(result) != 0 {
		t.Fatalf("deleting Pod remained candidate: %v", err)
	}
	delete(f.pod["metadata"].(map[string]any), "deletionTimestamp")
	f.pod["status"].(map[string]any)["containerStatuses"] = []any{map[string]any{"state": map[string]any{"running": map[string]any{}}}}
	result, err = ObserveV1Alpha2ManagedPods(context.Background(), f.store, phase2Ref, 9090)
	if err != nil || len(result) != 1 || result[0].Candidate {
		t.Fatalf("running main selected for first repair: %v", err)
	}
	f.pod["metadata"].(map[string]any)["ownerReferences"] = []any{}
	if _, err := ObserveV1Alpha2ManagedPods(context.Background(), f.store, phase2Ref, 9090); err == nil {
		t.Fatal("accepted unrelated owner")
	}
	if calls == 0 || f.puts != 0 {
		t.Fatalf("discovery wrote status or omitted list: lists=%d puts=%d", calls, f.puts)
	}
}

func TestManagedPodObservationsSkipStartingAndFailedPods(t *testing.T) {
	f, _ := repairTestSetup(t)
	original := f.store.Kube.Client.Transport
	starting := map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "starting", "namespace": phase2Ref.Namespace, "uid": "starting-uid", "resourceVersion": "3"}, "status": map[string]any{"phase": "Pending"}}
	failed := map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "failed", "namespace": phase2Ref.Namespace, "uid": "failed-uid", "resourceVersion": "4"}, "status": map[string]any{"phase": "Failed", "podIP": "10.0.0.9"}}
	f.store.Kube.Client.Transport = escalationProbeTransport(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/pods") {
			return phase2Response(200, string(mustManagedJSON(map[string]any{"apiVersion": "v1", "kind": "PodList", "metadata": map[string]any{"resourceVersion": "9"}, "items": []any{
				map[string]any{"metadata": map[string]any{"name": "replacement", "namespace": phase2Ref.Namespace, "uid": "replacement-uid"}},
				map[string]any{"metadata": map[string]any{"name": "starting", "namespace": phase2Ref.Namespace, "uid": "starting-uid"}},
				map[string]any{"metadata": map[string]any{"name": "failed", "namespace": phase2Ref.Namespace, "uid": "failed-uid"}},
			}}))), nil
		}
		if strings.HasSuffix(r.URL.Path, "/pods/starting") {
			return phase2Response(200, string(mustManagedJSON(starting))), nil
		}
		if strings.HasSuffix(r.URL.Path, "/pods/failed") {
			return phase2Response(200, string(mustManagedJSON(failed))), nil
		}
		return original.RoundTrip(r)
	})
	result, err := ObserveV1Alpha2ManagedPods(context.Background(), f.store, phase2Ref, 9090)
	if err != nil || len(result) != 1 || result[0].Member != f.plan.Replacement {
		t.Fatalf("transient Pods blocked healthy voters: result=%+v err=%v", result, err)
	}
}
