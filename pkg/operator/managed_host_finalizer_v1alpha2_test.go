package operator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func managedHostFinalizerCluster(ref V1Alpha2ClusterRef, finalizers []string, deleting bool) string {
	meta := map[string]any{"name": ref.Name, "namespace": ref.Namespace, "uid": ref.UID, "resourceVersion": "1"}
	if finalizers != nil {
		meta["finalizers"] = finalizers
	}
	if deleting {
		meta["deletionTimestamp"] = "2026-11-17T00:00:00Z"
	}
	return string(mustManagedJSON(map[string]any{"apiVersion": V1Alpha2APIVersion, "kind": "RhizaCluster", "metadata": meta, "spec": map[string]any{}, "status": map[string]any{}}))
}

func TestManagedHostFinalizerLifecycle(t *testing.T) {
	ref := V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "uid"}
	protected := []string{"example.invalid/keep", V1Alpha2ManagedHostCleanupFinalizer}
	for _, tc := range []struct {
		name, initial, readback string
		putErr                  bool
		wantDeleting            bool
		wantErr                 bool
		wantPuts                int
		wantFinalizers          []string
	}{
		{name: "install", initial: managedHostFinalizerCluster(ref, []string{"example.invalid/keep"}, false), wantPuts: 1, wantFinalizers: protected},
		{name: "already installed after restart", initial: managedHostFinalizerCluster(ref, protected, false), wantFinalizers: protected},
		{name: "applied unknown response", initial: managedHostFinalizerCluster(ref, []string{"example.invalid/keep"}, false), readback: "applied", putErr: true, wantPuts: 1, wantFinalizers: protected},
		{name: "not applied unknown response", initial: managedHostFinalizerCluster(ref, []string{"example.invalid/keep"}, false), readback: "initial", putErr: true, wantErr: true, wantPuts: 1},
		{name: "deleting remains manual", initial: managedHostFinalizerCluster(ref, protected, true), wantDeleting: true, wantFinalizers: protected},
		{name: "deleting without finalizer", initial: managedHostFinalizerCluster(ref, nil, true), wantDeleting: true, wantErr: true},
		{name: "changed uid", initial: strings.Replace(managedHostFinalizerCluster(ref, nil, false), `"uid":"uid"`, `"uid":"other"`, 1), wantErr: true},
		{name: "invalid finalizers", initial: strings.Replace(managedHostFinalizerCluster(ref, nil, false), `"resourceVersion":"1"`, `"resourceVersion":"1","finalizers":"bad"`, 1), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			puts := 0
			var applied string
			path := managedHostClusterPath(ref)
			store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != path {
					t.Fatalf("unexpected finalizer lifecycle request: %s %s", r.Method, r.URL.Path)
				}
				if r.Method == http.MethodGet {
					if puts > 0 {
						if tc.readback == "initial" {
							return phase2Response(200, tc.initial), nil
						}
						return phase2Response(200, applied), nil
					}
					return phase2Response(200, tc.initial), nil
				}
				if r.Method != http.MethodPut {
					t.Fatalf("unexpected method %s", r.Method)
				}
				puts++
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatal(err)
				}
				var obj map[string]json.RawMessage
				if json.Unmarshal(body, &obj) != nil {
					t.Fatal("invalid PUT body")
				}
				meta, _ := jsonObject(obj["metadata"])
				var got []string
				if json.Unmarshal(meta["finalizers"], &got) != nil {
					t.Fatal("invalid finalizers")
				}
				if tc.wantFinalizers != nil && !reflect.DeepEqual(got, tc.wantFinalizers) {
					t.Fatalf("finalizers=%v want=%v", got, tc.wantFinalizers)
				}
				applied = managedHostFinalizerCluster(ref, got, false)
				if tc.putErr {
					return nil, errors.New("lost finalizer response")
				}
				return phase2Response(200, applied), nil
			})
			deleting, err := reconcileV1Alpha2ManagedHostClusterLifecycle(context.Background(), store.Kube, ref)
			if (err != nil) != tc.wantErr || deleting != tc.wantDeleting || puts != tc.wantPuts {
				t.Fatalf("deleting=%v err=%v puts=%d", deleting, err, puts)
			}
		})
	}
}

func TestManagedHostFinalizerForcePodDeletionAndRecreateStayBlocked(t *testing.T) {
	ref := V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "uid"}
	protected := []string{V1Alpha2ManagedHostCleanupFinalizer}
	// An absent Pod list is represented by making the only permitted API call
	// the CR read. A recreated same-name CR cannot be admitted while this finalizer remains.
	store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != managedHostClusterPath(ref) || r.Method != http.MethodGet {
			t.Fatalf("unexpected cleanup request: %s %s", r.Method, r.URL.Path)
		}
		return phase2Response(200, managedHostFinalizerCluster(ref, protected, true)), nil
	})
	deleting, err := reconcileV1Alpha2ManagedHostClusterLifecycle(context.Background(), store.Kube, ref)
	if err != nil || !deleting {
		t.Fatalf("deleting=%v err=%v", deleting, err)
	}
}
