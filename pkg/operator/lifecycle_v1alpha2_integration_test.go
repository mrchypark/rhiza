package operator

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"testing"
)

func TestV1Alpha2LifecycleAPIServerContract(t *testing.T) {
	server := os.Getenv("RHIZA_CONTRACT_SERVER")
	if server == "" {
		if os.Getenv("RHIZA_CONTRACT_REQUIRED") != "" {
			t.Fatal("RHIZA_CONTRACT_SERVER is required in the contract harness")
		}
		t.Skip("API server contract environment not configured")
	}
	getenv := func(key string) string {
		t.Helper()
		value := os.Getenv(key)
		if value == "" {
			t.Fatalf("%s is required", key)
		}
		return value
	}
	ca, err := os.ReadFile(getenv("RHIZA_CONTRACT_CA"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid contract CA")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	ns := getenv("RHIZA_CONTRACT_NAMESPACE")
	kube := &Kubernetes{BaseURL: server, Namespace: ns, TokenFile: getenv("RHIZA_CONTRACT_TOKEN"), Client: &http.Client{Transport: transport}}
	ctx := context.Background()
	load := func(name string) (V1Alpha2RecoveryRef, map[string]json.RawMessage) {
		t.Helper()
		var obj map[string]json.RawMessage
		if err := kube.Get(ctx, "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/"+ns+"/rhizarecoveries/"+name, &obj); err != nil {
			t.Fatal(err)
		}
		meta, err := jsonObject(obj["metadata"])
		if err != nil {
			t.Fatal(err)
		}
		ref := V1Alpha2RecoveryRef{Namespace: ns, Name: name}
		if json.Unmarshal(meta["uid"], &ref.UID) != nil || json.Unmarshal(meta["resourceVersion"], &ref.ResourceVersion) != nil || ref.UID == "" || ref.ResourceVersion == "" {
			t.Fatal("invalid recovery identity")
		}
		return ref, meta
	}
	installer := &V1Alpha2FinalizerStore{Kube: kube}
	ref, _ := load("finalizer-install")
	if err := installer.Install(ctx, ref); err != nil {
		t.Fatalf("install: %v", err)
	}
	_, meta := load(ref.Name)
	var finalizers []string
	if json.Unmarshal(meta["finalizers"], &finalizers) != nil || len(finalizers) != 2 || finalizers[0] != "example.invalid/keep" || finalizers[1] != V1Alpha2RecoveryRecordFinalizer {
		t.Fatalf("finalizers not preserved: %s", meta["finalizers"])
	}
	if err := installer.Install(ctx, ref); !errors.Is(err, ErrV1Alpha2FinalizerConflict) {
		t.Fatalf("stale revision accepted: %v", err)
	}
	deleting, deletingMeta := load("finalizer-deleting")
	if len(deletingMeta["deletionTimestamp"]) == 0 || string(deletingMeta["deletionTimestamp"]) == "null" {
		t.Fatal("fixture is not deleting")
	}
	if err := installer.Install(ctx, deleting); err == nil {
		t.Fatal("installed finalizer on deleting request")
	}
	_, deletingMeta = load(deleting.Name)
	if json.Unmarshal(deletingMeta["finalizers"], &finalizers) != nil || len(finalizers) != 1 || finalizers[0] != "example.invalid/keep" {
		t.Fatalf("deleting finalizers changed: %s", deletingMeta["finalizers"])
	}
	store := &V1Alpha2StatusStore{Kube: kube}
	cluster := V1Alpha2ClusterRef{Name: "finalizer-contract", Namespace: ns, UID: getenv("RHIZA_CONTRACT_FINALIZER_UID")}
	_, err = ClaimV1Alpha2Recovery(ctx, store, cluster)
	var queueErr *V1Alpha2QueueError
	if !errors.As(err, &queueErr) || queueErr.Reason != V1Alpha2QueueRequestBlocked {
		t.Fatalf("deleting queue request was not blocked: %v", err)
	}
	for _, tc := range []struct {
		name, uidEnv, phase string
		cancel              bool
	}{
		{"lifecycle-cancel", "RHIZA_CONTRACT_CANCEL_UID", "CancelRequested", true},
		{"lifecycle-prepare", "RHIZA_CONTRACT_PREPARE_UID", "Preparing", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cluster := V1Alpha2ClusterRef{Name: tc.name, Namespace: ns, UID: getenv(tc.uidEnv)}
			claim, err := ClaimV1Alpha2Recovery(ctx, store, cluster)
			if err != nil {
				t.Fatal(err)
			}
			before, err := store.Load(ctx, cluster)
			if err != nil {
				t.Fatal(err)
			}
			request, _ := load(tc.name)
			if tc.cancel {
				path := "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/" + ns + "/rhizarecoveries/" + tc.name
				if err := kube.request(ctx, http.MethodDelete, path, object{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": object{"uid": request.UID}}, nil); err != nil {
					t.Fatal(err)
				}
				err = RegisterV1Alpha2RecoveryCancellation(ctx, store, cluster, tc.name, claim)
			} else {
				err = BeginV1Alpha2RecoveryPreparation(ctx, store, cluster, tc.name, claim)
			}
			if err != nil {
				t.Fatal(err)
			}
			after, err := store.Load(ctx, cluster)
			if err != nil || !sameJSON(before.authorityRaw, after.authorityRaw) {
				t.Fatalf("authority changed or read failed: %v", err)
			}
			var queue map[string]json.RawMessage
			if json.Unmarshal(after.Queue(), &queue) != nil || string(queue["phase"]) != `"`+tc.phase+`"` || string(queue["requestUID"]) != `"`+claim.RequestUID+`"` {
				t.Fatalf("unexpected persisted claim: %s", after.Queue())
			}
			// This checks a sequential stale attempt, not concurrent arbitration.
			if err := BeginV1Alpha2RecoveryPreparation(ctx, store, cluster, tc.name, claim); err == nil {
				t.Fatal("competing preparation accepted")
			}
			again, err := store.Load(ctx, cluster)
			if err != nil || !sameJSON(after.Queue(), again.Queue()) || !sameJSON(before.authorityRaw, again.authorityRaw) {
				t.Fatalf("competing preparation changed state: %v", err)
			}
			_, meta := load(tc.name)
			var fs []string
			if json.Unmarshal(meta["finalizers"], &fs) != nil || len(fs) != 2 || fs[0] != V1Alpha2RecoveryRecordFinalizer || fs[1] != "example.invalid/keep" {
				t.Fatalf("finalizers changed: %s", meta["finalizers"])
			}
		})
	}
}
