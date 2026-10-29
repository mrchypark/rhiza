package operator

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestV1Alpha2CancellationAPIServerContract(t *testing.T) {
	server := os.Getenv("RHIZA_CONTRACT_SERVER")
	if server == "" {
		if os.Getenv("RHIZA_CONTRACT_REQUIRED") != "" {
			t.Fatal("RHIZA_CONTRACT_SERVER is required")
		}
		t.Skip("API server contract environment not configured")
	}
	getenv := func(key string) string {
		t.Helper()
		v := os.Getenv(key)
		if v == "" {
			t.Fatalf("%s is required", key)
		}
		return v
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
	store := &V1Alpha2StatusStore{Kube: kube}
	ctx := context.Background()
	base := "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/" + ns + "/rhizarecoveries"
	for _, tc := range []struct {
		name, uidEnv      string
		retained, waiting bool
	}{
		{"cancel-complete", "RHIZA_CONTRACT_COMPLETE_UID", false, false},
		{"cancel-retained", "RHIZA_CONTRACT_RETAINED_UID", true, false},
		{"cancel-waiting", "RHIZA_CONTRACT_WAITING_UID", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref := V1Alpha2ClusterRef{Name: tc.name, Namespace: ns, UID: getenv(tc.uidEnv)}
			name := tc.name + "-first"
			var obj map[string]json.RawMessage
			if err := kube.Get(ctx, base+"/"+name, &obj); err != nil {
				t.Fatal(err)
			}
			meta, err := jsonObject(obj["metadata"])
			if err != nil {
				t.Fatal(err)
			}
			var uid string
			if json.Unmarshal(meta["uid"], &uid) != nil || uid == "" {
				t.Fatal("invalid head request UID")
			}
			var claim V1Alpha2QueueClaim
			if !tc.waiting {
				claim, err = ClaimV1Alpha2Recovery(ctx, store, ref)
				if err != nil || claim.RequestUID != uid {
					t.Fatalf("wrong head claim: %+v %v", claim, err)
				}
			}
			if err := kube.request(ctx, http.MethodDelete, base+"/"+name, object{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": object{"uid": uid}}, nil); err != nil {
				t.Fatal(err)
			}
			if tc.waiting {
				claim, err = ClaimV1Alpha2RecoveryCancellation(ctx, store, ref)
				if err != nil || claim.RequestUID != uid || claim.Phase != "CancelRequested" {
					t.Fatalf("waiting cancellation claim: %+v %v", claim, err)
				}
			} else {
				if err := RegisterV1Alpha2RecoveryCancellation(ctx, store, ref, name, claim); err != nil {
					t.Fatal(err)
				}
				claim.Phase = "CancelRequested"
			}
			before, err := store.Load(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			if tc.retained {
				for i := 0; i < 8; i++ {
					result, err := ReconcileV1Alpha2Cancellation(ctx, store, ref, name, claim)
					if err != nil || result != V1Alpha2CancellationPending {
						t.Fatalf("retained reconciliation %d: result=%q error=%v", i, result, err)
					}
				}
				live, err := store.Load(ctx, ref)
				if err != nil || len(live.Queue()) == 0 || !sameJSON(before.authorityRaw, live.authorityRaw) {
					t.Fatalf("retained request lost claim or authority: %v", err)
				}
				if err := kube.Get(ctx, base+"/"+name, &obj); err != nil {
					t.Fatalf("retained request missing: %v", err)
				}
				var decision struct {
					Claim   V1Alpha2QueueClaim `json:"claim"`
					Outcome string             `json:"outcome"`
				}
				if json.Unmarshal(obj["status"], &decision) != nil || decision.Outcome != "Canceled" || decision.Claim != claim {
					t.Fatalf("wrong request acknowledgment: %s", obj["status"])
				}
				meta, err = jsonObject(obj["metadata"])
				if err != nil {
					t.Fatal(err)
				}
				var fs []string
				if json.Unmarshal(meta["finalizers"], &fs) != nil || len(fs) != 1 || fs[0] != "example.invalid/keep" {
					t.Fatalf("unexpected retained finalizers: %s", meta["finalizers"])
				}
				// Administrative fixture cleanup: remove only the fixture's own finalizer.
				meta["finalizers"] = json.RawMessage(`[]`)
				obj["metadata"], _ = json.Marshal(meta)
				if err := kube.Put(ctx, base+"/"+name, obj, nil); err != nil {
					t.Fatal(err)
				}
			}
			cleared := false
			for i := 0; i < 40; i++ {
				result, err := ReconcileV1Alpha2Cancellation(ctx, store, ref, name, claim)
				if err != nil {
					t.Fatalf("reconciliation %d: %v", i, err)
				}
				if result != V1Alpha2CancellationPending && result != V1Alpha2CancellationComplete {
					t.Fatalf("reconciliation %d returned unknown result %q", i, result)
				}
				live, err := store.Load(ctx, ref)
				if err != nil {
					t.Fatal(err)
				}
				if !sameJSON(before.authorityRaw, live.authorityRaw) {
					t.Fatal("authority changed")
				}
				if result == V1Alpha2CancellationComplete {
					if len(live.Queue()) != 0 {
						t.Fatalf("complete with queue %s", live.Queue())
					}
					cleared = true
					break
				}
				if len(live.Queue()) == 0 {
					t.Fatal("pending with empty queue")
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !cleared {
				t.Fatal("cancellation did not clear queue")
			}
			var list struct {
				APIVersion string `json:"apiVersion"`
				Kind       string `json:"kind"`
				Metadata   struct {
					ResourceVersion string `json:"resourceVersion"`
					Continue        string `json:"continue"`
				} `json:"metadata"`
				Items []json.RawMessage `json:"items"`
			}
			if err := kube.Get(ctx, base, &list); err != nil || list.APIVersion != V1Alpha2APIVersion || list.Kind != "RhizaRecoveryList" || strings.TrimSpace(list.Metadata.ResourceVersion) == "" || list.Metadata.Continue != "" || list.Items == nil {
				t.Fatalf("invalid or incomplete list: %v", err)
			}
			seenUIDs := map[string]bool{}
			seenNames := map[string]bool{}
			for _, raw := range list.Items {
				var item struct {
					Metadata struct {
						UID             string `json:"uid"`
						Namespace       string `json:"namespace"`
						Name            string `json:"name"`
						ResourceVersion string `json:"resourceVersion"`
					} `json:"metadata"`
				}
				if json.Unmarshal(raw, &item) != nil || strings.TrimSpace(item.Metadata.UID) == "" || item.Metadata.Namespace != ns || strings.TrimSpace(item.Metadata.Name) == "" || strings.TrimSpace(item.Metadata.ResourceVersion) == "" || seenUIDs[item.Metadata.UID] || seenNames[item.Metadata.Name] {
					t.Fatal("invalid or duplicate list identity")
				}
				if item.Metadata.UID == uid {
					t.Fatal("old UID remains")
				}
				seenUIDs[item.Metadata.UID] = true
				seenNames[item.Metadata.Name] = true
			}
			next, err := ClaimV1Alpha2Recovery(ctx, store, ref)
			if err != nil || next.RequestUID == uid {
				t.Fatalf("next claim: %+v %v", next, err)
			}
			if err := kube.Get(ctx, base+"/"+tc.name+"-next", &obj); err != nil {
				t.Fatal(err)
			}
			meta, err = jsonObject(obj["metadata"])
			if err != nil {
				t.Fatal(err)
			}
			var nextUID string
			if json.Unmarshal(meta["uid"], &nextUID) != nil || next.RequestUID != nextUID {
				t.Fatal("wrong successor claimed")
			}
		})
	}
}
