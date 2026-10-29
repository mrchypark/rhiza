package operator

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"sort"
	"testing"
)

// The harness seeds authority as an administrative fixture, not as genesis
// evidence or permission to activate a recovery.
func TestV1Alpha2StatusAPIServerContract(t *testing.T) {
	server := os.Getenv("RHIZA_CONTRACT_SERVER")
	if server == "" {
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
	store := &V1Alpha2StatusStore{Kube: &Kubernetes{BaseURL: server, Namespace: ns, TokenFile: getenv("RHIZA_CONTRACT_TOKEN"), Client: &http.Client{Transport: transport}}}
	ctx := context.Background()
	ref := V1Alpha2ClusterRef{Name: "status-contract", Namespace: ns, UID: getenv("RHIZA_CONTRACT_STATUS_UID")}
	old, err := store.Load(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	queue := json.RawMessage(`{"test":"winner"}`)
	updated, err := store.compareAndSwapQueueObserved(ctx, old, queue)
	if err != nil {
		t.Fatal(err)
	}
	if updated.ResourceVersion() == old.ResourceVersion() || !sameJSON(updated.authorityRaw, old.authorityRaw) || !sameJSON(updated.Queue(), queue) {
		t.Fatal("status update changed authority or failed to persist queue and revision")
	}
	var before, after map[string]json.RawMessage
	if err := json.Unmarshal(old.object["status"], &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(updated.object["status"], &after); err != nil {
		t.Fatal(err)
	}
	if !sameJSON(before["other"], after["other"]) {
		t.Fatal("unrelated status changed")
	}
	if _, err := store.compareAndSwapQueueObserved(ctx, old, json.RawMessage(`{"test":"loser"}`)); !errors.Is(err, ErrV1Alpha2StatusConflict) {
		t.Fatalf("stale update: %v", err)
	}
	live, err := store.Load(ctx, ref)
	if err != nil || !sameJSON(live.Queue(), queue) {
		t.Fatalf("winner overwritten: %v", err)
	}
	missing := V1Alpha2ClusterRef{Name: "missing-authority", Namespace: ns, UID: getenv("RHIZA_CONTRACT_MISSING_UID")}
	if _, err := store.Load(ctx, missing); err == nil {
		t.Fatal("missing authority accepted")
	}
	var missingObject map[string]json.RawMessage
	if err := store.Kube.Get(ctx, "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/"+ns+"/rhizaclusters/missing-authority", &missingObject); err != nil {
		t.Fatal(err)
	}
	if len(missingObject["status"]) != 0 && string(missingObject["status"]) != "{}" && string(missingObject["status"]) != "null" {
		t.Fatal("missing authority was bootstrapped")
	}
	queueRef := V1Alpha2ClusterRef{Name: "queue-contract", Namespace: ns, UID: getenv("RHIZA_CONTRACT_QUEUE_UID")}
	var list struct {
		Items []struct {
			Metadata struct{ Name, UID, CreationTimestamp string }
		}
	}
	if err := store.Kube.Get(ctx, "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/"+ns+"/rhizarecoveries", &list); err != nil {
		t.Fatal(err)
	}
	var candidates []struct{ Name, UID, CreationTimestamp string }
	for _, item := range list.Items {
		if item.Metadata.Name == "queue-first" || item.Metadata.Name == "queue-second" {
			candidates = append(candidates, item.Metadata)
		}
	}
	if len(candidates) != 2 {
		t.Fatalf("expected two protected requests, got %d", len(candidates))
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].CreationTimestamp == candidates[j].CreationTimestamp {
			return candidates[i].UID < candidates[j].UID
		}
		return candidates[i].CreationTimestamp < candidates[j].CreationTimestamp
	})
	claim, err := ClaimV1Alpha2Recovery(ctx, store, queueRef)
	if err != nil || claim.RequestUID != candidates[0].UID {
		t.Fatalf("claim %+v: %v", claim, err)
	}
	again, err := ClaimV1Alpha2Recovery(ctx, store, queueRef)
	if err != nil || again.OperationID != claim.OperationID || again.RequestUID != claim.RequestUID {
		t.Fatalf("resume %+v: %v", again, err)
	}
}
