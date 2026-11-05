package operator

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
)

// The harness seeds host evidence administratively; it is not a DB outcome.
func TestV1Alpha2AuthorityAPIServerContract(t *testing.T) {
	server := os.Getenv("RHIZA_CONTRACT_SERVER")
	if server == "" {
		t.Skip("API server contract environment not configured")
	}
	required := func(key string) string {
		t.Helper()
		v := os.Getenv(key)
		if v == "" {
			t.Fatalf("%s is required", key)
		}
		return v
	}
	ca, err := os.ReadFile(required("RHIZA_CONTRACT_CA"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid contract CA")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	ns := required("RHIZA_CONTRACT_NAMESPACE")
	store := &V1Alpha2StatusStore{Kube: &Kubernetes{BaseURL: server, Namespace: ns, TokenFile: required("RHIZA_CONTRACT_TOKEN"), Client: &http.Client{Transport: transport}}}
	ctx := context.Background()
	ref := V1Alpha2ClusterRef{Name: "authority-contract", Namespace: ns, UID: required("RHIZA_CONTRACT_AUTHORITY_UID")}
	first, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	original := network.ExecuteRequest{RequestID: "request-1", SQL: "SELECT 1"}
	wrong := ref
	wrong.UID += "-other"
	if _, err := LoadV1Alpha2Admission(ctx, store, wrong); err == nil {
		t.Fatal("wrong UID accepted")
	}
	stored := historicalPendingSnapshot(t, first, original)
	endpoint, err := store.path(ref)
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]json.RawMessage
	// Administrative historical UNKNOWN fixture, not a production writer API.
	if err := store.Kube.Put(ctx, endpoint+"/status", stored.status.object, &response); err != nil {
		t.Fatal(err)
	}
	observed, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := stored.Pending()
	if got, pending := observed.Pending(); !pending || got != request {
		t.Fatalf("pending request: %+v %t", got, pending)
	}
	finished, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil || finished.ResourceVersion() != observed.ResourceVersion() {
		t.Fatalf("read-only UNKNOWN observation mutated status: %v", err)
	}
	beforeStatus, err := jsonObject(first.status.object["status"])
	if err != nil {
		t.Fatal(err)
	}
	afterStatus, err := jsonObject(finished.status.object["status"])
	if err != nil {
		t.Fatal(err)
	}
	beforeAuth, err := jsonObject(beforeStatus["authority"])
	if err != nil {
		t.Fatal(err)
	}
	afterAuth, err := jsonObject(afterStatus["authority"])
	if err != nil {
		t.Fatal(err)
	}
	if !sameJSON(beforeStatus["other"], afterStatus["other"]) || !sameJSON(beforeAuth["receipt"], afterAuth["receipt"]) || first.Authority() != finished.Authority() {
		t.Fatal("unrelated status or authority binding changed")
	}
}

// A fully bound pending request is still forbidden on the normal append-only
// CR. This is a real API-server dry run, not a structural CEL-string proof.
func verifyMixedReservationCEL(ctx context.Context, t *testing.T, store *V1Alpha2StatusStore, old V1Alpha2AdmissionSnapshot, command network.ExecuteRequest) {
	t.Helper()
	if _, pending := old.Pending(); pending {
		t.Fatal("new pending prohibition fixture must start clean")
	}
	fixture := historicalPendingSnapshot(t, old, command)
	endpoint, err := store.path(old.status.ref)
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]json.RawMessage
	err = store.Kube.Put(ctx, endpoint+"/status?dryRun=All", fixture.status.object, &response)
	var api *APIError
	if !errors.As(err, &api) || api.StatusCode != 422 || !strings.Contains(err.Error(), "append-only profiles cannot create CR application pending writes") {
		t.Fatalf("fully bound new CR pending was not explicitly rejected: %v", err)
	}
	loaded, err := LoadV1Alpha2Admission(ctx, store, old.status.ref)
	if err != nil || loaded.ResourceVersion() != old.ResourceVersion() || !sameJSON(loaded.status.object["status"], old.status.object["status"]) {
		t.Fatal("rejected new pending changed status", err)
	}
}
