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
	// Use an opaque one-key payload so a same-size unknown-key substitution
	// exercises actual admission rather than CEL's incomplete object equality.
	seedStatus, err := jsonObject(stored.status.object["status"])
	if err != nil {
		t.Fatal(err)
	}
	seedAuthority, err := jsonObject(seedStatus["authority"])
	if err != nil {
		t.Fatal(err)
	}
	seedPending, err := jsonObject(seedAuthority["pendingRequest"])
	if err != nil {
		t.Fatal(err)
	}
	seedAuthority["evidence"] = json.RawMessage(`{"digest":"a"}`)
	seedPending["priorEvidence"] = json.RawMessage(`{"digest":"a"}`)
	seedAuthority["pendingRequest"], err = json.Marshal(seedPending)
	if err != nil {
		t.Fatal(err)
	}
	seedStatus["authority"], err = json.Marshal(seedAuthority)
	if err != nil {
		t.Fatal(err)
	}
	stored.status.object["status"], err = json.Marshal(seedStatus)
	if err != nil {
		t.Fatal(err)
	}
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
	if !sameJSON(observed.Evidence(), json.RawMessage(`{"digest":"a"}`)) {
		t.Fatal("historical opaque evidence was pruned or changed during seed")
	}
	for _, mutation := range []struct {
		name   string
		change func(map[string]json.RawMessage, map[string]json.RawMessage)
	}{
		{"unknown-evidence-key-swap", func(_ map[string]json.RawMessage, auth map[string]json.RawMessage) {
			auth["evidence"] = json.RawMessage(`{"other":"a"}`)
		}},
		{"clear-pending", func(_ map[string]json.RawMessage, auth map[string]json.RawMessage) {
			auth["pendingWrite"] = json.RawMessage(`false`)
			delete(auth, "pendingRequest")
		}},
		{"transition", func(_ map[string]json.RawMessage, auth map[string]json.RawMessage) {
			auth["transition"] = json.RawMessage(`{"id":"must-not-activate"}`)
		}},
		{"unrelated-status", func(status map[string]json.RawMessage, _ map[string]json.RawMessage) {
			status["diagnostic"] = json.RawMessage(`true`)
		}},
		{"identical-update", func(map[string]json.RawMessage, map[string]json.RawMessage) {}},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			status, err := jsonObject(observed.status.object["status"])
			if err != nil {
				t.Fatal(err)
			}
			auth, err := jsonObject(status["authority"])
			if err != nil {
				t.Fatal(err)
			}
			mutation.change(status, auth)
			status["authority"], err = json.Marshal(auth)
			if err != nil {
				t.Fatal(err)
			}
			proposed := make(map[string]json.RawMessage, len(observed.status.object))
			for key, value := range observed.status.object {
				proposed[key] = value
			}
			proposed["status"], err = json.Marshal(status)
			if err != nil {
				t.Fatal(err)
			}
			err = store.Kube.Put(ctx, endpoint+"/status", proposed, &response)
			var api *APIError
			if !errors.As(err, &api) || api.StatusCode != 422 || !strings.Contains(err.Error(), "historical unresolved CR pending is read-only; updates are unsupported") {
				t.Fatalf("historical UNKNOWN update was not explicitly denied: %v", err)
			}
			live, err := LoadV1Alpha2Admission(ctx, store, ref)
			if err != nil || live.ResourceVersion() != observed.ResourceVersion() || !sameJSON(live.status.object["status"], observed.status.object["status"]) {
				t.Fatalf("rejected historical UNKNOWN update changed original status: %v", err)
			}
		})
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
