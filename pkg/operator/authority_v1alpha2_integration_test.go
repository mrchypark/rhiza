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
	request := V1Alpha2WriteRequest{Kind: "fixture", ID: "request-1", Fingerprint: "fingerprint-1"}
	wrong := ref
	wrong.UID += "-other"
	if _, err := LoadV1Alpha2Admission(ctx, store, wrong); err == nil {
		t.Fatal("wrong UID accepted")
	}
	reserved, outcome, err := ReserveV1Alpha2Write(ctx, store, first, first.Authority(), first.Evidence(), request)
	if err != nil || outcome != V1Alpha2CASApplied || reserved.status.ResourceVersion() == first.status.ResourceVersion() {
		t.Fatalf("reserve: %s %v", outcome, err)
	}
	if _, _, err := ReserveV1Alpha2Write(ctx, store, first, first.Authority(), first.Evidence(), request); !errors.Is(err, ErrV1Alpha2StatusConflict) {
		t.Fatalf("stale reservation: %v", err)
	}
	observed, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	if got, pending := observed.Pending(); !pending || got != request {
		t.Fatalf("pending request: %+v %t", got, pending)
	}
	other := request
	other.ID = "other"
	if _, _, err := FinalizeV1Alpha2Write(ctx, store, observed, observed.Authority(), other, V1Alpha2WriteCommitted, json.RawMessage(`{"host":"after"}`)); err == nil {
		t.Fatal("different request finalized pending write")
	}
	finished, outcome, err := FinalizeV1Alpha2Write(ctx, store, observed, observed.Authority(), request, V1Alpha2WriteCommitted, json.RawMessage(`{"host":"after"}`))
	if err != nil || outcome != V1Alpha2CASApplied || finished.status.ResourceVersion() == observed.status.ResourceVersion() {
		t.Fatalf("finalize: %s %v", outcome, err)
	}
	if _, pending := finished.Pending(); pending || !sameJSON(finished.Evidence(), json.RawMessage(`{"host":"after"}`)) {
		t.Fatal("finalized state differs")
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
