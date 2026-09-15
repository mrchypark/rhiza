package recoveryanchor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// httpGenTestTLS creates a TLS httptest server and returns the server and a
// client pre-configured with the server's CA trust chain.
func httpGenTestTLS(t *testing.T, handler http.Handler) (*httptest.Server, *http.Client) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	return server, server.Client()
}

// --- Client tests ---

func TestClient_ActivateAndVerify(t *testing.T) {
	coord, _ := testCoord(t)
	handler := NewHandler(coord, "ctok")
	server, client := httpGenTestTLS(t, handler)
	defer server.Close()

	tokFile := filepath.Join(t.TempDir(), "token")
	os.WriteFile(tokFile, []byte("ctok"), 0o600)

	cl := &Client{URL: server.URL, TokenFile: tokFile, Client: client}

	receipt, err := cl.Activate(context.Background(), testReq())
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	if receipt.Generation != 1 {
		t.Errorf("gen = %d", receipt.Generation)
	}
	if receipt.Target.StorageID == "" {
		t.Error("empty StorageID")
	}
	if receipt.AnchorID != "anc" {
		t.Errorf("AnchorID = %q, want anc", receipt.AnchorID)
	}
	if receipt.OperationID != "op" {
		t.Errorf("OperationID = %q, want op", receipt.OperationID)
	}

	if err := cl.Verify(context.Background(), testReq(), receipt); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestClient_HTTPSOnly(t *testing.T) {
	cl := &Client{URL: "http://localhost:9191", TokenFile: "/dev/null"}
	_, err := cl.Activate(context.Background(), testReq())
	if err == nil {
		t.Error("expected error for HTTP URL")
	}
}

func TestClient_ForceQueryReject(t *testing.T) {
	cl := &Client{URL: "https://host?x=1", TokenFile: "/dev/null"}
	_, err := cl.Activate(context.Background(), testReq())
	if err == nil {
		t.Error("expected error for URL with query")
	}
}

func TestClient_NilClientNoPanic(t *testing.T) {
	// Client.Client is nil — cloneClient must use defaults, no panic.
	// The TLS unknown CA error proves the default transport was used.
	coord, _ := testCoord(t)
	handler := NewHandler(coord, "x")
	server := httptest.NewTLSServer(handler)
	defer server.Close()

	tokFile := filepath.Join(t.TempDir(), "tok")
	os.WriteFile(tokFile, []byte("x"), 0o600)

	cl := &Client{URL: server.URL, TokenFile: tokFile}
	// Client.Client is nil here — should not panic.
	_, err := cl.Activate(context.Background(), testReq())
	if err == nil {
		t.Error("expected TLS error from default transport")
	}
}

func TestClient_RedirectDoesNotLeak(t *testing.T) {
	// First server redirects to second server. Client must not follow.
	secondHit := make(chan struct{}, 1)
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondHit <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer second.Close()

	firstHit := make(chan struct{}, 1)
	first := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstHit <- struct{}{}
		http.Redirect(w, r, second.URL, http.StatusFound)
	}))
	defer first.Close()

	tokFile := filepath.Join(t.TempDir(), "tok")
	os.WriteFile(tokFile, []byte("t"), 0o600)

	// Use first server's trust chain but allow redirects so the 302 is
	// returned as a response (not an error) — then check it wasn't followed.
	cl := &Client{
		URL:       first.URL,
		TokenFile: tokFile,
		Client: &http.Client{
			Transport:     first.Client().Transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return nil },
		},
	}
	_, err := cl.Activate(context.Background(), testReq())
	// The redirect response is 302, not 200 — client should return an error.
	if err == nil {
		t.Error("expected error for redirect response")
	}

	// First server was hit.
	select {
	case <-firstHit:
	default:
		t.Error("first server not hit")
	}

	// Second server was NOT hit.
	select {
	case <-secondHit:
		t.Error("redirect was followed")
	default:
	}
}

func TestClient_MissingReceiptResponse(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	server, client := httpGenTestTLS(t, handler)
	defer server.Close()

	tokFile := filepath.Join(t.TempDir(), "tok")
	os.WriteFile(tokFile, []byte("t"), 0o600)

	cl := &Client{URL: server.URL, TokenFile: tokFile, Client: client}
	_, err := cl.Activate(context.Background(), testReq())
	if err == nil {
		t.Error("expected decode error for empty response")
	}
}

func TestClient_ForgedWrongID(t *testing.T) {
	// Server returns receipt with wrong AnchorID — client must reject.
	// RequestHash must be actual to avoid hash-reject masking the ID check.
	served := make(chan struct{}, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served <- struct{}{}
		var ar actionRequest
		json.NewDecoder(r.Body).Decode(&ar)
		rhash, _ := RequestHash(ar.Request)
		receipt := Receipt{
			AnchorID:    "WRONG",
			OperationID: ar.Request.OperationID,
			RequestHash: rhash,
			Target:      Binding{ClusterID: ar.Request.TargetClusterID, StorageID: "x"},
			Generation:  1,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(receipt)
	})
	server, client := httpGenTestTLS(t, handler)
	defer server.Close()

	tokFile := filepath.Join(t.TempDir(), "tok")
	os.WriteFile(tokFile, []byte("t"), 0o600)

	cl := &Client{URL: server.URL, TokenFile: tokFile, Client: client}
	_, err := cl.Activate(context.Background(), testReq())
	if err == nil {
		t.Error("expected error for forged AnchorID")
	}

	// Server was actually hit — error is from client validation, not URL rejection.
	select {
	case <-served:
	default:
		t.Error("server not hit — error came before request")
	}
}
