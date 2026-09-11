package operator

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testCert(t *testing.T) (caCert *x509.Certificate, certFile, keyFile string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err = x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	srvTpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, "server.pem")
	keyFile = filepath.Join(dir, "server.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	keyBytes, err := x509.MarshalECPrivateKey(srvKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}), 0o600); err != nil {
		t.Fatal(err)
	}
	return caCert, certFile, keyFile
}

func tlsClient(t *testing.T, ca *x509.Certificate) *http.Client {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
}

func writeToken(t *testing.T) string {
	t.Helper()
	f := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(f, []byte("test-bearer-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

func newTLSHandler(t *testing.T, h http.Handler) (*httptest.Server, *x509.Certificate) {
	t.Helper()
	caCert, certFile, keyFile := testCert(t)
	srv := httptest.NewUnstartedServer(h)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, caCert
}

func goodProofBody(req FenceRequest, hash string) []byte {
	p := FenceProof{
		OperationID:         req.OperationID,
		SourceClusterID:     req.SourceClusterID,
		BindingUID:          req.BindingUID,
		RequestHash:         hash,
		ProofID:             "proof-001",
		ProcessesTerminated: true,
		RecreationBlocked:   true,
		StorageQuiesced:     true,
	}
	b, _ := json.Marshal(p)
	return b
}

func TestFenceTLSRealHTTP(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "ct", http.StatusBadRequest)
			return
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			http.Error(w, "auth", http.StatusUnauthorized)
			return
		}
		if r.Header.Get("Idempotency-Key") == "" {
			http.Error(w, "idem", http.StatusBadRequest)
			return
		}
		var req FenceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		hashStr, _ := requestHash(&req)
		w.Header().Set("Content-Type", "application/json")
		w.Write(goodProofBody(req, hashStr))
	})
	srv, caCert := newTLSHandler(t, handler)
	fc := &FencingClient{URL: srv.URL + "/fence", TokenFile: writeToken(t), Client: tlsClient(t, caCert)}
	req := FenceRequest{
		LogicalID:       "l-1",
		BindingUID:      "b-1",
		Namespace:       "default",
		StatefulSet:     "ss-1",
		StatefulSetUID:  "ssuid-1",
		SourceClusterID: "src-1",
		OperationID:     "op-1",
		Scope:           ScopeGeneration,
		Targets:         []FenceTarget{{NodeID: "n1", Pod: "p1", PodUID: "pu1", WALIdentity: "w1"}},
	}
	proof, err := fc.Fence(t.Context(), req)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if proof.ProofID != "proof-001" {
		t.Errorf("proofID = %q, want proof-001", proof.ProofID)
	}
	if !proof.ProcessesTerminated || !proof.RecreationBlocked || !proof.StorageQuiesced {
		t.Error("expected all booleans true")
	}
}

func TestFencePending202(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	srv, caCert := newTLSHandler(t, handler)
	fc := &FencingClient{URL: srv.URL + "/fence", TokenFile: writeToken(t), Client: tlsClient(t, caCert)}
	_, err := fc.Fence(t.Context(), FenceRequest{OperationID: "op-2", SourceClusterID: "s", BindingUID: "b", Scope: ScopeVoter})
	if !errors.Is(err, ErrFencePending) {
		t.Errorf("got %v, want ErrFencePending", err)
	}
}

func TestFenceMismatchedProofOperationID(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req FenceRequest
		json.NewDecoder(r.Body).Decode(&req)
		hashStr, _ := requestHash(&req)
		p := FenceProof{
			OperationID:         "wrong-op",
			SourceClusterID:     req.SourceClusterID,
			BindingUID:          req.BindingUID,
			RequestHash:         hashStr,
			ProofID:             "proof-x",
			ProcessesTerminated: true,
			RecreationBlocked:   true,
			StorageQuiesced:     true,
		}
		b, _ := json.Marshal(p)
		w.Write(b)
	})
	srv, caCert := newTLSHandler(t, handler)
	fc := &FencingClient{URL: srv.URL + "/fence", TokenFile: writeToken(t), Client: tlsClient(t, caCert)}
	_, err := fc.Fence(t.Context(), FenceRequest{OperationID: "op-3", SourceClusterID: "s", BindingUID: "b", Scope: ScopeGeneration})
	if err == nil || !strings.Contains(err.Error(), "operationID") {
		t.Errorf("expected operationID mismatch, got: %v", err)
	}
}

func TestFenceRecreationFalseRejected(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req FenceRequest
		json.NewDecoder(r.Body).Decode(&req)
		hashStr, _ := requestHash(&req)
		p := FenceProof{
			OperationID:         req.OperationID,
			SourceClusterID:     req.SourceClusterID,
			BindingUID:          req.BindingUID,
			RequestHash:         hashStr,
			ProofID:             "proof-002",
			ProcessesTerminated: true,
			RecreationBlocked:   false,
			StorageQuiesced:     true,
		}
		b, _ := json.Marshal(p)
		w.Write(b)
	})
	srv, caCert := newTLSHandler(t, handler)
	fc := &FencingClient{URL: srv.URL + "/fence", TokenFile: writeToken(t), Client: tlsClient(t, caCert)}
	_, err := fc.Fence(t.Context(), FenceRequest{OperationID: "op-4", SourceClusterID: "s", BindingUID: "b", Scope: ScopeGeneration})
	if err == nil || !strings.Contains(err.Error(), "booleans incomplete") {
		t.Errorf("expected booleans incomplete, got: %v", err)
	}
}

func TestFenceRedirectNoCredentialForward(t *testing.T) {
	caCert, certFile, keyFile := testCert(t)
	tokFile := writeToken(t)
	var gotAuth string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		http.Redirect(w, r, "/fence", http.StatusTemporaryRedirect)
	})
	srv := httptest.NewUnstartedServer(handler)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	redirectClient := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			req.Header.Del("Authorization")
			return nil
		},
	}
	fc := &FencingClient{URL: srv.URL + "/fence", TokenFile: tokFile, Client: redirectClient}
	_, err = fc.Fence(t.Context(), FenceRequest{OperationID: "op-5", SourceClusterID: "s", BindingUID: "b", Scope: ScopeVoter})
	if err == nil {
		t.Fatal("expected error from redirect")
	}
	if gotAuth != "Bearer test-bearer-token" {
		t.Errorf("first server saw %q, want Bearer test-bearer-token", gotAuth)
	}
}

func TestFenceTokenRotation(t *testing.T) {
	var tokens []string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokens = append(tokens, r.Header.Get("Authorization"))
		var req FenceRequest
		json.NewDecoder(r.Body).Decode(&req)
		hashStr, _ := requestHash(&req)
		w.Write(goodProofBody(req, hashStr))
	})
	srv, caCert := newTLSHandler(t, handler)
	dir := t.TempDir()
	tokFile := filepath.Join(dir, "token")
	fc := &FencingClient{URL: srv.URL + "/fence", TokenFile: tokFile, Client: tlsClient(t, caCert)}
	os.WriteFile(tokFile, []byte("token-v1"), 0o600)
	req1 := FenceRequest{OperationID: "op-a", SourceClusterID: "s", BindingUID: "b", Scope: ScopeGeneration}
	if _, err := fc.Fence(t.Context(), req1); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(tokFile, []byte("token-v2"), 0o600)
	req2 := FenceRequest{OperationID: "op-b", SourceClusterID: "s", BindingUID: "b", Scope: ScopeGeneration}
	if _, err := fc.Fence(t.Context(), req2); err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 {
		t.Fatalf("got %d calls, want 2", len(tokens))
	}
	if tokens[0] != "Bearer token-v1" {
		t.Errorf("first = %q, want Bearer token-v1", tokens[0])
	}
	if tokens[1] != "Bearer token-v2" {
		t.Errorf("second = %q, want Bearer token-v2", tokens[1])
	}
}

func TestFenceBoundedResponse(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(strings.Repeat("x", maxResponseBytes+100)))
	})
	srv, caCert := newTLSHandler(t, handler)
	fc := &FencingClient{URL: srv.URL + "/fence", TokenFile: writeToken(t), Client: tlsClient(t, caCert)}
	_, err := fc.Fence(t.Context(), FenceRequest{OperationID: "op-6", SourceClusterID: "s", BindingUID: "b", Scope: ScopeVoter})
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("expected exceeds error, got: %v", err)
	}
}

func TestFenceInvalidJSONResponse(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("{not valid json"))
	})
	srv, caCert := newTLSHandler(t, handler)
	fc := &FencingClient{URL: srv.URL + "/fence", TokenFile: writeToken(t), Client: tlsClient(t, caCert)}
	_, err := fc.Fence(t.Context(), FenceRequest{OperationID: "op-7", SourceClusterID: "s", BindingUID: "b", Scope: ScopeGeneration})
	if err == nil || !strings.Contains(err.Error(), "decode proof") {
		t.Errorf("expected decode error, got: %v", err)
	}
}

func TestFenceStrictJSONRejectsUnknownFields(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"operationID":"x","sourceClusterID":"s","bindingUID":"b","requestHash":"h","proofID":"p","processesTerminated":true,"recreationBlocked":true,"storageQuiesced":true,"extraField":"surprise"}`))
	})
	srv, caCert := newTLSHandler(t, handler)
	fc := &FencingClient{URL: srv.URL + "/fence", TokenFile: writeToken(t), Client: tlsClient(t, caCert)}
	_, err := fc.Fence(t.Context(), FenceRequest{OperationID: "x", SourceClusterID: "s", BindingUID: "b", Scope: ScopeVoter})
	if err == nil || !strings.Contains(err.Error(), "decode proof") {
		t.Errorf("expected decode error for unknown field, got: %v", err)
	}
}

func TestValidateURL(t *testing.T) {
	tests := []struct {
		name, url, msg string
	}{
		{"http scheme", "http://host/f", "https"},
		{"no host", "https:///f", "host is empty"},
		{"user info", "https://u:p@host/f", "credentials"},
		{"query", "https://host/f?x=1", "query"},
		{"fragment", "https://host/f#s", "fragment"},
		{"empty", "", "invalid endpoint"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateURL(tt.url)
			if err == nil || !strings.Contains(err.Error(), tt.msg) {
				t.Errorf("url=%q: want %q, got %v", tt.url, tt.msg, err)
			}
		})
	}
}

func TestValidateURLNoUserInfoLeaks(t *testing.T) {
	err := validateURL("https://user:pass@host/f")
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "user") || strings.Contains(err.Error(), "pass") {
		t.Errorf("error leaks credentials: %v", err)
	}
}

func TestFenceEmptyTokenFile(t *testing.T) {
	dir := t.TempDir()
	tokFile := filepath.Join(dir, "token")
	os.WriteFile(tokFile, []byte(""), 0o600)
	_, err := readToken(tokFile)
	if err == nil || !strings.Contains(err.Error(), "empty token") {
		t.Errorf("expected empty token error, got: %v", err)
	}
}

func TestFenceMissingTokenFile(t *testing.T) {
	_, err := readToken("/nonexistent/path/token")
	if err == nil {
		t.Fatal("expected error for missing token file")
	}
}

func TestFenceNilClientUsesDefault(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req FenceRequest
		json.NewDecoder(r.Body).Decode(&req)
		hashStr, _ := requestHash(&req)
		w.Write(goodProofBody(req, hashStr))
	})
	srv, caCert := newTLSHandler(t, handler)
	_ = caCert
	fc := &FencingClient{URL: srv.URL + "/fence", TokenFile: writeToken(t)}
	// nil Client uses default with no TLS pool, so TLS handshake fails.
	_, err := fc.Fence(t.Context(), FenceRequest{OperationID: "x", SourceClusterID: "s", BindingUID: "b", Scope: ScopeVoter})
	if err == nil {
		t.Fatal("expected TLS error with default client")
	}
}

func TestRequestHashDeterministic(t *testing.T) {
	req := FenceRequest{LogicalID: "l", BindingUID: "b", Namespace: "ns", OperationID: "op", Scope: ScopeGeneration}
	h1, _ := requestHash(&req)
	h2, _ := requestHash(&req)
	if h1 != h2 {
		t.Errorf("not deterministic: %s != %s", h1, h2)
	}
	if len(h1) != 64 {
		t.Errorf("want 64-char hex, got %d", len(h1))
	}
}

func TestFenceRequestValidation(t *testing.T) {
	tests := []struct {
		name string
		req  FenceRequest
		msg  string
	}{
		{"missing operationID", FenceRequest{SourceClusterID: "s", BindingUID: "b", Scope: ScopeGeneration}, "operationID"},
		{"missing sourceClusterID", FenceRequest{OperationID: "o", BindingUID: "b", Scope: ScopeGeneration}, "sourceClusterID"},
		{"missing bindingUID", FenceRequest{OperationID: "o", SourceClusterID: "s", Scope: ScopeGeneration}, "bindingUID"},
		{"invalid scope", FenceRequest{OperationID: "o", SourceClusterID: "s", BindingUID: "b", Scope: "Bad"}, "scope"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRequest(&tt.req)
			if err == nil || !strings.Contains(err.Error(), tt.msg) {
				t.Errorf("want %q, got %v", tt.msg, err)
			}
		})
	}
}

func TestCloneClientTimeout(t *testing.T) {
	c := cloneClient(nil)
	if c.Timeout != maxFenceTimeout {
		t.Errorf("nil client: timeout = %v, want %v", c.Timeout, maxFenceTimeout)
	}
	cc := cloneClient(&http.Client{Timeout: 30 * time.Second})
	if cc.Timeout != maxFenceTimeout {
		t.Errorf("over-limit: timeout = %v, want %v", cc.Timeout, maxFenceTimeout)
	}
	cd := cloneClient(&http.Client{Timeout: 5 * time.Second})
	if cd.Timeout != 5*time.Second {
		t.Errorf("under-limit: timeout = %v, want 5s", cd.Timeout)
	}
}

// Backend replacement cannot weaken the Operator's recovery barrier.
type resultFencer struct{ proof *FenceProof }

func (f resultFencer) Fence(context.Context, FenceRequest) (*FenceProof, error) {
	return f.proof, nil
}

func TestOperatorValidatesBackendProof(t *testing.T) {
	req := FenceRequest{OperationID: "op", SourceClusterID: "source", BindingUID: "binding", Scope: ScopeGeneration}
	hash, err := requestHash(&req)
	if err != nil {
		t.Fatal(err)
	}
	valid := FenceProof{OperationID: req.OperationID, SourceClusterID: req.SourceClusterID, BindingUID: req.BindingUID, RequestHash: hash, ProofID: "evidence", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}
	for _, name := range []string{"valid", "missing", "only-deleted", "wrong-operation", "wrong-target"} {
		t.Run(name, func(t *testing.T) {
			proof := valid
			var candidate *FenceProof = &proof
			switch name {
			case "missing":
				candidate = nil
			case "only-deleted":
				proof.ProcessesTerminated = false
			case "wrong-operation":
				proof.OperationID = "old-operation"
			case "wrong-target":
				proof.RequestHash = "other-request"
			}
			c := Controller{Fencer: resultFencer{candidate}}
			_, err := c.fence(t.Context(), req)
			if (err == nil) != (name == "valid") {
				t.Fatalf("unexpected proof acceptance: %v", err)
			}
		})
	}
}
