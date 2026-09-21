package recoveryanchor

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/recovery"
)

type testBackend struct {
	mu   sync.Mutex
	data map[string]Record
	rvs  map[string]string
}

func newTestBackend() *testBackend {
	return &testBackend{data: make(map[string]Record), rvs: make(map[string]string)}
}

func (b *testBackend) Read(_ context.Context, id string) (Record, string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rec, ok := b.data[id]
	if !ok {
		return Record{}, "", fmt.Errorf("not found")
	}
	return rec, b.rvs[id], nil
}

func (b *testBackend) CAS(_ context.Context, id string, version string, rec Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.data[id]; ok && version == "" {
		return fmt.Errorf("already exists")
	}
	if version != "" && b.rvs[id] != version {
		return fmt.Errorf("version mismatch")
	}
	b.data[id] = rec
	b.rvs[id] = fmt.Sprintf("rv-%d", rec.Generation)
	return nil
}

func seed(b *testBackend, id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data[id] = Record{
		Version:        Version1,
		EvidenceFormat: "test-schema",
		Binding:        Binding{ClusterID: "src", StorageID: "src-store"},
		Generation:     0,
	}
	b.rvs[id] = "rv-0"
}

func httpHex64(b byte) string {
	s := make([]byte, 64)
	for i := range s {
		s[i] = "0123456789abcdef"[i%16]
	}
	if b != 0 {
		s[0] = "0123456789abcdef"[b%16]
	}
	return string(s)
}

func testReq() Request {
	return Request{
		AnchorID:         "anc",
		OperationID:      "op",
		StatefulSetUID:   "sts",
		SourceClusterID:  "src",
		TargetClusterID:  "tgt",
		SourcePrefix:     "src",
		TargetPrefix:     "tgt",
		SourceMembership: "sm",
		FenceHash:        httpHex64(1),
		TargetAnchorHash: httpHex64(2),
		Fork:             recovery.ForkResult{Tip: 10, ManifestHash: httpHex64(3), PrefixHash: httpHex64(4)},
		TargetMembership: recovery.MembershipRecord{Version: recovery.MembershipRecordVersion, Cluster: "tgt"},
	}
}

func testCoord(t *testing.T) (*Coordinator, *testBackend) {
	t.Helper()
	b := newTestBackend()
	seed(b, "anc")
	coord := &Coordinator{
		Backend:        b,
		EvidenceFormat: "test-schema",
		Verifier: func(_ context.Context, req Request, rec Record) (Binding, []byte, error) {
			if rec.Transition == nil {
				return Binding{}, nil, fmt.Errorf("no transition")
			}
			return Binding{ClusterID: req.TargetClusterID, StorageID: "tgt-store"}, rec.Transition.Evidence, nil
		},
	}
	return coord, b
}

func genCert(t *testing.T, dir string) (cert, key string) {
	t.Helper()
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	keyBytes, _ := x509.MarshalECPrivateKey(k)
	os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}), 0o600)
	return certPath, keyPath
}

func doAction(t *testing.T, url, token, action string, req Request, receipt *Receipt) *http.Response {
	t.Helper()
	ar := actionRequest{Action: action, Request: req, Receipt: receipt}
	body, _ := json.Marshal(ar)
	httpReq, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	httpReq.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// --- Handler tests ---

func TestNewHandler_ActivateRoundtrip(t *testing.T) {
	coord, _ := testCoord(t)
	handler := NewHandler(coord, "tok")
	server := httptest.NewServer(handler)
	defer server.Close()

	resp := doAction(t, server.URL, "tok", "activate", testReq(), nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var receipt Receipt
	if err := json.NewDecoder(resp.Body).Decode(&receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Generation != 1 {
		t.Errorf("gen = %d", receipt.Generation)
	}
}

func TestNewHandler_VerifyRoundtrip(t *testing.T) {
	coord, _ := testCoord(t)
	handler := NewHandler(coord, "tok")
	server := httptest.NewServer(handler)
	defer server.Close()

	resp := doAction(t, server.URL, "tok", "activate", testReq(), nil)
	var receipt Receipt
	json.NewDecoder(resp.Body).Decode(&receipt)
	resp.Body.Close()

	resp2 := doAction(t, server.URL, "tok", "verify", testReq(), &receipt)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("verify status = %d", resp2.StatusCode)
	}
	var out Receipt
	if err := json.NewDecoder(resp2.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != receipt {
		t.Error("returned receipt != submitted")
	}
}

func TestNewHandler_EmptyToken(t *testing.T) {
	coord, _ := testCoord(t)
	handler := NewHandler(coord, "")
	server := httptest.NewServer(handler)
	defer server.Close()

	resp := doAction(t, server.URL, "tok", "activate", testReq(), nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestNewHandler_NilCoordinator(t *testing.T) {
	handler := NewHandler(nil, "tok")
	server := httptest.NewServer(handler)
	defer server.Close()

	resp := doAction(t, server.URL, "tok", "activate", testReq(), nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

func TestNewHandler_WrongMethod(t *testing.T) {
	coord, _ := testCoord(t)
	handler := NewHandler(coord, "tok")
	server := httptest.NewServer(handler)
	defer server.Close()

	resp, _ := http.Get(server.URL)
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

func TestNewHandler_MalformedTrailing(t *testing.T) {
	coord, _ := testCoord(t)
	handler := NewHandler(coord, "tok")
	server := httptest.NewServer(handler)
	defer server.Close()

	body := []byte(`{"action":"activate","request":{}}}`)
	httpReq, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewReader(body))
	httpReq.Header.Set("Authorization", "Bearer tok")
	resp, _ := http.DefaultClient.Do(httpReq)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("trailing bracket status = %d, want 400", resp.StatusCode)
	}
}

func TestNewHandler_UnknownField(t *testing.T) {
	coord, _ := testCoord(t)
	handler := NewHandler(coord, "tok")
	server := httptest.NewServer(handler)
	defer server.Close()

	body := []byte(`{"action":"activate","request":{},"bogus":true}`)
	httpReq, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewReader(body))
	httpReq.Header.Set("Authorization", "Bearer tok")
	resp, _ := http.DefaultClient.Do(httpReq)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("unknown field status = %d, want 400", resp.StatusCode)
	}
}

func TestNewHandler_Oversize(t *testing.T) {
	coord, _ := testCoord(t)
	handler := NewHandler(coord, "tok")
	server := httptest.NewServer(handler)
	defer server.Close()

	big := bytes.Repeat([]byte("x"), maxBody+100)
	httpReq, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewReader(big))
	httpReq.Header.Set("Authorization", "Bearer tok")
	resp, _ := http.DefaultClient.Do(httpReq)
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", resp.StatusCode)
	}
}

func TestNewHandler_VerifyMissingReceipt(t *testing.T) {
	coord, _ := testCoord(t)
	handler := NewHandler(coord, "tok")
	server := httptest.NewServer(handler)
	defer server.Close()

	body := []byte(`{"action":"verify","request":{}}`)
	httpReq, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewReader(body))
	httpReq.Header.Set("Authorization", "Bearer tok")
	resp, _ := http.DefaultClient.Do(httpReq)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("missing receipt status = %d, want 400", resp.StatusCode)
	}
}

func TestNewHandler_ForgedReceipt(t *testing.T) {
	coord, _ := testCoord(t)
	handler := NewHandler(coord, "tok")
	server := httptest.NewServer(handler)
	defer server.Close()

	resp := doAction(t, server.URL, "tok", "activate", testReq(), nil)
	var receipt Receipt
	json.NewDecoder(resp.Body).Decode(&receipt)
	resp.Body.Close()

	forged := receipt
	forged.Generation = 999
	resp2 := doAction(t, server.URL, "tok", "verify", testReq(), &forged)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Errorf("forged receipt status = %d, want 400", resp2.StatusCode)
	}
}
