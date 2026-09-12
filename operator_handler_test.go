package rhiza_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestEmbeddedOperatorHandler(t *testing.T) {
	db, err := rhiza.Open(context.Background(), rhiza.Config{ClusterID: "embedded", NodeID: "app-0", DataDir: t.TempDir(), AdminToken: "test-admin", ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler := db.OperatorHandler()
	for _, path := range []string{"/sql/execute", "/kv/get", "/ready", "/metrics"} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("%s exposed: %d", path, w.Code)
		}
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/recovery/status", nil))
	var status struct {
		Cluster string `json:"cluster_id"`
		Quorum  bool   `json:"quorum"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &status) != nil || status.Cluster != "embedded" || !status.Quorum {
		t.Fatalf("status: %d %s", w.Code, w.Body)
	}
	for _, token := range []string{"", "wrong", "test-admin"} {
		req := httptest.NewRequest(http.MethodPost, "/recovery/archive", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		want := http.StatusForbidden
		if token == "test-admin" {
			want = http.StatusOK
		}
		if w.Code != want {
			t.Fatalf("archive status %d, want %d: %s", w.Code, want, w.Body)
		}
	}
}

// TestOperatorHandlerRecoveryProbe verifies that the recovery probe endpoint is
// exposed on the operator handler, authenticates correctly, and validates the
// challenge nonce. Regression test for the missing /recovery/probe route.
func TestOperatorHandlerRecoveryProbe(t *testing.T) {
	db, err := rhiza.Open(context.Background(), rhiza.Config{
		ClusterID:        "probe-test",
		NodeID:           "app-0",
		DataDir:          t.TempDir(),
		AdminToken:       "probe-admin",
		ObjStoreProvider: "filesystem",
		ObjStoreDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler := db.OperatorHandler()

	var nonce [32]byte
	for i := range nonce {
		nonce[i] = byte(i)
	}
	challenge := hex.EncodeToString(nonce[:])

	// Valid authenticated probe returns 200 with verifiable RecoveryProbe.
	t.Run("valid_auth_returns_probe", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/recovery/probe?nonce="+challenge, nil)
		req.Header.Set("Authorization", "Bearer probe-admin")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body)
		}
		var probe network.RecoveryProbe
		if err := json.Unmarshal(w.Body.Bytes(), &probe); err != nil {
			t.Fatalf("unmarshal probe: %v", err)
		}
		if !network.VerifyRecoveryProbe(probe, challenge, "probe-admin") {
			t.Fatal("valid probe rejected by VerifyRecoveryProbe")
		}
		if probe.Status.ClusterID != "probe-test" {
			t.Fatalf("cluster ID = %q, want probe-test", probe.Status.ClusterID)
		}
		if probe.Status.NodeID != "app-0" {
			t.Fatalf("node ID = %q, want app-0", probe.Status.NodeID)
		}
	})

	// Wrong admin token is denied.
	t.Run("wrong_token_denied", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/recovery/probe?nonce="+challenge, nil)
		req.Header.Set("Authorization", "Bearer wrong-token")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusForbidden {
			t.Fatalf("expected 403, got %d: %s", w.Code, w.Body)
		}
	})

	// No token is denied.
	t.Run("no_token_denied", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/recovery/probe?nonce="+challenge, nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusForbidden {
			t.Fatalf("expected 403, got %d: %s", w.Code, w.Body)
		}
	})

	// Malformed nonce is rejected.
	t.Run("malformed_nonce_denied", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/recovery/probe?nonce=short", nil)
		req.Header.Set("Authorization", "Bearer probe-admin")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d: %s", w.Code, w.Body)
		}
	})

	// Non-management routes still return 404.
	t.Run("non_management_routes_404", func(t *testing.T) {
		for _, path := range []string{"/sql/execute", "/kv/get", "/ready", "/metrics"} {
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
			if w.Code != http.StatusNotFound {
				t.Fatalf("%s: expected 404, got %d", path, w.Code)
			}
		}
	})
}
