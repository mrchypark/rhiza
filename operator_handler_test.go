package rhiza_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mrchypark/rhiza"
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
