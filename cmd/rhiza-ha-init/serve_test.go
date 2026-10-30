package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/managedsql"
)

func TestCheckReadyUsesPrivateBearerAndRequiresNoContent(t *testing.T) {
	status := http.StatusNoContent
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/readyz" || r.Header.Get("Authorization") != "Bearer private-token" {
			t.Error("incorrect readiness request")
			http.Error(w, "bad", 400)
			return
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(s.Close)
	if err := checkReady(context.Background(), s.Client(), s.URL+"/readyz", "private-token"); err != nil {
		t.Fatal(err)
	}
	status = http.StatusServiceUnavailable
	if err := checkReady(context.Background(), s.Client(), s.URL+"/readyz", "private-token"); err == nil || strings.Contains(err.Error(), "private-token") {
		t.Fatal("failed readiness accepted or token leaked")
	}
	if err := checkReady(context.Background(), s.Client(), s.URL+"/readyz", ""); err == nil {
		t.Fatal("empty token accepted")
	}
}

func TestManagedHandlerAuthenticationAndClosedRoutes(t *testing.T) {
	if _, err := managedHandler(nil, "token"); err == nil {
		t.Fatal("nil host accepted")
	}
	if _, err := managedHandler(&managedsql.Host{}, ""); err == nil {
		t.Fatal("empty token accepted")
	}
	h, err := managedHandler(&managedsql.Host{}, "secret")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path, auth string
		status     int
	}{
		{"/readyz", "", http.StatusUnauthorized}, {"/execute", "Bearer wrong", http.StatusUnauthorized},
		{"/kv", "Bearer secret", http.StatusNotFound}, {"/graph", "Bearer secret", http.StatusNotFound},
		{"/execute", "Bearer secret", http.StatusMethodNotAllowed},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		req.Header.Set("Authorization", tc.auth)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != tc.status || strings.Contains(w.Body.String(), "secret") {
			t.Fatalf("%s status %d", tc.path, w.Code)
		}
	}
	for _, body := range []string{`{"sql":"x","unknown":true}`, `{} {}`, strings.Repeat(" ", (1<<20)+1)} {
		req := httptest.NewRequest(http.MethodPost, "/execute", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer secret")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("invalid body status %d", w.Code)
		}
	}
}
