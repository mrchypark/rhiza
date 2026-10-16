package network

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTerminalDrainRequiresAuthorizationAndKnownOutcome(t *testing.T) {
	for _, tc := range []struct {
		name, method, token string
		failure             error
		status              int
		called              bool
	}{
		{"method", http.MethodGet, "admin", nil, http.StatusMethodNotAllowed, false},
		{"missing", http.MethodPost, "", nil, http.StatusForbidden, false},
		{"wrong", http.MethodPost, "other", nil, http.StatusForbidden, false},
		{"unknown", http.MethodPost, "admin", errors.New("unknown mutation"), http.StatusServiceUnavailable, true},
		{"known", http.MethodPost, "admin", nil, http.StatusOK, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			s := &Server{recoveryArchiveToken: "admin"}
			s.SetTerminalDrain(func(context.Context, TerminalDrainRequest) (any, error) {
				called = true
				return struct {
					StorageKnown bool `json:"storageKnown"`
				}{true}, tc.failure
			})
			req := httptest.NewRequest(tc.method, "/recovery/drain", strings.NewReader(`{"clusterID":"cluster","nodeID":"node","walIdentity":"wal"}`))
			req.Header.Set("Authorization", "Bearer "+tc.token)
			response := httptest.NewRecorder()
			s.handleTerminalDrain(response, req)
			if response.Code != tc.status || called != tc.called {
				t.Fatalf("status=%d called=%t", response.Code, called)
			}
		})
	}
}

func TestTerminalDrainClosedHTTPPreservesNotReadyErrorCode(t *testing.T) {
	s := &Server{handlerClosing: true, mux: http.NewServeMux()}
	for _, route := range []string{"/membership/change", "/membership/abort", "/membership/status", "/sql/execute", "/sql/query", "/recovery/archive", "/recovery/probe", "/recovery/status", "/healthz", "/ready"} {
		recorder := httptest.NewRecorder()
		s.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, route, nil))
		var response ErrorResponse
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if recorder.Code != http.StatusServiceUnavailable || response.Code != "not_ready" {
			t.Fatalf("%s status=%d code=%s", route, recorder.Code, response.Code)
		}
	}
}
