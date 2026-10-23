package operator

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/fencing/runtimeagent"
)

func TestRuntimeAgentEnrollClassifiesWrapperFailures(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		cancel     bool
		deadline   bool
		badConfig  bool
		stage      string
		code       string
		wantIs     error
		wantCanary string
	}{
		{name: "invalid wrapper config", badConfig: true, stage: "client", code: "config_invalid"},
		{name: "node lookup rejected", status: http.StatusForbidden, body: "private node response", stage: "client", code: "node_lookup_failed", wantCanary: "private node response"},
		{name: "node has no internal address", status: http.StatusOK, body: `{"status":{"addresses":[]}}`, stage: "client", code: "node_address_unavailable"},
		{name: "TLS trust invalid", status: http.StatusOK, body: `{"status":{"addresses":[{"type":"InternalIP","address":"127.0.0.1"}]}}`, stage: "client", code: "trust_invalid"},
		{name: "context canceled", cancel: true, stage: "client", code: "context_canceled", wantIs: context.Canceled},
		{name: "deadline exceeded", deadline: true, stage: "client", code: "deadline_exceeded", wantIs: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.status != 0 {
					w.WriteHeader(tt.status)
				}
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()
			tokenFile := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenFile, []byte("fixture-token\n"), 0600); err != nil {
				t.Fatal(err)
			}
			ca := []byte("not-a-certificate")
			if tt.name == "TLS trust invalid" {
				ca = []byte("invalid nonempty certificate bundle")
			}
			client := &RuntimeAgentClient{Kube: &Kubernetes{BaseURL: server.URL, Namespace: "rhiza", TokenFile: tokenFile, Client: server.Client()},
				AuthorityID: "authority", TokenFile: tokenFile, CA: ca, ServerName: "runtime-agent", Port: "9445"}
			if tt.badConfig {
				client = &RuntimeAgentClient{}
			}
			ctx := context.Background()
			if tt.cancel {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			} else if tt.deadline {
				deadline, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				ctx = deadline
			}
			_, err := client.Enroll(ctx, fencing.RuntimeBinding{NodeName: "node-a"})
			var diagnostic *runtimeagent.EnrollmentDiagnosticError
			if !errors.As(err, &diagnostic) || diagnostic.Stage != tt.stage || diagnostic.Code != tt.code || !runtimeagent.ValidEnrollmentDiagnostic(diagnostic.Stage, diagnostic.Code) {
				t.Fatalf("Enroll error=%v; want bounded %s/%s", err, tt.stage, tt.code)
			}
			if tt.wantIs != nil && !errors.Is(err, tt.wantIs) {
				t.Fatalf("Enroll error %v does not preserve %v", err, tt.wantIs)
			}
			if tt.wantCanary != "" && strings.Contains(err.Error(), tt.wantCanary) {
				t.Fatalf("Enroll error leaked Kubernetes response: %v", err)
			}
		})
	}
}
