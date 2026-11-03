//go:build cgo

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/managedbootstrap"
)

func TestManagedStartupDiagnosticPreservesCodeAndRedactsCause(t *testing.T) {
	const secret = "secret-SQL-password-https://private/path-request-ID"
	for _, tc := range []struct {
		name, code, cause string
		err               error
	}{
		{"unknown", "internal", "unclassified", errors.New(secret)},
		{"canceled", "canceled", "canceled", fmt.Errorf("%s: %w", secret, context.Canceled)},
		{"deadline", "timeout", "deadline", errors.Join(errors.New(secret), context.DeadlineExceeded)},
		{"bootstrap", "internal", "bootstrap_pending", fmt.Errorf("%s: %w", secret, managedbootstrap.ErrBootstrapPending)},
		{"learner", "internal", "learner_pending", managedbootstrap.ErrLearnerAdmissionPending},
		{"root", "internal", "required_root_unavailable", fmt.Errorf("%s: %w", secret, errors.New("managed Graph required recovery root is unavailable"))},
		{"prefix", "internal", "source_prefix_mismatch", fmt.Errorf("%s: %w", secret, errors.New("verified archive prefix mismatch"))},
		{"identity", "internal", "identity_mismatch", fmt.Errorf("%s: %w", secret, errors.New("internal application WAL manifest identity changed"))},
		{"history", "internal", "unresolved_certified_history", fmt.Errorf("%s: internal application has unresolved certified history: target=0 recorded=1 certified=0 applied=0", secret)},
		{"pending", "internal", "pending_graph_state", errors.Join(errors.New(secret), errors.New("previous Graph publication is pending or ambiguous"))},
		{"poison", "internal", "graph_commit_unknown", fmt.Errorf("%s: graph commit outcome is unknown; reopen before use", secret)},
		{"budget", "internal", "budget_refusal", fmt.Errorf("%s: read-only archive suffix exceeds byte limit", secret)},
		{"context-precedence", "canceled", "canceled", errors.Join(context.Canceled, errors.New("managed Graph required recovery root is unavailable"))},
	} {
		for _, hostOpen := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/host=%t", tc.name, hostOpen), func(t *testing.T) {
				var output bytes.Buffer
				old := log.Writer()
				log.SetOutput(&output)
				defer log.SetOutput(old)
				raw := managedOpenError(hostOpen, tc.err)
				var envelope struct {
					Error *ffiError `json:"error"`
				}
				if err := json.Unmarshal(raw, &envelope); err != nil || envelope.Error == nil {
					t.Fatalf("invalid error envelope: %v", err)
				}
				stage, message := "load_config", "managed startup inputs rejected"
				if hostOpen {
					stage, message = "host_open", "managed startup rejected"
				}
				if envelope.Error.Code != tc.code || envelope.Error.Code != errorCode(tc.err) || envelope.Error.Message != message || envelope.Error.MutationContext != nil {
					t.Fatalf("changed public error contract: %+v", envelope.Error)
				}
				line := output.String()
				if strings.Count(line, "managed startup unavailable") != 1 || !strings.Contains(line, "stage="+stage+" code="+tc.code+" cause="+tc.cause) || strings.Contains(line, secret) || bytes.Contains(raw, []byte(secret)) {
					t.Fatal("diagnostic labels/redaction changed")
				}
			})
		}
	}
}
