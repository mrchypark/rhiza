package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func captureManagedDiagnostic(t *testing.T) *bytes.Buffer {
	t.Helper()
	writer, flags := log.Writer(), log.Flags()
	var output bytes.Buffer
	log.SetOutput(&output)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(writer); log.SetFlags(flags) })
	return &output
}

func TestManagedOperationDiagnosticSanitizedAndResponseUnchanged(t *testing.T) {
	output := captureManagedDiagnostic(t)
	secret := "PRIVATE_SQL_PROFILE_CREDENTIAL_REQUEST_ID\nINJECTED_LOG_LINE"
	pre := &managedsql.SQLNotSubmittedError{Stage: managedsql.SQLStagePending, RecoveryRequired: true, Cause: errors.New(secret)}
	for _, tc := range []struct {
		name, operation, want string
		err                   error
	}{
		{"pending", "execute", "code=sql_recovery_required stage=pending recovery_required=true", pre},
		{"pre_admission", "execute", "code=sql_not_submitted stage=admission recovery_required=false", &managedsql.SQLNotSubmittedError{Stage: managedsql.SQLStageAdmission, Cause: errors.New(secret)}},
		{"unknown_stage", "execute", "code=sql_not_submitted stage=unknown recovery_required=false", &managedsql.SQLNotSubmittedError{Stage: managedsql.SQLSubmissionStage(secret), Cause: errors.New(secret)}},
		{"commit_unknown_precedence", "execute", "code=commit_unknown stage=unknown recovery_required=true", errors.Join(pre, &network.CommitUnknownError{RequestID: secret, Slot: 42, Cause: context.Canceled})},
		{"timeout", "query", "code=timeout stage=unknown recovery_required=false", context.DeadlineExceeded},
		{"cancelled", "recover-pending", "code=canceled stage=unknown recovery_required=false", context.Canceled},
		{"invalid", "query", "code=invalid_request stage=unknown recovery_required=false", network.ErrInvalidRequest},
		{"not_ready", "execute", "code=not_ready stage=unknown recovery_required=false", network.ErrNotReady},
		{"quorum", "query", "code=quorum_unavailable stage=unknown recovery_required=false", quepaxa.ErrQuorumUnavailable},
		{"conflict", "execute", "code=request_conflict stage=unknown recovery_required=false", network.ErrRequestConflict},
		{"durability", "execute", "code=durability_unavailable stage=unknown recovery_required=false", network.ErrDurabilityUnavailable},
		{"overload", "execute", "code=overloaded stage=unknown recovery_required=false", network.ErrOverloaded},
		{"unknown", secret, "operation=unknown code=internal stage=unknown recovery_required=false", errors.New(secret)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output.Reset()
			w := httptest.NewRecorder()
			managedOperationUnavailable(w, tc.operation, tc.err)
			if w.Code != http.StatusServiceUnavailable || w.Body.String() != "operation unavailable\n" {
				t.Fatalf("HTTP contract changed: %d %q", w.Code, w.Body.String())
			}
			line := output.String()
			if !strings.Contains(line, tc.want) || strings.Contains(line, "PRIVATE_") || strings.Contains(line, "INJECTED_LOG_LINE") || strings.Count(line, "\n") != 1 || len(line) > 180 {
				t.Fatalf("unexpected diagnostic: %q", line)
			}
		})
	}
}

func TestManagedHandlerOperationDiagnosticsPreserveLocalReads(t *testing.T) {
	_, _, server, _, _ := managedHTTPQuorum(t)
	output := captureManagedDiagnostic(t)
	for _, tc := range []struct {
		route, want string
		body        any
	}{
		{"/recover-pending", "operation=recover-pending code=commit_unknown", struct{}{}},
		{"/query", "operation=query code=invalid_request", managedsql.QueryRequest{SQL: "SELECT 'PRIVATE_SQL'", Consistency: "invalid"}},
	} {
		output.Reset()
		code, body := managedHTTPPost(t, server, tc.route, tc.body)
		if code != http.StatusServiceUnavailable || string(body) != "operation unavailable\n" || !strings.Contains(output.String(), tc.want) || strings.Contains(output.String(), "private-") || strings.Contains(output.String(), "PRIVATE_SQL") {
			t.Fatalf("route %s: HTTP %d %q log=%q", tc.route, code, body, output.String())
		}
	}
	if code, body := managedHTTPPost(t, server, "/query", managedsql.QueryRequest{SQL: "SELECT 1", Consistency: "local"}); code != http.StatusOK || !bytes.Contains(body, []byte(`"rows":[[1]]`)) {
		t.Fatalf("diagnostics changed safe local reads: %d %s", code, body)
	}
}
