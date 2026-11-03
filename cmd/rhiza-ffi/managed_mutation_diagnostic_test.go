//go:build cgo

package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
)

func TestManagedMutationDiagnosticPreservesWireAndRedacts(t *testing.T) {
	const secret = "secret-SQL-https://private/path-raw-ID"
	for _, tc := range []struct {
		op    string
		err   error
		cause string
	}{
		{"graph_execute", &network.CommitUnknownError{Slot: 17, RequestID: secret, Cause: context.Canceled}, "canceled"},
		{"recover_pending_detailed", errors.Join(errors.New(secret), context.DeadlineExceeded), "deadline"},
		{"query", errors.New(secret), "unclassified"},
		{"graph_execute", &ffiError{Code: "commit_unknown", Message: secret, MutationContext: &ffiMutationContext{Kind: "graph", RequestID: []byte(secret), Slot: 17, RetryThroughSlot: 1041}}, "unclassified"},
		{secret, &ffiError{Code: "invalid_request", Message: secret, MutationContext: &ffiMutationContext{Kind: "graph", RequestID: []byte(secret), Slot: 0, RetryThroughSlot: 100}}, "unclassified"},
	} {
		var previous *ffiError
		if !errors.As(tc.err, &previous) {
			previous = managedError(tc.err)
		}
		want := marshalFFIError(previous)
		var output bytes.Buffer
		old := log.Writer()
		log.SetOutput(&output)
		got := managedCallError(tc.op, tc.err)
		log.SetOutput(old)
		if !bytes.Equal(want, marshalFFIError(got)) {
			t.Fatal("diagnostic changed existing public error wire")
		}
		line := output.String()
		if strings.Count(line, "managed operation unavailable") != 1 || strings.Contains(line, secret) || !strings.Contains(line, "cause="+tc.cause) {
			t.Fatal("unsafe or missing bounded diagnostic")
		}
		if tc.op == secret && !strings.Contains(line, "operation=unsupported code=invalid_request cause=unclassified context_known=true slot=0 retry=100") {
			t.Fatal("zero-slot known context was lost")
		}
		if got.MutationContext != nil && got.MutationContext.Slot == 17 && !strings.Contains(line, "context_known=true slot=17 retry=1041") {
			t.Fatal("captured positive slot was lost")
		}
	}
}
