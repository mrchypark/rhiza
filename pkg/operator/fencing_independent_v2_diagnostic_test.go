package operator

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

type fenceStageDiagnosticRoundTripper func(*http.Request) (*http.Response, error)

func (f fenceStageDiagnosticRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type fenceStageDiagnosticHTTPError struct{ status int }

func (e fenceStageDiagnosticHTTPError) Error() string       { return "private provider response" }
func (e fenceStageDiagnosticHTTPError) HTTPStatusCode() int { return e.status }

func captureFenceStageDiagnostic(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	var output bytes.Buffer
	previousWriter, previousFlags := log.Writer(), log.Flags()
	log.SetOutput(&output)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
	})
	err := fn()
	return output.String(), err
}

func requireFenceStageDiagnostic(t *testing.T, output, stage string) {
	t.Helper()
	if !strings.Contains(output, "fencing stage blocked: stage="+stage+" class=") {
		t.Fatalf("missing %s stage diagnostic: %q", stage, output)
	}
}

func TestFenceStageDiagnosticsDistinguishRecoveryBoundaries(t *testing.T) {
	t.Run("root close", func(t *testing.T) {
		e, _, record, runtime := newIndependentV2Fixture(t)
		hash, err := requestHash(&record.Spec)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error {
			return l.Close("other-operation", hash)
		}); err != nil {
			t.Fatal(err)
		}
		output, err := captureFenceStageDiagnostic(t, func() error {
			return e.reconcileFence(t.Context(), v2FenceEndpoint(), record)
		})
		if err == nil {
			t.Fatal("conflicting root close unexpectedly succeeded")
		}
		requireFenceStageDiagnostic(t, output, "root-close")
	})

	t.Run("inventory", func(t *testing.T) {
		e, _, record, _ := newIndependentV2Fixture(t)
		client := *e.Kube.Client
		base := client.Transport
		client.Transport = fenceStageDiagnosticRoundTripper(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodGet && request.URL.Path == e.controller().corePath("pods", "") {
				return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{}")), Request: request}, nil
			}
			return base.RoundTrip(request)
		})
		e.Kube.Client = &client
		output, err := captureFenceStageDiagnostic(t, func() error {
			return e.reconcileFence(t.Context(), v2FenceEndpoint(), record)
		})
		if err == nil {
			t.Fatal("failed inventory read unexpectedly succeeded")
		}
		requireFenceStageDiagnostic(t, output, "inventory")
	})

	t.Run("producer settlement", func(t *testing.T) {
		e, _, record, runtime := newIndependentV2Fixture(t)
		if _, err := e.changeMutationProducer(t.Context(), runtime, func(p *fencing.MutationProducer) error {
			run, err := e.enrolledMutationRun(t.Context(), runtime)
			if err != nil {
				return err
			}
			return p.Admit(2, completeStoreIntent(fencing.MutationIntent{
				OperationID: fencing.MutationRequestID(run.MutationProducerUID, 2), Kind: "delete", Key: "pending",
			}))
		}); err != nil {
			t.Fatal(err)
		}
		output, err := captureFenceStageDiagnostic(t, func() error {
			return e.reconcileFence(t.Context(), v2FenceEndpoint(), record)
		})
		if !errors.Is(err, ErrFencePending) {
			t.Fatalf("unsettled producer error = %v", err)
		}
		requireFenceStageDiagnostic(t, output, "producer-settlement")
	})

	t.Run("runtime collection", func(t *testing.T) {
		e, _, record, _ := newIndependentV2Fixture(t)
		e.CollectRuntimeEvidence = func(context.Context, fencing.RuntimeBinding, fencing.StopRequest) ([]byte, error) {
			return nil, errors.New("private runtime endpoint token detail")
		}
		output, err := captureFenceStageDiagnostic(t, func() error {
			return e.reconcileFence(t.Context(), v2FenceEndpoint(), record)
		})
		if err == nil || !strings.Contains(err.Error(), "private runtime endpoint") {
			t.Fatalf("runtime collection cause was not retained: %v", err)
		}
		requireFenceStageDiagnostic(t, output, "runtime-collection")
		if strings.Contains(output, "private runtime endpoint") {
			t.Fatalf("raw runtime error leaked to stage diagnostic: %q", output)
		}
	})

	t.Run("terminal retention", func(t *testing.T) {
		e, _, record, _ := newIndependentV2Fixture(t)
		e.VerifyRuntimeEvidence = func(context.Context, fencing.RuntimeBinding, fencing.StopRequest, []byte) error {
			return errors.New("private receipt verification detail")
		}
		output, err := captureFenceStageDiagnostic(t, func() error {
			return e.reconcileFence(t.Context(), v2FenceEndpoint(), record)
		})
		if err == nil {
			t.Fatal("invalid terminal receipt unexpectedly succeeded")
		}
		requireFenceStageDiagnostic(t, output, "terminal-retention")
		if strings.Contains(output, "private receipt verification") {
			t.Fatalf("raw verification error leaked to stage diagnostic: %q", output)
		}
	})

	t.Run("terminal verification", func(t *testing.T) {
		e, _, record, _ := newIndependentV2Fixture(t)
		e.VerifyRuntimeEvidence = nil
		output, err := captureFenceStageDiagnostic(t, func() error {
			return e.reconcileFence(t.Context(), v2FenceEndpoint(), record)
		})
		if !errors.Is(err, ErrFencePending) {
			t.Fatalf("missing runtime verifier error = %v", err)
		}
		requireFenceStageDiagnostic(t, output, "cohort-validation")
	})

	t.Run("proof status write", func(t *testing.T) {
		e, _, record, _ := newIndependentV2Fixture(t)
		client := *e.Kube.Client
		base := client.Transport
		client.Transport = fenceStageDiagnosticRoundTripper(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodPut && strings.HasSuffix(request.URL.Path, "/status") {
				return &http.Response{StatusCode: http.StatusInternalServerError, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{}")), Request: request}, nil
			}
			return base.RoundTrip(request)
		})
		e.Kube.Client = &client
		output, err := captureFenceStageDiagnostic(t, func() error {
			return e.reconcileFence(t.Context(), v2FenceEndpoint(), record)
		})
		if err == nil {
			t.Fatal("failed proof status write unexpectedly succeeded")
		}
		requireFenceStageDiagnostic(t, output, "proof-status-write")
	})
}

func TestFenceStageDiagnosticAllowsIndependentFenceBoundaries(t *testing.T) {
	stages := []string{"cohort-validation", "cohort-revalidation", "terminal-receipt-read", "root-reread", "inventory-revalidation"}
	output, _ := captureFenceStageDiagnostic(t, func() error {
		for _, stage := range stages {
			logFenceStageDiagnostic(stage, errors.New("private terminal detail"))
		}
		return nil
	})
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != len(stages) {
		t.Fatalf("stage diagnostic count=%d, want %d: %q", len(lines), len(stages), output)
	}
	for i, stage := range stages {
		if !strings.HasSuffix(lines[i], "fencing stage blocked: stage="+stage+" class=error") {
			t.Errorf("stage %q mapped to unexpected diagnostic %q", stage, lines[i])
		}
	}
	if strings.Contains(output, "private terminal detail") {
		t.Fatalf("stage diagnostic leaked error text: %q", output)
	}
}

func TestFenceStageDiagnosticUsesOnlyFixedFields(t *testing.T) {
	output, _ := captureFenceStageDiagnostic(t, func() error {
		logFenceStageDiagnostic("untrusted-stage", errors.New("private endpoint and token"))
		return nil
	})
	if !strings.Contains(output, "fencing stage blocked: stage=other class=error") || strings.Contains(output, "private endpoint") || strings.Contains(output, "token") {
		t.Fatalf("diagnostic did not redact free-form values: %q", output)
	}
}

func TestFenceStageDiagnosticMapsErrorsToFixedClasses(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		class string
	}{
		{name: "pending", err: ErrFencePending, class: "pending"},
		{name: "fencing pending", err: fencing.ErrPending, class: "pending"},
		{name: "deadline", err: context.DeadlineExceeded, class: "timeout"},
		{name: "canceled", err: context.Canceled, class: "canceled"},
		{name: "root conflict", err: errAuthorityStateConflict, class: "conflict"},
		{name: "closed authority", err: fencing.ErrClosed, class: "conflict"},
		{name: "unavailable", err: fencing.ErrUnavailable, class: "unavailable"},
		{name: "permission", err: fenceStageDiagnosticHTTPError{status: http.StatusForbidden}, class: "permission"},
		{name: "provider unavailable", err: fenceStageDiagnosticHTTPError{status: http.StatusServiceUnavailable}, class: "unavailable"},
		{name: "unknown status", err: fenceStageDiagnosticHTTPError{status: http.StatusBadRequest}, class: "error"},
	}
	output, _ := captureFenceStageDiagnostic(t, func() error {
		for _, tt := range tests {
			logFenceStageDiagnostic("bootstrap-revalidation", tt.err)
		}
		return nil
	})
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != len(tests) {
		t.Fatalf("diagnostic count=%d, want %d: %q", len(lines), len(tests), output)
	}
	for i, tt := range tests {
		want := "fencing stage blocked: stage=bootstrap-revalidation class=" + tt.class
		if !strings.HasSuffix(lines[i], want) {
			t.Errorf("%s diagnostic=%q, want suffix %q", tt.name, lines[i], want)
		}
	}
	if strings.Contains(output, "private provider response") {
		t.Fatalf("raw error escaped fixed class mapping: %q", output)
	}
}
