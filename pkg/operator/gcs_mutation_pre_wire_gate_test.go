package operator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGCSMutationPreWireGateReleasesSameGrantedRequestToProvider(t *testing.T) {
	f := newQualificationFixture()
	releasePath := filepath.Join(t.TempDir(), "release.json")
	f.config.PreWireGate = &GCSMutationPreWireGate{Nonce: strings.Repeat("a", 64), ReleaseFile: releasePath}
	receipts := make(chan GCSMutationAckLossReceipt, 8)
	fault, err := NewGCSMutationAckLossFault(f.config, func(receipt GCSMutationAckLossReceipt) {
		receipts <- receipt
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := fault.arm(context.Background(), f.runtime, f.run, f.run.MutationProducerUID, 7, "dispatch-delayed", f.intent, f.preparation, f.scope)
	request := preWireGateRequest(t, ctx, f)
	baseCalls := 0
	transport := fault.WrapTransport(qualificationRoundTripper(func(got *http.Request) (*http.Response, error) {
		baseCalls++
		if got != request || got.Context() != ctx {
			return nil, errors.New("gate did not release the original request and context")
		}
		return &http.Response{StatusCode: http.StatusPreconditionFailed, Header: make(http.Header), Body: http.NoBody}, nil
	}))
	result := make(chan struct {
		response *http.Response
		err      error
	}, 1)
	go func() {
		response, roundTripErr := transport.RoundTrip(request)
		result <- struct {
			response *http.Response
			err      error
		}{response, roundTripErr}
	}()
	entered := waitForGateReceipt(t, receipts, "entered")
	if baseCalls != 0 {
		t.Fatal("provider was called before explicit gate release")
	}
	release, err := json.Marshal(gcsMutationPreWireRelease{Version: 1, Nonce: entered.GateNonce, ProcessID: entered.ProcessID, TupleDigest: entered.TupleDigest})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(releasePath, release, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.err != nil || got.response == nil || got.response.StatusCode != http.StatusPreconditionFailed {
			t.Fatalf("released provider result = (%v, %v), want actual HTTP 412", got.response, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("selected request did not resume after its exact release")
	}
	released := waitForGateReceipt(t, receipts, "released")
	wireResult := waitForGateReceipt(t, receipts, "wire-result")
	if baseCalls != 1 || wireResult.HTTPStatus != http.StatusPreconditionFailed || entered.GateNonce != f.config.PreWireGate.Nonce || entered.ProcessID == "" || entered.TupleDigest != wireResult.TupleDigest || wireResult.WireRequests != 1 || released.ProcessID != entered.ProcessID {
		t.Fatalf("gate receipts do not prove one exact released wire request: calls=%d entered=%+v released=%+v result=%+v", baseCalls, entered, released, wireResult)
	}
}

func TestGCSMutationPreWireGateCancellationDoesNotReachProvider(t *testing.T) {
	f := newQualificationFixture()
	f.config.PreWireGate = &GCSMutationPreWireGate{Nonce: strings.Repeat("b", 64), ReleaseFile: filepath.Join(t.TempDir(), "release.json")}
	receipts := make(chan GCSMutationAckLossReceipt, 8)
	fault, err := NewGCSMutationAckLossFault(f.config, func(receipt GCSMutationAckLossReceipt) {
		receipts <- receipt
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	armedContext := fault.arm(ctx, f.runtime, f.run, f.run.MutationProducerUID, 7, "dispatch-cancelled", f.intent, f.preparation, f.scope)
	request := preWireGateRequest(t, armedContext, f)
	baseCalls := 0
	transport := fault.WrapTransport(qualificationRoundTripper(func(*http.Request) (*http.Response, error) {
		baseCalls++
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}, nil
	}))
	result := make(chan error, 1)
	go func() {
		_, roundTripErr := transport.RoundTrip(request)
		result <- roundTripErr
	}()
	waitForGateReceipt(t, receipts, "entered")
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled gate error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled gate did not stop waiting")
	}
	cancelled := waitForGateReceipt(t, receipts, "cancelled")
	if baseCalls != 0 || cancelled.HTTPStatus != 0 {
		t.Fatalf("cancelled gate reached provider or recorded a wire result: calls=%d receipt=%+v", baseCalls, cancelled)
	}
}

func TestGCSMutationPreWireGateRejectsInvalidReleaseBinding(t *testing.T) {
	f := newQualificationFixture()
	releasePath := filepath.Join(t.TempDir(), "release.json")
	f.config.PreWireGate = &GCSMutationPreWireGate{Nonce: strings.Repeat("c", 64), ReleaseFile: releasePath}
	receipts := make(chan GCSMutationAckLossReceipt, 8)
	fault, err := NewGCSMutationAckLossFault(f.config, func(receipt GCSMutationAckLossReceipt) {
		receipts <- receipt
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := fault.arm(context.Background(), f.runtime, f.run, f.run.MutationProducerUID, 7, "dispatch-wrong-release", f.intent, f.preparation, f.scope)
	request := preWireGateRequest(t, ctx, f)
	baseCalls := 0
	transport := fault.WrapTransport(qualificationRoundTripper(func(*http.Request) (*http.Response, error) {
		baseCalls++
		return &http.Response{StatusCode: http.StatusPreconditionFailed, Header: make(http.Header), Body: http.NoBody}, nil
	}))
	result := make(chan error, 1)
	go func() {
		_, roundTripErr := transport.RoundTrip(request)
		result <- roundTripErr
	}()
	entered := waitForGateReceipt(t, receipts, "entered")
	wrong := gcsMutationPreWireRelease{Version: 1, Nonce: strings.Repeat("d", 64), ProcessID: entered.ProcessID, TupleDigest: entered.TupleDigest}
	data, err := json.Marshal(wrong)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(releasePath, data, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "release rejected") {
			t.Fatalf("wrong-bound release error = %v, want fail-closed rejection", err)
		}
	case <-time.After(time.Second):
		t.Fatal("invalid release was not rejected")
	}
	waitForGateReceipt(t, receipts, "invalid-release")
	if baseCalls != 0 {
		t.Fatalf("invalid release reached provider %d times", baseCalls)
	}
}

func TestGCSMutationPreWireGateRejectsUnsafeReleaseFiles(t *testing.T) {
	tests := []struct {
		name  string
		write func(t *testing.T, path string, release []byte)
	}{
		{name: "permissions", write: func(t *testing.T, path string, release []byte) {
			t.Helper()
			if err := os.WriteFile(path, release, 0644); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink", write: func(t *testing.T, path string, release []byte) {
			t.Helper()
			target := filepath.Join(filepath.Dir(path), "target.json")
			if err := os.WriteFile(target, release, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "oversize", write: func(t *testing.T, path string, _ []byte) {
			t.Helper()
			if err := os.WriteFile(path, make([]byte, 1025), 0600); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newQualificationFixture()
			releasePath := filepath.Join(t.TempDir(), "release.json")
			f.config.PreWireGate = &GCSMutationPreWireGate{Nonce: strings.Repeat("e", 64), ReleaseFile: releasePath}
			receipts := make(chan GCSMutationAckLossReceipt, 8)
			fault, err := NewGCSMutationAckLossFault(f.config, func(receipt GCSMutationAckLossReceipt) { receipts <- receipt })
			if err != nil {
				t.Fatal(err)
			}
			ctx := fault.arm(context.Background(), f.runtime, f.run, f.run.MutationProducerUID, 7, "dispatch-unsafe-release", f.intent, f.preparation, f.scope)
			request := preWireGateRequest(t, ctx, f)
			baseCalls := 0
			transport := fault.WrapTransport(qualificationRoundTripper(func(*http.Request) (*http.Response, error) {
				baseCalls++
				return &http.Response{StatusCode: http.StatusPreconditionFailed, Header: make(http.Header), Body: http.NoBody}, nil
			}))
			result := make(chan error, 1)
			go func() {
				_, roundTripErr := transport.RoundTrip(request)
				result <- roundTripErr
			}()
			entered := waitForGateReceipt(t, receipts, "entered")
			release, marshalErr := json.Marshal(gcsMutationPreWireRelease{Version: 1, Nonce: entered.GateNonce, ProcessID: entered.ProcessID, TupleDigest: entered.TupleDigest})
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			test.write(t, releasePath, release)
			select {
			case err := <-result:
				if err == nil || !strings.Contains(err.Error(), "release rejected") {
					t.Fatalf("unsafe release error = %v, want fail-closed rejection", err)
				}
			case <-time.After(time.Second):
				t.Fatal("unsafe release file was not rejected")
			}
			waitForGateReceipt(t, receipts, "invalid-release")
			if baseCalls != 0 {
				t.Fatalf("unsafe release reached provider %d times", baseCalls)
			}
		})
	}
}

func preWireGateRequest(t *testing.T, ctx context.Context, f qualificationFixture) *http.Request {
	t.Helper()
	query := url.Values{"uploadType": {"multipart"}, "ifGenerationMatch": {"0"}, "name": {path.Join(f.scope.Prefix, f.intent.Key)}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://storage.googleapis.com/upload/storage/v1/b/"+f.scope.Bucket+"/o?"+query.Encode(), strings.NewReader("multipart payload"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "multipart/related; boundary=qualification")
	return request
}

func waitForGateReceipt(t *testing.T, receipts <-chan GCSMutationAckLossReceipt, phase string) GCSMutationAckLossReceipt {
	t.Helper()
	select {
	case receipt := <-receipts:
		if receipt.GatePhase != phase {
			t.Fatalf("gate receipt phase = %q, want %q", receipt.GatePhase, phase)
		}
		return receipt
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for gate receipt phase %q", phase)
	}
	return GCSMutationAckLossReceipt{}
}
