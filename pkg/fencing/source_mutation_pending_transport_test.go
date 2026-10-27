package fencing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func pendingResponse(status int, headers http.Header, request *http.Request) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     headers.Clone(),
		Body:       io.NopCloser(strings.NewReader("")),
		Request:    request,
	}
}

func pendingTestTuple(receipt BindReceipt, step string, sequence uint64) MutationIntent {
	intent := sourceRetryIntent(step)
	intent.StoreScopeHash = receipt.Producer.StoreScopeHash
	intent.OperationID = MutationRequestID(receipt.Producer.ProducerUID, sequence)
	return intent
}

func directPendingTestClient(t *testing.T, response func(*http.Request) *http.Response) *AuthorityClient {
	t.Helper()
	receipt := logicalRetryReceiptForTest()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("authority-token"), 0600); err != nil {
		t.Fatal(err)
	}
	client := logicalAuthorityForRetryTest(receipt)
	client.URL = "https://authority.invalid/v1/supervisor"
	client.TokenFile = tokenFile
	client.Client = &http.Client{Transport: authorityClientRoundTrip(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("Authorization") != "Bearer authority-token" {
			return nil, fmt.Errorf("authority token not sent")
		}
		return response(request), nil
	})}
	return client
}

func sessionPendingTestClient(response func(*http.Request) *http.Response) *Session {
	receipt := logicalRetryReceiptForTest()
	return &Session{
		client: &http.Client{Transport: authorityClientRoundTrip(func(request *http.Request) (*http.Response, error) {
			if request.Header.Get("Authorization") != "Bearer supervisor-ticket" {
				return nil, fmt.Errorf("supervisor ticket not sent")
			}
			return response(request), nil
		})},
		ticket:   "supervisor-ticket",
		identity: receipt.Identity,
		binding:  &receipt,
	}
}

func pendingMarkerHeader(values ...string) http.Header {
	header := make(http.Header)
	for _, value := range values {
		header.Add(MutationPendingReasonHeader, value)
	}
	return header
}

func TestAuthorityAndSessionPreserveOnlyExactMutationPendingReason(t *testing.T) {
	receipt := logicalRetryReceiptForTest()
	for _, operation := range []string{"admit", "execute"} {
		for _, tc := range []struct {
			name      string
			header    http.Header
			wantTyped bool
		}{
			{name: "exact reason", header: pendingMarkerHeader(MutationPreDispatchContentionReason), wantTyped: true},
			{name: "missing reason"},
			{name: "unknown reason", header: pendingMarkerHeader("future-reason")},
			{name: "malformed reason", header: pendingMarkerHeader("foreign-key-pre-dispatch-v1,other")},
			{name: "duplicate reason", header: pendingMarkerHeader(MutationPreDispatchContentionReason, MutationPreDispatchContentionReason)},
		} {
			t.Run(operation+"/direct/"+tc.name, func(t *testing.T) {
				client := directPendingTestClient(t, func(request *http.Request) *http.Response {
					wantPath := "/v1/source-mutations/" + operation
					if request.URL.Path != wantPath {
						t.Errorf("authority path = %q, want %q", request.URL.Path, wantPath)
					}
					return pendingResponse(http.StatusAccepted, tc.header, request)
				})
				intent := pendingTestTuple(receipt, "http-"+operation, 1)
				var err error
				if operation == "admit" {
					err = client.Admit(context.Background(), 1, intent)
				} else {
					err = client.Execute(context.Background(), 1, intent)
				}
				assertPendingClassification(t, err, tc.wantTyped)
			})

			t.Run(operation+"/session/"+tc.name, func(t *testing.T) {
				session := sessionPendingTestClient(func(request *http.Request) *http.Response {
					wantPath := "/v1/source-mutations/" + operation
					if request.URL.Path != wantPath {
						t.Errorf("session path = %q, want %q", request.URL.Path, wantPath)
					}
					return pendingResponse(http.StatusAccepted, tc.header, request)
				})
				intent := pendingTestTuple(receipt, "http-"+operation, 1)
				var err error
				if operation == "admit" {
					err = session.Admit(context.Background(), 1, intent)
				} else {
					err = session.Execute(context.Background(), 1, intent)
				}
				assertPendingClassification(t, err, tc.wantTyped)
			})
		}
	}
}

func TestOtherSourceEndpointsIgnoreMutationPendingReason(t *testing.T) {
	receipt := logicalRetryReceiptForTest()
	header := pendingMarkerHeader(MutationPreDispatchContentionReason)
	client := directPendingTestClient(t, func(request *http.Request) *http.Response {
		if request.URL.Path != "/v1/source-mutations/genesis-status" {
			t.Errorf("authority path = %q, want genesis-status", request.URL.Path)
		}
		return pendingResponse(http.StatusAccepted, header, request)
	})
	_, err := client.ArchiveGenesisStatus(context.Background(), "archive/head.bin", 1)
	assertPendingClassification(t, err, false)

	session := sessionPendingTestClient(func(request *http.Request) *http.Response {
		if request.URL.Path != "/v1/source-mutations/lookup" {
			t.Errorf("session path = %q, want lookup", request.URL.Path)
		}
		return pendingResponse(http.StatusAccepted, header, request)
	})
	intent := pendingTestTuple(receipt, "lookup", 1)
	_, err = session.Lookup(context.Background(), 1, intent)
	assertPendingClassification(t, err, false)
}

func TestMutationPendingReasonRelayWriterUsesClosedSubtype(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		wantValue string
	}{
		{name: "typed", err: ErrMutationPreDispatchContention, wantValue: MutationPreDispatchContentionReason},
		{name: "wrapped typed", err: fmt.Errorf("executor: %w", ErrMutationPreDispatchContention), wantValue: MutationPreDispatchContentionReason},
		{name: "generic pending", err: ErrPending},
		{name: "wrapped generic pending", err: fmt.Errorf("authority: %w", ErrPending)},
		{name: "joined typed and secondary error", err: errors.Join(ErrMutationPreDispatchContention, errors.New("unrelated"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writer := httptest.NewRecorder()
			writeSourceRelayError(writer, tc.err)
			if writer.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202", writer.Code)
			}
			values := writer.Header().Values(MutationPendingReasonHeader)
			if tc.wantValue == "" {
				if len(values) != 0 {
					t.Fatalf("generic pending leaked subtype header values %q", values)
				}
				return
			}
			if len(values) != 1 || values[0] != tc.wantValue {
				t.Fatalf("typed pending headers = %q, want exactly %q", values, tc.wantValue)
			}
		})
	}
}

func TestMutationPendingReasonLeavesOtherHTTPStatusesUnchanged(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusNoContent, http.StatusConflict, http.StatusPreconditionFailed, http.StatusServiceUnavailable} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			header := pendingMarkerHeader(MutationPreDispatchContentionReason)
			client := directPendingTestClient(t, func(request *http.Request) *http.Response {
				return pendingResponse(status, header, request)
			})
			intent := pendingTestTuple(logicalRetryReceiptForTest(), "other-status", 1)
			err := client.Execute(context.Background(), 1, intent)
			if errors.Is(err, ErrMutationPreDispatchContention) {
				t.Fatalf("HTTP %d was reclassified as contention: %v", status, err)
			}
			switch status {
			case http.StatusOK, http.StatusNoContent:
				if err != nil {
					t.Fatalf("HTTP %d error = %v", status, err)
				}
			case http.StatusPreconditionFailed:
				if !errors.Is(err, ErrMutationConditionFailed) {
					t.Fatalf("HTTP 412 error = %v", err)
				}
			case http.StatusServiceUnavailable:
				if !errors.Is(err, ErrUnavailable) {
					t.Fatalf("HTTP 503 error = %v", err)
				}
			default:
				if err == nil || errors.Is(err, ErrPending) {
					t.Fatalf("HTTP %d error = %v, want permanent source error", status, err)
				}
			}
		})
	}
}

func assertPendingClassification(t *testing.T, err error, wantTyped bool) {
	t.Helper()
	if wantTyped {
		if !errors.Is(err, ErrMutationPreDispatchContention) || !errors.Is(err, ErrPending) {
			t.Fatalf("error = %v, want typed pending matching both sentinels", err)
		}
		return
	}
	if err != ErrPending || errors.Is(err, ErrMutationPreDispatchContention) {
		t.Fatalf("error = %v, want generic ErrPending only", err)
	}
}
