package node

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
)

func TestReadVoterRegistrationMissingMetricsDoNotHideOtherFailures(t *testing.T) {
	tests := []struct {
		name              string
		status            int
		code              string
		stopServer        bool
		wantMissing       bool
		wantUnexpected4xx uint64
		wantHTTP5xx       uint64
		wantTransport     uint64
	}{
		{name: "missing is expected", status: http.StatusNotFound, code: "NoSuchKey", wantMissing: true},
		{name: "forbidden remains failure", status: http.StatusForbidden, code: "AccessDenied", wantUnexpected4xx: 1},
		{name: "server error remains failure", status: http.StatusInternalServerError, code: "InternalError", wantHTTP5xx: 1},
		{name: "transport error remains failure", stopServer: true, wantTransport: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Uint64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(test.status)
				_, _ = fmt.Fprintf(w, `<Error><Code>%s</Code><Message>fixture response</Message></Error>`, test.code)
			}))
			endpoint := strings.TrimPrefix(server.URL, "http://")
			if test.stopServer {
				server.Close()
			} else {
				defer server.Close()
			}

			bucket, err := objmetrics.NewBucket(objmetrics.Config{
				Provider: objmetrics.ProviderS3, Endpoint: endpoint, Bucket: "registration-test",
				Region: "us-east-1", Insecure: true, AccessKey: "test-access", SecretKey: "test-secret",
				MaxRetries: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer bucket.Close()

			data, err := readVoterRegistration(context.Background(), bucket, "cluster/voters/missing.json")
			if test.wantMissing {
				if err != nil || data != nil {
					t.Fatalf("missing registration data=%q err=%v; want nil, nil", data, err)
				}
			} else if err == nil {
				t.Fatal("read error was masked")
			}

			stats := bucket.Stats()
			if stats.Gets != 1 || stats.HTTPRequests != 1 {
				t.Fatalf("logical GET/HTTP attempts=%d/%d; want 1/1", stats.Gets, stats.HTTPRequests)
			}
			if stats.Unexpected4xx != test.wantUnexpected4xx || stats.HTTP5xx != test.wantHTTP5xx || stats.TransportFailures != test.wantTransport {
				t.Fatalf("failure classification=%+v; want unexpected4xx=%d HTTP5xx=%d transport=%d", stats, test.wantUnexpected4xx, test.wantHTTP5xx, test.wantTransport)
			}
			wantHTTPFailures := test.wantUnexpected4xx + test.wantHTTP5xx + test.wantTransport
			if stats.HTTPFailures != wantHTTPFailures {
				t.Fatalf("HTTP failures=%d; want %d", stats.HTTPFailures, wantHTTPFailures)
			}
			if test.stopServer {
				if requests.Load() != 0 {
					t.Fatalf("closed server received %d requests", requests.Load())
				}
			} else if requests.Load() != 1 {
				t.Fatalf("fixture server received %d requests; want 1", requests.Load())
			}
		})
	}
}
