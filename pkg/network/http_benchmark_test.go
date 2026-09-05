package network

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func BenchmarkHTTPQueryLoopback(b *testing.B) {
	core := mustCore(b, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	material, err := materializer.Open(b.TempDir()+"/db.sqlite", 4)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = material.Close() })
	server := NewServer(core, material, "cluster", true, nil)
	b.Cleanup(server.Close)

	httpServer := httptest.NewServer(server)
	b.Cleanup(httpServer.Close)
	client := httpServer.Client()
	body := []byte(`{"sql":"SELECT 1","consistency":"local"}`)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, httpServer.URL+"/sql/query", bytes.NewReader(body))
			if err != nil {
				b.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			response, err := client.Do(request)
			if err != nil {
				b.Fatal(err)
			}
			_, readErr := io.Copy(io.Discard, response.Body)
			closeErr := response.Body.Close()
			if readErr != nil || closeErr != nil {
				b.Fatalf("read response: %v; close response: %v", readErr, closeErr)
			}
			if response.StatusCode != http.StatusOK {
				b.Fatalf("status=%d, want %d", response.StatusCode, http.StatusOK)
			}
		}
	})
}
