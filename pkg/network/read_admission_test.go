package network

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type blockingResponseWriter struct {
	header  http.Header
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingResponseWriter) Header() http.Header { return w.header }
func (w *blockingResponseWriter) WriteHeader(int)     {}
func (w *blockingResponseWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}

func TestReadAdmissionLimitsValidateAndReserveLongPollCapacity(t *testing.T) {
	server := NewServer(nil, nil, "cluster", false, nil)
	defer server.Close()
	for _, limits := range []ReadAdmissionLimits{{}, {MaxConcurrent: 1, MaxLongPoll: 2}} {
		if err := server.SetReadAdmissionLimits(limits); err == nil {
			t.Fatalf("limits %+v unexpectedly accepted", limits)
		}
	}
	if err := server.SetReadAdmissionLimits(ReadAdmissionLimits{MaxConcurrent: 2, MaxLongPoll: 1}); err != nil {
		t.Fatal(err)
	}
	longRelease, err := server.acquireRead(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	shortRelease, err := server.acquireRead(context.Background(), false)
	if err != nil {
		t.Fatalf("normal read was blocked by one long poll: %v", err)
	}
	if _, err := server.acquireRead(context.Background(), true); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("second long poll error=%v, want overloaded", err)
	}
	shortRelease()
	longRelease()
}

func TestReadAdmissionRejectsSaturationAndHonorsCancellation(t *testing.T) {
	server := NewServer(nil, nil, "cluster", false, nil)
	defer server.Close()
	if err := server.SetReadAdmissionLimits(ReadAdmissionLimits{MaxConcurrent: 1}); err != nil {
		t.Fatal(err)
	}
	release, err := server.acquireRead(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.acquireRead(context.Background(), false); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("saturated admission error=%v, want overloaded", err)
	}
	release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := server.acquireRead(ctx, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled admission error=%v, want canceled", err)
	}
}

func TestHTTPReadAdmissionHoldsSlotUntilResponseWriteCompletes(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}}
	core := mustCore(t, "n1", members, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil)
	defer server.Close()
	if err := server.SetReadAdmissionLimits(ReadAdmissionLimits{MaxConcurrent: 1}); err != nil {
		t.Fatal(err)
	}

	writer := &blockingResponseWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		server.ServeHTTP(writer, httptest.NewRequest(http.MethodPost, "/sql/query", strings.NewReader(`{"sql":"SELECT 1"}`)))
		close(done)
	}()
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("query did not reach response write")
	}
	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/sql/query", strings.NewReader(`{"sql":"SELECT 1"}`)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("saturated HTTP status=%d body=%s", response.Code, response.Body.String())
	}
	close(writer.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("response writer did not release read admission")
	}
	if _, err := server.Query(context.Background(), QueryRequest{SQL: "SELECT 1"}); err != nil {
		t.Fatalf("read after response completion: %v", err)
	}
}
