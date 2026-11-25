package managedsql

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type blockedWireResponse struct {
	header  http.Header
	entered chan struct{}
	release chan struct{}
	body    string
}

func (w *blockedWireResponse) Header() http.Header { return w.header }
func (w *blockedWireResponse) WriteHeader(int)     {}
func (w *blockedWireResponse) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	w.body = string(p)
	return len(p), nil
}

func TestSQLWireStoreBlockedGetDoesNotBlockPut(t *testing.T) {
	s, _ := sqlWireStore(t)
	seed := httptest.NewRecorder()
	s.Config.Handler.ServeHTTP(seed, httptest.NewRequest(http.MethodPut, "/bucket/first", strings.NewReader("value")))
	if seed.Code != http.StatusOK {
		t.Fatalf("seed PUT: status %d", seed.Code)
	}
	blocked := &blockedWireResponse{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{})}
	getDone := make(chan struct{})
	go func() {
		defer close(getDone)
		s.Config.Handler.ServeHTTP(blocked, httptest.NewRequest(http.MethodGet, "/bucket/first", nil))
	}()
	defer func() {
		close(blocked.release)
		<-getDone
		if blocked.body != "value" {
			t.Errorf("GET did not retain its snapshot: %q", blocked.body)
		}
	}()
	select {
	case <-blocked.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("GET did not enter response write")
	}
	putDone := make(chan int, 1)
	go func() {
		response := httptest.NewRecorder()
		s.Config.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/bucket/first", strings.NewReader("other")))
		putDone <- response.Code
	}()
	select {
	case code := <-putDone:
		if code != http.StatusOK {
			t.Fatalf("independent PUT: status %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked GET prevented independent PUT")
	}
}
