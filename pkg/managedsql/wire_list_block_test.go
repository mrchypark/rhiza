package managedsql

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type blockedListResponse struct {
	header  http.Header
	entered chan struct{}
	release chan struct{}
	body    string
}

func (w *blockedListResponse) Header() http.Header { return w.header }
func (w *blockedListResponse) WriteHeader(int)     {}
func (w *blockedListResponse) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	w.body = string(p)
	return len(p), nil
}

func TestSQLWireBlockedListDoesNotBlockPut(t *testing.T) {
	s, _ := sqlWireStore(t)
	list := &blockedListResponse{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{})}
	listDone := make(chan struct{})
	go func() {
		defer close(listDone)
		s.Config.Handler.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/bucket/?list-type=2&prefix=objects/", nil))
	}()
	defer func() {
		close(list.release)
		<-listDone
		if strings.Contains(list.body, "objects/item") {
			t.Error("list response included an object added after the snapshot")
		}
	}()
	select {
	case <-list.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("list response did not start")
	}
	putDone := make(chan int, 1)
	go func() {
		response := httptest.NewRecorder()
		s.Config.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/bucket/objects/item", strings.NewReader("value")))
		putDone <- response.Code
	}()
	select {
	case code := <-putDone:
		if code != http.StatusOK {
			t.Fatalf("PUT status: %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked list response prevented PUT")
	}
}
