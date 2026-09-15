package operator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

type fakeK struct {
	mu   sync.Mutex
	cms  map[string]object
	puts int
	fail int
}

func newFakeK() *fakeK { return &fakeK{cms: map[string]object{}} }

func (f *fakeK) ServeHTTP(w2 http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w2.Header().Set("Content-Type", "application/json")
	parts := strings.Split(r.URL.Path, "/")
	key := "default/" + parts[len(parts)-1]
	switch r.Method {
	case http.MethodGet:
		cm, ok := f.cms[key]
		if !ok {
			http.Error(w2, `{"reason":"NotFound"}`, 404)
			return
		}
		json.NewEncoder(w2).Encode(cm)
	case http.MethodPut:
		if f.fail > 0 {
			f.fail--
			http.Error(w2, `{"reason":"Conflict"}`, 409)
			return
		}
		var cm object
		if json.NewDecoder(r.Body).Decode(&cm) != nil {
			http.Error(w2, "bad", 400)
			return
		}
		rv := str(nested(cm, "metadata", "resourceVersion"))
		if rv == "" {
			http.Error(w2, `{"reason":"rv"}`, 422)
			return
		}
		if ex, ok := f.cms[key]; ok {
			old := str(nested(ex, "metadata", "resourceVersion"))
			if old != "" && rv != old {
				http.Error(w2, `{"reason":"Conflict"}`, 409)
				return
			}
		}
		n, _ := strconv.Atoi(rv)
		asObject(nested(cm, "metadata"))["resourceVersion"] = strconv.Itoa(n + 1)
		f.cms[key] = cm
		f.puts++
		json.NewEncoder(w2).Encode(cm)
	default:
		http.Error(w2, "nope", 405)
	}
}

func aKube(t *testing.T, f *fakeK) *Kubernetes {
	t.Helper()
	srv := httptest.NewTLSServer(f)
	t.Cleanup(srv.Close)
	tok := filepath.Join(t.TempDir(), "token")
	os.WriteFile(tok, []byte("tok"), 0600)
	return &Kubernetes{BaseURL: srv.URL, Namespace: "default", TokenFile: tok, Client: srv.Client()}
}

func seed(f *fakeK, id string, rec recoveryanchor.Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, _ := json.Marshal(rec)
	f.cms["default/rhiza-anchor-"+id] = object{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": object{"name": "rhiza-anchor-" + id, "namespace": "default", "resourceVersion": "100"},
		"data":     object{anchorDataKey: string(d)},
	}
}

func TestReadOK(t *testing.T) {
	f := newFakeK()
	b := recoveryanchor.Binding{ClusterID: "tgt", StorageID: "s1"}
	seed(f, "a1", recoveryanchor.Record{Version: 1, Binding: b, Generation: 1})
	be := &KubernetesAnchorBackend{Kube: aKube(t, f)}
	rec, rv, err := be.Read(context.Background(), "a1")
	if err != nil {
		t.Fatal(err)
	}
	if rv != "100" {
		t.Fatalf("rv=%q", rv)
	}
	if rec.Binding != b || rec.Generation != 1 {
		t.Fatalf("rec=%+v", rec)
	}
}

func TestReadMissingData(t *testing.T) {
	f := newFakeK()
	f.mu.Lock()
	f.cms["default/rhiza-anchor-x"] = object{"apiVersion": "v1", "kind": "ConfigMap", "metadata": object{"name": "rhiza-anchor-x", "namespace": "default", "resourceVersion": "1"}, "data": object{}}
	f.mu.Unlock()
	be := &KubernetesAnchorBackend{Kube: aKube(t, f)}
	if _, _, err := be.Read(context.Background(), "x"); err == nil {
		t.Fatal("expected error")
	}
}

func TestReadMissingCM(t *testing.T) {
	f := newFakeK()
	be := &KubernetesAnchorBackend{Kube: aKube(t, f)}
	if _, _, err := be.Read(context.Background(), "x"); err == nil {
		t.Fatal("expected error")
	}
}

func TestCasEmptyRV(t *testing.T) {
	f := newFakeK()
	be := &KubernetesAnchorBackend{Kube: aKube(t, f)}
	if err := be.CAS(context.Background(), "a1", "", recoveryanchor.Record{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestCASStale(t *testing.T) {
	f := newFakeK()
	seed(f, "a1", recoveryanchor.Record{Version: 1})
	be := &KubernetesAnchorBackend{Kube: aKube(t, f)}
	if err := be.CAS(context.Background(), "a1", "99", recoveryanchor.Record{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestCASKeepsMeta(t *testing.T) {
	f := newFakeK()
	f.mu.Lock()
	f.cms["default/rhiza-anchor-a"] = object{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": object{"name": "rhiza-anchor-a", "namespace": "default", "resourceVersion": "100", "labels": object{"app": "r"}, "annotations": object{"k": "v"}},
		"data":     object{"other": "preserved", anchorDataKey: "{}"},
	}
	f.mu.Unlock()
	b := recoveryanchor.Binding{ClusterID: "tgt", StorageID: "s1"}
	be := &KubernetesAnchorBackend{Kube: aKube(t, f)}
	if err := be.CAS(context.Background(), "a", "100", recoveryanchor.Record{Version: 1, Binding: b, Generation: 2}); err != nil {
		t.Fatal(err)
	}
	if f.puts != 1 {
		t.Fatalf("puts=%d", f.puts)
	}
	f.mu.Lock()
	cm := f.cms["default/rhiza-anchor-a"]
	f.mu.Unlock()
	if str(nested(cm, "data", "other")) != "preserved" {
		t.Fatal("data lost")
	}
	if str(nested(cm, "metadata", "labels", "app")) != "r" {
		t.Fatal("labels lost")
	}
	if str(nested(cm, "metadata", "annotations", "k")) != "v" {
		t.Fatal("ann lost")
	}
	var s recoveryanchor.Record
	json.Unmarshal([]byte(str(nested(cm, "data", anchorDataKey))), &s)
	if s.Binding != b || s.Generation != 2 {
		t.Fatalf("stored=%+v", s)
	}
}

func TestCASConflict(t *testing.T) {
	f := newFakeK()
	seed(f, "a1", recoveryanchor.Record{Version: 1})
	f.fail = 1
	be := &KubernetesAnchorBackend{Kube: aKube(t, f)}
	if err := be.CAS(context.Background(), "a1", "100", recoveryanchor.Record{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestCASNoRecord(t *testing.T) {
	f := newFakeK()
	f.mu.Lock()
	f.cms["default/rhiza-anchor-a"] = object{"apiVersion": "v1", "kind": "ConfigMap", "metadata": object{"name": "rhiza-anchor-a", "namespace": "default", "resourceVersion": "3"}, "data": object{}}
	f.mu.Unlock()
	be := &KubernetesAnchorBackend{Kube: aKube(t, f)}
	err := be.CAS(context.Background(), "a", "3", recoveryanchor.Record{})
	if err == nil || !strings.Contains(err.Error(), "no existing anchor record") {
		t.Fatalf("err=%v", err)
	}
}

func TestNilKube(t *testing.T) {
	be := &KubernetesAnchorBackend{}
	if _, _, err := be.Read(context.Background(), "a"); err == nil {
		t.Fatal("expected error")
	}
	if err := be.CAS(context.Background(), "a", "1", recoveryanchor.Record{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestReadRejectsUnknown(t *testing.T) {
	f := newFakeK()
	f.mu.Lock()
	f.cms["default/rhiza-anchor-a"] = object{"apiVersion": "v1", "kind": "ConfigMap", "metadata": object{"name": "rhiza-anchor-a", "namespace": "default", "resourceVersion": "1"}, "data": object{anchorDataKey: `{"version":1,"bogus":true}`}}
	f.mu.Unlock()
	be := &KubernetesAnchorBackend{Kube: aKube(t, f)}
	if _, _, err := be.Read(context.Background(), "a"); err == nil || !strings.Contains(err.Error(), "corrupt record") {
		t.Fatalf("err=%v", err)
	}
}

func TestReadRejectsTrailingBytes(t *testing.T) {
	f := newFakeK()
	f.mu.Lock()
	f.cms["default/rhiza-anchor-a"] = object{"apiVersion": "v1", "kind": "ConfigMap", "metadata": object{"name": "rhiza-anchor-a", "namespace": "default", "resourceVersion": "1"}, "data": object{anchorDataKey: `{"version":1} ]`}}
	f.mu.Unlock()
	be := &KubernetesAnchorBackend{Kube: aKube(t, f)}
	if _, _, err := be.Read(context.Background(), "a"); err == nil || !strings.Contains(err.Error(), "trailing data") {
		t.Fatalf("expected trailing data error, got: %v", err)
	}
}
