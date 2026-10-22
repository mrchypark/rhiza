package operator

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func sourceStageHTTPFixture(t *testing.T) (*FencingExecutor, fencing.Identity, fencing.MutationIntent) {
	t.Helper()
	e, f, id, _, _ := mutationLookupFixture(t)
	pod := supervisedTestPod()
	asObject(pod["metadata"])["name"] = id.NodeID
	asObject(pod["metadata"])["uid"] = id.PodUID
	asObject(pod["metadata"])["ownerReferences"] = []any{object{"kind": "StatefulSet", "name": "rhiza", "uid": "sts", "controller": true}}
	asObject(list(nested(pod, "spec", "containers"))[0])["env"] = []any{object{"name": "RHIZA_CLUSTER_ID", "value": "source"}}
	sts := object{"metadata": object{"uid": "sts"}, "spec": object{"template": object{"spec": object{"containers": []any{object{"name": "rhiza", "env": []any{object{"name": "RHIZA_CLUSTER_ID", "value": "source"}}}}}}}}
	f.mu.Lock()
	f.authPod, f.authStatefulSet = pod, sts
	f.mu.Unlock()
	dir := filepath.Join(t.TempDir(), "spool")
	provisionStageStoreFixture(t, dir, strings.Repeat("4", 64))
	store, err := OpenSourceStageStore(dir, strings.Repeat("4", 64))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	e.SourceStages = store
	return e, id, stageStoreIntent()
}

func sourceStageRequest(t *testing.T, method, url string, id fencing.Identity, body io.Reader, token string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set(sourceStageIdentityHeader, base64.RawURLEncoding.EncodeToString(mustJSON(t, id)))
	if method == http.MethodPost && body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestSourceStageAuthenticatedHTTPReservesAndStreamsSealedBytes(t *testing.T) {
	e, id, draft := sourceStageHTTPFixture(t)
	server := httptest.NewTLSServer(e.Handler())
	t.Cleanup(server.Close)
	ticket := strings.Repeat("d", 64)
	reserve := sourceStageReserveRequest{Identity: id, Ticket: ticket, Draft: draft}
	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/source-stages/reserve", bytes.NewReader(mustJSON(t, reserve)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer test")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("reservation status %d", resp.StatusCode)
	}

	payload := []byte(`{"node":"bootstrap"}`)
	hash := sha256.Sum256(payload)
	digest := hex.EncodeToString(hash[:])
	url := fmt.Sprintf("%s/v1/source-stages/%s?digest=%s&size=%d", server.URL, ticket, digest, len(payload))
	req = sourceStageRequest(t, http.MethodPost, url, id, bytes.NewReader(payload), "test")
	resp, err = server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("stage seal status %d: %s", resp.StatusCode, data)
	}
	var descriptor sourceStageDescriptor
	if err := json.NewDecoder(resp.Body).Decode(&descriptor); err != nil {
		t.Fatal(err)
	}
	if descriptor.Ref != "payload-"+ticket || descriptor.Digest != digest || descriptor.Size != int64(len(payload)) {
		t.Fatalf("sealed descriptor differs: %+v", descriptor)
	}
	got, err := os.ReadFile(filepath.Join(e.SourceStages.dir, descriptor.Ref))
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("sealed bytes differ: %q %v", got, err)
	}
}

func TestSourceStageHTTPRejectsForeignAndChangedScope(t *testing.T) {
	e, id, draft := sourceStageHTTPFixture(t)
	server := httptest.NewTLSServer(e.Handler())
	t.Cleanup(server.Close)
	foreign := id
	foreign.PodUID = "foreign-pod"
	request := sourceStageReserveRequest{Identity: foreign, Ticket: strings.Repeat("e", 64), Draft: draft}
	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/source-stages/reserve", bytes.NewReader(mustJSON(t, request)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer test")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("foreign pod reserved source stage")
	}

	draft.StoreScopeHash = strings.Repeat("f", 64)
	request = sourceStageReserveRequest{Identity: id, Ticket: strings.Repeat("f", 64), Draft: draft}
	req, err = http.NewRequest(http.MethodPost, server.URL+"/v1/source-stages/reserve", bytes.NewReader(mustJSON(t, request)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer test")
	resp, err = server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("foreign store scope reserved source stage")
	}
}

func TestSourceStageEndpointDisabledWithoutExplicitStore(t *testing.T) {
	server := httptest.NewServer((&FencingExecutor{}).Handler())
	t.Cleanup(server.Close)
	resp, err := server.Client().Post(server.URL+"/v1/source-stages/reserve", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured stage endpoint returned %d", resp.StatusCode)
	}
}
