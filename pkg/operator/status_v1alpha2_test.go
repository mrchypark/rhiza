package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type phase2Transport func(*http.Request) (*http.Response, error)

func (f phase2Transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func phase2Response(code int, body string) *http.Response {
	return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func phase2Store(t *testing.T, f phase2Transport) *V1Alpha2StatusStore {
	t.Helper()
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("token"), 0600); err != nil {
		t.Fatal(err)
	}
	return &V1Alpha2StatusStore{Kube: &Kubernetes{BaseURL: "https://kube.invalid", Namespace: "ns", TokenFile: token, Client: &http.Client{Transport: f}}}
}

const phase2Object = `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","metadata":{"name":"cluster","namespace":"ns","uid":"uid","resourceVersion":"1","labels":{"keep":"yes"}},"spec":{"logicalID":"logical","keep":true},"status":{"authority":{"owner":{"namespace":"ns","name":"cluster","uid":"uid","logicalID":"logical"},"generation":2,"binding":{"deploymentUID":"deployment","storageID":"storage"},"everApplicationReserved":false,"pendingWrite":{"keep":1},"transition":{"keep":2},"receipt":{"keep":3}},"unrelated":{"keep":true},"recoveryQueue":{"version":1}}}`

var phase2Ref = V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "uid"}

func TestPhase2StatusCASPreservesObject(t *testing.T) {
	var methods []string
	store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		methods = append(methods, r.Method+" "+r.URL.Path)
		if r.Method == http.MethodGet {
			return phase2Response(200, phase2Object), nil
		}
		if r.Method != http.MethodPut || r.URL.Path != "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/ns/rhizaclusters/cluster/status" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		var got, original map[string]json.RawMessage
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(phase2Object), &original); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"apiVersion", "kind", "metadata", "spec"} {
			if !sameJSON(got[key], original[key]) {
				t.Errorf("changed %s", key)
			}
		}
		var gs, os map[string]json.RawMessage
		_ = json.Unmarshal(got["status"], &gs)
		_ = json.Unmarshal(original["status"], &os)
		for _, key := range []string{"authority", "unrelated"} {
			if !sameJSON(gs[key], os[key]) {
				t.Errorf("changed status.%s", key)
			}
		}
		if !sameJSON(gs["recoveryQueue"], json.RawMessage(`{"version":1,"claim":"ours"}`)) {
			t.Error("queue not updated")
		}
		return phase2Response(200, strings.Replace(string(body), `"resourceVersion":"1"`, `"resourceVersion":"2"`, 1)), nil
	})
	snap, err := store.Load(context.Background(), phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.compareAndSwapQueueObserved(context.Background(), snap, json.RawMessage(`{"version":1,"claim":"ours"}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(methods) != 2 {
		t.Fatalf("requests: %v", methods)
	}
}

func TestPhase2StatusCASRejectsLostUnrelatedStatus(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing=%t", missing), func(t *testing.T) {
			applied := strings.Replace(strings.Replace(phase2Object, `"resourceVersion":"1"`, `"resourceVersion":"2"`, 1), `"recoveryQueue":{"version":1}`, `"recoveryQueue":{"version":2}`, 1)
			if missing {
				applied = strings.Replace(applied, `"unrelated":{"keep":true},`, "", 1)
			}
			gets := 0
			store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodGet {
					gets++
					if gets == 1 {
						return phase2Response(200, phase2Object), nil
					}
				}
				return phase2Response(200, applied), nil
			})
			snapshot, err := store.Load(context.Background(), phase2Ref)
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.compareAndSwapQueueObserved(context.Background(), snapshot, json.RawMessage(`{"version":2}`))
			if missing && !errors.Is(err, ErrV1Alpha2StatusOutcomeUnknown) {
				t.Fatalf("missing unrelated status: %v", err)
			}
			if !missing && err != nil {
				t.Fatalf("preserved unrelated status: %v", err)
			}
		})
	}
}

func TestPhase2StatusRejectsInvalidIdentityAndAuthorityWithoutWrites(t *testing.T) {
	tests := map[string]string{
		"missing authority":           strings.Replace(phase2Object, `"authority":{`, `"notAuthority":{`, 1),
		"null authority":              strings.Replace(phase2Object, `"authority":{`, `"authority":null,"notAuthority":{`, 1),
		"corrupt authority":           strings.Replace(phase2Object, `"generation":2`, `"generation":"bad"`, 1),
		"missing reservation history": strings.Replace(phase2Object, `"everApplicationReserved":false,`, "", 1),
		"null reservation history":    strings.Replace(phase2Object, `"everApplicationReserved":false`, `"everApplicationReserved":null`, 1),
		"wrong uid":                   strings.Replace(phase2Object, `"uid":"uid"`, `"uid":"other"`, 1),
		"wrong namespace":             strings.Replace(phase2Object, `"namespace":"ns"`, `"namespace":"other"`, 1),
		"wrong version":               strings.Replace(phase2Object, `v1alpha2`, `v1alpha1`, 1),
		"missing resource version":    strings.Replace(phase2Object, `"resourceVersion":"1",`, "", 1),
		"wrong logical ID":            strings.Replace(phase2Object, `"spec":{"logicalID":"logical"`, `"spec":{"logicalID":"other"`, 1),
		"deleting cluster":            strings.Replace(phase2Object, `"resourceVersion":"1"`, `"deletionTimestamp":"2026-01-01T00:00:00Z","resourceVersion":"1"`, 1),
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			var methods []string
			store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				methods = append(methods, r.Method)
				return phase2Response(200, body), nil
			})
			if _, err := store.Load(context.Background(), phase2Ref); err == nil {
				t.Fatal("invalid object accepted")
			}
			if len(methods) != 1 || methods[0] != http.MethodGet {
				t.Fatalf("requests: %v", methods)
			}
		})
	}
}

func TestPhase2StatusAmbiguousWrites(t *testing.T) {
	queue := json.RawMessage(`{"version":1,"claim":"ours"}`)
	applied := strings.Replace(strings.Replace(phase2Object, `"resourceVersion":"1"`, `"resourceVersion":"2"`, 1), `"recoveryQueue":{"version":1}`, `"recoveryQueue":{"version":1,"claim":"ours"}`, 1)
	tests := []struct {
		name     string
		putCode  int
		putErr   error
		readback string
		want     error
		gets     int
	}{
		{"conflict", 409, nil, "", ErrV1Alpha2StatusConflict, 1},
		{"timeout after apply", 0, fmt.Errorf("timeout"), applied, nil, 2},
		{"timeout before apply", 0, fmt.Errorf("timeout"), phase2Object, ErrV1Alpha2StatusOutcomeUnknown, 2},
		{"empty response no evidence", 200, nil, phase2Object, ErrV1Alpha2StatusOutcomeUnknown, 2},
		{"changed uid", 0, fmt.Errorf("timeout"), strings.Replace(applied, `"uid":"uid"`, `"uid":"other"`, 1), ErrV1Alpha2StatusOutcomeUnknown, 2},
		{"changed authority", 0, fmt.Errorf("timeout"), strings.Replace(applied, `"generation":2`, `"generation":3`, 1), ErrV1Alpha2StatusOutcomeUnknown, 2},
		{"other claim", 0, fmt.Errorf("timeout"), strings.Replace(applied, `"claim":"ours"`, `"claim":"other"`, 1), ErrV1Alpha2StatusOutcomeUnknown, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gets, puts := 0, 0
			store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				switch r.Method {
				case http.MethodGet:
					gets++
					if gets == 1 {
						return phase2Response(200, phase2Object), nil
					}
					return phase2Response(200, tt.readback), nil
				case http.MethodPut:
					puts++
					if tt.putErr != nil {
						return nil, tt.putErr
					}
					return phase2Response(tt.putCode, ""), nil
				default:
					t.Fatalf("unexpected method %s", r.Method)
					return nil, nil
				}
			})
			snap, err := store.Load(context.Background(), phase2Ref)
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.compareAndSwapQueueObserved(context.Background(), snap, queue)
			if (tt.want == nil && err != nil) || (tt.want != nil && !errors.Is(err, tt.want)) {
				t.Fatalf("got error %v, want %v", err, tt.want)
			}
			if gets != tt.gets || puts != 1 {
				t.Fatalf("got %d GET, %d PUT", gets, puts)
			}
		})
	}
}
