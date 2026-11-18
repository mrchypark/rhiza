package operator

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestCooperativeLedgerCASRegistrationRacesClosure(t *testing.T) {
	var mu sync.Mutex
	var current object
	version := 0
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			if current == nil {
				w.WriteHeader(404)
				return
			}
			_ = json.NewEncoder(w).Encode(current)
		case http.MethodPost, http.MethodPut:
			var next object
			if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			if (r.Method == http.MethodPost && current != nil) || (r.Method == http.MethodPut && str(nested(next, "metadata", "resourceVersion")) != fmt.Sprint(version)) {
				w.WriteHeader(409)
				return
			}
			version++
			asObject(next["metadata"])["resourceVersion"] = fmt.Sprint(version)
			current = next
			w.WriteHeader(200)
		default:
			w.WriteHeader(405)
		}
	}))
	defer api.Close()
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: token, Client: api.Client()}}
	id := fencing.Identity{SourceClusterID: "source", NodeID: "n1", PodUID: "pod", RunID: "run"}
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		_, err := e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error { return l.Register(id) })
		results <- err
	}()
	go func() {
		<-start
		_, err := e.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error { return l.Close("op", "hash") })
		results <- err
	}()
	close(start)
	for range 2 {
		if err := <-results; err != nil && !errors.Is(err, fencing.ErrClosed) {
			t.Fatal(err)
		}
	}
	state, err := e.read(t.Context(), "sts", "source")
	if err != nil {
		t.Fatal(err)
	}
	if state.Ledger.ClosedBy != "op" {
		t.Fatal("closure lost")
	}
	if run, exists := state.Ledger.Runs[id.RunID]; exists && (run.Stop == nil || run.Stop.OperationID != "op") {
		t.Fatal("registered run escaped closure")
	}
	if err := state.Ledger.Register(fencing.Identity{SourceClusterID: "source", NodeID: "n2", PodUID: "p2", RunID: "r2"}); err != fencing.ErrClosed {
		t.Fatalf("restart gate reopened: %v", err)
	}
}

func TestCooperativeLedgerCapacityFailsClosed(t *testing.T) {
	const limit = 700000 // Characterizes the persisted ledger contract.
	boundary, below := ledgerRunsAroundCapacity(t, limit)
	for _, tc := range []struct {
		name       string
		runs       int
		wantStored bool
	}{
		{name: "below limit persists", runs: below, wantStored: true},
		{name: "oversized update preserves evidence", runs: boundary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			initial := ledgerWithRuns(tc.runs)
			if size := len(jsonBytes(initial)); size > limit {
				t.Fatalf("initial ledger has %d bytes, limit is %d", size, limit)
			}
			identity := ledgerRunIdentity(tc.runs)
			identity.WALIdentity = ""
			expected := ledgerWithRuns(tc.runs)
			if err := expected.Ledger.Register(identity); err != nil {
				t.Fatal(err)
			}
			expectedSize := len(jsonBytes(expected))
			if (expectedSize <= limit) != tc.wantStored {
				t.Fatalf("updated ledger has %d bytes; wantStored=%t", expectedSize, tc.wantStored)
			}

			executor, mutations, persisted := newLedgerWriteFixture(t, initial)
			before := persisted()
			state, err := executor.change(t.Context(), "sts", "source", func(l *fencing.Ledger) error {
				return l.Register(identity)
			})
			if tc.wantStored {
				if err != nil {
					t.Fatalf("below-limit update failed: %v", err)
				}
				if mutations() != 1 {
					t.Fatalf("below-limit update wrote %d times, want 1", mutations())
				}
			} else {
				if err == nil || err.Error() != "fencing: ledger capacity exhausted; old evidence cannot be discarded" {
					t.Fatalf("oversized update error = %v", err)
				}
				if len(jsonBytes(state)) <= limit {
					t.Fatal("test did not characterize an oversized update")
				}
				if mutations() != 0 {
					t.Fatalf("oversized update issued %d mutations, want 0", mutations())
				}
				if after := persisted(); after != before {
					t.Fatal("oversized update changed persisted ledger evidence")
				}
			}

			stored, err := executor.read(t.Context(), "sts", "source")
			if err != nil {
				t.Fatal(err)
			}
			wantStored := initial
			if tc.wantStored {
				wantStored = expected
			}
			if got, want := string(jsonBytes(stored)), string(jsonBytes(wantStored)); got != want {
				t.Fatal("persisted ledger differs from expected evidence")
			}
		})
	}
}

func ledgerRunIdentity(index int) fencing.Identity {
	return fencing.Identity{
		SourceClusterID: "source",
		NodeID:          fmt.Sprintf("node-%04d", index),
		PodUID:          fmt.Sprintf("pod-%04d", index),
		RunID:           fmt.Sprintf("%064x", index+1),
		WALIdentity:     fmt.Sprintf("%064x", index+1),
	}
}

func ledgerWithRuns(count int) fenceLedger {
	state := fenceLedger{
		StatefulSetUID: "sts",
		Ledger: fencing.Ledger{
			SourceClusterID: "source",
			Runs:            make(map[string]fencing.Run, count),
		},
	}
	for i := 0; i < count; i++ {
		identity := ledgerRunIdentity(i)
		state.Ledger.Runs[identity.RunID] = fencing.Run{Identity: identity}
	}
	return state
}

func ledgerRunsAroundCapacity(t *testing.T, limit int) (boundary, below int) {
	t.Helper()
	for {
		candidate := boundary + 8
		if len(jsonBytes(ledgerWithRuns(candidate))) > limit {
			break
		}
		boundary = candidate
	}
	for len(jsonBytes(ledgerWithRuns(boundary+1))) <= limit {
		boundary++
	}
	below = boundary - 64
	if below < 1 || len(jsonBytes(ledgerWithRuns(below+1))) >= limit {
		t.Fatal("could not construct ledgers on both sides of the capacity limit")
	}
	return boundary, below
}

func newLedgerWriteFixture(t *testing.T, state fenceLedger) (*FencingExecutor, func() int, func() string) {
	t.Helper()
	current := object{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   object{"resourceVersion": "1"},
		"data":       object{"ledger.json": string(jsonBytes(state))},
	}
	mutations := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(current)
		case http.MethodPost, http.MethodPut:
			mutations++
			var next object
			if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			current = next
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	executor := &FencingExecutor{Kube: &Kubernetes{BaseURL: server.URL, Namespace: "test", TokenFile: token, Client: server.Client()}}
	return executor,
		func() int { return mutations },
		func() string { return str(nested(current, "data", "ledger.json")) }
}

func supervisedTestPod() object {
	return object{
		"metadata": object{"name": "rhiza-0", "uid": "pod-uid", "annotations": object{fencingProtocolAnnotation: "1"}},
		"spec":     object{"containers": []any{object{"command": []any{"/usr/local/bin/rhiza-fencer"}, "args": []any{"supervise", "--", "/usr/local/bin/rhiza"}, "securityContext": object{"allowPrivilegeEscalation": false, "capabilities": object{"drop": []any{"ALL"}}}}}},
	}
}

func TestCooperativeRejectsUnsupportedProcessBoundaries(t *testing.T) {
	for _, scenario := range []string{"unwrapped", "shared", "host", "sidecar", "init", "ephemeral", "privileged", "hostpath", "capability", "lifecycle", "execprobe", "procmount", "procpath"} {
		t.Run(scenario, func(t *testing.T) {
			pod := supervisedTestPod()
			spec := asObject(pod["spec"])
			ctr := asObject(list(spec["containers"])[0])
			switch scenario {
			case "unwrapped":
				ctr["command"] = []any{"/usr/local/bin/rhiza"}
			case "shared":
				spec["shareProcessNamespace"] = true
			case "host":
				spec["hostPID"] = true
			case "sidecar":
				spec["containers"] = append(list(spec["containers"]), object{"name": "writer"})
			case "init":
				spec["initContainers"] = []any{object{"name": "writer"}}
			case "ephemeral":
				spec["ephemeralContainers"] = []any{object{"name": "debug"}}
			case "privileged":
				asObject(ctr["securityContext"])["privileged"] = true
			case "hostpath":
				spec["volumes"] = []any{object{"hostPath": object{"path": "/"}}}
			case "capability":
				asObject(nested(ctr, "securityContext", "capabilities"))["add"] = []any{"SYS_ADMIN"}
			case "lifecycle":
				ctr["lifecycle"] = object{"postStart": object{"exec": object{"command": []any{"writer"}}}}
			case "execprobe":
				ctr["readinessProbe"] = object{"exec": object{"command": []any{"writer"}}}
			case "procmount":
				asObject(ctr["securityContext"])["procMount"] = "Unmasked"
			case "procpath":
				ctr["volumeMounts"] = []any{object{"mountPath": "/proc", "name": "hidden"}}
			}
			if err := validateSupervisedPod(pod); err == nil {
				t.Fatal("unsupported process boundary accepted")
			}
		})
	}
	if err := validateSupervisedPod(supervisedTestPod()); err != nil {
		t.Fatal(err)
	}
}
