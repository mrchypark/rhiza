package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
)

type rootInitStatusError struct {
	status int
	cause  error
}

func (e rootInitStatusError) Error() string       { return "private endpoint and credential detail" }
func (e rootInitStatusError) Unwrap() error       { return e.cause }
func (e rootInitStatusError) HTTPStatusCode() int { return e.status }

type rootInitFailingBucket struct {
	objstore.Bucket
	err error
}

func (b rootInitFailingBucket) Attributes(context.Context, string) (objstore.ObjectAttributes, error) {
	return objstore.ObjectAttributes{}, b.err
}

type rootInitCreateFailingBucket struct {
	objstore.Bucket
	err error
}

func (b rootInitCreateFailingBucket) Upload(context.Context, string, io.Reader, ...objstore.ObjectUploadOption) error {
	return b.err
}

func TestAuthorityRootInitDiagnosticPreservesCauseAndSafeStatus(t *testing.T) {
	rootCause := errors.New("underlying provider sentinel")
	base := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = base.Close() })
	bucket := rootInitFailingBucket{Bucket: base, err: rootInitStatusError{status: 403, cause: rootCause}}
	store, err := NewAuthorityStateStore(bucket, "diagnostic-test")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.ReadRoot(t.Context(), "sts", "source")
	if err == nil || !errors.Is(err, rootCause) {
		t.Fatalf("root wrapper lost the provider cause: %v", err)
	}
	if !strings.Contains(err.Error(), "fencing: root-init phase=load cause=permission:") {
		t.Fatalf("root wrapper omitted fixed phase/cause: %v", err)
	}
	if got := store.rootCause(errors.New("private unknown backend code")); got != "unknown" {
		t.Fatalf("unknown backend cause = %q", got)
	}
}

func TestAuthorityRootGenesisCreateCauseSurvivesMissingReadback(t *testing.T) {
	for _, tc := range []struct {
		name, causeCode string
		status          int
	}{
		{name: "permission", causeCode: "permission", status: 403},
		{name: "timeout", causeCode: "timeout", status: 504},
	} {
		t.Run(tc.name, func(t *testing.T) {
			providerCause := errors.New("private provider sentinel")
			base := objstore.NewInMemBucket()
			t.Cleanup(func() { _ = base.Close() })
			bucket := rootInitCreateFailingBucket{Bucket: base, err: rootInitStatusError{status: tc.status, cause: providerCause}}
			store, err := NewAuthorityStateStore(bucket, "genesis-diagnostic-test")
			if err != nil {
				t.Fatal(err)
			}
			err = store.ensureRootGenesis(t.Context(), "root-identity")
			if err == nil || !errors.Is(err, providerCause) || !errors.Is(err, errAuthorityStateNotFound) {
				t.Fatalf("genesis error lost provider/readback chain: %v", err)
			}
			wantPrefix := "fencing: root-init phase=genesis cause=" + tc.causeCode + ":"
			if !strings.HasPrefix(err.Error(), wantPrefix) {
				t.Fatalf("genesis create cause was replaced by readback error: got %q, want prefix %q", err, wantPrefix)
			}
		})
	}
}

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
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: token, Client: api.Client()}, LegacyCooperativeControlState: true}
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

func TestRegisterStageErrorPreservesCause(t *testing.T) {
	cause := fmt.Errorf("private backend failure: %w", fencing.ErrPending)
	for _, stage := range []string{"root-init", "startup-grant", "ledger-register", "producer-reserve", "archive-scope"} {
		t.Run(stage, func(t *testing.T) {
			err := registerStageError(stage, cause)
			if !errors.Is(err, cause) {
				t.Fatalf("stage wrapper lost cause: %v", err)
			}
			if !errors.Is(err, fencing.ErrPending) {
				t.Fatalf("stage wrapper lost sentinel semantics: %v", err)
			}
			if !strings.HasPrefix(err.Error(), "fencing: register "+stage+" rejected:") {
				t.Fatalf("stage wrapper has no fixed diagnostic prefix: %v", err)
			}
		})
	}
}

func TestRegisterRootInitRejectionEmitsStageDiagnostic(t *testing.T) {
	f := newSupervisorRegistrationFixture(t, false)
	ctx := t.Context()
	if err := f.e.initializeRoot(ctx, "sts-uid", f.identity.SourceClusterID); err != nil {
		t.Fatal(err)
	}
	f.e.RequireMutationJournal = true
	client := fencing.AuthorityClient{URL: f.server.URL + "/v1/supervisor", TokenFile: f.token, Client: f.server.Client()}

	var captured bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&captured)
	t.Cleanup(func() { log.SetOutput(previous) })
	if err := client.Register(ctx, f.identity); err == nil {
		t.Fatal("required executor accepted an existing cooperative root")
	}
	if !strings.Contains(captured.String(), `fencing supervisor transition rejected: action="register" podUID="pod-0" error="fencing: register root-init rejected:`) {
		t.Fatalf("actual handler rejection omitted the fixed register diagnostic: %s", captured.String())
	}
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
