package runtimeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/fencing/cri"
)

type testAuth struct {
	principal Principal
	err       error
}

func (a testAuth) Authenticate(context.Context, string) (Principal, error) { return a.principal, a.err }

type testBackend struct {
	enrolls, stops, resumes int
	enrollErr               error
	stopErr                 error
	resumeErr               error
	crashAfterEffect        bool
	terminal                bool
	receipt                 json.RawMessage
	scope                   json.RawMessage
	changeBinding           func(*fencing.RuntimeBinding)
	onStop                  func()
}

type cgroupNamespaceDiagnosticTestError struct{}

func (cgroupNamespaceDiagnosticTestError) Error() string {
	return "unified cgroup path is unsafe (traversal)"
}
func (cgroupNamespaceDiagnosticTestError) Unwrap() error { return cri.ErrUnverified }
func (cgroupNamespaceDiagnosticTestError) CgroupNamespaceDiagnosticCode() string {
	return "cgroup_namespace_all_different"
}

func (b *testBackend) Enroll(_ context.Context, base fencing.RuntimeBinding) (fencing.RuntimeBinding, json.RawMessage, error) {
	b.enrolls++
	if b.enrollErr != nil {
		return fencing.RuntimeBinding{}, nil, b.enrollErr
	}
	base.NodeUID, base.MachineID, base.BootID = "node-uid", "machine", "boot"
	base.RuntimeID, base.SandboxID = "cri-profile", "sandbox"
	if b.changeBinding != nil {
		b.changeBinding(&base)
	}
	if len(b.scope) != 0 {
		return base, append(json.RawMessage(nil), b.scope...), nil
	}
	scope, err := json.Marshal(cri.CgroupScope{Path: "kubepods/pod-a", Dev: 1, Ino: 2, PID: 123, StartTime: 789,
		RuntimeHash: fencing.RuntimeScopeBindingDigest(base), ProfileID: "qualified-test-profile"})
	return base, scope, err
}

func TestEnrollmentFailureReturnsGenericConflictAndOnlyAllowlistedDiagnostic(t *testing.T) {
	const canary = "backend-error-secret-canary"
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	backend := &testBackend{enrollErr: fmt.Errorf("%w: containerd verbose info schema differs (%s)", cri.ErrUnverified, canary)}
	var gotStage, gotCode string
	cfg := Config{AuthorityID: "authority", AuthorityNamespace: "rhiza", AuthorityServiceAccount: "rhiza-fencer", AuthorityWorkloadUID: "authority-deployment-uid",
		Audience: "rhiza-runtime-agent", Directory: dir,
		Auth:    testAuth{principal: Principal{Username: "system:serviceaccount:rhiza:rhiza-fencer", PodUID: "authority-pod", WorkloadUID: "authority-deployment-uid", Audiences: []string{"rhiza-runtime-agent"}}},
		Backend: backend, Diagnostic: func(stage, code string) { gotStage, gotCode = stage, code }}
	service, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	requestBody, err := json.Marshal(enrollRequest{Binding: testBaseBinding()})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/enroll", bytes.NewReader(requestBody))
	request.Header.Set("Authorization", "Bearer trusted-test-token")
	response := httptest.NewRecorder()
	service.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusConflict || strings.Contains(response.Body.String(), canary) || response.Body.String() != "runtime enrollment unavailable\n" {
		t.Fatalf("enrollment response leaked backend detail or changed status: %d %q", response.Code, response.Body.String())
	}
	if gotStage != "cri_enroll" || gotCode != "containerd_verbose_schema_mismatch" {
		t.Fatalf("unexpected private enrollment diagnostic stage=%q code=%q", gotStage, gotCode)
	}
}

func TestTraversalEnrollmentEmitsBoundedNamespaceDiagnosticWithoutChangingRejection(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	backend := &testBackend{enrollErr: cgroupNamespaceDiagnosticTestError{}}
	var diagnostics []string
	cfg := Config{AuthorityID: "authority", AuthorityNamespace: "rhiza", AuthorityServiceAccount: "rhiza-fencer", AuthorityWorkloadUID: "authority-deployment-uid",
		Audience: "rhiza-runtime-agent", Directory: dir,
		Auth:    testAuth{principal: Principal{Username: "system:serviceaccount:rhiza:rhiza-fencer", PodUID: "authority-pod", WorkloadUID: "authority-deployment-uid", Audiences: []string{"rhiza-runtime-agent"}}},
		Backend: backend, Diagnostic: func(stage, code string) {
			diagnostics = append(diagnostics, stage+"/"+code)
		}}
	service, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	requestBody, err := json.Marshal(enrollRequest{Binding: testBaseBinding()})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/enroll", bytes.NewReader(requestBody))
	request.Header.Set("Authorization", "Bearer trusted-test-token")
	response := httptest.NewRecorder()
	service.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusConflict || response.Body.String() != "runtime enrollment unavailable\n" {
		t.Fatalf("traversal enrollment was not rejected generically: %d %q", response.Code, response.Body.String())
	}
	want := []string{
		"cri_enroll/process_cgroup_path_traversal",
		"cri_enroll/cgroup_namespace_all_different",
	}
	if strings.Join(diagnostics, ",") != strings.Join(want, ",") {
		t.Fatalf("diagnostics = %#v, want %#v", diagnostics, want)
	}
}

func TestEnrollmentBackendDiagnosticUsesGenericClassForUnclassifiedErrors(t *testing.T) {
	if got := enrollmentBackendErrorCode(errors.New("untrusted diagnostic text")); got != "backend_error" {
		t.Fatalf("unclassified backend error code=%q", got)
	}
	if got := enrollmentBackendErrorCode(fmt.Errorf("%w: untrusted details", cri.ErrUnverified)); got != "cri_scope_unverified" {
		t.Fatalf("unmatched CRI error code=%q", got)
	}
}
func (b *testBackend) Stop(_ context.Context, binding fencing.RuntimeBinding, request fencing.StopRequest, _ json.RawMessage) (json.RawMessage, error) {
	b.stops++
	if b.onStop != nil {
		b.onStop()
	}
	if b.stopErr != nil {
		return nil, b.stopErr
	}
	data, err := json.Marshal(map[string]any{"runtime": binding, "request": request, "containerExited": true, "scopeEmpty": true})
	b.receipt, b.terminal = data, true
	if b.crashAfterEffect {
		return nil, errors.New("simulated agent crash after CRI stop effect")
	}
	return data, err
}

func (b *testBackend) ResumeStop(context.Context, fencing.RuntimeBinding, fencing.StopRequest, json.RawMessage) (json.RawMessage, error) {
	b.resumes++
	if b.resumeErr != nil {
		return nil, b.resumeErr
	}
	if !b.terminal || len(b.receipt) == 0 {
		return nil, fencing.ErrPending
	}
	return append(json.RawMessage(nil), b.receipt...), nil
}

func testBaseBinding() fencing.RuntimeBinding {
	return fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "namespace-uid", BindingUID: "binding-uid", StatefulSetUID: "statefulset-uid", SourceClusterID: "source",
		NodeID: "rhiza-0", PodUID: "pod-uid", RunID: strings.Repeat("a", 32), NodeName: "node-a", ContainerID: "containerd://container", ImageID: "image-digest"}
}

func testServiceConfig(dir string, backend Backend) Config {
	return Config{AuthorityID: "authority", AuthorityNamespace: "rhiza", AuthorityServiceAccount: "rhiza-fencer", AuthorityWorkloadUID: "authority-deployment-uid",
		Audience: "rhiza-runtime-agent", Directory: dir,
		Auth:    testAuth{principal: Principal{Username: "system:serviceaccount:rhiza:rhiza-fencer", PodUID: "authority-pod", WorkloadUID: "authority-deployment-uid", Audiences: []string{"rhiza-runtime-agent"}}},
		Backend: backend}
}

func TestStopRecapturesMissingDirectoryOnlyForExactLiveScope(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	backend := &testBackend{}
	cfg := testServiceConfig(dir, backend)
	service, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := service.enroll(t.Context(), testBaseBinding())
	if err != nil || !validDigest(binding.CapturedScopeDigest) {
		t.Fatalf("initial enrollment lacks captured-scope digest: binding=%+v err=%v", binding, err)
	}
	request := fencing.StopRequest{OperationID: "closed-op", RequestHash: strings.Repeat("b", 64), Identity: fencing.Identity{
		SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID, WALIdentity: "wal"}}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	backend.onStop = func() {
		stored, readErr := restarted.read(enrollmentKey(binding))
		if readErr != nil || stored.Binding != binding || stored.Stop == nil || *stored.Stop != request || rawDigest(stored.Scope) != binding.CapturedScopeDigest {
			t.Errorf("stop effect began before exact binding, scope, and intent were durable: %+v %v", stored, readErr)
		}
	}
	receipt, err := restarted.stop(t.Context(), binding, request)
	if err != nil || len(receipt) == 0 || backend.enrolls != 2 || backend.stops != 1 {
		t.Fatalf("missing live enrollment was not recaptured exactly once: receipt=%s err=%v enrolls=%d stops=%d", receipt, err, backend.enrolls, backend.stops)
	}
	if err := restarted.verify(binding, request, receipt); err != nil {
		t.Fatalf("recaptured exact stop receipt did not verify: %v", err)
	}
	if _, err := restarted.stop(t.Context(), binding, request); err != nil || backend.enrolls != 2 || backend.stops != 1 {
		t.Fatalf("saved receipt was not reused: err=%v enrolls=%d stops=%d", err, backend.enrolls, backend.stops)
	}
}

func TestStopDoesNotRecaptureCorruptOrMismatchedEnrollment(t *testing.T) {
	for _, test := range []struct {
		name      string
		corrupt   bool
		legacy    bool
		enrollErr error
		change    func(*testBackend)
	}{
		{name: "corrupt state", corrupt: true},
		{name: "legacy binding lacks scope digest", legacy: true},
		{name: "CRI not found", enrollErr: errors.New("container not found")},
		{name: "measured binding changed", change: func(backend *testBackend) {
			backend.changeBinding = func(binding *fencing.RuntimeBinding) { binding.SandboxID = "replacement-sandbox" }
		}},
		{name: "measured scope changed", change: func(backend *testBackend) {
			backend.scope, _ = json.Marshal(cri.CgroupScope{Path: "kubepods/other", Dev: 1, Ino: 3, PID: 123, StartTime: 789,
				RuntimeHash: fencing.RuntimeScopeBindingDigest(testMeasuredBinding(testBaseBinding())), ProfileID: "qualified-test-profile"})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			backend := &testBackend{}
			cfg := testServiceConfig(dir, backend)
			service, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			binding, err := service.enroll(t.Context(), testBaseBinding())
			if err != nil {
				t.Fatal(err)
			}
			request := fencing.StopRequest{OperationID: "closed-op", RequestHash: strings.Repeat("c", 64), Identity: fencing.Identity{
				SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID}}
			if test.corrupt {
				if err := os.WriteFile(filepath.Join(dir, enrollmentKey(binding)+".json"), []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Remove(filepath.Join(dir, enrollmentKey(binding)+".json")); err != nil {
				t.Fatal(err)
			}
			if test.legacy {
				binding.CapturedScopeDigest = ""
			}
			backend.enrollErr = test.enrollErr
			if test.change != nil {
				test.change(backend)
			}
			if _, err := service.stop(t.Context(), binding, request); err == nil {
				t.Fatal("unsafe recapture returned terminal evidence")
			}
			wantEnrolls := 2
			if test.corrupt || test.legacy {
				wantEnrolls = 1
			}
			if backend.enrolls != wantEnrolls || backend.stops != 0 {
				t.Fatalf("unsafe recapture reached stop: enrolls=%d want=%d stops=%d", backend.enrolls, wantEnrolls, backend.stops)
			}
			stored, readErr := service.read(enrollmentKey(binding))
			if test.corrupt {
				if readErr == nil {
					t.Fatal("corrupt local state was overwritten")
				}
			} else if errors.Is(readErr, os.ErrNotExist) {
				return
			} else if readErr != nil || stored.Stop != nil || len(stored.Receipt) != 0 {
				t.Fatalf("failed recapture persisted stop/receipt: %+v err=%v", stored, readErr)
			}
		})
	}
}

func testMeasuredBinding(base fencing.RuntimeBinding) fencing.RuntimeBinding {
	base.NodeUID, base.MachineID, base.BootID = "node-uid", "machine", "boot"
	base.RuntimeID, base.SandboxID = "cri-profile", "sandbox"
	return base
}

func TestDurableExactReceiptIsRecoveredBeforeBackendAfterRestart(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	backend := &testBackend{}
	auth := testAuth{principal: Principal{Username: "system:serviceaccount:rhiza:rhiza-fencer", PodUID: "authority-pod", WorkloadUID: "authority-deployment-uid", Audiences: []string{"rhiza-runtime-agent"}}}
	cfg := Config{AuthorityID: "authority", AuthorityNamespace: "rhiza", AuthorityServiceAccount: "rhiza-fencer", AuthorityWorkloadUID: "authority-deployment-uid", Audience: "rhiza-runtime-agent", Directory: dir, Auth: auth, Backend: backend}
	service, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var active atomic.Value
	active.Store(service.Handler())
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { active.Load().(http.Handler).ServeHTTP(w, r) }))
	defer server.Close()
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte("authority-token"), 0600); err != nil {
		t.Fatal(err)
	}
	client := &Client{BaseURL: server.URL, TokenFile: token, HTTP: server.Client()}
	base := testBaseBinding()
	binding, err := client.Enroll(t.Context(), base)
	if err != nil || binding.BootID != "boot" {
		t.Fatalf("enrollment failed: %+v %v", binding, err)
	}
	request := fencing.StopRequest{OperationID: "closed-op", RequestHash: strings.Repeat("b", 64), Identity: fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID, WALIdentity: "wal"}}
	want, err := client.Stop(t.Context(), binding, request)
	if err != nil || backend.stops != 1 {
		t.Fatalf("stop failed: %s %v calls=%d", want, err, backend.stops)
	}
	backend.stopErr = errors.New("CRI metadata has been garbage-collected")
	restarted, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	active.Store(restarted.Handler())
	got, err := client.Stop(t.Context(), binding, request)
	if err != nil || string(got) != string(want) || backend.stops != 1 {
		t.Fatalf("retry did not recover the persisted original receipt: %s %v calls=%d", got, err, backend.stops)
	}
	if err := client.Verify(t.Context(), binding, request, got); err != nil {
		t.Fatalf("durable receipt verification failed: %v", err)
	}
	request.OperationID = "different-op"
	if _, err := client.Stop(t.Context(), binding, request); err == nil || backend.stops != 1 {
		t.Fatal("different stop request reused the prior receipt")
	}
}

func TestCrashAfterStopEffectPersistsIntentAndResumesExactReceipt(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	backend := &testBackend{crashAfterEffect: true}
	auth := testAuth{principal: Principal{Username: "system:serviceaccount:rhiza:rhiza-fencer", PodUID: "authority-pod", WorkloadUID: "authority-deployment-uid", Audiences: []string{"rhiza-runtime-agent"}}}
	cfg := Config{AuthorityID: "authority", AuthorityNamespace: "rhiza", AuthorityServiceAccount: "rhiza-fencer", AuthorityWorkloadUID: "authority-deployment-uid", Audience: "rhiza-runtime-agent", Directory: dir, Auth: auth, Backend: backend}
	service, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	base := testBaseBinding()
	binding, err := service.enroll(t.Context(), base)
	if err != nil {
		t.Fatal(err)
	}
	request := fencing.StopRequest{OperationID: "closed-op", RequestHash: strings.Repeat("d", 64), Identity: fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID}}
	if _, err := service.stop(t.Context(), binding, request); err == nil {
		t.Fatal("simulated lost process returned a receipt")
	}
	intent, err := service.read(enrollmentKey(binding))
	if err != nil || intent.Stop == nil || *intent.Stop != request || len(intent.Receipt) != 0 {
		t.Fatalf("exact intent was not durable before effect: %+v %v", intent, err)
	}

	// A new agent process opens the same disk record while the fake qualified
	// backend exposes complete terminal observations from the completed effect.
	backend.crashAfterEffect = false
	restarted, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	first, err := restarted.stop(t.Context(), binding, request)
	if err != nil || backend.resumes != 1 || backend.stops != 1 {
		t.Fatalf("exact stop was not resumed: receipt=%s err=%v stops=%d resumes=%d", first, err, backend.stops, backend.resumes)
	}
	second, err := restarted.stop(t.Context(), binding, request)
	if err != nil || string(first) != string(second) || backend.resumes != 1 {
		t.Fatalf("persisted recovery receipt changed: first=%s second=%s err=%v", first, second, err)
	}
	changed := request
	changed.OperationID = "different-operation"
	if _, err := restarted.stop(t.Context(), binding, changed); err == nil || backend.resumes != 1 {
		t.Fatal("different tuple reached recovery backend")
	}
}

func TestIncompleteStopRecoveryRemainsPending(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	backend := &testBackend{crashAfterEffect: true}
	auth := testAuth{principal: Principal{Username: "system:serviceaccount:rhiza:rhiza-fencer", PodUID: "authority-pod", WorkloadUID: "authority-deployment-uid", Audiences: []string{"rhiza-runtime-agent"}}}
	cfg := Config{AuthorityID: "authority", AuthorityNamespace: "rhiza", AuthorityServiceAccount: "rhiza-fencer", AuthorityWorkloadUID: "authority-deployment-uid", Audience: "rhiza-runtime-agent", Directory: dir, Auth: auth, Backend: backend}
	service, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := service.enroll(t.Context(), testBaseBinding())
	if err != nil {
		t.Fatal(err)
	}
	request := fencing.StopRequest{OperationID: "closed-op", RequestHash: strings.Repeat("e", 64), Identity: fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID}}
	if _, err := service.stop(t.Context(), binding, request); err == nil {
		t.Fatal("expected simulated crash")
	}
	backend.resumeErr = fencing.ErrPending
	if _, err := service.stop(t.Context(), binding, request); !errors.Is(err, fencing.ErrPending) || backend.resumes != 1 {
		t.Fatalf("incomplete recovery did not remain pending: %v resumes=%d", err, backend.resumes)
	}
	changed := request
	changed.RequestHash = strings.Repeat("f", 64)
	if _, err := service.stop(t.Context(), binding, changed); err == nil || backend.resumes != 1 {
		t.Fatal("changed stop tuple reached recovery backend")
	}
}

func TestWrongAuthorityIdentityIsRejectedBeforeBackend(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	backend := &testBackend{}
	cfg := Config{AuthorityID: "authority", AuthorityNamespace: "rhiza", AuthorityServiceAccount: "rhiza-fencer", Audience: "rhiza-runtime-agent", Directory: dir,
		AuthorityWorkloadUID: "authority-deployment-uid", Auth: testAuth{principal: Principal{Username: "system:serviceaccount:rhiza:application", PodUID: "app-pod", WorkloadUID: "authority-deployment-uid", Audiences: []string{"rhiza-runtime-agent"}}}, Backend: backend}
	service, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("POST", "/v1/enroll", strings.NewReader(`{"binding":{}}`))
	request.Header.Set("Authorization", "Bearer app-token")
	response := httptest.NewRecorder()
	service.Handler().ServeHTTP(response, request)
	if response.Code != 403 || backend.enrolls != 0 {
		t.Fatalf("application identity reached CRI: status=%d enrolls=%d", response.Code, backend.enrolls)
	}
}

func TestSameServiceAccountWrongWorkloadRejectedBeforeBackend(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	backend := &testBackend{}
	cfg := Config{AuthorityID: "authority", AuthorityNamespace: "rhiza", AuthorityServiceAccount: "rhiza-fencer", AuthorityWorkloadUID: "authority-deployment-uid", Audience: "rhiza-runtime-agent", Directory: dir,
		Auth: testAuth{principal: Principal{Username: "system:serviceaccount:rhiza:rhiza-fencer", PodUID: "other-pod", WorkloadUID: "other-deployment-uid", Audiences: []string{"rhiza-runtime-agent"}}}, Backend: backend}
	service, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{"/v1/enroll", "/v1/stop"} {
		request := httptest.NewRequest("POST", route, strings.NewReader(`{"binding":{},"request":{}}`))
		request.Header.Set("Authorization", "Bearer token")
		response := httptest.NewRecorder()
		service.Handler().ServeHTTP(response, request)
		if response.Code != 403 {
			t.Fatalf("same-SA wrong workload route=%s status=%d", route, response.Code)
		}
	}
	if backend.enrolls != 0 || backend.stops != 0 {
		t.Fatalf("wrong authority reached backend: enroll=%d stop=%d", backend.enrolls, backend.stops)
	}
}

func TestReceiptWriteFailureDoesNotAcknowledgeStop(t *testing.T) {
	dir := t.TempDir()
	backup := dir + ".unavailable"
	backend := &testBackend{}
	cfg := Config{AuthorityID: "authority", AuthorityNamespace: "rhiza", AuthorityServiceAccount: "rhiza-fencer", Audience: "rhiza-runtime-agent", Directory: dir,
		AuthorityWorkloadUID: "authority-deployment-uid", Auth: testAuth{principal: Principal{Username: "system:serviceaccount:rhiza:rhiza-fencer", PodUID: "authority-pod", WorkloadUID: "authority-deployment-uid", Audiences: []string{"rhiza-runtime-agent"}}}, Backend: backend}
	service := &Service{cfg: cfg, base: dir}
	base := testBaseBinding()
	binding, err := service.enroll(t.Context(), base)
	if err != nil {
		t.Fatal(err)
	}
	backend.onStop = func() {
		if err := os.Rename(dir, backup); err != nil {
			t.Error(err)
		}
	}
	request := fencing.StopRequest{OperationID: "closed", RequestHash: strings.Repeat("c", 64), Identity: fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID}}
	if _, err := service.stop(t.Context(), binding, request); err == nil {
		t.Fatal("stop was acknowledged despite receipt persistence failure")
	}
	if err := os.Rename(backup, dir); err != nil {
		t.Fatal(err)
	}
	if backend.stops != 1 {
		t.Fatalf("expected one exact stop attempt, got %d", backend.stops)
	}
	intent, err := service.read(enrollmentKey(binding))
	if err != nil || intent.Stop == nil || len(intent.Receipt) != 0 {
		t.Fatalf("failed receipt write lost durable intent or acknowledged receipt: %+v %v", intent, err)
	}
}
