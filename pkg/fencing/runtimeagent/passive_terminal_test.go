package runtimeagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/fencing/cri"
)

type passiveTestBackend struct {
	base         Backend
	service      *Service
	scope        json.RawMessage
	badDigest    bool
	observations atomic.Int32
	stopCalls    atomic.Int32
	completed    atomic.Int32
}

func (b *passiveTestBackend) Enroll(ctx context.Context, base fencing.RuntimeBinding) (fencing.RuntimeBinding, json.RawMessage, error) {
	binding, _, err := b.base.Enroll(ctx, base)
	if err != nil {
		return binding, nil, err
	}
	scope := cri.CgroupScope{Path: "pod/db", Dev: 1, Ino: 2, PID: 12, StartTime: 13, RuntimeHash: fencing.RuntimeScopeBindingDigest(binding), ProfileID: "qualified",
		Passive: &cri.PassiveScope{Version: 1, SandboxPID: 14, SandboxStart: 15, SandboxPath: "pod/pause", SandboxDev: 1, SandboxIno: 3,
			SandboxPIDNSDev: 4, SandboxPIDNSIno: 5, TargetPIDNSDev: 4, TargetPIDNSIno: 6, SandboxImage: "registry.example/pause:3", SandboxExecutableSHA256: strings.Repeat("a", 64), SandboxArgs: []string{"/pause"},
			RuntimeHash: fencing.RuntimeScopeBindingDigest(binding), ProfileID: "qualified"}}
	data, err := json.Marshal(scope)
	b.scope = data
	return binding, data, err
}

func (b *passiveTestBackend) Stop(context.Context, fencing.RuntimeBinding, fencing.StopRequest, json.RawMessage) (json.RawMessage, error) {
	b.stopCalls.Add(1)
	return nil, errors.New("normal CRI stop must not be used after complete passive evidence")
}
func (b *passiveTestBackend) ResumeStop(context.Context, fencing.RuntimeBinding, fencing.StopRequest, json.RawMessage) (json.RawMessage, error) {
	return nil, fencing.ErrPending
}
func (b *passiveTestBackend) ObservePassiveTerminal(_ context.Context, binding fencing.RuntimeBinding, scope json.RawMessage) (cri.PassiveObservation, bool, error) {
	b.observations.Add(1)
	digest := sha256.Sum256(scope)
	scopeDigest := hex.EncodeToString(digest[:])
	if b.badDigest {
		scopeDigest = strings.Repeat("0", 64)
	}
	return cri.PassiveObservation{Version: 1, RuntimeHash: fencing.RuntimeScopeBindingDigest(binding), ScopeDigest: scopeDigest, TargetPIDExited: true, TargetCgroupGone: true,
		SandboxPIDExited: true, SandboxCgroupGone: true, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)}, true, nil
}
func (b *passiveTestBackend) BindPassiveTerminal(binding fencing.RuntimeBinding, request fencing.StopRequest, scope json.RawMessage, observation cri.PassiveObservation) (json.RawMessage, error) {
	state, err := b.service.read(enrollmentKey(binding))
	if err != nil || state.PassiveObservation == nil || state.Stop == nil || *state.Stop != request {
		return nil, errors.New("observation and exact request were not durably ordered")
	}
	return json.Marshal(map[string]any{"mode": "passive-kernel-v1", "runtime": binding, "request": request, "observation": observation})
}
func (b *passiveTestBackend) CompletePassiveTerminal(fencing.RuntimeBinding, json.RawMessage) {
	b.completed.Add(1)
}

func TestPassiveTerminalObservationIsDurableBeforeFreshRequestReceipt(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	backend := &passiveTestBackend{base: &testBackend{}}
	service, err := New(testServiceConfig(dir, backend))
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	backend.service = service
	binding, err := service.enroll(t.Context(), testBaseBinding())
	if err != nil {
		t.Fatal(err)
	}
	request := fencing.StopRequest{OperationID: "fresh-stop", RequestHash: strings.Repeat("b", 64), Identity: fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID}}
	receipt, err := service.stop(t.Context(), binding, request)
	if err != nil {
		t.Fatalf("passive stop receipt failed: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(receipt, &got); err != nil || got["mode"] != "passive-kernel-v1" {
		t.Fatalf("wrong passive receipt: %s err=%v", receipt, err)
	}
	if backend.stopCalls.Load() != 0 || backend.observations.Load() == 0 || backend.completed.Load() == 0 {
		t.Fatalf("passive flow fell through or failed cleanup: stops=%d observations=%d completed=%d", backend.stopCalls.Load(), backend.observations.Load(), backend.completed.Load())
	}
	if err := service.verify(binding, request, receipt); err != nil {
		t.Fatalf("persisted passive receipt did not verify: %v", err)
	}
}

func TestInvalidPassiveObservationCannotBecomeReceiptOrSkipNormalStop(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	backend := &passiveTestBackend{base: &testBackend{}, badDigest: true}
	service, err := New(testServiceConfig(dir, backend))
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	backend.service = service
	binding, err := service.enroll(t.Context(), testBaseBinding())
	if err != nil {
		t.Fatal(err)
	}
	request := fencing.StopRequest{OperationID: "fresh-stop", RequestHash: strings.Repeat("c", 64), Identity: fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID}}
	if _, err := service.stop(t.Context(), binding, request); err == nil {
		t.Fatal("bad passive scope digest produced a receipt")
	}
	state, readErr := service.read(enrollmentKey(binding))
	if readErr != nil || state.Stop != nil || state.PassiveObservation != nil || len(state.Receipt) != 0 {
		t.Fatalf("invalid passive evidence changed durable stop state: %+v err=%v", state, readErr)
	}
	if backend.stopCalls.Load() != 0 {
		t.Fatal("malformed passive observation fell through into a stop effect")
	}
}

func TestPassiveObservationWriteFailureRetainsProofUntilDurableRetry(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	backend := &passiveTestBackend{base: &testBackend{}}
	service, err := New(testServiceConfig(dir, backend))
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	backend.service = service
	binding, err := service.enroll(t.Context(), testBaseBinding())
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	var failedBeforeComplete atomic.Bool
	service.writeRecord = func(key string, state record) error {
		if state.PassiveObservation != nil && attempts.Add(1) == 1 {
			failedBeforeComplete.Store(backend.completed.Load() == 0)
			return errors.New("injected durable-write failure")
		}
		return service.write(key, state)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		state, readErr := service.read(enrollmentKey(binding))
		if readErr == nil && state.PassiveObservation != nil && backend.completed.Load() == 1 {
			if attempts.Load() < 2 || !failedBeforeComplete.Load() {
				t.Fatalf("passive proof was not retained through failed persistence: attempts=%d failedBeforeComplete=%v", attempts.Load(), failedBeforeComplete.Load())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("passive observation did not persist and complete after retry: attempts=%d completed=%d", attempts.Load(), backend.completed.Load())
}

func TestPassiveObservationExhaustedWriteRetriesRemainRecoverable(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	backend := &passiveTestBackend{base: &testBackend{}}
	service, err := New(testServiceConfig(dir, backend))
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	backend.service = service
	binding, err := service.enroll(t.Context(), testBaseBinding())
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	service.writeRecord = func(string, record) error {
		attempts.Add(1)
		return errors.New("injected persistent durable-write failure")
	}
	deadline := time.Now().Add(5 * time.Second)
	watchStopped := false
	for time.Now().Before(deadline) {
		if attempts.Load() >= 5 {
			service.mu.Lock()
			stopped := !service.watching[enrollmentKey(binding)]
			service.mu.Unlock()
			if stopped {
				watchStopped = true
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if attempts.Load() != 5 || backend.completed.Load() != 0 || !watchStopped {
		t.Fatalf("failed persistence incorrectly completed or was not bounded: attempts=%d completed=%d watchStopped=%v", attempts.Load(), backend.completed.Load(), watchStopped)
	}
	state, err := service.read(enrollmentKey(binding))
	if err != nil || state.PassiveObservation != nil || state.Stop != nil || len(state.Receipt) != 0 {
		t.Fatalf("failed persistence altered durable stop state: %+v err=%v", state, err)
	}
	service.writeRecord = service.write
	request := fencing.StopRequest{OperationID: "fresh-stop-after-retry", RequestHash: strings.Repeat("d", 64), Identity: fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID}}
	if _, err := service.stop(t.Context(), binding, request); err != nil {
		t.Fatalf("retained passive session was not recoverable through a fresh stop: %v", err)
	}
	if backend.completed.Load() == 0 || backend.stopCalls.Load() != 0 {
		t.Fatalf("recovered passive session fell through to normal stop: completed=%d stops=%d", backend.completed.Load(), backend.stopCalls.Load())
	}
}
