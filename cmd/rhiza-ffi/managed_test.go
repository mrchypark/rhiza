//go:build cgo

package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/managedsql"
)

func TestManagedOpenRejectsOverridesAndMissingIdentity(t *testing.T) {
	for _, key := range []string{"RHIZA_CLUSTER_NAME", "POD_NAMESPACE", "RHIZA_CLUSTER_UID", "POD_NAME", "POD_UID"} {
		t.Setenv(key, "")
	}
	for _, input := range []string{`null`, `[]`, `{"authority":{}}`, `{"runtime_profile":2}`, `{}`} {
		if code := responseErrorCode(t, goOpenManaged([]byte(input))); code != "invalid_request" {
			t.Fatalf("managed open %s: %s", input, code)
		}
	}
	for _, key := range []string{"RHIZA_CLUSTER_NAME", "POD_NAMESPACE", "RHIZA_CLUSTER_UID", "POD_NAME", "POD_UID"} {
		t.Setenv(key, "fixture")
	}
	profile := filepath.Join(t.TempDir(), "runtime.json")
	secret := "DO-NOT-ECHO-PRIVATE-PROFILE"
	if err := os.WriteFile(profile, []byte(`{"unknown":"`+secret+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	input, _ := json.Marshal(managedOpenRequest{RuntimeProfile: profile})
	response := goOpenManaged(input)
	if responseErrorCode(t, response) != "invalid_request" || strings.Contains(string(response), secret) || strings.Contains(string(response), profile) {
		t.Fatal("invalid profile was accepted or exposed")
	}
}

func TestManagedDispatchRejectsLegacySurfaceWithoutHostAccess(t *testing.T) {
	entry := &ffiEntry{managed: &managedsql.Host{}}
	for _, operation := range []string{"membership_change", "membership_abort", "membership_status", "execute_returning", "notify_publish", "kv_put", "object_store_stats"} {
		_, err := dispatch(context.Background(), entry, callEnvelope{Operation: operation, Request: json.RawMessage(`{}`)})
		var typed *ffiError
		if !errors.As(err, &typed) || typed.Code != "invalid_request" {
			t.Fatalf("legacy operation %s was accepted", operation)
		}
	}
}

func TestManagedCleanupSharesDrainedHandleLifecycle(t *testing.T) {
	handle := openTestDB(t)
	entry, release, err := acquire(handle)
	if err != nil {
		t.Fatal(err)
	}
	cleaned := make(chan struct{})
	entry.cleanup = func() error { close(cleaned); return nil }
	done := make(chan []byte, 1)
	go func() { done <- goClose(handle) }()
	select {
	case <-entry.ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("close did not cancel active calls")
	}
	select {
	case <-cleaned:
		t.Fatal("cleanup ran before active calls drained")
	default:
	}
	release()
	select {
	case result := <-done:
		requireData(t, mustResponse(t, result))
	case <-time.After(5 * time.Second):
		t.Fatal("close did not finish")
	}
	select {
	case <-cleaned:
	default:
		t.Fatal("owned cleanup omitted")
	}
}
