package node

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
)

// Negative construction states only: no synthetic Core/receipt or permissive
// authority checker is used to claim that a real runtime was admitted.
func TestInternalApplicationRequiresActualRuntime(t *testing.T) {
	for _, policy := range []sqlpolicy.MaterializationPolicy{sqlpolicy.LegacyMaterializationPolicy, sqlpolicy.ManagedSQLKVMaterializationPolicy, sqlpolicy.ManagedRequestMaterializationPolicy} {
		n := New(&types.ExecutionConfig{})
		n.managedStartup = &ManagedStartupConfig{expectedPolicy: policy}
		if _, err := n.checkInternalApplicationRuntime(context.Background()); err == nil {
			t.Fatalf("policy %d accepted unopened runtime", policy)
		}
		if err := n.installInternalApplicationBinding(context.Background()); err == nil {
			t.Fatalf("policy %d installed an unvalidated binding", policy)
		}
	}
	n := New(&types.ExecutionConfig{})
	n.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedRequestMaterializationPolicy}
	n.opened.Store(true)
	n.ready.Store(true)
	n.managedPeerVerified.Store(true)
	if n.Ready() || n.managedLearnReady() {
		t.Fatal("flags without actual Core/WAL/materializer became ready")
	}
	if _, _, err := n.RetainedRequest(context.Background(), "kv", "\xff"); err == nil {
		t.Fatal("unbound runtime observed a retained request")
	}
	_, err := n.ResolveRequest(context.Background(), "kv", "\xff")
	var unknown *network.CommitUnknownError
	if !errors.As(err, &unknown) || unknown.RequestID != "\xff" || unknown.Slot != 0 {
		t.Fatalf("missing runtime must preserve exact selector as UNKNOWN, not success: %v", err)
	}
}

func TestInternalApplicationShutdownDrainsBorrowedResources(t *testing.T) {
	// This private lifetime test uses actual WAL/SQLite objects but does not
	// claim consensus, bootstrap permission or successful application admission.
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.BindIdentity([]byte("01234567890123456789012345678901")); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	m, err := materializer.OpenManagedSQLKVWithOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	n := New(&types.ExecutionConfig{})
	n.managedStartup = &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedRequestMaterializationPolicy}
	n.wal, n.material = wal, m
	n.opened.Store(true)
	n.ready.Store(true)
	n.managedPeerVerified.Store(true)
	lifetime, cancel := context.WithCancel(context.Background())
	n.internalApplication.Store(&internalApplicationBinding{lifetime: lifetime, cancel: cancel, walStore: wal, material: m})
	_, operation, release, err := n.borrowInternalApplication(context.Background())
	if err != nil {
		_ = n.Shutdown()
		t.Fatal(err)
	}
	var once sync.Once
	finish := func() { once.Do(release) }
	defer finish()
	done := make(chan error, 1)
	go func() { done <- n.Shutdown() }()
	deadline, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	select {
	case <-operation.Done():
	case <-deadline.Done():
		t.Fatal("shutdown did not cancel the borrowed operation")
	}
	select {
	case err := <-done:
		t.Fatalf("shutdown closed resources before borrower release: %v", err)
	default:
	}
	if _, ok := wal.Identity(); !ok {
		t.Fatal("borrowed WAL identity disappeared")
	}
	if err := m.Health(context.Background()); err != nil {
		t.Fatal("borrowed SQLite was closed before release", err)
	}
	if _, _, _, err := n.borrowInternalApplication(context.Background()); err == nil {
		t.Fatal("closing runtime accepted a new borrower")
	}
	// Exercise every new normal ingress while shutdown is deterministically
	// held before closing the real objects. This is also a race-detector target.
	checks := []func() bool{
		func() bool { return !n.Ready() },
		func() bool { return !n.managedLearnReady() },
		func() bool { return n.CheckManagedLocalRead(context.Background()) != nil },
		func() bool { return n.EnsureInitialManagedAdmission(context.Background()) != nil },
		func() bool { _, _, err := n.RetainedRequest(context.Background(), "kv", "id"); return err != nil },
		func() bool { _, err := n.ResolveRequest(context.Background(), "kv", "id"); return err != nil },
		func() bool {
			_, err := n.QueryManagedSQL(context.Background(), network.QueryRequest{SQL: "SELECT 1", Consistency: "local"})
			return err != nil
		},
		func() bool {
			_, err := n.ExecuteManagedSQL(context.Background(), network.ExecuteRequest{RequestID: "sql", SQL: "CREATE TABLE forbidden(id INTEGER)"})
			return err != nil
		},
		func() bool {
			_, err := n.KVGetManaged(context.Background(), network.KVGetRequest{Key: "key", Consistency: "local"})
			return err != nil
		},
		func() bool {
			_, err := n.ExecuteManagedKV(context.Background(), "put", network.KVMutationRequest{RequestID: "kv", Key: "key"})
			return err != nil
		},
		func() bool {
			_, err := n.RequestStatusManaged(context.Background(), network.RequestStatusRequest{Kind: "kv", RequestID: "id"})
			return err != nil
		},
	}
	results := make(chan bool, len(checks))
	for _, check := range checks {
		go func() { results <- check() }()
	}
	for range checks {
		select {
		case ok := <-results:
			if !ok {
				t.Error("closing normal ingress unexpectedly succeeded")
			}
		case <-deadline.Done():
			t.Fatal("closing ingress did not complete")
		}
	}
	finish()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-deadline.Done():
		t.Fatal("shutdown failed to drain after borrower release")
	}
	if n.wal != nil || n.material != nil {
		t.Fatal("shutdown left owned runtime objects installed")
	}
	if err := m.Health(context.Background()); err == nil {
		t.Fatal("SQLite remained open after shutdown completed")
	}
}

func TestInternalApplicationCancelledAndInvalidSelector(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var n *Node
	if _, err := n.checkInternalApplicationRuntime(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// This helper recognizes canonical selector syntax, not profile permission.
	for kind, want := range map[string]types.MutationKind{"sql": types.MutationSQL, "kv": types.MutationKV, "graph": types.MutationGraph} {
		if got, err := retainedRequestKind(kind); err != nil || got != want {
			t.Fatalf("canonical original kind changed: %q %v", kind, err)
		}
	}
	for _, kind := range []string{"", "notify", "SQL", "Graph"} {
		if _, err := retainedRequestKind(kind); !errors.Is(err, network.ErrInvalidRequest) {
			t.Fatalf("invalid original kind accepted: %q", kind)
		}
	}
	for _, graphProfile := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "sqlite.db")
		var m *materializer.Materializer
		var err error
		if graphProfile {
			m, err = materializer.OpenManagedSQLKVGraphWithOriginals(path, 1)
		} else {
			m, err = materializer.OpenManagedSQLKVWithOriginals(path, 1)
		}
		if err != nil {
			t.Fatal(err)
		}
		_, found, lookupErr := m.RetainedRequestOriginal(context.Background(), types.MutationGraph, "original")
		closeErr := m.Close()
		if closeErr != nil || found || graphProfile && lookupErr != nil || !graphProfile && lookupErr == nil {
			t.Fatalf("profile-aware Graph original lookup changed: graph=%t found=%t lookup=%v close=%v", graphProfile, found, lookupErr, closeErr)
		}
	}
}
