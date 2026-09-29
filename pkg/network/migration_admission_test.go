package network

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestMigrationAdmissionRejectsSubstitutionReleaseAndWrongServer(t *testing.T) {
	first := NewServer(nil, nil, "cluster", true, nil)
	defer first.Close()
	second := NewServer(nil, nil, "cluster", true, nil)
	defer second.Close()

	statements := []types.SQLStatement{{SQL: "CREATE TABLE migration_admission (id INTEGER)"}}
	admission, err := first.AdmitMigration(context.Background(), 1, "admission", statements)
	if err != nil {
		t.Fatal(err)
	}
	req := MigrationRequest{RequestID: strings.Repeat("a", 64), Version: 1, Name: "admission", Checksum: strings.Repeat("a", 64), Statements: statements}

	larger := MigrationRequest{RequestID: req.RequestID, Version: req.Version, Name: req.Name, Checksum: req.Checksum, Statements: []types.SQLStatement{{SQL: strings.Repeat("x", 2<<10)}}}
	if _, err := first.MigrateAdmitted(context.Background(), larger, admission); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("larger substituted request error=%v", err)
	}
	if _, err := second.MigrateAdmitted(context.Background(), req, admission); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("wrong-server reservation error=%v", err)
	}
	if got := first.admittedMutationCount(); got != 1 {
		t.Fatalf("reservation released by rejected use: count=%d", got)
	}
	admission.Release()
	if got := first.admittedMutationCount(); got != 0 {
		t.Fatalf("released reservation leaked: count=%d", got)
	}
	if _, err := first.MigrateAdmitted(context.Background(), req, admission); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("released reservation reuse error=%v", err)
	}
}

func TestMigrationAdmissionIsSingleUseDuringConcurrentSubmission(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/state.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil)
	defer server.Close()

	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	server.SetDurabilityBarrier(func(context.Context, quepaxa.Slot) error {
		close(entered)
		<-release
		return nil
	})

	statements := []types.SQLStatement{{SQL: "CREATE TABLE migration_admission_once (id INTEGER)"}}
	admission, err := server.AdmitMigration(context.Background(), 1, "once", statements)
	if err != nil {
		t.Fatal(err)
	}
	req := MigrationRequest{RequestID: strings.Repeat("b", 64), Version: 1, Name: "once", Checksum: strings.Repeat("b", 64), Statements: statements}
	done := make(chan error, 1)
	go func() {
		_, err := server.MigrateAdmitted(context.Background(), req, admission)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("migration did not reach delayed proposal completion")
	}
	if _, err := server.MigrateAdmitted(context.Background(), req, admission); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("concurrent reservation reuse error=%v", err)
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("migration did not complete")
	}
	if _, err := server.MigrateAdmitted(context.Background(), req, admission); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("completed reservation reuse error=%v", err)
	}
	if got := server.admittedMutationCount(); got != 0 {
		t.Fatalf("completed reservation leaked: count=%d", got)
	}
}

func TestMutationLeaseTransferFailsClosed(t *testing.T) {
	lease := &mutationLease{release: func() {}}
	if !lease.transfer() {
		t.Fatal("first ownership transfer failed")
	}
	if lease.transfer() {
		t.Fatal("second ownership transfer succeeded")
	}
	lease.releaseWorker()
	if lease.transfer() {
		t.Fatal("transfer succeeded after release")
	}
}

func (s *Server) admittedMutationCount() int {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	return s.mutationAdmission.count
}
