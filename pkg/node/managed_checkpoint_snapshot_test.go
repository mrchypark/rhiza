package node

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
)

// Reuse actual filesystem birth/registration, three certified voters, SQLite
// materialization and the fsync/archive durability barrier. The fixture holds
// maintenance so the automatic publisher cannot race this explicit snapshot.
func TestManagedCheckpointOwnedSnapshotValidation(t *testing.T) {
	n, _, _ := newKVWrapperFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	receipt, err := n.ExecuteManagedKV(ctx, "put", network.KVMutationRequest{RequestID: "checkpoint-owned-snapshot", Key: "key", Value: []byte("certified")})
	if err != nil || receipt.Status != types.MutationCommitted || receipt.Slot == 0 {
		t.Fatalf("actual durable source mutation: receipt=%+v error=%v", receipt, err)
	}
	files, cut, cleanup, err := n.material.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	if len(files) != 1 || files[0].Role != materializer.CheckpointSQLite || cut < receipt.Slot || filepath.Dir(files[0].Path) != n.config.DataDir || !strings.HasPrefix(filepath.Base(files[0].Path), ".rhiza-checkpoint-sqlite-") {
		t.Fatalf("fixture did not produce the real adjacent owned snapshot at the ACK cut: cut=%d", cut)
	}
	before, err := os.ReadFile(files[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	actual := []checkpoint.File{{Role: checkpoint.RoleSQLite, Path: files[0].Path}}
	t.Run("owned-snapshot", func(t *testing.T) {
		if err := n.validateCheckpointFiles(ctx, actual, cut); err != nil {
			t.Fatalf("publisher rejected its own completed profile-9 snapshot: %v", err)
		}
	})
	t.Run("wrong-tip", func(t *testing.T) {
		if err := n.validateCheckpointFiles(ctx, actual, cut+1); err == nil || !strings.Contains(err.Error(), "mixed checkpoint applied tip differs from certified base") {
			t.Fatalf("snapshot was not rejected at the exact tip boundary: %v", err)
		}
	})
	t.Run("wrong-window", func(t *testing.T) {
		window := uint64(1024)
		if window == n.receiptWindow() {
			window = 2048
		}
		validator := New(n.config)
		validator.managedStartup = &ManagedStartupConfig{Identity: n.managedStartup.Identity, expectedPolicy: sqlpolicy.ManagedRequestMaterializationPolicy, idempotencyWindow: window}
		if err := validator.validateCheckpointFiles(ctx, actual, cut); err == nil {
			t.Fatal("checkpoint from a different receipt window was accepted")
		}
	})
	if after, err := os.ReadFile(files[0].Path); err != nil || !bytes.Equal(after, before) {
		t.Fatalf("snapshot validation changed the source bytes: %v", err)
	}
	cleanup()
	if _, err := os.Stat(files[0].Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned snapshot cleanup failed: %v", err)
	}
	// Once no legitimate snapshot remains, a separate interrupted artifact must
	// still block the live startup inspector and retain the original evidence.
	interrupted := filepath.Join(n.config.DataDir, ".rhiza-restore-interrupted")
	sentinel := []byte("preserve interrupted recovery evidence")
	if err := os.WriteFile(interrupted, sentinel, 0600); err != nil {
		t.Fatal(err)
	}
	live := filepath.Join(n.config.DataDir, "sqlite.db")
	for name, inspect := range map[string]func() error{
		"startup-guard": func() error { return materializer.ValidateManagedRestoreState(live) },
		"startup-policy-inspector": func() error { _, _, err := n.inspectApplicationFile(ctx, live); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := inspect(); err == nil || !strings.Contains(err.Error(), "mixed materialization found interrupted restore/checkpoint artifact") {
				t.Fatalf("interrupted startup artifact was not rejected: %v", err)
			}
		})
	}
	if after, err := os.ReadFile(interrupted); err != nil || !bytes.Equal(after, sentinel) {
		t.Fatalf("startup validation removed or changed interrupted evidence: %v", err)
	}
}
