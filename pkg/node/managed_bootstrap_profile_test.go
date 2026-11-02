package node

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
)

func TestManagedBootstrapProfileRefusesActivationBeforeEffects(t *testing.T) {
	for _, protocol := range []string{types.ManagedApplicationProtocolSQLKV, "unknown"} {
		dir := t.TempDir()
		n := New(&types.ExecutionConfig{ClusterID: "cluster", NodeID: "member", DataDir: dir, PeerAddr: "127.0.0.1:0"})
		checker := &managedTestChecker{}
		err := n.OpenManaged(context.Background(), ManagedStartupConfig{Checker: checker, Identity: ManagedStartupIdentity{ApplicationProtocol: protocol}})
		if err == nil {
			t.Fatalf("activation guard: %v", err)
		}
		files, err := os.ReadDir(dir)
		if err != nil || len(files) != 0 || n.opened.Load() || n.material != nil || n.wal != nil || n.peer != nil {
			t.Fatalf("rejected profile created runtime effects: %v", err)
		}
	}
	b, err := json.Marshal(ManagedStartupIdentity{})
	if err != nil || strings.Contains(string(b), "ApplicationProtocol") {
		t.Fatalf("legacy identity serialization changed: %s %v", b, err)
	}
}
