package node

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
)

func TestManagedBootstrapProfileRefusesActivationBeforeEffects(t *testing.T) {
	for _, test := range []struct {
		protocol string
		wantErr  string
	}{
		{types.ManagedApplicationProtocolSQLKV, ""},
		{"unknown", ""},
		{sqlpolicy.ManagedRequestApplicationProtocol, "unsupported managed application protocol"},
		{sqlpolicy.ManagedGraphRequestApplicationProtocol, "unsupported managed application protocol"},
	} {
		dir := t.TempDir()
		n := New(&types.ExecutionConfig{ClusterID: "cluster", NodeID: "member", DataDir: dir, PeerAddr: "127.0.0.1:0"})
		checker := &managedTestChecker{}
		err := n.OpenManaged(context.Background(), ManagedStartupConfig{Checker: checker, Identity: ManagedStartupIdentity{ApplicationProtocol: test.protocol}})
		if err == nil || test.wantErr != "" && !strings.Contains(err.Error(), test.wantErr) {
			t.Fatalf("activation guard protocol=%q error=%v, want error containing %q", test.protocol, err, test.wantErr)
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
