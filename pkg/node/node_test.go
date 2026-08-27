package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestOperationSyncPeerPermutationIsDeterministicAndBalanced(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	first, ok := syncSource("n1", members, 0)
	if !ok {
		t.Fatal("no sync peer")
	}
	second, _ := syncSource("n1", members, 1)
	again, _ := syncSource("n1", members, 0)
	if first != again || first == second || first == "n1" || second == "n1" {
		t.Fatalf("sources=%s,%s again=%s", first, second, again)
	}
	for round := uint64(0); round < 32; round++ {
		delay := syncInterval("n1", round)
		if delay < 900*time.Millisecond || delay > 1100*time.Millisecond {
			t.Fatalf("round %d delay=%v", round, delay)
		}
	}
}

func TestTransientCatchUpFailureKeepsReadyNodeServing(t *testing.T) {
	n := &Node{}
	n.ready.Store(true)
	n.observeCatchUp(errors.New("temporary peer timeout"))
	if !n.ready.Load() {
		t.Fatal("transient catch-up failure cleared readiness")
	}
}

func TestMultiNodeFilesystemObjectStoreFailsClosed(t *testing.T) {
	for name, configure := range map[string]func(*types.ExecutionConfig){
		"filesystem": func(config *types.ExecutionConfig) {
			config.ObjStoreProvider = "filesystem"
			config.ObjStoreDir = t.TempDir()
		},
		"implicit filesystem directory": func(config *types.ExecutionConfig) { config.ObjStoreDir = t.TempDir() },
		"S3 without bucket":             func(config *types.ExecutionConfig) { config.ObjStoreProvider = "s3" },
	} {
		t.Run(name, func(t *testing.T) {
			dataDir := filepath.Join(t.TempDir(), "data")
			config := &types.ExecutionConfig{
				NodeID: "n1", Profile: materializer.BuildProfile(), DataDir: dataDir,
				Members: []types.NodeConfig{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}},
			}
			configure(config)
			err := New(config).Open(context.Background())
			if err == nil || (!strings.Contains(err.Error(), "shared S3-compatible") && !strings.Contains(err.Error(), "S3 bucket is required")) {
				t.Fatalf("error=%v, want object-store rejection", err)
			}
			if _, statErr := os.Stat(filepath.Join(dataDir, "qlog")); !os.IsNotExist(statErr) {
				t.Fatalf("invalid config created qlog: %v", statErr)
			}
		})
	}
}

func TestObjectStoreConfigMatrix(t *testing.T) {
	members := []types.NodeConfig{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	for _, test := range []struct {
		name    string
		members []types.NodeConfig
		apply   func(*types.ExecutionConfig)
		wantErr bool
	}{
		{name: "single filesystem", members: members[:1], apply: func(config *types.ExecutionConfig) {
			config.ObjStoreProvider, config.ObjStoreDir = "filesystem", t.TempDir()
		}},
		{name: "multi filesystem", members: members, apply: func(config *types.ExecutionConfig) {
			config.ObjStoreProvider, config.ObjStoreDir = "filesystem", t.TempDir()
		}, wantErr: true},
		{name: "multi implicit directory", members: members, apply: func(config *types.ExecutionConfig) { config.ObjStoreDir = t.TempDir() }, wantErr: true},
		{name: "multi S3", members: members, apply: func(config *types.ExecutionConfig) { config.ObjStoreProvider, config.ObjStoreBucket = "s3", "rhiza" }},
		{name: "multi S3 without bucket", members: members, apply: func(config *types.ExecutionConfig) { config.ObjStoreProvider = "s3" }, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := &types.ExecutionConfig{Members: test.members}
			test.apply(config)
			_, err := validateObjectStoreConfig(config)
			if (err != nil) != test.wantErr {
				t.Fatalf("error=%v wantErr=%v", err, test.wantErr)
			}
		})
	}
}
