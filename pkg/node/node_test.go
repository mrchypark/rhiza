package node

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
)

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
