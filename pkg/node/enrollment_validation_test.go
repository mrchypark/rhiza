package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
)

func TestOfflineEnrollmentRejectsInvalidMembershipBeforeRegistration(t *testing.T) {
	for name, members := range map[string][]types.NodeConfig{
		"duplicate member": {{ID: "a"}, {ID: "a"}},
		"local absent":     {{ID: "b"}, {ID: "c"}},
	} {
		t.Run(name, func(t *testing.T) {
			dataDir := t.TempDir()
			config := &types.ExecutionConfig{
				ClusterID: "cluster", NodeID: "a", DataDir: dataDir,
				ObjStoreProvider: "s3", ObjStoreBucket: "bucket", Members: members,
			}
			if err := New(config).EnrollExistingVoter(context.Background()); err == nil {
				t.Fatal("offline enrollment accepted invalid membership")
			}
			if _, err := os.Stat(filepath.Join(dataDir, "qlog")); !os.IsNotExist(err) {
				t.Fatalf("invalid enrollment reached WAL or registration setup: %v", err)
			}
		})
	}
}
