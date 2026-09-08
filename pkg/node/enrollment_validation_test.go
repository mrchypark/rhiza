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
		"duplicate member": {{ID: "a", Token: "a-token"}, {ID: "a", Token: "other-token"}},
		"local absent":     {{ID: "b", Token: "b-token"}, {ID: "c", Token: "c-token"}},
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
