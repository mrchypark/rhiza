package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestInitialRegistrationRequiresPristineDataDirectory(t *testing.T) {
	for _, name := range []string{"sqlite.db", "sqlite.db-wal", "local-store.json", "latticedb", "qlog-symlink", "empty-qlog"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			var err error
			switch name {
			case "qlog-symlink":
				err = os.Symlink(t.TempDir(), filepath.Join(dir, "qlog"))
			case "empty-qlog":
				err = os.Mkdir(filepath.Join(dir, "qlog"), 0700)
			case "latticedb":
				err = os.Mkdir(filepath.Join(dir, name), 0700)
			default:
				err = os.WriteFile(filepath.Join(dir, name), []byte("existing state"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			pristine, err := initialVoterLocalStateIsPristine(&types.ExecutionConfig{DataDir: dir}, localVoterState{})
			if err != nil || pristine != (name == "empty-qlog") {
				t.Fatalf("pristine=%t err=%v for %s", pristine, err, name)
			}
		})
	}
}

func TestEngineRepairPreflightRejectsUnrelatedLocalQlogEvidenceForInitialRegistration(t *testing.T) {
	ctx := context.Background()
	config := voterTestConfig(t)
	config.PeerToken = "initial-peer-token"
	config.ObjStoreProvider = string(objmetrics.ProviderFilesystem)
	config.ObjStoreDir = filepath.Join(t.TempDir(), "archive")
	config.ObjStorePrefix = "rhiza-root"
	config.EnableReconfiguration = true
	config.EngineOwnedRemovals = true
	physical := fencing.Identity{
		SourceClusterID: string(config.ClusterID), NodeID: string(config.NodeID),
		PodName: "rhiza-0", PodUID: "initial-pod", RunID: "initial-run",
	}

	bucket, err := objmetrics.NewBucketForSupervisedStartup(ctx, objmetrics.Config{
		Provider: objmetrics.ProviderFilesystem, FilesystemDir: config.ObjStoreDir, Prefix: config.ObjStorePrefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	genesis, err := json.Marshal(recovery.NewMembershipRecord(
		string(config.ClusterID), config.Members, string(config.ObjStoreDurability)))
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, voterMembershipRegistrationKey(config), bytes.NewReader(genesis), objstore.WithIfNotExists()); err != nil {
		t.Fatalf("seed canonical genesis: %v", err)
	}

	qlogDir := filepath.Join(config.DataDir, "qlog")
	if err := os.MkdirAll(qlogDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(qlogDir, "partial-local-state"), []byte("not a WAL identity"), 0600); err != nil {
		t.Fatal(err)
	}
	result, err := EngineRepairPreflight(config)(ctx, physical)
	if !errors.Is(err, ErrVoterStateLost) || result.InitialRegistration || result.PendingEngineRepair != nil {
		t.Fatalf("unrelated local evidence was treated as pristine first registration: result=%+v err=%v", result, err)
	}
	if registration, err := readVoterRegistration(ctx, bucket, voterIdentityRegistrationKey(config, string(config.NodeID))); err != nil || registration != nil {
		t.Fatalf("rejected preflight mutated immutable registration: present=%t err=%v", registration != nil, err)
	}
}
