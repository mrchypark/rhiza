package node

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestEngineRepairPreflightGenesisWithoutLocalRegistrationRequestsAuthorityInitialEnrollment(t *testing.T) {
	ctx := context.Background()
	config := voterTestConfig(t)
	config.PeerToken = "a-token"
	config.ObjStoreProvider = string(objmetrics.ProviderFilesystem)
	config.ObjStoreDir = filepath.Join(t.TempDir(), "archive")
	config.ObjStorePrefix = "rhiza-root"
	config.EnableReconfiguration = true
	config.EngineOwnedRemovals = true
	physical := fencing.Identity{
		SourceClusterID: string(config.ClusterID),
		NodeID:          string(config.NodeID),
		PodName:         "rhiza-0",
		PodUID:          "initial-pod-uid",
		RunID:           "initial-run",
	}

	bucket, err := objmetrics.NewBucketForSupervisedStartup(ctx, objmetrics.Config{
		Provider:      objmetrics.ProviderFilesystem,
		FilesystemDir: config.ObjStoreDir,
		Prefix:        config.ObjStorePrefix,
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
		t.Fatalf("seed canonical genesis only: %v", err)
	}

	// This is the initial-registration race state: canonical genesis exists,
	// but this voter's immutable registration and all local WAL/identity state
	// are absent. Preflight classifies the state without creating candidate
	// or voter files; only the authority can admit the first registration.
	local, err := loadVoterIdentity(config)
	if err != nil || local.identity != nil || local.hadWAL {
		t.Fatalf("fixture is not locally empty: state=%+v err=%v", local, err)
	}
	result, err := EngineRepairPreflight(config)(ctx, physical)
	if err != nil || !result.InitialRegistration || result.PendingEngineRepair != nil {
		t.Fatalf("genesis-only preflight did not classify first registration: result=%+v err=%v", result, err)
	}
	if state, stateErr := loadVoterIdentity(config); stateErr != nil || state.identity != nil || state.hadWAL {
		t.Fatalf("preflight mutated local initial state: state=%+v err=%v", state, stateErr)
	}
	if registration, readErr := readVoterRegistration(ctx, bucket, voterIdentityRegistrationKey(config, string(config.NodeID))); readErr != nil || registration != nil {
		t.Fatalf("preflight mutated remote voter registration: present=%t err=%v", registration != nil, readErr)
	}
	// The ordinary Node identity helper accepts the same genesis-only state;
	// this preflight merely requests an authority check and never writes it.
	if err := registerTestVoter(t, config, bucket, false); err != nil {
		t.Fatalf("initial voter registration rejected canonical genesis: %v", err)
	}
	registered, err := loadVoterIdentity(config)
	if err != nil || registered.identity == nil || !registered.identity.Registered || !registered.hadWAL {
		t.Fatalf("initial registration did not establish local identity and WAL: state=%+v err=%v", registered, err)
	}
	remote, err := objmetrics.NewBucketForSupervisedStartup(ctx, objmetrics.Config{
		Provider:      objmetrics.ProviderFilesystem,
		FilesystemDir: config.ObjStoreDir,
		Prefix:        config.ObjStorePrefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	if registration, err := readVoterRegistration(ctx, remote, voterIdentityRegistrationKey(config, string(config.NodeID))); err != nil || registration == nil {
		t.Fatalf("initial registration did not publish immutable voter identity: present=%t err=%v", registration != nil, err)
	}
	if len(config.Members) < 2 {
		t.Fatal("test config no longer exercises an initial configured voter")
	}
	if _, ok := findSlotMember(config.Members, config.NodeID); !ok {
		t.Fatal("test config no longer includes the initial local voter")
	}
}
