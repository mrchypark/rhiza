package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestCompactedPeerDoesNotOverrideUsableQuorumSuffix(t *testing.T) {
	applied := quepaxa.Slot(10)
	best := &network.DecisionsResponse{Tip: 11}
	if !hasUsablePeerSuffix(applied, 2, 2, best) {
		t.Fatal("usable quorum suffix was rejected")
	}
	best.Tip = applied
	if hasUsablePeerSuffix(applied, 2, 2, best) {
		t.Fatal("stale peer response was accepted as a suffix")
	}
	best.Tip = applied + 1
	if hasUsablePeerSuffix(applied, 2, 3, best) {
		t.Fatal("non-quorum suffix was accepted")
	}
}

func TestOperationSyncPeerPermutationIsDeterministicAndBalanced(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, {ID: "n3"}}
	first := syncSources("n1", members, 0)
	if len(first) != 2 {
		t.Fatal("no sync peer")
	}
	second := syncSources("n1", members, 1)
	again := syncSources("n1", members, 0)
	if !slices.Equal(first, again) || first[0] == second[0] || first[0] == "n1" || second[0] == "n1" || first[1] != second[0] {
		t.Fatalf("sources=%v,%v again=%v", first, second, again)
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

func TestRestoreArchiveCatchUpMarksNodeNotReadyBeforeValidation(t *testing.T) {
	n := &Node{}
	n.ready.Store(true)
	if err := n.restoreArchiveCatchUp(context.Background()); err == nil {
		t.Fatal("restore unexpectedly succeeded")
	}
	if n.ready.Load() {
		t.Fatal("restore left node ready before recovery could be validated")
	}
}

func TestCompactedWakeMarksNodeNotReadyAndCoalesces(t *testing.T) {
	n := &Node{catchUpWake: make(chan struct{}, 1)}
	n.ready.Store(true)
	n.wakeCatchUp()
	n.wakeCatchUp()
	if n.ready.Load() {
		t.Fatal("compacted foreground path left node ready")
	}
	select {
	case <-n.catchUpWake:
	default:
		t.Fatal("compacted foreground path did not wake catch-up worker")
	}
	select {
	case <-n.catchUpWake:
		t.Fatal("compacted foreground wake was not coalesced")
	default:
	}
}

func TestArchiveRecoveryRequiredBelowTrimmedBase(t *testing.T) {
	if !archiveRecoveryRequired(148, 149) {
		t.Fatal("lagging local tip did not require checkpoint recovery")
	}
	if archiveRecoveryRequired(149, 149) || archiveRecoveryRequired(150, 149) || archiveRecoveryRequired(0, 0) {
		t.Fatal("retained archive suffix incorrectly required checkpoint recovery")
	}
}

func TestCheckpointGCWaitsForFirstArchiveBase(t *testing.T) {
	floor, ready := advanceArchiveFloor(0, 0, false)
	if ready || floor != 0 {
		t.Fatalf("no-base floor=(%d,%t), want GC blocked", floor, ready)
	}
	floor, ready = advanceArchiveFloor(floor, 7, true)
	if !ready || floor != 7 {
		t.Fatalf("first-base floor=(%d,%t), want (7,true)", floor, ready)
	}
	floor, ready = advanceArchiveFloor(floor, 5, true)
	if !ready || floor != 7 {
		t.Fatalf("stale-base floor=(%d,%t), want monotonic 7", floor, ready)
	}
}

func TestCertifiedCheckpointCompactionIsSingleFlight(t *testing.T) {
	n := &Node{}
	n.compactionMu.Lock()
	done := make(chan error, 1)
	go func() { done <- n.compactCertifiedCheckpoint(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("compaction entered while another transition held the sequence lock: %v", err)
	default:
	}
	n.compactionMu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("compaction did not resume after sequence lock release")
	}
}

func TestMultiNodeFilesystemObjectStoreFailsClosed(t *testing.T) {
	for name, configure := range map[string]func(*types.ExecutionConfig){
		"missing": func(*types.ExecutionConfig) {},
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
				NodeID: "n1", DataDir: dataDir,
				Members: []types.NodeConfig{{ID: "n1", Token: "token-1"}, {ID: "n2", Token: "token-2"}, {ID: "n3", Token: "token-3"}},
			}
			configure(config)
			err := New(config).Open(context.Background())
			if err == nil || (!strings.Contains(err.Error(), "shared object storage") && !strings.Contains(err.Error(), "object-store bucket is required")) {
				t.Fatalf("error=%v, want object-store rejection", err)
			}
			if _, statErr := os.Stat(filepath.Join(dataDir, "qlog")); !os.IsNotExist(statErr) {
				t.Fatalf("invalid config created qlog: %v", statErr)
			}
		})
	}
}

func TestMultiNodeVoterTokensAreDistinctFromAdmin(t *testing.T) {
	for name, config := range map[string]*types.ExecutionConfig{
		"missing voter token": {NodeID: "n1", Members: []types.NodeConfig{{ID: "n1"}, {ID: "n2", Token: "voter-2"}}},
		"admin token reused":  {NodeID: "n1", AdminToken: "shared", Members: []types.NodeConfig{{ID: "n1", Token: "shared"}, {ID: "n2", Token: "voter-2"}}},
	} {
		t.Run(name, func(t *testing.T) {
			config.DataDir = filepath.Join(t.TempDir(), "data")
			if err := New(config).Open(context.Background()); err == nil {
				t.Fatal("invalid peer credentials were accepted")
			}
			if _, err := os.Stat(filepath.Join(config.DataDir, "qlog")); !os.IsNotExist(err) {
				t.Fatalf("invalid credentials created state: %v", err)
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
		{name: "multi GCS", members: members, apply: func(config *types.ExecutionConfig) { config.ObjStoreProvider, config.ObjStoreBucket = "gcs", "rhiza" }},
		{name: "multi Azure", members: members, apply: func(config *types.ExecutionConfig) {
			config.ObjStoreProvider, config.ObjStoreBucket, config.ObjStoreAzureStorageAccount = "azure", "rhiza", "account"
		}},
		{name: "multi Azure without account", members: members, apply: func(config *types.ExecutionConfig) {
			config.ObjStoreProvider, config.ObjStoreBucket = "azure", "rhiza"
		}, wantErr: true},
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

func TestReadAdmissionConfig(t *testing.T) {
	for name, config := range map[string]*types.ExecutionConfig{
		"defaults":                {},
		"configured":              {MaxConcurrentReads: 4, MaxLongPollReads: 1},
		"long poll disabled":      {MaxConcurrentReads: 1},
		"negative total":          {MaxConcurrentReads: -1},
		"negative long poll":      {MaxConcurrentReads: 1, MaxLongPollReads: -1},
		"missing total":           {MaxLongPollReads: 1},
		"long poll exceeds total": {MaxConcurrentReads: 1, MaxLongPollReads: 2},
	} {
		t.Run(name, func(t *testing.T) {
			err := validateReadAdmissionConfig(config)
			valid := name == "defaults" || name == "configured" || name == "long poll disabled"
			if (err == nil) != valid {
				t.Fatalf("error=%v valid=%t", err, valid)
			}
		})
	}
}
