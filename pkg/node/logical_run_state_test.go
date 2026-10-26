package node

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestLogicalRunStatePersistsSamePodIdentityAndRejectsNewPodReuse(t *testing.T) {
	config := &types.ExecutionConfig{ClusterID: "cluster", NodeID: "node-a", DataDir: t.TempDir()}
	first, continuation, err := loadOrCreateLogicalRunState(config, "pod-uid-a", localVoterState{})
	if err != nil || continuation {
		t.Fatalf("first run state = %+v, continuation=%t, err=%v", first, continuation, err)
	}
	second, continuation, err := loadOrCreateLogicalRunState(config, "pod-uid-a", localVoterState{})
	if err != nil || continuation || second != first {
		t.Fatalf("same-Pod run state = %+v, continuation=%t, err=%v; want exact %+v", second, continuation, err, first)
	}
	if _, _, err := loadOrCreateLogicalRunState(config, "pod-uid-b", localVoterState{}); err == nil {
		t.Fatal("different PodUID reused the prior local logical RunID")
	}
}

func TestHAStageFreshnessAllowsOnlyExactPreWALSamePodRun(t *testing.T) {
	config := &types.ExecutionConfig{ClusterID: "cluster", NodeID: "node-a", DataDir: t.TempDir()}
	if err := os.Mkdir(filepath.Join(config.DataDir, "qlog"), 0o700); err != nil {
		t.Fatal(err)
	}
	run, continuation, err := loadOrCreateLogicalRunState(config, "pod-uid-a", localVoterState{})
	if err != nil || continuation {
		t.Fatalf("create pending Run: state=%+v continuation=%t err=%v", run, continuation, err)
	}
	if err := validateHAStageFreshness(config.DataDir, "cluster", "node-a", "pod-uid-a", false, localVoterState{}); err == nil {
		t.Fatal("pending logical Run was accepted without logical-authority mode")
	}
	if err := validateHAStageFreshness(config.DataDir, "cluster", "node-a", "pod-uid-a", true, localVoterState{}); err != nil {
		t.Fatalf("exact same-Pod no-WAL pending Run was rejected: %v", err)
	}
	got, err := readLogicalRunState(filepath.Join(config.DataDir, "logical-run.json"))
	if err != nil || got != run {
		t.Fatalf("freshness validation changed or lost RunID: got=%+v want=%+v err=%v", got, run, err)
	}
	if err := validateHAStageFreshness(config.DataDir, "cluster", "node-a", "pod-uid-b", true, localVoterState{}); err == nil {
		t.Fatal("another PodUID inherited the pending Run")
	}
	if err := validateHAStageFreshness(config.DataDir, "cluster", "node-a", "pod-uid-a", true, localVoterState{hadWAL: true}); err == nil {
		t.Fatal("pending Run was accepted despite an existing WAL")
	}
	if err := validateHAStageFreshness(config.DataDir, "foreign-cluster", "node-a", "pod-uid-a", true, localVoterState{}); err == nil {
		t.Fatal("pending Run from a different source cluster was accepted")
	}
}

func TestHAStageFreshnessRejectsMalformedOrForeignPreWALFiles(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{name: "malformed run", mutate: func(t *testing.T, dir string) {
			if err := os.WriteFile(filepath.Join(dir, "logical-run.json"), []byte("{}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "foreign file", mutate: func(t *testing.T, dir string) {
			if err := os.WriteFile(filepath.Join(dir, "foreign.json"), []byte("{}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Mkdir(filepath.Join(dir, "qlog"), 0o700); err != nil {
				t.Fatal(err)
			}
			if tc.name == "foreign file" {
				config := &types.ExecutionConfig{ClusterID: "cluster", NodeID: "node-a", DataDir: dir}
				if _, _, err := loadOrCreateLogicalRunState(config, "pod-uid-a", localVoterState{}); err != nil {
					t.Fatal(err)
				}
			}
			tc.mutate(t, dir)
			if err := validateHAStageFreshness(dir, "cluster", "node-a", "pod-uid-a", true, localVoterState{}); err == nil {
				t.Fatal("invalid pre-WAL DataDir state was accepted")
			}
		})
	}
}

func TestLogicalRunStatePersistsAuthorityAssignmentForSamePodContinuation(t *testing.T) {
	config := &types.ExecutionConfig{ClusterID: "cluster", NodeID: "physical", DataDir: t.TempDir()}
	state, _, err := loadOrCreateLogicalRunState(config, "pod-uid", localVoterState{})
	if err != nil {
		t.Fatal(err)
	}
	identity := fencing.Identity{SourceClusterID: "cluster", NodeID: "logical-candidate", PodUID: "pod-uid", RunID: state.RunID, AssignmentID: strings.Repeat("a", 64)}
	updated, err := persistLogicalRunAssignment(config, state, identity)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := readLogicalRunState(filepath.Join(config.DataDir, "logical-run.json"))
	if err != nil || loaded != updated || loaded.AssignedNodeID != identity.NodeID || loaded.AssignmentID != identity.AssignmentID {
		t.Fatalf("persisted logical assignment=%+v err=%v; want %+v", loaded, err, updated)
	}
	local := localVoterState{identity: &voterIdentity{Cluster: "cluster", Node: identity.NodeID, Registered: false}, hadWAL: true}
	resume, continuation, err := loadOrCreateLogicalRunState(config, "pod-uid", local)
	if err != nil || !continuation || resume != loaded {
		t.Fatalf("same-Pod assigned continuation=%+v continuation=%t err=%v; want %+v", resume, continuation, err, loaded)
	}
}

func TestLogicalRunStateRequiresPersistedIdentityForExistingWAL(t *testing.T) {
	config := &types.ExecutionConfig{ClusterID: "cluster", NodeID: "node-a", DataDir: t.TempDir()}
	local := localVoterState{identity: &voterIdentity{Cluster: "cluster", Node: "node-a", Registered: true}}
	if _, _, err := loadOrCreateLogicalRunState(config, "pod-uid", local); !errors.Is(err, ErrVoterStateLost) {
		t.Fatalf("registered WAL without logical RunID error=%v, want ErrVoterStateLost", err)
	}
	if _, _, err := loadOrCreateLogicalRunState(config, "pod-uid", localVoterState{hadWAL: true}); !errors.Is(err, ErrVoterStateLost) {
		t.Fatalf("WAL without logical RunID error=%v, want ErrVoterStateLost", err)
	}
}

func TestReadLogicalRunStateRejectsMalformedAndNonRegularFiles(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "logical-run.json")
	if err := os.WriteFile(file, []byte(`{"version":1,"clusterID":"cluster","nodeID":"node-a","podUID":"pod","runID":"not-a-run-id"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readLogicalRunState(file); err == nil {
		t.Fatal("malformed persisted logical RunID was accepted")
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(file, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := readLogicalRunState(file); err == nil {
		t.Fatal("non-regular persisted logical Run state was accepted")
	}
}
