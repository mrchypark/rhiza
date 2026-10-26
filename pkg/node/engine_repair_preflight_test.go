package node

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestEngineRepairCandidatePreflightAcceptsFreshSupervisorRun(t *testing.T) {
	const (
		physical = "rhiza-2"
		podUID   = "pod-uid"
		oldNode  = "voter-2"
		newNode  = "recovered-ordinal-2"
		walID    = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	config := &types.ExecutionConfig{ClusterID: "cluster", NodeID: types.NodeID(oldNode), PeerToken: "fixed-peer-token",
		DataDir: t.TempDir(), Members: []quepaxa.Member{peerMember("cluster", "voter-1", "peer-1"), peerMember("cluster", oldNode, "fixed-peer-token")}}
	oldIdentity := voterIdentityFor(config)
	oldIdentity.Nonce, oldIdentity.Registered = strings.Repeat("a", 64), true
	member, ok := findSlotMember(config.Members, config.NodeID)
	if !ok {
		t.Fatal("source voter member missing")
	}
	member.ID = quepaxa.NodeID(newNode)
	member.WALIdentity = walID
	member.PublicKey = peerMember("cluster", member.ID, config.PeerToken).PublicKey
	assignedConfig := *config
	assignedConfig.NodeID = types.NodeID(newNode)
	assignedConfig.Learner = &member
	candidateIdentity := voterIdentityFor(&assignedConfig)
	candidateIdentity.Nonce, candidateIdentity.Registered = walID, true
	firstRun := fencing.Identity{SourceClusterID: "cluster", NodeID: oldNode, PodName: physical, PodUID: podUID, RunID: "first-run"}
	descriptor := engineRepairCandidateDescriptor{Bootstrap: firstRun, OldIdentity: oldIdentity,
		Candidate: fencing.Identity{SourceClusterID: "cluster", NodeID: newNode, PodName: physical, PodUID: podUID, RunID: firstRun.RunID, WALIdentity: walID},
		Member:    member}
	qlogDir := filepath.Join(config.DataDir, "qlog")
	if err := os.MkdirAll(qlogDir, 0700); err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(qlogDir)
	if err != nil {
		t.Fatal(err)
	}
	nonce, _ := hex.DecodeString(walID)
	if err := wal.BindIdentity(nonce); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	identityBytes, err := json.Marshal(candidateIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(qlogDir, "voter-identity.json"), identityBytes, 0600); err != nil {
		t.Fatal(err)
	}
	descriptorBytes, err := json.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(qlogDir, "engine-repair-candidate.json"), descriptorBytes, 0600); err != nil {
		t.Fatal(err)
	}
	local, err := loadVoterIdentity(config)
	if err != nil {
		t.Fatal(err)
	}
	current := firstRun
	current.RunID = "replacement-supervisor-run"
	got, ok, err := loadEngineRepairCandidateRestart(config, current, local)
	if err != nil || !ok || got.Candidate != descriptor.Candidate || got.OldIdentity != oldIdentity {
		t.Fatalf("fresh supervisor run did not preserve the exact candidate assignment: ok=%t descriptor=%+v err=%v", ok, got, err)
	}
}

func TestEngineRepairPreflightContinuesSavedAssignmentBeforeWALCreation(t *testing.T) {
	ctx := context.Background()
	config := voterTestConfig(t)
	config.PeerToken = "fixed-peer-token"
	config.ObjStoreProvider = string(objmetrics.ProviderFilesystem)
	config.ObjStoreDir = filepath.Join(t.TempDir(), "archive")
	config.ObjStorePrefix = "rhiza-root"
	config.EngineOwnedRemovals = true
	config.EnableReconfiguration = true
	for i := range config.Members {
		name := string(config.Members[i].ID)
		config.Members[i].URL = "http://" + name + ":8080"
		config.Members[i].PeerURL = "quic://" + name + ":9090"
		config.Members[i].LogURL = "http://" + name + ":8081"
	}
	bucket, err := objmetrics.NewBucketForSupervisedStartup(ctx, objmetrics.Config{
		Provider: objmetrics.ProviderFilesystem, FilesystemDir: config.ObjStoreDir, Prefix: config.ObjStorePrefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registerTestVoter(t, config, bucket, false); err != nil {
		_ = bucket.Close()
		t.Fatalf("register immutable source voter: %v", err)
	}
	if err := bucket.Close(); err != nil {
		t.Fatal(err)
	}
	source, exists, err := readRemoteVoterIdentity(ctx, config)
	if err != nil || !exists {
		t.Fatalf("read immutable source voter: exists=%t err=%v", exists, err)
	}
	config.DataDir = t.TempDir()
	oldMember, ok := findSlotMember(config.Members, config.NodeID)
	if !ok {
		t.Fatal("configured source voter missing")
	}
	const candidateNode = "recovered-ordinal-0"
	const assignedWAL = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	candidateMember := oldMember
	candidateMember.ID = quepaxa.NodeID(candidateNode)
	candidateMember.WALIdentity = assignedWAL
	candidateMember.PublicKey = peerMember(string(config.ClusterID), candidateMember.ID, config.PeerToken).PublicKey
	firstPhysical := fencing.Identity{SourceClusterID: string(config.ClusterID), NodeID: string(config.NodeID),
		PodName: "rhiza-0", PodUID: "stable-pod-uid", RunID: "initial-run"}
	descriptor := engineRepairCandidateDescriptor{Bootstrap: firstPhysical, OldIdentity: source,
		Candidate: fencing.Identity{SourceClusterID: string(config.ClusterID), NodeID: candidateNode,
			PodName: firstPhysical.PodName, PodUID: firstPhysical.PodUID, RunID: firstPhysical.RunID, WALIdentity: assignedWAL},
		Member: candidateMember}
	qlogDir := filepath.Join(config.DataDir, "qlog")
	if err := os.MkdirAll(qlogDir, 0700); err != nil {
		t.Fatal(err)
	}
	descriptorBytes, err := json.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(qlogDir, "engine-repair-candidate.json"), descriptorBytes, 0600); err != nil {
		t.Fatal(err)
	}
	state, err := loadVoterIdentity(config)
	if err != nil || state.identity != nil || state.hadWAL {
		t.Fatalf("fixture is not pre-WAL: state=%+v err=%v", state, err)
	}
	physicalRestart := firstPhysical
	physicalRestart.RunID = "replacement-run"
	assigned, err := loadUnboundEngineRepairCandidateRestart(ctx, config, physicalRestart, state, source)
	if err != nil || !assigned {
		t.Fatalf("saved assignment was not accepted for fresh physical RunID: assigned=%t err=%v", assigned, err)
	}
	result, err := EngineRepairPreflight(config)(ctx, physicalRestart)
	if err != nil || result.InitialRegistration || result.PendingEngineRepair != nil {
		t.Fatalf("pre-WAL retry created a new old-voter request: result=%+v err=%v", result, err)
	}
}

func TestEngineRepairPreflightReadsWriterMembershipKeyWhenLocalNoPVCStateIsAbsent(t *testing.T) {
	ctx := context.Background()
	config := voterTestConfig(t)
	config.PeerToken = "a-token"
	config.ObjStoreProvider = string(objmetrics.ProviderFilesystem)
	config.ObjStoreDir = filepath.Join(t.TempDir(), "archive")
	config.ObjStorePrefix = "rhiza-root"
	config.EngineOwnedRemovals = true
	config.EnableReconfiguration = true
	for i := range config.Members {
		name := string(config.Members[i].ID)
		config.Members[i].URL = "http://" + name + ":8080"
		config.Members[i].PeerURL = "quic://" + name + ":9090"
		config.Members[i].LogURL = "http://" + name + ":8081"
	}
	bucket, err := objmetrics.NewBucketForSupervisedStartup(ctx, objmetrics.Config{
		Provider: objmetrics.ProviderFilesystem, FilesystemDir: config.ObjStoreDir, Prefix: config.ObjStorePrefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registerTestVoter(t, config, bucket, false); err != nil {
		_ = bucket.Close()
		t.Fatalf("register initial voter with canonical writer: %v", err)
	}
	if err := bucket.Close(); err != nil {
		t.Fatal(err)
	}
	genesis, err := objmetrics.NewBucketForSupervisedStartup(ctx, objmetrics.Config{
		Provider: objmetrics.ProviderFilesystem, FilesystemDir: config.ObjStoreDir, Prefix: config.ObjStorePrefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := readVoterRegistration(ctx, genesis, voterMembershipRegistrationKey(config))
	_ = genesis.Close()
	if err != nil || data == nil {
		t.Fatalf("writer genesis at shared canonical key missing: present=%t err=%v", data != nil, err)
	}

	firstIdentity, exists, err := readRemoteVoterIdentity(ctx, config)
	if err != nil || !exists {
		t.Fatalf("read writer registration through supervisor preflight path: exists=%t err=%v", exists, err)
	}
	lost := *config
	lost.DataDir = t.TempDir() // NoPVC replacement: both local voter identity and WAL are absent.
	physical := fencing.Identity{SourceClusterID: string(config.ClusterID), NodeID: string(config.NodeID),
		PodName: "rhiza-0", PodUID: "replacement-pod-uid", RunID: "replacement-supervisor-run"}
	result, err := EngineRepairPreflight(&lost)(ctx, physical)
	proposal := result.PendingEngineRepair
	if err != nil || proposal == nil || result.InitialRegistration {
		t.Fatalf("NoPVC replacement did not produce a pending repair proposal: result=%+v err=%v", result, err)
	}
	if proposal.OldMemberID != string(config.NodeID) || proposal.OldWALIdentity != firstIdentity.Nonce ||
		proposal.CandidateIdentity.PodUID != physical.PodUID {
		t.Fatalf("preflight did not retain the canonical registered source identity: proposal=%+v source=%+v", proposal, firstIdentity)
	}

	firstBoot := *config
	firstBoot.DataDir = t.TempDir()
	firstBoot.ObjStoreDir = filepath.Join(t.TempDir(), "empty-archive")
	firstBootResult, err := EngineRepairPreflight(&firstBoot)(ctx, physical)
	if err != nil || firstBootResult.InitialRegistration || firstBootResult.PendingEngineRepair != nil {
		t.Fatalf("genuinely absent genesis was not treated as ordinary first startup: result=%+v err=%v", firstBootResult, err)
	}
}
