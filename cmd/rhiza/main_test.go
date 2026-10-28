package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/pkg/fencing"
)

type managedAssignmentReaderFixture struct {
	responses map[string]fencing.BootstrapStatusResult
	errors    map[string]error
	calls     []string
}

func (f *managedAssignmentReaderFixture) BootstrapStatus(_ context.Context, operationID string, _ fencing.Identity) (fencing.BootstrapStatusResult, error) {
	f.calls = append(f.calls, operationID)
	if err := f.errors[operationID]; err != nil {
		return fencing.BootstrapStatusResult{}, err
	}
	return f.responses[operationID], nil
}

func TestReplicaMembersRequirePinnedIdentityOnlyForLearner(t *testing.T) {
	members := []rhiza.Member{{ID: "n1"}, {ID: "n2"}}
	objectMembers, err := objectReplicaMembers("cluster", members, false)
	if err != nil || len(objectMembers) != 2 || objectMembers[1].ID != "n2" {
		t.Fatalf("object-store members=%+v err=%v", objectMembers, err)
	}
	derived, err := rhiza.NewReplicaMember("cluster", rhiza.Member{ID: "n1", PeerURL: "quic://n1:9090", PublicKey: rhiza.PublicKey{1}})
	if err != nil {
		t.Fatal(err)
	}
	key := base64.StdEncoding.EncodeToString(derived.PublicKey[:])
	learnerMembers, err := learnerReplicaMembers(`[{"node_id":"n1","peer_url":"quic://n1:9090","public_key":"` + key + `"}]`)
	if err != nil || len(learnerMembers) != 1 || learnerMembers[0].PeerURL != "quic://n1:9090" {
		t.Fatalf("learner members=%+v err=%v", learnerMembers, err)
	}
	if _, err := learnerReplicaMembers(`[{"node_id":"n1","peer_url":"quic://n1:9090","public_key":"` + key + `","token":"voter-secret"}]`); err == nil {
		t.Fatal("learner accepted a voter token field")
	}
	if _, err := learnerReplicaMembers(`[{"node_id":"n1","peer_url":"quic://n1:9090"}]`); err == nil {
		t.Fatal("learner accepted a member without a public key")
	}
	if _, err := learnerReplicaMembers(`[{"node_id":"n1","peer_url":"quic://n1:9090","public_key":"` + key + `"}] trailing`); err == nil {
		t.Fatal("learner accepted trailing replica member input")
	}
	historyMembers, err := objectReplicaMembers("cluster", []rhiza.Member{{ID: "n1", URL: "http://n1", PeerURL: "quic://n1:9090", PublicKey: rhiza.PublicKey{1}}}, true)
	if err != nil || len(historyMembers) != 1 || historyMembers[0].PeerURL != "quic://n1:9090" || historyMembers[0].PublicKey != ([32]byte{1}) {
		t.Fatalf("history-aware members=%+v err=%v", historyMembers, err)
	}
}

func TestRunReturnsConfigurationErrors(t *testing.T) {
	t.Setenv("RHIZA_ROLE", "unknown")
	t.Setenv("RHIZA_OBJSTORE_INSECURE", "")
	if err := run(context.Background()); err == nil || !strings.Contains(err.Error(), "invalid RHIZA_ROLE") {
		t.Fatalf("error=%v", err)
	}
}

func TestPrepareRestoreFlagUsesOfflineAPIAndRejectsUnknownFlags(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "must-not-be-created")
	t.Setenv("RHIZA_ROLE", "learner")
	t.Setenv("RHIZA_CLUSTER_ID", "prepare-test")
	t.Setenv("RHIZA_NODE_ID", "fresh-learner")
	t.Setenv("RHIZA_DATA_DIR", dataDir)
	t.Setenv("RHIZA_PREPARED_POD_UID", "")
	t.Setenv("RHIZA_ENABLE_RECONFIGURATION", "true")
	t.Setenv("RHIZA_CLUSTER_MEMBERS", `[{"node_id":"n1","peer_url":"quic://n1:9090","public_key":"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="},{"node_id":"n2","peer_url":"quic://n2:9090","public_key":"AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI="}]`)
	t.Setenv("RHIZA_LEARNER", `{"node_id":"fresh-learner","peer_url":"quic://fresh-learner:9090","public_key":"AwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM="}`)
	t.Setenv("RHIZA_REPLICA_MEMBERS", "")

	err := runArgs(context.Background(), []string{"--prepare-restore"})
	if err == nil || !strings.Contains(err.Error(), "Pod UID") {
		t.Fatalf("--prepare-restore error=%v; want the PrepareRestore API's missing Pod UID refusal", err)
	}
	if strings.Contains(err.Error(), "RHIZA_REPLICA_MEMBERS") || strings.Contains(err.Error(), "invalid RHIZA_ROLE") {
		t.Fatalf("prepare mode entered ordinary role startup: %v", err)
	}
	if _, statErr := os.Stat(dataDir); !os.IsNotExist(statErr) {
		t.Fatalf("prepare validation created DataDir: stat error=%v", statErr)
	}

	if err := runArgs(context.Background(), []string{"--prepare-restore", "--unknown-option"}); err == nil {
		t.Fatal("unknown prepare-mode flag was accepted")
	}
	if _, statErr := os.Stat(dataDir); !os.IsNotExist(statErr) {
		t.Fatalf("unknown flag path created DataDir: stat error=%v", statErr)
	}
}

func TestManagedPrepareEnvironmentNeverFallsBackWhenPartiallyConfigured(t *testing.T) {
	for _, key := range []string{"RHIZA_PREPARE_OPERATION_ID", "POD_NAME", "POD_UID", "RHIZA_PREPARED_POD_UID", "FENCER_AUTHORITY_URL", "FENCER_CA_FILE"} {
		t.Setenv(key, "")
	}
	t.Setenv("FENCER_AUTHORITY_URL", "https://fencer.example/v1/supervisor")
	if _, managed, err := readManagedPrepareEnvironment(); !managed || err == nil {
		t.Fatalf("partial managed profile managed=%t err=%v; want fail-closed", managed, err)
	}
}

func TestManagedPrepareEnvironmentRequiresExactPodUIDAndNoRuntimeSecrets(t *testing.T) {
	for key, value := range map[string]string{
		"RHIZA_PREPARE_OPERATION_ID": "op-1", "POD_NAME": "rhiza-0", "POD_UID": "uid-1",
		"RHIZA_PREPARED_POD_UID": "uid-1", "FENCER_AUTHORITY_URL": "https://fencer.example/v1/supervisor",
		"FENCER_CA_FILE": "/ca.pem", "RHIZA_CLUSTER_ID": "cluster", "RHIZA_NODE_ID": "rhiza-0",
		"RHIZA_DATA_DIR": "/data", "RHIZA_CLUSTER_MEMBERS": "[]",
		"RHIZA_ADMIN_TOKEN": "", "RHIZA_PEER_TOKEN": "", "RHIZA_PEER_TOKENS": "",
	} {
		t.Setenv(key, value)
	}
	if _, managed, err := readManagedPrepareEnvironment(); !managed || err != nil {
		t.Fatalf("valid managed profile managed=%t err=%v", managed, err)
	}
	t.Setenv("POD_UID", "different")
	if _, _, err := readManagedPrepareEnvironment(); err == nil || !strings.Contains(err.Error(), "differs") {
		t.Fatalf("mismatched Pod UID error=%v", err)
	}
	t.Setenv("POD_UID", "uid-1")
	t.Setenv("RHIZA_PEER_TOKENS", `{"rhiza-0":"secret"}`)
	if _, _, err := readManagedPrepareEnvironment(); err == nil || !strings.Contains(err.Error(), "must not receive") {
		t.Fatalf("runtime credential error=%v", err)
	}
}

func TestManagedPrepareAllowsReadOnlyOwnLearnerDiscovery(t *testing.T) {
	for key, value := range map[string]string{
		"RHIZA_PREPARE_OPERATION_ID": "", "POD_NAME": "rhiza-0", "POD_UID": "uid-1",
		"RHIZA_PREPARED_POD_UID": "uid-1", "FENCER_AUTHORITY_URL": "https://fencer.example/v1/supervisor",
		"FENCER_CA_FILE": "/ca.pem", "RHIZA_CLUSTER_ID": "cluster", "RHIZA_NODE_ID": "bootstrap-hint",
		"RHIZA_DATA_DIR": "/data", "RHIZA_CLUSTER_MEMBERS": "[]",
		"RHIZA_ADMIN_TOKEN": "", "RHIZA_PEER_TOKEN": "", "RHIZA_PEER_TOKENS": "",
	} {
		t.Setenv(key, value)
	}
	settings, managed, err := readManagedPrepareEnvironment()
	if err != nil || !managed || settings.operationID != "" {
		t.Fatalf("own-init profile managed=%t settings=%+v err=%v", managed, settings, err)
	}
}

func TestManagedAssignmentDiscoveryPrefersExactOwnLearnerOverStaticOperation(t *testing.T) {
	candidate := fencing.Identity{SourceClusterID: "cluster", NodeID: "rhiza-0", PodName: "rhiza-0", PodUID: "new-pod"}
	assignment := testManagedLearnerAssignment(candidate, "ordinary-repair-op")
	reader := &managedAssignmentReaderFixture{responses: map[string]fencing.BootstrapStatusResult{
		"": {Assignment: &assignment, AssignmentSetHash: strings.Repeat("a", 64)},
	}}
	got, err := resolveManagedAssignment(context.Background(), reader, "initial-op", candidate)
	if err != nil || got != assignment {
		t.Fatalf("assignment=%+v err=%v", got, err)
	}
	if !reflect.DeepEqual(reader.calls, []string{""}) {
		t.Fatalf("queries=%q; static operation annotation was consulted", reader.calls)
	}
}

func TestManagedAssignmentDiscoveryKeepsInitialLookupExactAndReadErrorsFailClosed(t *testing.T) {
	candidate := fencing.Identity{SourceClusterID: "cluster", NodeID: "rhiza-0", PodName: "rhiza-0", PodUID: "initial-pod"}
	initial := fencing.BootstrapAssignment{
		OperationID: "initial-op", RequestHash: strings.Repeat("a", 64), AuthorizationHash: strings.Repeat("b", 64),
		Purpose: fencing.BootstrapInitial, RootUID: "root", StatefulSetUID: "sts", LedgerUID: "ledger",
		SourceClusterID: "cluster", PodName: candidate.PodName, PodUID: candidate.PodUID, NodeID: candidate.NodeID,
		CredentialName: "secret", CredentialUID: "secret-uid", MembershipHash: strings.Repeat("c", 64), ProfileHash: strings.Repeat("d", 64),
	}
	reader := &managedAssignmentReaderFixture{responses: map[string]fencing.BootstrapStatusResult{
		"": {}, "initial-op": {Assignment: &initial, AssignmentSetHash: strings.Repeat("e", 64)},
	}}
	got, err := resolveManagedAssignment(context.Background(), reader, "initial-op", candidate)
	if err != nil || got != initial || !reflect.DeepEqual(reader.calls, []string{"", "initial-op"}) {
		t.Fatalf("assignment=%+v calls=%q err=%v", got, reader.calls, err)
	}
	reader = &managedAssignmentReaderFixture{errors: map[string]error{"": errors.New("authority unavailable")}}
	if _, err := resolveManagedAssignment(context.Background(), reader, "initial-op", candidate); err == nil || !reflect.DeepEqual(reader.calls, []string{""}) {
		t.Fatalf("read error=%v calls=%q; auth failure fell back", err, reader.calls)
	}
	newPod := candidate
	newPod.PodUID = "replacement-pod"
	reader = &managedAssignmentReaderFixture{responses: map[string]fencing.BootstrapStatusResult{"": {}}}
	if _, err := resolveManagedAssignment(context.Background(), reader, "initial-op", newPod); err == nil || !reflect.DeepEqual(reader.calls, []string{"", "initial-op"}) {
		t.Fatalf("new Pod UID reused initial assignment: err=%v calls=%q", err, reader.calls)
	}
}

func testManagedLearnerAssignment(candidate fencing.Identity, operationID string) fencing.BootstrapAssignment {
	nodeID, _ := fencing.ReplacementNodeID(candidate.PodUID)
	return fencing.BootstrapAssignment{
		OperationID: operationID, RequestHash: strings.Repeat("a", 64), AuthorizationHash: strings.Repeat("b", 64),
		Purpose: fencing.BootstrapLearner, RootUID: "root", StatefulSetUID: "sts", LedgerUID: "ledger",
		SourceClusterID: candidate.SourceClusterID, PodName: candidate.PodName, PodUID: candidate.PodUID, NodeID: nodeID,
		CredentialName: "secret", CredentialUID: "secret-uid", MembershipHash: strings.Repeat("c", 64), ProfileHash: strings.Repeat("d", 64),
		PublicMember: fencing.BootstrapPublicMember{ID: nodeID, URL: "http://rhiza-0", PeerURL: "quic://rhiza-0", LogURL: "http://rhiza-0/log", PublicKey: strings.Repeat("0", 64)},
		Durability:   "async", StoreIdentityHash: strings.Repeat("e", 64),
	}
}

func TestBindManagedLearnerAssignmentPinsStaticMembershipAndStore(t *testing.T) {
	member := rhiza.Member{ID: "n1", PublicKey: rhiza.PublicKey{1}}
	config := rhiza.Config{
		ClusterID: "cluster", Members: []rhiza.Member{member},
		ObjStoreProvider: "s3", ObjStoreEndpoint: "https://s3.example", ObjStoreBucket: "archive", ObjStorePrefix: "rhiza",
		ObjStoreDurability: rhiza.ObjectStoreDurabilityBeforeAck,
	}
	assignment := fencing.BootstrapAssignment{
		OperationID: "repair-op", RequestHash: strings.Repeat("a", 64), AuthorizationHash: strings.Repeat("b", 64),
		Purpose: fencing.BootstrapLearner, RootUID: "root", StatefulSetUID: "sts", LedgerUID: "ledger",
		SourceClusterID: "cluster", PodName: "rhiza-0", PodUID: "uid-1", NodeID: "pod-uid-1",
		CredentialName: "rhiza-0-credentials", CredentialUID: "secret-uid", ProfileHash: strings.Repeat("c", 64),
		MembershipHash: bootstrapMembershipHash(config.Members),
		PublicMember:   fencing.BootstrapPublicMember{ID: "pod-uid-1", URL: "http://rhiza-0", PeerURL: "quic://rhiza-0", LogURL: "http://rhiza-0/log", PublicKey: strings.Repeat("0", 62) + "01"},
		Durability:     string(config.ObjStoreDurability), StoreIdentityHash: bootstrapStoreIdentityHash(config),
	}
	if err := bindManagedLearnerAssignment(&config, assignment); err != nil {
		t.Fatalf("bind exact assignment: %v", err)
	}
	if config.NodeID != assignment.NodeID || config.Learner == nil || string(config.Learner.ID) != assignment.NodeID || config.Members[0] != member {
		t.Fatalf("config=%+v; assignment did not bind without changing source membership", config)
	}
	for name, mutate := range map[string]func(*rhiza.Config){
		"membership": func(c *rhiza.Config) { c.Members[0].PublicKey[0]++ },
		"store":      func(c *rhiza.Config) { c.ObjStorePrefix += "/other" },
		"durability": func(c *rhiza.Config) { c.ObjStoreDurability = rhiza.ObjectStoreDurabilityAsync },
	} {
		t.Run(name, func(t *testing.T) {
			wrong := config
			wrong.Members = append([]rhiza.Member(nil), config.Members...)
			mutate(&wrong)
			if err := bindManagedLearnerAssignment(&wrong, assignment); err == nil {
				t.Fatal("assignment accepted a mismatched source profile")
			}
		})
	}
}

func TestManagedInitOutcomeConstruction(t *testing.T) {
	const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tests := []struct {
		name    string
		purpose fencing.BootstrapPurpose
		cleanup fencing.InitLiveCleanup
		prepare error
		quiesce error
		want    fencing.InitAttemptState
		failure string
		genesis string
	}{
		{
			name: "initial genesis", purpose: fencing.BootstrapInitial,
			cleanup: fencing.InitLiveCleanup{Mode: fencing.InitModeGenesis, CleanupSucceeded: true},
			want:    fencing.InitAttemptMaterialized, genesis: digest,
		},
		{
			name: "initial marker noop", purpose: fencing.BootstrapInitial,
			cleanup: fencing.InitLiveCleanup{Mode: fencing.InitModeGenesis, CleanupSucceeded: true},
			want:    fencing.InitAttemptMaterialized, genesis: digest,
		},
		{
			name: "learner restore", purpose: fencing.BootstrapLearner,
			cleanup: fencing.InitLiveCleanup{Mode: fencing.InitModeRestore, BucketDrainSucceeded: true, CleanupSucceeded: true},
			want:    fencing.InitAttemptMaterialized,
		},
		{
			name: "learner marker noop", purpose: fencing.BootstrapLearner,
			cleanup: fencing.InitLiveCleanup{Mode: fencing.InitModeMarkerNoop, CleanupSucceeded: true},
			want:    fencing.InitAttemptMaterialized,
		},
		{
			name: "restore failed", purpose: fencing.BootstrapLearner,
			cleanup: fencing.InitLiveCleanup{Mode: fencing.InitModeRestore, BucketDrainSucceeded: true, CleanupSucceeded: true},
			prepare: errors.New("injected preparation failure"), want: fencing.InitAttemptFailed, failure: fencing.InitFailureRestore,
		},
		{
			name: "cleanup failed", purpose: fencing.BootstrapInitial,
			cleanup: fencing.InitLiveCleanup{Mode: fencing.InitModeGenesis},
			failure: fencing.InitFailureCleanup, want: fencing.InitAttemptFailed,
		},
		{
			name: "namespace quiescence unknown", purpose: fencing.BootstrapInitial,
			cleanup: fencing.InitLiveCleanup{Mode: fencing.InitModeGenesis, CleanupSucceeded: true},
			quiesce: errors.New("injected quiescence failure"), failure: fencing.InitFailureCleanup, want: fencing.InitAttemptFailed,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcome := managedInitOutcome(test.purpose, test.cleanup, digest, test.prepare, test.quiesce)
			if outcome.State != test.want || outcome.FailureClass != test.failure || outcome.GenesisDigest != test.genesis {
				t.Fatalf("outcome=%+v", outcome)
			}
			if err := outcome.Validate(); err != nil {
				t.Fatalf("constructed outcome is invalid: %v", err)
			}
		})
	}
}

func TestHelpFlagsPrintUsageAndExitSuccessfully(t *testing.T) {
	for _, arg := range []string{"-h", "--help"} {
		t.Run(arg, func(t *testing.T) {
			var output bytes.Buffer
			if err := runArgsWithOutput(context.Background(), []string{arg}, &output); err != nil {
				t.Fatalf("help returned error: %v", err)
			}
			for _, want := range []string{"Usage of rhiza:", "-prepare-restore", "-enroll-existing-voter"} {
				if !strings.Contains(output.String(), want) {
					t.Fatalf("help output %q does not contain %q", output.String(), want)
				}
			}
		})
	}
}

func TestClusterMemberJSONRejectsTypos(t *testing.T) {
	var members []rhiza.Member
	if err := decodeStrictJSON(`[{"node_id":"n1","peer_ur":"quic://n1"}]`, &members); err == nil {
		t.Fatal("unknown cluster member field was accepted")
	}
}

func TestServerBinaryUsesOnlyRootRhizaAPI(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	mainFile := filepath.Join(filepath.Dir(file), "main.go")
	parsed, err := parser.ParseFile(token.NewFileSet(), mainFile, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range parsed.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(path, "github.com/mrchypark/rhiza/") && path != "github.com/mrchypark/rhiza/pkg/fencing" {
			t.Fatalf("server bypasses the root embedded API with import %q", path)
		}
	}
}
