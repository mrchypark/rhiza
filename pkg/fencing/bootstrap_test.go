package fencing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestBootstrapMembersHashMatchesFullSerializedProducerDigest(t *testing.T) {
	members := []quepaxa.Member{{ID: "n1", URL: "https://n1", PublicKey: quepaxa.PublicKey{1}}, {ID: "n2", PeerURL: "quic://n2", PublicKey: quepaxa.PublicKey{2}}}
	encoded, err := json.Marshal(members)
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(encoded)
	if got := BootstrapMembersHash(members); got != hex.EncodeToString(want[:]) {
		t.Fatalf("bootstrap assignment digest=%s, want full-member JSON digest %x", got, want)
	}
}

func TestBootstrapLedgerRetainsPerProcessInitEvidence(t *testing.T) {
	ledger := &Ledger{SourceClusterID: "source"}
	op := BootstrapOperation{OperationID: "initial-op", RequestHash: "request", AuthorizationHash: "auth", Purpose: BootstrapInitial, RootUID: "root-uid", StatefulSetUID: "sts-uid", SourceClusterID: "source"}
	if err := ledger.InstallBootstrapOperation(op); err != nil {
		t.Fatal(err)
	}
	if err := ledger.InstallBootstrapOperation(op); err != nil {
		t.Fatalf("identical operation retry: %v", err)
	}
	changed := op
	changed.RequestHash = "changed"
	if err := ledger.InstallBootstrapOperation(changed); err == nil {
		t.Fatal("changed operation authorization replaced retained state")
	}

	a := BootstrapAssignment{OperationID: op.OperationID, RequestHash: op.RequestHash, AuthorizationHash: op.AuthorizationHash, Purpose: op.Purpose, RootUID: op.RootUID, StatefulSetUID: op.StatefulSetUID, SourceClusterID: op.SourceClusterID, PodName: "rhiza-0", PodUID: "pod-uid-0", NodeID: "rhiza-0", CredentialName: "rhiza-0-credentials", CredentialUID: "secret-uid-0", MembershipHash: "membership", ProfileHash: "profile", LedgerUID: "ledger-uid"}
	if err := ledger.AssignBootstrap(a); err != nil {
		t.Fatal(err)
	}
	if err := ledger.AssignBootstrap(a); err != nil {
		t.Fatalf("identical assignment retry: %v", err)
	}
	changedAssignment := a
	changedAssignment.CredentialUID = "replacement-secret"
	if err := ledger.AssignBootstrap(changedAssignment); err == nil {
		t.Fatal("changed credential replaced the Pod UID assignment")
	}

	scope := InitExecutionScope{PodName: a.PodName, PodUID: a.PodUID, ContainerName: "prepare", ContainerID: "containerd://first", ImageID: "sha256:image", NodeName: "node-a", NodeUID: "node-uid-a"}
	attempt, err := ledger.BeginInit(a, "process-nonce-1", scope)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.AttemptID != "process-nonce-1" || attempt.Assignment != a || attempt.Scope != scope || len(attempt.Outcomes) != 0 || attempt.Terminal != nil {
		t.Fatalf("unexpected begin result: %+v", attempt)
	}
	if again, err := ledger.BeginInit(a, "process-nonce-1", scope); err != nil || again.AttemptID != attempt.AttemptID {
		t.Fatalf("same process retry=%+v err=%v", again, err)
	}
	if _, err := ledger.BeginInit(a, "process-nonce-2", scope); !errors.Is(err, ErrPending) {
		t.Fatalf("new nonce adopted same ContainerID: %v", err)
	}
	if _, err := ledger.BeginInit(a, "process-nonce-2", InitExecutionScope{}); err == nil {
		t.Fatal("incomplete authority runtime scope was accepted")
	}
	materialized := InitAttemptOutcome{State: InitAttemptMaterialized, GenesisDigest: BootstrapAssignmentDigest(a), LiveCleanup: InitLiveCleanup{Mode: InitModeGenesis, CleanupSucceeded: true, NamespaceQuiescent: true}}
	if err := ledger.RecordInitOutcome(a, attempt.AttemptID, materialized); err != nil {
		t.Fatal(err)
	}
	if err := ledger.RecordInitOutcome(a, attempt.AttemptID, materialized); err != nil {
		t.Fatalf("identical outcome retry: %v", err)
	}
	if err := ledger.RecordInitOutcome(a, attempt.AttemptID, InitAttemptOutcome{State: InitAttemptFailed, FailureClass: "cleanup"}); err == nil {
		t.Fatal("changed outcome rewrote attempt history")
	}
	terminal := InitTerminalEvidence{ContainerID: scope.ContainerID, ImageID: scope.ImageID, ExitCode: 0, Reason: "Completed"}
	if err := ledger.RecordInitTerminal(a, attempt.AttemptID, terminal); err != nil {
		t.Fatal(err)
	}
	if err := ledger.RecordInitTerminal(a, attempt.AttemptID, terminal); err != nil {
		t.Fatalf("identical terminal retry: %v", err)
	}
	if err := ledger.RecordInitTerminal(a, attempt.AttemptID, InitTerminalEvidence{ContainerID: "containerd://other", ImageID: scope.ImageID, ExitCode: 0, Reason: "Completed"}); err == nil {
		t.Fatal("terminal evidence from another container was accepted")
	}

	status, err := ledger.BootstrapStatus(op.OperationID, a.PodUID)
	if err != nil {
		t.Fatal(err)
	}
	if status.Assignment == nil || *status.Assignment != a || len(status.Attempts) != 1 || status.Attempts[0].AttemptID != attempt.AttemptID || status.Attempts[0].Terminal == nil || len(status.Attempts[0].Outcomes) != 1 {
		t.Fatalf("status lost retained history: %+v", status)
	}
	if err := ledger.CloseBootstrapAdmission(op.OperationID, op.RequestHash); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.BeginInit(a, "process-nonce-3", InitExecutionScope{PodName: a.PodName, PodUID: a.PodUID, ContainerName: "prepare", ContainerID: "containerd://third", ImageID: "sha256:image", NodeName: "node-a", NodeUID: "node-uid-a"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed admission accepted new process: %v", err)
	}
	// Admission closure is not history deletion; status remains readable.
	status, err = ledger.BootstrapStatus(op.OperationID, a.PodUID)
	if err != nil || !status.AdmissionClosed || len(status.Attempts) != 1 {
		t.Fatalf("closed status=%+v err=%v", status, err)
	}
}

func TestBootstrapAssignmentPurposeSpecificNodeID(t *testing.T) {
	a := BootstrapAssignment{OperationID: "learner-op", RequestHash: "request", AuthorizationHash: "auth", Purpose: BootstrapLearner, RootUID: "root", StatefulSetUID: "sts", SourceClusterID: "source", PodName: "learner", PodUID: "pod-uid", NodeID: "learner", CredentialName: "secret", CredentialUID: "secret-uid", MembershipHash: "membership", ProfileHash: "profile", LedgerUID: "ledger"}
	if err := a.Validate(); err == nil {
		t.Fatal("learner assignment accepted a Pod-name identity instead of its UID-derived NodeID")
	}
	a.NodeID = "pod-pod-uid"
	if err := a.Validate(); err != nil {
		t.Fatalf("valid UID-derived learner identity rejected: %v", err)
	}
}

func TestAuthorityClientBootstrapActionsUseExistingEndpoint(t *testing.T) {
	assignment := BootstrapAssignment{OperationID: "initial-op", RequestHash: "request", AuthorizationHash: "auth", Purpose: BootstrapInitial, RootUID: "root-uid", StatefulSetUID: "sts-uid", SourceClusterID: "source", PodName: "rhiza-0", PodUID: "pod-uid-0", NodeID: "rhiza-0", CredentialName: "rhiza-0-credentials", CredentialUID: "secret-uid-0", MembershipHash: "membership", ProfileHash: "profile", LedgerUID: "ledger-uid"}
	scope := InitExecutionScope{PodName: assignment.PodName, PodUID: assignment.PodUID, ContainerName: "prepare", ContainerID: "containerd://first", ImageID: "sha256:image", NodeName: "node-a", NodeUID: "node-uid-a"}
	var actions []string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/supervisor" {
			t.Errorf("unexpected endpoint path %q", r.URL.Path)
		}
		var message AuthorityMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		actions = append(actions, message.Action)
		w.Header().Set("Content-Type", "application/json")
		switch message.Action {
		case "bootstrap-status":
			if message.BootstrapStatus == nil || message.BootstrapStatus.OperationID != assignment.OperationID {
				t.Errorf("status request=%+v", message)
			}
			_ = json.NewEncoder(w).Encode(BootstrapStatusResult{Assignment: &assignment, AssignmentSetHash: BootstrapAssignmentSetHash([]BootstrapAssignment{assignment})})
		case "init-begin":
			if message.BeginInit == nil || message.BeginInit.Assignment != assignment || message.BeginInit.AttemptID != "nonce-1" {
				t.Errorf("begin request=%+v", message)
			}
			_ = json.NewEncoder(w).Encode(InitAttempt{AttemptID: "nonce-1", Assignment: assignment, Scope: scope})
		case "init-outcome", "bootstrap-complete":
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected action %q", message.Action)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	client := &AuthorityClient{URL: server.URL + "/v1/supervisor", TokenFile: tokenPath, Client: server.Client()}
	candidate := Identity{SourceClusterID: assignment.SourceClusterID, NodeID: assignment.NodeID, PodName: assignment.PodName, PodUID: assignment.PodUID}
	status, err := client.BootstrapStatus(context.Background(), assignment.OperationID, candidate)
	if err != nil || status.Assignment == nil || *status.Assignment != assignment {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	if _, err := client.BeginInit(context.Background(), assignment, "nonce-1"); err != nil {
		t.Fatal(err)
	}
	if err := client.RecordInitOutcome(context.Background(), assignment, "nonce-1", InitAttemptOutcome{State: InitAttemptMaterialized, GenesisDigest: BootstrapAssignmentDigest(assignment), LiveCleanup: InitLiveCleanup{Mode: InitModeGenesis, CleanupSucceeded: true, NamespaceQuiescent: true}}); err != nil {
		t.Fatal(err)
	}
	runtime := Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "pod-uid-0", RunID: "run-1", WALIdentity: "wal-1"}
	completion := BootstrapCompletion{OperationID: assignment.OperationID, AuthorizationHash: assignment.AuthorizationHash, AssignmentSetHash: BootstrapAssignmentSetHash([]BootstrapAssignment{assignment}), CertifiedBarrierRef: "9"}
	if err := client.CompleteBootstrap(context.Background(), runtime, completion); err != nil {
		t.Fatal(err)
	}
	if want := []string{"bootstrap-status", "init-begin", "init-outcome", "bootstrap-complete"}; !reflect.DeepEqual(actions, want) {
		t.Fatalf("actions=%v, want %v", actions, want)
	}
}

func TestAuthorityClientAcceptsUnassignedOnlyForExactOwnInitLookup(t *testing.T) {
	requests := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		var message AuthorityMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(BootstrapStatusResult{AssignmentSetHash: BootstrapAssignmentSetHash(nil)})
	}))
	defer server.Close()
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("test-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := &AuthorityClient{URL: server.URL + "/v1/supervisor", TokenFile: tokenPath, Client: server.Client()}
	candidate := Identity{SourceClusterID: "source", NodeID: "pod-live-uid", PodName: "rhiza-2", PodUID: "live-uid"}
	status, err := client.BootstrapStatus(context.Background(), "", candidate)
	if err != nil || status.Assignment != nil || status.AssignmentSetHash != BootstrapAssignmentSetHash(nil) {
		t.Fatalf("empty own-init lookup status=%+v err=%v", status, err)
	}
	if _, err := client.BootstrapStatus(context.Background(), "initial-operation", candidate); err == nil {
		t.Fatal("accepted an empty result for an operation-specific status lookup")
	}
	invalid := candidate
	invalid.NodeID = "pod-other-uid"
	if _, err := client.BootstrapStatus(context.Background(), "", invalid); err == nil {
		t.Fatal("accepted an unassigned lookup outside the candidate Pod UID scope")
	}
	if requests != 2 {
		t.Fatalf("made %d authority requests, want 2 (invalid local scope must not call)", requests)
	}
}

func TestMaterializedOutcomeRequiresActualCleanupAndNamespaceEvidence(t *testing.T) {
	valid := InitAttemptOutcome{State: InitAttemptMaterialized, LiveCleanup: InitLiveCleanup{Mode: InitModeRestore, BucketDrainSucceeded: true, CleanupSucceeded: true, NamespaceQuiescent: true}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid restore cleanup evidence rejected: %v", err)
	}
	invalid := []InitAttemptOutcome{
		{State: InitAttemptMaterialized, LiveCleanup: InitLiveCleanup{Mode: InitModeRestore, CleanupSucceeded: true, NamespaceQuiescent: true}},
		{State: InitAttemptMaterialized, LiveCleanup: InitLiveCleanup{Mode: InitModeRestore, BucketDrainSucceeded: true, NamespaceQuiescent: true}},
		{State: InitAttemptMaterialized, LiveCleanup: InitLiveCleanup{Mode: InitModeMarkerNoop, BucketDrainSucceeded: true, CleanupSucceeded: true, NamespaceQuiescent: true}},
		{State: InitAttemptMaterialized, LiveCleanup: InitLiveCleanup{Mode: InitModeGenesis, BucketDrainSucceeded: true, CleanupSucceeded: true, NamespaceQuiescent: true}},
	}
	for i, outcome := range invalid {
		if err := outcome.Validate(); err == nil {
			t.Errorf("invalid outcome %d accepted: %+v", i, outcome)
		}
	}
	failed := InitAttemptOutcome{State: InitAttemptFailed, FailureClass: InitFailureCleanup, LiveCleanup: valid.LiveCleanup}
	if err := failed.Validate(); err != nil {
		t.Fatalf("bounded failed cleanup report rejected: %v", err)
	}
	ledger := &Ledger{SourceClusterID: "source"}
	operation := BootstrapOperation{OperationID: "op", RequestHash: "request", AuthorizationHash: "auth", Purpose: BootstrapInitial, RootUID: "root", StatefulSetUID: "sts", SourceClusterID: "source"}
	if err := ledger.InstallBootstrapOperation(operation); err != nil {
		t.Fatal(err)
	}
	assignment := BootstrapAssignment{OperationID: "op", RequestHash: "request", AuthorizationHash: "auth", Purpose: BootstrapInitial, RootUID: "root", StatefulSetUID: "sts", SourceClusterID: "source", PodName: "node", PodUID: "pod-uid", NodeID: "node", CredentialName: "secret", CredentialUID: "secret-uid", MembershipHash: "membership", ProfileHash: "profile", LedgerUID: "ledger-uid"}
	if err := ledger.AssignBootstrap(assignment); err != nil {
		t.Fatal(err)
	}
	scope := InitExecutionScope{PodName: "node", PodUID: "pod-uid", ContainerName: "prepare", ContainerID: "containerd://first", ImageID: "sha256:image", NodeName: "worker", NodeUID: "node-uid"}
	if _, err := ledger.BeginInit(assignment, "nonce", scope); err != nil {
		t.Fatal(err)
	}
	failedAttempt := InitAttemptOutcome{State: InitAttemptFailed, FailureClass: InitFailureCleanup, LiveCleanup: valid.LiveCleanup}
	if err := ledger.RecordInitOutcome(assignment, "nonce", failedAttempt); err != nil {
		t.Fatal(err)
	}
	if err := ledger.RecordInitTerminal(assignment, "nonce", InitTerminalEvidence{ContainerID: scope.ContainerID, ImageID: scope.ImageID, ExitCode: 1, Reason: "Error"}); err != nil {
		t.Fatal(err)
	}
	if ledger.bootstrapInitComplete(map[string]bool{"node": true}) {
		t.Fatal("failed init attempt qualified as successful completion")
	}
}

func TestGenesisOutcomeRequiresExactInitialAssignmentProvenance(t *testing.T) {
	makeAttempt := func(t *testing.T, purpose BootstrapPurpose) (*Ledger, BootstrapAssignment, string) {
		t.Helper()
		podUID := "pod-uid"
		nodeID := "node"
		if purpose == BootstrapLearner {
			nodeID = "pod-pod-uid"
		}
		assignment := BootstrapAssignment{
			OperationID: "op", RequestHash: "request", AuthorizationHash: "auth", Purpose: purpose,
			RootUID: "root", StatefulSetUID: "sts", SourceClusterID: "source", PodName: "node", PodUID: podUID, NodeID: nodeID,
			CredentialName: "secret", CredentialUID: "secret-uid", MembershipHash: "membership", ProfileHash: "profile", LedgerUID: "ledger-uid",
		}
		ledger := &Ledger{SourceClusterID: "source"}
		if err := ledger.InstallBootstrapOperation(BootstrapOperation{OperationID: "op", RequestHash: "request", AuthorizationHash: "auth", Purpose: purpose, RootUID: "root", StatefulSetUID: "sts", SourceClusterID: "source"}); err != nil {
			t.Fatal(err)
		}
		if err := ledger.AssignBootstrap(assignment); err != nil {
			t.Fatal(err)
		}
		scope := InitExecutionScope{PodName: assignment.PodName, PodUID: podUID, ContainerName: "prepare", ContainerID: "containerd://init", ImageID: "sha256:image", NodeName: "worker", NodeUID: "node-uid"}
		if _, err := ledger.BeginInit(assignment, "nonce", scope); err != nil {
			t.Fatal(err)
		}
		return ledger, assignment, "nonce"
	}
	genesisCleanup := InitLiveCleanup{Mode: InitModeGenesis, CleanupSucceeded: true, NamespaceQuiescent: true}

	t.Run("empty digest is not materialized", func(t *testing.T) {
		ledger, assignment, attemptID := makeAttempt(t, BootstrapInitial)
		outcome := InitAttemptOutcome{State: InitAttemptMaterialized, LiveCleanup: genesisCleanup}
		if err := ledger.RecordInitOutcome(assignment, attemptID, outcome); err == nil {
			t.Fatal("initial genesis with empty provenance digest was accepted")
		}
		if ledger.BootstrapInitComplete([]string{assignment.NodeID}) {
			t.Fatal("empty provenance digest qualified for bootstrap completion")
		}
	})
	t.Run("wrong digest is not materialized", func(t *testing.T) {
		ledger, assignment, attemptID := makeAttempt(t, BootstrapInitial)
		outcome := InitAttemptOutcome{State: InitAttemptMaterialized, GenesisDigest: strings.Repeat("0", 64), LiveCleanup: genesisCleanup}
		if err := ledger.RecordInitOutcome(assignment, attemptID, outcome); err == nil {
			t.Fatal("initial genesis with mismatched assignment digest was accepted")
		}
		if ledger.BootstrapInitComplete([]string{assignment.NodeID}) {
			t.Fatal("mismatched provenance digest qualified for bootstrap completion")
		}
	})
	t.Run("genesis is not authorized for learner", func(t *testing.T) {
		ledger, assignment, attemptID := makeAttempt(t, BootstrapLearner)
		outcome := InitAttemptOutcome{State: InitAttemptMaterialized, GenesisDigest: BootstrapAssignmentDigest(assignment), LiveCleanup: genesisCleanup}
		if err := ledger.RecordInitOutcome(assignment, attemptID, outcome); err == nil {
			t.Fatal("learner assignment accepted initial-genesis provenance")
		}
		if ledger.BootstrapInitComplete([]string{assignment.NodeID}) {
			t.Fatal("learner genesis outcome qualified for bootstrap completion")
		}
	})
	t.Run("exact initial assignment digest qualifies with terminal evidence", func(t *testing.T) {
		ledger, assignment, attemptID := makeAttempt(t, BootstrapInitial)
		outcome := InitAttemptOutcome{State: InitAttemptMaterialized, GenesisDigest: BootstrapAssignmentDigest(assignment), LiveCleanup: genesisCleanup}
		if err := ledger.RecordInitOutcome(assignment, attemptID, outcome); err != nil {
			t.Fatal(err)
		}
		if err := ledger.RecordInitTerminal(assignment, attemptID, InitTerminalEvidence{ContainerID: "containerd://init", ImageID: "sha256:image", ExitCode: 0, Reason: "Completed"}); err != nil {
			t.Fatal(err)
		}
		if !ledger.BootstrapInitComplete([]string{assignment.NodeID}) {
			t.Fatal("exact authorized genesis provenance and terminal evidence did not qualify")
		}
	})
}
