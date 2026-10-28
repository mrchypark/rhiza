package node

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

func TestValidateManagedLearnerAssignmentPinsReturnedIdentityAndSource(t *testing.T) {
	config, assignment := managedLearnerProfileFixture()
	if err := validateManagedLearnerAssignment(&config, assignment); err != nil {
		t.Fatalf("validate assignment: %v", err)
	}
	for name, mutate := range map[string]func(*types.ExecutionConfig, *fencing.BootstrapAssignment){
		"member URL": func(c *types.ExecutionConfig, _ *fencing.BootstrapAssignment) { c.Learner.URL = "http://other" },
		"public key": func(c *types.ExecutionConfig, _ *fencing.BootstrapAssignment) { c.Members[0].PublicKey[0]++ },
		"durability": func(c *types.ExecutionConfig, _ *fencing.BootstrapAssignment) {
			c.ObjStoreDurability = types.ObjectStoreDurabilityAsync
		},
		"store": func(c *types.ExecutionConfig, _ *fencing.BootstrapAssignment) { c.ObjStorePrefix += "/other" },
		"secret WAL": func(_ *types.ExecutionConfig, a *fencing.BootstrapAssignment) {
			a.PublicMember.WALIdentity = "unexpected"
		},
	} {
		t.Run(name, func(t *testing.T) {
			wrongConfig, wrongAssignment := config, assignment
			wrongConfig.Members = append([]quepaxa.Member(nil), config.Members...)
			wrongLearner := *config.Learner
			wrongConfig.Learner = &wrongLearner
			mutate(&wrongConfig, &wrongAssignment)
			if err := validateManagedLearnerAssignment(&wrongConfig, wrongAssignment); err == nil {
				t.Fatal("accepted changed assignment binding")
			}
		})
	}
}

func managedLearnerProfileFixture() (types.ExecutionConfig, fencing.BootstrapAssignment) {
	key := quepaxa.PublicKey{1}
	config := types.ExecutionConfig{
		ClusterID: "cluster", NodeID: "pod-pod-uid", PreparedPodUID: "pod-uid",
		Members:          []quepaxa.Member{{ID: "n1", PublicKey: quepaxa.PublicKey{2}}, {ID: "n2", PublicKey: quepaxa.PublicKey{3}}},
		ObjStoreProvider: "s3", ObjStoreEndpoint: "https://s3.example", ObjStoreBucket: "archive", ObjStorePrefix: "rhiza",
		ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck,
	}
	config.Learner = &quepaxa.Member{ID: quepaxa.NodeID(config.NodeID), URL: "http://rhiza-0", PeerURL: "quic://rhiza-0", LogURL: "http://rhiza-0/log", PublicKey: key}
	publicKey := hex.EncodeToString(key[:])
	assignment := fencing.BootstrapAssignment{
		OperationID: "repair-op", RequestHash: strings.Repeat("a", 64), AuthorizationHash: strings.Repeat("b", 64),
		Purpose: fencing.BootstrapLearner, RootUID: "root", StatefulSetUID: "sts", LedgerUID: "ledger",
		SourceClusterID: string(config.ClusterID), PodName: "rhiza-0", PodUID: config.PreparedPodUID, NodeID: string(config.NodeID),
		CredentialName: "rhiza-0-credentials", CredentialUID: "secret-uid", ProfileHash: strings.Repeat("c", 64),
		MembershipHash: recovery.NewMembershipRecord(string(config.ClusterID), config.Members, string(config.ObjStoreDurability)).Membership,
		PublicMember:   fencing.BootstrapPublicMember{ID: string(config.NodeID), URL: config.Learner.URL, PeerURL: config.Learner.PeerURL, LogURL: config.Learner.LogURL, PublicKey: publicKey},
		Durability:     string(config.ObjStoreDurability), StoreIdentityHash: managedStoreIdentityHash(&config),
	}
	return config, assignment
}

func TestManagedDRAssignmentRequiresUnforgeableFreshAdmission(t *testing.T) {
	config, assignment := managedDRProfileFixture()
	if err := assignment.Validate(); err != nil {
		t.Fatalf("fixture assignment invalid: %v", err)
	}
	if _, err := validateManagedDRSuccessorAssignment(&config, assignment); err != nil {
		t.Fatalf("validate exact assignment: %v", err)
	}
	var copied fencing.InitAdmission
	if err := copied.ValidateFor(assignment); err == nil {
		t.Fatal("accepted a caller-constructed empty admission")
	}
	if _, _, err := PrepareManagedInit(context.Background(), &config, assignment); err == nil || !strings.Contains(err.Error(), "admission") {
		t.Fatalf("assignment-only preparation error=%v; want required authority admission", err)
	}
	unapproved := assignment
	unapproved.DRApproval.Request.TargetAnchorHash = strings.Repeat("f", 64)
	if _, err := validateManagedDRSuccessorAssignment(&config, unapproved); err == nil {
		t.Fatal("accepted copied request/receipt fields without the matching authorized assignment hash")
	}
}

func TestAuthorityClientIssuesAdmissionOnlyForExactBeginInitResponse(t *testing.T) {
	_, assignment := managedDRProfileFixture()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("authorization=%q", got)
		}
		var message fencing.AuthorityMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			t.Errorf("decode request: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if message.Action != "init-begin" || message.BeginInit == nil || message.BeginInit.Assignment != assignment || message.BeginInit.AttemptID != "attempt-1" {
			t.Errorf("unexpected begin-init request: %+v", message)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(fencing.InitAttempt{
			AttemptID: "attempt-1", Assignment: assignment,
			Scope: fencing.InitExecutionScope{PodName: assignment.PodName, PodUID: assignment.PodUID, ContainerName: "prepare", ContainerID: "container://one", ImageID: "image://one", NodeName: "worker-1", NodeUID: "node-uid"},
		})
	}))
	defer server.Close()
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	client := &fencing.AuthorityClient{URL: server.URL + "/v1/supervisor", TokenFile: tokenPath, Client: server.Client()}
	admission, err := client.BeginInitAdmission(context.Background(), assignment, "attempt-1")
	if err != nil {
		t.Fatalf("begin init admission: %v", err)
	}
	if err := admission.ValidateFor(assignment); err != nil {
		t.Fatalf("validate issued admission: %v", err)
	}
	changed := assignment
	changed.RequestHash = strings.Repeat("9", 64)
	if err := admission.ValidateFor(changed); err == nil {
		t.Fatal("admission accepted a different assignment")
	}
}

func managedDRProfileFixture() (types.ExecutionConfig, fencing.BootstrapAssignment) {
	config := types.ExecutionConfig{
		ClusterID: "target-cluster", NodeID: "n1", PreparedPodUID: "pod-uid",
		Members:               []quepaxa.Member{{ID: "n1", PublicKey: quepaxa.PublicKey{1}}, {ID: "n2", PublicKey: quepaxa.PublicKey{2}}},
		EnableReconfiguration: true, ObjStoreProvider: "s3", ObjStoreEndpoint: "https://s3.example",
		ObjStoreBucket: "archive", ObjStorePrefix: "rhiza", ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck,
	}
	membership := recovery.NewMembershipRecord(string(config.ClusterID), config.Members, string(config.ObjStoreDurability))
	request := recoveryanchor.Request{
		AnchorID: "anchor-1", OperationID: "dr-op", StatefulSetUID: "sts-uid", SourceClusterID: "source-cluster",
		TargetClusterID: string(config.ClusterID), SourcePrefix: "source", TargetPrefix: "rhiza/target-cluster",
		SourceMembership: strings.Repeat("a", 64), FenceHash: strings.Repeat("b", 64), TargetAnchorHash: strings.Repeat("c", 64),
		Fork:             recovery.ForkResult{Tip: 10, PrefixHash: strings.Repeat("d", 64), ManifestHash: strings.Repeat("e", 64)},
		TargetMembership: membership,
	}
	requestHash, _ := recoveryanchor.RequestHash(request)
	assignment := fencing.BootstrapAssignment{
		OperationID: "bootstrap-op", RequestHash: strings.Repeat("1", 64), Purpose: fencing.BootstrapDRSuccessor,
		RootUID: "root-uid", StatefulSetUID: "sts-uid", SourceClusterID: string(config.ClusterID),
		PodName: "rhiza-0", PodUID: config.PreparedPodUID, NodeID: string(config.NodeID),
		CredentialName: "rhiza-0-credentials", CredentialUID: "secret-uid", MembershipHash: membership.Membership,
		ProfileHash: strings.Repeat("2", 64), LedgerUID: "ledger-uid", Durability: string(config.ObjStoreDurability),
		StoreIdentityHash: managedStoreIdentityHash(&config),
		DRApproval: fencing.BootstrapDRApproval{
			Request: request,
			Receipt: recoveryanchor.Receipt{AnchorID: request.AnchorID, OperationID: request.OperationID, RequestHash: requestHash, Target: recoveryanchor.Binding{ClusterID: request.TargetClusterID, StorageID: "target-storage"}, Generation: 2},
		},
	}
	assignment.AuthorizationHash = fencing.BootstrapDRAuthorizationHash(assignment)
	return config, assignment
}
