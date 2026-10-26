package fencing

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func logicalAssignedReviewBinding(role StartupRole) LogicalWriterBindingV1 {
	assignment, wal := strings.Repeat("1", 64), strings.Repeat("2", 64)
	recovery := LogicalRecoveryBindingV1{Version: LogicalRecoveryBindingVersionV1, Kind: LogicalRecoveryReplacement,
		OperationID: "repair-operation", AssignmentID: assignment, ExpectedWALIdentity: wal,
		ReplacedNodeID: "old-node", ReplacedWALIdentity: strings.Repeat("3", 64), RemoveOperationID: "remove-operation",
		AddOperationID: "add-operation", TargetConfigID: 2, TargetMembershipHash: strings.Repeat("4", 64)}
	binding := LogicalWriterBindingV1{Version: LogicalWriterVersionV1, Profile: LogicalWriterProfileV1,
		SourceClusterID: "source", NodeID: "new-node", RunID: "run-new", AssignmentID: assignment,
		WALIdentity: wal, Role: role, Recovery: recovery}
	if role == StartupRoleVoterActive {
		binding.ProducerUID = "producer-new"
		binding.StoreScopeHash = strings.Repeat("5", 64)
		binding.Recovery.PromotionEvidenceHash = strings.Repeat("6", 64)
	}
	return binding
}

func cloneLogicalReviewBinding(binding *LogicalWriterBindingV1) *LogicalWriterBindingV1 {
	if binding == nil {
		return nil
	}
	copy := *binding
	return &copy
}

func TestAuthorityClientLogicalPromotionRequiresExactRecoveryProof(t *testing.T) {
	pending := logicalAssignedReviewBinding(StartupRoleLearnerPendingRemove)
	id := Identity{SourceClusterID: pending.SourceClusterID, NodeID: pending.NodeID, RunID: pending.RunID,
		AssignmentID: pending.AssignmentID, WALIdentity: pending.WALIdentity}
	valid := logicalAssignedReviewBinding(StartupRoleVoterActive)
	producer := &ProducerBinding{ContractVersion: MutationIntentVersion, Profile: LogicalWriterProfileV1,
		LogicalBinding: &valid, ProducerUID: valid.ProducerUID, StoreScopeHash: valid.StoreScopeHash,
		ExecutionProfile: MutationExecutionProfile, StageIncarnation: strings.Repeat("7", 64)}
	baseline := valid

	tests := []struct {
		name    string
		mutate  func(*BindReceipt)
		wantErr bool
	}{
		{name: "valid promotion"},
		{name: "missing promotion evidence", mutate: func(r *BindReceipt) { r.LogicalBinding.Recovery.PromotionEvidenceHash = "" }, wantErr: true},
		{name: "changed recovery lineage", mutate: func(r *BindReceipt) { r.LogicalBinding.Recovery.RemoveOperationID = "different-remove" }, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			producerCopy := *producer
			producerCopy.LogicalBinding = cloneLogicalReviewBinding(&valid)
			receipt := BindReceipt{Identity: id, Role: StartupRoleVoterActive, AuthorityProfile: LogicalWriterProfileV1,
				LogicalBinding: cloneLogicalReviewBinding(&valid), Producer: &producerCopy}
			if tc.mutate != nil {
				tc.mutate(&receipt)
				if receipt.Producer != nil && receipt.Producer.LogicalBinding != nil {
					*receipt.Producer.LogicalBinding = *receipt.LogicalBinding
				}
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Errorf("method=%s, want POST", r.Method)
				}
				_ = json.NewEncoder(w).Encode(receipt)
			}))
			defer server.Close()
			tokenPath := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenPath, []byte("review-token"), 0o600); err != nil {
				t.Fatal(err)
			}
			client := &AuthorityClient{URL: server.URL, TokenFile: tokenPath, Client: server.Client(),
				AuthorityProfile: LogicalWriterProfileV1, boundIdentity: id, role: StartupRoleLearnerPendingRemove,
				logicalBinding: cloneLogicalReviewBinding(&pending)}
			got, err := client.PromoteLearner(context.Background(), id)
			if (err != nil) != tc.wantErr {
				t.Fatalf("PromoteLearner() error=%v, wantErr=%v", err, tc.wantErr)
			}
			if tc.wantErr {
				if client.role != StartupRoleLearnerPendingRemove {
					t.Fatalf("invalid receipt changed local role to %q", client.role)
				}
				if valid != baseline || *producer.LogicalBinding != baseline {
					t.Fatal("negative receipt fixture mutated the shared valid promotion baseline")
				}
				return
			}
			if client.role != StartupRoleVoterActive || client.logicalBinding == nil || *client.logicalBinding != valid || got.LogicalBinding == nil {
				t.Fatalf("valid promotion did not install exact active binding: role=%q binding=%+v", client.role, client.logicalBinding)
			}
			if valid != baseline || *producer.LogicalBinding != baseline {
				t.Fatal("valid receipt fixture mutated the shared promotion baseline")
			}
		})
	}
}

func TestLogicalMutationProducerBindsAssignmentIdentity(t *testing.T) {
	binding := logicalAssignedReviewBinding(StartupRoleVoterActive)
	wrong := Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, RunID: binding.RunID,
		AssignmentID: strings.Repeat("8", 64), WALIdentity: binding.WALIdentity}

	activated, err := NewLogicalMutationProducer(binding)
	if err != nil {
		t.Fatal(err)
	}
	if err := activated.Activate(wrong); err == nil {
		t.Fatal("Activate accepted an identity with a different assignment")
	}

	unbound := binding
	unbound.WALIdentity = ""
	producer, err := NewLogicalMutationProducer(unbound)
	if err != nil {
		t.Fatal(err)
	}
	wrong.WALIdentity = binding.Recovery.ExpectedWALIdentity
	if err := producer.BindWAL(wrong); err == nil {
		t.Fatal("BindWAL accepted an identity with a different assignment")
	}

	if got := activated.identity(binding.WALIdentity); got.AssignmentID != binding.AssignmentID {
		t.Fatalf("synthesized identity lost assignment: got=%+v want assignment=%q", got, binding.AssignmentID)
	}
}
