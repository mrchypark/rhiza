package fencing

import (
	"encoding/json"
	"testing"
)

func TestReplacementNodeIDIsExactPodUIDBinding(t *testing.T) {
	got, err := ReplacementNodeID("a-uid-123")
	if err != nil || got != "pod-a-uid-123" {
		t.Fatalf("ReplacementNodeID() = %q, %v", got, err)
	}
	for _, invalid := range []string{"", " uid", "uid ", "../uid", "uid/name", "uid_name"} {
		if _, err := ReplacementNodeID(invalid); err == nil {
			t.Errorf("ReplacementNodeID(%q) accepted malformed UID", invalid)
		}
	}
}

func TestIdentityValidatesLegacyAndReplacementBindings(t *testing.T) {
	legacy := Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "uid", RunID: "run"}
	if err := legacy.Validate(); err != nil {
		t.Fatalf("legacy identity rejected: %v", err)
	}
	nodeID, _ := ReplacementNodeID("uid")
	replacement := Identity{SourceClusterID: "source", NodeID: nodeID, PodName: "rhiza-0", PodUID: "uid", CredentialUID: "secret-uid", CredentialName: "credential", RunID: "run"}
	if err := replacement.Validate(); err != nil {
		t.Fatalf("replacement identity rejected: %v", err)
	}
	for name, bad := range map[string]Identity{
		"name used as replacement ID": {SourceClusterID: "source", NodeID: "rhiza-0", PodName: "rhiza-0", PodUID: "uid", CredentialUID: "secret-uid", CredentialName: "credential", RunID: "run"},
		"credential missing":          {SourceClusterID: "source", NodeID: nodeID, PodName: "rhiza-0", PodUID: "uid", CredentialUID: "secret-uid", RunID: "run"},
		"legacy credential":           {SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "uid", CredentialUID: "secret-uid", CredentialName: "credential", RunID: "run"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := bad.Validate(); err == nil {
				t.Fatal("invalid identity accepted")
			}
		})
	}
}

func TestValidatePodBindingAcceptsOnlyLegacyOrExactUIDID(t *testing.T) {
	if err := ValidatePodBinding("rhiza-0", "rhiza-0", "uid"); err != nil {
		t.Fatal(err)
	}
	replacement, _ := ReplacementNodeID("uid")
	if err := ValidatePodBinding(replacement, "rhiza-0", "uid"); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePodBinding("pod-other", "rhiza-0", "uid"); err == nil {
		t.Fatal("different UID-derived identity accepted")
	}
}

func TestRegistrationValidationUsesAuthenticatedPodNameWhenCandidateOmitsIt(t *testing.T) {
	candidate := Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "uid-1", RunID: "run-1"}
	replacement := Registration{Identity: Identity{
		SourceClusterID: "source", NodeID: "pod-uid-1", PodName: "rhiza-0", PodUID: "uid-1",
		CredentialUID: "credential-uid", CredentialName: "rhiza-inc-1", RunID: "run-1",
	}, PeerToken: "peer-secret", Learner: json.RawMessage(`{"node_id":"pod-uid-1"}`)}
	if err := replacement.ValidateFor(candidate); err != nil {
		t.Fatalf("authenticated replacement registration rejected: %v", err)
	}
	wrongName := replacement
	wrongName.Identity.PodName = "rhiza-1"
	if err := wrongName.ValidateFor(candidate); err == nil {
		t.Fatal("accepted a Pod name unrelated to the startup candidate")
	}
}
