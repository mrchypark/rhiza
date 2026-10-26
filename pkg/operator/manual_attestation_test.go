package operator

import (
	"strings"
	"testing"
	"time"
)

func TestValidateManualRecoveryAttestation(t *testing.T) {
	challenge, approval, current, now := validManualAttestationFixture(t)
	if err := ValidateManualRecoveryAttestation(challenge, approval, current, "", now); err != nil {
		t.Fatalf("valid approval rejected: %v", err)
	}
	if err := ValidateManualRecoveryAttestation(challenge, approval, current, approval.UID, now); err != nil {
		t.Fatalf("same immutable API object rejected on replay validation: %v", err)
	}
}

func TestValidateManualRecoveryAttestationRejectsChangedAuthority(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ManualRecoveryChallenge, *ManualRecoveryAttestationObject, *ManualRecoveryBinding, *time.Time)
	}{
		{
			name: "recovery UID changed",
			mutate: func(_ *ManualRecoveryChallenge, _ *ManualRecoveryAttestationObject, current *ManualRecoveryBinding, _ *time.Time) {
				current.RecoveryUID = "different-uid"
			},
		},
		{
			name: "policy or data-loss choice changed",
			mutate: func(_ *ManualRecoveryChallenge, _ *ManualRecoveryAttestationObject, current *ManualRecoveryBinding, _ *time.Time) {
				current.PolicyDigest = strings.Repeat("b", 64)
			},
		},
		{
			name: "canonical root changed",
			mutate: func(_ *ManualRecoveryChallenge, _ *ManualRecoveryAttestationObject, current *ManualRecoveryBinding, _ *time.Time) {
				current.RootDigest = strings.Repeat("b", 64)
			},
		},
		{
			name: "cohort changed",
			mutate: func(_ *ManualRecoveryChallenge, _ *ManualRecoveryAttestationObject, current *ManualRecoveryBinding, _ *time.Time) {
				current.CohortDigest = strings.Repeat("b", 64)
			},
		},
		{
			name: "storage scope changed",
			mutate: func(_ *ManualRecoveryChallenge, _ *ManualRecoveryAttestationObject, current *ManualRecoveryBinding, _ *time.Time) {
				current.StorageScopeDigest = strings.Repeat("b", 64)
			},
		},
		{
			name: "source archive changed",
			mutate: func(_ *ManualRecoveryChallenge, _ *ManualRecoveryAttestationObject, current *ManualRecoveryBinding, _ *time.Time) {
				current.SourceArchiveDigest = strings.Repeat("b", 64)
			},
		},
		{
			name: "target membership changed",
			mutate: func(_ *ManualRecoveryChallenge, _ *ManualRecoveryAttestationObject, current *ManualRecoveryBinding, _ *time.Time) {
				current.TargetMembershipHash = strings.Repeat("b", 64)
			},
		},
		{
			name: "fence request hash changed",
			mutate: func(_ *ManualRecoveryChallenge, _ *ManualRecoveryAttestationObject, current *ManualRecoveryBinding, _ *time.Time) {
				current.FenceRequestHash = strings.Repeat("b", 64)
			},
		},
		{
			name: "source StatefulSet incarnation changed",
			mutate: func(_ *ManualRecoveryChallenge, _ *ManualRecoveryAttestationObject, current *ManualRecoveryBinding, _ *time.Time) {
				current.StatefulSetUID = "replacement-statefulset-uid"
			},
		},
		{
			name: "current Pod inventory changed",
			mutate: func(_ *ManualRecoveryChallenge, _ *ManualRecoveryAttestationObject, current *ManualRecoveryBinding, _ *time.Time) {
				current.CurrentPodUIDs = []string{"pod-a", "pod-c"}
			},
		},
		{
			name: "expired challenge",
			mutate: func(_ *ManualRecoveryChallenge, _ *ManualRecoveryAttestationObject, _ *ManualRecoveryBinding, now *time.Time) {
				*now = now.Add(time.Hour)
			},
		},
		{
			name: "approval created after expiry",
			mutate: func(challenge *ManualRecoveryChallenge, approval *ManualRecoveryAttestationObject, _ *ManualRecoveryBinding, _ *time.Time) {
				approval.CreatedAt = challenge.ExpiresAt
			},
		},
		{
			name: "approval object replaced",
			mutate: func(_ *ManualRecoveryChallenge, approval *ManualRecoveryAttestationObject, _ *ManualRecoveryBinding, _ *time.Time) {
				approval.UID = "replacement-object-uid"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			challenge, approval, current, now := validManualAttestationFixture(t)
			tt.mutate(&challenge, &approval, &current, &now)
			if err := ValidateManualRecoveryAttestation(challenge, approval, current, "api-object-uid", now); err == nil {
				t.Fatal("changed authority was accepted")
			}
		})
	}
}

func TestValidateManualRecoveryAttestationRejectsMalformedApproval(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ManualRecoveryChallenge, *ManualRecoveryAttestationObject)
	}{
		{
			name: "wrong deterministic object name",
			mutate: func(_ *ManualRecoveryChallenge, approval *ManualRecoveryAttestationObject) {
				approval.Name = "other"
			},
		},
		{
			name: "wrong namespace",
			mutate: func(_ *ManualRecoveryChallenge, approval *ManualRecoveryAttestationObject) {
				approval.Namespace = "other-namespace"
			},
		},
		{
			name: "wrong challenge digest",
			mutate: func(_ *ManualRecoveryChallenge, approval *ManualRecoveryAttestationObject) {
				approval.Spec.ChallengeDigest = strings.Repeat("b", 64)
			},
		},
		{
			name: "false terminal assertion",
			mutate: func(_ *ManualRecoveryChallenge, approval *ManualRecoveryAttestationObject) {
				approval.Spec.Assertions.AllHistoricalRunsTerminated = false
			},
		},
		{
			name: "missing evidence reference",
			mutate: func(_ *ManualRecoveryChallenge, approval *ManualRecoveryAttestationObject) {
				approval.Spec.EvidenceRefs = nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			challenge, approval, current, now := validManualAttestationFixture(t)
			tt.mutate(&challenge, &approval)
			if err := ValidateManualRecoveryAttestation(challenge, approval, current, "", now); err == nil {
				t.Fatal("malformed approval was accepted")
			}
		})
	}
}

func TestManualRecoveryChallengeAndPodInventoryAreDeterministic(t *testing.T) {
	challenge, _, _, _ := validManualAttestationFixture(t)
	digest, err := ManualRecoveryChallengeDigest(challenge)
	if err != nil {
		t.Fatal(err)
	}
	if len(digest) != 64 {
		t.Fatalf("challenge digest length=%d, want 64", len(digest))
	}
	challenge.ExpiresAt = challenge.IssuedAt.Add(manualAttestationMaxLifetime + time.Second)
	if _, err := ManualRecoveryChallengeDigest(challenge); err == nil {
		t.Fatal("challenge lifetime longer than one hour accepted")
	}
	canonical, err := CanonicalizePodUIDs([]string{"pod-c", "pod-a", "pod-b"})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(canonical, ","); got != "pod-a,pod-b,pod-c" {
		t.Fatalf("canonical Pod inventory=%q", got)
	}
	if _, err := CanonicalizePodUIDs([]string{"pod-a", "pod-a"}); err == nil {
		t.Fatal("duplicate Pod UID accepted")
	}
	empty, err := CanonicalizePodUIDs(nil)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("empty inventory should represent zero current Pods: %#v, %v", empty, err)
	}
}

func validManualAttestationFixture(t *testing.T) (ManualRecoveryChallenge, ManualRecoveryAttestationObject, ManualRecoveryBinding, time.Time) {
	t.Helper()
	now := time.Date(2026, time.October, 23, 12, 0, 0, 0, time.UTC)
	binding := ManualRecoveryBinding{
		EvidenceKind:         ManualTerminalEvidenceKind,
		RecoveryNamespace:    "rhiza-test",
		RecoveryName:         "recovery-1",
		RecoveryUID:          "recovery-object-uid",
		RecoveryID:           "recovery-operation-1",
		OperationID:          "fence-operation-1",
		SourceClusterID:      "source-cluster",
		StatefulSetName:      "rhiza",
		StatefulSetUID:       "source-statefulset-uid",
		FenceRequestHash:     strings.Repeat("8", 64),
		PolicyDigest:         strings.Repeat("1", 64),
		RootDigest:           strings.Repeat("2", 64),
		LedgerDigest:         strings.Repeat("3", 64),
		CohortDigest:         strings.Repeat("4", 64),
		StorageScopeDigest:   strings.Repeat("5", 64),
		SourceArchiveDigest:  strings.Repeat("6", 64),
		TargetClusterID:      "target-cluster",
		TargetBaseKind:       "fork",
		TargetMembershipHash: strings.Repeat("7", 64),
		CurrentPodUIDs:       []string{"pod-a", "pod-b", "pod-c"},
	}
	challenge := ManualRecoveryChallenge{
		Version:               manualAttestationVersion,
		ManualRecoveryBinding: binding,
		IssuedAt:              now.Add(-time.Minute),
		ExpiresAt:             now.Add(30 * time.Minute),
	}
	digest, err := ManualRecoveryChallengeDigest(challenge)
	if err != nil {
		t.Fatal(err)
	}
	name, err := ManualRecoveryAttestationName(binding.RecoveryUID)
	if err != nil {
		t.Fatal(err)
	}
	approval := ManualRecoveryAttestationObject{
		Namespace: "rhiza-test",
		Name:      name,
		UID:       "api-object-uid",
		CreatedAt: now,
		Spec: ManualRecoveryAttestationSpec{
			Version:               manualAttestationVersion,
			ChallengeDigest:       digest,
			ManualRecoveryBinding: binding,
			Assertions: ManualRecoveryAssertions{
				AllHistoricalRunsTerminated:        true,
				RecreationBlocked:                  true,
				OldPeerAndClientWriteAccessBlocked: true,
				OldArchiveWriterAccessRevoked:      true,
				StorageQuiesced:                    true,
			},
			Attester:     "incident-ticket-123",
			EvidenceRefs: []string{"incident-evidence-1"},
		},
	}
	return challenge, approval, binding, now
}
