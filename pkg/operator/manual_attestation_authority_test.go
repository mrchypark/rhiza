package operator

import (
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestManualRecoveryApprovalRequiresCanonicalChallengeAndStableObject(t *testing.T) {
	e, fixture, _, _ := newMutationStoreFixture(t)
	now := time.Now().UTC().Truncate(time.Second)
	challenge, approval, current, _ := validManualAttestationFixture(t)
	challenge.IssuedAt, challenge.ExpiresAt = now.Add(-time.Minute), now.Add(20*time.Minute)
	digest, err := ManualRecoveryChallengeDigest(challenge)
	if err != nil {
		t.Fatal(err)
	}
	approval.CreatedAt = now
	approval.Spec.ChallengeDigest = digest
	if got, err := e.persistManualRecoveryChallenge(t.Context(), challenge); err != nil || got != digest {
		t.Fatalf("persist challenge digest=%q err=%v", got, err)
	}
	if got, err := e.validateAndPersistManualRecoveryAttestation(t.Context(), challenge, current, now); !errors.Is(err, fencing.ErrPending) || got.UID != "" {
		t.Fatalf("missing approval result=%+v err=%v, want pending", got, err)
	}

	name, err := ManualRecoveryAttestationName(challenge.RecoveryUID)
	if err != nil {
		t.Fatal(err)
	}
	apiPath := manualRecoveryAttestationPath(challenge.RecoveryNamespace, name)
	fixture.mu.Lock()
	fixture.objects[apiPath] = manualAttestationAPIObject(approval)
	fixture.mu.Unlock()
	accepted, err := e.validateAndPersistManualRecoveryAttestation(t.Context(), challenge, current, now)
	if err != nil || accepted.UID != approval.UID {
		t.Fatalf("valid approval was not accepted and persisted: %+v %v", accepted, err)
	}
	accepted, err = e.validateAndPersistManualRecoveryAttestation(t.Context(), challenge, current, now)
	if err != nil || accepted.UID != approval.UID {
		t.Fatalf("identical approval retry failed: %+v %v", accepted, err)
	}

	replacement := manualAttestationAPIObject(approval)
	asObject(replacement["metadata"])["uid"] = "replacement-api-uid"
	fixture.mu.Lock()
	fixture.objects[apiPath] = replacement
	fixture.mu.Unlock()
	if _, err := e.validateAndPersistManualRecoveryAttestation(t.Context(), challenge, current, now); !errors.Is(err, fencing.ErrClosed) {
		t.Fatalf("replacement approval UID error=%v, want closed", err)
	}
}

func TestManualRecoveryChallengeIsCreateOnly(t *testing.T) {
	e, _, _, _ := newMutationStoreFixture(t)
	challenge, _, _, _ := validManualAttestationFixture(t)
	if _, err := e.persistManualRecoveryChallenge(t.Context(), challenge); err != nil {
		t.Fatal(err)
	}
	if _, err := e.persistManualRecoveryChallenge(t.Context(), challenge); err != nil {
		t.Fatalf("identical challenge retry failed: %v", err)
	}
	challenge.LedgerDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := e.persistManualRecoveryChallenge(t.Context(), challenge); !errors.Is(err, fencing.ErrClosed) {
		t.Fatalf("changed challenge error=%v, want closed", err)
	}
}

func manualAttestationAPIObject(approval ManualRecoveryAttestationObject) object {
	return object{
		"apiVersion": "rhiza.mrchypark.dev/v1alpha1",
		"kind":       "RhizaManualRecoveryAttestation",
		"metadata": object{
			"namespace":         approval.Namespace,
			"name":              approval.Name,
			"uid":               approval.UID,
			"creationTimestamp": approval.CreatedAt.Format(time.RFC3339Nano),
		},
		"spec": approval.Spec,
	}
}
