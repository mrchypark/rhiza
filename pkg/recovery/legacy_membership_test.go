package recovery

import (
	"context"
	"testing"
)

// TestLegacyMembershipRecordVersionIsRejected proves that the version-1
// membership record — which bound voters by a hash of the retired private peer
// token — can never re-enter an authority path. Every entry point fails closed,
// and the same members under the current version still verify.
func TestLegacyMembershipRecordVersionIsRejected(t *testing.T) {
	ctx := context.Background()
	bucket, srcPrefix, sourceBootstrap := buildSourceWithSQL(t)
	targetPrefix, opID := "legacy-target", "legacy-op"
	result, targetMembership, anchorHash := forkToTarget(t, bucket, srcPrefix, targetPrefix, opID, sourceBootstrap)

	legacy := targetMembership
	legacy.Version = 1

	anchor, _, err := ReadGenerationAnchor(ctx, bucket, targetPrefix)
	if err != nil {
		t.Fatal(err)
	}
	forged := anchor
	forged.TargetMembership = legacy
	if err := ValidateGenerationAnchor(forged); err == nil {
		t.Fatal("generation anchor validation accepted a version-1 membership record")
	}
	if _, err := VerifyGenerationAnchor(ctx, bucket, targetPrefix, legacy, anchorHash); err == nil {
		t.Fatal("generation anchor verification accepted a version-1 membership record")
	}
	called := false
	if _, err := RecoverApplicationEvidence(ctx, bucket, targetPrefix, EvidenceOptions{
		ExpectedAnchorHash: anchorHash, ExpectedMembership: legacy, ExpectedForkResult: result,
		ExpectedSourcePrefix: srcPrefix, ExpectedOperationID: opID,
	}, func(context.Context, *RecoveredMaterializer) error { called = true; return nil }); err == nil || called {
		t.Fatalf("application evidence accepted a version-1 membership record: err=%v callback=%t", err, called)
	}
	if _, err := MaterializeGeneration(ctx, bucket, result, srcPrefix, "legacy-materialize", opID, sourceBootstrap, legacy); err == nil {
		t.Fatal("generation materialization accepted a version-1 membership record")
	}
	if _, err := VerifyGenerationAnchor(ctx, bucket, targetPrefix, targetMembership, anchorHash); err != nil {
		t.Fatalf("current membership record rejected: %v", err)
	}
}
