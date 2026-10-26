package recovery

import (
	"context"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestVerifyHAArchiveForRestoreRejectsBootstrapMembershipMismatchAtAnchorTip(t *testing.T) {
	ctx := context.Background()
	bucket, _, sourceBootstrap := buildSourceWithSQL(t)
	targetMembers := []quepaxa.Member{{ID: "target", PublicKey: testPublicKey("target")}}
	wrongMembers := []quepaxa.Member{{ID: "wrong", PublicKey: testPublicKey("wrong")}}
	targetPrefix := "ha-restore-bootstrap-binding"
	targetMembership := NewMembershipRecord(targetPrefix, targetMembers, "async")
	if err := Seal(ctx, bucket, "source", "ha-restore-bootstrap-binding-source"); err != nil {
		t.Fatalf("seal source archive: %v", err)
	}
	result, err := Fork(ctx, bucket, ForkOptions{
		SourcePrefix: "source", TargetPrefix: targetPrefix,
		SourceBootstrap: sourceBootstrap,
		TargetMembers:   targetMembers, TargetMembership: targetMembership,
		OperationID: "ha-restore-bootstrap-binding",
	})
	if err != nil {
		t.Fatalf("create anchored target: %v", err)
	}
	archive := NewManager(bucket, targetPrefix, ArchiveConfigID)
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if archive.Tip() != quepaxa.Slot(result.Tip) {
		t.Fatalf("fixture tip=%d, fork source tip=%d; want anchored base equal tip", archive.Tip(), result.Tip)
	}
	archive.Close()

	wrongBootstrap := quepaxa.Cluster{ConfigID: 1, Members: wrongMembers}
	if _, err := VerifyHAArchiveForRestore(ctx, bucket, targetPrefix, wrongBootstrap, targetMembership); err == nil {
		t.Fatal("verifier accepted a bootstrap roster that differs from the generation anchor membership")
	}
}
