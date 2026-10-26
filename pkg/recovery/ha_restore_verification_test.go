package recovery

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

type denyArchiveMutationBucket struct{ objstore.Bucket }

func (denyArchiveMutationBucket) Upload(context.Context, string, io.Reader, ...objstore.ObjectUploadOption) error {
	return fmt.Errorf("restore verifier attempted object-store upload")
}

func (denyArchiveMutationBucket) Delete(context.Context, string) error {
	return fmt.Errorf("restore verifier attempted object-store delete")
}

func TestVerifyHAArchiveForRestoreReusesCertifiedCheckpointObserverReadOnly(t *testing.T) {
	ctx := context.Background()
	bucket, prefix, bootstrap := buildSourceWithSQL(t)
	readOnly := denyArchiveMutationBucket{Bucket: bucket}
	selection, err := VerifyHAArchiveForRestore(ctx, readOnly, prefix, bootstrap, MembershipRecord{})
	if err != nil {
		t.Fatalf("verify HA archive: %v", err)
	}
	if !selection.HasCheckpoint || selection.SourcePrefix != prefix || selection.BootstrapConfigID != bootstrap.ConfigID || selection.Tip == 0 ||
		selection.CheckpointIndex == 0 || selection.CheckpointRootHash == ([32]byte{}) || selection.CheckpointStateHash == ([32]byte{}) || selection.PrefixHash == ([32]byte{}) {
		t.Fatalf("verified selection is incomplete: %+v", selection)
	}
	wrong := bootstrap
	wrong.Members = []quepaxa.Member{{ID: "wrong", PublicKey: testPublicKey("wrong")}}
	if _, err := VerifyHAArchiveForRestore(ctx, readOnly, prefix, wrong, MembershipRecord{}); err == nil {
		t.Fatal("observer accepted archive history under an incorrect bootstrap membership")
	}
}

func TestVerifyHAArchiveForRestoreUsesAnchorAndCheckpointlessReconfigurationMode(t *testing.T) {
	ctx := context.Background()
	bucket, _, _ := buildSourceWithSQL(t)
	sourceMembers := []quepaxa.Member{{ID: "old", PublicKey: testPublicKey("old")}}
	targetMembers := []quepaxa.Member{{ID: "new", PublicKey: testPublicKey("new")}}
	targetPrefix := "ha-restore-anchor-target"
	wantMembership := NewMembershipRecord(targetPrefix, targetMembers, "async")
	if err := Seal(ctx, bucket, "source", "ha-restore-anchor-source"); err != nil {
		t.Fatalf("seal source archive: %v", err)
	}
	if _, err := Fork(ctx, bucket, ForkOptions{
		SourcePrefix: "source", TargetPrefix: targetPrefix,
		SourceBootstrap: quepaxa.Cluster{ConfigID: 1, Members: sourceMembers}, TargetMembers: targetMembers,
		TargetMembership: wantMembership, OperationID: "ha-restore-anchor",
	}); err != nil {
		t.Fatalf("create anchored target: %v", err)
	}
	selection, err := VerifyHAArchiveForRestore(ctx, denyArchiveMutationBucket{Bucket: bucket}, targetPrefix,
		quepaxa.Cluster{ConfigID: 1, Members: targetMembers}, wantMembership)
	if err != nil {
		t.Fatalf("verify anchored target: %v", err)
	}
	if !selection.HasCheckpoint || selection.AnchorHash == ([32]byte{}) || selection.CheckpointIndex == 0 || selection.CheckpointStateHash == ([32]byte{}) {
		t.Fatalf("anchored restore selection is incomplete: %+v", selection)
	}
	wrongMembership := NewMembershipRecord(targetPrefix, []quepaxa.Member{{ID: "impostor", PublicKey: testPublicKey("impostor")}}, "async")
	if _, err := VerifyHAArchiveForRestore(ctx, denyArchiveMutationBucket{Bucket: bucket}, targetPrefix,
		quepaxa.Cluster{ConfigID: 1, Members: targetMembers}, wrongMembership); err == nil {
		t.Fatal("generation anchor accepted a different target membership")
	}
	wrongBootstrap := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "impostor", PublicKey: testPublicKey("impostor")}}}
	if _, err := VerifyHAArchiveForRestore(ctx, denyArchiveMutationBucket{Bucket: bucket}, targetPrefix, wrongBootstrap, wantMembership); err == nil {
		t.Fatal("generation anchor base-only replay accepted the wrong bootstrap membership")
	}

	// A base-zero archive has no checkpoint seal from which to infer whether
	// transition history is present. Keep the existing fork materializer's mode:
	// reconfiguration is enabled for observer replay from the bootstrap prefix.
	baseZero, initial := buildBaseZeroReconfigurationArchive(t, ctx)
	verified, err := VerifyHAArchiveForRestore(ctx, denyArchiveMutationBucket{Bucket: baseZero}, "base-zero-ha-restore", initial, MembershipRecord{})
	if err != nil {
		t.Fatalf("verify checkpointless reconfiguration archive: %v", err)
	}
	if verified.HasCheckpoint || verified.Tip == 0 || verified.PrefixHash == ([32]byte{}) {
		t.Fatalf("checkpointless archive selection=%+v, want verified history without a checkpoint", verified)
	}
}

func TestVerifyHAArchiveForRestoreRejectsLostCheckpointEvidence(t *testing.T) {
	ctx := context.Background()
	bucket, prefix, bootstrap := buildSourceWithSQL(t)
	archive := NewManager(bucket, prefix, ArchiveConfigID)
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	seal, _, ok := archive.RecoveryBase()
	archive.Close()
	if !ok || seal.RootHash == ([32]byte{}) {
		t.Fatal("fixture is missing certified checkpoint")
	}
	root := fmt.Sprintf("checkpoint/roots/%020d_%x.json", seal.Index, seal.RootHash)
	if err := bucket.Delete(ctx, prefix+"/"+root); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyHAArchiveForRestore(ctx, denyArchiveMutationBucket{Bucket: bucket}, prefix, bootstrap, MembershipRecord{}); err == nil {
		t.Fatal("archive with missing checkpoint root was accepted")
	}
}

func buildBaseZeroReconfigurationArchive(t *testing.T, ctx context.Context) (objstore.Bucket, quepaxa.Cluster) {
	t.Helper()
	bucket := objstore.NewInMemBucket()
	initialMembers := []quepaxa.Member{{ID: "ra", PublicKey: testPublicKey("ra")}, {ID: "rb", PublicKey: testPublicKey("rb")}}
	transport := newArchiveTestCluster(t, initialMembers)
	source := transport.cores["ra"]
	if _, err := source.BeginReconfiguration(ctx, quepaxa.Cluster{ConfigID: 2, Members: initialMembers[:1]}); err != nil {
		t.Fatal(err)
	}
	if err := source.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	archive := NewManager(bucket, "base-zero-ha-restore", ArchiveConfigID)
	defer archive.Close()
	if err := archive.SyncThrough(ctx, source, source.Tip()); err != nil {
		t.Fatal(err)
	}
	return bucket, quepaxa.Cluster{ConfigID: 1, Members: initialMembers}
}
