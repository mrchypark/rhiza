package operator

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type lostSourceSuccessorCreateResponseBucket struct {
	objstore.Bucket
	loseNextCreateResponse bool
}

func (b *lostSourceSuccessorCreateResponseBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	params := objstore.ApplyObjectUploadOptions(options...)
	if err := b.Bucket.Upload(ctx, name, reader, options...); err != nil {
		return err
	}
	if params.IfNotExists && b.loseNextCreateResponse {
		b.loseNextCreateResponse = false
		return errors.New("injected lost create-only response")
	}
	return nil
}

func TestSourceSuccessorClaimArbitratesRecoveryModesAndJoinsExactRetry(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = bucket.Close() })
	store, err := NewAuthorityStateStore(bucket, "successor-claims")
	if err != nil {
		t.Fatal(err)
	}
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: "archive", Prefix: "source", ClusterID: "source"}
	scopeHash, err := scope.Hash()
	if err != nil {
		t.Fatal(err)
	}
	const ownerUID, recoveryUID, fenceID = "sts-uid", "recovery-uid", "closed-generation"
	operationID := recoveryUID + ":" + fenceID
	frozenTarget, err := recovery.FrozenTargetID(recovery.FrozenSourceIdentity{ScopeHash: scopeHash, ClusterID: scope.ClusterID, StatefulSetUID: ownerUID})
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	frozen := sourceSuccessorClaim{
		Version: sourceSuccessorClaimVersion, SourceScopeHash: scopeHash, SourceClusterID: scope.ClusterID,
		StatefulSetUID: ownerUID, RecoveryResourceUID: recoveryUID, RecoveryMode: RecoveryModeFrozenSource,
		TargetBaseKind: "fork", TargetClusterID: frozenTarget, OperationID: operationID,
		FenceOperationID: fenceID, ClosedRootDigest: digest, FenceDigest: digest,
	}
	legacy := frozen
	legacy.RecoveryMode = sourceSuccessorModeLegacy
	legacy.TargetClusterID = "rhiza-r-" + shortID(operationID)
	legacy.ReservationDigest = digest
	badOperation := frozen
	badOperation.OperationID += "-other"
	if err := badOperation.validate(scope); err == nil {
		t.Fatal("claim validation accepted an operation unrelated to its recovery and closed fence")
	}
	badLegacyTarget := legacy
	badLegacyTarget.TargetClusterID += "-other"
	if err := badLegacyTarget.validate(scope); err == nil {
		t.Fatal("claim validation accepted a legacy target unrelated to its operation")
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, candidate := range []sourceSuccessorClaim{frozen, legacy} {
		candidate := candidate
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- store.claimSourceSuccessor(ctx, scope, candidate)
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var winner sourceSuccessorClaim
	var successes, conflicts int
	for err := range results {
		if err == nil {
			successes++
			// The stored record identifies the winner independently of goroutine order.
			winner, err = store.readSourceSuccessorClaim(ctx, scope, ownerUID)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			conflicts++
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("cross-mode source arbitration successes=%d conflicts=%d; want exactly one each", successes, conflicts)
	}
	if err := store.claimSourceSuccessor(ctx, scope, winner); err != nil {
		t.Fatalf("exact retry did not join the immutable winner: %v", err)
	}
	other := frozen
	if winner.RecoveryMode == RecoveryModeFrozenSource {
		other = legacy
	}
	if err := store.claimSourceSuccessor(ctx, scope, other); err == nil {
		t.Fatal("different recovery mode joined an existing source successor claim")
	}

	// Reopening the store models a process crash after the winner's create-only
	// CAS but before downstream registration. The exact tuple must be resumable.
	reopened, err := NewAuthorityStateStore(bucket, "successor-claims")
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.claimSourceSuccessor(ctx, scope, winner); err != nil {
		t.Fatalf("exact post-CAS retry did not resume: %v", err)
	}
}

func TestSourceSuccessorClaimReconcilesLostCreateResponse(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = base.Close() })
	bucket := &lostSourceSuccessorCreateResponseBucket{Bucket: base, loseNextCreateResponse: true}
	store, err := NewAuthorityStateStore(bucket, "successor-claims")
	if err != nil {
		t.Fatal(err)
	}
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: "archive", Prefix: "source", ClusterID: "source"}
	scopeHash, err := scope.Hash()
	if err != nil {
		t.Fatal(err)
	}
	const ownerUID, recoveryUID, fenceID = "sts-uid", "recovery-uid", "closed-generation"
	operationID := recoveryUID + ":" + fenceID
	target, err := recovery.FrozenTargetID(recovery.FrozenSourceIdentity{ScopeHash: scopeHash, ClusterID: scope.ClusterID, StatefulSetUID: ownerUID})
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	claim := sourceSuccessorClaim{
		Version: sourceSuccessorClaimVersion, SourceScopeHash: scopeHash, SourceClusterID: scope.ClusterID,
		StatefulSetUID: ownerUID, RecoveryResourceUID: recoveryUID, RecoveryMode: RecoveryModeFrozenSource,
		TargetBaseKind: "empty", TargetClusterID: target, OperationID: operationID,
		FenceOperationID: fenceID, ClosedRootDigest: digest, FenceDigest: digest,
	}
	if err := store.claimSourceSuccessor(ctx, scope, claim); err != nil {
		t.Fatalf("ambiguous create-only response did not reconcile to the exact stored winner: %v", err)
	}
	if got, err := store.readSourceSuccessorClaim(ctx, scope, ownerUID); err != nil || got != claim {
		t.Fatalf("claim after ambiguous response=%+v err=%v", got, err)
	}
	if err := store.claimSourceSuccessor(ctx, scope, claim); err != nil {
		t.Fatalf("exact retry after ambiguous response failed: %v", err)
	}
}

func TestArchiveForkRegistrarRejectsCompetingSuccessorBeforeTargetAuthority(t *testing.T) {
	f := newForkAuthorityRegistrarFixture(t)
	ctx := t.Context()
	rootBefore, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, "source")
	if err != nil || rootBefore.Ledger.ClosedBy != f.request.SourceFenceOperationID {
		t.Fatalf("source root is not the expected closed generation: %+v err=%v", rootBefore.Ledger, err)
	}
	if _, err := f.e.registerArchiveForkGenesis(ctx, f.request); err != nil {
		t.Fatalf("initial legacy fork registration failed: %v", err)
	}
	sourceScope := fencing.ArchiveScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source", ClusterID: "source"}
	claim, err := f.e.AuthorityState.readSourceSuccessorClaim(ctx, sourceScope, f.runtime.StatefulSetUID)
	if err != nil || claim.RecoveryMode != sourceSuccessorModeLegacy || claim.TargetClusterID != f.request.TargetClusterID {
		t.Fatalf("legacy registrar did not persist its source-generation winner: %+v err=%v", claim, err)
	}
	rootAfter, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, "source")
	if err != nil || f.e.ledgerHash(rootBefore) != f.e.ledgerHash(rootAfter) {
		t.Fatalf("source successor arbitration mutated the closed fence ledger: before=%+v after=%+v err=%v", rootBefore.Ledger, rootAfter.Ledger, err)
	}

	reservationData, err := readBoundedArchiveObject(ctx, f.e.ArchiveBucket, "root/source/recovery/successor.json", 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	var competitor archiveForkReservation
	if err := decodeCanonicalArchiveJSON(reservationData, &competitor); err != nil {
		t.Fatal(err)
	}
	competitor.OperationID = "other-recovery:" + f.request.SourceFenceOperationID
	competitor.Target = "other-target"
	competitor.SpecHash = strings.Repeat("b", 64)
	competitor.TargetBaseKind = "fork"
	putForkJSON(t, f.e.ArchiveBucket, "root/source/recovery/successor.json", competitor)

	loser := f.request
	loser.OperationID = competitor.OperationID
	loser.TargetClusterID = competitor.Target
	if _, err := f.e.registerArchiveForkGenesis(ctx, loser); err == nil {
		t.Fatal("competing successor registration unexpectedly succeeded")
	}
	if _, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, loser.TargetClusterID); !errors.Is(err, errAuthorityStateNotFound) {
		t.Fatalf("losing registration created target authority before source arbitration: %v", err)
	}
}
