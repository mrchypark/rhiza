package recovery

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// buildSourceWithSQL creates a source archive with a SQL user table, a graph
// mutation, and a certified checkpoint. Returns bucket, source prefix, and
// source bootstrap cluster.
func buildSourceWithSQL(t *testing.T) (objstore.Bucket, string, quepaxa.Cluster) {
	t.Helper()
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	srcPrefix := "source"
	sourceMembers := []quepaxa.Member{{ID: "old", PublicKey: testPublicKey("old")}}
	w, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "old", Cluster: quepaxa.Cluster{ConfigID: 1, Members: sourceMembers}, WAL: w})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, policySQL(t, "CREATE TABLE epoch_proof (epoch INTEGER, token TEXT)")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, policySQL(t, "INSERT INTO epoch_proof VALUES (7, 'proof-token')")); err != nil {
		t.Fatal(err)
	}
	graphVal, err := types.EncodeGraphCommand(types.GraphCommand{
		RequestID: "dedupe",
		Events:    []types.GraphStreamEvent{{Stream: "anchor", Kind: "created", Payload: "state"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, graphVal); err != nil {
		t.Fatal(err)
	}
	state, err := materializer.Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	for i := quepaxa.Slot(1); i <= core.Tip(); i++ {
		decision, ok := core.CertifiedValue(i)
		if !ok {
			t.Fatalf("missing decision at slot %d", i)
		}
		if err := state.ApplyBatch(ctx, []quepaxa.DecidedValue{decision}); err != nil {
			t.Fatal(err)
		}
	}
	cp := checkpoint.NewManager(bucket, srcPrefix, "", 1)
	claim, err := cp.AcquirePublisherClaim(ctx, srcPrefix, 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	files, index, cleanup, err := state.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	sources := make([]checkpoint.Source, 0, len(files))
	for _, f := range files {
		sources = append(sources, checkpoint.Source{Role: string(f.Role), Path: f.Path})
	}
	root, err := cp.CreateFiles(ctx, claim, sources, index)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cp.BindPublisherClaim(ctx, claim, index, root.RootHash, time.Minute); err != nil {
		t.Fatal(err)
	}
	prefix, _ := core.PrefixHash(core.Tip())
	next, following, _ := core.CheckpointLeaderOrders(core.Tip())
	seal := quepaxa.CheckpointSeal{
		ConfigID: 1, Index: core.Tip(), RootHash: root.RootHash, StateHash: root.Hash,
		PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following,
	}
	core.SetCheckpointValidator(func(context.Context, quepaxa.CheckpointSeal) error { return nil })
	if err := core.PrepareCheckpoint(ctx, seal); err != nil {
		t.Fatal(err)
	}
	encoded, _ := quepaxa.EncodeCheckpointSeal(seal)
	if _, _, err := core.Propose(ctx, encoded); err != nil {
		t.Fatal(err)
	}
	archive := NewManager(bucket, srcPrefix, 1)
	defer archive.Close()
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	sealed, ok, err := core.LatestCheckpointSeal()
	if err != nil || !ok {
		t.Fatal(err)
	}
	base, ok := core.CertifiedValue(sealed.DecisionSlot)
	if !ok {
		t.Fatal("missing seal")
	}
	if err := archive.TrimThrough(ctx, sealed, base); err != nil {
		t.Fatal(err)
	}
	if err := cp.ReleasePublisherClaim(ctx, claim); err != nil {
		t.Fatal(err)
	}
	return bucket, srcPrefix, quepaxa.Cluster{ConfigID: 1, Members: sourceMembers}
}

func forkToTarget(t *testing.T, bucket objstore.Bucket, srcPrefix, targetPrefix, opID string, sourceBootstrap quepaxa.Cluster) (ForkResult, MembershipRecord, [32]byte) {
	t.Helper()
	ctx := context.Background()
	if err := Seal(ctx, bucket, srcPrefix, opID); err != nil {
		t.Fatal(err)
	}
	targetMembers := []quepaxa.Member{{ID: "new", PublicKey: testPublicKey("new")}}
	targetMembership := NewMembershipRecord(targetPrefix, targetMembers, "async")
	result, err := Fork(ctx, bucket, ForkOptions{
		SourcePrefix: srcPrefix, TargetPrefix: targetPrefix,
		SourceBootstrap: sourceBootstrap, TargetMembers: targetMembers,
		TargetMembership: targetMembership, OperationID: opID,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, anchorHash, err := ReadGenerationAnchor(ctx, bucket, targetPrefix)
	if err != nil {
		t.Fatal(err)
	}
	return result, targetMembership, anchorHash
}

func TestRecoverApplicationEvidenceQueryProof(t *testing.T) {
	ctx := context.Background()
	bucket, srcPrefix, sourceBootstrap := buildSourceWithSQL(t)
	targetPrefix := "evidence-target"
	opID := "evidence-op"
	result, targetMembership, anchorHash := forkToTarget(t, bucket, srcPrefix, targetPrefix, opID, sourceBootstrap)

	evidence, err := RecoverApplicationEvidence(ctx, bucket, targetPrefix, EvidenceOptions{
		ExpectedAnchorHash:   anchorHash,
		ExpectedMembership:   targetMembership,
		ExpectedForkResult:   result,
		ExpectedSourcePrefix: srcPrefix,
		ExpectedOperationID:  opID,
	}, func(ctx context.Context, rm *RecoveredMaterializer) error {
		rows, err := rm.Query(ctx, "SELECT epoch, token FROM epoch_proof WHERE epoch = 7")
		if err != nil {
			return err
		}
		defer rows.Close()
		if !rows.Next() {
			return fmt.Errorf("expected one row from epoch_proof")
		}
		var epoch int
		var token string
		if err := rows.Scan(&epoch, &token); err != nil {
			return err
		}
		if epoch != 7 || token != "proof-token" {
			return fmt.Errorf("epoch_proof mismatch: epoch=%d token=%s", epoch, token)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Tip == 0 {
		t.Fatal("evidence tip should be non-zero")
	}
	if evidence.AnchorHash != anchorHash {
		t.Fatal("evidence anchor hash mismatch")
	}
}

func TestRecoverApplicationEvidenceBadHash(t *testing.T) {
	ctx := context.Background()
	bucket, srcPrefix, sourceBootstrap := buildSourceWithSQL(t)
	targetPrefix := "bad-hash"
	opID := "bad-op"
	result, targetMembership, _ := forkToTarget(t, bucket, srcPrefix, targetPrefix, opID, sourceBootstrap)

	var badHash [32]byte
	badHash[0] = 0xff
	_, err := RecoverApplicationEvidence(ctx, bucket, targetPrefix, EvidenceOptions{
		ExpectedAnchorHash:   badHash,
		ExpectedMembership:   targetMembership,
		ExpectedForkResult:   result,
		ExpectedSourcePrefix: srcPrefix,
		ExpectedOperationID:  opID,
	}, func(ctx context.Context, rm *RecoveredMaterializer) error {
		t.Fatal("callback should not be invoked on bad hash")
		return nil
	})
	if err == nil {
		t.Fatal("expected error for bad anchor hash")
	}
}

func TestRecoverApplicationEvidenceMissingAnchor(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	targetMembers := []quepaxa.Member{{ID: "new", PublicKey: testPublicKey("new")}}
	targetMembership := NewMembershipRecord("missing", targetMembers, "async")
	var badHash [32]byte
	badHash[0] = 0x01
	_, err := RecoverApplicationEvidence(ctx, bucket, "missing", EvidenceOptions{
		ExpectedAnchorHash:   badHash,
		ExpectedMembership:   targetMembership,
		ExpectedForkResult:   ForkResult{Tip: 1, PrefixHash: "a", ManifestHash: "b"},
		ExpectedSourcePrefix: "source",
		ExpectedOperationID:  "op",
	}, func(ctx context.Context, rm *RecoveredMaterializer) error {
		t.Fatal("callback should not be invoked")
		return nil
	})
	if err == nil {
		t.Fatal("expected error for missing anchor")
	}
}

func TestRecoverApplicationEvidenceBadMembership(t *testing.T) {
	ctx := context.Background()
	bucket, srcPrefix, sourceBootstrap := buildSourceWithSQL(t)
	targetPrefix := "bad-membership"
	opID := "membership-op"
	result, _, anchorHash := forkToTarget(t, bucket, srcPrefix, targetPrefix, opID, sourceBootstrap)

	wrongMembers := []quepaxa.Member{{ID: "wrong", PublicKey: testPublicKey("wrong")}}
	wrongMembership := NewMembershipRecord(targetPrefix, wrongMembers, "async")
	_, err := RecoverApplicationEvidence(ctx, bucket, targetPrefix, EvidenceOptions{
		ExpectedAnchorHash:   anchorHash,
		ExpectedMembership:   wrongMembership,
		ExpectedForkResult:   result,
		ExpectedSourcePrefix: srcPrefix,
		ExpectedOperationID:  opID,
	}, func(ctx context.Context, rm *RecoveredMaterializer) error {
		t.Fatal("callback should not be invoked on bad membership")
		return nil
	})
	if err == nil {
		t.Fatal("expected error for bad membership")
	}
}

func TestRecoverApplicationEvidenceBadForkResult(t *testing.T) {
	ctx := context.Background()
	bucket, srcPrefix, sourceBootstrap := buildSourceWithSQL(t)
	targetPrefix := "bad-fork"
	opID := "fork-op"
	_, targetMembership, anchorHash := forkToTarget(t, bucket, srcPrefix, targetPrefix, opID, sourceBootstrap)

	_, err := RecoverApplicationEvidence(ctx, bucket, targetPrefix, EvidenceOptions{
		ExpectedAnchorHash:   anchorHash,
		ExpectedMembership:   targetMembership,
		ExpectedForkResult:   ForkResult{Tip: 999, PrefixHash: "wrong", ManifestHash: "wrong"},
		ExpectedSourcePrefix: srcPrefix,
		ExpectedOperationID:  opID,
	}, func(ctx context.Context, rm *RecoveredMaterializer) error {
		t.Fatal("callback should not be invoked on bad fork result")
		return nil
	})
	if err == nil {
		t.Fatal("expected error for bad fork result")
	}
}

func TestRecoverApplicationEvidenceBadSourcePrefix(t *testing.T) {
	ctx := context.Background()
	bucket, srcPrefix, sourceBootstrap := buildSourceWithSQL(t)
	targetPrefix := "bad-source"
	opID := "source-op"
	result, targetMembership, anchorHash := forkToTarget(t, bucket, srcPrefix, targetPrefix, opID, sourceBootstrap)

	_, err := RecoverApplicationEvidence(ctx, bucket, targetPrefix, EvidenceOptions{
		ExpectedAnchorHash:   anchorHash,
		ExpectedMembership:   targetMembership,
		ExpectedForkResult:   result,
		ExpectedSourcePrefix: "wrong-source",
		ExpectedOperationID:  opID,
	}, func(ctx context.Context, rm *RecoveredMaterializer) error {
		t.Fatal("callback should not be invoked on bad source prefix")
		return nil
	})
	if err == nil {
		t.Fatal("expected error for bad source prefix")
	}
}

func TestRecoverApplicationEvidenceBadOperationID(t *testing.T) {
	ctx := context.Background()
	bucket, srcPrefix, sourceBootstrap := buildSourceWithSQL(t)
	targetPrefix := "bad-opid"
	opID := "opid-op"
	result, targetMembership, anchorHash := forkToTarget(t, bucket, srcPrefix, targetPrefix, opID, sourceBootstrap)

	_, err := RecoverApplicationEvidence(ctx, bucket, targetPrefix, EvidenceOptions{
		ExpectedAnchorHash:   anchorHash,
		ExpectedMembership:   targetMembership,
		ExpectedForkResult:   result,
		ExpectedSourcePrefix: srcPrefix,
		ExpectedOperationID:  "wrong-opid",
	}, func(ctx context.Context, rm *RecoveredMaterializer) error {
		t.Fatal("callback should not be invoked on bad operation ID")
		return nil
	})
	if err == nil {
		t.Fatal("expected error for bad operation ID")
	}
}

func TestRecoverApplicationEvidenceNilCallback(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	targetMembers := []quepaxa.Member{{ID: "new", PublicKey: testPublicKey("new")}}
	targetMembership := NewMembershipRecord("nil-cb", targetMembers, "async")
	var h [32]byte
	h[0] = 0x01
	_, err := RecoverApplicationEvidence(ctx, bucket, "nil-cb", EvidenceOptions{
		ExpectedAnchorHash:   h,
		ExpectedMembership:   targetMembership,
		ExpectedForkResult:   ForkResult{Tip: 1, PrefixHash: "a", ManifestHash: "b"},
		ExpectedSourcePrefix: "source",
		ExpectedOperationID:  "op",
	}, nil)
	if err == nil {
		t.Fatal("expected error for nil callback")
	}
}

func TestRecoverApplicationEvidenceEmptySourcePrefix(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	targetMembers := []quepaxa.Member{{ID: "new", PublicKey: testPublicKey("new")}}
	targetMembership := NewMembershipRecord("empty-src", targetMembers, "async")
	var h [32]byte
	h[0] = 0x01
	_, err := RecoverApplicationEvidence(ctx, bucket, "empty-src", EvidenceOptions{
		ExpectedAnchorHash:   h,
		ExpectedMembership:   targetMembership,
		ExpectedForkResult:   ForkResult{Tip: 1, PrefixHash: "a", ManifestHash: "b"},
		ExpectedSourcePrefix: "",
		ExpectedOperationID:  "op",
	}, func(ctx context.Context, rm *RecoveredMaterializer) error { return nil })
	if err == nil {
		t.Fatal("expected error for empty source prefix")
	}
}

func TestRecoverApplicationEvidenceEmptyOperationID(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	targetMembers := []quepaxa.Member{{ID: "new", PublicKey: testPublicKey("new")}}
	targetMembership := NewMembershipRecord("empty-opid", targetMembers, "async")
	var h [32]byte
	h[0] = 0x01
	_, err := RecoverApplicationEvidence(ctx, bucket, "empty-opid", EvidenceOptions{
		ExpectedAnchorHash:   h,
		ExpectedMembership:   targetMembership,
		ExpectedForkResult:   ForkResult{Tip: 1, PrefixHash: "a", ManifestHash: "b"},
		ExpectedSourcePrefix: "source",
		ExpectedOperationID:  "",
	}, func(ctx context.Context, rm *RecoveredMaterializer) error { return nil })
	if err == nil {
		t.Fatal("expected error for empty operation ID")
	}
}

func TestRecoverApplicationEvidenceCanceledContext(t *testing.T) {
	ctx := context.Background()
	bucket, srcPrefix, sourceBootstrap := buildSourceWithSQL(t)
	targetPrefix := "canceled"
	opID := "cancel-op"
	result, targetMembership, anchorHash := forkToTarget(t, bucket, srcPrefix, targetPrefix, opID, sourceBootstrap)

	done := make(chan struct{})
	canceledCtx, cancel := context.WithCancel(ctx)
	var cbErr error
	go func() {
		defer close(done)
		_, cbErr = RecoverApplicationEvidence(canceledCtx, bucket, targetPrefix, EvidenceOptions{
			ExpectedAnchorHash:   anchorHash,
			ExpectedMembership:   targetMembership,
			ExpectedForkResult:   result,
			ExpectedSourcePrefix: srcPrefix,
			ExpectedOperationID:  opID,
		}, func(ctx context.Context, rm *RecoveredMaterializer) error {
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done
	if cbErr == nil {
		t.Fatal("expected error from canceled context")
	}
}

func TestRecoverApplicationEvidenceReadOnlyMutation(t *testing.T) {
	ctx := context.Background()
	bucket, srcPrefix, sourceBootstrap := buildSourceWithSQL(t)
	targetPrefix := "readonly"
	opID := "readonly-op"
	result, targetMembership, anchorHash := forkToTarget(t, bucket, srcPrefix, targetPrefix, opID, sourceBootstrap)

	_, err := RecoverApplicationEvidence(ctx, bucket, targetPrefix, EvidenceOptions{
		ExpectedAnchorHash:   anchorHash,
		ExpectedMembership:   targetMembership,
		ExpectedForkResult:   result,
		ExpectedSourcePrefix: srcPrefix,
		ExpectedOperationID:  opID,
	}, func(ctx context.Context, rm *RecoveredMaterializer) error {
		// The Query API returns lazy Rows; SQLite defers write errors to row
		// iteration on read-only connections. Drain rows and check Err/Close.
		xRows, qErr := rm.Query(ctx, "INSERT INTO epoch_proof VALUES (99, 'mutation')")
		if qErr != nil {
			// Immediate rejection — acceptable.
		} else {
			for xRows.Next() {
			}
			if xRows.Err() == nil {
				xRows.Close()
				return fmt.Errorf("INSERT through read-only Query succeeded without deferred error")
			}
			xRows.Close()
		}
		// Verify the table is unchanged.
		rows, qErr := rm.Query(ctx, "SELECT count(*) FROM epoch_proof")
		if qErr != nil {
			return qErr
		}
		defer rows.Close()
		if !rows.Next() {
			return fmt.Errorf("expected count row")
		}
		var count int
		if err := rows.Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("expected 1 row after rejected INSERT, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
