package recovery

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/thanos-io/objstore"
)

// EvidenceOptions identifies the caller-trusted anchor, membership, and fork
// result that RecoverApplicationEvidence must verify before restoring state.
type EvidenceOptions struct {
	ExpectedAnchorHash   [32]byte
	ExpectedMembership   MembershipRecord
	ExpectedForkResult   ForkResult
	ExpectedSourcePrefix string
	ExpectedOperationID  string
}

// ApplicationEvidence is the verified anchor and restored tip returned by
// RecoverApplicationEvidence after the callback succeeds.
type ApplicationEvidence struct {
	Anchor     GenerationAnchor
	AnchorHash [32]byte
	Tip        uint64
}

// RecoveredMaterializer wraps a restored materializer for read-only query
// access during recovery. The underlying materializer is unexported to
// prevent callers from issuing mutations through the recovery path.
type RecoveredMaterializer struct {
	m *materializer.Materializer
}

// Query executes a read-only SQL query against the restored state.
func (rm *RecoveredMaterializer) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return rm.m.Query(ctx, query, args...)
}

// Tip returns the highest decided slot materialized into the restored state.
func (rm *RecoveredMaterializer) Tip() uint64 {
	return rm.m.Tip()
}

// RecoverApplicationEvidence reads a generation anchor from the target prefix,
// verifies it against caller-trusted expected values, pins the immutable
// checkpoint, downloads and restores the materialized state, then invokes the
// callback with a read-only RecoveredMaterializer. The callback must not
// issue bucket writes or start voter processes. After the callback returns,
// the function verifies the recovery pin is still valid and cleans up.
func RecoverApplicationEvidence(ctx context.Context, bucket objstore.Bucket, targetPrefix string, opts EvidenceOptions, fn func(context.Context, *RecoveredMaterializer) error) (ApplicationEvidence, error) {
	if fn == nil {
		return ApplicationEvidence{}, fmt.Errorf("recovery callback is required")
	}
	if opts.ExpectedAnchorHash == ([32]byte{}) || opts.ExpectedMembership.Version != MembershipRecordVersion {
		return ApplicationEvidence{}, fmt.Errorf("expected generation anchor and immutable membership are required")
	}
	if opts.ExpectedForkResult.Tip == 0 {
		return ApplicationEvidence{}, fmt.Errorf("expected fork result is required")
	}
	if opts.ExpectedSourcePrefix == "" {
		return ApplicationEvidence{}, fmt.Errorf("expected source prefix is required")
	}
	if opts.ExpectedOperationID == "" {
		return ApplicationEvidence{}, fmt.Errorf("expected operation ID is required")
	}

	// Verify the anchor against caller-trusted values and bucket state.
	anchor, err := VerifyGenerationAnchor(ctx, bucket, targetPrefix, opts.ExpectedMembership, opts.ExpectedAnchorHash)
	if err != nil {
		return ApplicationEvidence{}, fmt.Errorf("verify generation anchor: %w", err)
	}

	// Bind the anchor against the caller-provided fork result and contract.
	if anchor.SourceTip != opts.ExpectedForkResult.Tip || anchor.SourceManifest != opts.ExpectedForkResult.ManifestHash || anchor.SourcePrefixHash != opts.ExpectedForkResult.PrefixHash {
		return ApplicationEvidence{}, fmt.Errorf("generation anchor does not match expected fork result")
	}
	if anchor.SourcePrefix != opts.ExpectedSourcePrefix {
		return ApplicationEvidence{}, fmt.Errorf("generation anchor source prefix does not match")
	}
	if anchor.OperationID != opts.ExpectedOperationID {
		return ApplicationEvidence{}, fmt.Errorf("generation anchor operation ID does not match")
	}

	// Pin the immutable checkpoint root.
	pin, err := PinGenerationAnchor(ctx, bucket, targetPrefix, "recovery-evidence-"+shortHash(anchor.OperationID), forkLease)
	if err != nil {
		return ApplicationEvidence{}, fmt.Errorf("pin generation anchor: %w", err)
	}
	defer closeRootPin(pin.RecoveryPin)

	// Validate the cached checkpoint descriptor from the pin against the anchor
	// rather than re-downloading the canonical root; this preserves the GC
	// contract by not opening a second root reference.
	pinRoot, err := pin.Root()
	if err != nil {
		return ApplicationEvidence{}, fmt.Errorf("read pinned checkpoint descriptor: %w", err)
	}
	if pinRoot.Index != anchor.Checkpoint.Index {
		return ApplicationEvidence{}, fmt.Errorf("pinned checkpoint index %d does not match anchor %d", pinRoot.Index, anchor.Checkpoint.Index)
	}
	anchorRootHash, err := parseGenerationHash(anchor.Checkpoint.RootHash)
	if err != nil {
		return ApplicationEvidence{}, fmt.Errorf("parse anchor root hash: %w", err)
	}
	if pinRoot.RootHash != anchorRootHash {
		return ApplicationEvidence{}, fmt.Errorf("pinned checkpoint root hash does not match anchor")
	}
	anchorStateHash, err := parseGenerationHash(anchor.Checkpoint.StateHash)
	if err != nil {
		return ApplicationEvidence{}, fmt.Errorf("parse anchor state hash: %w", err)
	}
	if pinRoot.Hash != anchorStateHash {
		return ApplicationEvidence{}, fmt.Errorf("pinned checkpoint state hash does not match anchor")
	}

	// Start the guard to renew the pin during recovery. Reuses the existing
	// fork evidence guard with nil snapshot; it already handles nil cleanly.
	guard := newForkEvidenceGuard(ctx, nil, pin.RecoveryPin)
	parentCtx := ctx
	defer guard.Close()
	ctx = guard.Context()

	// Download checkpoint files into a private temp directory.
	dir, err := os.MkdirTemp("", "rhiza-recovery-evidence-*")
	if err != nil {
		return ApplicationEvidence{}, err
	}
	defer os.RemoveAll(dir)

	cp := checkpoint.NewManager(bucket, targetPrefix, "", 1)
	files, err := cp.DownloadAndVerifyRootFiles(ctx, pinRoot, dir)
	if err != nil {
		return ApplicationEvidence{}, fmt.Errorf("download checkpoint files: %w", err)
	}
	if len(files) == 0 {
		return ApplicationEvidence{}, fmt.Errorf("checkpoint contains no files")
	}

	// Restore the materializer locally.
	workDir := dir + "/materializer"
	if err := os.Mkdir(workDir, 0o700); err != nil {
		return ApplicationEvidence{}, err
	}
	mat, err := materializer.Open(workDir+"/sqlite.db", 1)
	if err != nil {
		return ApplicationEvidence{}, fmt.Errorf("open materializer: %w", err)
	}
	defer mat.Close()

	restoreFiles := make([]materializer.CheckpointFile, 0, len(files))
	for _, file := range files {
		restoreFiles = append(restoreFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
	}
	if err := mat.RestoreCheckpoint(ctx, restoreFiles); err != nil {
		return ApplicationEvidence{}, fmt.Errorf("restore checkpoint: %w", err)
	}

	// The restored tip must match both the anchor checkpoint index and the
	// fork result tip before the callback sees any state.
	if mat.Tip() != anchor.Checkpoint.Index {
		return ApplicationEvidence{}, fmt.Errorf("restored tip %d does not match anchor checkpoint index %d", mat.Tip(), anchor.Checkpoint.Index)
	}
	if mat.Tip() != opts.ExpectedForkResult.Tip {
		return ApplicationEvidence{}, fmt.Errorf("restored tip %d does not match fork result tip %d", mat.Tip(), opts.ExpectedForkResult.Tip)
	}

	// Invoke the callback with a read-only RecoveredMaterializer.
	rm := &RecoveredMaterializer{m: mat}
	if err := fn(ctx, rm); err != nil {
		return ApplicationEvidence{}, fmt.Errorf("recovery callback: %w", err)
	}

	// Stop the renewal goroutine first, then validate the lease was live.
	guard.Close()
	if parentCtx.Err() != nil {
		return ApplicationEvidence{}, fmt.Errorf("parent context canceled during recovery")
	}
	if err := guard.Check(); err != nil {
		return ApplicationEvidence{}, fmt.Errorf("recovery pin lost: %w", err)
	}
	// Final lease renewal proves the pin was still valid; any race between
	// goroutine stop and this check is caught by the error path.
	if err := pin.Renew(parentCtx, forkLease); err != nil {
		return ApplicationEvidence{}, fmt.Errorf("recovery pin lease expired: %w", err)
	}

	return ApplicationEvidence{Anchor: anchor, AnchorHash: opts.ExpectedAnchorHash, Tip: mat.Tip()}, nil
}
