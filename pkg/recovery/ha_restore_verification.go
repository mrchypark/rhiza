package recovery

import (
	"context"
	"fmt"
	"slices"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// HAArchiveRestoreSelection is returned only after the fixed archive prefix has been
// replayed by the existing isolated QuePaxa observer. It is an internal
// handoff to the HA init staging code, not a durable authorization or voter
// credential.
type HAArchiveRestoreSelection struct {
	SourcePrefix        string
	ArchiveConfigID     uint
	BootstrapConfigID   uint
	BootstrapMembership MembershipRecord
	Tip                 quepaxa.Slot
	PrefixHash          [32]byte
	HasCheckpoint       bool
	CheckpointIndex     uint64
	CheckpointRootHash  [32]byte
	CheckpointStateHash [32]byte
	AnchorHash          [32]byte
}

// verifyHAArchiveForRestore reads one fixed archive head, validates its
// recovery base and complete available decision suffix in a scratch observer,
// and returns the exact checkpoint files that may be staged. It does not
// publish a remote pin, open a target qlog, create a Node, start transport, or
// write object storage. If concurrent GC removes required evidence during
// this operation, verification fails closed.
func VerifyHAArchiveForRestore(ctx context.Context, bucket objstore.Bucket, prefix string, bootstrap quepaxa.Cluster, expectedMembership MembershipRecord) (HAArchiveRestoreSelection, error) {
	var result HAArchiveRestoreSelection
	if bucket == nil || prefix == "" || bootstrap.ConfigID == 0 || len(bootstrap.Members) == 0 {
		return result, fmt.Errorf("HA restore verification requires an archive and bootstrap membership")
	}
	archive := NewManager(bucket, prefix, ArchiveConfigID)
	defer archive.Close()
	if err := archive.LoadOnce(ctx); err != nil {
		return result, fmt.Errorf("capture fixed HA archive head: %w", err)
	}
	archive.mu.Lock()
	snapshot := &RecoverySnapshot{manager: archive, head: archive.head, refs: slices.Clone(archive.extents)}
	archive.mu.Unlock()

	seal, decision, hasSeal := snapshot.RecoveryBase()
	anchorHash, anchored := snapshot.GenerationAnchor()
	var anchor *GenerationAnchor
	if anchored {
		if expectedMembership.Version != MembershipRecordVersion || NewMembershipRecord(expectedMembership.Cluster, bootstrap.Members, expectedMembership.Durability) != expectedMembership {
			return result, fmt.Errorf("HA observer bootstrap membership does not match generation anchor target")
		}
		storedAnchor, storedHash, err := ReadGenerationAnchor(ctx, bucket, prefix)
		if err != nil {
			return result, fmt.Errorf("read selected HA generation anchor: %w", err)
		}
		verified, err := verifyGenerationAnchorAgainstHead(ctx, bucket, prefix, expectedMembership, anchorHash, storedAnchor, storedHash, snapshot.head)
		if err != nil {
			return result, fmt.Errorf("verify selected HA generation anchor: %w", err)
		}
		anchor = &verified
	} else if snapshot.head.Base != 0 && !hasSeal {
		return result, fmt.Errorf("HA archive has an unsupported recovery base without generation anchor")
	}
	if hasSeal && uint64(seal.Index) != uint64(snapshot.head.Base) {
		return result, fmt.Errorf("HA archive recovery seal does not match captured base")
	}
	if !hasSeal && snapshot.head.Base != 0 && !anchored {
		return result, fmt.Errorf("HA archive has no proof for its nonzero recovery base")
	}

	checkpoints := checkpoint.NewManager(bucket, prefix, "", ArchiveConfigID)
	var selectedRoot *checkpoint.Checkpoint
	if hasSeal {
		root, err := checkpoints.OpenRoot(ctx, uint64(seal.Index), seal.RootHash)
		if err != nil {
			return result, fmt.Errorf("open certified HA checkpoint root: %w", err)
		}
		if root.Hash != seal.StateHash {
			return result, fmt.Errorf("certified HA checkpoint state hash mismatch")
		}
		if err := checkpoints.Verify(ctx, root.Index, root.RootHash, seal.StateHash); err != nil {
			return result, fmt.Errorf("verify certified HA checkpoint root: %w", err)
		}
		selectedRoot = root
	} else if anchored {
		rootHash, err := parseGenerationHash(anchor.Checkpoint.RootHash)
		if err != nil {
			return result, err
		}
		stateHash, err := parseGenerationHash(anchor.Checkpoint.StateHash)
		if err != nil {
			return result, err
		}
		root, err := checkpoints.OpenRoot(ctx, anchor.Checkpoint.Index, rootHash)
		if err != nil || root.Hash != stateHash {
			return result, fmt.Errorf("HA generation anchor checkpoint is unavailable or invalid")
		}
		if err := checkpoints.Verify(ctx, root.Index, root.RootHash, stateHash); err != nil {
			return result, fmt.Errorf("verify HA generation anchor checkpoint: %w", err)
		}
		selectedRoot = root
	}

	// Match generation materialization's historical-verifier mode: without a
	// checkpoint, the full archive begins at bootstrap and can contain the
	// reconfiguration history; with a checkpoint, only membership-bearing
	// seals attest that reconfiguration history is in scope.
	verifyMembership := !hasSeal || seal.Membership != nil
	var checkpointValidator *checkpoint.Manager
	if hasSeal {
		checkpointValidator = checkpoints
	}
	var verifiedPrefix [32]byte
	var err error
	if anchor != nil {
		verifiedPrefix, err = verifyForkSnapshotWithAnchor(ctx, snapshot, seal, decision, checkpointValidator, bootstrap, verifyMembership, anchor, anchorHash)
	} else {
		verifiedPrefix, err = verifyForkSnapshotWithMode(ctx, snapshot, seal, decision, checkpointValidator, bootstrap, verifyMembership)
	}
	if err != nil {
		return result, fmt.Errorf("verify fixed HA archive history: %w", err)
	}
	if snapshot.Tip() == 0 || verifiedPrefix == ([32]byte{}) {
		return result, fmt.Errorf("HA archive has no verifiable decided history")
	}
	result = HAArchiveRestoreSelection{
		SourcePrefix: prefix, ArchiveConfigID: ArchiveConfigID, BootstrapConfigID: bootstrap.ConfigID,
		BootstrapMembership: NewMembershipRecord(expectedMembership.Cluster, bootstrap.Members, expectedMembership.Durability),
		Tip:                 snapshot.Tip(), PrefixHash: verifiedPrefix, AnchorHash: anchorHash,
	}
	if selectedRoot != nil {
		result.HasCheckpoint = true
		result.CheckpointIndex = selectedRoot.Index
		result.CheckpointRootHash = selectedRoot.RootHash
		result.CheckpointStateHash = selectedRoot.Hash
	}
	return result, nil
}
