package recovery

import (
	"context"
	"fmt"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// InitializeCertifiedRecoveryBase verifies and installs the selected archive's
// generation anchor and checkpoint base into a passive observer. Expected
// lineage inputs must come from trusted registration or authorization, not
// from the selected archive view itself.
func InitializeCertifiedRecoveryBase(
	ctx context.Context,
	core *quepaxa.Core,
	view CertifiedArchiveView,
	expectedMembership MembershipRecord,
	expectedAnchorHash [32]byte,
	checkpoints *checkpoint.Manager,
) (*checkpoint.Checkpoint, error) {
	if core == nil || view.manager == nil || view.head.Generation == 0 {
		return nil, fmt.Errorf("certified observer and archive view are required")
	}
	localAnchor := core.GenerationAnchorHash()
	hash, anchored := view.GenerationAnchor()
	var generationRoot *checkpoint.Checkpoint
	if anchored {
		if expectedAnchorHash == ([32]byte{}) || hash != expectedAnchorHash {
			return nil, fmt.Errorf("archive generation anchor differs from trusted expectation")
		}
		anchor, err := VerifyGenerationAnchorForView(ctx, view, expectedMembership, expectedAnchorHash)
		if err != nil {
			return nil, err
		}
		if localAnchor != ([32]byte{}) && localAnchor != hash {
			return nil, fmt.Errorf("archive and observer generation anchors differ")
		}
		rootHash, err := parseGenerationHash(anchor.Checkpoint.RootHash)
		if err != nil {
			return nil, err
		}
		stateHash, err := parseGenerationHash(anchor.Checkpoint.StateHash)
		if err != nil {
			return nil, err
		}
		prefixHash, err := parseGenerationHash(anchor.SourcePrefixHash)
		if err != nil {
			return nil, err
		}
		seal, _, hasRegularBase := view.RecoveryBase()
		if hasRegularBase {
			if seal.GenerationAnchorHash != hash || uint64(seal.Index) < anchor.SourceTip {
				return nil, fmt.Errorf("checkpoint has inconsistent generation lineage")
			}
		} else {
			if checkpoints == nil {
				return nil, fmt.Errorf("generation checkpoint validation is unavailable")
			}
			generationRoot, err = checkpoints.OpenRoot(ctx, anchor.Checkpoint.Index, rootHash)
			if err != nil {
				return nil, err
			}
			if err := checkpoints.Verify(ctx, generationRoot.Index, rootHash, stateHash); err != nil {
				return nil, err
			}
		}
		if localAnchor == ([32]byte{}) {
			if err := core.InstallFencedGenerationBase(ctx, quepaxa.FencedGenerationBase{
				ConfigID: 1, Index: quepaxa.Slot(anchor.SourceTip), PrefixHash: prefixHash,
				RootHash: rootHash, AnchorHash: hash,
			}); err != nil {
				return nil, err
			}
		}
	} else if expectedAnchorHash != ([32]byte{}) || localAnchor != ([32]byte{}) {
		return nil, fmt.Errorf("expected generation anchor is unavailable")
	}

	if seal, decision, ok := view.RecoveryBase(); ok {
		if core.Tip() < seal.Index {
			if err := core.RestoreCheckpointBase(ctx, seal, decision); err != nil {
				return nil, fmt.Errorf("verify certified source checkpoint base: %w", err)
			}
		} else if err := core.ValidateCheckpointBase(ctx, seal, decision); err != nil {
			return nil, fmt.Errorf("validate certified source checkpoint base: %w", err)
		}
	}
	return generationRoot, nil
}
