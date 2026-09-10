package node

import (
	"encoding/hex"
	"fmt"
	"path"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// installGenerationAnchor runs before peer listeners. A generation anchor is
// an explicit administrative trust transition, never an invented certificate.
func (n *Node) installGenerationAnchor(guard *startupRecoveryGuard) (*checkpoint.Checkpoint, error) {
	snapshot := guard.Snapshot()
	local := n.core.GenerationAnchorHash()
	if snapshot == nil {
		if local != ([32]byte{}) {
			return nil, fmt.Errorf("anchored WAL requires its shared generation evidence")
		}
		return nil, nil
	}
	hash, anchored := snapshot.GenerationAnchor()
	if !anchored {
		if local != ([32]byte{}) {
			return nil, fmt.Errorf("archive lost the WAL generation anchor")
		}
		return nil, nil
	}
	if local != ([32]byte{}) && local != hash {
		return nil, fmt.Errorf("archive and WAL generation anchors differ")
	}
	prefix := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID))
	want := recovery.NewMembershipRecord(string(n.config.ClusterID), n.config.Members, string(n.config.ObjStoreDurability))
	anchor, err := recovery.VerifyGenerationAnchor(guard.Context(), n.bucket, prefix, want, hash)
	if err != nil {
		return nil, err
	}
	var rootHash, stateHash, prefixHash [32]byte
	for _, item := range []struct {
		value string
		hash  *[32]byte
	}{{anchor.Checkpoint.RootHash, &rootHash}, {anchor.Checkpoint.StateHash, &stateHash}, {anchor.SourcePrefixHash, &prefixHash}} {
		decoded, err := hex.DecodeString(item.value)
		if err != nil || len(decoded) != 32 {
			return nil, fmt.Errorf("invalid generation hash")
		}
		copy(item.hash[:], decoded)
	}
	var root *checkpoint.Checkpoint
	if seal, _, regular := snapshot.RecoveryBase(); regular {
		if seal.GenerationAnchorHash != hash || uint64(seal.Index) < anchor.SourceTip {
			return nil, fmt.Errorf("checkpoint has inconsistent generation lineage")
		}
	} else {
		root, err = n.checkpoints.OpenRoot(guard.Context(), anchor.Checkpoint.Index, rootHash)
		if err != nil {
			return nil, err
		}
		if err := n.checkpoints.Verify(guard.Context(), root.Index, rootHash, stateHash); err != nil {
			return nil, err
		}
	}
	if local == ([32]byte{}) {
		if err := n.core.InstallFencedGenerationBase(guard.Context(), quepaxa.FencedGenerationBase{ConfigID: 1, Index: quepaxa.Slot(anchor.SourceTip), PrefixHash: prefixHash, RootHash: rootHash, AnchorHash: hash}); err != nil {
			return nil, err
		}
	}
	return root, nil
}
