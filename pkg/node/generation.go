package node

import (
	"fmt"

	"github.com/mrchypark/rhiza/pkg/checkpoint"
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
	_, anchored := snapshot.GenerationAnchor()
	_, _, hasBase := snapshot.RecoveryBase()
	if snapshot.Tip() == 0 && local == ([32]byte{}) && !anchored && !hasBase {
		return nil, nil
	}
	view, err := snapshot.CaptureCertifiedView()
	if err != nil {
		return nil, err
	}
	hash, anchored := view.GenerationAnchor()
	if local != ([32]byte{}) && (!anchored || local != hash) {
		return nil, fmt.Errorf("archive and WAL generation anchors differ")
	}
	var expectedAnchor [32]byte
	if anchored {
		expectedAnchor = hash
	}
	want := recovery.NewMembershipRecord(string(n.config.ClusterID), n.config.Members, string(n.config.ObjStoreDurability))
	// This public startup path preserves its existing WAL/archive anchor check;
	// authority-side verification must instead supply an independently approved anchor.
	return recovery.InitializeCertifiedRecoveryBase(guard.Context(), n.core, view, want, expectedAnchor, n.checkpoints)
}
