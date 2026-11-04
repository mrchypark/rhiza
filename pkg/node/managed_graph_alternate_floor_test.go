//go:build rhiza_local_testhooks

package node

import (
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"testing"
)

func TestManagedGraphStartupReconstructionPreparedLearnerSameFloorAlternateRoot(t *testing.T) {
	fixture := newPreparedLearnerFixture(t)
	node := fixture.node
	if node.core == nil || node.core.IsVoter() || node.Ready() {
		t.Fatalf("prepared learner voted or served before membership promotion: core=%t voter=%t ready=%t", node.core != nil, node.core != nil && node.core.IsVoter(), node.Ready())
	}
	floor, floorRoot, ok := node.core.RecoveryRoot()
	if !ok || floor != fixture.cut || floorRoot != fixture.sealA.RootHash {
		t.Fatalf("startup changed physical A recovery floor: floor=%d root=%x ok=%t", floor, floorRoot, ok)
	}
	selected, exists, err := node.checkpoints.LoadManagedAppendOnlyLocator(fixture.ctx, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil || !exists || selected.RootHash != fixture.rootA || selected.Index != uint64(fixture.cut) {
		t.Fatalf("startup changed source CURRENT while installing same-floor B: current=%+v exists=%t err=%v A=%x B=%x", selected, exists, err, fixture.rootA, fixture.rootB)
	}
	if node.core.CompactionFloor() != fixture.cut || node.material == nil || node.core.Tip() != fixture.viewTip || quepaxa.Slot(node.material.Tip()) != fixture.viewTip {
		t.Fatalf("learner floor/materialization mismatch: floor=%d core=%d material=%v K=%d", node.core.CompactionFloor(), node.core.Tip(), node.material, fixture.viewTip)
	}
}
