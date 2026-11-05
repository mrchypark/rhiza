//go:build rhiza_local_testhooks

package node

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
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
	latest, exists, err := node.core.LatestManagedCheckpointSeal()
	if err != nil || !exists || latest.DecisionSlot != fixture.sealB.DecisionSlot || latest.RootHash != fixture.rootB {
		t.Fatalf("learner Core does not retain the exact same-floor B producer: latest=%+v exists=%t err=%v B=%+v root=%x", latest, exists, err, fixture.sealB, fixture.rootB)
	}
	db, err := sql.Open("sqlite3", filepath.Join(fixture.dataDir, "sqlite.db"))
	if err != nil {
		t.Fatal("open installed learner SQLite marker", err)
	}
	var installedUserVersion int
	queryErr := db.QueryRowContext(fixture.ctx, "PRAGMA user_version").Scan(&installedUserVersion)
	closeErr := db.Close()
	if queryErr != nil || closeErr != nil || installedUserVersion != 1 {
		t.Fatalf("learner did not install B's SQLite marker: user_version=%d query_err=%v close_err=%v", installedUserVersion, queryErr, closeErr)
	}
}

func TestManagedGraphStartupPreparedLearnerAdoptsHBeforeLaterCurrentAndJ(t *testing.T) {
	fixture := newPreparedLearnerFixture(t, true)
	node := fixture.node
	if node.core == nil || node.core.IsVoter() || node.Ready() {
		t.Fatalf("prepared learner voted or served during H-to-J recovery: core=%t voter=%t ready=%t", node.core != nil, node.core != nil && node.core.IsVoter(), node.Ready())
	}
	if fixture.suffixJ <= fixture.viewTip || node.core.Tip() != fixture.suffixJ || node.material == nil || quepaxa.Slot(node.material.Tip()) != fixture.suffixJ {
		t.Fatalf("learner did not catch up from exact H through actual certified J: H=%d J=%d core=%d material=%v", fixture.viewTip, fixture.suffixJ, node.core.Tip(), node.material)
	}
	adoption, err := loadPreparedAdoption(filepath.Join(fixture.dataDir, "prepared-adoption.json"))
	if err != nil || adoption.Tip != fixture.viewTip {
		t.Fatalf("durable adoption boundary moved from exact H: adoption=%+v H=%d err=%v", adoption, fixture.viewTip, err)
	}
	floor, floorRoot, ok := node.core.RecoveryRoot()
	if !ok || floor != fixture.cut || floorRoot != fixture.sealA.RootHash || node.core.CompactionFloor() != fixture.cut {
		t.Fatalf("later CURRENT changed physical A recovery floor: floor=%d root=%x ok=%t", floor, floorRoot, ok)
	}
	current, exists, err := node.checkpoints.LoadManagedAppendOnlyLocator(fixture.ctx, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil || !exists || current.RootHash != fixture.currentRoot || current.SealDecisionSlot != uint64(fixture.currentSeal.DecisionSlot) {
		t.Fatalf("learner overwrote or lost the actual later CURRENT C: current=%+v exists=%t err=%v C=%+v", current, exists, err, fixture.currentSeal)
	}
	latest, exists, err := node.core.LatestManagedCheckpointSeal()
	if err != nil || !exists || latest.DecisionSlot != fixture.currentSeal.DecisionSlot || latest.RootHash != fixture.currentRoot {
		t.Fatalf("learner did not independently accept the actual certified CURRENT C suffix: latest=%+v exists=%t err=%v C=%+v", latest, exists, err, fixture.currentSeal)
	}
	decision, certified := node.core.CertifiedValue(fixture.suffixJ)
	if !certified || decision.Slot != fixture.suffixJ {
		t.Fatalf("learner did not retain the actual J certificate: decision=%+v certified=%t", decision, certified)
	}
	rows, err := node.material.GraphQuery(fixture.ctx, "MATCH (n:ManagedGraphResume) RETURN n.id", nil)
	if err != nil || len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || rows.Rows[0][0] != "suffix-j" {
		t.Fatalf("Graph readback did not include the actual J suffix after H adoption: rows=%+v err=%v", rows.Rows, err)
	}
}
