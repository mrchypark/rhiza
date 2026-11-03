package node

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// Two actual independent singleton Cores isolate the prepared-fence conflict
// deterministically. This is not the separate public three-voter worker test.
func TestManagedGraphPeerPreparedConflictAdvancesCertifiedCut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mat, err := materializer.OpenManagedSQLKVGraphWithOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer mat.Close()
	cp, err := checkpoint.NewAppendOnlyManagerForPolicy(objstore.NewInMemBucket(), "conflict", t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	cores := make([]*quepaxa.Core, 2)
	for i := range cores {
		wal, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		cores[i], err = quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal, ValidateValue: types.ValidateManagedGraphExecutionPolicy})
		if err != nil {
			t.Fatal(err)
		}
		cores[i].SetCheckpointValidator(func(ctx context.Context, s quepaxa.CheckpointSeal) error {
			return cp.Verify(ctx, uint64(s.Index), s.RootHash, s.StateHash)
		})
	}
	value, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "same-cut", Cypher: "CREATE (:ConflictCI {n:1})"})
	if err != nil {
		t.Fatal(err)
	}
	for _, core := range cores {
		if slot, _, err := core.Propose(ctx, value); err != nil || slot != 1 {
			t.Fatal("actual certified origin", slot, err)
		}
	}
	if err := mat.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	files, cut, release, err := mat.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	sources := make([]checkpoint.Source, len(files))
	for i, f := range files {
		sources[i] = checkpoint.Source{Role: string(f.Role), Path: f.Path}
	}
	rootA, err := cp.CreateManagedGraphCandidate(ctx, sources, cut)
	if err != nil {
		t.Fatal(err)
	}
	// A harmless SQLite header setting makes a different physical snapshot at
	// the same logical/certified cut, without changing application/history rows.
	db, err := sql.Open("sqlite3", files[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	_, changeErr := db.ExecContext(ctx, "PRAGMA user_version=1")
	closeErr := db.Close()
	if changeErr != nil || closeErr != nil {
		t.Fatal(changeErr, closeErr)
	}
	rootB, err := cp.CreateManagedGraphCandidate(ctx, sources, cut)
	if err != nil {
		t.Fatal(err)
	}
	if rootA.RootHash == rootB.RootHash {
		t.Fatal("distinct physical roots missing")
	}
	prefix, _ := cores[0].PrefixHash(1)
	next, following, err := cores[0].CheckpointLeaderOrders(1)
	if err != nil {
		t.Fatal(err)
	}
	sealA := quepaxa.CheckpointSeal{ConfigID: 1, Index: 1, RootHash: rootA.RootHash, StateHash: rootA.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following}
	sealB := sealA
	sealB.RootHash, sealB.StateHash = rootB.RootHash, rootB.Hash
	if err := cores[0].PrepareCheckpoint(ctx, sealA); err != nil {
		t.Fatal(err)
	}
	if err := cores[1].PrepareCheckpoint(ctx, sealB); err != nil {
		t.Fatal(err)
	}
	peerErr := cores[1].PrepareCheckpoint(ctx, sealA)
	if peerErr == nil {
		t.Fatal("actual peer accepted different root at prepared cut")
	}
	if _, hash, ok := cores[0].LatestPreparedCheckpoint(); !ok || hash != rootA.RootHash {
		t.Fatal("local root is not the matching candidate")
	}
	n := New(&types.ExecutionConfig{})
	n.core, n.material = cores[0], mat
	n.server, err = network.NewServerForPolicy(n.core, mat, "conflict", true, nil, sqlpolicy.ManagedGraphRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer n.server.Close()
	prepareErr := fmt.Errorf("%w: prepare checkpoint: %v", quepaxa.ErrQuorumUnavailable, peerErr)
	if err := n.advanceManagedGraphFailedPrepare(ctx, prepareErr); !errors.Is(err, quepaxa.ErrQuorumUnavailable) {
		t.Fatal("original prepare failure lost", err)
	}
	if n.core.Tip() <= 1 || mat.Tip() != uint64(n.core.Tip()) {
		t.Fatalf("same-cut peer conflict made no certified progress: tip=%d applied=%d", n.core.Tip(), mat.Tip())
	}
	for i, root := range [][32]byte{rootA.RootHash, rootB.RootHash} {
		if index, hash, ok := cores[i].LatestPreparedCheckpoint(); !ok || index != 1 || hash != root {
			t.Fatal("prepared conflict evidence erased")
		}
	}
	control, ok := n.core.CertifiedValue(2)
	if !ok || sha256.Sum256(control.Value) != control.Hash || len(control.Certificate) == 0 {
		t.Fatal("new cut is not certified")
	}
	if barrier, err := types.DecodeReadBarrier(control.Value); err != nil || !barrier {
		t.Fatal("progress was not the existing read barrier control")
	}
	if _, _, err := cores[1].Propose(ctx, control.Value); err != nil {
		t.Fatal(err)
	}
	files2, cut2, release2, err := mat.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer release2()
	sources2 := make([]checkpoint.Source, len(files2))
	for i, f := range files2 {
		sources2[i] = checkpoint.Source{Role: string(f.Role), Path: f.Path}
	}
	root2, err := cp.CreateManagedGraphCandidate(ctx, sources2, cut2)
	if err != nil {
		t.Fatal(err)
	}
	prefix2, _ := n.core.PrefixHash(quepaxa.Slot(cut2))
	next2, following2, err := n.core.CheckpointLeaderOrders(quepaxa.Slot(cut2))
	if err != nil {
		t.Fatal(err)
	}
	seal2 := quepaxa.CheckpointSeal{ConfigID: 1, Index: quepaxa.Slot(cut2), RootHash: root2.RootHash, StateHash: root2.Hash, PrefixHash: prefix2, NextLeaderOrder: next2, FollowingLeaderOrder: following2}
	for _, core := range cores {
		if err := core.PrepareCheckpoint(ctx, seal2); err != nil {
			t.Fatal("higher cut did not clear the actual prepare obstruction", err)
		}
	}
}
