package node

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type managedAppendOnlySelectorMutation struct {
	operation string
	key       string
}

type managedAppendOnlySelectorBucket struct {
	objstore.Bucket
	mu        sync.Mutex
	mutations []managedAppendOnlySelectorMutation
}

func (b *managedAppendOnlySelectorBucket) Upload(ctx context.Context, key string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	b.mu.Lock()
	b.mutations = append(b.mutations, managedAppendOnlySelectorMutation{operation: "upload", key: key})
	b.mu.Unlock()
	return b.Bucket.Upload(ctx, key, reader, options...)
}

func (b *managedAppendOnlySelectorBucket) Delete(ctx context.Context, key string) error {
	b.mu.Lock()
	b.mutations = append(b.mutations, managedAppendOnlySelectorMutation{operation: "delete", key: key})
	b.mu.Unlock()
	return b.Bucket.Delete(ctx, key)
}

func (b *managedAppendOnlySelectorBucket) mutationSnapshot() []managedAppendOnlySelectorMutation {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]managedAppendOnlySelectorMutation(nil), b.mutations...)
}

func newManagedAppendOnlySelectorFixture(t *testing.T) (*Node, *quepaxa.Core, *recovery.Manager, *managedAppendOnlySelectorBucket) {
	t.Helper()
	ctx := context.Background()
	policy := sqlpolicy.ManagedGraphRequestMaterializationPolicy
	protocol := sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
	bucket := &managedAppendOnlySelectorBucket{Bucket: objstore.NewInMemBucket()}
	t.Cleanup(func() {
		if err := bucket.Close(); err != nil {
			t.Errorf("close selector fixture bucket: %v", err)
		}
	})
	checkpoints, err := checkpoint.NewAppendOnlyManagerForPolicy(bucket, "readonly-selector", t.TempDir(), policy, 1)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := recovery.NewAppendOnlyManagerWithValueValidator(bucket, "readonly-selector", 1, types.ValidateManagedGraphExecutionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(archive.Close)
	archive.SetGroupDelay(0)
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	identity := make([]byte, 32)
	for i := range identity {
		identity[i] = byte(i + 1)
	}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := wal.Close(); err != nil {
			t.Errorf("close selector fixture WAL: %v", err)
		}
	})
	if err := wal.BindIdentity(identity); err != nil {
		t.Fatal(err)
	}
	var walIdentity [32]byte
	copy(walIdentity[:], identity)
	memberID := quepaxa.NodeID("selector-voter")
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: memberID, WALIdentity: hex.EncodeToString(identity)}}}
	n := New(&types.ExecutionConfig{NodeID: memberID})
	n.managedStartup = &ManagedStartupConfig{expectedPolicy: policy, Identity: ManagedStartupIdentity{ApplicationProtocol: protocol}}
	n.checkpoints, n.archive = checkpoints, archive
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: memberID, Cluster: cluster, WAL: wal,
		ValidateValue: types.ValidateManagedGraphExecutionPolicy,
		ManagedAppendOnly: &quepaxa.ManagedAppendOnlyConfig{
			Binding: quepaxa.ManagedAppendOnlyBinding{
				ApplicationProtocol: protocol,
				BootstrapPlanDigest: fmt.Sprintf("%064x", 1), MemberConfigDigest: fmt.Sprintf("%064x", 2),
				Generation: 0, WALIdentity: walIdentity,
			},
			Verify: func(ctx context.Context, candidate quepaxa.ManagedCheckpointCandidate) error {
				return checkpoints.Verify(ctx, uint64(candidate.Seal.Index), candidate.Seal.RootHash, candidate.Seal.StateHash)
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	n.core = core
	core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
		return checkpoints.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
	})
	mat, err := materializer.OpenManagedSQLKVGraphWithAppendOnlyOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1, types.DefaultIdempotencyWindowSlots)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mat.Close(); err != nil {
			t.Errorf("close selector fixture materializer: %v", err)
		}
	})
	command, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "selector-prefix", Cypher: "CREATE (:SelectorFixture {ok:true})"})
	if err != nil {
		t.Fatal(err)
	}
	if slot, _, err := core.Propose(ctx, command); err != nil || slot != 1 {
		t.Fatalf("certify selector prefix: slot=%d err=%v", slot, err)
	}
	if err := mat.Apply(ctx, 1, command); err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, core, 1); err != nil {
		t.Fatalf("archive actual application prefix: %v", err)
	}
	files, cut, release, err := mat.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if cut != 1 || len(files) != 2 || files[0].Role != materializer.CheckpointSQLite || files[1].Role != materializer.CheckpointGraphData {
		t.Fatalf("fixture did not produce actual paired snapshot: cut=%d files=%+v", cut, files)
	}
	sources := make([]checkpoint.Source, len(files))
	for i, file := range files {
		sources[i] = checkpoint.Source{Role: string(file.Role), Path: file.Path}
	}
	root, err := checkpoints.CreateManagedAppendOnlyCandidate(ctx, sources, cut)
	if err != nil {
		t.Fatalf("upload actual immutable paired root: %v", err)
	}
	prefix, ok := core.PrefixHash(quepaxa.Slot(cut))
	if !ok {
		t.Fatal("actual selector root prefix unavailable")
	}
	next, following, err := core.CheckpointLeaderOrders(quepaxa.Slot(cut))
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{
		ConfigID: 1, Index: quepaxa.Slot(cut), RootHash: root.RootHash, StateHash: root.Hash,
		PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following,
	}
	sealValue, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	if slot, _, err := core.Propose(ctx, sealValue); err != nil || slot != 2 {
		t.Fatalf("certify actual local QCKP while archive remains at slot 1: slot=%d err=%v", slot, err)
	}
	return n, core, archive, bucket
}

func TestManagedAppendOnlyRootSelectorDoesNotPublishUnarchivedLocalSeal(t *testing.T) {
	ctx := context.Background()
	n, core, archive, bucket := newManagedAppendOnlySelectorFixture(t)
	before := bucket.mutationSnapshot()
	root, seal, err := n.selectManagedAppendOnlyRoot(ctx)
	after := bucket.mutationSnapshot()
	if len(after) != len(before) {
		t.Fatalf("read-only selection mutated provider while local seal was absent from archive: new mutations=%+v selectorErr=%v", after[len(before):], err)
	}
	if err == nil || root != nil || seal != nil {
		t.Fatalf("selection accepted unarchived local seal: root=%+v seal=%+v err=%v", root, seal, err)
	}
	if archive.Tip() != 1 || core.Tip() != 2 {
		t.Fatalf("fixture no longer has local/archive gap: Core=%d archive=%d", core.Tip(), archive.Tip())
	}

	// Explicit fixture publication creates the valid archived-root neighbor;
	// selection itself must remain read-only in that case too.
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatalf("publish actual certified QCKP for positive neighbor: %v", err)
	}
	before = bucket.mutationSnapshot()
	root, seal, err = n.selectManagedAppendOnlyRoot(ctx)
	after = bucket.mutationSnapshot()
	if err != nil || root == nil || seal == nil || seal.DecisionSlot != 2 {
		t.Fatalf("selection rejected actual archived paired QCKP: root=%+v seal=%+v err=%v", root, seal, err)
	}
	if len(after) != len(before) {
		t.Fatalf("selection mutated provider for valid archived root: new mutations=%+v", after[len(before):])
	}
}
