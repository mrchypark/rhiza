package materializer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	latticedb "github.com/mrchypark/latticedb-go"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func graphOriginalMaterializer(t *testing.T, path string) *Materializer {
	t.Helper()
	m, err := OpenManagedSQLKVGraphWithOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	return m
}
func graphOriginalValue(t *testing.T, command types.GraphCommand) []byte {
	t.Helper()
	value, err := types.EncodeManagedGraphExecution(command)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestManagedGraphOriginalNativeTransactionAndHistory(t *testing.T) {
	ctx := context.Background()
	m := graphOriginalMaterializer(t, filepath.Join(t.TempDir(), "sqlite.db"))
	original := types.GraphCommand{RequestID: string([]byte{0xff, 0}), Cypher: `CREATE (:Item {id: 'first'})`, Events: []types.GraphStreamEvent{{Stream: "events", Kind: "created", Payload: map[string]any{"n": int64(1)}}}}
	value := graphOriginalValue(t, original)
	if err := m.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	first := retainedOriginal(t, m, types.MutationGraph, original.RequestID)
	if first.Graph == nil || first.Slot != 1 || first.BatchIndex != 0 || first.DecisionHash != sha256.Sum256(value) || !bytes.Equal(first.ExecutionItem, value) {
		t.Fatalf("first original=%+v", first)
	}
	want, err := types.EncodeManagedGraphOriginal(original)
	if err != nil || !bytes.Equal(first.CanonicalOriginal, want) {
		t.Fatal("lossless original changed")
	}
	history, err := m.ManagedGraphHistory(ctx)
	if err != nil || history.LastGraphSlot != 1 {
		t.Fatalf("history=%+v err=%v", history, err)
	}
	if err := m.Apply(ctx, 2, value); err != nil {
		t.Fatal(err)
	}
	conflict := original
	conflict.Cypher = `CREATE (:Item {id: 'conflicting'})`
	if err := m.Apply(ctx, 3, graphOriginalValue(t, conflict)); err != nil {
		t.Fatal(err)
	}
	after := retainedOriginal(t, m, types.MutationGraph, original.RequestID)
	if !reflect.DeepEqual(first, after) {
		t.Fatal("duplicate/conflict overwrote first native original")
	}
	rows, err := m.GraphQuery(ctx, `MATCH (n:Item) RETURN n.id`, nil)
	if err != nil || len(rows.Rows) != 1 {
		t.Fatalf("effects=%v err=%v", rows.Rows, err)
	}
	records, _, err := m.GraphReadStream(ctx, "events", 0, 10, 0)
	if err != nil || len(records) != 1 {
		t.Fatalf("events=%v err=%v", records, err)
	}
	fp, err := types.ManagedGraphIntentFingerprint(original)
	if err != nil {
		t.Fatal(err)
	}
	receipt, found, matches, err := m.GraphRequestStatusFingerprint(ctx, original.RequestID, fp)
	if err != nil || !found || !matches || receipt.Slot != 1 {
		t.Fatalf("status=%+v %v %v %v", receipt, found, matches, err)
	}
	if _, _, err := m.RetainedRequestOriginal(ctx, types.MutationGraph, string([]byte{0xfe, 0})); err != nil {
		t.Fatal(err)
	}
	if err := m.graph.validateOriginalAccounting(); err != nil {
		t.Fatal(err)
	}
}

func TestManagedGraphOriginalReplayAfterSQLiteRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sqlite.db")
	m := graphOriginalMaterializer(t, path)
	ctx := context.Background()
	// The reused context observes this actual node before canceling SQLite.
	command := types.GraphCommand{RequestID: "once", Cypher: `CREATE (:Item {id: 'first'})`}
	value := graphOriginalValue(t, command)
	if err := m.Apply(newCancelAfterGraphCommitContext(m.graph.db), 1, value); err == nil {
		t.Fatal("expected cancellation after actual native commit")
	}
	if m.Tip() != 0 || m.graph.tip != 1 {
		t.Fatal("fixture did not create split publication")
	}
	if err := m.RuntimeHealth(ctx); err != nil {
		t.Fatalf("replay entry blocked: %v", err)
	}
	if err := m.ProjectionReadable(ctx); err == nil {
		t.Fatal("split projection was public")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m = graphOriginalMaterializer(t, path)
	if err := m.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	history, err := m.ManagedGraphHistory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := foldGraphHistory(GraphHistory{}, 1, sha256.Sum256(value))
	if err != nil || history != expected {
		t.Fatal("native completed replay folded twice")
	}
	rows, err := m.GraphQuery(ctx, `MATCH (n:Item) RETURN n.id`, nil)
	if err != nil || len(rows.Rows) != 1 {
		t.Fatal("replay duplicated effect")
	}
}

func TestManagedGraphOriginalPoisonAndCheckpointSuffix(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sqlite.db")
	m := graphOriginalMaterializer(t, path)
	first := types.GraphCommand{RequestID: "first", Cypher: `CREATE (:Item {id: 'first'})`}
	value := graphOriginalValue(t, first)
	cause := errors.New("lost native commit acknowledgement")
	m.graph.afterCommitError = func() error { return cause }
	if err := m.Apply(ctx, 1, value); !errors.Is(err, ErrGraphCommitUnknown) || !errors.Is(err, cause) {
		t.Fatalf("actual native commit unknown=%v", err)
	}
	if err := m.RuntimeHealth(ctx); !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("poison=%v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m = graphOriginalMaterializer(t, path)
	if err := m.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	files, tip, cleanup, err := m.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if tip != 1 || len(files) != 2 {
		t.Fatal("not paired")
	}
	restored := graphOriginalMaterializer(t, filepath.Join(t.TempDir(), "sqlite.db"))
	if err := restored.RestoreCheckpoint(ctx, files); err != nil {
		t.Fatal(err)
	}
	second := types.GraphCommand{RequestID: "second", Cypher: `CREATE (:Item {id: 'second'})`}
	suffix := graphOriginalValue(t, second)
	for _, target := range []*Materializer{m, restored} {
		if err := target.Apply(ctx, 2, suffix); err != nil {
			t.Fatal(err)
		}
	}
	h1, e1 := m.ManagedGraphHistory(ctx)
	h2, e2 := restored.ManagedGraphHistory(ctx)
	if e1 != nil || e2 != nil || h1 != h2 {
		t.Fatal("paired checkpoint+suffix history differs")
	}
	for _, id := range []string{"first", "second"} {
		if !reflect.DeepEqual(retainedOriginal(t, m, types.MutationGraph, id), retainedOriginal(t, restored, types.MutationGraph, id)) {
			t.Fatal("checkpoint+suffix lost original")
		}
	}
	if wrong, err := OpenManagedSQLKVWithOriginals(path, 1, 1024); err == nil {
		_ = wrong.Close()
		t.Fatalf("profile9 accepted10: %v", err)
	}
}

func TestManagedGraphOriginalRollbackAndTamper(t *testing.T) {
	ctx := context.Background()
	m := graphOriginalMaterializer(t, filepath.Join(t.TempDir(), "sqlite.db"))
	cause := errors.New("native rollback")
	m.graph.queryError = func() error { return cause }
	command := types.GraphCommand{RequestID: "rollback", Cypher: `CREATE (:Item {id: 'never'})`}
	if err := m.Apply(ctx, 1, graphOriginalValue(t, command)); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if _, found, err := m.graph.request(command.RequestID); err != nil || found {
		t.Fatal("rolled back native original persisted")
	}
	m.graph.queryError = nil
	if err := m.Apply(ctx, 1, graphOriginalValue(t, command)); err != nil {
		t.Fatal(err)
	}
	if err := m.graph.db.Update(func(tx *latticedb.Tx) error {
		return tx.PutAppMetadata(graphHistoryKey, encodeGraphHistory(GraphHistory{}))
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := OpenManagedSQLKVGraphWithOriginals(m.dbPath, 1, 1024); err == nil {
		_ = reopened.Close()
		t.Fatal("native history tamper accepted")
	}
}

func TestManagedGraphOriginalControlExpiryAndPruneAccounting(t *testing.T) {
	ctx := context.Background()
	m := graphOriginalMaterializer(t, filepath.Join(t.TempDir(), "sqlite.db"))
	command := types.GraphCommand{RequestID: "expired", Cypher: `CREATE (:Item {id: 'expired'})`}
	if err := m.Apply(ctx, 1, graphOriginalValue(t, command)); err != nil {
		t.Fatal(err)
	}
	first := retainedOriginal(t, m, types.MutationGraph, command.RequestID)
	var controls []quepaxa.DecidedValue
	for slot := quepaxa.Slot(2); slot <= 1024; slot++ {
		controls = append(controls, quepaxa.DecidedValue{Slot: slot, Value: types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})})
	}
	if err := m.ApplyBatch(ctx, controls); err != nil {
		t.Fatal(err)
	}
	if got := retainedOriginal(t, m, types.MutationGraph, command.RequestID); !reflect.DeepEqual(got, first) {
		t.Fatal("original changed at inclusive window boundary")
	}
	if receipt, found, matches, err := m.GraphRequestStatusFingerprint(ctx, command.RequestID, first.Fingerprint); err != nil || !found || !matches || receipt.Slot != 1 || receipt.RetryThroughSlot != 1024 {
		t.Fatalf("boundary status=%+v found=%t matches=%t err=%v", receipt, found, matches, err)
	}
	if err := m.Apply(ctx, 1025, types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})); err != nil {
		t.Fatal(err)
	}
	if _, found, err := m.RetainedRequestOriginal(ctx, types.MutationGraph, command.RequestID); err != nil || found {
		t.Fatalf("original visible immediately after expiry: %t %v", found, err)
	}
	if _, found, _, err := m.GraphRequestStatusFingerprint(ctx, command.RequestID, first.Fingerprint); err != nil || found {
		t.Fatalf("receipt visible immediately after expiry: %t %v", found, err)
	}
	// Control-only progress changes visibility, not the immutable native effect.
	if _, found, err := m.graph.request(command.RequestID); err != nil || !found {
		t.Fatalf("control-only slot physically pruned native record: %t %v", found, err)
	}
	if err := m.graph.validateOriginalAccounting(); err != nil {
		t.Fatal(err)
	}
	files, tip, cleanup, err := m.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if tip != 1025 {
		t.Fatal("checkpoint lost actual control tip")
	}
	restored := graphOriginalMaterializer(t, filepath.Join(t.TempDir(), "sqlite.db"))
	if err := restored.RestoreCheckpoint(ctx, files); err != nil {
		t.Fatal(err)
	}
	if _, found, err := restored.RetainedRequestOriginal(ctx, types.MutationGraph, command.RequestID); err != nil || found {
		t.Fatalf("checkpoint revived expired original: %t %v", found, err)
	}
	next := types.GraphCommand{RequestID: "next", Cypher: `CREATE (:Item {id: 'next'})`}
	if err := m.Apply(ctx, 1026, graphOriginalValue(t, next)); err != nil {
		t.Fatal(err)
	}
	if _, found, err := m.graph.request(command.RequestID); err != nil || found {
		t.Fatalf("new Graph apply failed to prune original+receipt: %t %v", found, err)
	}
	if err := m.graph.validateOriginalAccounting(); err != nil {
		t.Fatal(err)
	}
	encoded, err := m.graph.getMetadata(graphOriginalBytesKey)
	if err != nil {
		t.Fatal(err)
	}
	charged, err := decodeGraphTip(encoded)
	if err != nil || charged == 0 {
		t.Fatal("new native original uncharged")
	}
	if err := m.graph.db.Update(func(tx *latticedb.Tx) error {
		return tx.PutAppMetadata(graphOriginalBytesKey, encodeGraphTip(charged+1))
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.graph.validateOriginalAccounting(); err == nil {
		t.Fatal("incorrect native byte accounting accepted")
	}
}

func TestManagedGraphOriginalDecisionHashBinding(t *testing.T) {
	ctx := context.Background()
	m := graphOriginalMaterializer(t, filepath.Join(t.TempDir(), "sqlite.db"))
	command := types.GraphCommand{RequestID: "hash-bound", Cypher: `CREATE (:Item {id: 'bound'})`}
	value := graphOriginalValue(t, command)
	if err := m.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	encoded, err := m.graph.getMetadata(graphRequestKey(command.RequestID))
	if err != nil {
		t.Fatal(err)
	}
	valid, found, err := decodeGraphOriginalRequest(encoded)
	if err != nil || !found || valid.Original.DecisionHash != sha256.Sum256(value) {
		t.Fatal("valid neighbor rejected", err)
	}
	altered := bytes.Clone(encoded)
	altered[50] ^= 1
	if _, _, err := decodeGraphOriginalRequest(altered); err == nil {
		t.Fatal("hash-only native original tamper accepted")
	}
	if err := m.graph.db.Update(func(tx *latticedb.Tx) error { return tx.PutAppMetadata(graphRequestKey(command.RequestID), altered) }); err != nil {
		t.Fatal(err)
	}
	if err := m.graph.validateOriginalAccounting(); err == nil {
		t.Fatal("accounting accepted record with altered decision binding")
	}
	if _, _, err := m.RetainedRequestOriginal(ctx, types.MutationGraph, command.RequestID); err == nil {
		t.Fatal("getter exposed altered decision binding")
	}
	path := m.dbPath
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := OpenManagedSQLKVGraphWithOriginals(path, 1, 1024); err == nil {
		_ = reopened.Close()
		t.Fatal("reopen accepted altered decision binding")
	}
}

func TestManagedGraphOriginalHistoryOverflowFailsClosed(t *testing.T) {
	if _, err := decodeGraphHistory(encodeGraphHistory(GraphHistory{LastGraphSlot: math.MaxUint64, Hash: [32]byte{1}})); err == nil {
		t.Fatal("native history slot overflow accepted")
	}
	g := &graphState{durableTip: math.MaxUint64, idempotencyWindow: 1024}
	if err := g.validateOriginalAccounting(); err == nil {
		t.Fatal("overflow accounting position accepted")
	}
}
