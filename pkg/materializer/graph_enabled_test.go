package materializer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	latticedb "github.com/mrchypark/latticedb-go"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type cancelAfterGraphCommitContext struct {
	context.Context
	db   *latticedb.DB
	done chan struct{}
	once sync.Once
}

func newCancelAfterGraphCommitContext(db *latticedb.DB) *cancelAfterGraphCommitContext {
	return &cancelAfterGraphCommitContext{Context: context.Background(), db: db, done: make(chan struct{})}
}

func (ctx *cancelAfterGraphCommitContext) Done() <-chan struct{} { return ctx.done }

func (ctx *cancelAfterGraphCommitContext) Err() error {
	select {
	case <-ctx.done:
		return context.Canceled
	default:
	}
	visible := false
	err := ctx.db.View(func(tx *latticedb.Tx) error {
		result, err := tx.QueryContext(context.Background(), `MATCH (n:Item {id: 'first'}) RETURN n.id`, nil, latticedb.QueryOptions{MaxRows: 1})
		visible = err == nil && len(result.Rows) > 0
		return err
	})
	if err == nil && visible {
		ctx.once.Do(func() { close(ctx.done) })
	}
	select {
	case <-ctx.done:
		return context.Canceled
	default:
		return nil
	}
}

func TestGraphBatchDoesNotExposePartialStateAtOldSlotAfterCancellation(t *testing.T) {
	commands := []types.GraphCommand{
		{RequestID: "first", Cypher: `CREATE (:Item {id: 'first'})`, Events: []types.GraphStreamEvent{{Stream: "events", Kind: "created", Payload: "first"}}},
		{RequestID: "second", Cypher: `CREATE (:Item {id: 'second'})`},
	}
	value, err := types.EncodeGraphBatch(commands)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "sqlite.db")
	m, err := Open(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	longPollErr := make(chan error, 1)
	go func() {
		_, _, err := m.GraphReadStream(context.Background(), "events", 0, 10, 2*time.Second)
		longPollErr <- err
	}()
	time.Sleep(10 * time.Millisecond)
	applyErr := m.ApplyBatch(newCancelAfterGraphCommitContext(m.graph.db), []quepaxa.DecidedValue{{Slot: 1, Value: value}})
	if !errors.Is(applyErr, context.Canceled) {
		t.Fatalf("ApplyBatch error=%v, want cancellation", applyErr)
	}
	select {
	case err := <-longPollErr:
		if !errors.Is(err, ErrGraphApplyPending) {
			t.Fatalf("long-poll error=%v, want pending publication", err)
		}
	case <-time.After(time.Second):
		t.Fatal("long-poll did not wake when graph publication became pending")
	}
	if _, err := m.GraphQuery(context.Background(), `MATCH (n:Item) RETURN n.id`, nil); !errors.Is(err, ErrGraphApplyPending) {
		t.Fatalf("GraphQuery error=%v, want pending publication", err)
	}
	if _, _, err := m.GraphMutationReceipt(context.Background(), "first"); !errors.Is(err, ErrGraphApplyPending) {
		t.Fatalf("GraphMutationReceipt error=%v, want pending publication", err)
	}
	if _, _, err := m.GraphReadStream(context.Background(), "events", 0, 10, 0); !errors.Is(err, ErrGraphApplyPending) {
		t.Fatalf("GraphReadStream error=%v, want pending publication", err)
	}
	if _, _, _, err := m.GraphStreamOffset(context.Background(), "events", "consumer"); !errors.Is(err, ErrGraphApplyPending) {
		t.Fatalf("GraphStreamOffset error=%v, want pending publication", err)
	}
	if _, err := m.GraphReachable(context.Background(), types.GraphReachableRequest{
		StartLabel: "Item", StartProperty: "id", StartValue: "first", EdgeType: "REL", ResultProperty: "id",
		MaxDepth: 1, MaxResults: 1, MaxScannedEdges: 1, MaxBytes: 128,
	}); !errors.Is(err, ErrGraphApplyPending) {
		t.Fatalf("GraphReachable error=%v, want pending publication", err)
	}
	if _, err := m.GraphRequestMatches(context.Background(), commands[0]); !errors.Is(err, ErrGraphApplyPending) {
		t.Fatalf("GraphRequestMatches error=%v, want pending publication", err)
	}
	if _, err := m.graphRequestExists("first"); !errors.Is(err, ErrGraphApplyPending) {
		t.Fatalf("graphRequestExists error=%v, want pending publication", err)
	}
	if err := m.Health(context.Background()); !errors.Is(err, ErrGraphApplyPending) {
		t.Fatalf("Health error=%v, want pending publication", err)
	}
	if _, _, _, err := m.CheckpointFilesAt(context.Background()); !errors.Is(err, ErrGraphApplyPending) {
		t.Fatalf("CheckpointFilesAt error=%v, want pending publication", err)
	}
	if closeErr := m.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopened, err := Open(path, 1, 1024)
	if err != nil {
		t.Fatalf("reopen partial graph: %v", err)
	}
	if _, err := reopened.GraphQuery(context.Background(), `MATCH (n:Item) RETURN n.id`, nil); !errors.Is(err, ErrGraphApplyPending) {
		t.Fatalf("reopened GraphQuery error=%v, want pending publication", err)
	}
	for name, altered := range map[string][]types.GraphCommand{
		"order": {commands[1], commands[0]},
		"tail":  {commands[0], commands[1], {RequestID: "tail", Cypher: `CREATE (:Item {id: 'tail'})`}},
	} {
		changedValue, err := types.EncodeGraphBatch(altered)
		if err != nil {
			t.Fatal(err)
		}
		if err := reopened.Apply(context.Background(), 1, changedValue); !errors.Is(err, ErrGraphApplyPending) {
			t.Errorf("%s replay error=%v, want pending decision hash mismatch", name, err)
		}
	}
	if err := reopened.Apply(context.Background(), 1, policySQL(t, "CREATE TABLE not_graph (id INTEGER)")); !errors.Is(err, ErrGraphApplyPending) {
		t.Fatalf("non-graph replacement error=%v, want pending decision hash mismatch", err)
	}
	replayErr := reopened.Apply(context.Background(), 1, value)
	if replayErr != nil {
		t.Fatalf("replay interrupted graph slot: %v", replayErr)
	}
	if _, found, err := reopened.graph.getMetadataValue(graphPendingKey); err != nil || found {
		t.Fatalf("pending marker after completion found=%v err=%v", found, err)
	}
	journalBytes, err := reopened.graph.getMetadata(graphJournalKey)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := decodeGraphJournal(journalBytes)
	if err != nil || len(journal) != 1 || journal[0].Slot != 1 {
		t.Fatalf("completed journal=%+v err=%v", journal, err)
	}
	replayedResult, replayedQueryErr := reopened.GraphQuery(context.Background(), `MATCH (n:Item) RETURN n.id ORDER BY n.id`, nil)
	if replayedQueryErr != nil || replayedResult.AppliedSlot != 1 || len(replayedResult.Rows) != 2 {
		t.Fatalf("replay result slot=%d rows=%v err=%v, want both commands at slot 1", replayedResult.AppliedSlot, replayedResult.Rows, replayedQueryErr)
	}
	replayedRecords, replayedStreamSlot, replayedStreamErr := reopened.GraphReadStream(context.Background(), "events", 0, 10, 0)
	if replayedStreamErr != nil || replayedStreamSlot != 1 || len(replayedRecords) != 1 {
		t.Fatalf("replay stream slot=%d records=%v err=%v, want one event at slot 1", replayedStreamSlot, replayedRecords, replayedStreamErr)
	}
	if _, found, err := reopened.GraphMutationReceipt(context.Background(), "first"); err != nil || !found {
		t.Fatalf("first replay receipt found=%v err=%v", found, err)
	}
	if _, found, err := reopened.GraphMutationReceipt(context.Background(), "second"); err != nil || !found {
		t.Fatalf("second replay receipt found=%v err=%v", found, err)
	}
	var nonce [types.ReadBarrierNonceSize]byte
	barrier := types.EncodeReadBarrier(nonce)
	decisions := make([]quepaxa.DecidedValue, 1024)
	for i := range decisions {
		decisions[i] = quepaxa.DecidedValue{Slot: quepaxa.Slot(i + 2), Value: barrier}
	}
	if err := reopened.ApplyBatch(context.Background(), decisions); err != nil {
		t.Fatal(err)
	}
	if receipt, found, err := reopened.GraphMutationReceipt(context.Background(), "first"); err != nil || found {
		t.Fatalf("expired receipt tip=%d receipt=%+v found=%v err=%v, want outside retry window", reopened.Tip(), receipt, found, err)
	}
	if closeErr := reopened.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
}

type crashAfterGraphCommitContext struct {
	context.Context
	db *latticedb.DB
}

func (ctx crashAfterGraphCommitContext) Err() error {
	visible := false
	_ = ctx.db.View(func(tx *latticedb.Tx) error {
		result, err := tx.QueryContext(context.Background(), `MATCH (n:Item {id: 'first'}) RETURN n.id`, nil, latticedb.QueryOptions{MaxRows: 1})
		visible = err == nil && len(result.Rows) > 0
		return nil
	})
	if visible {
		os.Exit(77)
	}
	return nil
}

func TestGraphPartialApplyProcessCrashChild(t *testing.T) {
	path := os.Getenv("RHIZA_GRAPH_CRASH_DB")
	if path == "" {
		t.Skip("subprocess helper")
	}
	m, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	commands := []types.GraphCommand{
		{RequestID: "first", Cypher: `CREATE (:Item {id: 'first'})`, Events: []types.GraphStreamEvent{{Stream: "events", Kind: "created", Payload: "first"}}},
		{RequestID: "second", Cypher: `CREATE (:Item {id: 'second'})`},
	}
	value, err := types.EncodeGraphBatch(commands)
	if err != nil {
		t.Fatal(err)
	}
	_ = m.ApplyBatch(crashAfterGraphCommitContext{Context: context.Background(), db: m.graph.db}, []quepaxa.DecidedValue{{Slot: 1, Value: value}})
	t.Fatal("expected process exit after the first graph command commit")
}

func TestGraphPartialApplyIsGatedAcrossProcessCrashAndExactReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sqlite.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestGraphPartialApplyProcessCrashChild$")
	cmd.Env = append(os.Environ(), "RHIZA_GRAPH_CRASH_DB="+path)
	err := cmd.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 77 {
		t.Fatalf("crash helper error=%v, want exit code 77", err)
	}
	m, err := Open(path, 1)
	if err != nil {
		t.Fatalf("open after process crash: %v", err)
	}
	defer m.Close()
	if _, err := m.GraphQuery(context.Background(), `MATCH (n:Item) RETURN n.id`, nil); !errors.Is(err, ErrGraphApplyPending) {
		t.Fatalf("GraphQuery after crash error=%v, want pending publication", err)
	}
	commands := []types.GraphCommand{
		{RequestID: "first", Cypher: `CREATE (:Item {id: 'first'})`, Events: []types.GraphStreamEvent{{Stream: "events", Kind: "created", Payload: "first"}}},
		{RequestID: "second", Cypher: `CREATE (:Item {id: 'second'})`},
	}
	value, err := types.EncodeGraphBatch(commands)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 1, value); err != nil {
		t.Fatalf("exact replay after process crash: %v", err)
	}
	result, err := m.GraphQuery(context.Background(), `MATCH (n:Item) RETURN n.id ORDER BY n.id`, nil)
	if err != nil || len(result.Rows) != 2 {
		t.Fatalf("replayed rows=%v err=%v, want both commands", result.Rows, err)
	}
	records, _, err := m.GraphReadStream(context.Background(), "events", 0, 10, 0)
	if err != nil || len(records) != 1 {
		t.Fatalf("replayed stream records=%v err=%v, want exactly one event", records, err)
	}
}

func TestGraphStoragePolicyRejectsMalformedMetadata(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  []byte
		data []byte
	}{
		{name: "pending marker", key: graphPendingKey, data: []byte{1}},
		{name: "storage policy", key: graphFormatKey, data: []byte("future-policy")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sqlite.db")
			m, err := Open(path, 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.graph.db.Update(func(tx *latticedb.Tx) error { return tx.PutAppMetadata(tc.key, tc.data) }); err != nil {
				t.Fatal(err)
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(path, 1); err == nil {
				t.Fatal("open accepted malformed or unsupported graph metadata")
			}
		})
	}
}

func TestUnknownCommitPoisonsReadsAndApplyUntilReopen(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	m.graph.mu.Lock()
	m.graph.poisonCommitOutcome()
	m.graph.mu.Unlock()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.GraphQuery(canceled, `RETURN 1 AS value`, nil); !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("GraphQuery error=%v, want commit outcome unknown", err)
	}
	if _, _, err := m.GraphMutationReceipt(context.Background(), "request"); !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("GraphMutationReceipt error=%v, want commit outcome unknown", err)
	}
	if _, _, _, err := m.GraphStreamOffset(canceled, "events", "consumer"); !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("GraphStreamOffset error=%v, want commit outcome unknown", err)
	}
	if err := m.Health(canceled); !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("Health error=%v, want commit outcome unknown", err)
	}
	if err := m.Apply(canceled, 1, policySQL(t, "CREATE TABLE blocked (id INTEGER)")); !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("Apply error=%v, want commit outcome unknown", err)
	}
}

func TestGraphCommitAcknowledgementLossPoisonsUntilReopen(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
	}{
		{name: "wrapped cancellation", cause: fmt.Errorf("commit acknowledgement: %w", context.Canceled)},
		{name: "unclassified error", cause: errors.New("simulated commit acknowledgement loss")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sqlite.db")
			m, err := Open(path, 1)
			if err != nil {
				t.Fatal(err)
			}
			command := types.GraphCommand{
				RequestID: "committed-before-ack-loss",
				Cypher:    `CREATE (:Item {id: 'committed'})`,
				Events:    []types.GraphStreamEvent{{Stream: "events", Kind: "created", Payload: "committed"}},
			}
			value, err := types.EncodeGraphCommand(command)
			if err != nil {
				t.Fatal(err)
			}
			injected := false
			m.graph.afterCommitError = func() error {
				injected = true
				return tc.cause
			}
			applyErr := m.Apply(context.Background(), 1, value)
			if !injected || !errors.Is(applyErr, ErrGraphCommitUnknown) || !errors.Is(applyErr, tc.cause) {
				t.Fatalf("Apply error=%v injected=%v, want unknown commit and cause %v", applyErr, injected, tc.cause)
			}
			if _, err := m.GraphQuery(context.Background(), `MATCH (n:Item) RETURN n.id`, nil); !errors.Is(err, ErrGraphCommitUnknown) {
				t.Fatalf("GraphQuery error=%v, want unknown commit", err)
			}
			if err := m.Apply(context.Background(), 1, value); !errors.Is(err, ErrGraphCommitUnknown) {
				t.Fatalf("same-instance Apply error=%v, want unknown commit", err)
			}
			committed, found, err := m.graph.request(command.RequestID)
			if err != nil || !found || committed.Receipt.Status != types.MutationCommitted {
				t.Fatalf("durable request=%+v found=%v err=%v, want original committed receipt", committed, found, err)
			}
			rows, err := m.graph.db.Query(`MATCH (n:Item {id: 'committed'}) RETURN n.id`, nil)
			if err != nil || len(rows.Rows) != 1 {
				t.Fatalf("durable graph effects rows=%v err=%v, want one committed node", rows.Rows, err)
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			m, err = Open(path, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			if err := m.Apply(context.Background(), 1, value); err != nil {
				t.Fatalf("exact replay after reopen: %v", err)
			}
			result, err := m.GraphQuery(context.Background(), `MATCH (n:Item {id: 'committed'}) RETURN n.id`, nil)
			if err != nil || result.AppliedSlot != 1 || len(result.Rows) != 1 {
				t.Fatalf("replayed result slot=%d rows=%v err=%v", result.AppliedSlot, result.Rows, err)
			}
			receipt, found, err := m.GraphMutationReceipt(context.Background(), command.RequestID)
			if err != nil || !found || receipt.Status != types.MutationCommitted {
				t.Fatalf("replayed receipt=%+v found=%v err=%v", receipt, found, err)
			}
			records, _, err := m.GraphReadStream(context.Background(), "events", 0, 10, 0)
			if err != nil || len(records) != 1 {
				t.Fatalf("replayed records=%v err=%v, want one event", records, err)
			}
		})
	}
}

func TestRejectedReceiptCommitAcknowledgementLossPoisonsUntilReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sqlite.db")
	m, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	command := types.GraphCommand{RequestID: "rejected-before-ack-loss", Cypher: `MATCH (`}
	value, err := types.EncodeGraphCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	injected := false
	m.graph.afterCommitError = func() error {
		injected = true
		return errors.New("simulated rejection commit acknowledgement loss")
	}
	applyErr := m.Apply(context.Background(), 1, value)
	if !injected || !errors.Is(applyErr, ErrGraphCommitUnknown) {
		t.Fatalf("Apply error=%v injected=%v, want unknown rejection commit", applyErr, injected)
	}
	stored, found, err := m.graph.request(command.RequestID)
	if err != nil || !found || stored.Receipt.Status != types.MutationRejected {
		t.Fatalf("stored rejection=%+v found=%v err=%v", stored, found, err)
	}
	if _, err := m.GraphQuery(context.Background(), `MATCH (n:Item) RETURN n.id`, nil); !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("GraphQuery error=%v, want unknown commit", err)
	}
	if err := m.Apply(context.Background(), 1, value); !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("same-instance Apply error=%v, want unknown commit", err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err = Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Apply(context.Background(), 1, value); err != nil {
		t.Fatalf("exact replay after reopen: %v", err)
	}
	if _, err := m.GraphQuery(context.Background(), `MATCH (n:Item) RETURN n.id`, nil); err != nil {
		t.Fatal(err)
	}
	receipt, found, err := m.GraphMutationReceipt(context.Background(), command.RequestID)
	if err != nil || !found || receipt.Status != types.MutationRejected {
		t.Fatalf("replayed rejection=%+v found=%v err=%v", receipt, found, err)
	}
}

func TestFailedRejectionRecordingDoesNotPersistReceipt(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.graph.db.Update(func(tx *latticedb.Tx) error { return tx.PutAppMetadata(graphJournalKey, []byte{1}) }); err != nil {
		t.Fatal(err)
	}
	command := types.GraphCommand{RequestID: "no-rejection-on-record-failure", Cypher: `MATCH (`}
	value, err := types.EncodeGraphCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 1, value); err == nil {
		t.Fatal("Apply succeeded despite failed rejection metadata transaction")
	}
	if _, found, err := m.graph.request(command.RequestID); err != nil || found {
		t.Fatalf("request after failed rejection recording found=%v err=%v, want no receipt", found, err)
	}
	rows, err := m.graph.db.Query(`MATCH (n:Item) RETURN n.id`, nil)
	if err != nil || len(rows.Rows) != 0 {
		t.Fatalf("graph effects after rejected command rows=%v err=%v", rows.Rows, err)
	}
}

func TestGraphEffectsAndPendingMarkerRollbackTogether(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	conflict := encodeGraphPending(graphJournalEntry{Slot: 99, Hash: sha256.Sum256([]byte("conflict"))})
	if err := m.graph.db.Update(func(tx *latticedb.Tx) error { return tx.PutAppMetadata(graphPendingKey, conflict) }); err != nil {
		t.Fatal(err)
	}
	command := types.GraphCommand{RequestID: "rolled-back", Cypher: `CREATE (:Item {id: 'rolled-back'})`}
	value, err := types.EncodeGraphCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 1, value); err == nil {
		t.Fatal("apply succeeded despite conflicting pending marker")
	}
	result, err := m.graph.db.Query(`MATCH (n:Item {id: 'rolled-back'}) RETURN n.id`, nil)
	if err != nil || len(result.Rows) != 0 {
		t.Fatalf("graph effects after rollback rows=%v err=%v", result.Rows, err)
	}
	if _, found, err := m.graph.request("rolled-back"); err != nil || found {
		t.Fatalf("receipt after rollback found=%v err=%v", found, err)
	}
}

func TestCompletedGraphSlotRemainsHiddenUntilSQLiteBatchReplays(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sqlite.db")
	m, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	command := types.GraphCommand{RequestID: "complete-before-sql", Cypher: `CREATE (:Item {id: 'first'})`}
	value, err := types.EncodeGraphCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	ctx := newCancelAfterGraphCommitContext(m.graph.db)
	if err := m.Apply(ctx, 1, value); err == nil {
		t.Fatal("Apply succeeded despite cancellation after graph commit")
	}
	if m.Tip() != 0 || m.graphTip() != 1 {
		t.Fatalf("SQLite tip=%d graph tip=%d, want graph-ahead state", m.Tip(), m.graphTip())
	}
	if _, err := m.GraphQuery(context.Background(), `MATCH (n:Item) RETURN n.id`, nil); err == nil {
		t.Fatal("GraphQuery exposed graph-complete/SQLite-uncommitted slot")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err = Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Apply(context.Background(), 1, value); err != nil {
		t.Fatalf("replay graph-complete/SQLite-uncommitted slot: %v", err)
	}
	result, err := m.GraphQuery(context.Background(), `MATCH (n:Item) RETURN n.id`, nil)
	if err != nil || result.AppliedSlot != 1 || len(result.Rows) != 1 {
		t.Fatalf("replayed graph slot=%d rows=%v err=%v", result.AppliedSlot, result.Rows, err)
	}
}

func TestEarlierCompletedGraphSlotCannotUnlockLaterPendingSlot(t *testing.T) {
	ctx := context.Background()
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	priorCommand := types.GraphCommand{RequestID: "prior", Cypher: `CREATE (:Item {id: 'prior'})`}
	priorValue, err := types.EncodeGraphCommand(priorCommand)
	if err != nil {
		t.Fatal(err)
	}
	priorCommands, priorGraph, err := types.DecodeGraphBatch(priorValue)
	if err != nil || !priorGraph {
		t.Fatalf("prior graph decode=%v err=%v", priorGraph, err)
	}
	if err := m.applyGraph(ctx, 1, priorValue, priorCommands, true, 0); err != nil {
		t.Fatal(err)
	}
	middleValue := policySQL(t, "CREATE TABLE replay_middle (id INTEGER)")
	if err := m.applyGraph(ctx, 2, middleValue, nil, false, 1); err != nil {
		t.Fatal(err)
	}
	commands := []types.GraphCommand{
		{RequestID: "later-first", Cypher: `CREATE (:Item {id: 'later-first'})`},
		{RequestID: "later-second", Cypher: `CREATE (:Item {id: 'later-second'})`},
	}
	value, err := types.EncodeGraphBatch(commands)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := prepareGraphCommand(commands[0])
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(value)
	m.graph.mu.Lock()
	err = m.graph.applyCommand(ctx, 3, hash, commands[0], fingerprint, false, 2)
	if err == nil {
		m.graph.pending = &graphJournalEntry{Slot: 3, Hash: hash}
	}
	m.graph.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := m.applyGraph(ctx, 1, priorValue, priorCommands, true, 0); err != nil {
		t.Fatalf("earlier completed slot evidence replay error=%v", err)
	}
	if _, err := m.GraphQuery(ctx, `MATCH (n:Item) RETURN n.id`, nil); !errors.Is(err, ErrGraphApplyPending) {
		t.Fatalf("GraphQuery error=%v, want later pending slot", err)
	}
	if err := m.Apply(ctx, 1, priorValue); err != nil {
		t.Fatalf("replay completed prefix before pending slot: %v", err)
	}
	if _, err := m.GraphQuery(ctx, `MATCH (n:Item) RETURN n.id`, nil); !errors.Is(err, ErrGraphApplyPending) {
		t.Fatalf("GraphQuery after committed prefix error=%v, want later pending slot", err)
	}
	if err := m.Apply(ctx, 2, middleValue); err != nil {
		t.Fatalf("replay non-graph prefix before pending slot: %v", err)
	}
	if _, err := m.GraphQuery(ctx, `MATCH (n:Item) RETURN n.id`, nil); !errors.Is(err, ErrGraphApplyPending) {
		t.Fatalf("GraphQuery after non-graph prefix error=%v, want later pending slot", err)
	}
	if err := m.Apply(ctx, 3, value); err != nil {
		t.Fatalf("exact replay of pending later slot: %v", err)
	}
	result, err := m.GraphQuery(ctx, `MATCH (n:Item) RETURN n.id`, nil)
	if err != nil || result.AppliedSlot != 3 || len(result.Rows) != 3 {
		t.Fatalf("replayed graph slot=%d rows=%v err=%v", result.AppliedSlot, result.Rows, err)
	}
}

func TestCleanCheckpointCanReplaceDirtyPendingTarget(t *testing.T) {
	ctx := context.Background()
	source, err := Open(filepath.Join(t.TempDir(), "source.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	files, _, cleanup, err := source.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	defer source.Close()

	target, err := Open(filepath.Join(t.TempDir(), "target.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	commands := []types.GraphCommand{
		{RequestID: "restore-first", Cypher: `CREATE (:Item {id: 'first'})`},
		{RequestID: "restore-second", Cypher: `CREATE (:Item {id: 'second'})`},
	}
	value, err := types.EncodeGraphBatch(commands)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.ApplyBatch(newCancelAfterGraphCommitContext(target.graph.db), []quepaxa.DecidedValue{{Slot: 1, Value: value}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted ApplyBatch error=%v, want cancellation", err)
	}
	if err := target.RestoreCheckpoint(ctx, files); err != nil {
		t.Fatalf("restore clean checkpoint over pending target: %v", err)
	}
	result, err := target.GraphQuery(ctx, `MATCH (n:Item) RETURN n.id`, nil)
	if err != nil || result.AppliedSlot != 0 || len(result.Rows) != 0 {
		t.Fatalf("restored graph slot=%d rows=%v err=%v, want clean slot 0", result.AppliedSlot, result.Rows, err)
	}
}

func TestGraphResultUsesAggregateByteBudget(t *testing.T) {
	value := strings.Repeat("x", 1<<20-2)
	result := latticedb.QueryResult{Columns: []string{"value"}}
	for range 17 {
		result.Rows = append(result.Rows, map[string]any{"value": value})
	}
	if _, err := collectLatticeRows(result); err == nil {
		t.Fatal("aggregate graph result byte limit was not enforced")
	}
}

func TestRetryableGraphApplyErrorOnlyMatchesTransientContention(t *testing.T) {
	checkpoint := fmt.Errorf("%w: WAL checkpoint is in progress", latticedb.ErrResourceLimit)
	if !isRetryableGraphApplyError(checkpoint) {
		t.Fatalf("checkpoint backpressure = %v, want retryable", checkpoint)
	}
	if !isRetryableGraphApplyError(latticedb.ErrWriteTxActive) {
		t.Fatal("active writer = not retryable")
	}
	for _, err := range []error{
		fmt.Errorf("%w: query rows exceed 1", latticedb.ErrResourceLimit),
		context.DeadlineExceeded,
		fmt.Errorf("execution failed"),
	} {
		if isRetryableGraphApplyError(err) {
			t.Fatalf("error = %v, unexpectedly retryable", err)
		}
	}
}

func TestGraphSnapshotGrowthBackpressureIsRetryable(t *testing.T) {
	for _, size := range []int{2 << 20, 17 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			snapshot, err := m.graph.db.BeginSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
			payload := bytes.Repeat([]byte{'x'}, size)
			write := func(tx *latticedb.Tx) error {
				return tx.PutAppMetadata([]byte("backpressure-test"), payload)
			}
			if err := m.graph.db.Update(write); !errors.Is(err, latticedb.ErrResourceLimit) || !isRetryableGraphApplyError(err) {
				t.Fatalf("snapshot backpressure = %v, want retryable resource limit", err)
			}
			if err := snapshot.Close(); err != nil {
				t.Fatal(err)
			}
			if err := m.graph.db.Update(write); err != nil {
				t.Fatalf("retry after snapshot release: %v", err)
			}
		})
	}
}

func TestBeginGraphSnapshotWaitsForWriterOrContext(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	writer, err := m.graph.db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	blocked, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	defer writer.Rollback()
	if snapshot, err := m.beginGraphSnapshot(blocked); snapshot != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("snapshot while writer is active = (%v, %v), want context cancellation", snapshot, err)
	}
	if err := writer.Rollback(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := m.beginGraphSnapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot after writer release: %v", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateGraphCommandRejectsPartialStandaloneCreate(t *testing.T) {
	for _, cypher := range []string{
		`CREATE (:Item {id: $id})`,
		`CREATE (:Item {text: ")-["})`,
		`CREATE (item:Item {id: $id}) RETURN item.id`,
		`MATCH (from:Item), (to:Item) CREATE (from)-[:LINK]->(to)`,
	} {
		if err := ValidateGraphCommandAdmission(types.GraphCommand{RequestID: "valid", Cypher: cypher}); err != nil {
			t.Fatalf("valid cypher %q: %v", cypher, err)
		}
	}
	for _, cypher := range []string{
		`CREATE (:Item)-[:LINK]->(:Item)`,
		`CREATE (:Item), (:Item)`,
		`CREATE (from:Item)-[edge:LINK]->(to:Item)`,
	} {
		if err := ValidateGraphCommandAdmission(types.GraphCommand{RequestID: "invalid", Cypher: cypher}); err == nil {
			t.Fatalf("unsafe cypher %q was accepted", cypher)
		}
	}
	legacy := types.GraphCommand{RequestID: "committed", Cypher: `CREATE (:Item)-[:LINK]->(:Item)`}
	if err := ValidateGraphCommand(legacy); err != nil {
		t.Fatalf("committed-log validation changed: %v", err)
	}
}

func TestGraphAheadRecoveryRequiresMatchingDecision(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sqlite.db")
	command := types.GraphCommand{RequestID: "ahead", Cypher: `CREATE (:Item {id: 'one'})`}
	value, err := types.EncodeGraphCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	commands, graph, err := types.DecodeGraphBatch(value)
	if err != nil || !graph {
		t.Fatalf("decode graph=%v err=%v", graph, err)
	}
	m, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.applyGraph(ctx, 1, value, commands, true, 0); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err = Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	other, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "other", Cypher: `CREATE (:Item {id: 'other'})`})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 1, other); err == nil {
		t.Fatal("graph-ahead state accepted a different decision")
	}
	if err := m.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	journal, err := m.graph.getMetadata(graphJournalKey)
	if err != nil || len(journal) != 40 {
		t.Fatalf("pending recovery journal=%x err=%v", journal, err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err = Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	journal, err = m.graph.getMetadata(graphJournalKey)
	if err != nil || len(journal) != 0 {
		t.Fatalf("recovered journal=%x err=%v", journal, err)
	}
}

func TestGraphJournalTracksOnlyGraphSlotsAndPrunesCommittedPrefix(t *testing.T) {
	ctx := context.Background()
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	graphValue, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "batch-graph", Cypher: `CREATE (:Batch {id: '2'})`})
	if err != nil {
		t.Fatal(err)
	}
	decisions := []quepaxa.DecidedValue{{Slot: 1, Value: policySQL(t, "CREATE TABLE batch_1 (id INTEGER)")}, {Slot: 2, Value: graphValue}}
	if err := m.ApplyBatch(ctx, decisions); err != nil {
		t.Fatal(err)
	}
	journal, err := m.graph.getMetadata(graphJournalKey)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := decodeGraphJournal(journal)
	if err != nil || len(entries) != 1 || entries[0].Slot != 2 {
		t.Fatalf("sparse batch journal=%+v err=%v", entries, err)
	}
	if err := m.Apply(ctx, 3, policySQL(t, "CREATE TABLE next (id INTEGER)")); err != nil {
		t.Fatal(err)
	}
	graphValue, err = types.EncodeGraphCommand(types.GraphCommand{RequestID: "next-graph", Cypher: `CREATE (:Batch {id: '4'})`})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 4, graphValue); err != nil {
		t.Fatal(err)
	}
	journal, err = m.graph.getMetadata(graphJournalKey)
	if err != nil {
		t.Fatal(err)
	}
	entries, err = decodeGraphJournal(journal)
	if err != nil || len(entries) != 1 || entries[0].Slot != 4 {
		t.Fatalf("pruned journal=%+v err=%v", entries, err)
	}
}

func TestGraphSparseAheadRecoveryAcceptsOnlyMatchingGraphDecisions(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sqlite.db")
	m, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 1, policySQL(t, "CREATE TABLE durable (id INTEGER)")); err != nil {
		t.Fatal(err)
	}
	encodedTip, err := m.graph.getMetadata(graphTipKey)
	if err != nil {
		t.Fatal(err)
	}
	durableTip, err := decodeGraphTip(encodedTip)
	if err != nil || durableTip != 0 {
		t.Fatalf("durable graph tip=%d err=%v, want 0", durableTip, err)
	}
	if err := m.applyGraph(ctx, 2, policySQL(t, "CREATE TABLE pending (id INTEGER)"), nil, false, 1); err != nil {
		t.Fatal(err)
	}
	graphValue, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "sparse-ahead", Cypher: `CREATE (:Sparse {id: '3'})`})
	if err != nil {
		t.Fatal(err)
	}
	commands, graph, err := types.DecodeGraphBatch(graphValue)
	if err != nil || !graph {
		t.Fatalf("decode graph=%v err=%v", graph, err)
	}
	if err := m.applyGraph(ctx, 3, graphValue, commands, true, 1); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err = Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Apply(ctx, 2, policySQL(t, "CREATE TABLE pending (id INTEGER)")); err != nil {
		t.Fatal(err)
	}
	other, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "conflict", Cypher: `CREATE (:Sparse {id: 'other'})`})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 3, other); err == nil {
		t.Fatal("sparse graph-ahead state accepted a different graph decision")
	}
	if err := m.Apply(ctx, 3, graphValue); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkGraphApply(b *testing.B) {
	m, err := Open(filepath.Join(b.TempDir(), "sqlite.db"), 1)
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		command := types.GraphCommand{RequestID: fmt.Sprintf("request-%d", i), Cypher: `CREATE (:Bench {id: $id})`, Args: map[string]any{"id": float64(i)}}
		value, err := types.EncodeGraphCommand(command)
		if err != nil {
			b.Fatal(err)
		}
		if err := m.Apply(ctx, uint64(i+1), value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGraphSnapshotFreezeByDatabaseSize(b *testing.B) {
	for _, sizeMiB := range []int{1, 16, 64} {
		b.Run(fmt.Sprintf("%dMiB", sizeMiB), func(b *testing.B) {
			m, err := Open(filepath.Join(b.TempDir(), "sqlite.db"), 1)
			if err != nil {
				b.Fatal(err)
			}
			defer m.Close()
			chunkMiB := min(sizeMiB, 8)
			payload := bytes.Repeat([]byte{'x'}, chunkMiB<<20)
			if err := m.graph.db.Update(func(tx *latticedb.Tx) error {
				for offset := 0; offset < sizeMiB; offset += chunkMiB {
					if err := tx.PutAppMetadata(fmt.Appendf(nil, "benchmark/base/%d", offset), payload); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			payload = nil
			initial, err := m.beginGraphSnapshot(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			// Measure an immutable backup: the live WAL may rotate during directory walks.
			backupPath := filepath.Join(b.TempDir(), "initial.ltdb")
			if err := initial.Backup(backupPath); err != nil {
				_ = initial.Close()
				b.Fatal(err)
			}
			if err := initial.Close(); err != nil {
				b.Fatal(err)
			}
			info, err := os.Stat(backupPath)
			if err != nil {
				b.Fatal(err)
			}
			databaseBytes := info.Size()
			if databaseBytes < int64(sizeMiB)<<20 {
				b.Fatalf("database size %d bytes is smaller than requested %d MiB", databaseBytes, sizeMiB)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				if err := m.graph.db.Update(func(tx *latticedb.Tx) error {
					return tx.PutAppMetadata([]byte("benchmark/dirty"), fmt.Appendf(nil, "%d", i))
				}); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				snapshot, err := m.beginGraphSnapshot(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				if err := snapshot.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(databaseBytes)/(1<<20), "db-MiB")
		})
	}
}

func TestGraphAndKVMaterializer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sqlite.db")
	m, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	applyGraph := func(slot uint64, command types.GraphCommand) {
		t.Helper()
		value, err := types.EncodeGraphCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(ctx, slot, value); err != nil {
			t.Fatal(err)
		}
	}
	person := types.GraphCommand{
		RequestID: "person-1", Cypher: `CREATE (p:Person {id: $id, name: $name}) RETURN p.name`, Args: map[string]any{"id": "1", "name": "Ada"},
		Events: []types.GraphStreamEvent{{Stream: "people", Kind: "person.created", Payload: map[string]any{"id": "1"}}},
	}
	applyGraph(1, person)
	applyGraph(2, person)
	applyGraph(3, types.GraphCommand{RequestID: "offset-1", StreamOffset: &types.GraphStreamOffsetMutation{Stream: "people", Consumer: "projector", Sequence: 1}})
	value, err := types.EncodeKVCommand(types.KVCommand{RequestID: "kv-1", Operation: "put", Key: "mode", Value: []byte("graph")})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 4, value); err != nil {
		t.Fatalf("apply KV: %v", err)
	}
	result, err := m.GraphQuery(ctx, `MATCH (p:Person {id: $id}) RETURN p.name`, map[string]any{"id": "1"})
	if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != "Ada" {
		t.Fatalf("query result=%+v err=%v", result, err)
	}
	got, found, err := m.KVGet(ctx, "mode", time.Now())
	if err != nil || !found || string(got) != "graph" {
		t.Fatalf("KV got=%q found=%v err=%v", got, found, err)
	}
	snapshot, _, cleanup, err := m.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	applyGraph(5, types.GraphCommand{
		RequestID: "person-2", Cypher: `CREATE (p:Person {id: '2', name: 'Grace'})`,
		Events: []types.GraphStreamEvent{{Stream: "people", Kind: "person.created", Payload: map[string]any{"id": "2"}}},
	})
	if err := m.RestoreCheckpoint(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	result, err = m.GraphQuery(ctx, `MATCH (p:Person) RETURN p.name ORDER BY p.name`, nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != "Ada" || result.AppliedSlot != 4 || m.Tip() != 4 {
		t.Fatalf("restored graph result=%+v tip=%d err=%v", result, m.Tip(), err)
	}
	got, found, err = m.KVGet(ctx, "mode", time.Now())
	if err != nil || !found || string(got) != "graph" {
		t.Fatalf("restored KV got=%q found=%v err=%v", got, found, err)
	}
	records, streamSlot, err := m.GraphReadStream(ctx, "people", 0, 100, 0)
	if err != nil || len(records) != 1 || records[0].Kind != "person.created" || streamSlot != 4 {
		t.Fatalf("restored stream records=%#v err=%v", records, err)
	}
	offset, found, _, err := m.GraphStreamOffset(ctx, "people", "projector")
	if err != nil || !found || offset != 1 {
		t.Fatalf("restored stream offset=%d found=%v err=%v", offset, found, err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err = Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if m.Tip() != 4 {
		t.Fatalf("tip=%d, want 4", m.Tip())
	}
}

func TestLocalGraphIndexIsReconciledAfterRestore(t *testing.T) {
	ctx := context.Background()
	source, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	value, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "source", Cypher: "CREATE (:Concept {key: 'a'})"})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	files, _, cleanup, err := source.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	target, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if err := target.ConfigureLocalGraphNodePropertyIndexes([]types.GraphNodePropertyIndex{{Label: "Concept", Property: "key"}}); err != nil {
		t.Fatal(err)
	}
	if err := target.RestoreCheckpoint(ctx, files); err != nil {
		t.Fatal(err)
	}
	result, err := target.GraphReachable(ctx, types.GraphReachableRequest{
		StartLabel: "Concept", StartProperty: "key", StartValue: "a", EdgeType: "REL", ResultProperty: "key",
		MaxDepth: 1, MaxResults: 1, MaxScannedEdges: 1, MaxBytes: 128,
	})
	if err != nil || !result.StartFound || result.AppliedSlot != 1 {
		t.Fatalf("restored reachability=%#v error=%v", result, err)
	}
}

func TestGraphRestoreFailureReopensOriginalMaterializer(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, err := Open(filepath.Join(dir, "materialized.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	first, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "first", Cypher: "CREATE (:RestoreLive {value: 'ready'})"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 1, first); err != nil {
		t.Fatal(err)
	}
	files, _, cleanup, err := m.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	var sqlitePath string
	for _, file := range files {
		if file.Role == CheckpointSQLite {
			sqlitePath = file.Path
		}
	}
	if err := m.restoreParts(ctx, snapshotParts{sqlitePath: sqlitePath, graphDir: filepath.Join(dir, "missing-graph")}); err == nil {
		t.Fatal("restore with a missing graph snapshot succeeded")
	}
	if err := m.Health(ctx); err != nil {
		t.Fatalf("materializer remained closed after failed restore: %v", err)
	}
	second, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "second", Cypher: "CREATE (:RestoreLive {value: 'again'})"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 2, second); err != nil {
		t.Fatalf("apply after failed restore: %v", err)
	}
	result, err := m.GraphQuery(ctx, "MATCH (n:RestoreLive) RETURN count(n)", nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != int64(2) {
		t.Fatalf("query after failed restore result=%+v err=%v", result, err)
	}
}

func TestGraphCheckpointProgressesDuringWrites(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx := context.Background()
	stopWrites := make(chan struct{})
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		for slot := uint64(1); ; slot++ {
			select {
			case <-stopWrites:
				done <- nil
				return
			default:
			}
			value, encodeErr := types.EncodeKVCommand(types.KVCommand{RequestID: fmt.Sprintf("write-%d", slot), Operation: "put", Key: "active", Value: []byte("1")})
			if encodeErr != nil {
				done <- encodeErr
				return
			}
			if applyErr := m.Apply(ctx, slot, value); applyErr != nil {
				done <- applyErr
				return
			}
			if slot == 1 {
				close(started)
			}
		}
	}()
	<-started
	deadline, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	files, index, cleanup, err := m.CheckpointFilesAt(deadline)
	close(stopWrites)
	writeErr := <-done
	if err != nil {
		t.Fatal(err)
	}
	cleanup()
	if index == 0 || len(files) != 2 {
		t.Fatalf("checkpoint index=%d files=%d", index, len(files))
	}
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if final := m.Tip(); final <= index {
		t.Fatalf("writes did not progress during checkpoint: snapshot=%d final=%d", index, final)
	}
}

func TestGraphStreamWaitDoesNotBlockGraphApply(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := make(chan []types.GraphStreamRecord, 1)
	errs := make(chan error, 1)
	go func() {
		records, _, err := m.GraphReadStream(ctx, "events", 0, 100, time.Second)
		if err != nil {
			errs <- err
			return
		}
		result <- records
	}()
	value, err := types.EncodeGraphCommand(types.GraphCommand{
		RequestID: "create", Cypher: `CREATE (:Item {id: '1'})`,
		Events: []types.GraphStreamEvent{{Stream: "events", Kind: "item.created", Payload: "1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errs:
		t.Fatal(err)
	case records := <-result:
		if len(records) != 1 || records[0].Kind != "item.created" {
			t.Fatalf("records=%#v", records)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestFailedGraphCommandRecordsResultAndAdvancesTip(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx := context.Background()
	commands := []types.GraphCommand{
		{RequestID: "first", Cypher: `CREATE (:Item {id: '1'})`},
		{RequestID: "invalid", Cypher: `MATCH (`},
		{RequestID: "after", Cypher: `CREATE (:Item {id: '2'})`},
	}
	for i, command := range commands {
		value, err := types.EncodeGraphCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(ctx, uint64(i+1), value); err != nil {
			t.Fatal(err)
		}
	}
	receipt, found, err := m.GraphMutationReceipt(ctx, "invalid")
	if err != nil || !found || receipt.Status != types.MutationRejected || m.Tip() != 3 {
		t.Fatalf("receipt=%+v found=%v tip=%d err=%v", receipt, found, m.Tip(), err)
	}
	rows, err := m.GraphQuery(ctx, `MATCH (n:Item) RETURN n.id ORDER BY n.id`, nil)
	if err != nil || len(rows.Rows) != 2 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
}

func TestGraphBatchAppliesEveryCommandAtOneSlot(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	commands := []types.GraphCommand{
		{RequestID: "first", Cypher: `CREATE (:Item {id: '1'})`},
		{RequestID: "second", Cypher: `CREATE (:Item {id: '2'})`},
	}
	value, err := types.EncodeGraphBatch(commands)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 1, value); err != nil {
		t.Fatal(err)
	}
	rows, err := m.GraphQuery(context.Background(), `MATCH (n:Item) RETURN n.id ORDER BY n.id`, nil)
	if err != nil || len(rows.Rows) != 2 || m.Tip() != 1 {
		t.Fatalf("rows=%+v tip=%d err=%v", rows, m.Tip(), err)
	}
	for _, command := range commands {
		if _, found, err := m.GraphMutationReceipt(context.Background(), command.RequestID); err != nil || !found {
			t.Fatalf("request %q: found=%v err=%v", command.RequestID, found, err)
		}
	}
}

func TestGraphBatchKeepsFirstFingerprintForDuplicateRequestID(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	commands := []types.GraphCommand{
		{RequestID: "shared", Cypher: `CREATE (:Item {id: $id})`, Args: map[string]any{"id": float64(1)}},
		{RequestID: "shared", Cypher: `CREATE (:Item {id: $id})`, Args: map[string]any{"id": float64(2)}},
	}
	value, err := types.EncodeGraphBatch(commands)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 1, value); err != nil {
		t.Fatal(err)
	}
	for i, command := range commands {
		matches, err := m.GraphRequestMatches(context.Background(), command)
		if err != nil {
			t.Fatal(err)
		}
		if matches != (i == 0) {
			t.Fatalf("command %d matches=%v", i, matches)
		}
	}
}

func TestGraphSlotsKeepFirstFingerprintForDuplicateRequestID(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	commands := []types.GraphCommand{
		{RequestID: "shared", Cypher: `CREATE (:Item {id: $id})`, Args: map[string]any{"id": float64(1)}},
		{RequestID: "shared", Cypher: `CREATE (:Item {id: $id})`, Args: map[string]any{"id": float64(2)}},
	}
	for i, command := range commands {
		value, err := types.EncodeGraphCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(context.Background(), uint64(i+1), value); err != nil {
			t.Fatal(err)
		}
	}
	for i, command := range commands {
		matches, err := m.GraphRequestMatches(context.Background(), command)
		if err != nil {
			t.Fatal(err)
		}
		if matches != (i == 0) {
			t.Fatalf("command %d matches=%v", i, matches)
		}
	}
}

func TestGraphIdempotencyReceiptExpiresAtWindowBoundary(t *testing.T) {
	ctx := context.Background()
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	value, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "old", Cypher: `CREATE (:Item {id: '1'})`})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	var nonce [types.ReadBarrierNonceSize]byte
	for slot := uint64(2); slot <= 1025; slot++ {
		nonce[0] = byte(slot)
		if err := m.Apply(ctx, slot, types.EncodeReadBarrier(nonce)); err != nil {
			t.Fatal(err)
		}
	}
	if _, found, err := m.GraphMutationReceipt(ctx, "old"); err != nil || found {
		t.Fatalf("expired receipt found=%v err=%v", found, err)
	}
	reused, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "old", Cypher: `CREATE (:Item {id: 'reused'})`})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 1026, reused); err != nil {
		t.Fatalf("reuse expired request ID: %v", err)
	}
}

func TestGraphQueryWaitsForSQLiteApply(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	m.mu.Lock()
	m.graph.tip = 1
	done := make(chan error, 1)
	go func() {
		_, err := m.GraphQuery(context.Background(), `MATCH (n) RETURN n LIMIT 1`, nil)
		done <- err
	}()
	select {
	case err := <-done:
		m.mu.Unlock()
		t.Fatalf("query escaped apply lock: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	m.tip = 1
	m.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestGraphMethodsAfterCloseReturnError(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "closed.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := m.GraphQuery(ctx, "MATCH (n) RETURN n", nil); err == nil {
		t.Fatal("GraphQuery succeeded after close")
	}
	if _, _, err := m.GraphMutationReceipt(ctx, "closed"); err == nil {
		t.Fatal("GraphMutationReceipt succeeded after close")
	}
	command := types.GraphCommand{RequestID: "closed", Cypher: "CREATE (:Closed)"}
	if _, err := m.GraphRequestMatches(ctx, command); err == nil {
		t.Fatal("GraphRequestMatches succeeded after close")
	}
	if _, err := m.graphRequestExists("closed"); err == nil {
		t.Fatal("graphRequestExists succeeded after close")
	}
}
