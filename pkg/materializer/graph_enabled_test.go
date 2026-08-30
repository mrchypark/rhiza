//go:build graph

package materializer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	latticedb "github.com/jeffhajewski/latticedb/bindings/go"
	"github.com/mrchypark/rhiza/internal/types"
)

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
	if err := m.applyGraph(ctx, 1, value, commands, true); err != nil {
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
	if err != nil || len(journal) != 0 {
		t.Fatalf("confirmed recovery journal=%x err=%v", journal, err)
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
			initial, err := m.beginGraphSnapshot()
			if err != nil {
				b.Fatal(err)
			}
			if err := initial.Close(); err != nil {
				b.Fatal(err)
			}
			info, err := os.Stat(filepath.Join(filepath.Dir(m.dbPath), "latticedb", "graph.ltdb"))
			if err != nil {
				b.Fatal(err)
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
				snapshot, err := m.beginGraphSnapshot()
				if err != nil {
					b.Fatal(err)
				}
				if err := snapshot.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(info.Size())/(1<<20), "db-MiB")
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
}

func TestGraphQueryWaitsForSQLiteApply(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	m.mu.Lock()
	if err := m.graph.advanceTip(context.Background(), 1, [32]byte{}); err != nil {
		m.mu.Unlock()
		t.Fatal(err)
	}
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
