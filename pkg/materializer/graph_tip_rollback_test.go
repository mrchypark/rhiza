package materializer

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/ncruces/go-sqlite3"
)

func TestGraphControlRollbackHardKillHelper(t *testing.T) {
	path := os.Getenv("RHIZA_GRAPH_ROLLBACK_CRASH_DB")
	if path == "" {
		return
	}
	m, err := OpenManagedSQLKVGraphWithAppendOnlyOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	graph, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "crash-once", Cypher: `CREATE (:Item {id: 'crash-once'})`})
	if err != nil {
		t.Fatal(err)
	}
	barrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	decisions := []quepaxa.DecidedValue{{Slot: 1, Value: graph}, {Slot: 2, Value: barrier}}
	ctx := &cancelAfterControlTip{Context: context.Background(), graph: m.graph, target: 2, done: make(chan struct{})}
	if err := m.ApplyBatch(ctx, decisions); err == nil || ctx.Err() != context.Canceled {
		t.Fatalf("apply=%v context=%v", err, ctx.Err())
	}
	if m.tip != 0 || m.graph.tip != 1 || m.graph.durableTip != 1 {
		t.Fatalf("rollback SQLite=%d Graph=%d durable=%d", m.tip, m.graph.tip, m.graph.durableTip)
	}
	if _, found, err := m.GraphMutationReceipt(context.Background(), "crash-once"); err == nil || found {
		t.Fatalf("premature receipt: found=%v err=%v", found, err)
	}
	fmt.Fprintln(os.Stdout, "ROLLBACK_READY")
	var b [1]byte
	_, _ = os.Stdin.Read(b[:])
	t.Fatal("child was not killed at the rollback boundary")
}

func TestGraphControlRollbackHardKillAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sqlite.db")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestGraphControlRollbackHardKillHelper$")
	cmd.Env = append(os.Environ(), "RHIZA_GRAPH_ROLLBACK_CRASH_DB="+path)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	ready := make(chan string, 1)
	go func() { line, _ := bufio.NewReader(stdout).ReadString('\n'); ready <- line }()
	select {
	case line := <-ready:
		if line != "ROLLBACK_READY\n" {
			_ = cmd.Wait()
			t.Fatalf("child boundary=%q stderr=%s", line, stderr.String())
		}
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("child boundary: %v stderr=%s", ctx.Err(), stderr.String())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	var exitErr *exec.ExitError
	if err := cmd.Wait(); !errors.As(err, &exitErr) || exitErr.ExitCode() != -1 {
		t.Fatalf("child termination=%v", err)
	}
	m, err := OpenManagedSQLKVGraphWithAppendOnlyOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if m.tip != 0 || m.graph.tip != 1 || m.graph.durableTip != 1 {
		t.Fatalf("reopen SQLite=%d Graph=%d durable=%d", m.tip, m.graph.tip, m.graph.durableTip)
	}
	if _, found, err := m.GraphMutationReceipt(context.Background(), "crash-once"); err == nil || found {
		t.Fatalf("reopened partial receipt: found=%v err=%v", found, err)
	}
	graph, err := types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "crash-once", Cypher: `CREATE (:Item {id: 'crash-once'})`})
	if err != nil {
		t.Fatal(err)
	}
	barrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	decisions := []quepaxa.DecidedValue{{Slot: 1, Value: graph}, {Slot: 2, Value: barrier}}
	if err := m.ApplyBatch(context.Background(), decisions); err != nil {
		t.Fatal(err)
	}
	first, found, err := m.GraphMutationReceipt(context.Background(), "crash-once")
	if err != nil || !found || first.Slot != 1 {
		t.Fatalf("receipt=%+v found=%v err=%v", first, found, err)
	}
	if err := m.ApplyBatch(context.Background(), decisions); err != nil {
		t.Fatal(err)
	}
	again, found, err := m.GraphMutationReceipt(context.Background(), "crash-once")
	if err != nil || !found || !reflect.DeepEqual(first, again) {
		t.Fatalf("replay receipt=%+v found=%v err=%v", again, found, err)
	}
	rows, err := m.GraphQuery(context.Background(), `MATCH (n:Item {id: 'crash-once'}) RETURN n.id`, nil)
	if err != nil || len(rows.Rows) != 1 {
		t.Fatalf("rows=%v err=%v", rows.Rows, err)
	}
}

type cancelAfterControlTip struct {
	context.Context
	graph  *graphState
	target uint64
	done   chan struct{}
	once   sync.Once
}

func (c *cancelAfterControlTip) Done() <-chan struct{} { return c.done }
func (c *cancelAfterControlTip) Err() error {
	reached := false
	if c.graph.mu.TryRLock() {
		reached = c.graph.tip >= c.target
		c.graph.mu.RUnlock()
	}
	if reached {
		c.once.Do(func() { close(c.done) })
	}
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

func TestGraphControlRollbackPreservesDurableGraphDecision(t *testing.T) {
	m, err := OpenManagedSQLKVGraphWithAppendOnlyOriginals(filepath.Join(t.TempDir(), "sqlite.db"), 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	command := types.GraphCommand{RequestID: "first", Cypher: `CREATE (:Item {id: 'first'})`, Events: []types.GraphStreamEvent{{Stream: "events", Kind: "created", Payload: "first"}}}
	graph, err := types.EncodeManagedGraphExecution(command)
	if err != nil {
		t.Fatal(err)
	}
	barrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	decisions := []quepaxa.DecidedValue{{Slot: 1, Value: graph}, {Slot: 2, Value: barrier}}
	ctx := &cancelAfterControlTip{Context: context.Background(), graph: m.graph, target: 2, done: make(chan struct{})}
	if err := m.ApplyBatch(ctx, decisions); err == nil || ctx.Err() != context.Canceled {
		t.Fatalf("apply: %v, context: %v", err, ctx.Err())
	} else if errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("confirmed auto-rollback became commit-unknown: %v", err)
	}
	if m.tip != 0 || m.graph.tip != 1 || m.graph.durableTip != 1 {
		t.Fatalf("SQLite=%d graph=%d durable graph=%d", m.tip, m.graph.tip, m.graph.durableTip)
	}
	journalBytes, err := m.graph.getMetadata(graphJournalKey)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := decodeGraphJournal(journalBytes)
	if err != nil || len(journal) != 1 || journal[0].Slot != 1 || journal[0].Hash != sha256.Sum256(graph) {
		t.Fatalf("durable journal=%+v err=%v", journal, err)
	}
	request, found, err := m.graph.request("first")
	if err != nil || !found || request.Original == nil {
		t.Fatalf("native original found=%v err=%v", found, err)
	}
	original, err := types.EncodeManagedGraphOriginal(command)
	if err != nil || !bytes.Equal(request.Original.CanonicalOriginal, original) || !bytes.Equal(request.Original.ExecutionItem, graph) || request.Original.DecisionHash != sha256.Sum256(graph) {
		t.Fatalf("native original=%+v err=%v", request.Original, err)
	}
	if err := m.Health(context.Background()); err == nil {
		t.Fatal("partially durable Graph decision must fail closed")
	}
	if _, found, err := m.GraphMutationReceipt(context.Background(), "first"); err == nil || found {
		t.Fatalf("partially applied request exposed receipt: found=%v err=%v", found, err)
	}
	if err := m.ApplyBatch(context.Background(), decisions); err != nil {
		t.Fatalf("same decision replay: %v", err)
	}
	if err := m.ApplyBatch(context.Background(), decisions); err != nil {
		t.Fatalf("duplicate replay: %v", err)
	}
	if receipt, found, err := m.GraphMutationReceipt(context.Background(), "first"); err != nil || !found || receipt.Slot != 1 {
		t.Fatalf("replayed receipt=%+v found=%v error=%v", receipt, found, err)
	}
	result, err := m.GraphQuery(context.Background(), `MATCH (n:Item {id: 'first'}) RETURN n.id`, nil)
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("replayed graph rows=%v error=%v", result.Rows, err)
	}
	events, _, err := m.GraphReadStream(context.Background(), "events", 0, 10, 0)
	if err != nil || len(events) != 1 {
		t.Fatalf("replayed events=%v error=%v", events, err)
	}
}

func TestGraphControlTipRollbackAfterCancel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sqlite.db")
	m, err := OpenManagedSQLKVGraphWithAppendOnlyOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	value := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	for slot := uint64(1); slot <= 2; slot++ {
		if err := m.Apply(context.Background(), slot, value); err != nil {
			t.Fatal(err)
		}
	}
	ctx := &cancelAfterControlTip{Context: context.Background(), graph: m.graph, target: 3, done: make(chan struct{})}
	if err := m.Apply(ctx, 3, value); (!errors.Is(err, sqlite3.INTERRUPT) && !errors.Is(err, context.Canceled)) || ctx.Err() != context.Canceled {
		t.Fatalf("apply: %v, context: %v", err, ctx.Err())
	} else {
		t.Logf("control cancellation: error=%v context-canceled=%v sqlite-interrupted=%v", err, errors.Is(err, context.Canceled), errors.Is(err, sqlite3.INTERRUPT))
	}
	if m.tip != 2 || m.graph.tip != 2 {
		t.Fatalf("rollback left SQLite tip %d and graph tip %d; want 2/2", m.tip, m.graph.tip)
	}
	if err := m.Health(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 3, value); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenManagedSQLKVGraphWithAppendOnlyOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.tip != 3 || reopened.graph.tip != 3 {
		t.Fatalf("reopen SQLite=%d Graph=%d", reopened.tip, reopened.graph.tip)
	}
}

func TestGraphControlTipRollbackAfterSlotGap(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "sqlite.db"), 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	barrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	if err := m.ApplyBatch(context.Background(), []quepaxa.DecidedValue{{Slot: 1, Value: barrier}, {Slot: 3, Value: barrier}}); err == nil {
		t.Fatal("expected slot gap")
	}
	if m.tip != 0 || m.graph.tip != 0 {
		t.Fatalf("rollback SQLite=%d Graph=%d", m.tip, m.graph.tip)
	}
	if err := m.Health(context.Background()); err != nil {
		t.Fatal(err)
	}
}
