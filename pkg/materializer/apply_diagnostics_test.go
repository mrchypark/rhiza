package materializer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/internal/applydiag"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	sqlite3 "github.com/ncruces/go-sqlite3"
)

func TestApplyDiagnosticsConflictGapAndIdentity(t *testing.T) {
	ctx := context.Background()
	m, err := Open(filepath.Join(t.TempDir(), "state.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	first := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	otherNonce := [types.ReadBarrierNonceSize]byte{1}
	other := types.EncodeReadBarrier(otherNonce)
	if err := m.Apply(ctx, 1, first); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		decision quepaxa.DecidedValue
		want     applydiag.Site
	}{
		{"conflict", quepaxa.DecidedValue{Slot: 1, Value: other}, applydiag.SlotConflict},
		{"gap", quepaxa.DecidedValue{Slot: 3, Value: first}, applydiag.SlotGap},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var recorder applydiag.Recorder
			observed := m.ApplyBatch(applydiag.WithRecorder(ctx, &recorder), []quepaxa.DecidedValue{tc.decision})
			plain := m.ApplyBatch(ctx, []quepaxa.DecidedValue{tc.decision})
			if observed == nil || plain == nil || observed.Error() != plain.Error() || recorder.Site() != tc.want || m.Tip() != 1 {
				t.Fatalf("observed=%v plain=%v site=%s tip=%d", observed, plain, recorder.Site(), m.Tip())
			}
		})
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	var recorder applydiag.Recorder
	err = m.ApplyBatch(applydiag.WithRecorder(ctx, &recorder), []quepaxa.DecidedValue{{Slot: 2, Value: first}})
	if err != sql.ErrConnDone || !errors.Is(err, sql.ErrConnDone) || recorder.Site() != applydiag.Writer {
		t.Fatalf("closed writer err=%v site=%s", err, recorder.Site())
	}
}

func TestApplyDiagnosticsRecorderResetsAfterSuccess(t *testing.T) {
	ctx := context.Background()
	m, err := Open(filepath.Join(t.TempDir(), "state.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	value := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	var recorder applydiag.Recorder
	observed := applydiag.WithRecorder(ctx, &recorder)
	if err := m.ApplyBatch(observed, []quepaxa.DecidedValue{{Slot: 2, Value: value}}); err == nil || recorder.Site() != applydiag.SlotGap {
		t.Fatalf("gap err=%v site=%s", err, recorder.Site())
	}
	if err := m.ApplyBatch(observed, []quepaxa.DecidedValue{{Slot: 1, Value: value}}); err != nil || recorder.Site() != applydiag.Unknown || m.Tip() != 1 {
		t.Fatalf("success err=%v site=%s tip=%d", err, recorder.Site(), m.Tip())
	}
}

func TestApplyDiagnosticsCommitAcknowledgementLossThenPoison(t *testing.T) {
	m, err := Open(filepath.Join(t.TempDir(), "state.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	cause := errors.New("private simulated commit acknowledgement loss")
	m.afterApplyCommitError = func() error { return cause }
	value := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	var recorder applydiag.Recorder
	ctx := applydiag.WithRecorder(context.Background(), &recorder)
	err = m.ApplyBatch(ctx, []quepaxa.DecidedValue{{Slot: 1, Value: value}})
	if !errors.Is(err, cause) || !errors.Is(err, ErrGraphCommitUnknown) || recorder.Site() != applydiag.Commit || m.Tip() != 0 {
		t.Fatalf("commit boundary err=%v site=%s tip=%d", err, recorder.Site(), m.Tip())
	}
	err = m.ApplyBatch(ctx, []quepaxa.DecidedValue{{Slot: 1, Value: value}})
	if !errors.Is(err, ErrGraphCommitUnknown) || errors.Is(err, cause) || recorder.Site() != applydiag.Health || m.Tip() != 0 {
		t.Fatalf("subsequent poison err=%v site=%s tip=%d", err, recorder.Site(), m.Tip())
	}
}

func TestApplyDiagnosticsActualRollbackFailureThenPoison(t *testing.T) {
	ctx := context.Background()
	m, err := Open(filepath.Join(t.TempDir(), "state.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	for i, statement := range []string{
		"CREATE TABLE items(id INTEGER PRIMARY KEY)",
		"CREATE TRIGGER tr AFTER INSERT ON items WHEN new.id=2 BEGIN INSERT INTO items VALUES(9223372036854775807); SELECT RAISE(ROLLBACK,'later'); END",
	} {
		value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: fmt.Sprint(i), SQL: statement}})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Apply(ctx, uint64(i+1), value); err != nil {
			t.Fatal(err)
		}
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "lost-savepoint", SQL: "INSERT INTO items VALUES(2)"}})
	if err != nil {
		t.Fatal(err)
	}
	var recorder applydiag.Recorder
	observed := applydiag.WithRecorder(ctx, &recorder)
	err = m.ApplyBatch(observed, []quepaxa.DecidedValue{{Slot: 3, Value: value}})
	if !errors.Is(err, ErrGraphCommitUnknown) || !errors.Is(err, sqlite3.ERROR) || !strings.Contains(err.Error(), "rollback apply batch:") || recorder.Site() != applydiag.ApplyValue || m.Tip() != 2 {
		t.Fatalf("rollback failure err=%v site=%s tip=%d", err, recorder.Site(), m.Tip())
	}
	err = m.ApplyBatch(observed, []quepaxa.DecidedValue{{Slot: 3, Value: value}})
	if !errors.Is(err, ErrGraphCommitUnknown) || recorder.Site() != applydiag.Health || m.Tip() != 2 {
		t.Fatalf("subsequent poison err=%v site=%s tip=%d", err, recorder.Site(), m.Tip())
	}
}

func TestApplyDiagnosticsConcurrentRecordersAreIndependent(t *testing.T) {
	ctx := context.Background()
	m, err := Open(filepath.Join(t.TempDir(), "state.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	first := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})
	if err := m.Apply(ctx, 1, first); err != nil {
		t.Fatal(err)
	}
	other := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{1})
	var recorders [2]applydiag.Recorder
	decisions := []quepaxa.DecidedValue{{Slot: 1, Value: other}, {Slot: 3, Value: first}}
	var outcomes [2]error
	var wg sync.WaitGroup
	for i := range decisions {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			outcomes[i] = m.ApplyBatch(applydiag.WithRecorder(ctx, &recorders[i]), []quepaxa.DecidedValue{decisions[i]})
		}(i)
	}
	wg.Wait()
	if outcomes[0] == nil || outcomes[1] == nil || recorders[0].Site() != applydiag.SlotConflict || recorders[1].Site() != applydiag.SlotGap || m.Tip() != 1 {
		t.Fatalf("outcomes=%v sites=%s,%s tip=%d", outcomes, recorders[0].Site(), recorders[1].Site(), m.Tip())
	}
	if applydiag.Site(999).String() != "unknown" {
		t.Fatal("invalid site leaked")
	}
}
