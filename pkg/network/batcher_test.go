package network

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestSQLBatcherIdleRequestDispatchesImmediately(t *testing.T) {
	proposed := make(chan []byte, 1)
	b := newSQLBatcher(func(_ context.Context, value []byte) (quepaxa.Slot, error) {
		proposed <- value
		return 1, nil
	}, func(context.Context, quepaxa.Slot) error { return nil })
	defer b.Close()

	done := make(chan error, 1)
	go func() {
		_, err := b.submit(context.Background(), types.SQLCommand{RequestID: "idle", SQL: "SELECT 1"})
		done <- err
	}()
	select {
	case value := <-proposed:
		commands, ok, err := types.DecodeSQLBatch(value)
		if err != nil || !ok || len(commands) != 1 || commands[0].RequestID != "idle" {
			t.Fatalf("decode batch: commands=%v ok=%v err=%v", commands, ok, err)
		}
	case <-time.After(20 * time.Millisecond):
		t.Fatal("idle request waited for a batching timer")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAdaptiveWaitBounds(t *testing.T) {
	if got := adaptiveWait(1<<40, 1, 0); got != minAdaptiveLinger {
		t.Fatalf("minimum wait=%v", got)
	}
	if got := adaptiveWait(1, 1, 0); got != maxAdaptiveLinger {
		t.Fatalf("maximum wait=%v", got)
	}
	if got := adaptiveWait(1, 1, maxOldestQueueAge); got != 0 {
		t.Fatalf("expired wait=%v", got)
	}
}

func TestSQLBatcherCloseStopsPendingWork(t *testing.T) {
	started := make(chan struct{})
	b := newSQLBatcher(func(ctx context.Context, _ []byte) (quepaxa.Slot, error) {
		close(started)
		<-ctx.Done()
		return 0, ctx.Err()
	}, func(context.Context, quepaxa.Slot) error { return nil })
	done := make(chan error, 1)
	go func() {
		_, err := b.submit(context.Background(), types.SQLCommand{RequestID: "pending", SQL: "SELECT 1"})
		done <- err
	}()
	<-started
	b.Close()
	if err := <-done; !errors.Is(err, ErrNotReady) {
		t.Fatalf("pending submit error=%v, want ErrNotReady", err)
	}
	if _, err := b.submit(context.Background(), types.SQLCommand{RequestID: "closed", SQL: "SELECT 1"}); !errors.Is(err, ErrNotReady) {
		t.Fatalf("closed submit error=%v, want ErrNotReady", err)
	}
}

func TestSQLBatcherRejectsOversizedItemAtAdmission(t *testing.T) {
	b := newSQLBatcher(func(context.Context, []byte) (quepaxa.Slot, error) {
		t.Fatal("oversized item reached proposer")
		return 0, nil
	}, func(context.Context, quepaxa.Slot) error { return nil })
	defer b.Close()
	_, err := b.submit(context.Background(), types.SQLCommand{
		RequestID: "oversized", SQL: "INSERT INTO t VALUES (?)", Args: []any{make([]byte, quepaxa.MaxReplicatedValueBytes)},
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("error=%v, want ErrInvalidRequest", err)
	}
}

func TestBatchAssemblersMatchCanonicalJSON(t *testing.T) {
	commands := []types.SQLCommand{{RequestID: "a", SQL: "SELECT 1"}, {RequestID: "b", SQL: "SELECT 2"}}
	want, err := types.EncodeSQLBatch(commands)
	if err != nil {
		t.Fatal(err)
	}
	items := make([][]byte, len(commands))
	for i := range commands {
		items[i], err = types.EncodeSQLBatchItem(commands[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	got := types.AssembleSQLBatch(items)
	if string(got) != string(want) {
		t.Fatalf("assembled=%q want=%q", got, want)
	}
}
