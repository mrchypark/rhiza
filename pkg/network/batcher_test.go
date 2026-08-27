package network

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestSQLBatcherCombinesQueuedCommands(t *testing.T) {
	var mu sync.Mutex
	proposals := 0
	commands := 0
	ctx, cancel := context.WithCancel(context.Background())
	b := &mutationBatcher[types.SQLCommand]{
		input:     make(chan batchItem[types.SQLCommand], 16),
		inflight:  make(chan struct{}, 16),
		ctx:       ctx,
		cancel:    cancel,
		encode:    types.EncodeSQLBatch,
		requestID: func(command types.SQLCommand) string { return command.RequestID },
		propose: func(_ context.Context, value []byte) (quepaxa.Slot, error) {
			batch, ok, err := types.DecodeSQLBatch(value)
			if err != nil || !ok {
				t.Fatalf("decode batch: ok=%v err=%v", ok, err)
			}
			mu.Lock()
			proposals++
			commands += len(batch)
			mu.Unlock()
			return 1, nil
		},
		apply: func(context.Context, quepaxa.Slot) error { return nil },
	}
	b.wg.Add(1)

	results := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func(i int) {
			_, err := b.submit(context.Background(), types.SQLCommand{RequestID: string(rune('a' + i)), SQL: "SELECT 1"})
			results <- err
		}(i)
	}
	deadline := time.Now().Add(time.Second)
	for len(b.input) < 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	go b.run()
	defer b.Close()
	for range 4 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if proposals != 1 || commands != 4 {
		t.Fatalf("proposals=%d commands=%d", proposals, commands)
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

func TestSQLBatcherSplitsOversizedCombinedValue(t *testing.T) {
	var sizes []int
	b := &mutationBatcher[types.SQLCommand]{
		ctx:       context.Background(),
		encode:    types.EncodeSQLBatch,
		requestID: func(command types.SQLCommand) string { return command.RequestID },
		propose: func(_ context.Context, value []byte) (quepaxa.Slot, error) {
			sizes = append(sizes, len(value))
			return quepaxa.Slot(len(sizes)), nil
		},
		apply: func(context.Context, quepaxa.Slot) error { return nil },
	}
	items := []batchItem[types.SQLCommand]{
		{result: make(chan batchResult, 1)},
		{result: make(chan batchResult, 1)},
	}
	commands := []types.SQLCommand{
		{RequestID: "a", SQL: "INSERT INTO t VALUES (?)", Args: []any{make([]byte, 70<<10)}},
		{RequestID: "b", SQL: "INSERT INTO t VALUES (?)", Args: []any{make([]byte, 70<<10)}},
	}
	b.execute(items, commands)
	if len(sizes) != 2 {
		t.Fatalf("proposals=%d, want split into 2", len(sizes))
	}
	for _, size := range sizes {
		if size > quepaxa.MaxReplicatedValueBytes {
			t.Fatalf("oversized proposal: %d", size)
		}
	}
}
