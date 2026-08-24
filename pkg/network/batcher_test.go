package network

import (
	"context"
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
	b := &sqlBatcher{
		input:    make(chan batchItem, 16),
		inflight: make(chan struct{}, 16),
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
