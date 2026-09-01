package network

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestSQLBatcherIdleRequestUsesBoundedMicrobatchWindow(t *testing.T) {
	if wait := adaptiveWait(0, 1, 0); wait != maxAdaptiveLinger {
		t.Fatalf("idle batch wait=%v, want %v", wait, maxAdaptiveLinger)
	}
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
	case <-time.After(5 * time.Second):
		t.Fatal("idle request did not dispatch")
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

func TestSQLBatcherReportsCommitUnknownAfterProposalStarts(t *testing.T) {
	b := newSQLBatcher(func(context.Context, []byte) (quepaxa.Slot, error) {
		return 7, quepaxa.ErrQuorumUnavailable
	}, func(context.Context, quepaxa.Slot) error { return nil })
	defer b.Close()

	requestID := "uncertain"
	_, err := b.submit(context.Background(), types.SQLCommand{RequestID: requestID, SQL: "INSERT INTO t VALUES (1)"})
	var unknown *CommitUnknownError
	if !errors.As(err, &unknown) || unknown.Slot != 7 || unknown.RequestID != requestID || unknown.RetryThroughSlot < 7 {
		t.Fatalf("commit unknown detail=%#v err=%v", unknown, err)
	}
}

func TestSQLBatcherCancellationAfterDispatchReportsCommitUnknown(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	b := newSQLBatcher(func(context.Context, []byte) (quepaxa.Slot, error) {
		close(started)
		<-release
		return 1, nil
	}, func(context.Context, quepaxa.Slot) error { return nil })
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := b.submit(ctx, types.SQLCommand{RequestID: "canceled", SQL: "INSERT INTO t VALUES (1)"})
		done <- err
	}()
	<-started
	cancel()
	err := <-done
	var unknown *CommitUnknownError
	if !errors.As(err, &unknown) || unknown.Slot != 0 || unknown.RequestID != "canceled" || !errors.Is(err, context.Canceled) {
		t.Fatalf("commit unknown detail=%#v err=%v", unknown, err)
	}
	close(release)
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

func TestSQLBatcherCoalescesAgedBacklog(t *testing.T) {
	entered := make(chan struct{}, 64)
	release := make(chan struct{})
	sizes := make(chan int, 64)
	var slots atomic.Uint64
	batcher := newSQLBatcher(func(_ context.Context, value []byte) (quepaxa.Slot, error) {
		commands, ok, err := types.DecodeSQLBatch(value)
		if err != nil || !ok {
			return 0, errors.New("invalid SQL batch")
		}
		sizes <- len(commands)
		entered <- struct{}{}
		<-release
		return quepaxa.Slot(slots.Add(1)), nil
	}, nil)
	released := false
	defer func() {
		if !released {
			close(release)
		}
		batcher.Close()
	}()

	done := make(chan error, maxInflightBatches)
	submit := func(id int) {
		_, err := batcher.submit(context.Background(), types.SQLCommand{RequestID: strconv.Itoa(id), SQL: "INSERT INTO bench(value) VALUES (1)"})
		done <- err
	}
	for i := range maxInflightBatches {
		go submit(i)
		<-entered
	}
	backlog := make([]*batchItem, 0, maxMutationBatch)
	batcher.budgetMu.Lock()
	for i := range maxMutationBatch {
		id := strconv.Itoa(maxInflightBatches + i)
		encoded, err := types.EncodeSQLBatchItem(types.SQLCommand{RequestID: id, SQL: "INSERT INTO bench(value) VALUES (1)"})
		if err != nil {
			batcher.budgetMu.Unlock()
			t.Fatal(err)
		}
		item := &batchItem{
			ctx: context.Background(), requestID: id, encoded: encoded,
			reserved: len(encoded) + batchItemOverhead, enqueued: time.Now().Add(-maxOldestQueueAge - time.Millisecond), result: make(chan batchResult, 1),
		}
		batcher.queuedN++
		batcher.queuedByte += item.reserved
		batcher.input <- item
		backlog = append(backlog, item)
	}
	batcher.budgetMu.Unlock()
	close(release)
	released = true
	for range maxInflightBatches {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	for _, item := range backlog {
		if result := <-item.result; result.err != nil {
			t.Fatal(result.err)
		}
	}
	close(sizes)
	maxSize := 0
	for size := range sizes {
		if size > maxSize {
			maxSize = size
		}
	}
	if maxSize != maxMutationBatch {
		t.Fatalf("largest batch=%d, want %d", maxSize, maxMutationBatch)
	}
}

func BenchmarkSQLBatcherParallel(b *testing.B) {
	for _, parallelism := range []int{2, 32} {
		b.Run("c"+strconv.Itoa(parallelism*runtime.GOMAXPROCS(0)), func(b *testing.B) {
			var batches atomic.Uint64
			var batchSizes [maxMutationBatch + 1]atomic.Uint64
			batcher := newSQLBatcher(func(_ context.Context, value []byte) (quepaxa.Slot, error) {
				commands, ok, err := types.DecodeSQLBatch(value)
				if err != nil || !ok || len(commands) == 0 {
					b.Fatalf("decode batch: commands=%d ok=%v err=%v", len(commands), ok, err)
				}
				batchSizes[len(commands)].Add(1)
				time.Sleep(5 * time.Millisecond)
				return quepaxa.Slot(batches.Add(1)), nil
			}, nil)
			defer batcher.Close()
			var sequence atomic.Uint64
			b.SetParallelism(parallelism)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := strconv.FormatUint(sequence.Add(1), 10)
					if _, err := batcher.submit(context.Background(), types.SQLCommand{RequestID: id, SQL: "INSERT INTO bench(value) VALUES (1)"}); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.StopTimer()
			if count := batches.Load(); count != 0 {
				b.ReportMetric(float64(b.N)/float64(count), "commands/batch")
				middle := (count + 1) / 2
				var cumulative uint64
				for size := 1; size <= maxMutationBatch; size++ {
					cumulative += batchSizes[size].Load()
					if cumulative >= middle {
						b.ReportMetric(float64(size), "p50-commands/batch")
						break
					}
				}
			}
		})
	}
}
