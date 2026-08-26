package network

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const (
	maxSQLBatch = 16
)

type batchResult struct {
	slot quepaxa.Slot
	err  error
}

type batchItem struct {
	ctx     context.Context
	command types.SQLCommand
	result  chan batchResult
}

type sqlBatcher struct {
	propose  func(context.Context, []byte) (quepaxa.Slot, error)
	apply    func(context.Context, quepaxa.Slot) error
	input    chan batchItem
	inflight chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	once     sync.Once
	wg       sync.WaitGroup
}

func newSQLBatcher(propose func(context.Context, []byte) (quepaxa.Slot, error), apply func(context.Context, quepaxa.Slot) error) *sqlBatcher {
	ctx, cancel := context.WithCancel(context.Background())
	b := &sqlBatcher{propose: propose, apply: apply, input: make(chan batchItem, 1024), inflight: make(chan struct{}, 16), ctx: ctx, cancel: cancel}
	b.wg.Add(1)
	go b.run()
	return b
}

func (b *sqlBatcher) submit(ctx context.Context, command types.SQLCommand) (quepaxa.Slot, error) {
	item := batchItem{ctx: ctx, command: command, result: make(chan batchResult, 1)}
	select {
	case b.input <- item:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-b.ctx.Done():
		return 0, ErrNotReady
	}
	select {
	case result := <-item.result:
		return result.slot, result.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-b.ctx.Done():
		return 0, ErrNotReady
	}
}

func (b *sqlBatcher) run() {
	defer b.wg.Done()
	for {
		select {
		case <-b.ctx.Done():
			return
		default:
		}
		var first batchItem
		select {
		case <-b.ctx.Done():
			return
		case first = <-b.input:
		}
		items := []batchItem{first}
		// Let concurrently submitted commands reach the queue without imposing
		// a fixed latency floor on an isolated request.
		runtime.Gosched()
	collect:
		for len(items) < maxSQLBatch {
			select {
			case item := <-b.input:
				items = append(items, item)
			default:
				break collect
			}
		}

		active := make([]batchItem, 0, len(items))
		commands := make([]types.SQLCommand, 0, len(items))
		for _, item := range items {
			if item.ctx.Err() == nil {
				active = append(active, item)
				commands = append(commands, item.command)
			}
		}
		if len(commands) == 0 {
			continue
		}
		b.inflight <- struct{}{}
		b.wg.Add(1)
		go func() {
			defer func() { <-b.inflight; b.wg.Done() }()
			b.execute(active, commands)
		}()
	}
}

func (b *sqlBatcher) execute(items []batchItem, commands []types.SQLCommand) {
	value, err := types.EncodeSQLBatch(commands)
	if err == nil && len(value) > quepaxa.MaxReplicatedValueBytes && len(items) > 1 {
		mid := len(items) / 2
		b.execute(items[:mid], commands[:mid])
		b.execute(items[mid:], commands[mid:])
		return
	}
	if err == nil && len(value) > quepaxa.MaxReplicatedValueBytes {
		err = ErrInvalidRequest
	}
	var slot quepaxa.Slot
	if err == nil {
		ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
		slot, err = b.propose(ctx, value)
		if err == nil {
			err = b.apply(ctx, slot)
		}
		cancel()
	}
	for _, item := range items {
		item.result <- batchResult{slot: slot, err: err}
	}
}

func (b *sqlBatcher) Close() {
	b.once.Do(func() {
		b.cancel()
		b.wg.Wait()
	})
}
