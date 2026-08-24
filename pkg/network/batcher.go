package network

import (
	"context"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const (
	maxSQLBatch = 16
	batchWait   = time.Millisecond
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
}

func newSQLBatcher(propose func(context.Context, []byte) (quepaxa.Slot, error), apply func(context.Context, quepaxa.Slot) error) *sqlBatcher {
	b := &sqlBatcher{propose: propose, apply: apply, input: make(chan batchItem, 1024), inflight: make(chan struct{}, 16)}
	go b.run()
	return b
}

func (b *sqlBatcher) submit(ctx context.Context, command types.SQLCommand) (quepaxa.Slot, error) {
	item := batchItem{ctx: ctx, command: command, result: make(chan batchResult, 1)}
	select {
	case b.input <- item:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	select {
	case result := <-item.result:
		return result.slot, result.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (b *sqlBatcher) run() {
	for first := range b.input {
		items := []batchItem{first}
		timer := time.NewTimer(batchWait)
	collect:
		for len(items) < maxSQLBatch {
			select {
			case item := <-b.input:
				items = append(items, item)
			case <-timer.C:
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		commands := make([]types.SQLCommand, 0, len(items))
		for _, item := range items {
			if item.ctx.Err() == nil {
				commands = append(commands, item.command)
			}
		}
		if len(commands) == 0 {
			continue
		}
		b.inflight <- struct{}{}
		go func() {
			defer func() { <-b.inflight }()
			b.execute(items, commands)
		}()
	}
}

func (b *sqlBatcher) execute(items []batchItem, commands []types.SQLCommand) {
	value, err := types.EncodeSQLBatch(commands)
	var slot quepaxa.Slot
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
