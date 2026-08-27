package network

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const (
	maxMutationBatch = 16
	maxBatchDelay    = 2 * time.Millisecond
)

type batchResult struct {
	slot quepaxa.Slot
	err  error
}

type batchItem[T any] struct {
	ctx     context.Context
	command T
	result  chan batchResult
}

type mutationBatcher[T any] struct {
	propose   func(context.Context, []byte) (quepaxa.Slot, error)
	apply     func(context.Context, quepaxa.Slot) error
	encode    func([]T) ([]byte, error)
	requestID func(T) string
	input     chan batchItem[T]
	inflight  chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	once      sync.Once
	wg        sync.WaitGroup
}

func newMutationBatcher[T any](propose func(context.Context, []byte) (quepaxa.Slot, error), apply func(context.Context, quepaxa.Slot) error, encode func([]T) ([]byte, error), requestID func(T) string) *mutationBatcher[T] {
	ctx, cancel := context.WithCancel(context.Background())
	b := &mutationBatcher[T]{propose: propose, apply: apply, encode: encode, requestID: requestID, input: make(chan batchItem[T], 1024), inflight: make(chan struct{}, 16), ctx: ctx, cancel: cancel}
	b.wg.Add(1)
	go b.run()
	return b
}

func newSQLBatcher(propose func(context.Context, []byte) (quepaxa.Slot, error), apply func(context.Context, quepaxa.Slot) error) *mutationBatcher[types.SQLCommand] {
	return newMutationBatcher(propose, apply, types.EncodeSQLBatch, func(command types.SQLCommand) string { return command.RequestID })
}

func newGraphBatcher(propose func(context.Context, []byte) (quepaxa.Slot, error), apply func(context.Context, quepaxa.Slot) error) *mutationBatcher[types.GraphCommand] {
	return newMutationBatcher(propose, apply, types.EncodeGraphBatch, func(command types.GraphCommand) string { return command.RequestID })
}

func (b *mutationBatcher[T]) submit(ctx context.Context, command T) (quepaxa.Slot, error) {
	item := batchItem[T]{ctx: ctx, command: command, result: make(chan batchResult, 1)}
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

func (b *mutationBatcher[T]) run() {
	defer b.wg.Done()
	for {
		select {
		case <-b.ctx.Done():
			return
		default:
		}
		var first batchItem[T]
		select {
		case <-b.ctx.Done():
			return
		case first = <-b.input:
		}
		items := []batchItem[T]{first}
		timer := time.NewTimer(maxBatchDelay)
	collect:
		for len(items) < maxMutationBatch {
			select {
			case item := <-b.input:
				items = append(items, item)
			case <-timer.C:
				break collect
			case <-b.ctx.Done():
				timer.Stop()
				return
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		active := make([]batchItem[T], 0, len(items))
		commands := make([]T, 0, len(items))
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

func (b *mutationBatcher[T]) execute(items []batchItem[T], commands []T) {
	value, err := b.encode(commands)
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
		itemErr := err
		if errors.Is(err, ErrDurabilityUnavailable) {
			itemErr = commitUnknown(slot, b.requestID(item.command), err)
		}
		item.result <- batchResult{slot: slot, err: itemErr}
	}
}

func (b *mutationBatcher[T]) Close() {
	b.once.Do(func() {
		b.cancel()
		b.wg.Wait()
	})
}
