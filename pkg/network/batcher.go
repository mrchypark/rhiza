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
	maxMutationBatch       = 32
	targetBatchBytes       = 64 << 10
	minAdaptiveLinger      = 25 * time.Microsecond
	maxAdaptiveLinger      = 250 * time.Microsecond
	maxOldestQueueAge      = 500 * time.Microsecond
	idleRateReset          = 10 * time.Millisecond
	maxQueuedRequests      = 4096
	maxQueuedEncodedBytes  = 8 << 20
	maxInflightBatches     = 8
	maxProposalOperations  = 12
	maxLocalProposals      = 8
	maxPeerProposals       = 4
	maxInflightEncodedByte = 1 << 20
	maxProposalEncodedByte = 2 << 20
	maxPeerEncodedByte     = 1 << 20
	batchItemOverhead      = 128
)

type batchResult struct {
	slot quepaxa.Slot
	err  error
}

type batchItem struct {
	ctx       context.Context
	requestID string
	encoded   []byte
	reserved  int
	enqueued  time.Time
	result    chan batchResult
}

type batchJob struct {
	items []*batchItem
	value []byte
}

type mutationBatcher[T any] struct {
	propose    func(context.Context, []byte) (quepaxa.Slot, error)
	apply      func(context.Context, quepaxa.Slot) error
	encodeItem func(T) ([]byte, error)
	assemble   func([][]byte) []byte
	requestID  func(T) string
	input      chan *batchItem
	jobs       chan batchJob
	inflight   chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	once       sync.Once
	wg         sync.WaitGroup
	budgetMu   sync.Mutex
	queuedN    int
	queuedByte int
	inflightB  int
}

func newMutationBatcher[T any](propose func(context.Context, []byte) (quepaxa.Slot, error), apply func(context.Context, quepaxa.Slot) error, encodeItem func(T) ([]byte, error), assemble func([][]byte) []byte, requestID func(T) string) *mutationBatcher[T] {
	ctx, cancel := context.WithCancel(context.Background())
	b := &mutationBatcher[T]{
		propose: propose, apply: apply, encodeItem: encodeItem, assemble: assemble, requestID: requestID,
		input: make(chan *batchItem, maxQueuedRequests), jobs: make(chan batchJob), inflight: make(chan struct{}, maxInflightBatches),
		ctx: ctx, cancel: cancel,
	}
	b.wg.Add(1 + maxInflightBatches)
	go b.run()
	for range maxInflightBatches {
		go b.worker()
	}
	return b
}

func newSQLBatcher(propose func(context.Context, []byte) (quepaxa.Slot, error), apply func(context.Context, quepaxa.Slot) error) *mutationBatcher[types.SQLCommand] {
	return newMutationBatcher(propose, apply, types.EncodeSQLBatchItem, types.AssembleSQLBatch, func(command types.SQLCommand) string { return command.RequestID })
}

func newGraphBatcher(propose func(context.Context, []byte) (quepaxa.Slot, error), apply func(context.Context, quepaxa.Slot) error) *mutationBatcher[types.GraphCommand] {
	return newMutationBatcher(propose, apply, types.EncodeGraphBatchItem, types.AssembleGraphBatch, func(command types.GraphCommand) string { return command.RequestID })
}

func newKVBatcher(propose func(context.Context, []byte) (quepaxa.Slot, error), apply func(context.Context, quepaxa.Slot) error) *mutationBatcher[types.KVCommand] {
	return newMutationBatcher(propose, apply, types.EncodeKVBatchItem, types.AssembleKVBatch, func(command types.KVCommand) string { return command.RequestID })
}

func (b *mutationBatcher[T]) submit(ctx context.Context, command T) (quepaxa.Slot, error) {
	encoded, err := b.encodeItem(command)
	if err != nil {
		return 0, err
	}
	if len(b.assemble([][]byte{encoded})) > quepaxa.MaxReplicatedValueBytes {
		return 0, ErrInvalidRequest
	}
	item := &batchItem{
		ctx: ctx, requestID: b.requestID(command), encoded: encoded,
		reserved: len(encoded) + batchItemOverhead, enqueued: time.Now(), result: make(chan batchResult, 1),
	}
	if !b.reserveQueue(item.reserved) {
		return 0, ErrOverloaded
	}
	select {
	case b.input <- item:
	case <-ctx.Done():
		b.releaseQueue(item.reserved)
		return 0, ctx.Err()
	case <-b.ctx.Done():
		b.releaseQueue(item.reserved)
		return 0, ErrNotReady
	default:
		b.releaseQueue(item.reserved)
		return 0, ErrOverloaded
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

func (b *mutationBatcher[T]) reserveQueue(size int) bool {
	b.budgetMu.Lock()
	defer b.budgetMu.Unlock()
	if b.queuedN >= maxQueuedRequests || size > maxQueuedEncodedBytes-b.queuedByte {
		return false
	}
	b.queuedN++
	b.queuedByte += size
	return true
}

func (b *mutationBatcher[T]) releaseQueue(size int) {
	b.budgetMu.Lock()
	b.queuedN--
	b.queuedByte -= size
	b.budgetMu.Unlock()
}

func (b *mutationBatcher[T]) idle() bool {
	b.budgetMu.Lock()
	idle := b.inflightB == 0 && b.queuedN == 0
	b.budgetMu.Unlock()
	return idle
}

func (b *mutationBatcher[T]) run() {
	defer b.wg.Done()
	defer close(b.jobs)
	var carry *batchItem
	var ewmaRate float64
	var lastArrival time.Time
	for {
		first, ok := b.next(carry)
		carry = nil
		if !ok {
			b.rejectQueued(ErrNotReady)
			return
		}
		idle := b.idle() && len(b.input) == 0
		items := []*batchItem{first}
		encoded := [][]byte{first.encoded}
		now := time.Now()
		ewmaRate, lastArrival = observeArrivalRate(ewmaRate, lastArrival, now, len(first.encoded))
		if idle {
			b.dispatch(items, encoded)
			continue
		}

	collect:
		for len(items) < maxMutationBatch {
			current := len(b.assemble(encoded))
			if current >= targetBatchBytes {
				break
			}
			select {
			case next := <-b.input:
				b.releaseQueue(next.reserved)
				now = time.Now()
				ewmaRate, lastArrival = observeArrivalRate(ewmaRate, lastArrival, now, len(next.encoded))
				if len(b.assemble(append(encoded, next.encoded))) > quepaxa.MaxReplicatedValueBytes {
					carry = next
					break collect
				}
				items = append(items, next)
				encoded = append(encoded, next.encoded)
				continue
			default:
			}

			wait := adaptiveWait(ewmaRate, current, time.Since(first.enqueued))
			if wait <= 0 {
				break
			}
			timer := time.NewTimer(wait)
			select {
			case next := <-b.input:
				if !timer.Stop() {
					<-timer.C
				}
				b.releaseQueue(next.reserved)
				now = time.Now()
				ewmaRate, lastArrival = observeArrivalRate(ewmaRate, lastArrival, now, len(next.encoded))
				if len(b.assemble(append(encoded, next.encoded))) > quepaxa.MaxReplicatedValueBytes {
					carry = next
					break collect
				}
				items = append(items, next)
				encoded = append(encoded, next.encoded)
			case <-timer.C:
				break
			case <-b.ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				b.fail(items, ErrNotReady)
				if carry != nil {
					b.fail([]*batchItem{carry}, ErrNotReady)
				}
				b.rejectQueued(ErrNotReady)
				return
			}
			break
		}
		if !lastArrival.IsZero() && time.Since(lastArrival) >= idleRateReset {
			ewmaRate = 0
		}
		b.dispatch(items, encoded)
	}
}

func (b *mutationBatcher[T]) next(carry *batchItem) (*batchItem, bool) {
	if carry != nil {
		return carry, true
	}
	select {
	case <-b.ctx.Done():
		return nil, false
	case item := <-b.input:
		b.releaseQueue(item.reserved)
		return item, true
	}
}

func observeArrivalRate(current float64, previous, now time.Time, bytes int) (float64, time.Time) {
	if previous.IsZero() || now.Sub(previous) >= idleRateReset {
		return 0, now
	}
	seconds := now.Sub(previous).Seconds()
	if seconds <= 0 {
		return current, now
	}
	rate := float64(bytes) / seconds
	if current == 0 {
		return rate, now
	}
	return current*7/8 + rate/8, now
}

func adaptiveWait(rate float64, currentBytes int, age time.Duration) time.Duration {
	remainingAge := maxOldestQueueAge - age
	if remainingAge <= 0 {
		return 0
	}
	wait := maxAdaptiveLinger
	if rate > 0 && currentBytes < targetBatchBytes {
		wait = time.Duration(float64(targetBatchBytes-currentBytes) / rate * float64(time.Second))
		if wait < minAdaptiveLinger {
			wait = minAdaptiveLinger
		}
		if wait > maxAdaptiveLinger {
			wait = maxAdaptiveLinger
		}
	}
	if wait > remainingAge {
		wait = remainingAge
	}
	return wait
}

func (b *mutationBatcher[T]) dispatch(items []*batchItem, encoded [][]byte) {
	active := items[:0]
	activeEncoded := encoded[:0]
	for i, item := range items {
		if item.ctx.Err() != nil {
			item.result <- batchResult{err: item.ctx.Err()}
			continue
		}
		active = append(active, item)
		activeEncoded = append(activeEncoded, encoded[i])
	}
	if len(active) == 0 {
		return
	}
	value := b.assemble(activeEncoded)
	b.inflight <- struct{}{}
	b.budgetMu.Lock()
	if len(value) > maxInflightEncodedByte-b.inflightB {
		b.budgetMu.Unlock()
		<-b.inflight
		b.fail(active, ErrOverloaded)
		return
	}
	b.inflightB += len(value)
	b.budgetMu.Unlock()
	select {
	case b.jobs <- batchJob{items: active, value: value}:
	case <-b.ctx.Done():
		b.releaseInflight(len(value))
		b.fail(active, ErrNotReady)
	}
}

func (b *mutationBatcher[T]) worker() {
	defer b.wg.Done()
	for job := range b.jobs {
		b.execute(job.items, job.value)
		b.releaseInflight(len(job.value))
	}
}

func (b *mutationBatcher[T]) releaseInflight(size int) {
	b.budgetMu.Lock()
	b.inflightB -= size
	b.budgetMu.Unlock()
	<-b.inflight
}

func (b *mutationBatcher[T]) execute(items []*batchItem, value []byte) {
	ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
	slot, err := b.propose(ctx, value)
	if err == nil && b.apply != nil {
		err = b.apply(ctx, slot)
	}
	cancel()
	for _, item := range items {
		itemErr := err
		if errors.Is(err, ErrDurabilityUnavailable) {
			itemErr = commitUnknown(slot, item.requestID, err)
		}
		item.result <- batchResult{slot: slot, err: itemErr}
	}
}

func (b *mutationBatcher[T]) fail(items []*batchItem, err error) {
	for _, item := range items {
		item.result <- batchResult{err: err}
	}
}

func (b *mutationBatcher[T]) rejectQueued(err error) {
	for {
		select {
		case item := <-b.input:
			b.releaseQueue(item.reserved)
			item.result <- batchResult{err: err}
		default:
			return
		}
	}
}

func (b *mutationBatcher[T]) Close() {
	b.once.Do(func() {
		b.cancel()
		b.wg.Wait()
	})
}
