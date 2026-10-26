package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/thanos-io/objstore"
)

const localRPCTimeout = 30 * time.Second

type localOperationKey struct{}
type localOperation struct {
	request   context.Context
	timeout   time.Duration
	uncertain atomic.Bool
}

// LocalBucket applies bounded, cancellation-aware RPC admission only to contexts
// issued by the Local gate. Ordinary readers and HA operations are unchanged.
func LocalBucket(bucket objstore.Bucket) objstore.Bucket {
	if _, ok := bucket.(*localBucket); ok {
		return bucket
	}
	return &localBucket{Bucket: bucket}
}

type localBucket struct{ objstore.Bucket }

func localCall[T any](ctx context.Context, mutate bool, work func(context.Context) (T, error)) (T, error) {
	state, _ := ctx.Value(localOperationKey{}).(*localOperation)
	if state == nil {
		value, err := work(ctx)
		var known localAcknowledgedError
		if errors.As(err, &known) {
			err = known.error
		}
		return value, err
	}
	var zero T
	if err := state.request.Err(); err != nil {
		return zero, localBeforeDispatchError{err}
	}
	rpcCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), state.timeout)
	defer cancel()
	type result struct {
		value T
		err   error
	}
	done := make(chan result, 1)
	go func() { value, err := work(rpcCtx); done <- result{value, err} }()
	select {
	case result := <-done:
		var known localAcknowledgedError
		acknowledged := errors.As(result.err, &known)
		if result.err != nil && mutate && !acknowledged {
			state.uncertain.Store(true)
		}
		if acknowledged {
			result.err = known.error
		}
		return result.value, result.err
	case <-rpcCtx.Done():
		if mutate {
			state.uncertain.Store(true)
		}
		return zero, fmt.Errorf("Local storage RPC deadline; outstanding completion unknown: %w", rpcCtx.Err())
	}
}

type localAcknowledgedError struct{ error }
type localBeforeDispatchError struct{ error }

func (e localBeforeDispatchError) Unwrap() error { return e.error }

func LocalBeforeDispatch(err error) bool {
	var before localBeforeDispatchError
	return errors.As(err, &before)
}

func localVoid(ctx context.Context, mutate bool, work func(context.Context) error) error {
	_, err := localCall(ctx, mutate, func(ctx context.Context) (struct{}, error) { return struct{}{}, work(ctx) })
	return err
}
func (b *localBucket) Upload(ctx context.Context, name string, r io.Reader, options ...objstore.ObjectUploadOption) error {
	return localVoid(ctx, true, func(ctx context.Context) error {
		err := b.Bucket.Upload(ctx, name, r, options...)
		// A conditional rejection acknowledges that no mutation occurred.
		if err != nil && b.IsConditionNotMetErr(err) {
			return localAcknowledgedError{err}
		}
		return err
	})
}
func (b *localBucket) Delete(ctx context.Context, name string) error {
	return localVoid(ctx, true, func(ctx context.Context) error {
		err := b.Bucket.Delete(ctx, name)
		if err != nil && b.IsObjNotFoundErr(err) {
			return localAcknowledgedError{err}
		}
		return err
	})
}
func (b *localBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	return localCall(ctx, false, func(ctx context.Context) (objstore.ObjectAttributes, error) { return b.Bucket.Attributes(ctx, name) })
}
func (b *localBucket) Exists(ctx context.Context, name string) (bool, error) {
	return localCall(ctx, false, func(ctx context.Context) (bool, error) { return b.Bucket.Exists(ctx, name) })
}
func (b *localBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	// Iter callbacks may perform further RPCs; run them synchronously to avoid
	// returning while callback mutations are still in flight.
	state, _ := ctx.Value(localOperationKey{}).(*localOperation)
	if state == nil {
		return b.Bucket.Iter(ctx, dir, f, options...)
	}
	var names []string
	err := localVoid(ctx, false, func(ctx context.Context) error {
		return b.Bucket.Iter(ctx, dir, func(name string) error { names = append(names, name); return nil }, options...)
	})
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := state.request.Err(); err != nil {
			return err
		}
		if err := f(name); err != nil {
			return err
		}
	}
	return nil
}
func (b *localBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	state, _ := ctx.Value(localOperationKey{}).(*localOperation)
	if state == nil {
		return b.Bucket.Get(ctx, name)
	}
	// Keep the stream's RPC context live through Close, including body reads.
	if err := state.request.Err(); err != nil {
		return nil, err
	}
	rpcCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), state.timeout)
	type result struct {
		reader io.ReadCloser
		err    error
	}
	done := make(chan result, 1)
	go func() { r, err := b.Bucket.Get(rpcCtx, name); done <- result{r, err} }()
	select {
	case result := <-done:
		if result.err != nil {
			cancel()
			return nil, result.err
		}
		return &localRPCReader{ReadCloser: result.reader, ctx: rpcCtx, cancel: cancel}, nil
	case <-rpcCtx.Done():
		cancel()
		go func() {
			result := <-done
			if result.reader != nil {
				_ = result.reader.Close()
			}
		}()
		return nil, rpcCtx.Err()
	}
}

type localRPCReader struct {
	io.ReadCloser
	ctx    context.Context
	cancel context.CancelFunc
}

func (r *localRPCReader) Read(p []byte) (int, error) {
	type result struct {
		data []byte
		n    int
		err  error
	}
	done := make(chan result, 1)
	go func() { data := make([]byte, len(p)); n, err := r.ReadCloser.Read(data); done <- result{data, n, err} }()
	select {
	case result := <-done:
		copy(p, result.data[:result.n])
		return result.n, result.err
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	}
}
func (r *localRPCReader) Close() error {
	r.cancel()
	done := make(chan error, 1)
	go func() { done <- r.ReadCloser.Close() }()
	timer := time.NewTimer(localRPCTimeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return fmt.Errorf("Local stream close deadline")
	}
}
