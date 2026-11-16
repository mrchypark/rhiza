//go:build rhiza_local_testhooks

package localtesthooks

import (
	"context"
	"sync"
)

var graphApplyContext struct {
	sync.RWMutex
	hook func(context.Context) context.Context
}

type graphApplyNodeKey struct{}

func WithGraphApplyNode(ctx context.Context, node string) context.Context {
	return context.WithValue(ctx, graphApplyNodeKey{}, node)
}

func GraphApplyNode(ctx context.Context) string {
	node, _ := ctx.Value(graphApplyNodeKey{}).(string)
	return node
}

func SetGraphApplyContext(hook func(context.Context) context.Context) func() {
	graphApplyContext.Lock()
	previous := graphApplyContext.hook
	graphApplyContext.hook = hook
	graphApplyContext.Unlock()
	return func() {
		graphApplyContext.Lock()
		graphApplyContext.hook = previous
		graphApplyContext.Unlock()
	}
}

func GraphApplyContext(ctx context.Context) context.Context {
	graphApplyContext.RLock()
	hook := graphApplyContext.hook
	graphApplyContext.RUnlock()
	if hook != nil {
		return hook(ctx)
	}
	return ctx
}
