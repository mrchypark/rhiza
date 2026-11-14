package managedbootstrap

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/mrchypark/rhiza/pkg/managedsql"
)

// EmbeddedHost owns the managed SQL host and its membership reconciliation loop.
type EmbeddedHost struct {
	Host      *managedsql.Host
	Control   http.Handler
	cancel    context.CancelFunc
	done      chan struct{}
	cleanup   func() error
	closeOnce sync.Once
	closeErr  error
}

// OpenEmbedded validates the bootstrap or repair handoff before opening the host.
// The caller must expose the private control endpoints and combine Host.Ready
// with application readiness before accepting traffic.
func OpenEmbedded(ctx context.Context, req ConfigRequest, applicationReady func(context.Context) error) (*EmbeddedHost, error) {
	if applicationReady == nil {
		return nil, errors.New("application readiness callback required")
	}
	cfg, cleanup, err := LoadConfig(ctx, req)
	if err != nil {
		return nil, err
	}
	host, err := managedsql.Open(ctx, cfg)
	if err != nil {
		if host != nil {
			err = errors.Join(err, host.Close())
		}
		return nil, errors.Join(err, cleanup())
	}
	handler, err := ManagedHandlerWithReadiness(host, cfg.Runtime.AdminToken, applicationReady)
	if err != nil {
		return nil, errors.Join(err, host.Close(), cleanup())
	}
	loopCtx, cancel := context.WithCancel(ctx)
	e := &EmbeddedHost{Host: host, Control: handler, cancel: cancel, done: make(chan struct{}), cleanup: cleanup}
	go func() { defer close(e.done); RunReplacementLoop(loopCtx, host, cfg) }()
	return e, nil
}

// Close stops reconciliation before closing the host and releasing handoff resources.
func (e *EmbeddedHost) Close() error {
	e.closeOnce.Do(func() {
		e.cancel()
		<-e.done
		e.closeErr = errors.Join(e.Host.Close(), e.cleanup())
	})
	return e.closeErr
}
