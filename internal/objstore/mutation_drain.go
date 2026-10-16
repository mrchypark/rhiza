package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
)

// mutationResponse waits for the terminal response body, not just its headers.
type mutationResponse struct {
	io.ReadCloser
	drain *mutationDrain
	once  sync.Once
	known bool
	body  []byte
}

func (r *mutationResponse) finish(known bool) {
	r.once.Do(func() { r.drain.end(known) })
}

func (r *mutationResponse) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if len(r.body)+n <= 64<<10 {
		r.body = append(r.body, p[:n]...)
	} else {
		r.known = false
	}
	if err == io.EOF {
		// Some stores report a failed mutation inside a successful HTTP status.
		if bytes.Contains(r.body, []byte("<Error")) || bytes.Contains(r.body, []byte(`"error"`)) {
			r.known = false
		}
		r.finish(r.known)
	} else if err != nil {
		r.finish(false)
	}
	return n, err
}

func (r *mutationResponse) Close() error {
	err := r.ReadCloser.Close()
	// Closing without EOF cannot establish the final remote outcome.
	r.finish(false)
	if err != nil {
		r.drain.mu.Lock()
		r.drain.unknown = true
		r.drain.mu.Unlock()
	}
	return err
}

var ErrMutationsStopped = errors.New("object-store mutations stopped for terminal drain")
var ErrMutationOutcomeUnknown = errors.New("object-store mutation outcome remains unknown")

// mutationDrain covers the entire bucket lifetime. Unknown attempts are sticky:
// a later retry cannot prove that an earlier remote request has stopped executing.
type mutationDrain struct {
	mu      sync.Mutex
	active  int
	unknown bool
	stopped bool
	changed chan struct{}
}

func (d *mutationDrain) begin() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped {
		return ErrMutationsStopped
	}
	d.active++
	return nil
}

func (d *mutationDrain) end(known bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.active--
	d.unknown = d.unknown || !known
	if d.changed != nil {
		close(d.changed)
		d.changed = nil
	}
}

// DrainMutations permanently stops new bucket mutations and joins admitted
// attempts. It proves known completion only within this live bucket incarnation;
// a supervisor must never reconstruct a drain receipt after a process crash.
func (b *MeteredBucket) DrainMutations(ctx context.Context) error {
	d := &b.metrics.mutations
	d.mu.Lock()
	d.stopped = true
	for d.active != 0 {
		if d.changed == nil {
			d.changed = make(chan struct{})
		}
		changed := d.changed
		d.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
		d.mu.Lock()
	}
	unknown := d.unknown
	d.mu.Unlock()
	if unknown {
		return ErrMutationOutcomeUnknown
	}
	return ctx.Err()
}
