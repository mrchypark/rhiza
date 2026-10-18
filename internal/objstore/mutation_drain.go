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
	drain    *mutationDrain
	once     sync.Once
	known    bool
	body     []byte
	expected int64
	received int64
}

func (r *mutationResponse) finish(known bool) {
	if !known {
		r.drain.mu.Lock()
		r.drain.unknown = true
		r.drain.mu.Unlock()
	}
	r.once.Do(func() { r.drain.end(known) })
}

func (r *mutationResponse) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.received += int64(n)
	if r.expected > 0 && r.received > r.expected {
		r.known = false
	}
	if len(r.body)+n <= 64<<10 {
		r.body = append(r.body, p[:n]...)
	} else {
		r.known = false
	}
	if err != nil && err != io.EOF {
		r.finish(false)
	} else if err == io.EOF || r.expected > 0 && r.received == r.expected {
		if r.expected > 0 && r.received != r.expected {
			r.known = false
		}
		// Some stores report a failed mutation inside a successful HTTP status.
		if bytes.Contains(r.body, []byte("<Error")) || bytes.Contains(r.body, []byte(`"error"`)) {
			r.known = false
		}
		r.finish(r.known)
	}
	return n, err
}

func (r *mutationResponse) Close() error {
	err := r.ReadCloser.Close()
	// A fully consumed Content-Length is terminal even if a decoder did not
	// request EOF. Unknown-length bodies still require EOF.
	r.once.Do(func() { r.drain.end(false) })
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
