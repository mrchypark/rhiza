//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
)

type interruptibleTraceBody struct {
	io.ReadCloser
	ctx     context.Context
	release <-chan struct{}
	sent    bool
}

func (b *interruptibleTraceBody) Read(p []byte) (int, error) {
	if !b.sent {
		b.sent = true
		return b.ReadCloser.Read(p)
	}
	select {
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	case <-b.release:
		return 0, io.ErrUnexpectedEOF
	}
}

func TestActualHostCheckpointCloseCauseTrace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	var selectedFailures atomic.Int32
	t.Cleanup(func() {
		if got := selectedFailures.Load(); got != 1 {
			t.Errorf("observed %d selected PUT failures, want exactly one", got)
		}
	})
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	type clientResult struct {
		id    string
		cause error
	}
	type serverResult struct {
		id       string
		err      error
		canceled bool
	}
	clientResults := make(chan clientResult, 1)
	serverStarted := make(chan string, 1)
	serverResults := make(chan serverResult, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseUpload := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseUpload()
	var armed atomic.Bool
	const traceID = "fixture-checkpoint-request"
	fixture.source.putReadStarted = func(r *http.Request) {
		if r.Header.Get("X-Rhiza-Test-ID") == traceID {
			select {
			case serverStarted <- traceID:
			default:
				t.Error("duplicate selected PUT start")
			}
		}
	}
	fixture.source.putReadFailure = func(r *http.Request, err error) {
		id := r.Header.Get("X-Rhiza-Test-ID")
		if id != traceID {
			t.Errorf("unexpected unselected PUT read error: %v", err)
			return
		}
		if selectedFailures.Add(1) != 1 {
			t.Errorf("duplicate selected PUT read error: %v", err)
			return
		}
		select {
		case serverResults <- serverResult{id, err, errors.Is(r.Context().Err(), context.Canceled)}:
		default:
			t.Error("selected PUT failure channel full")
		}
	}
	unregister, err := objectstore.RegisterS3RequestTrace(configs[0].Runtime.ObjStoreEndpoint, func(r *http.Request, next http.RoundTripper) (*http.Response, error) {
		if r.Method != http.MethodPut || !strings.Contains(r.URL.Path, "/checkpoint/blocks/") || !strings.Contains(r.Header.Get("Authorization"), "Credential=fixture-pod-0/") || !armed.CompareAndSwap(true, false) {
			return next.RoundTrip(r)
		}
		r.Header.Set("X-Rhiza-Test-ID", traceID)
		r.Body = &interruptibleTraceBody{ReadCloser: r.Body, ctx: r.Context(), release: release}
		response, err := next.RoundTrip(r)
		select {
		case clientResults <- clientResult{traceID, context.Cause(r.Context())}:
		default:
			t.Error("selected client result channel full")
		}
		return response, err
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(unregister)
	hosts := make([]*Host, len(configs))
	for i, cfg := range configs {
		cfg.Runtime.CheckpointInterval = 50 * time.Millisecond
		cfg.Runtime.ObjStoreAccessKey = fmt.Sprintf("fixture-pod-%d", i)
		host, err := fixture.open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = host
		t.Cleanup(func() {
			releaseUpload()
			cancel()
			done := make(chan error, 1)
			go func() { done <- host.Close() }()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("cleanup Host close: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("cleanup Host Close did not complete")
			}
		})
	}
	var closeDone []<-chan error
	t.Cleanup(func() {
		releaseUpload()
		cancel()
		for _, done := range closeDone {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("Host Close goroutine failed to exit")
			}
		}
	})
	closeAsync := func(host *Host) <-chan error {
		done := make(chan error, 1)
		closeDone = append(closeDone, done)
		go func() { defer close(done); done <- host.Close() }()
		return done
	}
	for _, host := range hosts {
		awaitPublicInternalReady(t, ctx, host)
	}
	armed.Store(true)
	if _, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "trace-checkpoint", SQL: "CREATE TABLE trace_checkpoint(x INTEGER)"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-serverStarted:
	case <-ctx.Done():
		t.Fatal("selected checkpoint never reached server")
	}
	select {
	case err := <-closeAsync(hosts[1]):
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("other Host Close did not complete")
	}
	select {
	case observed := <-clientResults:
		t.Fatalf("closing another Host canceled the selected upload: %+v", observed)
	default:
	}
	closed := closeAsync(hosts[0])
	select {
	case observed := <-serverResults:
		if observed.id != traceID || !errors.Is(observed.err, io.ErrUnexpectedEOF) || !observed.canceled {
			t.Fatalf("unexpected server failure %+v", observed)
		}
	case <-ctx.Done():
		t.Fatal("server did not observe partial checkpoint EOF")
	}
	select {
	case observed := <-clientResults:
		if observed.id != traceID || !errors.Is(observed.cause, localtesthooks.ErrLifecycleStopped) {
			t.Fatalf("unexpected client cause id=%s cause=%v is_context_canceled=%t is_lifecycle=%t", observed.id, observed.cause, errors.Is(observed.cause, context.Canceled), errors.Is(observed.cause, localtesthooks.ErrLifecycleStopped))
		}
	case <-ctx.Done():
		t.Fatal("client did not report cancellation cause")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Host close did not complete")
	}
}
