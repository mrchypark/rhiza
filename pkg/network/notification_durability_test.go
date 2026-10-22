package network

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

type notificationStreamWriter struct {
	mu      sync.Mutex
	header  http.Header
	body    bytes.Buffer
	flushed chan struct{}
}

func newNotificationStreamWriter() *notificationStreamWriter {
	return &notificationStreamWriter{header: make(http.Header), flushed: make(chan struct{}, 8)}
}

func (w *notificationStreamWriter) Header() http.Header { return w.header }
func (w *notificationStreamWriter) WriteHeader(int)     {}
func (w *notificationStreamWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.Write(p)
}
func (w *notificationStreamWriter) Flush() {
	select {
	case w.flushed <- struct{}{}:
	default:
	}
}
func (w *notificationStreamWriter) bodyString() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.String()
}

func notificationTestServer(t *testing.T) *Server {
	t.Helper()
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/notify.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	server := NewServer(core, material, "cluster", true, nil)
	t.Cleanup(func() { _ = server.Close() })
	return server
}

func publishNotification(t *testing.T, server *Server) types.MutationReceipt {
	t.Helper()
	receipt, err := server.NotifyPublish(context.Background(), types.NotifyCommand{
		RequestID: "durable-notification", Topic: "durable-topic", Payload: []byte("trusted-payload"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func TestNotifySubscribeWaitsForAppliedSlotDurabilityAndCancelStopsRelay(t *testing.T) {
	server := notificationTestServer(t)
	entered := make(chan quepaxa.Slot, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	server.SetReadDurabilityBarrier(func(_ context.Context, slot quepaxa.Slot) error {
		entered <- slot
		<-release
		return nil
	})

	ch, cancel, err := server.NotifySubscribe("durable-topic")
	if err != nil {
		t.Fatal(err)
	}
	receipt := publishNotification(t, server)
	select {
	case slot := <-entered:
		if slot != quepaxa.Slot(receipt.Slot) {
			t.Fatalf("barrier slot=%d, notification receipt slot=%d", slot, receipt.Slot)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-process notification did not reach read durability barrier")
	}
	select {
	case payload := <-ch:
		t.Fatalf("payload %q escaped before durability release", payload)
	default:
	}

	releaseOnce.Do(func() { close(release) })
	select {
	case payload := <-ch:
		if !bytes.Equal(payload, []byte("trusted-payload")) {
			t.Fatalf("in-process payload=%q", payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-process notification was not delivered after durability")
	}
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("relay emitted notification after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not stop notification relay")
	}
}

func TestNotifySubscribeCancelUnblocksDurabilityWait(t *testing.T) {
	server := notificationTestServer(t)
	entered := make(chan struct{}, 1)
	server.SetReadDurabilityBarrier(func(ctx context.Context, _ quepaxa.Slot) error {
		entered <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})
	ch, cancel, err := server.NotifySubscribe("durable-topic")
	if err != nil {
		t.Fatal(err)
	}
	publishNotification(t, server)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("in-process notification did not reach durability barrier")
	}
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("payload escaped while canceled durability wait was pending")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not unblock in-process durability wait")
	}
}

func TestNotifySubscribeServerShutdownJoinsRelay(t *testing.T) {
	server := notificationTestServer(t)
	entered := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	server.SetReadDurabilityBarrier(func(ctx context.Context, _ quepaxa.Slot) error {
		entered <- struct{}{}
		<-ctx.Done()
		canceled <- struct{}{}
		<-release
		return ctx.Err()
	})
	ch, _, err := server.NotifySubscribe("durable-topic")
	if err != nil {
		t.Fatal(err)
	}
	publishNotification(t, server)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("notification did not reach durability wait")
	}

	closed := make(chan struct{})
	go func() {
		server.Close()
		close(closed)
	}()
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("server shutdown did not cancel relay durability context")
	}
	select {
	case <-closed:
		t.Fatal("server close returned before the registered relay exited")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("server close did not join subscription relay")
	}
	select {
	case payload, ok := <-ch:
		if ok {
			t.Fatalf("payload %q remained readable after server shutdown", payload)
		}
	default:
		t.Fatal("subscription channel was not closed before Server.Close returned")
	}
}

func TestNotifySSEWaitsForAppliedSlotDurabilityAndPreservesPayload(t *testing.T) {
	server := notificationTestServer(t)
	entered := make(chan quepaxa.Slot, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseBarrier := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseBarrier()
	server.SetReadDurabilityBarrier(func(_ context.Context, slot quepaxa.Slot) error {
		entered <- slot
		<-release
		return nil
	})

	w := newNotificationStreamWriter()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := httptest.NewRequest(http.MethodGet, "/notify/subscribe?topic=durable-topic", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		server.ServeHTTP(w, request)
		close(done)
	}()
	select {
	case <-w.flushed: // Initial comment; no event payload has been sent.
	case <-time.After(5 * time.Second):
		t.Fatal("SSE handler did not start")
	}
	receipt := publishNotification(t, server)
	select {
	case slot := <-entered:
		if slot != quepaxa.Slot(receipt.Slot) {
			t.Fatalf("barrier slot=%d, notification receipt slot=%d", slot, receipt.Slot)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SSE notification did not reach read durability barrier")
	}
	if body := w.bodyString(); strings.Contains(body, "data:") || strings.Contains(body, "trusted-payload") {
		t.Fatalf("SSE leaked notification before durability: %q", body)
	}

	releaseBarrier()
	select {
	case <-w.flushed:
	case <-time.After(5 * time.Second):
		t.Fatal("SSE event was not flushed after durability")
	}
	body := w.bodyString()
	if !strings.Contains(body, `data: "trusted-payload"`) || strings.Contains(body, "applied_slot") {
		t.Fatalf("SSE body=%q; want unchanged JSON payload without internal slot metadata", body)
	}

	if !strings.Contains(body, ": connected") {
		t.Fatalf("SSE startup comment missing: %q", body)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("SSE stream did not stop after request cancellation")
	}
}

func TestNotifySSETerminatesWithoutPayloadWhenDurabilityFails(t *testing.T) {
	server := notificationTestServer(t)
	entered := make(chan struct{}, 1)
	server.SetReadDurabilityBarrier(func(context.Context, quepaxa.Slot) error {
		entered <- struct{}{}
		return recovery.ErrArchiveSealed
	})

	w := newNotificationStreamWriter()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := httptest.NewRequest(http.MethodGet, "/notify/subscribe?topic=durable-topic", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		server.ServeHTTP(w, request)
		close(done)
	}()
	select {
	case <-w.flushed:
	case <-time.After(5 * time.Second):
		t.Fatal("SSE handler did not start")
	}
	publishNotification(t, server)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("SSE event did not reach failed durability barrier")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("SSE stream did not terminate after durability error")
	}
	if body := w.bodyString(); strings.Contains(body, "data:") || strings.Contains(body, "trusted-payload") {
		t.Fatalf("SSE leaked failed event payload: %q", body)
	}
}
