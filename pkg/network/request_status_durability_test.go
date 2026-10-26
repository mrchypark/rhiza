package network

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func committedStatusFixture(t *testing.T) (*Server, ExecuteResponse, RequestStatusRequest) {
	t.Helper()
	server := readGateServer(t)
	write, err := server.Execute(context.Background(), ExecuteRequest{RequestID: "status-durable", SQL: "CREATE TABLE status_durable (id INTEGER)"})
	if err != nil {
		t.Fatal(err)
	}
	return server, write, RequestStatusRequest{Kind: "sql", RequestID: "status-durable"}
}

func TestRequestStatusRechecksCommittedReceiptDurability(t *testing.T) {
	server, write, req := committedStatusFixture(t)
	var got quepaxa.Slot
	server.SetDurabilityBarrier(func(_ context.Context, slot quepaxa.Slot) error {
		got = slot
		return recovery.ErrArchiveSealed
	})
	response, err := server.RequestStatus(context.Background(), req)
	if !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, recovery.ErrArchiveSealed) {
		t.Fatalf("response=%+v err=%v; want sealed receipt durability failure", response, err)
	}
	if response.Receipt != nil {
		t.Fatalf("sealed receipt leaked to caller: %+v", response)
	}
	if got != quepaxa.Slot(write.Slot) {
		t.Fatalf("durability checked slot %d, want exact receipt slot %d", got, write.Slot)
	}
}

func TestRequestStatusWaitsForReceiptBarrierBeforeReturning(t *testing.T) {
	server, write, req := committedStatusFixture(t)
	entered := make(chan quepaxa.Slot, 1)
	release := make(chan struct{})
	server.SetDurabilityBarrier(func(_ context.Context, slot quepaxa.Slot) error {
		entered <- slot
		<-release
		return nil
	})
	type result struct {
		response RequestStatusResponse
		err      error
	}
	done := make(chan result, 1)
	go func() {
		response, err := server.RequestStatus(context.Background(), req)
		done <- result{response: response, err: err}
	}()
	select {
	case slot := <-entered:
		if slot != quepaxa.Slot(write.Slot) {
			close(release)
			t.Fatalf("barrier checked slot %d, want exact receipt slot %d", slot, write.Slot)
		}
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("request status did not reach receipt durability barrier")
	}
	select {
	case got := <-done:
		close(release)
		t.Fatalf("status returned before durability completed: %+v, %v", got.response, got.err)
	default:
	}
	close(release)
	select {
	case got := <-done:
		if got.err != nil || got.response.Receipt == nil || got.response.Receipt.Slot != write.Slot {
			t.Fatalf("status=%+v err=%v; want durable receipt", got.response, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("request status did not return after durability completed")
	}
}

func TestRequestStatusUnknownChecksManagedReadGate(t *testing.T) {
	server := readGateServer(t)
	want := errors.New("archive epoch sealed")
	var got quepaxa.Slot = 99
	server.SetReadDurabilityBarrier(func(_ context.Context, slot quepaxa.Slot) error {
		got = slot
		return want
	})
	response, err := server.RequestStatus(context.Background(), RequestStatusRequest{Kind: "sql", RequestID: "missing-status"})
	if !errors.Is(err, ErrDurabilityUnavailable) || !errors.Is(err, want) {
		t.Fatalf("response=%+v err=%v; want managed read-gate failure", response, err)
	}
	if response.Receipt != nil || response.State != "" {
		t.Fatalf("failed read-gate status leaked a result: %+v", response)
	}
	if got != quepaxa.Slot(server.material.Tip()) {
		t.Fatalf("read gate checked slot %d, want current tip %d", got, server.material.Tip())
	}
}

func TestRequestStatusSamplesTipAfterReceiptLookup(t *testing.T) {
	server, _, req := committedStatusFixture(t)
	var advanced ExecuteResponse
	server.statusLookupHook = func() {
		var err error
		advanced, err = server.Execute(context.Background(), ExecuteRequest{RequestID: "status-tip-advance", SQL: "INSERT INTO status_durable VALUES (1)"})
		if err != nil {
			t.Errorf("advance materializer between receipt lookup and tip sample: %v", err)
		}
	}
	response, err := server.RequestStatus(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if response.Receipt == nil || response.Receipt.Slot > response.Tip {
		t.Fatalf("status receipt slot must be covered by response tip: %+v", response)
	}
	if response.Tip < advanced.Slot {
		t.Fatalf("response tip %d predates apply at slot %d between lookup and tip sample", response.Tip, advanced.Slot)
	}
}

func TestHTTPRequestStatusDoesNotReturnBeforeDurability(t *testing.T) {
	server, write, _ := committedStatusFixture(t)
	entered := make(chan quepaxa.Slot, 1)
	release := make(chan struct{})
	server.SetDurabilityBarrier(func(_ context.Context, slot quepaxa.Slot) error {
		entered <- slot
		<-release
		return nil
	})
	body := bytes.NewBufferString(`{"kind":"sql","request_id":"status-durable"}`)
	request := httptest.NewRequest(http.MethodPost, "/request/status", body)
	writer := newReadGateWriter()
	done := make(chan struct{})
	go func() {
		server.ServeHTTP(writer, request)
		close(done)
	}()
	select {
	case slot := <-entered:
		if slot != quepaxa.Slot(write.Slot) {
			close(release)
			t.Fatalf("barrier checked slot %d, want receipt slot %d", slot, write.Slot)
		}
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("HTTP status did not reach durability barrier")
	}
	select {
	case <-writer.wrote:
		close(release)
		t.Fatal("HTTP status response began before durability completed")
	default:
	}
	close(release)
	select {
	case <-done:
		if writer.status != http.StatusOK || !bytes.Contains(writer.body.Bytes(), []byte(`"receipt"`)) {
			t.Fatalf("HTTP status=%d body=%s; want committed receipt after barrier", writer.status, writer.body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP status did not finish after durability completed")
	}
}
