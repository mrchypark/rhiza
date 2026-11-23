package managedsql

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type causeProbeTransport struct {
	results chan<- struct {
		id    string
		cause error
	}
}

func TestSQLWireConcurrentConditionalPutsRemainAtomic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server, control := sqlWireStore(t)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	control.putReadStarted = func(*http.Request) { entered <- struct{}{}; <-release }
	statuses := make(chan int, 2)
	var writers sync.WaitGroup
	for _, data := range []string{"one", "two"} {
		writers.Add(1)
		go func(data string) {
			defer writers.Done()
			r, err := http.NewRequestWithContext(ctx, http.MethodPut, server.URL+"/bucket/test/contended", strings.NewReader(data))
			if err != nil {
				statuses <- 0
				return
			}
			r.Header.Set("If-None-Match", "*")
			response, err := http.DefaultClient.Do(r)
			if err != nil {
				statuses <- 0
				return
			}
			statuses <- response.StatusCode
			_ = response.Body.Close()
		}(data)
	}
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			close(release)
			writers.Wait()
			t.Fatal("both concurrent requests did not enter body read")
		}
	}
	close(release)
	writers.Wait()
	a, b := <-statuses, <-statuses
	if !((a == http.StatusOK && b == http.StatusPreconditionFailed) || (b == http.StatusOK && a == http.StatusPreconditionFailed)) {
		t.Fatalf("conditional request statuses %d and %d, want one 200 and one 412", a, b)
	}
}

func (t causeProbeTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := http.DefaultTransport.RoundTrip(r)
	t.results <- struct {
		id    string
		cause error
	}{r.Header.Get("X-Test-Request-ID"), context.Cause(r.Context())}
	return response, err
}

// This probe demonstrates causal transport correlation, not Host integration.
func TestRequestCauseCorrelationProbe(t *testing.T) {
	ownerStopped := errors.New("probe owner stopped")
	callerStopped := errors.New("probe caller stopped")
	for _, tc := range []struct {
		name       string
		ownerFirst bool
	}{{"owner cancellation", true}, {"independent caller cancellation", false}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			type observation struct {
				id       string
				err      error
				canceled bool
			}
			serverRead := make(chan struct{})
			serverResult := make(chan observation, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var first [1]byte
				_, err := io.ReadFull(r.Body, first[:])
				if err != nil {
					serverResult <- observation{r.Header.Get("X-Test-Request-ID"), err, false}
					return
				}
				close(serverRead)
				_, err = io.Copy(io.Discard, r.Body)
				serverResult <- observation{r.Header.Get("X-Test-Request-ID"), err, errors.Is(r.Context().Err(), context.Canceled)}
			}))
			defer server.Close()
			owner, stopOwner := context.WithCancelCause(ctx)
			defer stopOwner(nil)
			requestCtx, stopCaller := context.WithCancelCause(owner)
			defer stopCaller(nil)
			body, writer := io.Pipe()
			defer writer.Close()
			go func() {
				<-requestCtx.Done()
				_ = writer.CloseWithError(requestCtx.Err())
			}()
			request, err := http.NewRequestWithContext(requestCtx, http.MethodPut, server.URL+"/checkpoint/block", body)
			if err != nil {
				t.Fatal(err)
			}
			request.ContentLength = 1024
			request.Header.Set("X-Test-Request-ID", "request-1")
			clientDone := make(chan error, 1)
			clientResult := make(chan struct {
				id    string
				cause error
			}, 1)
			transport := causeProbeTransport{results: clientResult}
			go func() {
				response, err := transport.RoundTrip(request)
				if response != nil {
					_ = response.Body.Close()
				}
				clientDone <- err
			}()
			if _, err := writer.Write([]byte("x")); err != nil {
				t.Fatal(err)
			}
			select {
			case <-serverRead:
			case <-ctx.Done():
				t.Fatal("server did not read the first byte")
			}
			if tc.ownerFirst {
				stopOwner(ownerStopped)
			} else {
				stopCaller(callerStopped)
				stopOwner(ownerStopped)
			}
			select {
			case <-clientDone:
			case <-ctx.Done():
				t.Fatal("client did not stop")
			}
			select {
			case got := <-serverResult:
				if got.id != "request-1" || !errors.Is(got.err, io.ErrUnexpectedEOF) || !got.canceled {
					t.Fatalf("unexpected server observation: %+v", got)
				}
			case <-ctx.Done():
				t.Fatal("server did not finish body read")
			}
			want := callerStopped
			if tc.ownerFirst {
				want = ownerStopped
			}
			clientObservation := <-clientResult
			if clientObservation.id != "request-1" || !errors.Is(clientObservation.cause, want) {
				t.Fatalf("client observation: got %+v, want request-1 and %v", clientObservation, want)
			}
		})
	}
}
