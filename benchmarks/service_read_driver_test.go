package benchmarks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/client"
)

// TestServiceReadDriver runs only when the disposable service fixture supplies
// both actual Rhiza HTTP endpoints. It never sends a mutation.
func TestServiceReadDriver(t *testing.T) {
	primary, secondary := os.Getenv("RHIZA_READ_PRIMARY"), os.Getenv("RHIZA_READ_SECONDARY")
	if primary == "" && secondary == "" {
		t.Skip("disposable service fixture not configured")
	}
	if primary == "" || secondary == "" || primary == secondary {
		t.Fatal("two distinct service endpoints required")
	}
	mode, scenario, output := os.Getenv("RHIZA_READ_MODE"), os.Getenv("RHIZA_READ_SCENARIO"), os.Getenv("RHIZA_READ_OUTPUT")
	if (mode != "enabled" && mode != "disabled") || output == "" {
		t.Fatal("invalid mode or output")
	}
	n, err := strconv.Atoi(os.Getenv("RHIZA_READ_REQUESTS"))
	if err != nil || n < 100 {
		t.Fatal("at least 100 read requests required")
	}
	var injected, alternate, faultAt atomic.Int64
	measurementStart := time.Now()
	faultDir := os.Getenv("RHIZA_READ_FAULT_DIR")
	if scenario == "slow-ingress" || scenario == "dead-ingress" {
		if scenario == "dead-ingress" && faultDir == "" {
			t.Fatal("dead ingress needs an owned fault directory")
		}
		u, err := url.Parse(primary)
		if err != nil {
			t.Fatal(err)
		}
		proxy := httputil.NewSingleHostReverseProxy(u)
		if scenario == "dead-ingress" {
			proxy.ModifyResponse = func(resp *http.Response) error {
				if resp.StatusCode != http.StatusOK {
					return fmt.Errorf("upstream read status %d", resp.StatusCode)
				}
				if injected.CompareAndSwap(0, 1) {
					if err := os.WriteFile(filepath.Join(faultDir, "response-held"), []byte("1"), 0600); err != nil {
						return err
					}
					for {
						if _, err := os.Stat(filepath.Join(faultDir, "released")); err == nil {
							faultAt.Store(time.Since(measurementStart).Nanoseconds())
							return errors.New("held upstream response dropped after ingress termination")
						}
						select {
						case <-time.After(5 * time.Millisecond):
						case <-resp.Request.Context().Done():
							return resp.Request.Context().Err()
						}
					}
				}
				return nil
			}
		}
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if scenario == "slow-ingress" && injected.CompareAndSwap(0, 1) {
				faultAt.Store(time.Since(measurementStart).Nanoseconds())
				select {
				case <-time.After(300 * time.Millisecond):
				case <-r.Context().Done():
					return
				}
			}
			proxy.ServeHTTP(w, r)
		}))
		defer s.Close()
		primary = s.URL
	} else if scenario != "healthy" {
		t.Fatalf("unsupported scenario %q", scenario)
	}
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	observed := &countingTransport{base: transport, alternate: secondary, calls: &alternate}
	endpoints := []string{primary}
	if mode == "enabled" {
		endpoints = append(endpoints, secondary)
	}
	timeout := 100 * time.Millisecond
	if scenario == "dead-ingress" {
		timeout = 2 * time.Second
	}
	c, err := client.New(endpoints, &http.Client{Transport: observed}, timeout)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"sql":"SELECT id FROM benchmark_writes","consistency":"linearizable"}`)
	latencies := make([]float64, 0, n)
	successes, failures := 0, 0
	start := time.Now()
	var firstLatency, firstSuccessAfterFault float64
	firstSuccessObserved := false
	for i := 0; i < n; i++ {
		began := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		response, e := c.Read(ctx, "/sql/query", body)
		callerErr := ctx.Err()
		cancel()
		elapsed := float64(time.Since(began)) / float64(time.Millisecond)
		if i == 0 {
			firstLatency = elapsed
		}
		latencies = append(latencies, elapsed)
		if e != nil || response.StatusCode != http.StatusOK {
			failures++
			expectedSlow := scenario == "slow-ingress" && mode == "disabled" && i == 0 && callerErr == nil && errors.Is(e, context.DeadlineExceeded)
			expectedDead := scenario == "dead-ingress" && mode == "disabled" && callerErr == nil && isExpectedDeadIngress(e, response.StatusCode)
			if !expectedSlow && !expectedDead {
				t.Fatalf("unexpected failure at %d: status=%d err=%v", i, response.StatusCode, e)
			}
			continue
		}
		var result struct {
			Rows [][]json.Number `json:"rows"`
		}
		if err := json.Unmarshal(response.Body, &result); err != nil || len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != "42" {
			t.Fatalf("incorrect read: body=%s err=%v", response.Body, err)
		}
		successes++
		if faultAt.Load() > 0 && !firstSuccessObserved {
			firstSuccessAfterFault = float64(time.Since(measurementStart).Nanoseconds()-faultAt.Load()) / float64(time.Millisecond)
			firstSuccessObserved = true
		}
	}
	if scenario == "slow-ingress" && (injected.Load() != 1 || (mode == "enabled" && alternate.Load() == 0) || (mode == "disabled" && failures != 1)) {
		t.Fatal("fault or alternate witness missing")
	}
	if failures != 0 && !(scenario == "slow-ingress" && mode == "disabled" && failures == 1) && !(scenario == "dead-ingress" && mode == "disabled" && failures == n) {
		t.Fatal("unexpected errors")
	}
	if scenario == "dead-ingress" && (injected.Load() != 1 || faultAt.Load() == 0 || (mode == "enabled" && (failures != 0 || alternate.Load() == 0))) {
		t.Fatal("dead ingress was not bypassed")
	}
	sort.Float64s(latencies)
	throughput := float64(successes) / time.Since(start).Seconds()
	stickyWrite := mode == "enabled" && scenario != "healthy"
	if stickyWrite {
		verifyStickyWrite(t, c, &alternate, os.Getenv("RHIZA_READ_THIRD"))
	}
	verifyUnknownMutation(t, secondary, os.Getenv("RHIZA_READ_THIRD"), stickyWrite)
	result := map[string]any{"mode": mode, "scenario": scenario, "successes": successes, "errors": failures, "p99_ms": latencies[(len(latencies)*99-1)/100], "first_request_ms": firstLatency, "first_success_after_fault_ms": nil, "throughput": throughput, "fault_request_observed": injected.Load() == 1, "fault_release_observed": faultAt.Load() > 0, "alternate_calls": alternate.Load(), "sticky_write_verified": stickyWrite, "unknown_mutation_verified": true, "correctness_verified": true}
	if scenario == "dead-ingress" {
		result["fault_stage"] = "upstream_response_received_then_held"
	}
	if firstSuccessObserved {
		result["first_success_after_fault_ms"] = firstSuccessAfterFault
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(output, encoded, 0600); err != nil {
		t.Fatal(err)
	}
}

func isExpectedDeadIngress(err error, status int) bool {
	return errors.Is(err, client.ErrReadUnavailable) && status == http.StatusBadGateway
}

func TestExpectedDeadIngressContract(t *testing.T) {
	if !isExpectedDeadIngress(client.ErrReadUnavailable, http.StatusBadGateway) {
		t.Fatal("unavailable response with 502 must be recognized")
	}
	if isExpectedDeadIngress(nil, http.StatusBadGateway) || isExpectedDeadIngress(client.ErrReadUnavailable, http.StatusInternalServerError) || isExpectedDeadIngress(errors.New("other"), http.StatusBadGateway) {
		t.Fatal("unrelated response or error must not be recognized")
	}
}

func verifyStickyWrite(t *testing.T, c *client.Client, alternateCalls *atomic.Int64, observer string) {
	t.Helper()
	if observer == "" {
		t.Fatal("independent observer endpoint required")
	}
	before := alternateCalls.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	response, err := c.Mutate(ctx, "/sql/execute", []byte(`{"request_id":"sticky-write-probe","sql":"INSERT INTO benchmark_writes(id) VALUES (77)"}`))
	cancel()
	if err != nil || response.StatusCode != http.StatusOK || alternateCalls.Load() != before+1 {
		t.Fatalf("post-failover mutation: status=%d err=%v alternate before=%d after=%d", response.StatusCode, err, before, alternateCalls.Load())
	}
	verifyRows(t, observer, "42", "77")
}

func verifyRows(t *testing.T, endpoint string, expected ...string) {
	t.Helper()
	reader, err := client.New([]string{endpoint}, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	response, err := reader.Read(ctx, "/sql/query", []byte(`{"sql":"SELECT id FROM benchmark_writes ORDER BY id","consistency":"linearizable"}`))
	cancel()
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("verification query: %v status=%d", err, response.StatusCode)
	}
	var rows struct {
		Rows [][]json.Number `json:"rows"`
	}
	if err = json.Unmarshal(response.Body, &rows); err != nil || len(rows.Rows) != len(expected) {
		t.Fatalf("verification result=%s err=%v", response.Body, err)
	}
	for i, want := range expected {
		if len(rows.Rows[i]) != 1 || rows.Rows[i][0] != json.Number(want) {
			t.Fatalf("unexpected verification result=%s", response.Body)
		}
	}
}

// A proxy withholds an actual successful mutation response. The client must
// return unknown without sending the mutation to another voter.
func verifyUnknownMutation(t *testing.T, backend, alternate string, stickyWrite bool) {
	t.Helper()
	if alternate == "" || alternate == backend {
		t.Fatal("distinct alternate endpoint required")
	}
	u, err := url.Parse(backend)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	committed := make(chan int, 1)
	proxy.ModifyResponse = func(resp *http.Response) error {
		committed <- resp.StatusCode
		<-resp.Request.Context().Done()
		return errors.New("response withheld after upstream result")
	}
	s := httptest.NewServer(proxy)
	defer s.Close()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	var alternateCalls atomic.Int64
	c, err := client.New([]string{s.URL, alternate}, &http.Client{Transport: &countingTransport{base: transport, alternate: alternate, calls: &alternateCalls}}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err = c.Mutate(ctx, "/sql/execute", []byte(`{"request_id":"unknown-probe","sql":"UPDATE benchmark_writes SET id=id+1"}`))
	cancel()
	if !errors.Is(err, client.ErrOutcomeUnknown) || alternateCalls.Load() != 0 {
		t.Fatalf("mutation was not kept unknown: err=%v alternate=%d", err, alternateCalls.Load())
	}
	select {
	case status := <-committed:
		if status != http.StatusOK {
			t.Fatalf("upstream mutation status=%d", status)
		}
	default:
		t.Fatal("no upstream mutation response witness")
	}
	if stickyWrite {
		verifyRows(t, alternate, "43", "78")
	} else {
		verifyRows(t, alternate, "43")
	}
}

type countingTransport struct {
	base      http.RoundTripper
	alternate string
	calls     *atomic.Int64
}

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if fmt.Sprintf("%s://%s", r.URL.Scheme, r.URL.Host) == c.alternate {
		c.calls.Add(1)
	}
	return c.base.RoundTrip(r)
}
