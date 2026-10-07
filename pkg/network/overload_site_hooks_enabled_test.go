//go:build rhiza_local_testhooks

package network

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// Fixed, allocation-free overload site names. Every rejecting boundary named by
// the issue-185 overload diagnostic emits exactly one of these literals via
// localtesthooks.Hit. They carry no payload: no request ID, key, payload,
// owner, or provider metadata, and they are never formatted on the hot path.
const (
	overloadSiteMutationAdmission    = "network:mutation-admission:rejected"
	overloadSiteQueueReservation     = "network:mutation-queue:reservation-rejected"
	overloadSiteInputChannelFull     = "network:mutation-queue:input-channel-full"
	overloadSiteWorkerInflightBytes  = "network:mutation-queue:worker-inflight-bytes-rejected"
	overloadSiteProposalByteBudget   = "network:proposal-admission:byte-budget-rejected"
	overloadSiteProposalLocalCap     = "network:proposal-admission:local-cap-rejected"
	overloadSiteProposalOperationCap = "network:proposal-admission:operation-cap-rejected"
	overloadSiteUnknown              = "site_unknown"
	overloadSiteEventLimit           = 16
	overloadSiteFirstEventLimit      = 8
)

// overloadSiteRecorder counts fixed overload site names. It is bounded and
// nonblocking on the mutation path: the recorder lock is taken with TryLock and
// a contended observation is dropped with an explicit site_unknown so a
// missing observation stays explicit rather than silently absent. It never
// allocates a formatted payload.
type overloadSiteRecorder struct {
	mu       sync.Mutex
	counts   [overloadSiteEventLimit]overloadSiteCounter
	count    int
	total    uint64
	dropped  atomic.Uint64
	first    [overloadSiteFirstEventLimit]overloadSiteFirstEvent
	firstLen int
}

type overloadSiteCounter struct {
	site  string
	count uint64
}

// overloadSiteFirstEvent retains the first observation of one site by value.
type overloadSiteFirstEvent struct {
	Site  string
	Count uint64
}

// overloadSiteSummary is an aggregate count of observed site events. It is
// deliberately NOT a per-request attribution: the global hook carries no
// request, batch, or context identity, so one site event may fail several
// queued requests and one request may pass several aggregate boundaries.
type overloadSiteSummary struct {
	counts  []overloadSiteCounter
	first   []overloadSiteFirstEvent
	total   uint64
	dropped uint64
}

// observe counts one fixed overload site name. An unrecognised name is still
// counted as an explicit site_unknown rather than being dropped silently.
func (c *overloadSiteRecorder) observe(site string) {
	if !isKnownOverloadSite(site) {
		site = overloadSiteUnknown
	}
	if !c.mu.TryLock() {
		c.dropped.Add(1)
		return
	}
	c.total++
	for i := 0; i < c.count; i++ {
		if c.counts[i].site == site {
			c.counts[i].count++
			c.mu.Unlock()
			return
		}
	}
	if c.count >= overloadSiteEventLimit {
		c.dropped.Add(1)
		c.mu.Unlock()
		return
	}
	c.counts[c.count] = overloadSiteCounter{site: site, count: 1}
	c.count++
	if c.firstLen < overloadSiteFirstEventLimit {
		c.first[c.firstLen] = overloadSiteFirstEvent{Site: site, Count: 1}
		c.firstLen++
	}
	c.mu.Unlock()
}

func (c *overloadSiteRecorder) countOf(site string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < c.count; i++ {
		if c.counts[i].site == site {
			return c.counts[i].count
		}
	}
	return 0
}

// snapshot copies the aggregate counts by value under the lock.
func (c *overloadSiteRecorder) snapshot() overloadSiteSummary {
	c.mu.Lock()
	defer c.mu.Unlock()
	return overloadSiteSummary{
		counts:  append([]overloadSiteCounter(nil), c.counts[:c.count]...),
		first:   append([]overloadSiteFirstEvent(nil), c.first[:c.firstLen]...),
		total:   c.total,
		dropped: c.dropped.Load(),
	}
}

func isKnownOverloadSite(site string) bool {
	switch site {
	case overloadSiteMutationAdmission, overloadSiteQueueReservation, overloadSiteInputChannelFull,
		overloadSiteWorkerInflightBytes, overloadSiteProposalByteBudget,
		overloadSiteProposalLocalCap, overloadSiteProposalOperationCap:
		return true
	}
	return false
}

// log emits one aggregate summary line plus a bounded row per observed site.
// The API error count is a separately observed metric and is never joined to a
// site row: absent rows mean "not observed", never "did not occur".
func (s overloadSiteSummary) log(t *testing.T) {
	t.Helper()
	t.Logf("node_foreground_overload_site total_events=%d distinct_sites=%d dropped=%d interpretation=aggregate_site_events_not_per_request_attribution",
		s.total, len(s.counts), s.dropped)
	for _, entry := range s.counts {
		t.Logf("node_foreground_overload_site site=%s events=%d interpretation=candidate_rejection_site", entry.site, entry.count)
	}
	for _, event := range s.first {
		t.Logf("node_foreground_overload_site first_site=%s first_seen_count=%d", event.Site, event.Count)
	}
	if s.dropped > 0 {
		t.Logf("node_foreground_overload_site attribution=%s dropped=%d", overloadSiteUnknown, s.dropped)
	}
}

// installOverloadSiteRecorder installs the process-global test callback and
// restores it at test end. The callback is process-global, so these tests must
// not run in parallel with any other hook user.
func installOverloadSiteRecorder(t *testing.T) *overloadSiteRecorder {
	t.Helper()
	if !localtesthooks.Enabled {
		t.Skip("requires rhiza_local_testhooks build tag")
	}
	recorder := &overloadSiteRecorder{}
	restore := localtesthooks.Set(recorder.observe)
	t.Cleanup(restore)
	return recorder
}

// TestOverloadMutationAdmissionEmitsFixedSite forces the admitted-count cap and
// checks that the fixed site name is observed while the public error contract
// is unchanged.
func TestOverloadMutationAdmissionEmitsFixedSite(t *testing.T) {
	recorder := installOverloadSiteRecorder(t)
	server := NewServer(nil, nil, "cluster", false, nil)
	defer server.Close()
	leases := make([]*mutationLease, 0, maxAdmittedMutations)
	for i := 0; i < maxAdmittedMutations; i++ {
		lease, err := server.admitMutation(1)
		if err != nil {
			t.Fatalf("admission %d: %v", i, err)
		}
		leases = append(leases, lease)
	}
	before := recorder.countOf(overloadSiteMutationAdmission)
	if _, err := server.admitMutation(1); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("count cap error=%v, want ErrOverloaded", err)
	}
	if got := recorder.countOf(overloadSiteMutationAdmission) - before; got != 1 {
		t.Fatalf("mutation admission site events=%d, want 1", got)
	}
	// A different boundary must not be attributed to this site.
	if got := recorder.countOf(overloadSiteQueueReservation); got != 0 {
		t.Fatalf("queue reservation site events=%d, want 0 for an admission-only force", got)
	}
	for _, lease := range leases {
		lease.releaseCaller()
	}
}

// TestOverloadQueueReservationEmitsFixedSite forces the batcher queue
// reservation cap with the run loop stopped, so the reservation is the only
// rejecting boundary.
func TestOverloadQueueReservationEmitsFixedSite(t *testing.T) {
	recorder := installOverloadSiteRecorder(t)
	batcher := newMutationBatcher(
		func(context.Context, []byte) (quepaxa.Slot, error) { return 0, nil },
		func(context.Context, quepaxa.Slot) error { return nil },
		func(KVMutationRequest) ([]byte, error) { return []byte("v"), nil },
		func([][]byte) []byte { return []byte("v") },
		func(KVMutationRequest) string { return "request" },
	)
	// Fill the queue reservation budget while the run loop cannot drain it.
	batcher.cancel()
	waitBatcherStop(t, batcher)
	batcher.budgetMu.Lock()
	batcher.queuedN = maxQueuedRequests
	batcher.queuedByte = maxQueuedEncodedBytes
	batcher.budgetMu.Unlock()
	before := recorder.countOf(overloadSiteQueueReservation)
	_, err := batcher.submit(context.Background(), KVMutationRequest{RequestID: "r", Key: "k", Value: []byte("v")})
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("queue reservation error=%v, want ErrOverloaded", err)
	}
	if got := recorder.countOf(overloadSiteQueueReservation) - before; got != 1 {
		t.Fatalf("queue reservation site events=%d, want 1", got)
	}
	if got := recorder.countOf(overloadSiteInputChannelFull); got != 0 {
		t.Fatalf("input channel site events=%d, want 0 for a reservation-only force", got)
	}
}

// TestOverloadInputChannelFullEmitsFixedSite forces the input-channel default
// arm with a queue reservation that still succeeds.
func TestOverloadInputChannelFullEmitsFixedSite(t *testing.T) {
	recorder := installOverloadSiteRecorder(t)
	// No consumer is started. A canceled batcher would choose its context-done
	// arm instead of the channel-full default arm, so keep this context live.
	batcher := &mutationBatcher[KVMutationRequest]{
		encodeItem: func(KVMutationRequest) ([]byte, error) { return []byte("v"), nil },
		assemble:   func([][]byte) []byte { return []byte("v") },
		requestID:  func(KVMutationRequest) string { return "request" },
		input:      make(chan *batchItem, maxQueuedRequests),
		ctx:        context.Background(),
	}
	// The reservation budget is free, but the (now unread) input channel is
	// full, so the default arm is the only reachable rejection.
	batcher.budgetMu.Lock()
	batcher.queuedN = 0
	batcher.queuedByte = 0
	batcher.budgetMu.Unlock()
	fillBatcherInput(t, batcher)
	before := recorder.countOf(overloadSiteInputChannelFull)
	_, err := batcher.submit(context.Background(), KVMutationRequest{RequestID: "r", Key: "k", Value: []byte("v")})
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("input channel error=%v, want ErrOverloaded", err)
	}
	if got := recorder.countOf(overloadSiteInputChannelFull) - before; got != 1 {
		t.Fatalf("input channel site events=%d, want 1", got)
	}
	if got := recorder.countOf(overloadSiteQueueReservation); got != 0 {
		t.Fatalf("queue reservation site events=%d, want 0 for an input-only force", got)
	}
}

// TestOverloadWorkerInflightBytesEmitsFixedSite forces the batch worker
// inflight-byte budget branch, which is a distinct boundary from the proposal
// admission byte budget even though both surface the same public error.
func TestOverloadWorkerInflightBytesEmitsFixedSite(t *testing.T) {
	recorder := installOverloadSiteRecorder(t)
	batcher := newMutationBatcher(
		func(context.Context, []byte) (quepaxa.Slot, error) { return 0, nil },
		func(context.Context, quepaxa.Slot) error { return nil },
		func(KVMutationRequest) ([]byte, error) { return []byte("v"), nil },
		func([][]byte) []byte { return []byte("v") },
		func(KVMutationRequest) string { return "request" },
	)
	batcher.cancel()
	waitBatcherStop(t, batcher)
	batcher.budgetMu.Lock()
	// A single byte of headroom forces the next batch to exceed the budget.
	batcher.inflightB = maxInflightEncodedByte
	batcher.budgetMu.Unlock()
	item := &batchItem{
		ctx: context.Background(), requestID: "inflight-bytes",
		encoded: []byte("v"), reserved: 1, result: make(chan batchResult, 1),
	}
	finished := make(chan error, 1)
	go func() {
		select {
		case result := <-item.result:
			finished <- result.err
		case <-time.After(10 * time.Second):
			finished <- errors.New("timed out waiting for the worker budget rejection")
		}
	}()
	before := recorder.countOf(overloadSiteWorkerInflightBytes)
	batcher.dispatch([]*batchItem{item}, [][]byte{item.encoded})
	if err := <-finished; !errors.Is(err, ErrOverloaded) {
		t.Fatalf("worker inflight-byte error=%v, want ErrOverloaded", err)
	}
	if got := recorder.countOf(overloadSiteWorkerInflightBytes) - before; got != 1 {
		t.Fatalf("worker inflight-byte site events=%d, want 1", got)
	}
	if got := recorder.countOf(overloadSiteProposalByteBudget) + recorder.countOf(overloadSiteProposalLocalCap) + recorder.countOf(overloadSiteProposalOperationCap); got != 0 {
		t.Fatalf("proposal admission site events=%d, want 0 for a worker-budget-only force", got)
	}
}

// TestOverloadProposalAdmissionSubtypeEmitsFixedSite forces each of the three
// proposal-admission refusal boundaries and checks that each branch emits only
// its own fixed marker.
func TestOverloadProposalAdmissionSubtypeEmitsFixedSite(t *testing.T) {
	cases := []struct {
		name    string
		site    string
		prepare func(*Server)
	}{
		{
			name: "byte-budget", site: overloadSiteProposalByteBudget,
			prepare: func(s *Server) {
				s.localB = maxInflightEncodedByte
			},
		},
		{
			name: "local-cap", site: overloadSiteProposalLocalCap,
			prepare: func(s *Server) {
				for {
					select {
					case s.localCap <- struct{}{}:
					default:
						return
					}
				}
			},
		},
		{
			name: "operation-cap", site: overloadSiteProposalOperationCap,
			prepare: func(s *Server) {
				for i := 0; i < cap(s.operationCap); i++ {
					select {
					case s.operationCap <- struct{}{}:
					default:
						t.Fatalf("operation cap was not full after %d inserts", i)
					}
				}
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			recorder := installOverloadSiteRecorder(t)
			server := NewServer(nil, nil, "cluster", false, nil)
			defer server.Close()
			testCase.prepare(server)
			_, err := server.propose(context.Background(), []byte("value"))
			if !errors.Is(err, ErrOverloaded) {
				t.Fatalf("proposal admission error=%v, want ErrOverloaded", err)
			}
			if got := recorder.countOf(testCase.site); got != 1 {
				t.Fatalf("proposal admission %s site events=%d, want 1", testCase.name, got)
			}
			// Each proposal branch must emit only its own fixed marker.
			for _, site := range []string{
				overloadSiteProposalByteBudget, overloadSiteProposalLocalCap, overloadSiteProposalOperationCap,
			} {
				if site == testCase.site {
					continue
				}
				if got := recorder.countOf(site); got != 0 {
					t.Fatalf("proposal admission site %s emitted for the %s force", site, testCase.name)
				}
			}
		})
	}
}

func waitBatcherStop(t *testing.T, batcher *mutationBatcher[KVMutationRequest]) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		batcher.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("batcher run loop did not stop")
	}
}

func fillBatcherInput(t *testing.T, batcher *mutationBatcher[KVMutationRequest]) {
	t.Helper()
	for i := 0; i < cap(batcher.input); i++ {
		batcher.input <- &batchItem{
			ctx: context.Background(), requestID: "queued",
			encoded: []byte("v"), reserved: 1, result: make(chan batchResult, 1),
		}
	}
}
