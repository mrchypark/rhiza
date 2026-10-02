package network

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/quic-go/quic-go"
)

const (
	foregroundAPIWorkers              = 16
	foregroundAPIKeys                 = 4096
	foregroundAPIValue                = 1024
	foregroundAPISeedLogEvery         = 512
	foregroundAPIWindowTotal          = 150 * time.Second
	foregroundAPIWindowCleanupReserve = 15 * time.Second
)

var errForegroundAPISeedBudgetExpired = errors.New("foreground API seed budget expired")

type foregroundAPIPeers struct {
	closeOnce   sync.Once
	config      quepaxa.Cluster
	cores       map[quepaxa.NodeID]*quepaxa.Core
	servers     map[quepaxa.NodeID]*Server
	materials   map[quepaxa.NodeID]*materializer.Materializer
	checkpoints map[quepaxa.NodeID]*checkpoint.Manager
	transports  []*Transport
	peerServers []*PeerServer
	listeners   []*quic.Transport
	connections []net.PacketConn
	wals        []*qlog.WAL
}

func newForegroundAPIPeers(t testing.TB, root string) *foregroundAPIPeers {
	t.Helper()
	clusterID := types.ClusterID("foreground-api")
	ids := []quepaxa.NodeID{"n1", "n2", "n3"}
	tokens := make(map[quepaxa.NodeID]string, len(ids))
	peers := &foregroundAPIPeers{
		cores: make(map[quepaxa.NodeID]*quepaxa.Core, len(ids)), servers: make(map[quepaxa.NodeID]*Server, len(ids)),
		materials: make(map[quepaxa.NodeID]*materializer.Materializer, len(ids)), checkpoints: make(map[quepaxa.NodeID]*checkpoint.Manager, len(ids)),
	}
	t.Cleanup(peers.close)
	for _, id := range ids {
		token := "foreground-voter-" + string(id)
		tokens[id] = token
		member := testMember(clusterID, id, token)
		conn, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		member.PeerURL = "quic://" + conn.LocalAddr().String()
		peers.connections = append(peers.connections, conn)
		peers.config.Members = append(peers.config.Members, member)
	}
	peers.config.ConfigID = 1
	for _, member := range peers.config.Members {
		transport := NewTransport(clusterID, member.ID, &peers.config, tokens[member.ID])
		peers.transports = append(peers.transports, transport)
		nodeDir := filepath.Join(root, string(member.ID))
		if err := os.MkdirAll(nodeDir, 0o700); err != nil {
			t.Fatal(err)
		}
		wal, err := qlog.Open(filepath.Join(nodeDir, "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		peers.wals = append(peers.wals, wal)
		core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: peers.config, WAL: wal, Transport: transport})
		if err != nil {
			_ = wal.Close()
			t.Fatal(err)
		}
		transport.BindCore(core)
		material, err := materializer.Open(filepath.Join(nodeDir, "sqlite.db"), 4)
		if err != nil {
			t.Fatal(err)
		}
		peers.materials[member.ID] = material
		server := NewServer(core, material, clusterID, true, transport)
		peers.cores[member.ID] = core
		peers.servers[member.ID] = server
	}
	for i, member := range peers.config.Members {
		listener := &quic.Transport{Conn: peers.connections[i]}
		peers.listeners = append(peers.listeners, listener)
		peer, err := StartPeerServerOnTransport(context.Background(), listener, peers.servers[member.ID], peers.config.Members, tokens[member.ID], "foreground-admin")
		if err != nil {
			t.Fatal(err)
		}
		peers.peerServers = append(peers.peerServers, peer)
	}
	return peers
}

func (p *foregroundAPIPeers) close() {
	p.closeOnce.Do(func() {
		for _, peer := range p.peerServers {
			_ = peer.Close()
		}
		for _, server := range p.servers {
			server.Close()
		}
		for _, transport := range p.transports {
			_ = transport.Close()
		}
		for _, listener := range p.listeners {
			_ = listener.Close()
		}
		for _, conn := range p.connections {
			_ = conn.Close()
		}
		for _, material := range p.materials {
			_ = material.Close()
		}
		for _, wal := range p.wals {
			_ = wal.Close()
		}
	})
}

type apiLatencySample struct {
	all, success, overlapAll, overlapSuccess []time.Duration
	errors, rejections, timeouts             int
	recording                                time.Duration
}

type apiLatencyAccumulator struct {
	mu sync.Mutex
	apiLatencySample
}

func (a *apiLatencyAccumulator) add(elapsed time.Duration, err error, overlap bool) {
	recordingStarted := time.Now()
	a.mu.Lock()
	a.all = append(a.all, elapsed)
	if err == nil {
		a.success = append(a.success, elapsed)
	} else {
		a.errors++
		if errors.Is(err, ErrNotReady) || errors.Is(err, quepaxa.ErrQuorumUnavailable) {
			a.rejections++
		}
		if errors.Is(err, context.DeadlineExceeded) || isForegroundTimeout(err) {
			a.timeouts++
		}
	}
	if overlap {
		a.overlapAll = append(a.overlapAll, elapsed)
		if err == nil {
			a.overlapSuccess = append(a.overlapSuccess, elapsed)
		}
	}
	a.recording += time.Since(recordingStarted)
	a.mu.Unlock()
}

func isForegroundTimeout(err error) bool {
	var networkErr net.Error
	return errors.As(err, &networkErr) && networkErr.Timeout()
}

func (a *apiLatencyAccumulator) snapshot() apiLatencySample {
	a.mu.Lock()
	defer a.mu.Unlock()
	return apiLatencySample{
		all: append([]time.Duration(nil), a.all...), success: append([]time.Duration(nil), a.success...),
		overlapAll: append([]time.Duration(nil), a.overlapAll...), overlapSuccess: append([]time.Duration(nil), a.overlapSuccess...),
		errors: a.errors, rejections: a.rejections, timeouts: a.timeouts, recording: a.recording,
	}
}

func apiLatencyJSON(sample apiLatencySample, seconds float64) map[string]any {
	return map[string]any{
		"requests": len(sample.all), "successes": len(sample.success), "errors": sample.errors,
		"rejections": sample.rejections, "timeouts": sample.timeouts,
		"successful_ops_per_second":     float64(len(sample.success)) / seconds,
		"all_completion_ops_per_second": float64(len(sample.all)) / seconds,
		"sample_recording_ns_per_operation": func() float64 {
			if len(sample.all) == 0 {
				return 0
			}
			return float64(sample.recording) / float64(len(sample.all))
		}(),
		"success_p99_ms": nearestRankDurationP99(sample.success), "all_completion_p99_ms": nearestRankDurationP99(sample.all),
		"maintenance_overlap_requests":              len(sample.overlapAll),
		"maintenance_overlap_success_p99_ms":        nearestRankDurationP99(sample.overlapSuccess),
		"maintenance_overlap_all_completion_p99_ms": nearestRankDurationP99(sample.overlapAll),
	}
}

func nearestRankDurationP99(values []time.Duration) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return float64(ordered[(99*len(ordered)+99)/100-1]) / float64(time.Millisecond)
}

func stopForegroundMaintenance(cancel context.CancelFunc, workers *sync.WaitGroup) {
	cancel()
	workers.Wait()
}

func foregroundMaintenanceFailure(scenario, stage string, err error, ctx context.Context, elapsed time.Duration) (string, bool) {
	if err == nil || errors.Is(err, context.Canceled) {
		return "", false
	}
	if prefix, _, ok := strings.Cut(err.Error(), ": "); ok {
		switch prefix {
		case "checkpoint-pre-sync", "checkpoint-publication", "certified-seal-read", "certified-seal-validation", "fresh-current-readback", "fresh-current-comparison":
			stage = prefix
		}
	}
	return fmt.Sprintf("scenario=%s stage=%s error=%q error_is_canceled=%t error_is_deadline_exceeded=%t context_error=%v elapsed=%s", scenario, stage, err, errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), ctx.Err(), elapsed), true
}

func TestForegroundMaintenanceFailureKeepsCauseAndFiltersWrappedCancellation(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if observation, counted := foregroundMaintenanceFailure("checkpoint-active", "checkpoint-publication", fmt.Errorf("publication: %w", context.Canceled), canceled, time.Second); counted || observation != "" {
		t.Fatalf("wrapped cancellation observation=%q counted=%t; want filtered", observation, counted)
	}

	deadlineErr := fmt.Errorf("archive sync: %w", context.DeadlineExceeded)
	observation, counted := foregroundMaintenanceFailure("checkpoint-active", "checkpoint-pre-sync", deadlineErr, context.Background(), 2*time.Second)
	if !counted || !strings.Contains(observation, "scenario=checkpoint-active") || !strings.Contains(observation, "stage=checkpoint-pre-sync") || !strings.Contains(observation, "error_is_deadline_exceeded=true") || !strings.Contains(observation, "error=\"archive sync: context deadline exceeded\"") {
		t.Fatalf("deadline failure observation=%q counted=%t", observation, counted)
	}

	nonContextErr := errors.New("publication mismatch")
	observation, counted = foregroundMaintenanceFailure("checkpoint-active", "fresh-current-readback", nonContextErr, canceled, 3*time.Second)
	if !counted || !strings.Contains(observation, "context_error=context canceled") || !strings.Contains(observation, "error_is_canceled=false") {
		t.Fatalf("non-context failure observation=%q counted=%t", observation, counted)
	}
}

func TestStopForegroundMaintenanceCancelsBlockedOperationBeforeJoin(t *testing.T) {
	operationCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var workers sync.WaitGroup
	workers.Add(1)
	started := make(chan struct{})
	go func() {
		defer workers.Done()
		close(started)
		<-operationCtx.Done()
	}()
	<-started
	stopped := make(chan struct{})
	go func() {
		stopForegroundMaintenance(cancel, &workers)
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("maintenance join did not cancel the blocked operation")
	}
}

func foregroundAPISeedDeadline(parentDeadline time.Time, hasParentDeadline bool, runnerDeadline time.Time) (time.Time, bool) {
	if runnerDeadline.IsZero() {
		return time.Time{}, false
	}
	seedDeadline := runnerDeadline.Add(-(foregroundAPIWindowTotal + foregroundAPIWindowCleanupReserve))
	if hasParentDeadline && !seedDeadline.Before(parentDeadline) {
		return parentDeadline, false
	}
	return seedDeadline, true
}

func withForegroundAPISeedBudget(parent context.Context, runnerDeadline time.Time) (context.Context, context.CancelFunc, time.Time, bool) {
	parentDeadline, hasParentDeadline := parent.Deadline()
	seedDeadline, runnerBound := foregroundAPISeedDeadline(parentDeadline, hasParentDeadline, runnerDeadline)
	if seedDeadline.IsZero() {
		ctx, cancel := context.WithCancel(parent)
		return ctx, cancel, time.Time{}, false
	}
	ctx, cancel := context.WithDeadline(parent, seedDeadline)
	return ctx, cancel, seedDeadline, runnerBound
}

func foregroundAPISeedResult(err error, ctx context.Context, budgetDeadline time.Time, runnerBound bool) error {
	contextErr := ctx.Err()
	if runnerBound && !budgetDeadline.IsZero() && !time.Now().Before(budgetDeadline) {
		return errors.Join(errForegroundAPISeedBudgetExpired, err, context.DeadlineExceeded)
	}
	return errors.Join(err, contextErr)
}

func seedForegroundAPIKeys(ctx context.Context, put func(context.Context, KVMutationRequest) error, value []byte, clientID int64, budgetDeadline time.Time, runnerBound bool, progress func(int)) error {
	if err := foregroundAPISeedResult(nil, ctx, budgetDeadline, runnerBound); err != nil {
		return err
	}
	seedCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int)
	var workers sync.WaitGroup
	var completed atomic.Uint64
	var errMu sync.Mutex
	var seedErr error
	workers.Add(foregroundAPIWorkers)
	for range foregroundAPIWorkers {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-seedCtx.Done():
					return
				case index, ok := <-jobs:
					if !ok {
						return
					}
					requestID := fmt.Sprintf("foreground-seed-%d-%d", clientID, index)
					key := fmt.Sprintf("key-%04d", index)
					if err := put(seedCtx, KVMutationRequest{RequestID: requestID, Key: key, Value: value}); err != nil {
						errMu.Lock()
						seedErr = errors.Join(seedErr, fmt.Errorf("seed key %d: %w", index, err))
						errMu.Unlock()
						cancel()
						return
					}
					count := int(completed.Add(1))
					if progress != nil && (count%foregroundAPISeedLogEvery == 0 || count == foregroundAPIKeys) {
						progress(count)
					}
				}
			}
		}()
	}

dispatch:
	for index := range foregroundAPIKeys {
		select {
		case jobs <- index:
		case <-seedCtx.Done():
			break dispatch
		}
	}
	close(jobs)
	workers.Wait()

	errMu.Lock()
	defer errMu.Unlock()
	return foregroundAPISeedResult(seedErr, ctx, budgetDeadline, runnerBound)
}

func foregroundAPIWindowBudgetAvailable(deadline, now time.Time) bool {
	if deadline.IsZero() {
		return true
	}
	return deadline.Sub(now) >= foregroundAPIWindowTotal+foregroundAPIWindowCleanupReserve
}

func requireForegroundAPIWindowBudget(t testing.TB, deadline time.Time, phase string) {
	t.Helper()
	now := time.Now()
	if !foregroundAPIWindowBudgetAvailable(deadline, now) {
		t.Fatalf("insufficient test deadline at %s before fixed foreground windows: remaining=%s required=%s (windows=%s cleanup_reserve=%s)", phase, deadline.Sub(now), foregroundAPIWindowTotal+foregroundAPIWindowCleanupReserve, foregroundAPIWindowTotal, foregroundAPIWindowCleanupReserve)
	}
}

type foregroundAPISeedRecorder struct {
	mu          sync.Mutex
	cond        *sync.Cond
	requests    map[string]KVMutationRequest
	failRequest string
	failure     error
	barrier     int
	started     int
	active      int
	maximum     int
}

func newForegroundAPISeedRecorder(barrier int) *foregroundAPISeedRecorder {
	recorder := &foregroundAPISeedRecorder{requests: make(map[string]KVMutationRequest), barrier: barrier}
	recorder.cond = sync.NewCond(&recorder.mu)
	return recorder
}

func (recorder *foregroundAPISeedRecorder) put(ctx context.Context, request KVMutationRequest) error {
	recorder.mu.Lock()
	recorder.active++
	if recorder.active > recorder.maximum {
		recorder.maximum = recorder.active
	}
	recorder.started++
	if recorder.barrier > 0 && recorder.started <= recorder.barrier && recorder.started < recorder.barrier {
		for recorder.started < recorder.barrier {
			recorder.cond.Wait()
		}
	} else if recorder.barrier > 0 && recorder.started == recorder.barrier {
		recorder.cond.Broadcast()
	}
	request.Value = append([]byte(nil), request.Value...)
	recorder.requests[request.RequestID] = request
	err := recorder.failure
	if request.RequestID != recorder.failRequest {
		err = nil
	}
	if err == nil {
		err = ctx.Err()
	}
	recorder.active--
	recorder.mu.Unlock()
	return err
}

func TestForegroundAPISeedPreservesExactRequestsWithBoundedConcurrency(t *testing.T) {
	const clientID = int64(42)
	value := make([]byte, foregroundAPIValue)
	for index := range value {
		value[index] = byte((index*31 + 7) % 251)
	}
	recorder := newForegroundAPISeedRecorder(foregroundAPIWorkers)
	var progressMu sync.Mutex
	progress := make(map[int]bool)
	if err := seedForegroundAPIKeys(context.Background(), recorder.put, value, clientID, time.Time{}, false, func(count int) {
		progressMu.Lock()
		progress[count] = true
		progressMu.Unlock()
	}); err != nil {
		t.Fatalf("seedForegroundAPIKeys: %v", err)
	}

	recorder.mu.Lock()
	requestCount, maximum, requests := len(recorder.requests), recorder.maximum, recorder.requests
	recorder.mu.Unlock()
	if requestCount != foregroundAPIKeys {
		t.Fatalf("seed request count=%d, want %d", requestCount, foregroundAPIKeys)
	}
	if maximum != foregroundAPIWorkers {
		t.Fatalf("maximum concurrent seed calls=%d, want exactly %d from the barrier", maximum, foregroundAPIWorkers)
	}
	for index := range foregroundAPIKeys {
		requestID := fmt.Sprintf("foreground-seed-%d-%d", clientID, index)
		request, ok := requests[requestID]
		if !ok {
			t.Fatalf("missing seed request %q", requestID)
		}
		if request.Key != fmt.Sprintf("key-%04d", index) || !bytes.Equal(request.Value, value) {
			t.Fatalf("seed request %q has key=%q value_bytes=%d", requestID, request.Key, len(request.Value))
		}
	}
	progressMu.Lock()
	defer progressMu.Unlock()
	for count := foregroundAPISeedLogEvery; count <= foregroundAPIKeys; count += foregroundAPISeedLogEvery {
		if !progress[count] {
			t.Errorf("missing seed progress milestone %d", count)
		}
	}
}

func TestForegroundAPISeedFailureCancelsAndJoinsWorkers(t *testing.T) {
	wantErr := errors.New("seed failure")
	const clientID = int64(43)
	recorder := newForegroundAPISeedRecorder(foregroundAPIWorkers)
	recorder.failRequest = fmt.Sprintf("foreground-seed-%d-%d", clientID, 17)
	recorder.failure = wantErr
	err := seedForegroundAPIKeys(context.Background(), recorder.put, make([]byte, foregroundAPIValue), clientID, time.Time{}, false, nil)
	if !errors.Is(err, wantErr) {
		t.Fatalf("seed error=%v, want wrapped %v", err, wantErr)
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.active != 0 {
		t.Fatalf("seed helper returned with %d active workers", recorder.active)
	}
	if len(recorder.requests) == 0 || len(recorder.requests) > foregroundAPIKeys {
		t.Fatalf("failure path attempted %d unique requests", len(recorder.requests))
	}
}

func TestForegroundAPISeedParentCancellationJoinsWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, foregroundAPIWorkers)
	var active atomic.Int64
	put := func(ctx context.Context, _ KVMutationRequest) error {
		active.Add(1)
		started <- struct{}{}
		<-ctx.Done()
		active.Add(-1)
		return ctx.Err()
	}
	done := make(chan error, 1)
	go func() {
		done <- seedForegroundAPIKeys(ctx, put, make([]byte, foregroundAPIValue), 44, time.Time{}, false, nil)
	}()
	for range foregroundAPIWorkers {
		<-started
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("seed error=%v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("seed helper did not join workers after parent cancellation")
	}
	if got := active.Load(); got != 0 {
		t.Fatalf("seed helper returned with %d active KVPut calls", got)
	}
}

func TestForegroundAPIWindowBudgetRequiresBothFixedWindowsAndCleanupReserve(t *testing.T) {
	deadline := time.Unix(1000, 0)
	minimumStart := deadline.Add(-(foregroundAPIWindowTotal + foregroundAPIWindowCleanupReserve))
	for _, test := range []struct {
		name string
		now  time.Time
		want bool
	}{
		{name: "exact minimum", now: minimumStart, want: true},
		{name: "one nanosecond short", now: minimumStart.Add(time.Nanosecond), want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := foregroundAPIWindowBudgetAvailable(deadline, test.now); got != test.want {
				t.Fatalf("budget available=%t, want %t", got, test.want)
			}
		})
	}
	if !foregroundAPIWindowBudgetAvailable(time.Time{}, deadline) {
		t.Fatal("missing test deadline must leave the fixed windows runnable")
	}
}

func TestForegroundAPISeedDeadlineReservesWindowsAndHonorsParent(t *testing.T) {
	runnerDeadline := time.Unix(2000, 0)
	want := runnerDeadline.Add(-(foregroundAPIWindowTotal + foregroundAPIWindowCleanupReserve))
	ctx, cancel, got, runnerBound := withForegroundAPISeedBudget(context.Background(), runnerDeadline)
	defer cancel()
	actual, ok := ctx.Deadline()
	if !ok || !actual.Equal(want) || !got.Equal(want) || !runnerBound {
		t.Fatalf("derived seed deadline=(%s, ok=%t, runnerBound=%t), want %s", actual, ok, runnerBound, want)
	}
	for _, test := range []struct {
		name           string
		parentDeadline time.Time
		hasParent      bool
		runnerDeadline time.Time
		wantDeadline   time.Time
		wantRunner     bool
	}{
		{name: "runner reserve", runnerDeadline: runnerDeadline, wantDeadline: want, wantRunner: true},
		{name: "earlier parent wins", parentDeadline: want.Add(-time.Second), hasParent: true, runnerDeadline: runnerDeadline, wantDeadline: want.Add(-time.Second)},
		{name: "later parent does not shorten reserve", parentDeadline: want.Add(time.Second), hasParent: true, runnerDeadline: runnerDeadline, wantDeadline: want, wantRunner: true},
		{name: "no runner deadline leaves parent inherited", parentDeadline: runnerDeadline, hasParent: true, wantRunner: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			gotDeadline, gotRunner := foregroundAPISeedDeadline(test.parentDeadline, test.hasParent, test.runnerDeadline)
			if !gotDeadline.Equal(test.wantDeadline) || gotRunner != test.wantRunner {
				t.Fatalf("seed deadline=(%s, runnerBound=%t), want (%s, runnerBound=%t)", gotDeadline, gotRunner, test.wantDeadline, test.wantRunner)
			}
		})
	}
}

func TestForegroundAPISeedBudgetExpiryIsNamedBeforeDispatch(t *testing.T) {
	deadline := time.Now().Add(-time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	var calls atomic.Uint64
	err := seedForegroundAPIKeys(ctx, func(context.Context, KVMutationRequest) error {
		calls.Add(1)
		return nil
	}, make([]byte, foregroundAPIValue), 45, deadline, true, nil)
	if !errors.Is(err, errForegroundAPISeedBudgetExpired) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired seed budget error=%v, want named budget and deadline causes", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("expired seed budget dispatched %d KVPut calls", got)
	}
}

// TestForegroundAPICost uses the existing embedded three-peer API harness.
// It is opt-in because each measured window is fixed at two minutes.
func TestForegroundAPICost(t *testing.T) {
	runnerDeadline, hasRunnerDeadline := t.Deadline()
	scenario := os.Getenv("RHIZA_FOREGROUND_API_COST_SCENARIO")
	if scenario == "" {
		t.Skip("set RHIZA_FOREGROUND_API_COST_SCENARIO to run the 150-second local API measurement")
	}
	if scenario != "baseline" && scenario != "checkpoint-active" && scenario != "gc-active" {
		t.Fatalf("unsupported scenario %q", scenario)
	}
	repetition := os.Getenv("RHIZA_FOREGROUND_API_COST_REPETITION")
	if repetition == "" {
		repetition = "1"
	}
	if repetition != "1" && repetition != "2" && repetition != "3" {
		t.Fatalf("repetition must be 1, 2, or 3; got %q", repetition)
	}
	root := t.TempDir()
	archiveBucket, err := objmetrics.NewBucket(objmetrics.Config{Provider: objmetrics.ProviderFilesystem, FilesystemDir: root + "/archive-objects"})
	if err != nil {
		t.Fatal(err)
	}
	defer archiveBucket.Close()
	cluster := newForegroundAPIPeers(t, filepath.Join(root, "peers"))
	defer cluster.close()
	server := cluster.servers["n1"]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	maintenanceCtx, cancelMaintenance := context.WithCancel(ctx)
	archive := recovery.NewManager(archiveBucket, "foreground-api/archive", 1)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	server.SetDurabilityBarrier(func(barrierCtx context.Context, slot quepaxa.Slot) error {
		return archive.SyncThrough(barrierCtx, cluster.cores["n1"], slot)
	})
	value := make([]byte, foregroundAPIValue)
	for i := range value {
		value[i] = byte((i*31 + 7) % 251)
	}
	var requestSequence atomic.Uint64
	clientID := time.Now().UnixNano()
	if hasRunnerDeadline {
		requireForegroundAPIWindowBudget(t, runnerDeadline, "before key seeding")
	}
	seedCtx, cancelSeed, seedDeadline, runnerBound := withForegroundAPISeedBudget(ctx, runnerDeadline)
	err = seedForegroundAPIKeys(seedCtx, func(putCtx context.Context, request KVMutationRequest) error {
		_, err := server.KVPut(putCtx, request)
		return err
	}, value, clientID, seedDeadline, runnerBound, func(count int) {
		t.Logf("foreground API seed progress %d/%d", count, foregroundAPIKeys)
	})
	cancelSeed()
	if err != nil {
		if errors.Is(err, errForegroundAPISeedBudgetExpired) {
			t.Fatalf("foreground API seed budget expired before completion: %v", err)
		}
		t.Fatalf("seed foreground API keys: %v", err)
	}
	maintenanceActive := atomic.Bool{}
	maintenanceEpoch := atomic.Uint64{}
	var maintenanceErrors atomic.Uint64
	firstMaintenanceError := "none"
	var checkpointPublications atomic.Uint64
	var maintenanceWG sync.WaitGroup
	var checkpointBucket *objmetrics.MeteredBucket
	var checkpointManager *checkpoint.Manager
	var checkpointer *checkpoint.AutoCheckpointer
	stopMaintenance := func() { stopForegroundMaintenance(cancelMaintenance, &maintenanceWG) }
	defer func() {
		stopMaintenance()
		if checkpointBucket != nil {
			_ = checkpointBucket.Close()
		}
	}()
	checkpointMaintenance, gcMaintenance := "not run", "not run"
	if scenario == "checkpoint-active" {
		checkpointBucket, err = objmetrics.NewBucket(objmetrics.Config{Provider: objmetrics.ProviderFilesystem, FilesystemDir: root + "/checkpoint-objects"})
		if err != nil {
			t.Fatal(err)
		}
		checkpointManager, err = configureForegroundCheckpoint(t, cluster, checkpointBucket, filepath.Join(root, "checkpoint-work"))
		if err != nil {
			t.Fatal(err)
		}
		checkpointer = foregroundCheckpointPublisher(cluster, checkpointManager, archive)
		checkpointMaintenance = "full publisher claim, snapshot/upload, peer prepare quorum, certified seal, CURRENT CAS promotion, and fresh-manager readback verification"
	}
	if scenario == "gc-active" {
		gcMaintenance = "recovery-manager archive Cleanup; checkpoint-root GC excluded"
	}
	if hasRunnerDeadline {
		requireForegroundAPIWindowBudget(t, runnerDeadline, "before warm-up")
	}
	if scenario != "baseline" {
		maintenanceWG.Add(1)
		go func() {
			defer maintenanceWG.Done()
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-maintenanceCtx.Done():
					return
				case <-ticker.C:
					maintenanceEpoch.Add(1)
					maintenanceActive.Store(true)
					maintenanceStarted := time.Now()
					if scenario == "checkpoint-active" {
						created, err := createForegroundCertifiedCheckpoint(maintenanceCtx, checkpointer, checkpointManager, checkpointBucket, archive, cluster)
						if err != nil {
							observation, counted := foregroundMaintenanceFailure(scenario, "checkpoint-maintenance", err, maintenanceCtx, time.Since(maintenanceStarted))
							if counted {
								maintenanceErrors.Add(1)
								if firstMaintenanceError == "none" {
									firstMaintenanceError = observation
									t.Logf("FOREGROUND_MAINTENANCE_ERROR %s", firstMaintenanceError)
								}
							}
						} else if created {
							checkpointPublications.Add(1)
						}
					}
					if scenario == "gc-active" {
						if err := archive.Cleanup(maintenanceCtx, 0); err != nil {
							observation, counted := foregroundMaintenanceFailure(scenario, "archive-cleanup", err, maintenanceCtx, time.Since(maintenanceStarted))
							if counted {
								maintenanceErrors.Add(1)
								if firstMaintenanceError == "none" {
									firstMaintenanceError = observation
									t.Logf("FOREGROUND_MAINTENANCE_ERROR %s", firstMaintenanceError)
								}
							}
						}
					}
					maintenanceActive.Store(false)
					maintenanceEpoch.Add(1)
				}
			}
		}()
	}
	if _, _, err := runForegroundAPIWindow(ctx, server, value, clientID, &requestSequence, 30*time.Second, &maintenanceActive, &maintenanceEpoch, false); err != nil {
		t.Fatal(err)
	}
	put, get, err := runForegroundAPIWindow(ctx, server, value, clientID, &requestSequence, 120*time.Second, &maintenanceActive, &maintenanceEpoch, true)
	if err != nil {
		t.Fatal(err)
	}
	stopMaintenance()
	putStats, getStats := put.snapshot(), get.snapshot()
	putJSON := apiLatencyJSON(putStats, 120)
	getJSON := apiLatencyJSON(getStats, 120)
	result, err := json.Marshal(map[string]any{
		"profile": "in-process-api-entry-to-return", "scenario": scenario, "repetition": repetition,
		"topology":               "three embedded API servers with real peer QUIC over loopback UDP; API calls measured directly",
		"durability":             "filesystem object-store before-ack shared archive barrier",
		"checkpoint_maintenance": checkpointMaintenance, "gc_maintenance": gcMaintenance,
		"workers": foregroundAPIWorkers, "seeded_keys": foregroundAPIKeys, "value_bytes": foregroundAPIValue,
		"warmup": "30s", "measured_window": "120s", "application_retries": 0,
		"sample_threshold": 10000, "insufficient_samples": len(putStats.success) < 10000 || len(getStats.success) < 10000,
		"put": putJSON, "linearizable_get": getJSON,
		"total_successful_ops_per_second":     float64(len(putStats.success)+len(getStats.success)) / 120,
		"total_all_completion_ops_per_second": float64(len(putStats.all)+len(getStats.all)) / 120,
		"maintenance_errors":                  maintenanceErrors.Load(), "maintenance_first_error": firstMaintenanceError,
		"checkpoint_publications_verified": checkpointPublications.Load(),
		"storage":                          "local filesystem object-store fixture",
		"go":                               runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH, "gomaxprocs": runtime.GOMAXPROCS(0),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("FOREGROUND_API_COST %s", result)
	if scenario == "checkpoint-active" && checkpointPublications.Load() == 0 {
		t.Fatal("checkpoint-active scenario completed without a certified checkpoint publication")
	}
	if maintenanceErrors.Load() != 0 {
		t.Fatalf("maintenance failed %d times; see cost record", maintenanceErrors.Load())
	}
}

func configureForegroundCheckpoint(t testing.TB, peers *foregroundAPIPeers, bucket *objmetrics.MeteredBucket, workRoot string) (*checkpoint.Manager, error) {
	t.Helper()
	for id, core := range peers.cores {
		localDir := filepath.Join(workRoot, string(id))
		if err := os.MkdirAll(localDir, 0o700); err != nil {
			return nil, err
		}
		manager := checkpoint.NewManager(bucket, "foreground-api/checkpoint", localDir, 1)
		if err := manager.Load(context.Background()); err != nil {
			return nil, err
		}
		core.SetCheckpointValidator(func(ctx context.Context, seal quepaxa.CheckpointSeal) error {
			return manager.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash)
		})
		peers.checkpoints[id] = manager
	}
	return peers.checkpoints["n1"], nil
}

func foregroundCheckpointPublisher(peers *foregroundAPIPeers, manager *checkpoint.Manager, archive *recovery.Manager) *checkpoint.AutoCheckpointer {
	core, server, material := peers.cores["n1"], peers.servers["n1"], peers.materials["n1"]
	transport := peers.transports[0]
	auto := checkpoint.NewAutoCheckpointer(manager, material, 1, time.Hour)
	auto.ConfigurePublisher(string(core.NodeID()), func() uint64 {
		floor := material.Tip()
		if seal, ok, err := core.LatestCheckpointSeal(); err == nil && ok {
			floor = max(floor, uint64(seal.Index))
		}
		if latest := manager.Latest(); latest != nil {
			floor = max(floor, latest.Index)
		}
		return floor
	}, func(ctx context.Context, reserved uint64) error {
		for material.Tip() < reserved {
			var nonce [types.ReadBarrierNonceSize]byte
			if _, err := rand.Read(nonce[:]); err != nil {
				return err
			}
			if _, err := server.ProposeControl(ctx, types.EncodeReadBarrier(nonce)); err != nil {
				return err
			}
		}
		return nil
	})
	auto.ConfigurePublication(func() bool {
		seal, ok, err := core.LatestCheckpointSeal()
		return core.IsVoter() && err == nil && (!ok || material.Tip() > uint64(seal.Index))
	}, func(ctx context.Context, root *checkpoint.Checkpoint) error {
		prefix, ok := core.PrefixHash(quepaxa.Slot(root.Index))
		if !ok {
			return fmt.Errorf("checkpoint prefix %d is unavailable", root.Index)
		}
		next, following, err := core.CheckpointLeaderOrders(quepaxa.Slot(root.Index))
		if err != nil {
			return err
		}
		seal := quepaxa.CheckpointSeal{
			ConfigID: core.ConfigIDForSlot(quepaxa.Slot(root.Index)), Index: quepaxa.Slot(root.Index),
			RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix,
			NextLeaderOrder: next, FollowingLeaderOrder: following, GenerationAnchorHash: core.GenerationAnchorHash(),
		}
		if err := manager.ValidatePublisherClaim(ctx, string(core.NodeID()), root.Index, root.RootHash); err != nil {
			return err
		}
		if err := core.PrepareCheckpoint(ctx, seal); err != nil {
			return err
		}
		if err := transport.PrepareCheckpoint(ctx, seal); err != nil {
			return err
		}
		value, err := quepaxa.EncodeCheckpointSeal(seal)
		if err != nil {
			return err
		}
		sealSlot, _, err := core.Propose(ctx, value)
		if err != nil {
			return err
		}
		if err := server.applyDecisions(ctx, sealSlot); err != nil {
			return err
		}
		if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
			return err
		}
		if err := manager.PromoteCertifiedCurrent(ctx, root); err != nil {
			return err
		}
		sealed, ok, err := core.LatestCheckpointSeal()
		if err != nil {
			return err
		}
		if !ok || sealed.Index <= core.CompactionFloor() {
			return nil
		}
		if quepaxa.Slot(material.Tip()) < sealed.Index {
			return fmt.Errorf("materializer tip %d is behind checkpoint %d", material.Tip(), sealed.Index)
		}
		if err := core.VerifyCheckpoint(ctx, sealed.CheckpointSeal); err != nil {
			return err
		}
		if err := archive.SyncThrough(ctx, core, sealed.DecisionSlot); err != nil {
			return err
		}
		decision, ok := core.CertifiedValue(sealed.DecisionSlot)
		if !ok {
			return fmt.Errorf("checkpoint seal decision %d is unavailable", sealed.DecisionSlot)
		}
		if err := archive.TrimThrough(ctx, sealed, decision); err != nil {
			return err
		}
		return core.CompactThrough(sealed.Index, sealed.RootHash)
	})
	return auto
}

func createForegroundCertifiedCheckpoint(ctx context.Context, auto *checkpoint.AutoCheckpointer, manager *checkpoint.Manager, bucket *objmetrics.MeteredBucket, archive *recovery.Manager, peers *foregroundAPIPeers) (bool, error) {
	core, material := peers.cores["n1"], peers.materials["n1"]
	before := manager.Latest()
	if material.Tip() == 0 || before != nil && before.Index >= material.Tip() {
		return false, nil
	}
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		return false, fmt.Errorf("checkpoint-pre-sync: %w", err)
	}
	if err := auto.CheckpointOnShutdown(ctx, material.Tip()); err != nil {
		return false, fmt.Errorf("checkpoint-publication: %w", err)
	}
	winner := manager.Latest()
	if winner == nil || before != nil && (winner.Index <= before.Index || winner.RootHash == before.RootHash) {
		return false, nil
	}
	seal, sealed, err := core.LatestCheckpointSeal()
	if err != nil {
		return false, fmt.Errorf("certified-seal-read: %w", err)
	}
	if !sealed || uint64(seal.Index) != winner.Index || seal.RootHash != winner.RootHash || seal.StateHash != winner.Hash {
		return false, fmt.Errorf("certified-seal-validation: checkpoint CURRENT candidate %d is not certified by the local Core", winner.Index)
	}
	readback := checkpoint.NewManager(bucket, "foreground-api/checkpoint", "", 1)
	if err := readback.Load(ctx); err != nil {
		return false, fmt.Errorf("fresh-current-readback: %w", err)
	}
	verified := readback.Latest()
	if verified == nil || verified.Index != winner.Index || verified.RootHash != winner.RootHash || verified.Hash != winner.Hash {
		return false, fmt.Errorf("fresh-current-comparison: checkpoint CURRENT readback does not match certified winner %d", winner.Index)
	}
	return true, nil
}

func TestForegroundAPICheckpointUsesCertifiedLoopbackPublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root := t.TempDir()
	archiveBucket, err := objmetrics.NewBucket(objmetrics.Config{Provider: objmetrics.ProviderFilesystem, FilesystemDir: filepath.Join(root, "archive-objects")})
	if err != nil {
		t.Fatal(err)
	}
	defer archiveBucket.Close()
	peers := newForegroundAPIPeers(t, filepath.Join(root, "peers"))
	defer peers.close()
	archive := recovery.NewManager(archiveBucket, "foreground-api/archive", 1)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	server, core := peers.servers["n1"], peers.cores["n1"]
	server.SetDurabilityBarrier(func(barrierCtx context.Context, slot quepaxa.Slot) error {
		return archive.SyncThrough(barrierCtx, core, slot)
	})
	checkpointBucket, err := objmetrics.NewBucket(objmetrics.Config{Provider: objmetrics.ProviderFilesystem, FilesystemDir: filepath.Join(root, "checkpoint-objects")})
	if err != nil {
		t.Fatal(err)
	}
	defer checkpointBucket.Close()
	manager, err := configureForegroundCheckpoint(t, peers, checkpointBucket, filepath.Join(root, "checkpoint-work"))
	if err != nil {
		t.Fatal(err)
	}
	checkpointer := foregroundCheckpointPublisher(peers, manager, archive)
	for i, requestID := range []string{"foreground-checkpoint-fixture-1", "foreground-checkpoint-fixture-2"} {
		if _, err := server.KVPut(ctx, KVMutationRequest{RequestID: requestID, Key: "key", Value: []byte(fmt.Sprintf("value-%d", i))}); err != nil {
			t.Fatal(err)
		}
		created, err := createForegroundCertifiedCheckpoint(ctx, checkpointer, manager, checkpointBucket, archive, peers)
		if err != nil {
			t.Fatal(err)
		}
		if !created {
			t.Fatalf("checkpoint publication helper did not publish certified CURRENT %d", i+1)
		}
	}
}

func runForegroundAPIWindow(ctx context.Context, server *Server, value []byte, clientID int64, sequence *atomic.Uint64, duration time.Duration, maintenanceActive *atomic.Bool, maintenanceEpoch *atomic.Uint64, collect bool) (*apiLatencyAccumulator, *apiLatencyAccumulator, error) {
	put, get := &apiLatencyAccumulator{}, &apiLatencyAccumulator{}
	window, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	var workers sync.WaitGroup
	for worker := range foregroundAPIWorkers {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			index := worker
			for window.Err() == nil {
				key := fmt.Sprintf("key-%04d", index%foregroundAPIKeys)
				id := sequence.Add(1)
				startedEpoch := maintenanceEpoch.Load()
				startedDuringMaintenance := maintenanceActive.Load()
				started := time.Now()
				_, putErr := server.KVPut(window, KVMutationRequest{RequestID: fmt.Sprintf("foreground-%d-%d", clientID, id), Key: key, Value: value})
				putElapsed := time.Since(started)
				putOverlap := startedDuringMaintenance || maintenanceActive.Load() || startedEpoch != maintenanceEpoch.Load()
				if collect {
					put.add(putElapsed, putErr, putOverlap)
				}
				if window.Err() != nil {
					return
				}
				startedEpoch = maintenanceEpoch.Load()
				startedDuringMaintenance = maintenanceActive.Load()
				started = time.Now()
				response, getErr := server.KVGet(window, KVGetRequest{Key: key, Consistency: "linearizable"})
				getElapsed := time.Since(started)
				getOverlap := startedDuringMaintenance || maintenanceActive.Load() || startedEpoch != maintenanceEpoch.Load()
				if getErr == nil && !response.Found {
					getErr = fmt.Errorf("linearizable get missing seeded value")
				}
				if collect {
					get.add(getElapsed, getErr, getOverlap)
				}
				index = (index + foregroundAPIWorkers) % foregroundAPIKeys
			}
		}(worker)
	}
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return put, get, err
	}
	return put, get, nil
}
