package quepaxa

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestLocalModeConfigRequiresExplicitStandaloneCore(t *testing.T) {
	tests := []struct {
		name   string
		config Config
	}{
		{
			name:   "multi-voter",
			config: Config{NodeID: "n1", Cluster: Cluster{Members: []Member{{ID: "n1"}, {ID: "n2"}}}, LocalMode: true},
		},
		{
			name:   "foreign-member",
			config: Config{NodeID: "n1", Cluster: Cluster{Members: []Member{{ID: "n2"}}}, LocalMode: true},
		},
		{
			name:   "transport",
			config: Config{NodeID: "n1", Cluster: Cluster{Members: []Member{{ID: "n1"}}}, Transport: &mockTransport{}, LocalMode: true},
		},
		{
			name:   "reconfiguration",
			config: Config{NodeID: "n1", Cluster: Cluster{Members: []Member{{ID: "n1"}}}, EnableReconfiguration: true, LocalMode: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wal, err := qlog.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer wal.Close()
			tt.config.WAL = wal
			if _, err := New(tt.config); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New error=%v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestSingletonClusterDoesNotImplicitlyEnableLocalMode(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := New(Config{
		NodeID:  "n1",
		Cluster: Cluster{Members: []Member{{ID: "n1"}}},
		WAL:     wal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if core.localMode {
		t.Fatal("singleton membership implicitly enabled explicit local mode")
	}
}

func TestLocalRecordQuorumRunsSelfRecordBeforeReturningCancellation(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	proposal := newProposal(highestPriority, "local", []byte("cancelled local record"))
	if err := core.acquireLocalExecution(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := core.recordQuorum(ctx, map[NodeID]RecordRequest{
		"local": {Slot: 1, Step: 4, Proposal: proposal},
	})
	core.releaseLocalExecution()
	if !errors.Is(err, ErrQuorumUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("recordQuorum error=%v, want quorum unavailable and cancellation", err)
	}
	core.mu.RLock()
	state := core.recorders[1]
	core.mu.RUnlock()
	if state.Step != 4 || !sameProposal(state.FirstCurrent, &proposal) {
		t.Fatalf("local receipt state at return=%+v, want completed step 4 record", state)
	}
	if got := localReserveReceiptCount(localReserveEntries(t, wal)); got != 1 {
		t.Fatalf("persisted receipt entries=%d, want 1 before return", got)
	}
}

func TestLocalProposeAndRecoveryCancellationPersistBeforeReturning(t *testing.T) {
	for _, mode := range []string{"propose", "recovery"} {
		t.Run(mode, func(t *testing.T) {
			core, wal := localReserveCore(t, t.TempDir(), "local")
			defer wal.Close()
			assertRecordBarrierWaitsForOuter(t, core, mode)
			entries := localReserveEntries(t, wal)
			if got := localReserveReceiptCount(entries); got == 0 {
				t.Fatal("operation returned without appending its local receipt")
			}
		})
	}
}

func TestSuccessfulProposeRetiresLiveRecorderAfterPersistingReceipt(t *testing.T) {
	core, wal := localReserveCore(t, t.TempDir(), "local")
	defer wal.Close()
	if slot, _, err := core.Propose(context.Background(), []byte("completed proposal")); err != nil {
		t.Fatalf("Propose: %v", err)
	} else if slot != 1 {
		t.Fatalf("Propose slot=%d, want 1", slot)
	}

	core.mu.RLock()
	state, recorderLive := core.recorders[1]
	_, decided := core.decided[1]
	tip := core.tip
	core.mu.RUnlock()
	if recorderLive || state.Step >= 4 {
		t.Fatalf("live recorder=%v state=%+v, want retired entry after decision", recorderLive, state)
	}
	if !decided || tip != 1 {
		t.Fatalf("decision present=%v tip=%d, want decided slot 1", decided, tip)
	}
	entries := localReserveEntries(t, wal)
	if got := localReserveReceiptCount(entries); got != 1 {
		t.Fatalf("persisted receipt entries=%d, want 1 despite live-map retirement", got)
	}
}

func TestNonLocalAsyncRecordIsNegativeControlForBarrier(t *testing.T) {
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := New(Config{
		NodeID:  "local",
		Cluster: Cluster{ConfigID: 1, Members: []Member{{ID: "local"}}},
		WAL:     wal,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertRecordBarrierReturnsOnOuterCancellation(t, core)
}

func assertRecordBarrierWaitsForOuter(t *testing.T, core *Core, mode string) {
	t.Helper()
	started, release := makeRecordBarrier(core)
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		if mode == "propose" {
			_, _, err := core.Propose(ctx, []byte("barrier cancellation"))
			result <- err
			return
		}
		result <- core.RecoverThrough(ctx, 1)
	}()

	awaitRecordBarrier(t, started, result)
	cancel()
	select {
	case err := <-result:
		t.Fatalf("outer %s returned while its Record call was held: %v", mode, err)
	case <-time.After(100 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("outer %s error=%v, want context cancellation", mode, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("outer %s did not return after releasing Record", mode)
	}
	core.mu.RLock()
	state := core.recorders[1]
	core.mu.RUnlock()
	if state.Step < 4 {
		t.Fatalf("record state after release=%+v, want completed partial phase", state)
	}
}

func assertRecordBarrierReturnsOnOuterCancellation(t *testing.T, core *Core) {
	t.Helper()
	started, release := makeRecordBarrier(core)
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, _, err := core.Propose(ctx, []byte("negative control"))
		result <- err
	}()

	awaitRecordBarrier(t, started, result)
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("non-local outer error=%v, want cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("non-local asynchronous path did not return on cancellation")
	}
	releaseOnce.Do(func() { close(release) })
	waitForLocalReceipt(t, core, 1)
	if err := core.commits.Sync(context.Background()); err != nil {
		t.Fatalf("join detached recorder sync: %v", err)
	}
}

func makeRecordBarrier(core *Core) (started, release chan struct{}) {
	started, release = make(chan struct{}), make(chan struct{})
	core.recordBeforeAppend = func() {
		close(started)
		<-release
	}
	return started, release
}

func awaitRecordBarrier(t *testing.T, started <-chan struct{}, result <-chan error) {
	t.Helper()
	select {
	case <-started:
	case err := <-result:
		t.Fatalf("operation returned before reaching Record barrier: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("operation neither returned nor reached the held Record boundary")
	}
}

func waitForLocalReceipt(t *testing.T, core *Core, slot Slot) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		core.mu.RLock()
		step := core.recorders[slot].Step
		core.mu.RUnlock()
		if step >= 4 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("local record did not reach the WAL append boundary")
}
