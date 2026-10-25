package quepaxa

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type terminalAttemptTransport struct {
	Transport
	terminal  Slot
	mu        sync.Mutex
	records   int
	decisions int
}

func (transport *terminalAttemptTransport) SendRecord(ctx context.Context, to NodeID, request RecordRequest) (Summary, error) {
	if request.Slot == transport.terminal {
		transport.mu.Lock()
		transport.records++
		transport.mu.Unlock()
	}
	return transport.Transport.SendRecord(ctx, to, request)
}

func (transport *terminalAttemptTransport) SendDecision(ctx context.Context, decision Decision) error {
	if decision.Slot == transport.terminal {
		transport.mu.Lock()
		transport.decisions++
		transport.mu.Unlock()
	}
	return transport.Transport.SendDecision(ctx, decision)
}

func (transport *terminalAttemptTransport) attempts() (records, decisions int) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.records, transport.decisions
}

func TestReconfigurationPrefixPublicationRunsBeforeTerminalWithoutLocks(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	freeze, err := core.BeginReconfiguration(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	terminal := freeze + 16
	called := 0
	core.reconfigurationPrefixPublication = func(ctx context.Context, published ReconfigurationPrefix) error {
		called++
		if err := ctx.Err(); err != nil {
			return err
		}
		if published.Through != terminal-1 {
			t.Fatalf("published through %d, want frozen terminal predecessor %d", published.Through, terminal-1)
		}
		if hash, ok := core.PrefixHash(published.Through); !ok || hash != published.PrefixHash {
			t.Fatalf("published prefix %x does not match the core prefix", published.PrefixHash)
		}
		if !core.mu.TryLock() {
			t.Fatal("prefix publication ran while holding the core mutex")
		}
		core.mu.Unlock()
		if len(core.pipeline) != 0 || len(core.pipelineExclusive) != 0 {
			t.Fatal("prefix publication ran while holding the pipeline")
		}
		if _, decided := core.CertifiedValue(terminal); decided {
			t.Fatal("terminal was decided before prefix publication")
		}
		return nil
	}
	if err := core.FinishReconfiguration(context.Background()); err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatalf("prefix publication callback calls=%d, want 1", called)
	}
	if _, decided := core.CertifiedValue(terminal); !decided {
		t.Fatal("terminal was not decided after prefix publication")
	}
	if err := core.FinishReconfiguration(context.Background()); err != nil {
		t.Fatalf("prior terminal replay: %v", err)
	}
	if called != 1 {
		t.Fatalf("prior terminal replay called prefix publisher %d times, want 1", called)
	}
}

func TestReconfigurationPrefixPublicationFailureDoesNotProposeTerminal(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	freeze, err := core.BeginReconfiguration(context.Background(), Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}})
	if err != nil {
		t.Fatal(err)
	}
	terminal := freeze + 16
	attempts := &terminalAttemptTransport{Transport: core.transport, terminal: terminal}
	core.transport = attempts
	wantErr := errors.New("prefix publication failed")
	core.reconfigurationPrefixPublication = func(context.Context, ReconfigurationPrefix) error { return wantErr }
	if err := core.FinishReconfiguration(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("FinishReconfiguration error=%v, want wrapped publication error", err)
	}
	if _, decided := core.CertifiedValue(terminal); decided {
		t.Fatal("terminal was proposed after prefix publication failed")
	}
	if core.ConfigID() != 1 {
		t.Fatalf("active configuration changed after publication failure: %d", core.ConfigID())
	}
	if records, decisions := attempts.attempts(); records != 0 || decisions != 0 {
		t.Fatalf("terminal network attempts after publication failure: records=%d decisions=%d, want 0/0", records, decisions)
	}
}

func TestReconfigurationPrefixPublicationCancellationDoesNotProposeTerminal(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	freeze, err := core.BeginReconfiguration(context.Background(), Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}})
	if err != nil {
		t.Fatal(err)
	}
	terminal := freeze + 16
	attempts := &terminalAttemptTransport{Transport: core.transport, terminal: terminal}
	core.transport = attempts
	ctx, cancel := context.WithCancel(context.Background())
	core.reconfigurationPrefixPublication = func(context.Context, ReconfigurationPrefix) error {
		cancel()
		return nil
	}
	if err := core.FinishReconfiguration(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("FinishReconfiguration error=%v, want context cancellation", err)
	}
	if _, decided := core.CertifiedValue(terminal); decided {
		t.Fatal("terminal was proposed after prefix publication canceled")
	}
	if core.ConfigID() != 1 {
		t.Fatalf("active configuration changed after publication cancellation: %d", core.ConfigID())
	}
	if records, decisions := attempts.attempts(); records != 0 || decisions != 0 {
		t.Fatalf("terminal network attempts after publication cancellation: records=%d decisions=%d, want 0/0", records, decisions)
	}
}

func TestReconfigurationPrefixPublicationUsesOwnedTargetSnapshot(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	if _, err := core.BeginReconfiguration(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	core.reconfigurationPrefixPublication = func(ctx context.Context, _ ReconfigurationPrefix) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	result := make(chan error, 1)
	go func() { result <- core.FinishReconfigurationAt(context.Background(), target, 0) }()
	<-entered
	// Callback entry proves the API boundary has already captured its input.
	target.Members[1].ID = "caller-mutated"
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("finish with caller-mutated target: %v", err)
	}
	if got := core.CurrentCluster(); got.ConfigID != 2 || !sameCluster(got, Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}) {
		t.Fatalf("terminal used caller-mutated target: %+v", got)
	}
}

func TestEngineRemovalPrefixPublicationUsesOwnedOperationSnapshot(t *testing.T) {
	cores, _, target := engineOwnedRemovalCluster(t)
	for _, core := range cores {
		core.reconfigAdmission = func(context.Context, ReconfigurationAdmission) error { return nil }
	}
	core := cores["a"]
	operation, err := core.BeginEngineOwnedRemovalAt(context.Background(), target, "snapshot-removal", 0)
	if err != nil {
		t.Fatal(err)
	}
	finishOperation := operation
	finishOperation.Target = cloneCluster(operation.Target)
	operation.Target.Members[0].ID = "caller-mutated-before-finish"
	finishOperation.StorageEffectsExcludedHash = ValueHash{1}
	entered := make(chan struct{})
	release := make(chan struct{})
	core.reconfigurationPrefixPublication = func(ctx context.Context, _ ReconfigurationPrefix) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	result := make(chan error, 1)
	go func() { result <- core.FinishEngineOwnedRemovalAt(context.Background(), finishOperation) }()
	<-entered
	// Target.Members must be detached both when the operation is returned and
	// when the later Finish call snapshots its input.
	finishOperation.Target.Members[0].ID = "caller-mutated-during-finish"
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("finish with caller-mutated engine operation target: %v", err)
	}
	if got := core.CurrentCluster(); got.ConfigID != 2 || !sameCluster(got, target) {
		t.Fatalf("terminal used caller-mutated engine operation target: %+v", got)
	}
}

func TestAbortReconfigurationSkipsPrefixPublication(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	if _, err := core.BeginReconfiguration(context.Background(), Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "lost", WALIdentity: "0000000000000000000000000000000000000000000000000000000000000000"}}}); err != nil {
		t.Fatal(err)
	}
	called := false
	core.reconfigurationPrefixPublication = func(context.Context, ReconfigurationPrefix) error {
		called = true
		return nil
	}
	if err := core.AbortReconfiguration(context.Background()); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("abort invoked the non-abort prefix publication callback")
	}
}

func TestReconfigurationPrefixPublicationRejectsRoundChangedDuringCallback(t *testing.T) {
	cores, _ := reconfigCluster(t)
	core := cores["a"]
	target := Cluster{ConfigID: 2, Members: []Member{{ID: "a"}, {ID: "b"}}}
	freeze, err := core.BeginReconfiguration(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	terminal := freeze + 16
	entered := make(chan struct{})
	release := make(chan struct{})
	var callbackCount int
	core.reconfigurationPrefixPublication = func(ctx context.Context, published ReconfigurationPrefix) error {
		callbackCount++
		if published.Through != terminal-1 {
			t.Errorf("published through %d, want %d", published.Through, terminal-1)
		}
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	finishCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	finishResult := make(chan error, 1)
	go func() { finishResult <- core.FinishReconfiguration(finishCtx) }()
	select {
	case <-entered:
	case <-finishCtx.Done():
		t.Fatalf("prefix callback was not reached: %v", finishCtx.Err())
	}

	// The other voter can finish the same certified round while the first
	// caller is outside the pipeline publishing the fixed old prefix.
	if err := cores["b"].FinishReconfiguration(finishCtx); err != nil {
		t.Fatalf("peer finish: %v", err)
	}
	decidedBefore, ok := core.CertifiedValue(terminal)
	if !ok {
		t.Fatal("peer terminal was not installed while publication callback was blocked")
	}
	close(release)
	select {
	case err := <-finishResult:
		if err == nil {
			t.Fatal("stale finish succeeded after the pending round changed")
		}
	case <-finishCtx.Done():
		t.Fatalf("stale finish did not return after callback release: %v", finishCtx.Err())
	}
	if callbackCount != 1 {
		t.Fatalf("prefix callback count=%d, want 1", callbackCount)
	}
	decidedAfter, ok := core.CertifiedValue(terminal)
	if !ok || !bytes.Equal(decidedBefore.Value, decidedAfter.Value) || !bytes.Equal(decidedBefore.Certificate, decidedAfter.Certificate) {
		t.Fatal("stale finish changed the already-certified terminal")
	}
	if core.ConfigID() != target.ConfigID {
		t.Fatalf("active ConfigID=%d, want %d", core.ConfigID(), target.ConfigID)
	}
	if _, decided := core.CertifiedValue(terminal + 1); decided {
		t.Fatal("stale finish created a decision after the certified terminal")
	}
}
