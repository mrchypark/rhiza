//go:build rhiza_local_testhooks && rhiza_receipt_quorum_fixture

package network

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// A certified slot is deliberately left unapplied on the query server so that
// linearizable read must apply the receipt under the actual caller context.
func TestHAReadCancelKeepsCertifiedReceiptApply(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 25*time.Second)
	defer stop()
	p := newForegroundAPIPeers(t, t.TempDir())
	if _, err := p.servers["n1"].Execute(ctx, ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE read_cancel_probe(id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan context.Context, 1), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	if err := p.materials["n1"].TestInstallReceiptGate(ctx, entered, release); err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "pending", SQL: "INSERT INTO read_cancel_probe VALUES(1)"}})
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := p.cores["n1"].Propose(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(slot) <= p.materials["n1"].Tip() {
		t.Fatalf("slot %d unexpectedly applied", slot)
	}
	if err := p.cores["n1"].EnsureDurableThrough(ctx, slot); err != nil {
		t.Fatalf("certified prefix was not locally durable: %v", err)
	}
	before, ok := p.cores["n1"].CertifiedValue(slot)
	if !ok || len(before.Certificate) == 0 || !bytes.Equal(before.Value, value) {
		t.Fatal("missing exact certified original")
	}
	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := p.servers["n1"].Query(queryCtx, QueryRequest{SQL: "SELECT count(*) FROM read_cancel_probe", Consistency: "linearizable"})
		done <- err
	}()
	var applyCtx context.Context
	select {
	case applyCtx = <-entered:
	case err := <-done:
		t.Fatalf("query returned before receipt gate: %v", err)
	case <-ctx.Done():
		t.Fatal("receipt gate not reached")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled query must return cancellation, got %v", err)
		}
	case <-ctx.Done():
		t.Fatal("query did not return while apply was gated")
	}
	select {
	case <-applyCtx.Done():
		t.Fatal("caller cancellation reached owned apply")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	for {
		receipt, found, err := p.materials["n1"].MutationReceipt(ctx, types.MutationSQL, "pending")
		if err != nil {
			t.Fatalf("owned apply failed: %v", err)
		}
		if found {
			if receipt.Status != types.MutationCommitted || receipt.Slot != uint64(slot) {
				t.Fatalf("receipt=%+v", receipt)
			}
			break
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("owned apply did not complete")
		}
	}
	result, err := p.servers["n1"].Query(ctx, QueryRequest{SQL: "SELECT count(*) FROM read_cancel_probe", Consistency: "linearizable"})
	if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != int64(1) {
		t.Fatalf("linearizable read after owned apply: %+v, %v", result.Rows, err)
	}
	after, ok := p.cores["n1"].CertifiedValue(slot)
	if !ok || !bytes.Equal(before.Value, after.Value) || !bytes.Equal(before.Certificate, after.Certificate) {
		t.Fatal("certified original changed")
	}
}

func TestHAReadOwnedApplyRejectsWALSyncFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	p := newForegroundAPIPeers(t, t.TempDir())
	blocked, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	releaseGate := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseGate)
	restoreEvents := localtesthooks.Set(func(event string) {
		if strings.Contains(event, "network:learned:phase=received:node=n1:") {
			select {
			case blocked <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	})
	t.Cleanup(restoreEvents)
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "wal-failure", SQL: "CREATE TABLE wal_failure_probe(id INTEGER)"}})
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := p.cores["n2"].Propose(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("n1 learned stream was not blocked")
	}
	decision, ok := p.cores["n2"].CertifiedValue(slot)
	if !ok || len(decision.Certificate) == 0 {
		t.Fatal("source decision is not certified")
	}
	if err := p.cores["n1"].AcceptCertifiedHints([]quepaxa.DecidedValue{decision}); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected-wal-sync-failure")
	restore := p.wals[0].TestInjectSyncError(injected)
	defer restore()
	err = p.servers["n1"].applyReadBarrierOwned(ctx, slot)
	if !errors.Is(err, injected) {
		t.Fatalf("expected actual WAL sync boundary failure, got %v", err)
	}
	if got := p.materials["n1"].Tip(); got != 0 {
		t.Fatalf("failed WAL sync applied tip=%d", got)
	}
	if _, found, err := p.materials["n1"].MutationReceipt(ctx, types.MutationSQL, "wal-failure"); err != nil || found {
		t.Fatalf("failed sync receipt found=%t err=%v", found, err)
	}
	releaseGate()
	if err := p.peerServers[0].Close(); err != nil {
		t.Fatal(err)
	}
	if got := p.materials["n1"].Tip(); got != 0 {
		t.Fatalf("canceled peer handler applied tip=%d", got)
	}
}

func TestHAReadOwnedApplyAdmissionBounded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p := newForegroundAPIPeers(t, t.TempDir())
	s := p.servers["n1"]
	for i := 0; i < cap(s.readReplayLimit); i++ {
		s.readReplayLimit <- struct{}{}
	}
	waitCtx, stopWait := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stopWait()
	if err := s.applyReadBarrierOwned(waitCtx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("full admission=%v", err)
	}
	for i := 0; i < cap(s.readReplayLimit); i++ {
		<-s.readReplayLimit
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if err := s.applyReadBarrierOwned(canceled, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled admission=%v", err)
	}
	if len(s.readReplayLimit) != 0 {
		t.Fatalf("admission leaked %d tokens", len(s.readReplayLimit))
	}
}

// Captures a restore race; success on an older snapshot is not a desired result.
func TestHAReadRestoreAfterBarrierCanReturnOlderSnapshot(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 25*time.Second)
	defer stop()
	p := newForegroundAPIPeers(t, t.TempDir())
	s := p.servers["n1"]
	if _, err := s.Execute(ctx, ExecuteRequest{RequestID: "restore-schema", SQL: "CREATE TABLE restore_probe(id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	files, index, cleanup, err := p.materials["n1"].CheckpointFilesAt(ctx)
	if err != nil || index != 1 {
		t.Fatalf("checkpoint index=%d err=%v", index, err)
	}
	defer cleanup()
	write, err := s.Execute(ctx, ExecuteRequest{RequestID: "restore-write", SQL: "INSERT INTO restore_probe VALUES(1)"})
	if err != nil || write.Slot != 2 {
		t.Fatalf("write=%+v err=%v", write, err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseGate)
	restore := localtesthooks.Set(func(event string) {
		if event == "network:read:after-barrier" {
			enterOnce.Do(func() { close(entered) })
			<-release
		}
	})
	t.Cleanup(restore)
	type outcome struct {
		response QueryResponse
		err      error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := s.Query(ctx, QueryRequest{SQL: "SELECT count(*) FROM restore_probe", Consistency: "linearizable"})
		done <- outcome{result, err}
	}()
	select {
	case <-entered:
	case result := <-done:
		t.Fatalf("query exited before gate: %+v", result)
	case <-ctx.Done():
		t.Fatal("query gate not reached")
	}
	if err := p.materials["n1"].RestoreCheckpoint(ctx, files); err != nil {
		t.Fatal(err)
	}
	releaseGate()
	select {
	case result := <-done:
		if result.err != nil || result.response.AppliedSlot != 1 || len(result.response.Rows) != 1 || result.response.Rows[0][0] != int64(0) {
			t.Fatalf("restore race not observed: %+v", result)
		}
	case <-ctx.Done():
		t.Fatal("query failed to exit")
	}
}

func TestHAReadAlreadyAppliedDoesNotUseCatchUpOrReplayCapacity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p := newForegroundAPIPeers(t, t.TempDir())
	s := p.servers["n1"]
	for i := 0; i < cap(s.syncLimit); i++ {
		s.syncLimit <- struct{}{}
	}
	for i := 0; i < cap(s.readReplayLimit); i++ {
		s.readReplayLimit <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(s.syncLimit); i++ {
			<-s.syncLimit
		}
		for i := 0; i < cap(s.readReplayLimit); i++ {
			<-s.readReplayLimit
		}
	}()
	if got := s.material.Tip(); got != 0 {
		t.Fatalf("fixture tip=%d", got)
	}
	if err := s.applyReadBarrierOwned(ctx, 0); err != nil {
		t.Fatalf("already applied fast path: %v", err)
	}
}

func TestHAReadConcurrentAlreadyAppliedDuringWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	p := newForegroundAPIPeers(t, t.TempDir())
	s := p.servers["n1"]
	if _, err := s.Execute(ctx, ExecuteRequest{RequestID: "busy-schema", SQL: "CREATE TABLE busy_probe(id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan context.Context, 1), make(chan struct{})
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseGate)
	if err := p.materials["n1"].TestInstallReceiptGate(ctx, entered, release); err != nil {
		t.Fatal(err)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := s.Execute(ctx, ExecuteRequest{RequestID: "busy-write", SQL: "INSERT INTO busy_probe VALUES(1)"})
		writeDone <- err
	}()
	select {
	case <-entered:
	case err := <-writeDone:
		t.Fatalf("write finished before gate: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waiting := make(chan struct{}, 3)
	restore := localtesthooks.Set(func(event string) {
		if event == "network:read:before-replay-admission" {
			waiting <- struct{}{}
		}
	})
	t.Cleanup(restore)
	results := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func() {
			response, err := s.Query(ctx, QueryRequest{SQL: "SELECT count(*) FROM busy_probe", Consistency: "linearizable"})
			if err == nil && (len(response.Rows) != 1 || response.Rows[0][0] != int64(1)) {
				err = fmt.Errorf("unexpected query result: %+v", response.Rows)
			}
			results <- err
		}()
	}
	for i := 0; i < 3; i++ {
		select {
		case <-waiting:
		case err := <-results:
			t.Fatalf("read returned while writer held materializer lock: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	releaseGate()
	for i := 0; i < 3; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("read %d: %v", i, err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestHAReadOwnedAdmissionShutdownBeforeFastPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	p := newForegroundAPIPeers(t, t.TempDir())
	s := p.servers["n1"]
	if _, err := s.Execute(ctx, ExecuteRequest{RequestID: "shutdown-schema", SQL: "CREATE TABLE admission_probe(id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	receiptEntered, receiptRelease := make(chan context.Context, 1), make(chan struct{})
	var releaseReceiptOnce sync.Once
	releaseReceipt := func() { releaseReceiptOnce.Do(func() { close(receiptRelease) }) }
	t.Cleanup(releaseReceipt)
	if err := p.materials["n1"].TestInstallReceiptGate(ctx, receiptEntered, receiptRelease); err != nil {
		t.Fatal(err)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := s.Execute(ctx, ExecuteRequest{RequestID: "shutdown-write", SQL: "INSERT INTO admission_probe VALUES(1)"})
		writeDone <- err
	}()
	select {
	case <-receiptEntered:
	case err := <-writeDone:
		t.Fatalf("write exited before gate: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	admitted, proceed := make(chan struct{}), make(chan struct{})
	var admittedOnce, proceedOnce sync.Once
	releaseAdmission := func() { proceedOnce.Do(func() { close(proceed) }) }
	t.Cleanup(releaseAdmission)
	restore := localtesthooks.Set(func(event string) {
		if event == "network:read:after-replay-admission" {
			admittedOnce.Do(func() { close(admitted) })
			<-proceed
		}
	})
	t.Cleanup(restore)
	readDone := make(chan error, 1)
	go func() { readDone <- s.applyReadBarrierOwned(ctx, 1) }()
	select {
	case <-admitted:
	case err := <-readDone:
		t.Fatalf("read exited before admission gate: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	releaseReceipt()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if tip := p.materials["n1"].Tip(); tip < 1 {
		t.Fatalf("tip %d is not fast-path eligible", tip)
	}
	s.Close()
	releaseAdmission()
	select {
	case err := <-readDone:
		if !errors.Is(err, ErrNotReady) {
			t.Fatalf("closed server returned %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if len(s.readReplayLimit) != 0 {
		t.Fatal("replay admission leaked")
	}
}

func TestHAReadOwnedApplyStopsAtOwnerDeadline(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	p := newForegroundAPIPeers(t, t.TempDir())
	s := p.servers["n1"]
	owned, cancel := context.WithTimeout(s.proposalCtx, 100*time.Millisecond)
	defer cancel()
	s.proposalCtx = owned
	if err := s.applyReadBarrierOwned(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing prefix deadline=%v", err)
	}
	s.Close()
	if len(s.readReplayLimit) != 0 {
		t.Fatal("deadline leaked worker admission")
	}
	if p.materials["n1"].Tip() != 0 {
		t.Fatal("missing prefix was applied")
	}
}

func TestHAReadOwnedApplyDeadlineWhileWaitingForLock(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	p := newForegroundAPIPeers(t, t.TempDir())
	s := p.servers["n1"]
	owned, cancel := context.WithTimeout(s.proposalCtx, 100*time.Millisecond)
	defer cancel()
	s.proposalCtx = owned
	s.applyMu.Lock()
	var unlock sync.Once
	release := func() { unlock.Do(s.applyMu.Unlock) }
	t.Cleanup(release)
	if err := s.applyReadBarrierOwned(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock-wait deadline=%v", err)
	}
	if len(s.readReplayLimit) != 1 {
		t.Fatalf("worker did not retain its admission: %d", len(s.readReplayLimit))
	}
	closed := make(chan struct{})
	go func() { s.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("shutdown did not join lock-waiting worker")
	default:
	}
	release()
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("shutdown failed to join after lock release")
	}
	if len(s.readReplayLimit) != 0 {
		t.Fatal("worker admission leaked after shutdown")
	}
}

func TestHAReadOwnedApplyShutdownJoinsReceipt(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 25*time.Second)
	defer stop()
	p := newForegroundAPIPeers(t, t.TempDir())
	if _, err := p.servers["n1"].Execute(ctx, ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE shutdown_probe(id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan context.Context, 1), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	if err := p.materials["n1"].TestInstallReceiptGate(ctx, entered, release); err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "pending-shutdown", SQL: "INSERT INTO shutdown_probe VALUES(1)"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.cores["n1"].Propose(ctx, value); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := p.servers["n1"].Query(ctx, QueryRequest{SQL: "SELECT count(*) FROM shutdown_probe", Consistency: "linearizable"})
		done <- err
	}()
	var owned context.Context
	select {
	case owned = <-entered:
	case err := <-done:
		t.Fatalf("query completed before gate: %v", err)
	case <-ctx.Done():
		t.Fatal("gate not reached")
	}
	closed := make(chan struct{})
	go func() { p.servers["n1"].Close(); close(closed) }()
	select {
	case <-owned.Done():
	case <-ctx.Done():
		t.Fatal("shutdown did not cancel owned apply")
	}
	select {
	case <-closed:
		t.Fatal("shutdown returned before gated apply exited")
	default:
	}
	once.Do(func() { close(release) })
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("shutdown did not join apply")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("interrupted query succeeded")
		}
	case <-ctx.Done():
		t.Fatal("query not completed")
	}
	if err := p.materials["n1"].Health(ctx); !errors.Is(err, materializer.ErrGraphCommitUnknown) {
		t.Fatalf("interrupted apply must remain unknown: %v", err)
	}
	if len(p.servers["n1"].readReplayLimit) != 0 {
		t.Fatal("worker admission leaked")
	}
}
