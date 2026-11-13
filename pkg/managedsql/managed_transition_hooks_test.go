//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
)

func TestMembershipTransitionWaitDoesNotHoldHostMutex(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	restore := localtesthooks.Set(func(event string) {
		if event == "managedsql:membership-transition:before-node-wait" {
			close(entered)
			<-release
		}
	})
	defer restore()
	h := &Host{node: &node.Node{}}
	done := make(chan error, 1)
	go func() { done <- h.waitForMembershipTransition(context.Background()) }()
	<-entered
	if !h.mu.TryLock() {
		t.Fatal("membership wait retained Host.mu")
	}
	h.mu.Unlock()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestHostExecuteAndLinearizableQueryRetryAfterReconfigurationDrain(t *testing.T) {
	runDrainRace := func(
		t *testing.T,
		waitHostHook bool,
		op func(context.Context, *Host) error,
	) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		configs, fixture, hosts := managedLeaveFixture(t)

		initAck, err := hosts[0].Execute(ctx, ExecuteRequest{
			RequestID: "drain-retry-init",
			SQL:       "CREATE TABLE drain_retry(id INTEGER PRIMARY KEY)",
		})
		if err != nil {
			t.Fatalf("init table execute: %v", err)
		}
		waitSQLCertifiedApplication(t, ctx, hosts[1], initAck.Slot)
		// Ensure the non-retained voter already completed initial admission so its later
		// check exercises live transition fail-closed behavior directly.
		if _, err := hosts[3].Query(ctx, QueryRequest{SQL: "SELECT id FROM drain_retry", Consistency: "linearizable"}); err != nil {
			t.Fatalf("voter 3 warm linearizable query: %v", err)
		}

		var holdDecision atomic.Bool
		holdDecision.Store(true)
		decisionHeld := make(chan struct{})
		decisionRelease := make(chan struct{})
		var decisionOnce, decisionReleased sync.Once
		releaseDecision := func() {
			holdDecision.Store(false)
			decisionReleased.Do(func() { close(decisionRelease) })
		}
		defer releaseDecision()

		var holdTerminal atomic.Bool
		terminalDispatch := make(chan struct{})
		terminalRelease := make(chan struct{})
		var terminalOnce, terminalReleased sync.Once
		releaseTerminal := func() {
			holdTerminal.Store(false)
			terminalReleased.Do(func() { close(terminalRelease) })
		}
		defer releaseTerminal()

		var watchFreeze atomic.Bool
		var freezeReceived atomic.Bool
		freezeApplied := make(chan struct{})
		var freezeAppliedOnce sync.Once

		var watchRetryWait atomic.Bool
		retryWaitEntered := make(chan struct{})
		var retryWaitOnce sync.Once

		node0ID := string(configs[0].Runtime.NodeID)
		restoreEvents := localtesthooks.Set(func(event string) {
			switch {
			case strings.HasPrefix(event, "network:learned:phase=accept-start:node="+node0ID+":") && holdDecision.Load():
				decisionOnce.Do(func() { close(decisionHeld) })
				select {
				case <-decisionRelease:
				case <-ctx.Done():
				}
			case watchFreeze.Load() && strings.HasPrefix(event, "network:learned-control:phase=received:node="+node0ID+":"):
				freezeReceived.Store(true)
			case watchFreeze.Load() && freezeReceived.Load() && strings.HasPrefix(event, "network:learned:phase=apply-complete:node="+node0ID+":"):
				freezeAppliedOnce.Do(func() { close(freezeApplied) })
			case strings.HasPrefix(event, "network:terminal-decision:") && strings.HasSuffix(event, ":phase=dispatch") && holdTerminal.Load():
				terminalOnce.Do(func() { close(terminalDispatch) })
				select {
				case <-terminalRelease:
				case <-ctx.Done():
				}
			case event == "managedsql:membership-transition:before-node-wait" && watchRetryWait.Load():
				retryWaitOnce.Do(func() { close(retryWaitEntered) })
			}
		})
		defer restoreEvents()

		// Ensure hosts[0] has RecorderTip > CertifiedTip from a real peer mutation.
		var seedSlot uint64
		for attempt := 0; ; attempt++ {
			req := ExecuteRequest{
				RequestID: fmt.Sprintf("drain-seed-%d", attempt),
				SQL:       fmt.Sprintf("INSERT INTO drain_retry VALUES (%d)", attempt+1),
			}
			ack, err := hosts[1].Execute(ctx, req)
			if err != nil {
				t.Fatalf("seed write: %v", err)
			}
			state, err := hosts[0].node.ManagedState()
			if err != nil {
				t.Fatalf("host0 state: %v", err)
			}
			if state.RecorderTip > state.CertifiedTip {
				seedSlot = ack.Slot
				break
			}
			if attempt >= 5 {
				t.Fatalf("could not create RecorderTip > CertifiedTip on host0 (slot=%d state=%+v)", ack.Slot, state)
			}
		}
		select {
		case <-decisionHeld:
		case <-ctx.Done():
			t.Fatal("did not observe accept-start hold on host0")
		}

		leaveErrCh := make(chan error, 1)
		var proposalOnce sync.Once
		var proposalErr atomic.Value
		proposalDone := make(chan struct{})
		var proposalDoneOnce sync.Once
		restoreProposal := localtesthooks.SetProposalEntry(func(value []byte) {
			if barrier, err := types.DecodeReadBarrier(value); err != nil || !barrier {
				return
			}
			proposalOnce.Do(func() {
				defer proposalDoneOnce.Do(func() { close(proposalDone) })
				releaseDecision()
				waitSQLCertifiedApplication(t, ctx, hosts[0], seedSlot)
				setManagedTarget(t, fixture, 3)
				holdTerminal.Store(true)
				watchFreeze.Store(true)
				go func() {
					leaveErrCh <- hosts[1].LeaveVoter(ctx, LeaveVoterRequest{NodeID: configs[3].Runtime.NodeID})
				}()
				select {
				case <-freezeApplied:
					// Enable retry wait observation just before returning to trigger ErrReconfigurationDrain.
					watchRetryWait.Store(true)
				case <-ctx.Done():
					proposalErr.Store(errors.New("timed out waiting for freeze apply on host0"))
				}
			})
		})
		defer restoreProposal()

		opErrCh := make(chan error, 1)
		go func() { opErrCh <- op(ctx, hosts[0]) }()

		if waitHostHook {
			select {
			case <-retryWaitEntered:
			case err := <-opErrCh:
				t.Fatalf("finished before entering transition retry wait: %v", err)
			case <-ctx.Done():
				t.Fatal("did not enter transition retry wait")
			}
		} else {
			select {
			case <-proposalDone:
			case <-ctx.Done():
				t.Fatal("did not hit proposal entry")
			}
			select {
			case err := <-opErrCh:
				t.Fatalf("finished without retrying during frozen transition: %v", err)
			case <-time.After(150 * time.Millisecond):
			}
		}

		// Verify Host.mu is NOT held while waiting in waitForMembershipTransition on retry.
		if !hosts[0].mu.TryLock() {
			t.Fatal("retained Host.mu during retry waitForMembershipTransition")
		}
		hosts[0].mu.Unlock()

		if stored := proposalErr.Load(); stored != nil {
			t.Fatalf("proposal hook failed: %v", stored)
		}

		// Non-retained voter must fail closed with ErrNotReady during the frozen transition.
		if _, err := hosts[3].Execute(ctx, ExecuteRequest{
			RequestID: "non-retained-write",
			SQL:       "INSERT INTO drain_retry VALUES (999)",
		}); !errors.Is(err, network.ErrNotReady) {
			t.Fatalf("non-retained Execute err=%v, want ErrNotReady", err)
		}
		if _, err := hosts[3].Query(ctx, QueryRequest{
			SQL:         "SELECT id FROM drain_retry",
			Consistency: "linearizable",
		}); !errors.Is(err, network.ErrNotReady) {
			t.Fatalf("non-retained linearizable Query err=%v, want ErrNotReady", err)
		}

		releaseTerminal()
		if err := <-leaveErrCh; err != nil {
			t.Fatalf("LeaveVoter: %v", err)
		}
		if err := <-opErrCh; err != nil {
			t.Fatalf("operation failed after transition unfreeze: %v", err)
		}
	}

	t.Run("Execute", func(t *testing.T) {
		runDrainRace(t, true, func(ctx context.Context, h *Host) error {
			ack, err := h.Execute(ctx, ExecuteRequest{
				RequestID: "drain-retry-write-2",
				SQL:       "INSERT INTO drain_retry VALUES (100)",
			})
			if err != nil {
				return err
			}
			if ack.Status != types.MutationCommitted {
				return fmt.Errorf("unexpected execute status: %+v", ack)
			}
			return nil
		})
	})

	t.Run("LinearizableQuery", func(t *testing.T) {
		runDrainRace(t, false, func(ctx context.Context, h *Host) error {
			resp, err := h.Query(ctx, QueryRequest{
				SQL:         "SELECT id FROM drain_retry ORDER BY id",
				Consistency: "linearizable",
			})
			if err != nil {
				return err
			}
			if len(resp.Rows) < 1 {
				return fmt.Errorf("unexpected rows after query retry: %+v", resp.Rows)
			}
			return nil
		})
	})
}

func TestHostReadyWaitsForStartupCatchUp(t *testing.T) {
	for _, protocol := range []string{
		sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol,
		sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol,
	} {
		t.Run(protocol, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			configs, fixture := sqlClusterForProtocol(t, protocol)
			hosts := make([]*Host, len(configs))
			for i := 1; i < len(configs); i++ {
				h, err := fixture.open(ctx, configs[i])
				if h != nil {
					hosts[i] = h
					t.Cleanup(func() { _ = fixture.closeHost(h) })
				}
				if err != nil {
					t.Fatalf("open voter %d: %v", i, err)
				}
			}
			for i := 1; i < len(configs); i++ {
				awaitPublicInternalReady(t, ctx, hosts[i])
			}

			blocked, observed, release, ok := holdReplacementBeforeReady(t, ctx, configs[0].Runtime.NodeID)
			if !ok {
				t.Fatal("missing startup catch-up test hook")
			}
			h0, err := fixture.open(ctx, configs[0])
			if h0 != nil {
				hosts[0] = h0
				t.Cleanup(func() { _ = fixture.closeHost(h0) })
			}
			if err != nil {
				t.Fatalf("open voter 0: %v", err)
			}

			// Trigger initial admission so startCatchUp runs and pauses before n.ready.Store(true).
			if err := h0.Ready(ctx); !errors.Is(err, network.ErrNotReady) {
				t.Fatalf("Host.Ready before catch-up err=%v, want ErrNotReady", err)
			}
			select {
			case <-blocked:
			case <-ctx.Done():
				t.Fatal("voter 0 did not reach startup catch-up before-ready gate")
			}
			if err := h0.Ready(ctx); !errors.Is(err, network.ErrNotReady) {
				t.Fatalf("Host.Ready while n.ready==false err=%v, want ErrNotReady", err)
			}

			release()
			select {
			case <-observed:
			case <-ctx.Done():
				t.Fatal("voter 0 did not reach after-ready after catch-up release")
			}
			awaitPublicInternalReady(t, ctx, h0)

			if protocol == sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol {
				if _, err := h0.GraphExecute(ctx, types.GraphCommand{
					RequestID: "after-ready-graph-write",
					Cypher:    "CREATE (:AfterReady {ok: true})",
				}); err != nil {
					t.Fatalf("GraphExecute immediately after Host.Ready failed: %v", err)
				}
			} else {
				if _, err := h0.Execute(ctx, ExecuteRequest{
					RequestID: "after-ready-sql-write",
					SQL:       "CREATE TABLE after_ready(id INTEGER PRIMARY KEY)",
				}); err != nil {
					t.Fatalf("Execute immediately after Host.Ready failed: %v", err)
				}
			}
		})
	}
}
