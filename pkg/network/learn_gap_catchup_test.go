//go:build rhiza_local_testhooks

package network

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestLearnGapBlocksQuorumUntilCatchUpInstallsPrefix(t *testing.T) {
	cluster := newForegroundAPIPeers(t, t.TempDir())
	const lower = quepaxa.Slot(1)
	const gap = quepaxa.Slot(2)
	const higher = quepaxa.Slot(3)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	value := func(id string) []byte {
		t.Helper()
		item, err := types.EncodeKVBatchItem(types.KVCommand{
			RequestID: id, Operation: "put", Key: id, Value: []byte(id), ObservedAtUnixMS: time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatal(err)
		}
		return types.AssembleKVBatch([][]byte{item})
	}
	wrapRestore := func(restore func()) func() {
		var once sync.Once
		return func() { once.Do(restore) }
	}
	dropLearned := func(slot quepaxa.Slot, nodes ...string) (<-chan string, <-chan string, func(), func()) {
		dropped := make(chan string, len(nodes))
		stopped := make(chan string, len(nodes))
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseHandlers := func() { releaseOnce.Do(func() { close(release) }) }
		restore := localtesthooks.Set(func(event string) {
			if !strings.Contains(event, "network:learned:phase=received:") || !strings.Contains(event, fmt.Sprintf(":slot=%d", slot)) {
				return
			}
			for _, node := range nodes {
				if strings.Contains(event, "node="+node+":") {
					dropped <- event
					<-release // Hold this real handler until the sender's outcome is known.
					stopped <- event
					runtime.Goexit() // Drop before Core.AcceptDecision, after explicit observation.
				}
			}
		})
		return dropped, stopped, releaseHandlers, wrapRestore(restore)
	}
	waitDrops := func(ch <-chan string, count int) {
		t.Helper()
		for i := 0; i < count; i++ {
			select {
			case <-ch:
			case <-ctx.Done():
				t.Fatalf("received %d/%d injected learned drops: %v", i, count, ctx.Err())
			}
		}
	}
	catchUp := func(node string, through quepaxa.Slot) error {
		return cluster.servers[quepaxa.NodeID(node)].catchUpFrom(ctx, "n1", through, true)
	}
	assertTips := func(node string, want quepaxa.Slot) {
		t.Helper()
		if got := cluster.cores[quepaxa.NodeID(node)].Tip(); got != want {
			t.Fatalf("%s core tip=%d, want %d", node, got, want)
		}
		if got := cluster.materials[quepaxa.NodeID(node)].Tip(); got != uint64(want) {
			t.Fatalf("%s materializer tip=%d, want %d", node, got, want)
		}
	}

	// Control: one laggard misses a lower decision, but the other remote ACK
	// completes quorum. This establishes that a single gap is not a quorum stall.
	drops, stopped, release, restore := dropLearned(lower, "n3")
	defer func() { release(); restore() }()
	controlCtx, cancelControl := context.WithTimeout(ctx, 2*time.Second)
	controlDone := make(chan error, 1)
	go func() {
		_, err := cluster.servers["n1"].proposeOnce(controlCtx, value("gap-single-laggard-control"))
		controlDone <- err
	}()
	waitDrops(drops, 1)
	controlErr := <-controlDone
	release()
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatalf("single laggard handler did not stop: %v", ctx.Err())
	}
	cancelControl()
	restore()
	if controlErr != nil {
		t.Fatalf("single-laggard quorum control: %v", controlErr)
	}
	if cluster.cores["n3"].Tip() != 0 || cluster.cores["n3"].IsDecided(lower) {
		t.Fatalf("control laggard unexpectedly learned slot %d: tip=%d", lower, cluster.cores["n3"].Tip())
	}
	if err := catchUp("n3", lower); err != nil {
		t.Fatalf("repair control laggard: %v", err)
	}
	for _, node := range []string{"n1", "n2", "n3"} {
		if err := cluster.servers[quepaxa.NodeID(node)].applyDecisions(ctx, lower); err != nil {
			t.Fatalf("apply control decision at %s: %v", node, err)
		}
		assertTips(node, lower)
	}

	// A second locally decided value whose learned RPC is dropped by both
	// receivers leaves a real certified prefix gap. Keep this failure visible;
	// do not turn the lower delivery into a success or retry it.
	drops, stopped, releaseLower, restoreLower := dropLearned(gap, "n2", "n3")
	defer func() { releaseLower(); restoreLower() }()
	failureCtx, cancelFailure := context.WithTimeout(ctx, 300*time.Millisecond)
	failureDone := make(chan error, 1)
	go func() {
		_, err := cluster.servers["n1"].proposeOnce(failureCtx, value("gap-both-laggards"))
		failureDone <- err
	}()
	waitDrops(drops, 2)
	lowerErr := <-failureDone
	cancelFailure()
	if lowerErr == nil || (!errors.Is(lowerErr, context.DeadlineExceeded) && !errors.Is(lowerErr, quepaxa.ErrQuorumUnavailable)) {
		t.Fatalf("uncertified learned delivery error=%v, want bounded quorum failure", lowerErr)
	}
	if !cluster.cores["n1"].IsDecided(gap) {
		t.Fatalf("source did not locally certify gap slot %d after failed delivery", gap)
	}
	for _, node := range []string{"n2", "n3"} {
		if cluster.cores[quepaxa.NodeID(node)].Tip() != lower || cluster.cores[quepaxa.NodeID(node)].IsDecided(gap) {
			t.Fatalf("%s did not retain the expected prefix gap: tip=%d decided=%t", node, cluster.cores[quepaxa.NodeID(node)].Tip(), cluster.cores[quepaxa.NodeID(node)].IsDecided(gap))
		}
	}
	// Restore the lower hook before installing the higher-slot observer. The
	// already-blocked handlers retain the callback closure until released.
	restoreLower()

	started := make(chan string, 2)
	events := make(chan string, 24)
	restoreHigher := wrapRestore(localtesthooks.Set(func(event string) {
		if strings.Contains(event, ":slot=3") && (strings.Contains(event, "node=n2:") || strings.Contains(event, "node=n3:")) {
			if strings.Contains(event, "phase=apply-start") {
				started <- event // Observe the existing apply boundary without holding it.
			} else if strings.Contains(event, "phase=apply-complete") || strings.Contains(event, "phase=response-written") {
				events <- event
			}
		}
	}))
	defer restoreHigher()
	proposalDone := make(chan error, 1)
	go func() {
		_, err := cluster.servers["n1"].proposeOnce(ctx, value("gap-higher"))
		proposalDone <- err
	}()
	startedNodes := map[string]bool{}
	for len(startedNodes) != 2 {
		select {
		case event := <-started:
			if strings.Contains(event, "node=n2:") {
				startedNodes["n2"] = true
			} else if strings.Contains(event, "node=n3:") {
				startedNodes["n3"] = true
			}
		case err := <-proposalDone:
			t.Fatalf("higher proposal returned before both lagging handlers reached apply: %v", err)
		case <-ctx.Done():
			t.Fatalf("higher handlers did not reach apply: %v", ctx.Err())
		}
	}
	for _, node := range []string{"n2", "n3"} {
		core := cluster.cores[quepaxa.NodeID(node)]
		if core.Tip() != lower || !core.IsDecided(higher) || core.IsDecided(gap) {
			t.Fatalf("%s expected higher decision blocked behind gap: tip=%d gap=%t higher=%t", node, core.Tip(), core.IsDecided(gap), core.IsDecided(higher))
		}
	}
	select {
	case err := <-proposalDone:
		t.Fatalf("higher SendDecision returned before prefix repair: %v", err)
	default:
	}

	// Repair both peers concurrently so ordered application and ACK can proceed.
	// apply-start does not prove entry into WaitTip; ordinary KV learned requests
	// do not use the reconfiguration control-only automatic catch-up branch.
	repairErr := make(chan error, 2)
	for _, node := range []string{"n2", "n3"} {
		go func(node string) { repairErr <- catchUp(node, higher) }(node)
	}
	for range 2 {
		select {
		case err := <-repairErr:
			if err != nil {
				t.Fatalf("concurrent prefix catch-up: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("prefix catch-up did not complete: %v", ctx.Err())
		}
	}
	for _, node := range []string{"n2", "n3"} {
		core := cluster.cores[quepaxa.NodeID(node)]
		if core.Tip() != higher || !core.IsDecided(gap) || !core.IsDecided(higher) {
			t.Fatalf("%s catch-up did not install the missing prefix: tip=%d gap=%t higher=%t", node, core.Tip(), core.IsDecided(gap), core.IsDecided(higher))
		}
	}
	select {
	case err := <-proposalDone:
		if err != nil {
			t.Fatalf("higher quorum after catch-up: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("higher quorum remained blocked after apply release: %v", ctx.Err())
	}

	applyComplete := map[string]bool{}
	responseWritten := map[string]bool{}
	for len(responseWritten) == 0 {
		select {
		case event := <-events:
			if strings.Contains(event, "phase=apply-complete") {
				if strings.Contains(event, "node=n2:") {
					applyComplete["n2"] = true
				}
				if strings.Contains(event, "node=n3:") {
					applyComplete["n3"] = true
				}
			}
			if strings.Contains(event, "phase=response-written") {
				if strings.Contains(event, "node=n2:") {
					responseWritten["n2"] = true
				}
				if strings.Contains(event, "node=n3:") {
					responseWritten["n3"] = true
				}
			}
		case <-ctx.Done():
			t.Fatalf("missing apply/ACK evidence: apply=%v response=%v", applyComplete, responseWritten)
		}
	}
	// SendDecision needs one remote ACK after local vote; quorum cancellation can
	// prevent the other response from being written. Any observed ACK must follow
	// that peer's apply-complete event; ACK-before-apply is never accepted.
	for node := range responseWritten {
		if !applyComplete[node] {
			t.Fatalf("%s wrote learned ACK before apply completion", node)
		}
		if got := cluster.materials[quepaxa.NodeID(node)].Tip(); got != uint64(higher) {
			t.Fatalf("ACKing peer %s materializer tip=%d, want %d", node, got, higher)
		}
	}
	for node := range applyComplete {
		if got := cluster.materials[quepaxa.NodeID(node)].Tip(); got != uint64(higher) {
			t.Fatalf("peer %s reported apply-complete with materializer tip=%d, want %d", node, got, higher)
		}
	}
	if len(responseWritten) == 0 {
		t.Fatal("no post-apply learned ACK was observed")
	}
	if err := cluster.servers["n1"].applyDecisions(ctx, higher); err != nil {
		t.Fatalf("apply source decisions: %v", err)
	}
	assertTips("n1", higher)
	releaseLower()
	for range 2 {
		select {
		case <-stopped:
		case <-ctx.Done():
			t.Fatalf("lower learned handler did not stop after catch-up: %v", ctx.Err())
		}
	}
}
