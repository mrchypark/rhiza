//go:build rhiza_local_testhooks

package node

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
)

func TestManagedGraphPublisherReplaySnapshotInterposition(t *testing.T) {
	for _, scenario := range []string{"control-cancel", "graph-cancel", "publisher-cancel", "lifetime-cancel", "node-shutdown"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := newManagedGraphAppendOnlyAdmissionFixture(t)
			fixture.node.managedGraphInitialRootOnce.Do(func() {})
			fixture.transport.releaseAll()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			publishCtx, cancelPublish := context.WithCancel(ctx)
			defer cancelPublish()
			if scenario == "lifetime-cancel" || scenario == "node-shutdown" {
				publishCtx = fixture.node.internalApplication.Load().lifetime
			}
			applyCtx, cancelApply := context.WithCancel(ctx)
			defer cancelApply()
			entered, release := make(chan struct{}), make(chan struct{})
			var once, releaseOnce sync.Once
			var candidate atomic.Bool
			restore := localtesthooks.Set(func(event string) {
				switch event {
				case "node:managed-checkpoint:replay-before-snapshot":
					once.Do(func() { close(entered); <-release })
				case "node:managed-checkpoint:before-candidate":
					candidate.Store(true)
				case "materializer:control:before-sqlite":
					if scenario == "control-cancel" {
						cancelApply()
					}
				case "materializer:graph:after-native-before-sqlite":
					if scenario == "graph-cancel" {
						cancelApply()
					}
				}
			})
			defer restore()
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			done := make(chan error, 1)
			if scenario == "node-shutdown" {
				fixture.node.wg.Add(1)
			}
			go func() {
				if scenario == "node-shutdown" {
					defer fixture.node.wg.Done()
				}
				done <- fixture.node.publishManagedAppendOnlyCheckpoint(publishCtx)
			}()
			select {
			case <-entered:
			case err := <-done:
				t.Fatalf("publisher did not reach interposition: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var shutdownDone chan error
			if scenario == "node-shutdown" {
				shutdownDone = make(chan error, 1)
				go func() { shutdownDone <- fixture.node.Shutdown() }()
				select {
				case <-publishCtx.Done():
				case <-ctx.Done():
					t.Fatal("shutdown did not cancel lifetime", ctx.Err())
				}
			} else if scenario == "lifetime-cancel" {
				fixture.node.stopInternalApplication()
			} else if scenario == "publisher-cancel" {
				cancelPublish()
			} else {
				var value []byte
				if scenario == "control-cancel" {
					value = types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{1})
				} else {
					var err error
					value, err = types.EncodeManagedGraphExecution(types.GraphCommand{RequestID: "interposed", Cypher: `CREATE (:Item {id:'interposed'})`})
					if err != nil {
						t.Fatal(err)
					}
				}
				slot, _, err := fixture.node.core.Propose(ctx, value)
				if err != nil || slot != 9 {
					t.Fatalf("certify interposed slot=%d err=%v", slot, err)
				}
				if err := fixture.node.material.Apply(applyCtx, 9, value); err == nil || !errors.Is(applyCtx.Err(), context.Canceled) {
					t.Fatalf("interposed apply=%v context=%v", err, applyCtx.Err())
				}
				if fixture.node.material.Tip() != 8 {
					t.Fatalf("SQLite tip=%d, want 8", fixture.node.material.Tip())
				}
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case err := <-done:
				switch scenario {
				case "control-cancel":
					if err != nil || !candidate.Load() {
						t.Fatalf("control rollback publisher err=%v candidate=%v", err, candidate.Load())
					}
				case "graph-cancel":
					if err == nil || !strings.Contains(err.Error(), "graph materializer tip") || candidate.Load() {
						t.Fatalf("native-ahead publisher err=%v candidate=%v", err, candidate.Load())
					}
				case "publisher-cancel", "lifetime-cancel", "node-shutdown":
					if !errors.Is(err, context.Canceled) || candidate.Load() {
						t.Fatalf("canceled publisher err=%v candidate=%v", err, candidate.Load())
					}
				}
			case <-ctx.Done():
				t.Fatal("publisher did not release its locks", ctx.Err())
			}
			if !fixture.node.compactionMu.TryLock() {
				t.Fatal("publisher retained compaction lock after return")
			}
			fixture.node.compactionMu.Unlock()
			if scenario == "node-shutdown" {
				select {
				case err := <-shutdownDone:
					if err != nil {
						t.Fatalf("shutdown: %v", err)
					}
				case <-ctx.Done():
					t.Fatal("shutdown did not join publisher", ctx.Err())
				}
			}
			if scenario == "control-cancel" {
				seal, exists, err := fixture.node.core.LatestManagedCheckpointSeal()
				if err != nil || !exists || seal.Index != 8 {
					t.Fatalf("certified previous checkpoint seal=%+v exists=%v err=%v", seal, exists, err)
				}
			}
			if scenario != "control-cancel" && scenario != "node-shutdown" {
				if _, exists, err := fixture.node.core.LatestManagedCheckpointSeal(); err != nil || exists {
					t.Fatalf("failed publisher seal exists=%v err=%v", exists, err)
				}
			}
			if scenario == "graph-cancel" {
				if _, found, err := fixture.node.material.GraphMutationReceipt(ctx, "interposed"); err == nil || found {
					t.Fatalf("native-ahead exposed receipt: found=%v err=%v", found, err)
				}
				if err := fixture.node.replayLocalDecisions(ctx); err != nil {
					t.Fatalf("certified replay: %v", err)
				}
				receipt, found, err := fixture.node.material.GraphMutationReceipt(ctx, "interposed")
				if err != nil || !found || receipt.Slot != 9 {
					t.Fatalf("replayed receipt=%+v found=%v err=%v", receipt, found, err)
				}
				rows, err := fixture.node.material.GraphQuery(ctx, `MATCH (n:Item {id:'interposed'}) RETURN n.id`, nil)
				if err != nil || len(rows.Rows) != 1 {
					t.Fatalf("replayed rows=%v err=%v", rows.Rows, err)
				}
			}
		})
	}
}
