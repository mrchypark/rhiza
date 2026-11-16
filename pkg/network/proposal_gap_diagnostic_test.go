//go:build rhiza_latency_diag && rhiza_local_testhooks

package network

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/latencydiag"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type proposalGapResult struct {
	slot quepaxa.Slot
	err  error
}

func TestProposalGapTagCrossesBatchAndWorkerBoundaries(t *testing.T) {
	hold := latencydiag.HoldTagTestFlush()
	defer hold()

	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil)
	defer server.Close()

	ctx := latencydiag.WithTag(context.Background(), 16)
	if _, err := server.KVPut(ctx, KVMutationRequest{RequestID: "proposal-gap-tag", Key: "key", Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}

	events, drops, preEnqueueDrops := latencydiag.TagTestSnapshot()
	if drops != 0 || preEnqueueDrops != 0 {
		t.Fatalf("drops=%d pre_enqueue_drops=%d", drops, preEnqueueDrops)
	}
	for _, stage := range []string{latencydiag.StageProposalMuWait, latencydiag.StageProposalWorkerStart} {
		if event := requireProposalGapEvent(t, events, 16, stage); event.Outcome != "ok" {
			t.Fatalf("stage=%s event=%+v", stage, event)
		}
	}
}

func TestProposalGapPermitWaitSuccessAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cancel     bool
		fillLocal  bool
		fillOps    bool
		wantCode   uint64
		wantResult string
	}{
		{name: "local-cap", fillLocal: true, wantCode: latencydiag.ProposalWaitLocalCap, wantResult: "ok"},
		{name: "operation-cap", fillOps: true, wantCode: latencydiag.ProposalWaitOperationCap, wantResult: "ok"},
		{name: "cancelled-operation-cap", cancel: true, fillOps: true, wantCode: latencydiag.ProposalWaitOperationCap, wantResult: "cancelled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hold := latencydiag.HoldTagTestFlush()
			defer hold()
			core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
			material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
			if err != nil {
				t.Fatal(err)
			}
			defer material.Close()
			server := NewServer(core, material, "cluster", true, nil)
			defer server.Close()

			server.proposeMu.Lock()
			if tc.fillLocal {
				for range cap(server.localCap) {
					server.localCap <- struct{}{}
				}
			}
			if tc.fillOps {
				for range cap(server.operationCap) {
					server.operationCap <- struct{}{}
				}
			}
			server.proposeMu.Unlock()
			defer func() {
				server.proposeMu.Lock()
				if tc.fillLocal {
					for len(server.localCap) > 0 {
						<-server.localCap
					}
				}
				if tc.fillOps {
					for len(server.operationCap) > 0 {
						<-server.operationCap
					}
				}
				server.proposeMu.Unlock()
			}()

			ctx, cancel := context.WithTimeout(latencydiag.WithTag(context.Background(), 16), 10*time.Second)
			defer cancel()
			resultCh := make(chan proposalGapResult, 1)
			go func() {
				slot, err := server.proposeBatch(ctx, types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{201}), 2*time.Second, 5*time.Second)
				resultCh <- proposalGapResult{slot: slot, err: err}
			}()
			waitForProposalGapAdmission(t, ctx, server, resultCh)

			if tc.cancel {
				cancel()
			} else {
				server.proposeMu.Lock()
				if tc.fillLocal {
					<-server.localCap
				}
				if tc.fillOps {
					<-server.operationCap
				}
				server.signalAdmissionLocked(false)
				server.proposeMu.Unlock()
			}

			var got proposalGapResult
			select {
			case got = <-resultCh:
			case <-time.After(5 * time.Second):
				t.Fatal("proposal did not return")
			}
			if tc.cancel {
				if got.slot != 0 || !errors.Is(got.err, context.Canceled) {
					t.Fatalf("cancelled result=%+v", got)
				}
			} else if got.err != nil || got.slot == 0 {
				t.Fatalf("successful result=%+v", got)
			}

			events, drops, _ := latencydiag.TagTestSnapshot()
			if drops != 0 {
				t.Fatalf("drops=%d", drops)
			}
			event := requireProposalGapEvent(t, events, 16, latencydiag.StageProposalPermitWait)
			if event.Shard != tc.wantCode || event.Outcome != tc.wantResult {
				t.Fatalf("permit event=%+v want code=%d outcome=%s", event, tc.wantCode, tc.wantResult)
			}
		})
	}
}

func waitForProposalGapAdmission(t *testing.T, ctx context.Context, server *Server, result <-chan proposalGapResult) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		server.proposeMu.Lock()
		waiters := server.admissionWaiters
		server.proposeMu.Unlock()
		if waiters == 1 {
			return
		}
		select {
		case got := <-result:
			t.Fatalf("proposal returned before permit wait: %+v", got)
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("proposal did not enter permit wait: %v", ctx.Err())
		}
	}
}

func requireProposalGapEvent(t *testing.T, events []latencydiag.TagTestEvent, tag uint64, stage string) latencydiag.TagTestEvent {
	t.Helper()
	for _, event := range events {
		if event.Tag == tag && event.Stage == stage {
			return event
		}
	}
	t.Fatalf("missing tag=%d stage=%s in %+v", tag, stage, events)
	return latencydiag.TagTestEvent{}
}
