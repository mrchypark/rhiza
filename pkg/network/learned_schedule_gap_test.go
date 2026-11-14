//go:build rhiza_local_testhooks

package network

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestLearnedRepairsMissingSchedule96To114(t *testing.T) {
	cluster := newForegroundAPIPeers(t, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	for n := 0; cluster.cores["n1"].Tip() < 114; n++ {
		id := fmt.Sprintf("schedule-gap-%d", n)
		item, err := types.EncodeKVBatchItem(types.KVCommand{RequestID: id, Operation: "put", Key: id, Value: []byte(id), ObservedAtUnixMS: time.Now().UnixMilli()})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cluster.servers["n1"].proposeOnce(ctx, types.AssembleKVBatch([][]byte{item})); err != nil {
			t.Fatalf("source at tip %d: %v", cluster.cores["n1"].Tip(), err)
		}
	}
	values, _, err := cluster.cores["n1"].DecisionsFrom(1, 128)
	if err != nil || len(values) < 114 {
		t.Fatalf("source decisions=%d err=%v", len(values), err)
	}
	var actual quepaxa.Decision
	if err := json.Unmarshal(values[113].Certificate, &actual); err != nil {
		t.Fatal(err)
	}
	actual.Proposal.Value = values[113].Value
	if actual.Step != 4 {
		t.Fatalf("source slot 114 did not produce real fast certificate: step=%d", actual.Step)
	}
	order, err := cluster.cores["n1"].LeaderOrder(114)
	if err != nil {
		t.Fatal(err)
	}
	if len(order) == 0 || actual.Proposal.ProposerID != order[0] {
		t.Fatal("source decision does not match certified leader schedule")
	}
	for _, mode := range []string{"valid", "wrong-proposer", "invalid-certificate", "competing-sync-failure"} {
		t.Run(mode, func(t *testing.T) {
			wal, err := qlog.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer wal.Close()
			core, err := quepaxa.New(quepaxa.Config{NodeID: "n2", Cluster: cluster.config, WAL: wal, Transport: cluster.transports[1]})
			if err != nil {
				t.Fatal(err)
			}
			if err := core.AcceptCertifiedValues(values[:96]); err != nil {
				t.Fatalf("seed certified prefix: %v", err)
			}
			mat, err := materializer.Open(t.TempDir()+"/state.sqlite", 4)
			if err != nil {
				t.Fatal(err)
			}
			defer mat.Close()
			server := NewServer(core, mat, "foreground-api", true, cluster.transports[1])
			defer server.Close()
			if err := server.applyDecisions(ctx, 96); err != nil {
				t.Fatalf("apply initial certified prefix: %v", err)
			}
			if _, err := core.LeaderOrder(114); !errors.Is(err, quepaxa.ErrLeaderScheduleUnavailable) {
				t.Fatalf("expected missing schedule, got %v", err)
			}
			decision := actual
			decision.Summaries = append([]quepaxa.Summary(nil), actual.Summaries...)
			if mode == "wrong-proposer" {
				for _, member := range cluster.config.Members {
					if member.ID != order[0] {
						decision.Proposal.ProposerID = member.ID
						break
					}
				}
				for i := range decision.Summaries {
					p := decision.Proposal
					decision.Summaries[i].FirstCurrent = &p
				}
			} else if mode == "invalid-certificate" {
				decision.Summaries[0].FirstCurrent = nil
			}
			if err := core.AcceptDecisionHint(decision); mode == "invalid-certificate" && errors.Is(err, quepaxa.ErrLeaderScheduleUnavailable) {
				t.Fatal("bad certificate misclassified as missing schedule")
			} else if mode != "invalid-certificate" && !errors.Is(err, quepaxa.ErrLeaderScheduleUnavailable) {
				t.Fatalf("expected unavailable schedule before repair: %v", err)
			}
			if mode == "competing-sync-failure" {
				entered, handlerStarted := make(chan struct{}, 1), make(chan struct{}, 1)
				release := make(chan struct{})
				var once sync.Once
				defer once.Do(func() { close(release) })
				restore := localtesthooks.Set(func(event string) {
					if event == "core:certified-values:before-sync:node=n2" {
						select {
						case entered <- struct{}{}:
						default:
						}
						<-release
					}
					if strings.Contains(event, "network:learned:phase=accept-start:node=n2:") && strings.Contains(event, ":slot=114") {
						select {
						case handlerStarted <- struct{}{}:
						default:
						}
					}
				})
				defer restore()
				other := make(chan error, 1)
				go func() { err, _ := server.catchUpLearnedPrefix(ctx, "n1", 113); other <- err }()
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("competing import not entered")
				}
				request := cluster.transports[0].request(peerfb.OperationLearned)
				request.Decision = decisionToWire(decision)
				peer := &PeerServer{server: server}
				response := make(chan error, 1)
				go func() { _, err := peer.handle(ctx, cluster.config.Members[0].PublicKey[:], request); response <- err }()
				select {
				case <-handlerStarted:
				case <-ctx.Done():
					t.Fatal("handler did not attempt prefix")
				}
				select {
				case err := <-response:
					t.Fatalf("premature response: %v", err)
				default:
				}
				if err := wal.Close(); err != nil {
					t.Fatal(err)
				}
				once.Do(func() { close(release) })
				select {
				case err := <-other:
					if err == nil {
						t.Fatal("competing sync unexpectedly passed")
					}
				case <-ctx.Done():
					t.Fatal("competing import leaked")
				}
				select {
				case err := <-response:
					if err == nil {
						t.Fatal("handler ACK after sync failure")
					}
				case <-ctx.Done():
					t.Fatal("handler leaked")
				}
				if mat.Tip() != 96 {
					t.Fatalf("applied after sync failure: %d", mat.Tip())
				}
				return
			}
			var fetches atomic.Int32
			restore := localtesthooks.Set(func(event string) {
				if event == "network:learned:prefix-page-fetched:node=n2:from=97:through=113" {
					fetches.Add(1)
				}
			})
			defer restore()
			request := cluster.transports[0].request(peerfb.OperationLearned)
			request.Decision = decisionToWire(decision)
			peer := &PeerServer{server: server}
			_, err = peer.handle(ctx, cluster.config.Members[0].PublicKey[:], request)
			if mode == "valid" {
				if err != nil {
					t.Fatalf("valid decision after prefix repair: %v", err)
				}
				if core.Tip() != 114 || mat.Tip() != 114 {
					t.Fatalf("tip=%d applied=%d", core.Tip(), mat.Tip())
				}
				if fetches.Load() == 0 {
					t.Fatal("no certified prefix fetch")
				}
			} else {
				if err == nil {
					t.Fatal("invalid decision acknowledged")
				}
				if core.IsDecided(114) {
					t.Fatal("invalid decision installed")
				}
				if mode == "invalid-certificate" && fetches.Load() != 0 {
					t.Fatal("invalid certificate triggered prefix fetch")
				}
			}
		})
	}
}
