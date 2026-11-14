//go:build rhiza_local_testhooks

package network

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestControlLearnedRepairsScheduleAndWaitsForDurability(t *testing.T) {
	cluster := newForegroundAPIPeers(t, t.TempDir(), true)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	for i := 0; cluster.cores["n1"].Tip() < 113; i++ {
		id := fmt.Sprintf("control-handler-%d", i)
		item, err := types.EncodeKVBatchItem(types.KVCommand{RequestID: id, Operation: "put", Key: id, Value: []byte(id), ObservedAtUnixMS: time.Now().UnixMilli()})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cluster.servers["n1"].proposeOnce(ctx, types.AssembleKVBatch([][]byte{item})); err != nil {
			t.Fatalf("source tip=%d: %v", cluster.cores["n1"].Tip(), err)
		}
	}
	target := cluster.config
	target.ConfigID++
	target.Members = []quepaxa.Member{cluster.config.Members[0], cluster.config.Members[2]}
	slot, err := cluster.cores["n1"].BeginReconfiguration(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if slot <= 113 {
		t.Fatalf("control slot=%d did not cross schedule boundary", slot)
	}
	values, _, err := cluster.cores["n1"].DecisionsFrom(1, 128)
	if err != nil || len(values) < int(slot) {
		t.Fatalf("source values=%d slot=%d err=%v", len(values), slot, err)
	}
	var decision quepaxa.Decision
	if err := json.Unmarshal(values[slot-1].Certificate, &decision); err != nil {
		t.Fatal(err)
	}
	decision.Proposal.Value = values[slot-1].Value
	if control, err := quepaxa.DecodeReconfiguration(decision.Proposal.Value); err != nil || !control {
		t.Fatalf("not real control: %v", err)
	}
	for _, mode := range []string{"valid", "original-helper-comparison", "competing-sync-failure"} {
		t.Run(mode, func(t *testing.T) {
			wal, err := qlog.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer wal.Close()
			core, err := quepaxa.New(quepaxa.Config{NodeID: "n2", Cluster: cluster.config, WAL: wal, Transport: cluster.transports[1], EnableReconfiguration: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := core.AcceptCertifiedValues(values[:96]); err != nil {
				t.Fatal(err)
			}
			mat, err := materializer.Open(t.TempDir()+"/state.sqlite", 4)
			if err != nil {
				t.Fatal(err)
			}
			defer mat.Close()
			server := NewServer(core, mat, "foreground-api", true, cluster.transports[1])
			defer server.Close()
			if err := server.applyDecisions(ctx, 96); err != nil {
				t.Fatal(err)
			}
			if _, err := core.LeaderOrder(slot - 1); err == nil {
				t.Fatal("missing schedule fixture not reproduced")
			}
			request := cluster.transports[0].request(peerfb.OperationLearned)
			request.Decision = decisionToWire(decision)
			peer := &PeerServer{server: server}
			if mode == "original-helper-comparison" {
				if err := server.catchUpFrom(ctx, "n1", slot-1, true); err != nil {
					t.Fatalf("original generic control catchup: %v", err)
				}
				if _, err := peer.handle(ctx, cluster.config.Members[0].PublicKey[:], request); err != nil {
					t.Fatalf("control handler after original helper: %v", err)
				}
				if core.Tip() < slot || mat.Tip() != uint64(slot) {
					t.Fatalf("tip=%d applied=%d", core.Tip(), mat.Tip())
				}
				return
			}
			if mode == "valid" {
				if _, err := peer.handle(ctx, cluster.config.Members[0].PublicKey[:], request); err != nil {
					t.Fatalf("control handler: %v", err)
				}
				if core.Tip() != slot || mat.Tip() != uint64(slot) {
					t.Fatalf("tip=%d applied=%d", core.Tip(), mat.Tip())
				}
				return
			}
			entered, waiting := make(chan struct{}, 1), make(chan struct{}, 1)
			release := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			restore := localtesthooks.Set(func(event string) {
				if event == "core:certified-reconfig:before-sync:node=n2" {
					select {
					case entered <- struct{}{}:
					default:
					}
					<-release
				}
				if strings.Contains(event, "network:learned-control:phase=prefix-durability-start:node=n2:") {
					select {
					case waiting <- struct{}{}:
					default:
					}
				}
			})
			defer restore()
			other := make(chan error, 1)
			go func() { err, _ := server.catchUpLearnedPrefix(ctx, "n1", slot-1); other <- err }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("competing import did not reach sync")
			}
			if core.Tip() != slot-1 {
				t.Fatalf("volatile prefix tip=%d want=%d", core.Tip(), slot-1)
			}
			response := make(chan error, 1)
			go func() { _, err := peer.handle(ctx, cluster.config.Members[0].PublicKey[:], request); response <- err }()
			select {
			case <-waiting:
			case <-ctx.Done():
				t.Fatal("handler did not reach durability gate")
			}
			select {
			case err := <-response:
				t.Fatalf("handler returned before durable sync: %v", err)
			default:
			}
			if err := wal.Close(); err != nil {
				t.Fatalf("inject WAL sync failure: %v", err)
			}
			once.Do(func() { close(release) })
			select {
			case err := <-other:
				if err == nil {
					t.Fatal("competing import concealed sync failure")
				}
			case <-ctx.Done():
				t.Fatal("competing import did not exit")
			}
			select {
			case err := <-response:
				if err == nil {
					t.Fatal("handler acknowledged failed durable prefix")
				}
			case <-ctx.Done():
				t.Fatal("handler did not exit")
			}
			if core.IsDecided(slot) || mat.Tip() != 96 {
				t.Fatalf("failed prefix applied or control accepted: tip=%d applied=%d", core.Tip(), mat.Tip())
			}
		})
	}
}
