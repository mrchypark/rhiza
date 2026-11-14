//go:build rhiza_local_testhooks

package network

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestLearnedPrefixRealControlAndLeaderScheduleSamePage(t *testing.T) {
	cluster := newForegroundAPIPeers(t, t.TempDir(), true)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	target := cluster.config
	target.ConfigID++
	target.Members = []quepaxa.Member{cluster.config.Members[0], cluster.config.Members[2]}
	if _, err := cluster.cores["n1"].BeginReconfiguration(ctx, target); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := cluster.cores["n1"].FinishReconfiguration(ctx); err != nil {
		t.Fatalf("finish: %v", err)
	}
	for i := 0; cluster.cores["n1"].Tip() < 120; i++ {
		id := fmt.Sprintf("control-schedule-%d", i)
		item, err := types.EncodeKVBatchItem(types.KVCommand{RequestID: id, Operation: "put", Key: id, Value: []byte(id), ObservedAtUnixMS: time.Now().UnixMilli()})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cluster.servers["n1"].proposeOnce(ctx, types.AssembleKVBatch([][]byte{item})); err != nil {
			t.Fatalf("source tip %d: %v", cluster.cores["n1"].Tip(), err)
		}
	}
	values, _, err := cluster.cores["n1"].DecisionsFrom(1, 128)
	if err != nil {
		t.Fatal(err)
	}
	control, schedule := false, false
	for _, v := range values {
		_, yes, err := quepaxa.DecodeLeaderSchedule(v.Value)
		if err != nil {
			t.Fatal(err)
		}
		schedule = schedule || yes
		c, err := quepaxa.DecodeReconfiguration(v.Value)
		if err != nil {
			t.Fatal(err)
		}
		control = control || c
	}
	if !control || !schedule {
		t.Fatalf("page lacks real control/schedule: control=%t schedule=%t values=%d", control, schedule, len(values))
	}
	dir := t.TempDir()
	wal, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n3", Cluster: cluster.config, WAL: wal, Transport: cluster.transports[2], EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(core, nil, "foreground-api", true, cluster.transports[2])
	defer server.Close()
	err, fatal := server.catchUpLearnedPrefix(ctx, "n1", 120)
	if err != nil || fatal {
		t.Fatalf("control/schedule catchup: %v fatal=%v tip=%d", err, fatal, core.Tip())
	}
	if core.Tip() != 120 || core.CurrentCluster().ConfigID != 2 {
		t.Fatalf("tip=%d config=%d", core.Tip(), core.CurrentCluster().ConfigID)
	}
	server.Close()
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := qlog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered, err := quepaxa.New(quepaxa.Config{NodeID: "n3", Cluster: cluster.config, WAL: reopened, Transport: cluster.transports[2], EnableReconfiguration: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if recovered.Tip() != 120 || recovered.CurrentCluster().ConfigID != 2 {
		t.Fatalf("reopened tip=%d config=%d", recovered.Tip(), recovered.CurrentCluster().ConfigID)
	}
	for _, mode := range []string{"cancel", "append-failure", "sync-failure"} {
		t.Run(mode, func(t *testing.T) {
			wal, err := qlog.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer wal.Close()
			core, err := quepaxa.New(quepaxa.Config{NodeID: "n3", Cluster: cluster.config, WAL: wal, Transport: cluster.transports[2], EnableReconfiguration: true})
			if err != nil {
				t.Fatal(err)
			}
			server := NewServer(core, nil, "foreground-api", true, cluster.transports[2])
			defer server.Close()
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			restore := localtesthooks.Set(func(event string) {
				if (mode != "sync-failure" && strings.Contains(event, "core:certified-reconfig:after-slot:node=n3:slot=5")) || (mode == "sync-failure" && event == "core:certified-reconfig:before-sync:node=n3") {
					select {
					case entered <- struct{}{}:
					default:
					}
					<-release
				}
			})
			defer restore()
			work, stop := context.WithCancel(ctx)
			defer stop()
			type outcome struct {
				err   error
				fatal bool
			}
			owner := make(chan outcome, 1)
			competitor := make(chan outcome, 1)
			go func() { err, fatal := server.catchUpLearnedPrefix(work, "n1", 120); owner <- outcome{err, fatal} }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("partial page not entered")
			}
			if (mode != "sync-failure" && (core.Tip() < 5 || core.Tip() >= 120)) || (mode == "sync-failure" && core.Tip() != 120) {
				t.Fatalf("not a partial page: tip=%d", core.Tip())
			}
			go func() { err, fatal := server.catchUpLearnedPrefix(ctx, "n1", 120); competitor <- outcome{err, fatal} }()
			if mode != "sync-failure" {
				select {
				case v := <-competitor:
					t.Fatalf("competing recovery completed before owned page: %+v", v)
				default:
				}
			}
			if mode == "cancel" {
				stop()
			} else if err := wal.Close(); err != nil {
				t.Fatalf("inject WAL failure: %v", err)
			}
			once.Do(func() { close(release) })
			select {
			case got := <-owner:
				if !got.fatal || got.err == nil {
					t.Fatalf("owned install hid failure: %+v", got)
				}
				if mode == "cancel" && !errors.Is(got.err, context.Canceled) {
					t.Fatalf("cancel result=%v", got.err)
				}
			case <-ctx.Done():
				t.Fatal("owned install did not return")
			}
			select {
			case got := <-competitor:
				if mode == "append-failure" && got.err == nil {
					t.Fatal("competing recovery hid WAL failure")
				}
				if mode == "sync-failure" && got.err == nil {
					if err := core.EnsureDurableThrough(ctx, 120); err == nil {
						t.Fatal("volatile competing tip falsely durable after sync failure")
					}
				}
				if mode == "cancel" && got.err == nil {
					if err := core.EnsureDurableThrough(ctx, 120); err != nil {
						t.Fatalf("competing tip was not durable: %v", err)
					}
				}
			case <-ctx.Done():
				t.Fatal("competing recovery leaked")
			}
		})
	}
}
