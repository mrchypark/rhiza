//go:build rhiza_local_testhooks

package node

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func verifyArchiveWhileLaterTerminalPending(t *testing.T, ctx context.Context, source *quepaxa.Core, initial quepaxa.Cluster, tr *managedAppendOnlyTestTransport, archive *recovery.Manager, seal quepaxa.CheckpointSeal, members []quepaxa.Member) {
	t.Helper()
	later := quepaxa.Cluster{ConfigID: 3, Members: members[:1]}
	if _, err := source.BeginReconfiguration(ctx, later); err != nil {
		t.Fatal(err)
	}
	if err := source.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	terminal := source.Tip()
	if err := tr.peers["n2"].WaitTip(ctx, terminal); err != nil {
		t.Fatal(err)
	}
	if err := tr.peers["n2"].EnsureDurableThrough(ctx, terminal); err != nil {
		t.Fatal(err)
	}
	values, _, err := source.DecisionsFrom(1, 256)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	reader, err := quepaxa.New(quepaxa.Config{NodeID: "n2", Cluster: initial, WAL: wal, Transport: tr, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		if value.Slot >= terminal {
			break
		}
		if err := reader.AcceptCertifiedValue(value); err != nil {
			t.Fatalf("replay slot %d: %v", value.Slot, err)
		}
	}
	terminalValue, ok := source.CertifiedValue(terminal)
	if !ok {
		t.Fatal("missing terminal proof")
	}
	reached, release := make(chan struct{}), make(chan struct{})
	var once, reachedOnce sync.Once
	defer once.Do(func() { close(release) })
	restore := localtesthooks.Set(func(event string) {
		if event == fmt.Sprintf("core:ensure-durable:before-sync:node=n2:slot=%d", terminal) {
			reachedOnce.Do(func() { close(reached) })
			<-release
		}
	})
	defer restore()
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); done <- reader.AcceptCertifiedValue(terminalValue) }()
	defer func() {
		once.Do(func() { close(release) })
		select {
		case <-finished:
		case <-ctx.Done():
			t.Errorf("terminal goroutine did not exit: %v", ctx.Err())
		}
	}()
	select {
	case <-reached:
	case <-ctx.Done():
		t.Fatal("terminal durability gate not reached", ctx.Err())
	}
	if reader.Tip() < terminal || reader.ConfigID() != later.ConfigID {
		t.Fatal("later terminal did not advance Core before gate")
	}
	n := &Node{core: reader, archive: archive, config: &types.ExecutionConfig{NodeID: "n2", Members: members, EnableReconfiguration: true}, managedStartup: &ManagedStartupConfig{expectedPolicy: sqlpolicy.ManagedRequestMaterializationPolicy}}
	if err := n.verifyManagedAppendOnlyArchiveHistory(ctx, seal); err != nil {
		t.Fatalf("actual archive history failed while later terminal sync pending: %v", err)
	}
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("terminal did not finish", ctx.Err())
	}
}
