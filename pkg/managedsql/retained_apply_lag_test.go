//go:build rhiza_local_testhooks

package managedsql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// Count actual certified SQL commands through authenticated QUIC. Counting the
// original receipt alone would not detect a deduplicated second proposal.
func retainedLagApplicationCounts(t *testing.T, ctx context.Context, cfg Config, original string) (int, int) {
	t.Helper()
	cluster := quepaxa.Cluster{ConfigID: 1, Members: cfg.Runtime.Members}
	transport := network.NewTransport(cfg.Runtime.ClusterID, cfg.Runtime.NodeID, &cluster, cfg.Runtime.PeerToken)
	defer transport.Close()
	tip, err := transport.ReadTip(ctx, cfg.Runtime.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	total, originalCount := 0, 0
	for from := quepaxa.Slot(1); from <= tip; {
		page, err := transport.FetchDecisions(ctx, cfg.Runtime.NodeID, from, 128)
		if err != nil || len(page.Decisions) == 0 {
			t.Fatal("actual command count page", err)
		}
		for _, decision := range page.Decisions {
			if decision.Slot > tip {
				break
			}
			commands, sql, err := types.DecodeSQLBatch(decision.Value)
			if err != nil {
				t.Fatal(err)
			}
			if sql {
				for _, command := range commands {
					total++
					if command.RequestID == original {
						originalCount++
					}
				}
			}
			from = decision.Slot + 1
		}
	}
	return total, originalCount
}

func TestManagedSQLRetainedOriginalAppliesCertifiedSuffixBeforeFinalize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	configs, hosts, _ := concurrentSQLHosts(t, nil)
	if _, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "retained-lag-schema", SQL: "CREATE TABLE retained_lag(id INTEGER PRIMARY KEY, value TEXT)"}); err != nil {
		t.Fatal(err)
	}
	original := ExecuteRequest{RequestID: "retained-lag-original", SQL: "INSERT INTO retained_lag VALUES(1,'original') RETURNING value", WantRows: true}
	ack, err := hosts[0].Execute(ctx, original)
	if err != nil || ack.Status != types.MutationCommitted || ack.Slot == 0 || len(ack.Statements) != 1 {
		t.Fatal("actual original ACK/result", ack, err)
	}
	// Synchronize the existing receipt before holding only later decisions.
	row, err := hosts[1].Query(ctx, QueryRequest{SQL: "SELECT value FROM retained_lag WHERE id=1", Consistency: "linearizable"})
	if err != nil || len(row.Rows) != 1 || row.Rows[0][0] != "original" {
		t.Fatal("target original readback", row, err)
	}
	initial, err := hosts[1].node.ManagedState()
	if err != nil || initial.AppliedTip < ack.Slot || initial.AppliedTip != initial.CertifiedTip {
		t.Fatal("target initial coherent receipt", initial, err)
	}
	var held atomic.Bool
	pushSeen, backgroundSeen, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var pushOnce, backgroundOnce, releaseOnce sync.Once
	target := configs[1].Runtime.NodeID
	restore := localtesthooks.Set(func(event string) {
		if !held.Load() {
			return
		}
		if strings.HasPrefix(event, fmt.Sprintf("network:learned:phase=apply-start:node=%s:", target)) {
			pushOnce.Do(func() { close(pushSeen) })
			<-release
		}
		if event == "node:catchup-peer:phase=before-local-replay:node="+string(target) {
			backgroundOnce.Do(func() { close(backgroundSeen) })
			<-release
		}
	})
	// Registered after Host cleanups so callbacks are unblocked before any
	// owner waits for peer handlers/catch-up workers to join.
	t.Cleanup(func() { held.Store(false); releaseOnce.Do(func() { close(release) }); restore() })
	held.Store(true)
	// Park the actual periodic worker before generating the later decision.
	// Host inspection calls replayLocalDecisions directly and is not blocked.
	select {
	case <-backgroundSeen:
		t.Log("observed target background before local replay")
	case <-ctx.Done():
		t.Fatal("target did not enter controlled catchUpPeer; no catchUpQuorum bypass claimed", ctx.Err())
	}
	later, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "retained-lag-later", SQL: "INSERT INTO retained_lag VALUES(2,'later')"})
	if err != nil || later.Status != types.MutationCommitted {
		t.Fatal("actual later committed application", later, err)
	}
	for _, observation := range []struct {
		name string
		done <-chan struct{}
	}{{"push after acceptance before apply", pushSeen}} {
		select {
		case <-observation.done:
			t.Log("observed", observation.name)
		case <-ctx.Done():
			t.Fatal("setup did not reach", observation.name, ctx.Err())
		}
	}
	lag, err := hosts[1].node.ManagedState()
	if err != nil || lag.CertifiedTip <= lag.AppliedTip || lag.AppliedTip < ack.Slot || lag.RecorderTip > lag.CertifiedTip {
		t.Fatal("real certified-but-unapplied setup absent", lag, err)
	}
	authority, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, pending := authority.Pending(); pending {
		t.Fatal("later original not actually finalized")
	}
	reserved, observed, err := operator.ReserveV1Alpha2SQLWrite(ctx, configs[0].Store, authority, authority.Authority(), authority.Evidence(), original)
	if err != nil || observed != operator.V1Alpha2CASApplied {
		t.Fatal("actual exact duplicate-original CAS reservation", observed, err)
	}
	retained, err := hosts[1].node.ResolveManagedSQL(ctx, original)
	if err != nil || retained.State != network.ResolveExecuteRetained || retained.Durability != network.ResolveExecuteDurabilityConfirmed || retained.Receipt == nil || retained.Receipt.Slot != ack.Slot {
		t.Fatal("real retained/barrier setup", retained, err)
	}
	afterRetained, err := hosts[1].node.ManagedState()
	if err != nil || afterRetained.AppliedTip != lag.AppliedTip || afterRetained.CertifiedTip <= afterRetained.AppliedTip {
		t.Fatal("retained resolver unexpectedly drained controlled suffix", afterRetained, err)
	}
	beforeTotal, beforeOriginal := retainedLagApplicationCounts(t, ctx, configs[0], original.RequestID)
	result, recoverErr := hosts[1].RecoverPendingDetailed(ctx)
	afterTotal, afterOriginal := retainedLagApplicationCounts(t, ctx, configs[0], original.RequestID)
	if beforeTotal != afterTotal || beforeOriginal != 1 || afterOriginal != 1 {
		t.Fatalf("application resubmitted: total %d->%d original %d->%d", beforeTotal, afterTotal, beforeOriginal, afterOriginal)
	}
	t.Logf("actual retained slot=%d certified=%d applied=%d recorder=%d barrier=%s SQL application commands %d->%d original commands %d->%d", ack.Slot, lag.CertifiedTip, lag.AppliedTip, lag.RecorderTip, retained.Durability, beforeTotal, afterTotal, beforeOriginal, afterOriginal)
	if recoverErr != nil {
		stillPending, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
		if err != nil || stillPending.ResourceVersion() != reserved.ResourceVersion() {
			t.Fatal("failed recovery changed reservation", err)
		}
		stored, ok, err := stillPending.PendingSQL()
		if err != nil || !ok || stored.RequestID != original.RequestID || stored.SQL != original.SQL {
			t.Fatal("failed recovery changed exact original", err)
		}
		t.Fatalf("retained original must finalize after existing certified replay: %v", recoverErr)
	}
	if result.Kind != "sql" || string(result.RequestID) != original.RequestID || result.SQL == nil {
		t.Fatal("original tagged result lost", result)
	}
	want, _ := json.Marshal(ack)
	got, _ := json.Marshal(result.SQL)
	if !bytes.Equal(want, got) {
		t.Fatalf("original result changed: want=%s got=%s", want, got)
	}
	after, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, pending := after.Pending(); pending {
		t.Fatal("confirmed original remains pending")
	}
	state, err := hosts[1].node.ManagedState()
	if err != nil || state.AppliedTip != state.CertifiedTip || state.AppliedTip < later.Slot {
		t.Fatal("inspection did not drain certified suffix", state, err)
	}
	rows, err := hosts[1].Query(ctx, QueryRequest{SQL: "SELECT id,value FROM retained_lag ORDER BY id", Consistency: "local"})
	if err != nil || len(rows.Rows) != 2 || rows.Rows[0][1] != "original" || rows.Rows[1][1] != "later" {
		t.Fatal("actual original/later effects", rows, err)
	}
}
