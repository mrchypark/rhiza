package node

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
)

type lagRecoveryChecker struct {
	managedTestChecker
	req       network.ExecuteRequest
	calls     int
	denyAfter bool
}

func (c *lagRecoveryChecker) ValidateManagedSQLRecovery(_ context.Context, _ ManagedStartupIdentity, req network.ExecuteRequest) error {
	c.calls++
	if req.RequestID != c.req.RequestID || req.SQL != c.req.SQL || c.denyAfter && c.calls > 1 {
		return errors.New("exact live pending recovery denied")
	}
	return nil
}

func TestManagedSQLRecoveryControlledApplyLag(t *testing.T) {
	for _, denyAfter := range []bool{false, true} {
		t.Run(map[bool]string{false: "retained", true: "revoked-after-replay"}[denyAfter], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			// Three genuine registered Core/WAL peers. Deliberately do not start
			// an apply goroutine: only ResolveManagedSQL may drain this lag.
			n, _, tr, _ := terminalRetryFixture(t, nil)
			m, err := materializer.Open(filepath.Join(t.TempDir(), "sql.db"), 1)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = m.Close() })
			n.material = m
			req := network.ExecuteRequest{RequestID: "lag-original", SQL: "CREATE TABLE applied_once (id INTEGER)"}
			checker := &lagRecoveryChecker{req: req, denyAfter: denyAfter}
			n.managedStartup = &ManagedStartupConfig{Checker: checker}
			n.opened.Store(true)
			n.server = network.NewServer(n.core, m, "cluster", false, nil)
			t.Cleanup(n.server.Close)
			value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: req.RequestID, SQL: req.SQL}})
			if err != nil {
				t.Fatal(err)
			}
			slot, _, err := tr.cores["b"].Propose(ctx, value)
			if err != nil {
				t.Fatal(err)
			}
			before, err := n.server.ResolveExecute(ctx, req)
			if err != nil || before.State != network.ResolveExecuteUnknownOrExpired || before.Receipt != nil || m.Tip() != 0 {
				t.Fatalf("controlled receipt absence: %+v %v tip=%d", before, err, m.Tip())
			}
			peerTip := tr.cores["b"].Tip()
			result, err := n.ResolveManagedSQL(ctx, req)
			if denyAfter {
				if err == nil || result.Receipt != nil {
					t.Fatalf("revocation authorized result: %+v %v", result, err)
				}
			} else if err != nil || result.State != network.ResolveExecuteRetained || result.Receipt == nil || result.Receipt.Slot != uint64(slot) || result.Receipt.Status != types.MutationCommitted {
				t.Fatalf("certified replay did not recover exact receipt: %+v %v", result, err)
			}
			if checker.calls != 2 || m.Tip() != uint64(slot) || n.core.Tip() != slot || tr.cores["b"].Tip() != peerTip {
				t.Fatalf("recovery proposed extra SQL or missed checks: calls=%d applied=%d target=%d peer=%d", checker.calls, m.Tip(), n.core.Tip(), tr.cores["b"].Tip())
			}
			// Even after a denied authorization, the applied original is retained
			// exactly once; resolving locally must not apply a second command.
			after, err := n.server.ResolveExecute(ctx, req)
			if err != nil || after.Receipt == nil || after.Receipt.Slot != uint64(slot) || after.Receipt.Status != types.MutationCommitted || m.Tip() != uint64(slot) {
				t.Fatalf("original execution count/receipt changed: %+v %v", after, err)
			}
		})
	}
}
