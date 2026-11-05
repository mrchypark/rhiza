package managedsql

import (
	"bytes"
	"context"
	"errors"
	"net/http"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/operator"
)

type concurrentSQLContextKey struct{}

func concurrentSQLHosts(t *testing.T, wrap func(int, http.RoundTripper) http.RoundTripper) ([]Config, []*Host, *sqlStatusFixture) {
	t.Helper()
	configs, fixture := sqlCluster(t)
	hosts := make([]*Host, 3)
	for i := range configs {
		kube := *configs[i].Store.Kube
		client := *kube.Client
		if wrap != nil {
			client.Transport = wrap(i, client.Transport)
		}
		kube.Client = &client
		configs[i].Store = &operator.V1Alpha2StatusStore{Kube: &kube}
		ctx, cancel := context.WithCancel(context.Background())
		host, err := fixture.open(ctx, configs[i])
		// Runtime owns ctx: don't cancel a successful Open until cleanup.
		if host != nil {
			t.Cleanup(func() {
				cancel()
				if err := host.Close(); err != nil {
					t.Error(err)
				}
			})
		} else {
			cancel()
		}
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = host
	}
	for _, h := range hosts {
		readySQL(t, h)
		deadline := time.NewTimer(10 * time.Second)
		tick := time.NewTicker(10 * time.Millisecond)
		for !h.node.Ready() {
			select {
			case <-deadline.C:
				tick.Stop()
				t.Fatal("actual node write/catch-up gate did not open")
			case <-tick.C:
			}
		}
		deadline.Stop()
		tick.Stop()
	}
	return configs, hosts, fixture
}

// Observe the normal private peer catch-up worker, not repeated SQL resolution
// or submission. A shared status CAS does not synchronously apply its SQL on
// every peer. Only after the actual certified and materialized tips include
// that slot is it valid to demand a peer-local retained receipt.
func waitSQLCertifiedApplication(t *testing.T, ctx context.Context, h *Host, through uint64) {
	t.Helper()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		state, err := h.node.ManagedState()
		if err != nil {
			t.Fatal(err)
		}
		if state.CertifiedTip >= through && state.AppliedTip == state.CertifiedTip && state.RecorderTip <= state.CertifiedTip {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("normal certified catch-up did not reach original slot=%d certified=%d applied=%d recorder=%d: %v", through, state.CertifiedTip, state.AppliedTip, state.RecorderTip, ctx.Err())
		case <-tick.C:
		}
	}
}

// The original is committed on its origin while actual certificate delivery to
// the target is held. Recovery must not invent a result or submit it again.
func testManagedSQLSubmittedUnknown(t *testing.T, ctx context.Context, configs []Config, hosts []*Host, _ *sqlStatusFixture, pushSeen, pullSeen <-chan struct{}, release func()) {
	t.Helper()
	req := ExecuteRequest{RequestID: "certified-gap-original", SQL: "CREATE TABLE certified_gap_original(id INTEGER PRIMARY KEY)"}
	ack, err := hosts[0].Execute(ctx, req)
	if err != nil || ack.Status != types.MutationCommitted || ack.Slot == 0 {
		t.Fatal("original durable ACK", ack, err)
	}
	for _, seen := range []<-chan struct{}{pushSeen, pullSeen} {
		select {
		case <-seen:
		case <-ctx.Done():
			t.Fatal("actual delivery gap not observed", ctx.Err())
		}
	}
	state, err := hosts[1].node.ManagedState()
	if err != nil || state.RecorderTip < ack.Slot || state.CertifiedTip >= ack.Slot || state.AppliedTip >= ack.Slot {
		t.Fatalf("missing actual certified gap: state=%+v slot=%d error=%v", state, ack.Slot, err)
	}
	before, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	count := hostApplicationDecisionCount(t, ctx, configs[0])
	selector := network.RequestStatusRequest{Kind: "sql", RequestID: req.RequestID}
	if _, err := hosts[1].RecoverRequest(ctx, selector); !errors.Is(err, network.ErrCommitUnknown) || errors.Is(err, ErrSQLNotSubmitted) {
		t.Fatalf("uncertified original recovery must remain UNKNOWN: %v", err)
	}
	release()
	waitSQLCertifiedApplication(t, ctx, hosts[1], ack.Slot)
	result, err := hosts[1].RecoverRequest(ctx, selector)
	if err != nil || result.SQL == nil || result.SQL.Slot != ack.Slot || result.SQL.Status != ack.Status {
		t.Fatalf("retained original after actual catch-up: %+v %v", result, err)
	}
	if got := hostApplicationDecisionCount(t, ctx, configs[0]); got != count {
		t.Fatalf("recovery resubmitted: %d -> %d", count, got)
	}
	after, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil || after.ResourceVersion() != before.ResourceVersion() || !bytes.Equal(after.Evidence(), before.Evidence()) {
		t.Fatal("recovery mutated CR authority", err)
	}
}
