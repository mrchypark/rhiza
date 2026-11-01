package managedsql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
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
		host, err := Open(ctx, configs[i])
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

func TestManagedSQLThreeHostsSubmittedUnknownRetainsExactOriginal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	configs, hosts, fixture, heldLearn := heldCertifiedRecoveryHosts(t)
	ctx = context.WithValue(ctx, certifiedRecoveryControlKey{}, true)
	heldLearn.Store(true)
	fixture.mu.Lock()
	fixture.failFinalize = true
	fixture.mu.Unlock()
	req := ExecuteRequest{RequestID: "submitted-finalize-unknown", SQL: "CREATE TABLE finalization_proof (id INTEGER PRIMARY KEY)"}
	response, err := hosts[0].Execute(ctx, req)
	var unknown *network.CommitUnknownError
	if response.Status != types.MutationCommitted || !errors.As(err, &unknown) || !errors.Is(err, network.ErrCommitUnknown) || errors.Is(err, ErrSQLNotSubmitted) || unknown.RequestID != req.RequestID || uint64(unknown.Slot) != response.Slot {
		t.Fatalf("submitted/finalize outcome classification: response=%+v error=%v", response, err)
	}
	lag, err := hosts[1].node.ManagedState()
	if err != nil || lag.RecorderTip < response.Slot || lag.CertifiedTip >= response.Slot || lag.AppliedTip >= response.Slot {
		t.Fatalf("actual peer certificate delivery gap missing: recorder=%d certified=%d applied=%d slot=%d error=%v", lag.RecorderTip, lag.CertifiedTip, lag.AppliedTip, response.Slot, err)
	}
	retained, err := hosts[0].node.ResolveManagedSQL(ctx, req)
	if err != nil || retained.State != network.ResolveExecuteRetained || retained.Receipt == nil || retained.Receipt.Slot != response.Slot || retained.Durability != network.ResolveExecuteDurabilityConfirmed {
		t.Fatalf("origin exact durable receipt missing despite target gap: %+v %v", retained, err)
	}
	before, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	beforePending, ok := before.Pending()
	if !ok || beforePending.ID != req.RequestID {
		t.Fatal("submitted original reservation missing")
	}
	var wg sync.WaitGroup
	for _, h := range hosts[1:] {
		wg.Go(func() {
			if err := h.RecoverPending(ctx); !errors.Is(err, network.ErrCommitUnknown) || errors.Is(err, ErrSQLNotSubmitted) {
				t.Errorf("unconfirmed recovery outcome: %v", err)
			}
		})
	}
	wg.Wait()
	after, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	pending, ok := after.Pending()
	if !ok || pending != beforePending || !bytes.Equal(before.Evidence(), after.Evidence()) {
		t.Fatal("concurrent failed finalizers changed unknown original/evidence")
	}
	fixture.mu.Lock()
	fixture.failFinalize = false
	fixture.mu.Unlock()
	logCertifiedRecoveryState(t, ctx, hosts, configs[0], req, "before-immediate-original-recovery")
	if err := hosts[1].ValidateManagedSQLRecovery(ctx, hosts[1].identity, req); err != nil {
		t.Fatalf("explicit recovery live authority rejected before certified gap check: %v", err)
	}
	if err := hosts[1].RecoverPending(ctx); err == nil || !errors.Is(err, network.ErrCommitUnknown) || errors.Is(err, ErrSQLNotSubmitted) || !strings.Contains(err.Error(), "managed SQL recovery has unresolved certified history") {
		t.Fatalf("uncertified target did not preserve unknown original: %v", err)
	}
	stillPending, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	stillOriginal, ok := stillPending.Pending()
	if !ok || stillOriginal != beforePending || !bytes.Equal(before.Evidence(), stillPending.Evidence()) {
		t.Fatal("uncertified recovery changed original pending fingerprint or evidence")
	}
	// A quorum ReadIndex is not delivery of that certificate to this peer.
	heldLearn.Store(false)
	waitSQLCertifiedApplication(t, ctx, hosts[1], response.Slot)
	if err := hosts[1].RecoverPending(ctx); err != nil {
		t.Fatalf("retained original recovery: %v", err)
	}
	for _, h := range hosts {
		// Recovery of the pending original is distinct from fresh admission.
		// ReadIndex may observe a newer peer tip than this Core has learned;
		// that admission must remain closed, not become a recovery failure.
		waitSQLCertifiedApplication(t, ctx, h, response.Slot)
		resolved, err := h.node.ResolveManagedSQL(ctx, req)
		if err != nil || resolved.Receipt == nil || resolved.Receipt.Slot != response.Slot || resolved.Receipt.Status != types.MutationCommitted {
			t.Fatalf("original retained receipt changed: %+v %v", resolved, err)
		}
	}
	final, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, pending := final.Pending(); pending {
		t.Fatal("explicit recovery left original pending")
	}
	resolved, err := hosts[0].node.ResolveManagedSQL(ctx, req)
	if err != nil || resolved.Receipt == nil || resolved.Receipt.Slot != response.Slot {
		t.Fatalf("recovery changed/re-executed original receipt: %v", err)
	}
	for _, h := range hosts {
		state, err := h.node.ManagedState()
		if err != nil || state.RecorderTip != response.Slot {
			t.Fatalf("recovery submitted another SQL slot: recorder=%d original=%d error=%v", state.RecorderTip, response.Slot, err)
		}
	}
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

func TestManagedSQLConcurrentSnapshotLoserIsNotSubmitted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	type reservation struct {
		host   int
		rv, id string
	}
	captured := make(chan reservation, 2)
	release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var releaseOnce [2]sync.Once
	t.Cleanup(func() {
		for i := range release {
			releaseOnce[i].Do(func() { close(release[i]) })
		}
	})
	configs, hosts, _ := concurrentSQLHosts(t, func(i int, base http.RoundTripper) http.RoundTripper {
		if i > 1 {
			return base
		}
		return repairGateTransport(func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodPut || r.Context().Value(concurrentSQLContextKey{}) != true {
				return base.RoundTrip(r)
			}
			body, err := io.ReadAll(r.Body)
			_ = r.Body.Close()
			if err != nil {
				return nil, err
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			var object struct {
				Metadata struct{ ResourceVersion string }
				Status   struct {
					Authority struct {
						PendingWrite   bool
						PendingRequest struct{ ID string }
					}
				}
			}
			if err := json.Unmarshal(body, &object); err != nil {
				return nil, err
			}
			// The actual reservation PUT exists only after quorum admission,
			// write readiness, SQL inspection and reservation preconditions pass.
			if !object.Status.Authority.PendingWrite {
				return base.RoundTrip(r)
			}
			select {
			case captured <- reservation{i, object.Metadata.ResourceVersion, object.Status.Authority.PendingRequest.ID}:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
			select {
			case <-release[i]:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
			return base.RoundTrip(r)
		})
	})
	schema, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "concurrent-schema", SQL: "CREATE TABLE concurrent_proof (id INTEGER PRIMARY KEY)"})
	if err != nil || schema.Status != types.MutationCommitted {
		t.Fatalf("schema: %v", err)
	}
	for _, h := range hosts {
		waitSQLCertifiedApplication(t, ctx, h, schema.Slot)
		readySQL(t, h)
	}
	loser := ExecuteRequest{RequestID: "snapshot-loser", SQL: "INSERT INTO concurrent_proof VALUES (2)"}
	winner := ExecuteRequest{RequestID: "snapshot-winner", SQL: "INSERT INTO concurrent_proof VALUES (1)"}
	type outcome struct {
		response ExecuteResponse
		err      error
	}
	done := [2]chan outcome{make(chan outcome, 1), make(chan outcome, 1)}
	for i, req := range []ExecuteRequest{winner, loser} {
		go func() {
			response, err := hosts[i].Execute(context.WithValue(ctx, concurrentSQLContextKey{}, true), req)
			done[i] <- outcome{response, err}
		}()
	}
	var reservations [2]reservation
	for range 2 {
		select {
		case value := <-captured:
			reservations[value.host] = value
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if reservations[0].rv == "" || reservations[0].rv != reservations[1].rv || reservations[0].id != winner.RequestID || reservations[1].id != loser.RequestID {
		t.Fatalf("both exact reservations did not reach same-RV CAS: %+v", reservations)
	}
	before, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, pending := before.Pending(); pending || before.ResourceVersion() != reservations[0].rv {
		t.Fatal("blocked reservation PUT escaped before release")
	}
	for i := range done {
		select {
		case result := <-done[i]:
			t.Fatalf("host %d completed before CAS release: %v", i, result.err)
		default:
		}
	}
	releaseOnce[0].Do(func() { close(release[0]) })
	var committed outcome
	select {
	case committed = <-done[0]:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if committed.err != nil || committed.response.Status != types.MutationCommitted {
		t.Fatalf("winner: %v", committed.err)
	}
	select {
	case result := <-done[1]:
		t.Fatalf("loser escaped blocked CAS: %v", result.err)
	default:
	}
	releaseOnce[1].Do(func() { close(release[1]) })
	select {
	case result := <-done[1]:
		err = result.err
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var pre *SQLNotSubmittedError
	if !errors.Is(err, ErrSQLNotSubmitted) || !errors.As(err, &pre) || pre.Stage != SQLStageReservation || pre.RecoveryRequired || errors.Is(err, network.ErrCommitUnknown) || !errors.Is(err, operator.ErrV1Alpha2StatusConflict) {
		t.Fatalf("loser classification: %v", err)
	}
	if hosts[1].uncertain.Load() {
		t.Fatal("known pre-submission contention poisoned the Host")
	}
	rows, err := hosts[0].Query(ctx, QueryRequest{SQL: "SELECT id FROM concurrent_proof ORDER BY id", Consistency: "linearizable"})
	if err != nil || len(rows.Rows) != 1 {
		t.Fatalf("blocked CAS loser submitted SQL: rows=%d error=%v", len(rows.Rows), err)
	}
	resolved, err := hosts[1].node.ResolveManagedSQL(ctx, loser)
	if err == nil || resolved.State != network.ResolveExecuteUnknownOrExpired || resolved.Receipt != nil {
		t.Fatalf("unreserved loser gained recovery authority: %v", err)
	}
	waitSQLCertifiedApplication(t, ctx, hosts[1], committed.response.Slot)
	readySQL(t, hosts[1])
	if response, err := hosts[1].Execute(ctx, loser); err != nil || response.Status != types.MutationCommitted {
		t.Fatalf("fresh invocation after known non-submission failed: %v", err)
	}
	a, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, pending := a.Pending(); pending {
		t.Fatal("successful writer left pending SQL")
	}
}

func TestManagedSQLThreeHostsConcurrentExecuteAndRecoverPreserveOriginal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	reserved, releaseReservation := make(chan struct{}), make(chan struct{})
	configs, hosts, _ := concurrentSQLHosts(t, func(i int, base http.RoundTripper) http.RoundTripper {
		if i != 0 {
			return base
		}
		return repairGateTransport(func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodPut || r.Context().Value(concurrentSQLContextKey{}) != true {
				return base.RoundTrip(r)
			}
			body, err := io.ReadAll(r.Body)
			_ = r.Body.Close()
			if err != nil {
				return nil, err
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			var object struct {
				Status struct{ Authority struct{ PendingWrite bool } }
			}
			if err := json.Unmarshal(body, &object); err != nil {
				return nil, err
			}
			response, err := base.RoundTrip(r)
			if err != nil || !object.Status.Authority.PendingWrite || response.StatusCode != http.StatusOK {
				return response, err
			}
			// The real API has accepted the original reservation, but the owner
			// cannot enter ExecuteManagedSQL until this response is released.
			close(reserved)
			select {
			case <-releaseReservation:
				return response, nil
			case <-r.Context().Done():
				_ = response.Body.Close()
				return nil, r.Context().Err()
			}
		})
	})
	schema, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "pending-schema", SQL: "CREATE TABLE pending_proof (id INTEGER PRIMARY KEY)"})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hosts {
		waitSQLCertifiedApplication(t, ctx, h, schema.Slot)
		readySQL(t, h)
	}
	var release sync.Once
	ownerDone := make(chan error, 1)
	var ownerResponse ExecuteResponse
	joined := false
	t.Cleanup(func() {
		release.Do(func() { close(releaseReservation) })
		if !joined {
			cancel()
			select {
			case <-ownerDone:
			case <-time.After(3 * time.Second):
				t.Error("owner did not stop")
			}
		}
	})
	owner := ExecuteRequest{RequestID: "pending-owner", SQL: "INSERT INTO pending_proof VALUES (1)"}
	go func() {
		var err error
		ownerResponse, err = hosts[0].Execute(context.WithValue(ctx, concurrentSQLContextKey{}, true), owner)
		ownerDone <- err
	}()
	select {
	case <-reserved:
	case err := <-ownerDone:
		joined = true
		t.Fatalf("owner completed before reservation barrier: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	before, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	original, ok, err := before.PendingSQL()
	if err != nil || !ok || original.RequestID != owner.RequestID {
		t.Fatal("actual reserved original missing")
	}
	var wg sync.WaitGroup
	for _, h := range hosts[1:] {
		wg.Go(func() {
			_, err := h.Execute(ctx, ExecuteRequest{RequestID: "not-submitted-" + h.cfg.PodUID, SQL: "INSERT INTO pending_proof VALUES (2)"})
			var pre *SQLNotSubmittedError
			if !errors.As(err, &pre) || pre.Stage != SQLStagePending || !pre.RecoveryRequired || errors.Is(err, network.ErrCommitUnknown) {
				t.Errorf("pending contention: %v", err)
			}
			if err := h.RecoverPending(ctx); !errors.Is(err, network.ErrCommitUnknown) || errors.Is(err, ErrSQLNotSubmitted) {
				t.Errorf("unresolved original is not typed unknown: %v", err)
			}
		})
	}
	wg.Wait()
	after, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	retained, ok, err := after.PendingSQL()
	if err != nil || !ok || retained.RequestID != owner.RequestID || !bytes.Equal(before.Evidence(), after.Evidence()) || before.ResourceVersion() != after.ResourceVersion() {
		t.Fatal("concurrent recovery cleared/replaced unknown original or evidence")
	}
	release.Do(func() { close(releaseReservation) })
	select {
	case err = <-ownerDone:
		joined = true
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err != nil {
		t.Fatalf("actual owner execution: %v", err)
	}
	for _, h := range hosts[1:] {
		waitSQLCertifiedApplication(t, ctx, h, ownerResponse.Slot)
		if err := h.RecoverPending(ctx); err != nil {
			t.Fatalf("explicit recovery after observed owner completion: %v", err)
		}
	}
	rows, err := hosts[0].Query(ctx, QueryRequest{SQL: "SELECT id FROM pending_proof ORDER BY id", Consistency: "linearizable"})
	if err != nil || len(rows.Rows) != 1 {
		t.Fatalf("non-owner SQL executed or original duplicated: %v", err)
	}
}
