package managedsql

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
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
	configs, hosts, fixture := concurrentSQLHosts(t, nil)
	fixture.mu.Lock()
	fixture.failFinalize = true
	fixture.mu.Unlock()
	req := ExecuteRequest{RequestID: "submitted-finalize-unknown", SQL: "CREATE TABLE finalization_proof (id INTEGER PRIMARY KEY)"}
	response, err := hosts[0].Execute(ctx, req)
	var unknown *network.CommitUnknownError
	if response.Status != types.MutationCommitted || !errors.As(err, &unknown) || !errors.Is(err, network.ErrCommitUnknown) || errors.Is(err, ErrSQLNotSubmitted) || unknown.RequestID != req.RequestID || uint64(unknown.Slot) != response.Slot {
		t.Fatalf("submitted/finalize outcome classification: response=%+v error=%v", response, err)
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
	if err := hosts[1].RecoverPending(ctx); err != nil {
		t.Fatalf("retained original recovery: %v", err)
	}
	for _, h := range hosts {
		// Recovery of the pending original is distinct from fresh admission.
		// ReadIndex may observe a newer peer tip than this Core has learned;
		// that admission must remain closed, not become a recovery failure.
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
}

func TestManagedSQLConcurrentSnapshotLoserIsNotSubmitted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	captured, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	gets := 0
	configs, hosts, _ := concurrentSQLHosts(t, func(i int, base http.RoundTripper) http.RoundTripper {
		if i != 1 {
			return base
		}
		return repairGateTransport(func(r *http.Request) (*http.Response, error) {
			response, err := base.RoundTrip(r)
			if err != nil || r.Method != http.MethodGet || r.Context().Value(concurrentSQLContextKey{}) != true {
				return response, err
			}
			gets++
			if gets != 2 {
				return response, nil
			}
			body, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if err != nil {
				return nil, err
			}
			response.Body = io.NopCloser(bytes.NewReader(body))
			close(captured)
			select {
			case <-release:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
			return response, nil
		})
	})
	if response, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "concurrent-schema", SQL: "CREATE TABLE concurrent_proof (id INTEGER PRIMARY KEY)"}); err != nil || response.Status != types.MutationCommitted {
		t.Fatalf("schema: %v", err)
	}
	loser := ExecuteRequest{RequestID: "snapshot-loser", SQL: "INSERT INTO concurrent_proof VALUES (2)"}
	done := make(chan error, 1)
	go func() {
		_, err := hosts[1].Execute(context.WithValue(ctx, concurrentSQLContextKey{}, true), loser)
		done <- err
	}()
	select {
	case <-captured:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if response, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "snapshot-winner", SQL: "INSERT INTO concurrent_proof VALUES (1)"}); err != nil || response.Status != types.MutationCommitted {
		t.Fatalf("winner: %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	var err error
	select {
	case err = <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var pre *SQLNotSubmittedError
	if !errors.Is(err, ErrSQLNotSubmitted) || !errors.As(err, &pre) || pre.Stage != SQLStageReservation || pre.RecoveryRequired || errors.Is(err, network.ErrCommitUnknown) {
		t.Fatalf("loser classification: %v", err)
	}
	if hosts[1].uncertain.Load() {
		t.Fatal("known pre-submission contention poisoned the Host")
	}
	resolved, err := hosts[1].node.ResolveManagedSQL(ctx, loser)
	if err == nil || resolved.State != network.ResolveExecuteUnknownOrExpired || resolved.Receipt != nil {
		t.Fatalf("unreserved loser gained recovery authority: %v", err)
	}
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
	configs, hosts, fixture := concurrentSQLHosts(t, nil)
	if _, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "pending-schema", SQL: "CREATE TABLE pending_proof (id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	for _, h := range hosts {
		readySQL(t, h)
	}
	control := fixture.source
	control.putReady, control.putRelease = make(chan string, 4), make(chan struct{})
	control.putSuffix = "voters/managed-sql-application-start.json"
	var release sync.Once
	ownerDone := make(chan error, 1)
	joined := false
	t.Cleanup(func() {
		release.Do(func() { close(control.putRelease) })
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
	go func() { _, err := hosts[0].Execute(ctx, owner); ownerDone <- err }()
	select {
	case <-control.putReady:
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
	release.Do(func() { close(control.putRelease) })
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
		if err := h.RecoverPending(ctx); err != nil {
			t.Fatalf("explicit recovery after observed owner completion: %v", err)
		}
	}
	rows, err := hosts[0].Query(ctx, QueryRequest{SQL: "SELECT id FROM pending_proof ORDER BY id", Consistency: "linearizable"})
	if err != nil || len(rows.Rows) != 1 {
		t.Fatalf("non-owner SQL executed or original duplicated: %v", err)
	}
}
