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
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
)

type finalizeRaceContextKey struct{}
type finalizeProofDriftKey struct{}

func TestManagedSQLRemoteFinalizationDuringOriginExecute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	paused, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var fixture *sqlStatusFixture
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	configs, hosts, fixture := concurrentSQLHosts(t, func(i int, base http.RoundTripper) http.RoundTripper {
		if i != 0 {
			return base
		}
		return repairGateTransport(func(r *http.Request) (*http.Response, error) {
			if reads, ok := r.Context().Value(finalizeProofDriftKey{}).(*atomic.Int32); ok && r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/rhizaclusters/") && reads.Add(1) == 6 {
				fixture.mu.Lock()
				var metadata map[string]json.RawMessage
				_ = json.Unmarshal(fixture.obj["metadata"], &metadata)
				metadata["resourceVersion"] = json.RawMessage(`"proof-drift"`)
				fixture.obj["metadata"], _ = json.Marshal(metadata)
				fixture.mu.Unlock()
			}
			if r.Method != http.MethodPut || r.Context().Value(finalizeRaceContextKey{}) != true {
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
			if object.Status.Authority.PendingWrite {
				return base.RoundTrip(r)
			}
			// This is the real finalization PUT: SQL has a retained receipt and
			// passed the configured before-ACK durability and both validators.
			close(paused)
			select {
			case <-release:
				return base.RoundTrip(r)
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		})
	})
	req := ExecuteRequest{RequestID: "shared-migration-original", SQL: "CREATE TABLE remote_finalize (id INTEGER PRIMARY KEY)"}
	type outcome struct {
		response ExecuteResponse
		err      error
	}
	done := make(chan outcome, 1)
	go func() {
		response, err := hosts[0].Execute(context.WithValue(ctx, finalizeRaceContextKey{}, true), req)
		done <- outcome{response, err}
	}()
	select {
	case <-paused:
	case result := <-done:
		t.Fatalf("origin missed actual finalization boundary: %v", result.err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	original, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	pending, ok, err := original.PendingSQL()
	if err != nil || !ok || pending.RequestID != req.RequestID {
		t.Fatal("origin reservation absent")
	}
	retained, err := hosts[0].node.ResolveManagedSQL(ctx, req)
	if err != nil || retained.Receipt == nil || retained.Receipt.Status != types.MutationCommitted || retained.Durability != network.ResolveExecuteDurabilityConfirmed {
		t.Fatalf("actual durable receipt missing: %+v %v", retained, err)
	}
	slot := retained.Receipt.Slot
	waitSQLCertifiedApplication(t, ctx, hosts[1], slot)
	if err := hosts[1].RecoverPending(ctx); err != nil {
		t.Fatal(err)
	}
	final, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := final.Pending(); ok || final.ResourceVersion() == original.ResourceVersion() {
		t.Fatal("remote exact-original finalization did not occur")
	}
	once.Do(func() { close(release) })
	var result outcome
	select {
	case result = <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if result.err != nil || result.response.Status != types.MutationCommitted || result.response.Slot != slot || errors.Is(result.err, network.ErrCommitUnknown) {
		t.Fatalf("remote completion turned committed origin into unknown: %+v %v", result.response, result.err)
	}
	if hosts[0].uncertain.Load() {
		t.Fatal("verified remote finalization poisoned origin")
	}
	// Cover the earlier validator boundary as well: same old reservation,
	// already finalized, exact retained receipt; no status write or SQL replay.
	if err := hosts[0].finalize(ctx, req, original); err != nil {
		t.Fatalf("already finalized original: %v", err)
	}
	after, err := operator.LoadV1Alpha2Admission(ctx, configs[0].Store, configs[0].Ref)
	if err != nil || after.ResourceVersion() != final.ResourceVersion() {
		t.Fatal("idempotent observation rewrote status")
	}
	resolved, err := hosts[0].node.ResolveManagedSQL(ctx, req)
	if err != nil || resolved.Receipt == nil || resolved.Receipt.Slot != slot {
		t.Fatal("idempotent completion replayed SQL")
	}
	t.Run("changed fingerprint", func(t *testing.T) {
		other := req
		other.SQL = "DROP TABLE remote_finalize"
		if err := hosts[0].verifyRemoteFinalization(ctx, other, original); err == nil {
			t.Fatal("different SQL accepted as original")
		}
	})
	t.Run("missing original reservation", func(t *testing.T) {
		if err := hosts[0].verifyRemoteFinalization(ctx, req, final); err == nil {
			t.Fatal("unreserved request accepted")
		}
	})
	t.Run("uncertainty is not cleared", func(t *testing.T) {
		hosts[0].uncertain.Store(true)
		defer hosts[0].uncertain.Store(false)
		if err := hosts[0].verifyRemoteFinalization(ctx, req, original); !errors.Is(err, ErrSQLRecoveryRequired) || !hosts[0].uncertain.Load() {
			t.Fatalf("uncertain host admitted: %v", err)
		}
	})
	for _, key := range []string{"evidence", "generation", "transition"} {
		t.Run("changed "+key, func(t *testing.T) {
			fixture.mu.Lock()
			prior := bytes.Clone(fixture.obj["status"])
			var status, authority map[string]json.RawMessage
			_ = json.Unmarshal(prior, &status)
			_ = json.Unmarshal(status["authority"], &authority)
			switch key {
			case "evidence":
				authority[key] = json.RawMessage(`{"format":"rhiza-managed-sql/v1","digest":"wrong"}`)
			case "generation":
				authority[key] = json.RawMessage(`1`)
			case "transition":
				authority[key] = json.RawMessage(`{"operationID":"other"}`)
			}
			status["authority"], _ = json.Marshal(authority)
			fixture.obj["status"], _ = json.Marshal(status)
			fixture.mu.Unlock()
			defer func() {
				fixture.mu.Lock()
				fixture.obj["status"] = prior
				fixture.mu.Unlock()
			}()
			if err := hosts[0].verifyRemoteFinalization(ctx, req, original); err == nil {
				t.Fatal("changed live proof accepted")
			}
		})
	}
	t.Run("authority RV changes during proof", func(t *testing.T) {
		fixture.mu.Lock()
		prior := bytes.Clone(fixture.obj["metadata"])
		fixture.mu.Unlock()
		defer func() {
			fixture.mu.Lock()
			fixture.obj["metadata"] = prior
			fixture.mu.Unlock()
		}()
		var reads atomic.Int32
		proofCtx := context.WithValue(ctx, finalizeProofDriftKey{}, &reads)
		if err := hosts[0].verifyRemoteFinalization(proofCtx, req, original); err == nil || reads.Load() < 6 {
			t.Fatalf("unstable live authority accepted or boundary missed: reads=%d error=%v", reads.Load(), err)
		}
	})
	t.Run("changed valid initialization assignment", func(t *testing.T) {
		fixture.mu.Lock()
		prior := bytes.Clone(fixture.obj["status"])
		var status map[string]json.RawMessage
		var assignment operator.V1Alpha2ApplicationInitAssignment
		_ = json.Unmarshal(prior, &status)
		_ = json.Unmarshal(status["applicationInit"], &assignment)
		assignment.Plan.AssignmentID = "different-assignment"
		status["applicationInit"], _ = json.Marshal(assignment)
		fixture.obj["status"], _ = json.Marshal(status)
		fixture.mu.Unlock()
		defer func() {
			fixture.mu.Lock()
			fixture.obj["status"] = prior
			fixture.mu.Unlock()
		}()
		changed, err := operator.LoadV1Alpha2ApplicationInit(ctx, configs[0].Store, configs[0].Ref)
		if err != nil || changed.Assignment().Plan.AssignmentID != "different-assignment" {
			t.Fatalf("changed assignment is not valid: %v", err)
		}
		if err := hosts[0].verifyRemoteFinalization(ctx, req, original); err == nil || !strings.Contains(err.Error(), "initialization binding changed") {
			t.Fatalf("changed initialization assignment accepted: %v", err)
		}
	})
	t.Run("changed valid repair", func(t *testing.T) {
		fixture.mu.Lock()
		priorStatus, priorMetadata := bytes.Clone(fixture.obj["status"]), bytes.Clone(fixture.obj["metadata"])
		fixture.mu.Unlock()
		defer func() {
			fixture.mu.Lock()
			fixture.obj["status"], fixture.obj["metadata"] = priorStatus, priorMetadata
			fixture.mu.Unlock()
		}()
		writer, plan := hostRepairWriter(t, configs, hosts)
		current, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
		if err != nil {
			t.Fatal(err)
		}
		closed, observation, err := operator.BeginV1Alpha2Repair(ctx, writer, current, current.Authority(), plan)
		if err != nil || observation != operator.V1Alpha2CASApplied {
			t.Fatalf("create valid changed repair: %s %v", observation, err)
		}
		if repair, ok := closed.Repair(); !ok || repair.State != "Closed" || repair.Plan.OperationID != plan.OperationID {
			t.Fatal("valid changed repair absent")
		}
		if err := hosts[0].verifyRemoteFinalization(ctx, req, original); err == nil || !strings.Contains(err.Error(), "repair changed") {
			t.Fatalf("changed valid repair accepted: %v", err)
		}
	})
	missing := ExecuteRequest{RequestID: "never-submitted", SQL: "CREATE TABLE never_submitted(id)"}
	reserved, observation, err := operator.ReserveV1Alpha2SQLWrite(ctx, configs[0].Store, final, hosts[0].authority, final.Evidence(), missing)
	if err != nil || observation != operator.V1Alpha2CASApplied {
		t.Fatalf("reserve missing-receipt negative: %s %v", observation, err)
	}
	if err := hosts[0].verifyRemoteFinalization(ctx, req, original); err == nil {
		t.Fatal("another pending request admitted")
	}
	w, _ := reserved.Pending()
	_, observation, err = operator.FinalizeV1Alpha2Write(ctx, configs[0].Store, reserved, hosts[0].authority, w, operator.V1Alpha2WriteNoEffect, final.Evidence())
	if err != nil || observation != operator.V1Alpha2CASApplied {
		t.Fatalf("finalize never-submitted negative: %s %v", observation, err)
	}
	if err := hosts[0].verifyRemoteFinalization(ctx, missing, reserved); err == nil {
		t.Fatal("missing retained receipt accepted as completed")
	}
}
