package network

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func managedKVRealServer(t *testing.T, policy sqlpolicy.MaterializationPolicy) (*Server, *materializer.Materializer, *quepaxa.Core) {
	t.Helper()
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	var m *materializer.Materializer
	var err error
	if policy == 8 {
		m, err = materializer.OpenManagedSQLKV(t.TempDir()+"/sqlite.db", 1, 1024)
	} else {
		m, err = materializer.Open(t.TempDir()+"/sqlite.db", 1, 1024)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	s, err := NewServerForPolicy(core, m, "cluster", true, nil, policy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s, m, core
}

func TestManagedKVServerRawRoundTrip(t *testing.T) {
	s, m, core := managedKVRealServer(t, 8)
	ctx := context.Background()
	if result, err := s.Execute(ctx, ExecuteRequest{RequestID: "sql", SQL: "CREATE TABLE mixed_sql(v INTEGER)"}); err != nil || result.Status != types.MutationCommitted {
		t.Fatal("mixed SQL path changed", result, err)
	}
	for i, id := range []string{string([]byte{255}), string([]byte{254}), "\ufffd", "a\x00b"} {
		req := KVMutationRequest{RequestID: id, Key: id, Value: []byte{byte(i)}, TTLMS: 60000}
		first, err := s.KVPut(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		tip := core.Tip()
		decisions, _, err := core.DecisionsFrom(quepaxa.Slot(first.Slot), 1)
		if err != nil || len(decisions) != 1 {
			t.Fatal("missing real decision", err)
		}
		commands, ok, err := types.DecodeManagedKVBatch(decisions[0].Value)
		if err != nil || !ok || len(commands) != 1 {
			t.Fatal("wrong production codec", err)
		}
		original := commands[0]
		if original.RequestID != id || original.Key != id || original.TTLMS != 60000 || original.ObservedAtUnixMS <= 0 || original.ExpiresAtUnixMS != original.ObservedAtUnixMS+60000 {
			t.Fatalf("lost original command %#v", original)
		}
		second, err := s.KVPut(ctx, req)
		if err != nil || second != first || core.Tip() != tip {
			t.Fatalf("duplicate resubmitted %+v %v", second, err)
		}
		after, _, err := core.DecisionsFrom(quepaxa.Slot(first.Slot), 1)
		if err != nil || !bytes.Equal(after[0].Value, decisions[0].Value) {
			t.Fatal("first timestamp refreshed", err)
		}
		value, err := s.KVGet(ctx, KVGetRequest{Key: id})
		if err != nil || !value.Found || !bytes.Equal(value.Value, req.Value) {
			t.Fatal("raw key lookup failed", value, err)
		}
		fingerprint, err := types.ManagedKVFingerprint(original)
		if err != nil {
			t.Fatal(err)
		}
		receipt, found, matches, err := m.KVRequestStatusFingerprint(ctx, id, fingerprint)
		if err != nil || !found || !matches || receipt.Slot != first.Slot {
			t.Fatal("original receipt lost", receipt, err)
		}
		req.Key += "different"
		if _, err := s.KVPut(ctx, req); !errors.Is(err, ErrRequestConflict) || core.Tip() != tip {
			t.Fatal("changed intent admitted", err)
		}
	}
	for _, blob := range [][]byte{nil, {}} {
		req := KVMutationRequest{RequestID: fmt.Sprintf("empty-%t", blob == nil), Key: "empty", Value: blob, Expected: bytes.Clone(blob)}
		response, err := s.KVPut(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		decisions, _, err := core.DecisionsFrom(quepaxa.Slot(response.Slot), 1)
		if err != nil {
			t.Fatal(err)
		}
		commands, ok, err := types.DecodeManagedKVBatch(decisions[0].Value)
		if err != nil || !ok || (commands[0].Value == nil) != (blob == nil) || (commands[0].Expected == nil) != (blob == nil) {
			t.Fatal("producer collapsed nil/empty", err)
		}
	}
	cas := KVMutationRequest{RequestID: "false-cas", Key: "absent", ExpectedExists: true, Value: []byte("new"), TTLMS: 60000}
	first, err := s.KVCAS(ctx, cas)
	if err != nil || first.Applied || first.Status != types.MutationCommitted {
		t.Fatal("CAS false lost", first, err)
	}
	tip := core.Tip()
	second, err := s.KVCAS(ctx, cas)
	if err != nil || second != first || core.Tip() != tip {
		t.Fatal("CAS false resubmitted", second, err)
	}
}

func TestManagedKVServerUnsupportedBeforeAdmission(t *testing.T) {
	s, _, core := managedKVRealServer(t, 8)
	// Saturation makes an accidental admission observable as ErrOverloaded.
	s.mutationAdmission = mutationAdmission{count: maxAdmittedMutations, bytes: maxAdmittedBytes}
	ctx := context.Background()
	checks := []func() error{
		func() error {
			_, err := s.GraphExecute(ctx, types.GraphCommand{RequestID: "g", Cypher: "CREATE (:Item)"})
			return err
		},
		func() error {
			_, err := s.NotifyPublish(ctx, types.NotifyCommand{RequestID: "n", Topic: "topic"})
			return err
		},
		func() error { _, _, err := s.NotifySubscribe("topic"); return err },
		func() error { _, err := s.Migrate(ctx, MigrationRequest{}); return err },
		func() error { _, err := s.AdmitMigration(ctx, 1, "name", nil); return err },
		func() error { _, err := s.MigrateAdmitted(ctx, MigrationRequest{}, nil); return err },
	}
	for _, check := range checks {
		if err := check(); !errors.Is(err, ErrInvalidRequest) {
			t.Fatal("unsupported reached admission", err)
		}
	}
	if core.Tip() != 0 || core.RecorderTip() != 0 || s.mutationAdmission.count != maxAdmittedMutations || s.mutationAdmission.bytes != maxAdmittedBytes {
		t.Fatal("unsupported changed state")
	}
}

func TestManagedKVServerLegacyGraphNeighbor(t *testing.T) {
	s, _, core := managedKVRealServer(t, 7)
	result, err := s.GraphExecute(context.Background(), types.GraphCommand{RequestID: "g", Cypher: "CREATE (:Item)"})
	if err != nil || result.Slot != 1 || core.Tip() != 1 {
		t.Fatal("legacy Graph changed", result, err)
	}
}

func TestManagedKVServerPostSubmitLookupFailureIsUnknown(t *testing.T) {
	for _, kind := range []string{"sql", "kv"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			path := t.TempDir() + "/sqlite.db"
			m, err := materializer.OpenManagedSQLKV(path, 1, 1024)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = m.Close() })
			core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
			s, err := NewServerForPolicy(core, m, "cluster", true, nil, 8)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(s.Close)
			if kind == "sql" {
				if _, err := s.Execute(ctx, ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE original_data(v)"}); err != nil {
					t.Fatal(err)
				}
			}
			before := core.Tip()
			var actualSlot quepaxa.Slot
			closed := make(chan error, 1)
			batcher := s.kvBatcher
			if kind == "sql" {
				batcherSQL := s.sqlBatcher
				previous := batcherSQL.apply
				batcherSQL.apply = func(ctx context.Context, slot quepaxa.Slot) error {
					if previous != nil {
						if err := previous(ctx, slot); err != nil {
							return err
						}
					}
					actualSlot = slot
					closed <- m.Close()
					return nil
				}
			} else {
				previous := batcher.apply
				batcher.apply = func(ctx context.Context, slot quepaxa.Slot) error {
					if previous != nil {
						if err := previous(ctx, slot); err != nil {
							return err
						}
					}
					actualSlot = slot
					closed <- m.Close()
					return nil
				}
			}
			id := "sql-original"
			kvRequest := KVMutationRequest{RequestID: string([]byte{255, 0}), Key: string([]byte{254}), Value: []byte("original")}
			var fingerprint [32]byte
			if kind == "sql" {
				_, err = s.ExecuteReturning(ctx, ExecuteRequest{RequestID: id, SQL: "INSERT INTO original_data VALUES(7) RETURNING v"})
				fingerprint, _ = types.SQLFingerprint(types.SQLCommand{RequestID: id, SQL: "INSERT INTO original_data VALUES(7) RETURNING v", WantRows: true})
			} else {
				id = kvRequest.RequestID
				_, err = s.KVPut(ctx, kvRequest)
				fingerprint, _ = types.ManagedKVFingerprint(types.KVCommand{RequestID: id, Operation: "put", Key: kvRequest.Key, Value: kvRequest.Value})
			}
			if closeErr := <-closed; closeErr != nil {
				t.Fatal(closeErr)
			}
			if ctx.Err() != nil {
				t.Fatal("caller context was canceled")
			}
			var unknown *CommitUnknownError
			if !errors.Is(err, ErrCommitUnknown) || errors.Is(err, ErrRequestConflict) || !errors.Is(err, sql.ErrConnDone) || !errors.As(err, &unknown) || unknown.RequestID != id || unknown.Slot != actualSlot {
				t.Fatalf("post-submit lookup failure lost uncertainty/original identity: slot=%d err=%v", actualSlot, err)
			}
			s.Close()
			reopened, openErr := materializer.OpenManagedSQLKV(path, 1, 1024)
			if openErr != nil {
				t.Fatal(openErr)
			}
			defer reopened.Close()
			var receipt types.MutationReceipt
			var found, matches bool
			if kind == "sql" {
				receipt, _, found, matches, err = reopened.SQLRequestResultFingerprint(ctx, id, fingerprint, true)
			} else {
				receipt, found, matches, err = reopened.KVRequestStatusFingerprint(ctx, id, fingerprint)
			}
			if err != nil || !found || !matches || receipt.Slot != uint64(actualSlot) || core.Tip() != before+1 || actualSlot != core.Tip() || reopened.Tip() != uint64(actualSlot) {
				t.Fatal("original application receipt/history not preserved", receipt, found, matches, err)
			}
			if kind == "sql" {
				result, _, err := reopened.QueryResultAt(ctx, "SELECT COUNT(*) FROM original_data", nil)
				if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != int64(1) {
					t.Fatal("SQL application duplicated", result, err)
				}
			} else {
				value, found, err := reopened.KVGet(ctx, kvRequest.Key, time.Now())
				if err != nil || !found || !bytes.Equal(value, kvRequest.Value) {
					t.Fatal("original KV data lost", err)
				}
			}
		})
	}
}

func TestManagedKVServerPostSubmitObservedMismatchIsConflict(t *testing.T) {
	for _, kind := range []string{"sql", "kv"} {
		t.Run(kind, func(t *testing.T) {
			s, m, core := managedKVRealServer(t, 8)
			ctx := context.Background()
			var winner []byte
			var err error
			calls := 0
			wrap := func(original func(context.Context, []byte) (quepaxa.Slot, error)) func(context.Context, []byte) (quepaxa.Slot, error) {
				return func(ctx context.Context, value []byte) (quepaxa.Slot, error) {
					calls++
					// A different actual ingress wins after precheck but before
					// this submit. Both values traverse Core/apply, not a fake receipt.
					if _, err := original(ctx, winner); err != nil {
						return 0, err
					}
					return original(ctx, value)
				}
			}
			if kind == "sql" {
				winner, err = types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "same", SQL: "CREATE TABLE winner(v)"}})
				if err != nil {
					t.Fatal(err)
				}
				s.sqlBatcher.propose = wrap(s.sqlBatcher.propose)
				_, err = s.Execute(ctx, ExecuteRequest{RequestID: "same", SQL: "CREATE TABLE loser(v)"})
			} else {
				winner, err = types.EncodeManagedKVCommand(types.KVCommand{RequestID: string([]byte{255}), Operation: "put", Key: "winner", Value: []byte("v")})
				if err != nil {
					t.Fatal(err)
				}
				s.kvBatcher.propose = wrap(s.kvBatcher.propose)
				_, err = s.KVPut(ctx, KVMutationRequest{RequestID: string([]byte{255}), Key: "loser", Value: []byte("v")})
			}
			s.sqlBatcher.Close()
			s.kvBatcher.Close()
			if !errors.Is(err, ErrRequestConflict) || errors.Is(err, ErrCommitUnknown) || calls != 1 || core.Tip() != 2 || m.Tip() != 2 {
				t.Fatal("actual observed mismatch lost or resubmitted", err, calls, core.Tip())
			}
		})
	}
}

func TestManagedKVServerPrecheckComposition(t *testing.T) {
	s, m, core := managedKVRealServer(t, 8)
	ctx := context.Background()
	intent := types.KVCommand{RequestID: string([]byte{255}), Operation: "put", Key: string([]byte{254}), Value: []byte("original")}
	unlock, err := s.lockRequest(ctx, intent.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	prematch, err := m.KVRequestMatches(ctx, intent)
	if err != nil || !prematch {
		t.Fatal("initial unused request", prematch, err)
	}
	foreign := intent
	foreign.Key = "different"
	value, err := types.EncodeManagedKVCommand(foreign)
	if err != nil {
		t.Fatal(err)
	}
	slot, _, err := core.Propose(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.applyDecisions(ctx, slot); err != nil {
		t.Fatal(err)
	}
	receipt, found, err := m.MutationReceipt(ctx, types.MutationKV, intent.RequestID)
	if err != nil || !found {
		t.Fatal("real intervening receipt absent", err)
	}
	// Preserve the actual old-composition counterexample, not a claim that this
	// deterministically schedules an uninstrumented Server between two calls.
	if !prematch || !found || receipt.Slot != uint64(slot) {
		t.Fatal("old split counterexample was not established")
	}
	fingerprint, err := types.ManagedKVFingerprint(intent)
	if err != nil {
		t.Fatal(err)
	}
	observed, atomicFound, matches, err := m.KVRequestStatusFingerprint(ctx, intent.RequestID, fingerprint)
	if err != nil || !atomicFound || matches || observed.Slot != receipt.Slot {
		t.Fatal("atomic observation did not distinguish intent", observed, err)
	}
	unlock()
	unlock = nil
	before := core.Tip()
	if _, err := s.KVPut(ctx, KVMutationRequest{RequestID: intent.RequestID, Key: intent.Key, Value: intent.Value}); !errors.Is(err, ErrRequestConflict) || core.Tip() != before {
		t.Fatal("actual Server precheck returned wrong receipt or resubmitted", err)
	}
}
