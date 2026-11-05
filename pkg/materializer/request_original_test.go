package materializer

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/ncruces/go-sqlite3/driver"
)

func requestMaterializer(t *testing.T) *Materializer {
	t.Helper()
	m, err := OpenManagedSQLKVWithOriginals(filepath.Join(t.TempDir(), "db.sqlite"), 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	return m
}

func requestSQL(t *testing.T, m *Materializer, slot uint64, commands ...types.SQLCommand) {
	t.Helper()
	value, err := types.EncodeSQLBatch(commands)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), slot, value); err != nil {
		t.Fatal(err)
	}
}

func requestKV(t *testing.T, m *Materializer, slot uint64, commands ...types.KVCommand) {
	t.Helper()
	var items [][]byte
	for _, command := range commands {
		item, err := types.EncodeManagedKVBatchItem(command)
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, item)
	}
	value, err := types.AssembleManagedKVBatch(items)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), slot, value); err != nil {
		t.Fatal(err)
	}
}

func retainedOriginal(t *testing.T, m *Materializer, kind types.MutationKind, id string) RequestOriginal {
	t.Helper()
	out, found, err := m.RetainedRequestOriginal(context.Background(), kind, id)
	if err != nil || !found {
		t.Fatalf("original missing: kind=%d found=%v err=%v", kind, found, err)
	}
	return out
}

func TestRequestOriginalFirstEffectAndResult(t *testing.T) {
	m := requestMaterializer(t)
	requestSQL(t, m, 1, types.SQLCommand{RequestID: "schema", SQL: `CREATE TABLE item(id INTEGER PRIMARY KEY,v BLOB)`})
	insert := types.SQLCommand{RequestID: "insert", SQL: `INSERT INTO item VALUES(1,?) RETURNING id`, Args: []any{[]byte{0, 255}}, WantRows: true}
	requestSQL(t, m, 2, insert, types.SQLCommand{RequestID: "other", SQL: `INSERT INTO item VALUES(2,NULL)`})
	original := retainedOriginal(t, m, types.MutationSQL, "insert")
	want, err := types.EncodeSQLBatchItem(insert)
	if err != nil || !bytes.Equal(want, original.ExecutionItem) || original.Slot != 2 || original.BatchIndex != 0 || len(original.SQLResult.Statements) != 1 || len(original.SQLResult.Statements[0].Rows) != 1 {
		t.Fatalf("SQL original/result mismatch: %+v %v", original, err)
	}
	if got := retainedOriginal(t, m, types.MutationSQL, "other"); got.Slot != 2 || got.BatchIndex != 1 {
		t.Fatal("batch position lost")
	}
	requestSQL(t, m, 3, types.SQLCommand{RequestID: "rejected", SQL: `INSERT INTO absent VALUES(1)`})
	if got := retainedOriginal(t, m, types.MutationSQL, "rejected"); got.Receipt.Status != types.MutationRejected || got.SQL == nil {
		t.Fatal("rejected original missing")
	}
	first := types.KVCommand{RequestID: string([]byte{255}), Key: string([]byte{254}), Operation: "put", Value: []byte{}, Expected: nil, TTLMS: 100, ObservedAtUnixMS: 10, ExpiresAtUnixMS: 110}
	second := types.KVCommand{RequestID: string([]byte{254}), Key: "nil", Operation: "put", Value: nil, Expected: []byte{}}
	requestKV(t, m, 4, first, second)
	kv := retainedOriginal(t, m, types.MutationKV, first.RequestID)
	if kv.Slot != 4 || kv.BatchIndex != 0 || kv.KV == nil || kv.KV.Value == nil || kv.KV.Expected != nil || kv.KV.ObservedAtUnixMS != 10 || kv.KV.ExpiresAtUnixMS != 110 {
		t.Fatalf("KV original changed: %+v", kv)
	}
	if got := retainedOriginal(t, m, types.MutationKV, second.RequestID); got.KV.Value != nil || got.KV.Expected == nil || got.BatchIndex != 1 {
		t.Fatal("nil/empty or position lost")
	}
	// Decided duplicate/conflicting commands must not stop prefix application.
	requestSQL(t, m, 5, types.SQLCommand{RequestID: "insert", SQL: `DELETE FROM item`})
	changed := first
	changed.Key = "must-not-execute"
	changed.Value = []byte("changed")
	requestKV(t, m, 6, changed)
	if got := retainedOriginal(t, m, types.MutationSQL, "insert"); !bytes.Equal(got.ExecutionItem, want) || got.Slot != 2 {
		t.Fatal("duplicate SQL overwrote first original")
	}
	if got := retainedOriginal(t, m, types.MutationKV, first.RequestID); !bytes.Equal(got.ExecutionItem, kv.ExecutionItem) || got.Slot != 4 {
		t.Fatal("conflict overwrote first KV original")
	}
	var count int
	if err := m.writer.QueryRow(`SELECT COUNT(*) FROM item`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("duplicate effect: %d %v", count, err)
	}
	original.ExecutionItem[0] = '?'
	original.SQL.Args[0].([]byte)[0] = 17
	if got := retainedOriginal(t, m, types.MutationSQL, "insert"); !bytes.Equal(got.ExecutionItem, want) {
		t.Fatal("returned memory aliases stored original")
	}
	if _, err := m.InspectManagedApplicationState(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRequestOriginalCheckpointReplayAndReopen(t *testing.T) {
	ctx := context.Background()
	m := requestMaterializer(t)
	requestKV(t, m, 1, types.KVCommand{RequestID: "before", Key: "key", Operation: "put", Value: []byte("before"), TTLMS: 1000, ObservedAtUnixMS: 10, ExpiresAtUnixMS: 1010})
	files, tip, cleanup, err := m.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if tip != 1 {
		t.Fatal("wrong checkpoint tip")
	}
	n := requestMaterializer(t)
	if err := n.RestoreCheckpoint(ctx, files); err != nil {
		t.Fatal(err)
	}
	if got := retainedOriginal(t, n, types.MutationKV, "before"); got.Slot != 1 {
		t.Fatal("checkpoint lost original")
	}
	// This is an actual same-slot suffix Apply, not a replacement proposal.
	suffix := types.KVCommand{RequestID: "suffix", Key: "key", Operation: "cas", Expected: []byte("wrong"), ExpectedExists: true, Value: []byte("after"), ObservedAtUnixMS: 20}
	requestKV(t, n, 2, suffix)
	requestKV(t, m, 2, suffix)
	if got := retainedOriginal(t, n, types.MutationKV, "suffix"); got.Receipt.Applied || got.Slot != 2 {
		t.Fatal("suffix false CAS changed")
	}
	path := n.dbPath
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenManagedSQLKVWithOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := retainedOriginal(t, reopened, types.MutationKV, "before"); got.Slot != 1 {
		t.Fatal("reopen lost original")
	}
	if got := retainedOriginal(t, reopened, types.MutationKV, "suffix"); got.KV.ObservedAtUnixMS != 20 || got.Receipt.Applied {
		t.Fatal("reopen changed metadata/result")
	}
	for _, id := range []string{"before", "suffix"} {
		if !reflect.DeepEqual(retainedOriginal(t, m, types.MutationKV, id), retainedOriginal(t, reopened, types.MutationKV, id)) {
			t.Fatalf("whole replay and checkpoint/suffix/reopen original differ: %s", id)
		}
	}
	whole, err := m.InspectManagedApplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := reopened.InspectManagedApplicationState(ctx)
	if err != nil || whole != restored {
		t.Fatalf("whole replay and restored evidence differ: %v", err)
	}
}

func TestRequestOriginalLogicalAndPhysicalPruning(t *testing.T) {
	m := requestMaterializer(t)
	requestKV(t, m, 1, types.KVCommand{RequestID: "expired", Key: "key", Operation: "put", Value: []byte("value")})
	var controls []quepaxa.DecidedValue
	for slot := 2; slot <= 1024; slot++ {
		controls = append(controls, quepaxa.DecidedValue{Slot: quepaxa.Slot(slot), Value: types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})})
	}
	if err := m.ApplyBatch(context.Background(), controls); err != nil {
		t.Fatal(err)
	}
	boundary := retainedOriginal(t, m, types.MutationKV, "expired")
	if boundary.Slot != 1 || boundary.Receipt.RetryThroughSlot != 1024 {
		t.Fatal("original expired before its inclusive retained-window end")
	}
	if receipt, found, matches, err := m.KVRequestStatusFingerprint(context.Background(), "expired", boundary.Fingerprint); err != nil || !found || !matches || receipt != boundary.Receipt {
		t.Fatalf("receipt missing at inclusive retained-window end: found=%t matches=%t error=%v", found, matches, err)
	}
	for slot := uint64(1025); slot <= 1026; slot++ {
		if err := m.Apply(context.Background(), slot, types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{})); err != nil {
			t.Fatal(err)
		}
		if _, found, err := m.RetainedRequestOriginal(context.Background(), types.MutationKV, "expired"); err != nil || found {
			t.Fatalf("original remains visible immediately after slot %d: found=%t error=%v", slot, found, err)
		}
		if _, found, _, err := m.KVRequestStatusFingerprint(context.Background(), "expired", boundary.Fingerprint); err != nil || found {
			t.Fatalf("receipt remains visible immediately after slot %d: found=%t error=%v", slot, found, err)
		}
	}
	if _, found, err := m.RetainedRequestOriginal(context.Background(), types.MutationKV, "expired"); err != nil || found {
		t.Fatalf("logical expiry revived result: %v %v", found, err)
	}
	var count int
	if err := m.writer.QueryRow(`SELECT COUNT(*) FROM _rhiza_request_original`).Scan(&count); err != nil || count != 1 {
		t.Fatal("control unexpectedly pruned original")
	}
	files, _, cleanup, err := m.CheckpointFilesAt(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	restored := requestMaterializer(t)
	if err := restored.RestoreCheckpoint(context.Background(), files); err != nil {
		t.Fatal(err)
	}
	if _, found, err := restored.RetainedRequestOriginal(context.Background(), types.MutationKV, "expired"); err != nil || found {
		t.Fatalf("checkpoint revived expired original: %v %v", found, err)
	}
	requestKV(t, m, 1027, types.KVCommand{RequestID: "next", Key: "key", Operation: "delete"})
	if err := m.writer.QueryRow(`SELECT COUNT(*) FROM _rhiza_request_original WHERE request_id='expired'`).Scan(&count); err != nil || count != 0 {
		t.Fatal("original not pruned with receipt")
	}
	if _, err := m.InspectManagedApplicationState(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRequestOriginalConflictingItemKeepsActualBatchOrdinal(t *testing.T) {
	m := requestMaterializer(t)
	requestSQL(t, m, 1, types.SQLCommand{RequestID: "schema", SQL: `CREATE TABLE batch_item(v INTEGER)`})
	a := types.SQLCommand{RequestID: "A", SQL: `INSERT INTO batch_item VALUES(1)`}
	conflict := types.SQLCommand{RequestID: "A", SQL: `DELETE FROM batch_item`}
	b := types.SQLCommand{RequestID: "B", SQL: `INSERT INTO batch_item VALUES(2)`}
	requestSQL(t, m, 2, a, conflict, b)
	for _, tc := range []struct {
		command types.SQLCommand
		index   uint32
	}{{a, 0}, {b, 2}} {
		got := retainedOriginal(t, m, types.MutationSQL, tc.command.RequestID)
		want, err := types.EncodeSQLBatchItem(tc.command)
		if err != nil || got.Slot != 2 || got.BatchIndex != tc.index || !bytes.Equal(got.ExecutionItem, want) || got.Receipt.Status != types.MutationCommitted {
			t.Fatalf("certified item ordinal/original changed: index=%d error=%v", tc.index, err)
		}
	}
	var count, sum int
	if err := m.writer.QueryRow(`SELECT COUNT(*),SUM(v) FROM batch_item`).Scan(&count, &sum); err != nil || count != 2 || sum != 3 {
		t.Fatalf("conflicting item changed effects or stopped B: count=%d sum=%d error=%v", count, sum, err)
	}
	ka := types.KVCommand{RequestID: "KA", Operation: "put", Key: "first", Value: []byte("A"), TTLMS: 100, ObservedAtUnixMS: 10, ExpiresAtUnixMS: 110}
	kconflict := ka
	kconflict.Key, kconflict.Value, kconflict.ObservedAtUnixMS, kconflict.ExpiresAtUnixMS = "forbidden", []byte("changed"), 20, 120
	kb := types.KVCommand{RequestID: "KB", Operation: "put", Key: "second", Value: []byte("B")}
	requestKV(t, m, 3, ka, kconflict, kb)
	for _, tc := range []struct {
		command types.KVCommand
		index   uint32
	}{{ka, 0}, {kb, 2}} {
		got := retainedOriginal(t, m, types.MutationKV, tc.command.RequestID)
		want, err := types.EncodeManagedKVBatchItem(tc.command)
		if err != nil || got.Slot != 3 || got.BatchIndex != tc.index || !bytes.Equal(got.ExecutionItem, want) || !got.Receipt.Applied {
			t.Fatalf("certified KV ordinal/first metadata changed: index=%d error=%v", tc.index, err)
		}
	}
	if value, found, err := m.KVGet(context.Background(), "first", time.UnixMilli(20)); err != nil || !found || string(value) != "A" {
		t.Fatalf("first KV effect changed: found=%t error=%v", found, err)
	}
	if _, found, err := m.KVGet(context.Background(), "forbidden", time.UnixMilli(20)); err != nil || found {
		t.Fatalf("conflicting KV effect executed: found=%t error=%v", found, err)
	}
	if _, err := m.InspectManagedApplicationState(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRequestOriginalMalformedAndPoison(t *testing.T) {
	for _, query := range []string{
		`UPDATE _rhiza_request_original SET execution=X'00'`,
		`UPDATE _rhiza_request_original SET commit_slot=2`,
		`UPDATE _rhiza_request_original SET batch_index=64`,
		`UPDATE _rhiza_idempotency SET fingerprint=zeroblob(32)`,
		`DELETE FROM _rhiza_request_original`,
	} {
		t.Run(query, func(t *testing.T) {
			m := requestMaterializer(t)
			requestKV(t, m, 1, types.KVCommand{RequestID: "first", Key: "key", Operation: "put"})
			if _, err := m.writer.Exec(query); err != nil {
				t.Fatal(err)
			}
			if _, _, err := m.RetainedRequestOriginal(context.Background(), types.MutationKV, "first"); err == nil {
				t.Fatal("malformed projection accepted")
			}
			if _, err := m.InspectManagedApplicationState(context.Background()); err == nil {
				t.Fatal("full evidence accepted malformed projection")
			}
		})
	}
	m := requestMaterializer(t)
	m.afterApplyCommitError = func() error { return errors.New("lost commit response") }
	value, err := types.EncodeManagedKVCommand(types.KVCommand{RequestID: "unknown", Key: "key", Operation: "put"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 1, value); !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("commit uncertainty lost: %v", err)
	}
	if _, _, err := m.RetainedRequestOriginal(context.Background(), types.MutationKV, "unknown"); !errors.Is(err, ErrGraphCommitUnknown) {
		t.Fatalf("poison bypassed: %v", err)
	}
	path := m.dbPath
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenManagedSQLKVWithOriginals(path, 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := retainedOriginal(t, reopened, types.MutationKV, "unknown"); got.Slot != 1 {
		t.Fatal("actual committed original lost on reopen")
	}
}

func TestRequestOriginalProfileBoundary(t *testing.T) {
	m := requestMaterializer(t)
	if m.MaterializationPolicy() != sqlpolicy.ManagedRequestMaterializationPolicy {
		t.Fatal("wrong profile")
	}
	if old, err := OpenManagedSQLKV(m.dbPath, 1, 1024); err == nil {
		old.Close()
		t.Fatal("profile 8 opened profile 9")
	}
	old := managedMaterializer(t)
	if _, _, err := old.RetainedRequestOriginal(context.Background(), types.MutationKV, "x"); !errors.Is(err, sqlpolicy.ErrIncompatible) {
		t.Fatal("profile 8 lookup enabled")
	}
	if upgraded, err := OpenManagedSQLKVWithOriginals(old.dbPath, 1, 1024); err == nil {
		upgraded.Close()
		t.Fatal("profile 8 silently upgraded")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.RetainedRequestOriginal(context.Background(), types.MutationKV, "x"); err == nil {
		t.Fatal("closed lookup accepted")
	}
}

func TestRequestOriginalInsertFailureRollsBackEffectAndReceipt(t *testing.T) {
	m := requestMaterializer(t)
	requestSQL(t, m, 1, types.SQLCommand{RequestID: "schema", SQL: `CREATE TABLE item(v INTEGER)`})
	// Install/remove the test-only TEMP fixture through the existing native
	// connection seam. Restore the real writer authorizer before any Apply.
	fixtureDDL := func(query string) error {
		conn, err := m.writer.Conn(context.Background())
		if err != nil {
			return err
		}
		err = conn.Raw(func(raw any) error { return raw.(driver.Conn).Raw().SetAuthorizer(nil) })
		if err != nil {
			return errors.Join(err, conn.Close())
		}
		_, execErr := conn.ExecContext(context.Background(), query)
		restoreErr := conn.Raw(func(raw any) error {
			return setSQLAuthorizer(raw.(driver.Conn).Raw(), true, false, false)
		})
		return errors.Join(execErr, restoreErr, conn.Close())
	}
	if err := fixtureDDL(`CREATE TEMP TRIGGER fail_original BEFORE INSERT ON main._rhiza_request_original WHEN NEW.request_id='failure' BEGIN SELECT RAISE(ABORT,'forced projection failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := m.writer.Exec(`CREATE TEMP TABLE forbidden_fixture_escape(v)`); err == nil || !strings.Contains(strings.ToLower(err.Error()), "authoriz") {
		t.Fatalf("writer TEMP protection was not restored: %v", err)
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "failure", SQL: `INSERT INTO item VALUES(17)`}})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(context.Background(), 2, value); err == nil || !strings.Contains(err.Error(), "forced projection failure") {
		t.Fatalf("expected actual projection insert failure, got %v", err)
	}
	var effects, receipts, originals int
	if err := m.writer.QueryRow(`SELECT (SELECT COUNT(*) FROM item),(SELECT COUNT(*) FROM _rhiza_idempotency WHERE request_id='failure'),(SELECT COUNT(*) FROM _rhiza_request_original WHERE request_id='failure')`).Scan(&effects, &receipts, &originals); err != nil {
		t.Fatal(err)
	}
	if effects != 0 || receipts != 0 || originals != 0 || m.Tip() != 1 {
		t.Fatal("partial effect/receipt/original committed")
	}
	if err := fixtureDDL(`DROP TRIGGER fail_original`); err != nil {
		t.Fatal(err)
	}
	requestSQL(t, m, 2, types.SQLCommand{RequestID: "neighbor", SQL: `INSERT INTO item VALUES(23)`})
	if got := retainedOriginal(t, m, types.MutationSQL, "neighbor"); got.Slot != 2 || got.Receipt.Status != types.MutationCommitted {
		t.Fatal("valid neighbor lost its original/receipt after rollback")
	}
	var valueAfter int
	if err := m.writer.QueryRow(`SELECT v FROM item`).Scan(&valueAfter); err != nil || valueAfter != 23 {
		t.Fatalf("valid neighbor effect differs: value=%d error=%v", valueAfter, err)
	}
	if _, err := m.InspectManagedApplicationState(context.Background()); err != nil {
		t.Fatal(err)
	}
}
