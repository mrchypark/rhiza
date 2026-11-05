package materializer

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
)

func TestManagedApplicationEvidenceTamper(t *testing.T) {
	ctx := context.Background()
	for _, query := range []string{
		`UPDATE _rhiza_kv SET rowid=17`,
		`UPDATE _rhiza_kv SET value=X'00ff'`,
		`UPDATE _rhiza_kv SET key=CAST(X'ff' AS TEXT)`,
		`UPDATE _rhiza_kv SET expires_at_unix_ms=2`,
		`UPDATE _rhiza_idempotency SET fingerprint=zeroblob(32)`,
		`UPDATE _rhiza_idempotency SET applied=0`,
		`UPDATE _rhiza_idempotency SET request_id=X'ff00'`,
	} {
		t.Run(query, func(t *testing.T) {
			m := managedMaterializer(t)
			managedKVApply(t, m, types.KVCommand{RequestID: "kv", Operation: "put", Key: "key", Value: []byte("value"), ExpiresAtUnixMS: 1})
			before, err := m.InspectManagedApplicationState(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := m.writer.Exec(query); err != nil {
				t.Fatal(err)
			}
			state, inspectErr := m.InspectManagedApplicationState(ctx)
			if inspectErr != nil || state.Digest == before.Digest {
				t.Fatalf("tamper not bound %s: %+v %v", query, state, inspectErr)
			}
		})
	}
	m := managedMaterializer(t)
	// File inspection and live inspection must describe the same stable state.
	live, err := m.InspectManagedApplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	file, tip, err := InspectManagedApplicationFile(ctx, m.dbPath, 1024)
	if err != nil || tip != m.tip || file != live {
		t.Fatalf("file mismatch: %+v %+v %d %v", live, file, tip, err)
	}
}

func TestManagedApplicationEvidenceCompleteSQLReceipt(t *testing.T) {
	for _, query := range []string{
		`UPDATE _rhiza_idempotency SET commit_slot=2 WHERE request_id='schema'`,
		`UPDATE _rhiza_idempotency SET rows_affected=7 WHERE request_id='schema'`,
		`UPDATE _rhiza_idempotency SET last_insert_id=-17 WHERE request_id='schema'`,
		`UPDATE _rhiza_idempotency SET status='rejected',error_code='execution_failed' WHERE request_id='schema'`,
	} {
		t.Run(query, func(t *testing.T) {
			m := managedMaterializer(t)
			evidenceApply(t, m, 1, "schema", `CREATE TABLE item (v INTEGER)`)
			managedKVApply(t, m, types.KVCommand{RequestID: "kv", Operation: "delete", Key: "absent"})
			before, err := m.InspectManagedApplicationState(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := m.writer.Exec(query); err != nil {
				t.Fatal(err)
			}
			after, err := m.InspectManagedApplicationState(context.Background())
			if err != nil || after.Digest == before.Digest {
				t.Fatalf("receipt field omitted: %+v %v", after, err)
			}
		})
	}
}

func TestManagedApplicationEvidenceInvalidReceipt(t *testing.T) {
	for _, query := range []string{
		`UPDATE _rhiza_idempotency SET kind=257`,
		`UPDATE _rhiza_idempotency SET fingerprint=X'00'`,
		`UPDATE _rhiza_idempotency SET commit_slot=0`,
		`UPDATE _rhiza_idempotency SET commit_slot=2`,
		`UPDATE _rhiza_idempotency SET applied=2`,
		`UPDATE _rhiza_idempotency SET status='rejected'`,
		`UPDATE _rhiza_idempotency SET error_code='bad'`,
		`UPDATE _rhiza_idempotency SET rows_affected=1`,
		`UPDATE _rhiza_idempotency SET last_insert_id=1`,
		`UPDATE _rhiza_idempotency SET sql_result=X'00'`,
	} {
		t.Run(query, func(t *testing.T) {
			m := managedMaterializer(t)
			managedKVApply(t, m, types.KVCommand{RequestID: "kv", Operation: "delete", Key: "absent"})
			// Ignore CHECK constraints only for the malformed-fingerprint fixture.
			if _, err := m.writer.Exec(`PRAGMA ignore_check_constraints=ON`); err != nil {
				t.Fatal(err)
			}
			if _, err := m.writer.Exec(query); err != nil {
				t.Fatal(err)
			}
			if _, err := m.InspectManagedApplicationState(context.Background()); err == nil {
				t.Fatal("invalid receipt accepted")
			}
		})
	}
}

func TestManagedApplicationEvidenceRawText(t *testing.T) {
	m := managedMaterializer(t)
	evidenceApply(t, m, 1, "schema", `CREATE TABLE text_values(v TEXT)`)
	if _, err := m.writer.Exec(`INSERT INTO text_values(v) VALUES(CAST(X'ff' AS TEXT))`); err != nil {
		t.Fatal(err)
	}
	first, err := m.InspectManagedApplicationState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.writer.Exec(`UPDATE text_values SET v=CAST(X'fe' AS TEXT)`); err != nil {
		t.Fatal(err)
	}
	second, err := m.InspectManagedApplicationState(context.Background())
	if err != nil || first.Digest == second.Digest {
		t.Fatalf("invalid UTF-8 collapsed: %v", err)
	}
}

func TestManagedApplicationEvidenceInvalidSQLReceipt(t *testing.T) {
	for _, query := range []string{
		`UPDATE _rhiza_idempotency SET applied=1`,
		`UPDATE _rhiza_idempotency SET status='unknown'`,
		`UPDATE _rhiza_idempotency SET error_code='unexpected'`,
		`UPDATE _rhiza_idempotency SET status='rejected',error_code='execution_failed',rows_affected=1`,
		`UPDATE _rhiza_idempotency SET sql_result=X'ff'`,
		`UPDATE _rhiza_meta SET value='99' WHERE key='sql_receipt_count'`,
		`UPDATE _rhiza_meta SET value='bad' WHERE key='state_slot'`,
		`UPDATE _rhiza_meta SET value='00' WHERE key='applied_hash'`,
		`INSERT INTO _rhiza_meta VALUES('unknown_policy','unaccounted')`,
	} {
		t.Run(query, func(t *testing.T) {
			m := managedMaterializer(t)
			evidenceApply(t, m, 1, "schema", `CREATE TABLE item (v INTEGER)`)
			if _, err := m.writer.Exec(query); err != nil {
				t.Fatal(err)
			}
			if _, err := m.InspectManagedApplicationState(context.Background()); err == nil {
				t.Fatal("invalid SQL receipt/accounting accepted")
			}
		})
	}
}

func TestManagedApplicationEvidenceMissingPolicy(t *testing.T) {
	for _, key := range []string{"managed_application_protocol", "managed_application_policy"} {
		t.Run(key, func(t *testing.T) {
			m := managedMaterializer(t)
			if _, err := m.writer.Exec(`DELETE FROM _rhiza_meta WHERE key=?`, key); err != nil {
				t.Fatal(err)
			}
			if _, err := m.InspectManagedApplicationState(context.Background()); err == nil {
				t.Fatal("missing policy accepted")
			}
			path := m.dbPath
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			if reopened, err := OpenManagedSQLKV(path, 1, 1024); err == nil {
				reopened.Close()
				t.Fatal("missing policy reopened")
			}
			if ordinary, err := Open(path, 1, 1024); err == nil {
				ordinary.Close()
				t.Fatal("orphan mixed policy downgraded")
			}
		})
	}
}

func TestManagedApplicationEvidenceGraphHistoryAndUncertainty(t *testing.T) {
	for _, state := range []string{"history", "pending", "missing-journal", "uncertain"} {
		t.Run(state, func(t *testing.T) {
			m := managedMaterializer(t)
			if state == "uncertain" {
				m.commitUnknown = true
			} else {
				if err := os.Mkdir(filepath.Join(filepath.Dir(m.dbPath), "latticedb"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := m.InspectManagedApplicationState(context.Background()); err == nil {
				t.Fatal("unsupported graph state accepted")
			}
		})
	}
}

func TestManagedApplicationEvidenceUnknownGraphState(t *testing.T) {
	for _, kind := range []string{"metadata", "stream"} {
		t.Run(kind, func(t *testing.T) {
			m := managedMaterializer(t)
			path := filepath.Join(filepath.Dir(m.dbPath), "latticedb")
			if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(path, kind), []byte("retained"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := m.InspectManagedApplicationState(context.Background()); err == nil {
				t.Fatal("unknown LatticeDB application state was not rejected")
			}
		})
	}
}

func TestManagedApplicationEvidencePolicyAndGraphRefusal(t *testing.T) {
	for _, key := range []string{"managed_application_protocol", "managed_application_policy", "sql_idempotency_window", "sql_execution_policy"} {
		t.Run(key, func(t *testing.T) {
			m := managedMaterializer(t)
			if _, err := m.writer.Exec(`UPDATE _rhiza_meta SET value='wrong' WHERE key=?`, key); err != nil {
				t.Fatal(err)
			}
			if _, err := m.InspectManagedApplicationState(context.Background()); err == nil {
				t.Fatal("policy mismatch accepted")
			}
			path := m.dbPath
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			if reopened, err := OpenManagedSQLKV(path, 1, 1024); err == nil {
				reopened.Close()
				t.Fatal("open accepted wrong policy")
			}
		})
	}
	m := managedMaterializer(t)
	if err := os.Mkdir(filepath.Join(filepath.Dir(m.dbPath), "latticedb"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := m.InspectManagedApplicationState(context.Background()); err == nil {
		t.Fatal("actual graph data accepted")
	}
	if _, _, err := InspectManagedApplicationFile(context.Background(), m.dbPath, 1024); err == nil {
		t.Fatal("graph file data accepted")
	}
	// A valid mixed SQLite snapshot cannot silently import a legacy companion.
	path := m.dbPath
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := OpenManagedSQLKV(path, 1, 1024); err == nil {
		reopened.Close()
		t.Fatal("nonempty graph reopened")
	}
}
