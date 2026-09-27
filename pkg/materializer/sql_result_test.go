package materializer

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/ncruces/go-sqlite3"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
)

func TestSQLiteColumnLimitPreservesEngineDefault(t *testing.T) {
	native, err := sqlite3.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	if got := native.Limit(sqlite3.LIMIT_COLUMN, -1); got != MaxSQLResultColumns {
		t.Fatalf("engine default=%d; review replay compatibility before changing the contract", got)
	}
	for _, writer := range []bool{false, true} {
		db, err := openSQLite(":memory:", writer)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		db.SetMaxIdleConns(0) // Each iteration initializes a new physical connection.
		for i := 0; i < 2; i++ {
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			err = conn.Raw(func(raw any) error {
				sqlite := raw.(sqlite3driver.Conn).Raw()
				if sqlite.Limit(sqlite3.LIMIT_COLUMN, -1) != 2000 || sqlite.Limit(sqlite3.LIMIT_VARIABLE_NUMBER, -1) != 999 {
					return fmt.Errorf("connection limits changed")
				}
				return nil
			})
			conn.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, count := range []int{2000, 2001} {
		columns := make([]string, count)
		for i := range columns {
			columns[i] = fmt.Sprintf("c%d INTEGER", i)
		}
		err := native.Exec("CREATE TABLE wide" + fmt.Sprint(count) + "(" + strings.Join(columns, ",") + ")")
		if (err != nil) != (count == 2001) {
			t.Fatalf("table columns=%d err=%v", count, err)
		}
	}
}

func TestSQLResultCodecBoundaries(t *testing.T) {
	for _, count := range []int{MaxSQLStatements, MaxSQLStatements + 1} {
		_, err := encodeSQLResult(types.SQLCommandResult{Statements: make([]types.SQLStatementResult, count)})
		if (err != nil) != (count > MaxSQLStatements) {
			t.Fatalf("statements=%d err=%v", count, err)
		}
	}
	for _, count := range []int{MaxReturningRows, MaxReturningRows + 1} {
		_, err := encodeSQLResult(types.SQLCommandResult{Statements: []types.SQLStatementResult{{Rows: make([][]any, count)}}})
		if (err != nil) != (count > MaxReturningRows) {
			t.Fatalf("rows=%d err=%v", count, err)
		}
	}
	for _, statement := range []types.SQLStatementResult{
		{Columns: make([]string, MaxSQLResultColumns+1)},
		{Columns: []string{"x"}, Rows: [][]any{{}}},
		{Rows: [][]any{{nil}}},
	} {
		if _, err := encodeSQLResult(types.SQLCommandResult{Statements: []types.SQLStatementResult{statement}}); err == nil {
			t.Fatal("invalid result structure accepted")
		}
	}
	if encoded, err := encodeSQLResult(types.SQLCommandResult{}); err != nil || len(encoded) != 0 {
		t.Fatal("empty receipt sentinel changed")
	}
	if _, err := encodeSQLResult(types.SQLCommandResult{Error: "lost"}); err == nil {
		t.Fatal("error-only result silently discarded")
	}
	for _, delta := range []int{-1, 0, 1} {
		// RSQL + statement count + empty statement + error length = 36 bytes.
		result := types.SQLCommandResult{Statements: []types.SQLStatementResult{{}}, Error: strings.Repeat("e", MaxMutationResultBytes-36+delta)}
		encoded, err := encodeSQLResult(result)
		if delta > 0 {
			if err == nil {
				t.Fatal("oversized encoding accepted")
			}
			continue
		}
		if err != nil || len(encoded) != MaxMutationResultBytes+delta {
			t.Fatalf("size=%d delta=%d err=%v", len(encoded), delta, err)
		}
		if _, err := decodeSQLResult(encoded); err != nil {
			t.Fatal(err)
		}
	}
	valid, err := encodeSQLResult(types.SQLCommandResult{Statements: []types.SQLStatementResult{{Columns: []string{""}, Rows: [][]any{{nil}}}}})
	if err != nil {
		t.Fatal(err)
	}
	badWidth := bytes.Clone(valid)
	binary.BigEndian.PutUint32(badWidth[36:40], 2)
	badTag := bytes.Clone(valid)
	badTag[40] = 255
	for _, encoded := range [][]byte{
		valid[:len(valid)-1], append(bytes.Clone(valid), 0), badWidth, badTag,
		make([]byte, MaxMutationResultBytes+1), legacyWideSQLResult(MaxSQLResultColumns + 1),
	} {
		if _, err := decodeSQLResult(encoded); err == nil {
			t.Fatal("malformed encoding accepted")
		}
	}
}

func TestOversizedSQLResultRollsBackOnlyItsCommand(t *testing.T) {
	ctx := context.Background()
	m, err := Open(t.TempDir()+"/oversized.db", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	commands := []types.SQLCommand{
		{RequestID: "oversized", Statements: []types.SQLStatement{
			{SQL: "CREATE TABLE rolled_back (n INTEGER)"},
			{SQL: "SELECT zeroblob(?)", Args: []any{MaxMutationResultBytes}, WantRows: true},
		}},
		{RequestID: "independent", SQL: "CREATE TABLE committed (n INTEGER)"},
	}
	value, err := types.EncodeSQLBatch(commands)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(ctx, 1, value); err != nil {
		t.Fatal(err)
	}
	for i, command := range commands {
		fingerprint, _ := types.SQLFingerprint(command)
		receipt, _, found, matches, err := m.SQLRequestResultFingerprint(ctx, command.RequestID, fingerprint, true)
		want := types.MutationRejected
		if i == 1 {
			want = types.MutationCommitted
		}
		if err != nil || !found || !matches || receipt.Status != want {
			t.Fatalf("%s receipt=%+v err=%v", command.RequestID, receipt, err)
		}
	}
	var count int
	if err := m.db.QueryRow("SELECT count(*) FROM sqlite_schema WHERE name = 'rolled_back'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed command was not rolled back: count=%d err=%v", count, err)
	}
}

// Independent v0.18 RSQL layout, not generated through the encoder under test.
func legacyWideSQLResult(columns int) []byte {
	encoded := []byte("RSQL")
	encoded = binary.BigEndian.AppendUint32(encoded, 1)
	encoded = binary.BigEndian.AppendUint64(encoded, 0)
	encoded = binary.BigEndian.AppendUint64(encoded, 0)
	encoded = binary.BigEndian.AppendUint32(encoded, uint32(columns))
	for i := 0; i < columns; i++ {
		encoded = binary.BigEndian.AppendUint32(encoded, 0) // Empty names are valid.
	}
	encoded = binary.BigEndian.AppendUint32(encoded, 0) // Zero rows retain columns.
	return binary.BigEndian.AppendUint32(encoded, 0)    // No error text.
}

func TestLegacyWideSQLReceipt(t *testing.T) {
	for _, columns := range []int{1000, 2000} {
		encoded := legacyWideSQLResult(columns)
		result, err := decodeSQLResult(encoded)
		if err != nil || len(result.Statements[0].Columns) != columns {
			t.Fatalf("legacy columns=%d err=%v", columns, err)
		}
		m, err := Open(t.TempDir()+"/legacy.db", 1)
		if err != nil {
			t.Fatal(err)
		}
		defer m.Close()
		command := types.SQLCommand{RequestID: "legacy", SQL: "SELECT 1", WantRows: true}
		value, _ := types.EncodeSQLBatch([]types.SQLCommand{command})
		if err := m.Apply(context.Background(), 1, value); err != nil {
			t.Fatal(err)
		}
		if _, err := m.db.Exec("UPDATE _rhiza_idempotency SET sql_result = ?", encoded); err != nil {
			t.Fatal(err)
		}
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		m, err = Open(m.dbPath, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer m.Close()
		fingerprint, _ := types.SQLFingerprint(command)
		_, got, found, matches, err := m.SQLRequestResultFingerprint(context.Background(), command.RequestID, fingerprint, true)
		if err != nil || !found || !matches || len(got.Statements[0].Columns) != columns {
			t.Fatalf("legacy receipt columns=%d found=%v matches=%v err=%v", columns, found, matches, err)
		}
	}
}

func FuzzSQLResultDecoder(f *testing.F) {
	f.Add(legacyWideSQLResult(1000))
	f.Add([]byte("RSQL"))
	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > MaxMutationResultBytes+1 {
			return
		}
		result, err := decodeSQLResult(encoded)
		if err != nil || len(result.Statements) == 0 {
			return
		}
		reencoded, err := encodeSQLResult(result)
		if err != nil || !bytes.Equal(reencoded, encoded) {
			t.Fatalf("valid decoding did not reencode: %v", err)
		}
	})
}
