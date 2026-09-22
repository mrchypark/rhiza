package recovery

import (
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/ncruces/go-sqlite3/driver"
	"testing"
)

func policySQL(t testing.TB, query string) []byte {
	t.Helper()
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{SQL: query}})
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func policySnapshot(t testing.TB, path string) {
	t.Helper()
	db, err := driver.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE _rhiza_meta(key TEXT PRIMARY KEY,value TEXT);INSERT INTO _rhiza_meta VALUES('sql_execution_policy','` + sqlpolicy.Marker() + `')`); err != nil {
		t.Fatal(err)
	}
}
