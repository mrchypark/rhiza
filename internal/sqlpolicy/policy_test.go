package sqlpolicy

import (
	"context"
	"database/sql"
	stdlibdriver "database/sql/driver"
	"errors"
	sqlite3driver "github.com/ncruces/go-sqlite3/driver"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestCheckFileRelativePath(t *testing.T) {
	dir, err := os.MkdirTemp(".", "policy-path-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path, err := filepath.Abs(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sqlite3driver.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TABLE _rhiza_meta(key TEXT PRIMARY KEY,value TEXT);INSERT INTO _rhiza_meta VALUES('sql_execution_policy','` + Marker() + `')`); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(cwd, path)
	if err != nil {
		t.Fatal(err)
	}
	if err = CheckExisting(context.Background(), relative); err != nil {
		t.Fatal(err)
	}
}

func TestCheckFileRejectsPreviousExecutionPolicyWithoutMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sqlite3driver.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	previous := strconv.Itoa(Version - 1)
	if _, err := db.Exec(`CREATE TABLE _rhiza_meta(key TEXT PRIMARY KEY,value TEXT);INSERT INTO _rhiza_meta VALUES('sql_execution_policy','` + previous + `')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := CheckExisting(context.Background(), path); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("previous policy error=%v", err)
	}
}

var errPolicyTestRowsClose = errors.New("injected policy rows close failure")
var errPolicyTestDBClose = errors.New("injected policy database close failure")

type policyCloseErrorConnector struct{}

func (policyCloseErrorConnector) Connect(context.Context) (stdlibdriver.Conn, error) {
	return policyCloseErrorConn{}, nil
}

func (policyCloseErrorConnector) Driver() stdlibdriver.Driver { return policyCloseErrorDriver{} }

type policyCloseErrorDriver struct{}

func (policyCloseErrorDriver) Open(string) (stdlibdriver.Conn, error) {
	return policyCloseErrorConn{}, nil
}

type policyCloseErrorConn struct{}

func (policyCloseErrorConn) Prepare(string) (stdlibdriver.Stmt, error) {
	return nil, errors.New("prepare is unused")
}

func (policyCloseErrorConn) Close() error { return errPolicyTestDBClose }

func (policyCloseErrorConn) Begin() (stdlibdriver.Tx, error) {
	return nil, errors.New("transactions are unused")
}

func (policyCloseErrorConn) QueryContext(_ context.Context, query string, _ []stdlibdriver.NamedValue) (stdlibdriver.Rows, error) {
	if strings.Contains(query, "_rhiza_meta") {
		return &policyCloseErrorRows{columns: []string{"value"}, values: []stdlibdriver.Value{Marker()}}, nil
	}
	if strings.Contains(query, "PRAGMA main.table_list") {
		return &policyCloseErrorRows{columns: []string{"schema", "name", "type", "ncol", "wr", "strict"}, closeErr: errPolicyTestRowsClose}, nil
	}
	return nil, errors.New("unexpected query")
}

type policyCloseErrorRows struct {
	columns  []string
	values   []stdlibdriver.Value
	closeErr error
	done     bool
}

func (r *policyCloseErrorRows) Columns() []string { return r.columns }

func (r *policyCloseErrorRows) Close() error { return r.closeErr }

func (r *policyCloseErrorRows) Next(dest []stdlibdriver.Value) error {
	if r.done || len(r.values) == 0 {
		return io.EOF
	}
	copy(dest, r.values)
	r.done = true
	return nil
}

func TestCheckDatabasePreservesRowsAndDBCloseErrors(t *testing.T) {
	db := sql.OpenDB(policyCloseErrorConnector{})
	err := checkDatabase(context.Background(), db)
	if !errors.Is(err, ErrIncompatible) {
		t.Fatalf("policy sentinel not preserved: %v", err)
	}
	if !errors.Is(err, errPolicyTestRowsClose) {
		t.Fatalf("rows close failure not preserved: %v", err)
	}
	if !errors.Is(err, errPolicyTestDBClose) {
		t.Fatalf("database close failure not preserved: %v", err)
	}
}
