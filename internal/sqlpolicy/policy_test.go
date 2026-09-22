package sqlpolicy

import (
	"context"
	"github.com/ncruces/go-sqlite3/driver"
	"os"
	"path/filepath"
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
	db, err := driver.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TABLE _rhiza_meta(key TEXT PRIMARY KEY,value TEXT);INSERT INTO _rhiza_meta VALUES('sql_execution_policy','1')`); err != nil {
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
