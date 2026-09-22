// Package sqlpolicy identifies the single supported materialization policy.
package sqlpolicy

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/ncruces/go-sqlite3/driver"
)

const Version = 1

var ErrIncompatible = errors.New("incompatible SQL execution policy: preserve the old installation and migrate logically to a new cluster")

func CheckExisting(ctx context.Context, path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	return CheckFile(ctx, path)
}

// CheckFile inspects provenance without opening a writer or upgrading the file.
func CheckFile(ctx context.Context, path string) error {
	db, err := driver.Open((&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String() + "?mode=ro")
	if err != nil {
		return fmt.Errorf("%w: %v", ErrIncompatible, err)
	}
	defer db.Close()
	var policy string
	if err := db.QueryRowContext(ctx, `SELECT value FROM _rhiza_meta WHERE key='sql_execution_policy'`).Scan(&policy); err != nil {
		return fmt.Errorf("%w: missing materialization marker: %v", ErrIncompatible, err)
	}
	if policy != "1" {
		return fmt.Errorf("%w: marker %q", ErrIncompatible, policy)
	}
	return nil
}
