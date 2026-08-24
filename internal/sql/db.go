package sql

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/ncruces/go-sqlite3/driver"
)

type DB struct {
	conn *sql.DB
}

func Open(path string) (*DB, error) {
	conn, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	return &DB{conn: conn}, nil
}

func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) Version(ctx context.Context) (string, error) {
	var version string
	if err := db.conn.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
		return "", fmt.Errorf("query version: %w", err)
	}
	return version, nil
}
