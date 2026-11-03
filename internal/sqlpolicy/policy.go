// Package sqlpolicy fixes SQL semantics and exact durable materialization profiles.
package sqlpolicy

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"

	"github.com/ncruces/go-sqlite3/driver"
)

const Version = 7
const PeerALPN = "rhiza-peer-v7"

// MaterializationPolicy identifies durable state, not the SQL parser version.
type MaterializationPolicy int

const (
	LegacyMaterializationPolicy              MaterializationPolicy = Version
	ManagedSQLKVMaterializationPolicy        MaterializationPolicy = 8
	ManagedRequestMaterializationPolicy      MaterializationPolicy = 9
	ManagedGraphRequestMaterializationPolicy MaterializationPolicy = 10
)

const ManagedRequestApplicationProtocol = "rhiza-managed-request/v1"
const ManagedGraphRequestApplicationProtocol = "rhiza-managed-request-graph/v1"
const ManagedRequestAppendOnlyApplicationProtocol = "rhiza-managed-request-append-only/v1"
const ManagedGraphRequestAppendOnlyApplicationProtocol = "rhiza-managed-request-graph-append-only/v1"

// IsRetiredExperimentalApplicationProtocol identifies unreleased prototypes
// that remain decodable for diagnosis but are not valid public startup profiles.
func IsRetiredExperimentalApplicationProtocol(protocol string) bool {
	return protocol == ManagedRequestApplicationProtocol || protocol == ManagedGraphRequestApplicationProtocol
}
const ManagedGraphRequestPeerALPN = "rhiza-peer-managed-request-graph-v1"

func IsManagedRequestApplicationProtocol(protocol string) bool {
	return protocol == ManagedRequestApplicationProtocol || protocol == ManagedRequestAppendOnlyApplicationProtocol
}

func IsManagedGraphRequestApplicationProtocol(protocol string) bool {
	return protocol == ManagedGraphRequestApplicationProtocol || protocol == ManagedGraphRequestAppendOnlyApplicationProtocol
}

func IsManagedAppendOnlyApplicationProtocol(protocol string) bool {
	return protocol == ManagedRequestAppendOnlyApplicationProtocol || protocol == ManagedGraphRequestAppendOnlyApplicationProtocol
}

func (p MaterializationPolicy) Validate() error {
	if p != LegacyMaterializationPolicy && p != ManagedSQLKVMaterializationPolicy && p != ManagedRequestMaterializationPolicy && p != ManagedGraphRequestMaterializationPolicy {
		return ErrIncompatible
	}
	return nil
}

func Marker() string { return strconv.Itoa(Version) }

var ErrIncompatible = errors.New("incompatible SQL execution policy: preserve the old installation and migrate logically to a new cluster")

func CheckExisting(ctx context.Context, path string) error {
	return CheckExistingForPolicy(ctx, path, LegacyMaterializationPolicy)
}

func CheckExistingForPolicy(ctx context.Context, path string, expected MaterializationPolicy) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	return CheckFileForPolicy(ctx, path, expected)
}

// CheckFile inspects provenance without opening a writer or upgrading the file.
func CheckFile(ctx context.Context, path string) error {
	return CheckFileForPolicy(ctx, path, LegacyMaterializationPolicy)
}

// CheckSnapshotFile inspects a standalone checkpoint SQLite image without
// allowing SQLite to create WAL/SHM sidecars beside the immutable source.
func CheckSnapshotFile(ctx context.Context, path string) error {
	return CheckSnapshotFileForPolicy(ctx, path, LegacyMaterializationPolicy)
}

func CheckFileForPolicy(ctx context.Context, path string, expected MaterializationPolicy) error {
	return checkFile(ctx, path, false, expected)
}

func CheckSnapshotFileForPolicy(ctx context.Context, path string, expected MaterializationPolicy) error {
	return checkFile(ctx, path, true, expected)
}

func checkFile(ctx context.Context, path string, immutable bool, expected MaterializationPolicy) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	query := "?mode=ro"
	if immutable {
		query += "&immutable=1"
	}
	db, err := driver.Open((&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String() + query)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrIncompatible, err)
	}
	defer db.Close()
	var policy string
	if err := db.QueryRowContext(ctx, `SELECT value FROM _rhiza_meta WHERE key='sql_execution_policy'`).Scan(&policy); err != nil {
		return fmt.Errorf("%w: missing materialization marker: %v", ErrIncompatible, err)
	}
	if policy != strconv.Itoa(int(expected)) {
		return fmt.Errorf("%w: marker %q", ErrIncompatible, policy)
	}
	rows, err := db.QueryContext(ctx, `PRAGMA main.table_list`)
	if err != nil {
		return fmt.Errorf("%w: inspect table kinds: %v", ErrIncompatible, err)
	}
	defer rows.Close()
	for rows.Next() {
		var schema, name, kind string
		var columns, withoutRowID, strict int
		if err := rows.Scan(&schema, &name, &kind, &columns, &withoutRowID, &strict); err != nil {
			return fmt.Errorf("%w: inspect table kinds: %v", ErrIncompatible, err)
		}
		if kind == "virtual" || kind == "shadow" {
			return fmt.Errorf("%w: persistent virtual tables are unsupported", ErrIncompatible)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: inspect table kinds: %v", ErrIncompatible, err)
	}
	return nil
}
