package node

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	sqlite "github.com/ncruces/go-sqlite3/driver"
)

func TestPrepareInitialGenesisCreatesDurableEmptyStateAndNoops(t *testing.T) {
	config, assignment := initialGenesisTestConfig(t)
	first, err := prepareInitialGenesis(context.Background(), &config, assignment)
	if err != nil {
		t.Fatal(err)
	}
	if first.MarkerNoop || !first.MaterializerDone || !first.FilesSynced || first.CleanupErr != nil {
		t.Fatalf("first genesis execution=%+v", first)
	}
	marker, exists, err := readPreparedGenesisMarkerForAssignment(config.DataDir, preparedGenesisMarkerForTest(t, &config, assignment))
	if err != nil || !exists || marker.Phase != "complete" || marker.Assignment != assignment {
		t.Fatalf("genesis marker=%+v exists=%t err=%v", marker, exists, err)
	}
	second, err := prepareInitialGenesis(context.Background(), &config, assignment)
	if err != nil {
		t.Fatal(err)
	}
	if !second.MarkerNoop || second.MaterializerDone || second.FilesSynced || second.CleanupErr != nil {
		t.Fatalf("complete marker was not a read-only no-op: %+v", second)
	}
	if _, err := os.Stat(filepath.Join(config.DataDir, preparedRestoreMarkerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("genesis wrote certified-restore marker: %v", err)
	}
	if err := validatePreparedGenesisRuntimeConfig(&config); err != nil {
		t.Fatalf("runtime rejected matching initial-genesis marker: %v", err)
	}
	wrongRuntime := config
	wrongRuntime.PreparedPodUID = "different-pod"
	if err := validatePreparedGenesisRuntimeConfig(&wrongRuntime); err == nil {
		t.Fatal("runtime accepted a genesis marker for another Pod UID")
	}
	state, err := materializer.Open(filepath.Join(config.DataDir, "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if state.Tip() != 0 {
		_ = state.Close()
		t.Fatalf("genesis materializer tip=%d, want zero", state.Tip())
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareManagedInitReportsGenesisWithoutNamespaceQuiescenceProof(t *testing.T) {
	config, assignment := initialGenesisTestConfig(t)
	cleanup, digest, err := PrepareManagedInit(context.Background(), &config, assignment)
	if err != nil {
		t.Fatal(err)
	}
	if cleanup.Mode != fencing.InitModeGenesis || !cleanup.CleanupSucceeded || cleanup.BucketDrainSucceeded || cleanup.NamespaceQuiescent {
		t.Fatalf("managed genesis cleanup=%+v", cleanup)
	}
	if digest == "" {
		t.Fatal("managed genesis omitted its local assignment digest")
	}
	if err := (fencing.InitAttemptOutcome{State: fencing.InitAttemptMaterialized, GenesisDigest: digest, LiveCleanup: cleanup}).Validate(); err == nil {
		t.Fatal("local cleanup report unexpectedly validated as a complete fencing outcome")
	}
}

func TestPreparedInitialOpenRequiresSupervisorBeforeQlogCreation(t *testing.T) {
	config, assignment := initialGenesisTestConfig(t)
	if _, err := prepareInitialGenesis(context.Background(), &config, assignment); err != nil {
		t.Fatal(err)
	}
	config.AdminToken = "admin"
	config.PeerToken = "peer"
	t.Setenv("RHIZA_FENCE_SOCKET", "")
	t.Setenv("RHIZA_FENCE_TICKET", "")
	n := New(&config)
	err := n.Open(context.Background())
	if err == nil || !strings.Contains(err.Error(), "supervised runtime session") {
		t.Fatalf("prepared initial Open error=%v, want missing supervised session refusal", err)
	}
	lockData, err := os.ReadFile(filepath.Join(config.DataDir, "qlog", "lock.qlog"))
	if err != nil || len(lockData) != 0 {
		t.Fatalf("unsupervised prepared Node changed qlog lock contents=%q err=%v", lockData, err)
	}
}

func TestPrepareInitialGenesisRefusesWrongAssignmentAndUnknownData(t *testing.T) {
	config, assignment := initialGenesisTestConfig(t)
	wrong := assignment
	wrong.PodUID = "different-pod"
	if _, err := prepareInitialGenesis(context.Background(), &config, wrong); err == nil {
		t.Fatal("genesis accepted a different Pod assignment")
	}
	unknownConfig, unknownAssignment := initialGenesisTestConfig(t)
	if err := os.MkdirAll(unknownConfig.DataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	unknownPath := filepath.Join(unknownConfig.DataDir, "user.db")
	if err := os.WriteFile(unknownPath, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareInitialGenesis(context.Background(), &unknownConfig, unknownAssignment); err == nil {
		t.Fatal("genesis accepted unknown DataDir content")
	}
	if got, err := os.ReadFile(unknownPath); err != nil || string(got) != "preserve" {
		t.Fatalf("unknown DataDir content changed: %q err=%v", got, err)
	}
}

func TestPrepareInitialGenesisRejectsSymlinkRootAndUnknownSQLiteObjectsReadOnly(t *testing.T) {
	rootConfig, rootAssignment := initialGenesisTestConfig(t)
	outside := t.TempDir()
	if err := os.Symlink(outside, rootConfig.DataDir); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareInitialGenesis(context.Background(), &rootConfig, rootAssignment); err == nil {
		t.Fatal("genesis accepted a symlink DataDir root")
	}
	if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
		t.Fatalf("symlink target was changed: entries=%v err=%v", entries, err)
	}

	for _, object := range []struct {
		name, ddl string
	}{
		{name: "table", ddl: `CREATE TABLE _rhiza_hidden(value TEXT)`},
		{name: "view", ddl: `CREATE VIEW _rhiza_hidden AS SELECT 1`},
	} {
		t.Run(object.name, func(t *testing.T) {
			config, assignment := initialGenesisTestConfig(t)
			if err := os.MkdirAll(config.DataDir, 0o700); err != nil {
				t.Fatal(err)
			}
			dbPath := filepath.Join(config.DataDir, "sqlite.db")
			dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(dbPath)}).String()
			db, err := sqlite.Open(dsn)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(object.ddl); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			marker, err := preparedGenesisMarkerFor(&config, assignment)
			if err != nil {
				t.Fatal(err)
			}
			if err := writePreparedGenesisMarker(config.DataDir, marker, false); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			markerPath := filepath.Join(config.DataDir, preparedGenesisMarkerName)
			markerBefore, err := os.ReadFile(markerPath)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := prepareInitialGenesis(context.Background(), &config, assignment); err == nil || !strings.Contains(err.Error(), "unknown "+object.name) {
				t.Fatalf("genesis accepted an unknown reserved-prefix SQLite %s", object.name)
			}
			after, err := os.ReadFile(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("read-only genesis rejection modified SQLite bytes")
			}
			markerAfter, err := os.ReadFile(markerPath)
			if err != nil || !bytes.Equal(markerBefore, markerAfter) {
				t.Fatalf("read-only rejection changed genesis intent: err=%v", err)
			}
			if _, err := os.Lstat(filepath.Join(config.DataDir, "qlog")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("read-only rejection created qlog lock path: %v", err)
			}
		})
	}
}

func TestPrepareInitialGenesisRejectsIncompatibleReservedSQLiteSchemaReadOnly(t *testing.T) {
	for _, test := range []struct {
		name string
		ddl  string
	}{
		{
			name: "column type",
			ddl: `ALTER TABLE _rhiza_kv RENAME TO _rhiza_kv_old;
CREATE TABLE _rhiza_kv (key BLOB PRIMARY KEY, value BLOB NOT NULL, expires_at_unix_ms INTEGER NOT NULL DEFAULT 0);
DROP TABLE _rhiza_kv_old;
CREATE INDEX _rhiza_kv_expiry ON _rhiza_kv(expires_at_unix_ms) WHERE expires_at_unix_ms > 0;`,
		},
		{
			name: "primary key and check constraint",
			ddl: `ALTER TABLE _rhiza_idempotency RENAME TO _rhiza_idempotency_old;
CREATE TABLE _rhiza_idempotency (kind INTEGER NOT NULL, request_id BLOB NOT NULL, fingerprint BLOB NOT NULL, commit_slot INTEGER NOT NULL, status TEXT NOT NULL, error_code TEXT NOT NULL, rows_affected INTEGER NOT NULL, last_insert_id INTEGER NOT NULL, applied INTEGER NOT NULL, sql_result BLOB NOT NULL DEFAULT X'');
DROP TABLE _rhiza_idempotency_old;
CREATE INDEX _rhiza_idempotency_slot ON _rhiza_idempotency(commit_slot);`,
		},
		{
			name: "index expression",
			ddl:  `DROP INDEX _rhiza_kv_expiry; CREATE INDEX _rhiza_kv_expiry ON _rhiza_kv(key);`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, assignment := initialGenesisTestConfig(t)
			if err := os.MkdirAll(config.DataDir, 0o700); err != nil {
				t.Fatal(err)
			}
			dbPath := filepath.Join(config.DataDir, "sqlite.db")
			state, err := materializer.Open(dbPath, 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := state.Close(); err != nil {
				t.Fatal(err)
			}
			dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(dbPath)}).String()
			db, err := sqlite.Open(dsn)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(test.ddl); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			marker, err := preparedGenesisMarkerFor(&config, assignment)
			if err != nil {
				t.Fatal(err)
			}
			if err := writePreparedGenesisMarker(config.DataDir, marker, false); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			markerPath := filepath.Join(config.DataDir, preparedGenesisMarkerName)
			markerBefore, err := os.ReadFile(markerPath)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := prepareInitialGenesis(context.Background(), &config, assignment); err == nil {
				t.Fatal("genesis accepted an incompatible reserved SQLite schema")
			}
			after, err := os.ReadFile(dbPath)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("schema refusal changed SQLite bytes: err=%v", err)
			}
			markerAfter, err := os.ReadFile(markerPath)
			if err != nil || !bytes.Equal(markerBefore, markerAfter) {
				t.Fatalf("schema refusal changed genesis marker: err=%v", err)
			}
			if _, err := os.Lstat(filepath.Join(config.DataDir, "qlog")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("schema refusal created qlog lock path: %v", err)
			}
		})
	}
}

func initialGenesisTestConfig(t *testing.T) (types.ExecutionConfig, fencing.BootstrapAssignment) {
	t.Helper()
	config := types.ExecutionConfig{
		ClusterID: "cluster", NodeID: "node-a", DataDir: filepath.Join(t.TempDir(), "data"),
		PreparedPodUID: "pod-a", EnableReconfiguration: true,
		Members:          []quepaxa.Member{peerMember("cluster", "node-a", "a"), peerMember("cluster", "node-b", "b")},
		ObjStoreProvider: "s3", ObjStoreBucket: "rhiza", ObjStorePrefix: "root",
	}
	config.ObjStoreDurability = types.ObjectStoreDurabilityAsync
	membership := recovery.NewMembershipRecord(string(config.ClusterID), config.Members, string(config.ObjStoreDurability))
	assignment := fencing.BootstrapAssignment{
		OperationID: "op-initial", RequestHash: "request-hash", AuthorizationHash: "auth-hash",
		Purpose: fencing.BootstrapInitial, RootUID: "root-uid", StatefulSetUID: "sts-uid",
		SourceClusterID: string(config.ClusterID), PodName: "rhiza-0", PodUID: config.PreparedPodUID,
		NodeID: string(config.NodeID), CredentialName: "bootstrap-peer", CredentialUID: "credential-uid",
		MembershipHash: membership.Membership, ProfileHash: "profile-hash", LedgerUID: "ledger-uid",
	}
	return config, assignment
}

func preparedGenesisMarkerForTest(t *testing.T, config *types.ExecutionConfig, assignment fencing.BootstrapAssignment) preparedGenesisMarker {
	t.Helper()
	marker, err := preparedGenesisMarkerFor(config, assignment)
	if err != nil {
		t.Fatal(err)
	}
	return marker
}
