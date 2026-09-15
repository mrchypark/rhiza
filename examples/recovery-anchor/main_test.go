package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	objstorecfg "github.com/mrchypark/rhiza/internal/objstore"
	_ "github.com/ncruces/go-sqlite3"
)

// --- StorageID tests ---

func TestHashStoreIdentity(t *testing.T) {
	cfg := objstorecfg.Config{
		Provider: "s3",
		Endpoint: "s3.us-east-1.amazonaws.com",
		Bucket:   "rhiza-prod",
		Prefix:   "clusters",
	}
	h1 := hashStoreIdentity(cfg, "cluster-a")
	h2 := hashStoreIdentity(cfg, "cluster-a")
	if h1 != h2 {
		t.Errorf("same inputs: %q vs %q", h1, h2)
	}
	h3 := hashStoreIdentity(cfg, "cluster-b")
	if h1 == h3 {
		t.Error("different cluster produced same hash")
	}
	identity := [5]string{"s3", "s3.us-east-1.amazonaws.com", "rhiza-prod", "clusters", "cluster-a"}
	data, _ := json.Marshal(identity)
	sum := sha256.Sum256(data)
	want := fmt.Sprintf("%x", sum)
	if h1 != want {
		t.Errorf("hash = %q, want %q", h1, want)
	}
}

func TestStoragePrefix(t *testing.T) {
	cfg := objstorecfg.Config{Prefix: "rhiza"}
	got := storagePrefix(cfg, "my-cluster")
	want := "rhiza/my-cluster"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestHashStoreIdentity_DifferentProviders(t *testing.T) {
	base := objstorecfg.Config{Provider: "s3", Endpoint: "ep", Bucket: "b", Prefix: "p"}
	gcs := objstorecfg.Config{Provider: "gcs", Endpoint: "ep", Bucket: "b", Prefix: "p"}
	if h1, h2 := hashStoreIdentity(base, "c1"), hashStoreIdentity(gcs, "c1"); h1 == h2 {
		t.Error("different providers produced same hash")
	}
}

func TestHashStoreIdentity_EmptyCluster(t *testing.T) {
	cfg := objstorecfg.Config{Provider: "gcs", Bucket: "b", Prefix: "p"}
	if h := hashStoreIdentity(cfg, ""); h == "" {
		t.Error("empty cluster should still produce a hash")
	}
}

// --- readEvidence tests (production helper) ---

func newTestDB(t *testing.T, name string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TABLE trust_state (id INTEGER PRIMARY KEY, epoch INTEGER NOT NULL, token TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestReadEvidence_CanonicalProof(t *testing.T) {
	db := newTestDB(t, "canonical.db")
	db.Exec(`INSERT INTO trust_state (id, epoch, token) VALUES (1, 42, 'tok-abc')`)

	rows, err := db.Query("SELECT epoch, token FROM trust_state WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := readEvidence(rows)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"epoch":42,"token":"tok-abc"}`
	if string(evidence) != want {
		t.Errorf("canonical = %s, want %s", evidence, want)
	}
}

func TestReadEvidence_NoRows(t *testing.T) {
	db := newTestDB(t, "empty.db")

	rows, err := db.Query("SELECT epoch, token FROM trust_state WHERE id = 999")
	if err != nil {
		t.Fatal(err)
	}
	_, err = readEvidence(rows)
	if err == nil {
		t.Error("expected error for no rows")
	}
}

func TestReadEvidence_EpochZero(t *testing.T) {
	db := newTestDB(t, "zero.db")
	db.Exec(`INSERT INTO trust_state (id, epoch, token) VALUES (1, 0, 'tok')`)

	rows, err := db.Query("SELECT epoch, token FROM trust_state WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = readEvidence(rows)
	if err == nil {
		t.Error("expected error for epoch < 1")
	}
}

func TestReadEvidence_EmptyToken(t *testing.T) {
	db := newTestDB(t, "emptytoken.db")
	db.Exec(`INSERT INTO trust_state (id, epoch, token) VALUES (1, 5, '')`)

	rows, err := db.Query("SELECT epoch, token FROM trust_state WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = readEvidence(rows)
	if err == nil {
		t.Error("expected error for empty token")
	}
}

func TestReadEvidence_MultipleRows(t *testing.T) {
	db := newTestDB(t, "multi.db")
	db.Exec(`INSERT INTO trust_state (id, epoch, token) VALUES (1, 1, 'a')`)
	db.Exec(`INSERT INTO trust_state (id, epoch, token) VALUES (2, 2, 'b')`)

	rows, err := db.Query("SELECT epoch, token FROM trust_state")
	if err != nil {
		t.Fatal(err)
	}
	_, err = readEvidence(rows)
	if err == nil {
		t.Error("expected error for multiple rows")
	}
}
