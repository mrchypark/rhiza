package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	objstorecfg "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
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

func TestAnchorStorageBindingConfigRequiresExplicitPolicy(t *testing.T) {
	store := objstorecfg.Config{Provider: objstorecfg.ProviderS3, Endpoint: "https://s3.example", Bucket: "archive", Prefix: "root"}
	derived, err := anchorStorageBindingConfig(store, "derived", "")
	if err != nil {
		t.Fatal(err)
	}
	source, err := derived.ExpectedBinding("source")
	if err != nil {
		t.Fatal(err)
	}
	target, err := derived.ExpectedBinding("target")
	if err != nil || source.StorageID == target.StorageID {
		t.Fatalf("derived source/target bindings did not separate clusters: source=%+v target=%+v err=%v", source, target, err)
	}
	if _, err := anchorStorageBindingConfig(store, "", "opaque-id"); err == nil {
		t.Fatal("missing explicit policy was accepted")
	}
	if _, err := anchorStorageBindingConfig(store, "derived", "opaque-id"); err == nil {
		t.Fatal("ambiguous derived plus opaque policy was accepted")
	}
	if _, err := anchorStorageBindingConfig(objstorecfg.Config{Provider: objstorecfg.ProviderAzure, Bucket: "container", AzureStorageAccount: "account"}, "derived", ""); err == nil {
		t.Fatal("Azure config silently used five-field recipe despite omitted account identity")
	}
	if _, err := anchorStorageBindingConfig(objstorecfg.Config{Provider: objstorecfg.ProviderAzure, Bucket: "container", AzureStorageAccount: "account"}, "opaque", "externally-pinned"); err != nil {
		t.Fatalf("explicit opaque Azure mapping rejected: %v", err)
	}
}

func TestAnchorServiceLoadsEffectiveS3AliasesForBindingAndRequests(t *testing.T) {
	var mu sync.Mutex
	var authorization, sessionToken string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		authorization = r.Header.Get("Authorization")
		sessionToken = r.Header.Get("X-Amz-Security-Token")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	for name, value := range map[string]string{
		"RHIZA_OBJSTORE_PROVIDER":      "s3",
		"RHIZA_OBJSTORE_ENDPOINT":      strings.TrimPrefix(server.URL, "http://"),
		"RHIZA_OBJSTORE_BUCKET":        "anchor-test",
		"RHIZA_OBJSTORE_PREFIX":        "anchor-prefix",
		"RHIZA_OBJSTORE_REGION":        "anchor-test-region",
		"RHIZA_OBJSTORE_INSECURE":      "true",
		"RHIZA_OBJSTORE_ACCESS_KEY":    "anchor-test-access",
		"RHIZA_OBJSTORE_SECRET_KEY":    "anchor-test-secret-never-log",
		"RHIZA_OBJSTORE_SESSION_TOKEN": "anchor-test-session-never-log",
		"AWS_REGION":                   "",
	} {
		t.Setenv(name, value)
	}

	storeCfg, err := objstorecfg.LoadOperatorConfig()
	if err != nil {
		t.Fatalf("load service store configuration: %v", err)
	}
	if storeCfg.Provider != objstorecfg.ProviderS3 || storeCfg.Endpoint != strings.TrimPrefix(server.URL, "http://") || storeCfg.Bucket != "anchor-test" ||
		storeCfg.Prefix != "anchor-prefix" || storeCfg.Region != "anchor-test-region" || !storeCfg.Insecure ||
		storeCfg.AccessKey != "anchor-test-access" || storeCfg.SecretKey == "" || storeCfg.SessionToken == "" {
		t.Fatal("service config did not resolve the effective RHIZA object-store settings")
	}
	bindingConfig, err := anchorStorageBindingConfig(storeCfg, string(recoveryanchor.StorageBindingDerived), "")
	if err != nil {
		t.Fatalf("derive service binding from effective config: %v", err)
	}
	binding, err := bindingConfig.ExpectedBinding("anchor-test-cluster")
	if err != nil || binding.ClusterID != "anchor-test-cluster" || binding.StorageID != hashStoreIdentity(storeCfg, "anchor-test-cluster") {
		t.Fatalf("binding does not use the same effective config: binding=%+v err=%v", binding, err)
	}

	bucket, err := objstorecfg.NewBucket(storeCfg)
	if err != nil {
		t.Fatalf("open S3 client from effective service config: %v", err)
	}
	defer bucket.Close()
	if err := bucket.Upload(context.Background(), "anchor-prefix/probe", strings.NewReader("probe")); err != nil {
		t.Fatalf("send signed request to local S3 fixture: %v", err)
	}
	mu.Lock()
	gotAuthorization, gotSessionToken := authorization, sessionToken
	mu.Unlock()
	if !strings.Contains(gotAuthorization, "Credential=anchor-test-access/") || !strings.Contains(gotAuthorization, "/anchor-test-region/s3/aws4_request") {
		t.Fatal("S3 request did not use the configured access key and region")
	}
	if gotSessionToken != "anchor-test-session-never-log" {
		t.Fatal("S3 request did not use the configured session token")
	}
	if strings.Contains(gotAuthorization, "anchor-test-secret-never-log") {
		t.Fatal("S3 authorization header exposed the secret key")
	}

	t.Setenv("RHIZA_OBJSTORE_INSECURE", "not-a-bool")
	if _, err := objstorecfg.LoadOperatorConfig(); err == nil {
		t.Fatal("invalid store boolean was accepted by the service config loader")
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
