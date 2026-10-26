package logicalauthority_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	objstore "github.com/mrchypark/rhiza/internal/objstore"
	thanosobjstore "github.com/thanos-io/objstore"
)

const flociGCSEndpoint = "http://floci-gcs:4588"

// TestFlociGCSLogicalAuthorityStorageCAS exercises the real GCS adapter's
// conditional object path. It is a storage prerequisite, not a claim of the
// multi-node Node.Open lifecycle; that remains gated on operator continuation
// and learner APIs.
func TestFlociGCSLogicalAuthorityStorageCAS(t *testing.T) {
	// Check all isolation gates before constructing a GCS SDK client. A missing
	// explicit endpoint is a hard failure, never a skip or live-cloud fallback.
	if os.Getenv("CI") != "true" || os.Getenv("RHIZA_LOGICAL_AUTHORITY_FLOCI") != "1" {
		t.Fatal("requires the dedicated pinned Floci-GCS CI job")
	}
	if os.Getenv("STORAGE_EMULATOR_HOST") != flociGCSEndpoint || os.Getenv("GOOGLE_CLOUD_PROJECT") != "floci-local" {
		t.Fatalf("unexpected GCS emulator configuration: endpoint=%q project=%q", os.Getenv("STORAGE_EMULATOR_HOST"), os.Getenv("GOOGLE_CLOUD_PROJECT"))
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		t.Fatal("ambient Google credentials are forbidden in the Floci test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	var suffixBytes [12]byte
	if _, err := rand.Read(suffixBytes[:]); err != nil {
		t.Fatal("generate task-owned bucket suffix")
	}
	suffix := hex.EncodeToString(suffixBytes[:])
	bucketName := "rhiza-logical-" + suffix
	credentials := localFlociCredentials(t)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	setupClient, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatalf("create Floci bucket setup client: %v", err)
	}
	if err := setupClient.Bucket(bucketName).Create(ctx, "floci-local", nil); err != nil {
		_ = setupClient.Close()
		t.Fatalf("create task-owned Floci bucket: %v", err)
	}
	t.Cleanup(func() {
		if err := setupClient.Bucket(bucketName).Delete(context.Background()); err != nil {
			t.Errorf("delete task-owned Floci bucket: %v", err)
		}
		if err := setupClient.Close(); err != nil {
			t.Errorf("close Floci setup client: %v", err)
		}
	})

	bucket, err := objstore.NewBucket(objstore.Config{
		Provider: objstore.ProviderGCS, Bucket: bucketName, ServiceAccount: credentials,
	})
	if err != nil {
		t.Fatalf("create Rhiza GCS adapter: %v", err)
	}
	t.Cleanup(func() {
		if err := bucket.Close(); err != nil {
			t.Errorf("close Rhiza GCS adapter: %v", err)
		}
	})

	// This object uses the same actual provider adapter and generation-based
	// conditional writes as the authority root and source-mutation stores.
	key := "integration/" + suffix + "/root-cas.json"
	first := []byte(`{"revision":1}`)
	if err := bucket.Upload(ctx, key, strings.NewReader(string(first)), thanosobjstore.WithIfNotExists()); err != nil {
		t.Fatalf("create initial test root conditionally: %v", err)
	}
	t.Cleanup(func() {
		if err := bucket.Delete(context.Background(), key); err != nil {
			t.Errorf("delete task-owned GCS test object: %v", err)
		}
	})
	attrs, err := bucket.Attributes(ctx, key)
	if err != nil || attrs.Version == nil || attrs.Version.Value == "" {
		t.Fatalf("read initial GCS generation: version=%+v error=%v", attrs.Version, err)
	}
	second := []byte(`{"revision":2}`)
	if err := bucket.Upload(ctx, key, strings.NewReader(string(second)), thanosobjstore.WithIfMatch(attrs.Version)); err != nil {
		t.Fatalf("advance test root with exact generation: %v", err)
	}
	if err := bucket.Upload(ctx, key, strings.NewReader(string(first)), thanosobjstore.WithIfMatch(attrs.Version)); err == nil {
		t.Fatal("Floci accepted stale root generation; expected CAS conflict")
	}
	reader, err := bucket.Get(ctx, key)
	if err != nil {
		t.Fatalf("read back test root: %v", err)
	}
	got, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || string(got) != string(second) {
		t.Fatalf("root CAS readback mismatch: body=%s read_error=%v close_error=%v", got, readErr, closeErr)
	}
}

func localFlociCredentials(t *testing.T) string {
	t.Helper()
	const (
		clientID = "rhiza-floci-client"
		secret   = "rhiza-floci-secret"
		refresh  = "rhiza-floci-refresh"
	)
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/token" {
			http.Error(w, "unexpected token request", http.StatusBadRequest)
			return
		}
		if err := r.ParseForm(); err != nil || r.Form.Get("grant_type") != "refresh_token" ||
			r.Form.Get("client_id") != clientID || r.Form.Get("client_secret") != secret || r.Form.Get("refresh_token") != refresh {
			http.Error(w, "unexpected local refresh grant", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"floci-test-access-token","token_type":"Bearer","expires_in":3600}`)
	}))
	t.Cleanup(tokenServer.Close)
	data, err := json.Marshal(struct {
		Type         string `json:"type"`
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
		RefreshToken string `json:"refresh_token"`
		TokenURI     string `json:"token_uri"`
	}{"authorized_user", clientID, secret, refresh, fmt.Sprintf("%s/token", tokenServer.URL)})
	if err != nil {
		t.Fatalf("marshal task-local Floci credentials: %v", err)
	}
	return string(data)
}
