package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestManagedSQLReferenceFormatParity(t *testing.T) {
	if operator.V1Alpha2ManagedSQLEvidenceFormat != managedsql.EvidenceFormat {
		t.Fatal("operator and actual SQL host evidence formats differ")
	}
}

func TestManagedRuntimeProfileFile(t *testing.T) {
	profile := operator.V1Alpha2RuntimeProfile{InitImage: "reference:v1", PeerPort: 9090, EvidenceFormat: "rhiza-managed-sql/v1", ProfileSecretName: "rhiza-runtime", Storage: types.ExecutionConfig{AdminToken: "private-admin", ObjStoreProvider: "s3", ObjStoreBucket: "bucket", ObjStoreEndpoint: "s3.example.test", ObjStoreSecretKey: "private-sentinel"}}
	b, _ := json.Marshal(profile)
	for _, tc := range []struct {
		name, body string
		mode       os.FileMode
		valid      bool
	}{
		{"valid", string(b), 0600, true},
		{"Secret group read", string(b), 0440, true},
		{"public file", string(b), 0644, false},
		{"unknown field", strings.Replace(string(b), `"InitImage":`, `"secret-sentinel":true,"InitImage":`, 1), 0600, false},
		{"unknown storage field", strings.Replace(string(b), `"objstore_provider":`, `"secret-sentinel":true,"objstore_provider":`, 1), 0600, false},
		{"trailing", string(b) + `{}`, 0600, false},
		{"duplicate field", strings.Replace(string(b), `"InitImage":`, `"InitImage":"secret-sentinel","InitImage":`, 1), 0600, false},
		{"duplicate case field", strings.Replace(string(b), `"InitImage":`, `"initimage":"secret-sentinel","InitImage":`, 1), 0600, false},
		{"null", `null`, 0600, false},
		{"oversized", string(b) + strings.Repeat(" ", 65536), 0600, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "runtime.json")
			if err := os.WriteFile(path, []byte(tc.body), tc.mode); err != nil {
				t.Fatal(err)
			}
			got, err := readManagedRuntimeProfile(path)
			if tc.valid {
				if err != nil || got.ProfileSecretName != profile.ProfileSecretName || got.Storage.ObjStoreSecretKey != "private-sentinel" {
					t.Fatalf("profile decode: %v", err)
				}
			} else if err == nil || strings.Contains(err.Error(), "private-sentinel") || strings.Contains(err.Error(), "secret-sentinel") {
				t.Fatalf("unsafe profile error: %v", err)
			}
		})
	}
}

func TestManagedRuntimeProfileSelectionBeforeKubernetes(t *testing.T) {
	t.Setenv("RHIZA_AUTOMATIC_RECOVERY", "")
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("RHIZA_MANAGED_RUNTIME_PROFILE", filepath.Join(t.TempDir(), "missing"))
	for _, paths := range [][]string{nil, {filepath.Join(t.TempDir(), "explicit-missing")}} {
		err := run(context.Background(), "ns", time.Second, "", paths...)
		if err == nil || !strings.Contains(err.Error(), "managed runtime profile") {
			t.Fatalf("profile not selected: %v", err)
		}
	}
}
