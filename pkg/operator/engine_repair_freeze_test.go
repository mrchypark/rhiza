package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestReadEngineRepairAdminTokenBindsSecretUIDAndDigest(t *testing.T) {
	const token = "original-admin-token-canary"
	secretUID := "source-secret-uid"
	secretName := "source-admin"
	namespace := "source-ns"
	secret := func(uid, value string) object {
		return object{
			"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
			"metadata": object{"name": secretName, "namespace": namespace, "uid": uid},
			"data":     object{"RHIZA_ADMIN_TOKEN": base64.StdEncoding.EncodeToString([]byte(value))},
		}
	}
	current := secret(secretUID, token)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/namespaces/"+namespace+"/secrets/"+secretName {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(current)
	}))
	defer server.Close()
	tokenPath := filepath.Join(t.TempDir(), "service-account-token")
	if err := os.WriteFile(tokenPath, []byte("test-service-account-token"), 0600); err != nil {
		t.Fatal(err)
	}
	executor := &FencingExecutor{Kube: &Kubernetes{BaseURL: server.URL, Namespace: namespace, TokenFile: tokenPath, Client: server.Client()}}
	record := fencing.EngineRepairRecord{SourceAdminCredential: fencing.EngineRepairAdminCredential{
		Name: secretName, UID: secretUID, AdminTokenDigest: fencing.EngineRepairAdminTokenDigest([]byte(token)),
	}}
	got, err := executor.readEngineRepairAdminToken(context.Background(), record)
	if err != nil || got != token {
		t.Fatalf("pinned original admin Secret rejected: tokenMatches=%t err=%v", got == token, err)
	}

	for name, replacement := range map[string]object{
		"replaced Secret UID":  secret("replacement-secret-uid", token),
		"changed token digest": secret(secretUID, "different-admin-token"),
	} {
		t.Run(name, func(t *testing.T) {
			current = replacement
			got, err := executor.readEngineRepairAdminToken(context.Background(), record)
			if err == nil || got != "" {
				t.Fatalf("mismatched original admin credential accepted or returned: token=%q err=%v", got, err)
			}
			if strings.Contains(err.Error(), token) || strings.Contains(err.Error(), "different-admin-token") {
				t.Fatal("admin credential value leaked through the diagnostic")
			}
		})
	}
}
