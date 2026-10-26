package operator

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestEnsurePublicMembershipProjectionExcludesCredentialSentinels(t *testing.T) {
	const (
		secretName = "rhiza-recovery-0123456789abcdef01234567"
		operation  = "resource-uid:recovery-id"
	)
	members := projectionTestMembers()
	secret := object{
		"apiVersion": "v1", "kind": "Secret", "immutable": true,
		"metadata": object{"name": secretName, "annotations": object{ownerAnnotation: operation}},
		"data": object{
			"members": base64.StdEncoding.EncodeToString(jsonBytes(members)),
			"admin":   base64.StdEncoding.EncodeToString([]byte("ADMIN_SENTINEL_12345678901234567890")),
			"peer_tokens": base64.StdEncoding.EncodeToString(jsonBytes(map[string]string{
				"voter-a": "PEER_SENTINEL_A", "voter-b": "PEER_SENTINEL_B", "voter-c": "PEER_SENTINEL_C",
			})),
			"private_key": base64.StdEncoding.EncodeToString([]byte("PRIVATE_KEY_SENTINEL")),
		},
	}
	secretBefore := string(jsonBytes(secret))
	var projected object
	postCount := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-kube-token" {
			http.Error(w, "missing test token", http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/rhiza/configmaps/"+secretName+"-membership":
			if projected == nil {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			writeProjectionJSON(t, w, projected)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/namespaces/rhiza/configmaps":
			postCount++
			if err := json.NewDecoder(r.Body).Decode(&projected); err != nil {
				t.Errorf("decode projected ConfigMap: %v", err)
				http.Error(w, "invalid", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/rhiza/secrets/"+secretName:
			writeProjectionJSON(t, w, secret)
		default:
			t.Errorf("unexpected Kubernetes request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	tokenPath := t.TempDir() + "/token"
	if err := os.WriteFile(tokenPath, []byte("test-kube-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := &Controller{Kube: &Kubernetes{BaseURL: server.URL, Namespace: "rhiza", TokenFile: tokenPath, Client: server.Client()}}
	resource := &Resource{Metadata: map[string]any{"uid": "resource-uid"}, Spec: Spec{RecoveryID: "recovery-id"}}
	validatedMembers, err := c.targetCredentials(t.Context(), resource, secretName, nil)
	if err != nil {
		t.Fatalf("read and validate existing private target credentials: %v", err)
	}
	if !reflect.DeepEqual(validatedMembers, members) {
		t.Fatal("validated members differ from the authoritative target Secret")
	}
	if err := c.ensurePublicMembershipProjection(t.Context(), secretName, operation, validatedMembers); err != nil {
		t.Fatal(err)
	}
	if err := c.ensurePublicMembershipProjection(t.Context(), secretName, operation, validatedMembers); err != nil {
		t.Fatalf("idempotent projection retry: %v", err)
	}
	if postCount != 1 {
		t.Fatalf("ConfigMap create calls=%d, want one", postCount)
	}
	if string(jsonBytes(secret)) != secretBefore {
		t.Fatal("publishing public membership modified the private target credential Secret")
	}
	if projected["kind"] != "ConfigMap" || projected["immutable"] != true ||
		str(nested(projected, "metadata", "name")) != secretName+"-membership" ||
		str(nested(projected, "metadata", "annotations", ownerAnnotation)) != operation {
		t.Fatalf("ConfigMap identity/ownership mismatch: %#v", projected)
	}
	data := asObject(projected["data"])
	if len(data) != 1 || data[publicMembershipKey] == nil {
		t.Fatalf("public ConfigMap must contain only %s: %#v", publicMembershipKey, data)
	}
	publicJSON := str(data[publicMembershipKey])
	for _, sentinel := range []string{"ADMIN_SENTINEL", "PEER_SENTINEL_A", "PEER_SENTINEL_B", "PEER_SENTINEL_C", "PRIVATE_KEY_SENTINEL"} {
		if strings.Contains(publicJSON, sentinel) {
			t.Fatalf("public membership leaked private credential sentinel %q", sentinel)
		}
	}
	var got []quepaxa.Member
	if err := json.Unmarshal([]byte(publicJSON), &got); err != nil || !reflect.DeepEqual(got, members) {
		t.Fatalf("public membership differs from validated source members: err=%v got=%#v", err, got)
	}
}

func TestEnsurePublicMembershipProjectionRejectsChangedOrCredentialData(t *testing.T) {
	const (
		secretName = "rhiza-recovery-0123456789abcdef01234567"
		operation  = "recovery-operation"
	)
	wanted, err := publicMembershipProjection(secretName, operation, projectionTestMembers())
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(object){
		"wrong owner": func(cm object) { asObject(cm["metadata"])["annotations"] = object{ownerAnnotation: "other-operation"} },
		"mutable":     func(cm object) { cm["immutable"] = false },
		"credential field": func(cm object) {
			asObject(cm["data"])["PEER_TOKEN"] = "PEER_SENTINEL"
		},
		"changed members": func(cm object) { asObject(cm["data"])[publicMembershipKey] = "[]" },
	} {
		t.Run(name, func(t *testing.T) {
			current := deepCopyObject(wanted)
			mutate(current)
			c := projectionTestController(t, current, secretName)
			if err := c.ensurePublicMembershipProjection(t.Context(), secretName, operation, projectionTestMembers()); err == nil {
				t.Fatal("accepted changed or credential-bearing public membership ConfigMap")
			}
		})
	}
}

func TestApplyLogicalMembershipProjectionWiresMainAndRestoreInit(t *testing.T) {
	main := object{"name": "rhiza", "env": []any{object{"name": publicMembershipKey, "value": "stale"}}}
	init := object{"name": "restore-init", "env": []any{object{"name": "RHIZA_HA_INIT_RESTORE_DIR", "value": "/ha-restore-staging"}}}
	otherInit := object{"name": "prepare", "env": []any{object{"name": "KEEP", "value": "same"}}}
	sts := object{"spec": object{"template": object{"spec": object{"initContainers": []any{otherInit, init}}}}}
	if !applyLogicalMembershipProjection(sts, main, "rhiza-recovery-abcd-membership", "target-cluster", "before-ack") {
		t.Fatal("logical membership projection did not find required restore-init")
	}
	for _, env := range []any{main["env"], init["env"]} {
		if !testEnvUsesConfigMapKey(t, env, publicMembershipKey, "rhiza-recovery-abcd-membership", publicMembershipKey) {
			t.Fatalf("membership does not reference the generated public ConfigMap: %#v", env)
		}
		if got := testEnvValue(t, env, "RHIZA_CLUSTER_ID"); got != "target-cluster" {
			t.Fatalf("cluster ID=%q, want target-cluster", got)
		}
		if got := testEnvValue(t, env, "RHIZA_OBJSTORE_DURABILITY"); got != "before-ack" {
			t.Fatalf("durability=%q, want before-ack", got)
		}
	}
	if !reflect.DeepEqual(otherInit["env"], []any{object{"name": "KEEP", "value": "same"}}) {
		t.Fatalf("unrelated init container was modified: %#v", otherInit)
	}
	withoutRestore := object{"spec": object{"template": object{"spec": object{"initContainers": []any{otherInit}}}}}
	if applyLogicalMembershipProjection(withoutRestore, main, "rhiza-recovery-abcd-membership", "target-cluster", "before-ack") {
		t.Fatal("logical profile accepted a missing restore-init container")
	}
}

func projectionTestController(t *testing.T, configMap object, secretName string) *Controller {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/rhiza/configmaps/"+secretName+"-membership" {
			writeProjectionJSON(t, w, configMap)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	tokenPath := t.TempDir() + "/token"
	if err := os.WriteFile(tokenPath, []byte("test-kube-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &Controller{Kube: &Kubernetes{BaseURL: server.URL, Namespace: "rhiza", TokenFile: tokenPath, Client: server.Client()}}
}

func projectionTestMembers() []quepaxa.Member {
	key := sha256.Sum256([]byte("public-peer-key"))
	return []quepaxa.Member{
		{ID: "voter-a", URL: "http://voter-a:8080", PeerURL: "quic://voter-a:9090", LogURL: "http://voter-a:8081", PublicKey: quepaxa.PublicKey(key), WALIdentity: strings.Repeat("a", 64)},
		{ID: "voter-b", URL: "http://voter-b:8080", PeerURL: "quic://voter-b:9090", LogURL: "http://voter-b:8081", PublicKey: quepaxa.PublicKey(key), WALIdentity: strings.Repeat("b", 64)},
		{ID: "voter-c", URL: "http://voter-c:8080", PeerURL: "quic://voter-c:9090", LogURL: "http://voter-c:8081", PublicKey: quepaxa.PublicKey(key), WALIdentity: strings.Repeat("c", 64)},
	}
}

func writeProjectionJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Errorf("encode test Kubernetes response: %v", err)
	}
}

func testEnvValue(t *testing.T, value any, name string) string {
	t.Helper()
	for _, item := range list(value) {
		entry := asObject(item)
		if str(entry["name"]) == name {
			return str(entry["value"])
		}
	}
	t.Fatalf("environment lacks %s", name)
	return ""
}

func testEnvUsesConfigMapKey(t *testing.T, value any, envName, configMapName, key string) bool {
	t.Helper()
	for _, item := range list(value) {
		entry := asObject(item)
		if str(entry["name"]) != envName {
			continue
		}
		ref := asObject(nested(entry, "valueFrom", "configMapKeyRef"))
		return str(ref["name"]) == configMapName && str(ref["key"]) == key
	}
	return false
}
