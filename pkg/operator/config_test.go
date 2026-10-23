package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestNoPVCUsesEffectiveDataMount(t *testing.T) {
	sts := object{"spec": object{"template": object{"spec": object{"volumes": []any{
		object{"name": "ephemeral", "emptyDir": object{}}, object{"name": "host", "hostPath": object{"path": "/state"}},
	}}}}}
	c := object{"volumeMounts": []any{object{"name": "ephemeral", "mountPath": "/data"}}}
	if err := noPVC(sts, c, map[string]string{}); err != nil {
		t.Fatal(err)
	}
	c["volumeMounts"] = append(list(c["volumeMounts"]), object{"name": "host", "mountPath": "/data/db"})
	if err := noPVC(sts, c, map[string]string{}); err == nil {
		t.Fatal("persistent mount inside data directory accepted")
	}
	if err := noPVC(sts, c, map[string]string{"RHIZA_DATA_DIR": "/data/db"}); err == nil {
		t.Fatal("nested persistent mount accepted")
	}
	if err := noPVC(sts, c, map[string]string{"RHIZA_DATA_DIR": "/data/../state"}); err == nil {
		t.Fatal("path outside emptyDir accepted")
	}
}

func TestConfigurationRejectsUnresolvedStoreAndNestedState(t *testing.T) {
	c := &Controller{}
	_, err := c.environment(context.Background(), object{"env": []any{object{"name": "RHIZA_OBJSTORE_PREFIX", "valueFrom": object{"fieldRef": object{"fieldPath": "metadata.labels[archive]"}}}}})
	if err == nil {
		t.Fatal("unresolved archive namespace accepted")
	}
	env := map[string]string{"RHIZA_OBJSTORE_PROVIDER": "azure", "RHIZA_OBJSTORE_AZURE_CONNECTION_STRING": "AccountName=one;AccountKey=test-one"}
	c.StoreIdentity = StoreIdentity(env)
	env["RHIZA_OBJSTORE_AZURE_CONNECTION_STRING"] = "AccountName=two;AccountKey=test-two"
	if c.storeConfigMatches(env) == nil {
		t.Fatal("different connection-string account accepted")
	}
}

// An envFrom-provided scalar outlives a deletion, so the clearing override is
// the name-only entry the API server stores for an empty EnvVar.Value. The
// reader must resolve it to an empty value instead of rejecting it as an
// unresolved dynamic source.
func TestEnvironmentReadsNameOnlyClearingOverride(t *testing.T) {
	c := &Controller{}
	ctr := object{"env": []any{
		object{"name": "RHIZA_PEER_TOKEN"},
		object{"name": "RHIZA_CLUSTER_ID", "value": "target"},
	}}
	env, err := c.environment(context.Background(), ctr)
	if err != nil {
		t.Fatalf("name-only override rejected: %v", err)
	}
	if value, ok := env["RHIZA_PEER_TOKEN"]; !ok || value != "" {
		t.Fatalf("override resolved to %q ok=%t", value, ok)
	}
	if env["RHIZA_CLUSTER_ID"] != "target" {
		t.Fatalf("later entries dropped: %v", env)
	}
	// An unresolved dynamic source is still an error for any other key.
	if _, err := c.environment(context.Background(), object{"env": []any{object{"name": "RHIZA_OBJSTORE_PREFIX", "valueFrom": object{"fieldRef": object{"fieldPath": "metadata.labels[archive]"}}}}}); err == nil {
		t.Fatal("unresolved archive namespace accepted")
	}
}

func TestEnvironmentResolvesAppliedSupervisedPatch(t *testing.T) {
	data, err := os.ReadFile("../../deploy/operator/fencing/supervised-statefulset.patch.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var patch object
	if err := yaml.NewDecoder(strings.NewReader(string(data))).Decode(&patch); err != nil {
		t.Fatal(err)
	}
	ctr, err := container(patch, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var body object
		switch r.URL.Path {
		case "/api/v1/namespaces/test/configmaps/rhiza-config":
			body = object{"data": object{"RHIZA_CLUSTER_ID": "chaos-source"}}
		case "/api/v1/namespaces/test/secrets/rhiza-peer-credentials":
			body = object{"data": object{"RHIZA_PEER_TOKENS": base64.StdEncoding.EncodeToString([]byte("{}"))}}
		default:
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer api.Close()

	token := t.TempDir() + "/token"
	if err := os.WriteFile(token, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	c := &Controller{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: token, Client: api.Client()}}
	env, err := c.environment(context.Background(), ctr)
	if err != nil {
		t.Fatalf("applied supervised patch environment was rejected: %v", err)
	}
	if env["RHIZA_CLUSTER_ID"] != "chaos-source" {
		t.Fatalf("resolved source cluster ID = %q", env["RHIZA_CLUSTER_ID"])
	}
	if _, ok := env["RHIZA_POD_NAME"]; ok {
		t.Fatal("Pod-name downward API value must remain kubelet-resolved, not enter static configuration")
	}
}

func TestEnvironmentOnlyAllowsValidatedPodNameFieldRef(t *testing.T) {
	for _, tc := range []struct {
		name    string
		field   object
		wantErr bool
	}{
		{name: "omitted API version", field: object{"fieldPath": "metadata.name"}},
		{name: "core v1 API version", field: object{"apiVersion": "v1", "fieldPath": "metadata.name"}},
		{name: "other field", field: object{"fieldPath": "metadata.namespace"}, wantErr: true},
		{name: "unsupported API version", field: object{"apiVersion": "v2", "fieldPath": "metadata.name"}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Controller{}
			ctr := object{"env": []any{object{"name": "RHIZA_POD_NAME", "valueFrom": object{"fieldRef": tc.field}}}}
			env, err := c.environment(context.Background(), ctr)
			if tc.wantErr {
				if err == nil {
					t.Fatal("invalid Pod-name fieldRef accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("valid Pod-name fieldRef rejected: %v", err)
			}
			if _, ok := env["RHIZA_POD_NAME"]; ok {
				t.Fatal("Pod-name fieldRef should be left for kubelet resolution")
			}
		})
	}
}
