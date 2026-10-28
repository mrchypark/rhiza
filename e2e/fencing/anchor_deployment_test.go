package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
	"gopkg.in/yaml.v3"
)

const anchorDeploymentConfigMap = "rhiza-recovery-anchor"

func TestRecoveryAnchorDeploymentIsSharedAndOptIn(t *testing.T) {
	deployments := []struct {
		name      string
		path      string
		container string
	}{
		{name: "operator", path: "../../deploy/operator/deployment.yaml", container: "operator"},
		{name: "fencer", path: "../../deploy/operator/fencing/authority.yaml", container: "authority"},
	}
	for _, deployment := range deployments {
		t.Run(deployment.name, func(t *testing.T) {
			root := readYAMLDocuments(t, deployment.path)
			var podSpec map[string]any
			for _, doc := range root {
				if doc["kind"] != "Deployment" {
					continue
				}
				metadata := yamlMap(t, doc["metadata"])
				if metadata["name"] != "rhiza-"+deployment.name {
					continue
				}
				spec := yamlMap(t, doc["spec"])
				template := yamlMap(t, spec["template"])
				podSpec = yamlMap(t, template["spec"])
				break
			}
			if podSpec == nil {
				t.Fatalf("Deployment %q missing from %s", "rhiza-"+deployment.name, deployment.path)
			}
			containers, ok := podSpec["containers"].([]any)
			if !ok {
				t.Fatal("Deployment has no containers")
			}
			var container map[string]any
			for _, raw := range containers {
				candidate := yamlMap(t, raw)
				if candidate["name"] == deployment.container {
					container = candidate
					break
				}
			}
			if container == nil {
				t.Fatalf("container %q missing", deployment.container)
			}
			assertOptionalAnchorConfig(t, container)
			assertAnchorMount(t, podSpec, container)
			if deployment.name == "fencer" {
				sources := map[string]bool{}
				for _, raw := range container["envFrom"].([]any) {
					from := yamlMap(t, raw)
					for _, refName := range []string{"configMapRef", "secretRef"} {
						if ref, ok := from[refName]; ok {
							refConfig := yamlMap(t, ref)
							name := refConfig["name"].(string)
							sources[name] = refConfig["optional"] == true
						}
					}
				}
				for _, name := range []string{"rhiza-config", "rhiza-object-store", anchorDeploymentConfigMap} {
					if !sources[name] {
						t.Fatalf("fencer lacks optional effective store/anchor env source %q", name)
					}
				}
			}
		})
	}

	data := readYAMLDocuments(t, "../../deploy/operator/fencing/recovery-anchor-config.example.yaml")
	if len(data) != 1 || data[0]["kind"] != "ConfigMap" {
		t.Fatalf("anchor example must be one ConfigMap document, got %d documents", len(data))
	}
	metadata := yamlMap(t, data[0]["metadata"])
	if metadata["name"] != anchorDeploymentConfigMap {
		t.Fatalf("example ConfigMap name=%v", metadata["name"])
	}
	values := yamlMap(t, data[0]["data"])
	for key, want := range map[string]string{
		"RHIZA_RECOVERY_ANCHOR_URL":            "https://replace-with-trusted-anchor.example/v1",
		"RHIZA_RECOVERY_ANCHOR_TOKEN_FILE":     "/var/run/secrets/rhiza-recovery-anchor-token/token",
		"RHIZA_RECOVERY_ANCHOR_BINDING_POLICY": "derived",
	} {
		if values[key] != want {
			t.Errorf("example %s=%v, want %q", key, values[key], want)
		}
	}
	if _, exists := values["RHIZA_RECOVERY_ANCHOR_STORAGE_ID"]; exists {
		t.Fatal("derived example must not require or pin a static storage ID")
	}
	if !strings.Contains(string(mustReadFile(t, "../../deploy/operator/fencing/recovery-anchor-config.example.yaml")), "RHIZA_RECOVERY_ANCHOR_BINDING_POLICY: opaque") {
		t.Fatal("example must document the explicit opaque alternative")
	}
	if _, exists := values["RHIZA_RECOVERY_ANCHOR_CA_FILE"]; exists {
		t.Fatal("example must leave the private-CA path unset unless the CA is supplied")
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestRecoveryAnchorHandlerUsesSharedStaticBearer(t *testing.T) {
	const token = "configured-anchor-client-token"
	handler := recoveryanchor.NewHandler(&recoveryanchor.Coordinator{}, token)
	call := func(bearer string) int {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
		request.Header.Set("Authorization", "Bearer "+bearer)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response.Code
	}
	if got := call(token); got != http.StatusBadRequest {
		t.Fatalf("matching bearer status=%d, want 400 after passing handler authentication", got)
	}
	if got := call("wrong-token"); got != http.StatusUnauthorized {
		t.Fatalf("wrong bearer status=%d, want 401", got)
	}
}

func TestRecoveryAnchorDoesNotExpandFencerRBAC(t *testing.T) {
	docs := readYAMLDocuments(t, "../../deploy/operator/fencing/authority.yaml")
	for _, doc := range docs {
		if doc["kind"] != "Role" {
			continue
		}
		metadata := yamlMap(t, doc["metadata"])
		if metadata["name"] != "rhiza-fencer" {
			continue
		}
		recoveryReadRules := 0
		for _, raw := range doc["rules"].([]any) {
			rule := yamlMap(t, raw)
			resources := rule["resources"].([]any)
			apiGroups := rule["apiGroups"].([]any)
			verbs := rule["verbs"].([]any)
			for _, resource := range resources {
				if resource == "nodes" {
					t.Fatal("recovery-anchor wiring must not grant node access")
				}
				if resource == "rhizarecoveries" {
					recoveryReadRules++
					if len(resources) != 1 || len(apiGroups) != 1 || apiGroups[0] != "rhiza.mrchypark.dev" || len(verbs) != 1 || verbs[0] != "get" {
						t.Fatalf("fencer recovery access must be a separate exact GET-only rule: resources=%v apiGroups=%v verbs=%v", resources, apiGroups, verbs)
					}
				}
				if resource == "rhizarecoveries/status" {
					t.Fatal("fencer recovery access must not include status subresource permissions")
				}
				if resource == "pods" || resource == "secrets" {
					allowed := map[string]bool{"get": true, "list": true}
					if resource == "secrets" {
						allowed = map[string]bool{"get": true}
					}
					for _, rawVerb := range rule["verbs"].([]any) {
						if verb, _ := rawVerb.(string); !allowed[verb] {
							t.Fatalf("anchor wiring expanded fencer %s permission to %q", resource, verb)
						}
					}
				}
			}
		}
		if recoveryReadRules != 1 {
			t.Fatalf("fencer must have one separate GET-only rhizarecoveries rule, found %d", recoveryReadRules)
		}
		return
	}
	t.Fatal("rhiza-fencer Role missing")
}

func assertOptionalAnchorConfig(t *testing.T, container map[string]any) {
	t.Helper()
	for _, raw := range container["env"].([]any) {
		entry := yamlMap(t, raw)
		name, _ := entry["name"].(string)
		if strings.HasPrefix(name, "RHIZA_RECOVERY_ANCHOR_") {
			t.Fatalf("anchor setting %q must remain unset unless the optional ConfigMap is supplied", name)
		}
	}
	for _, raw := range container["envFrom"].([]any) {
		from := yamlMap(t, raw)
		ref, exists := from["configMapRef"]
		if !exists {
			continue
		}
		config := yamlMap(t, ref)
		if config["name"] == anchorDeploymentConfigMap {
			if config["optional"] != true {
				t.Fatal("recovery-anchor ConfigMap must be optional so non-DR deployments remain disabled")
			}
			return
		}
	}
	t.Fatal("container must import the shared optional recovery-anchor ConfigMap")
}

func assertAnchorMount(t *testing.T, podSpec, container map[string]any) {
	t.Helper()
	mounts := map[string]map[string]any{}
	for _, raw := range container["volumeMounts"].([]any) {
		mount := yamlMap(t, raw)
		mounts[mount["name"].(string)] = mount
	}
	for name, path := range map[string]string{
		"recovery-anchor-token": "/var/run/secrets/rhiza-recovery-anchor-token",
		"recovery-anchor-ca":    "/var/run/secrets/rhiza-recovery-anchor-ca",
	} {
		mount, ok := mounts[name]
		if !ok || mount["mountPath"] != path || mount["readOnly"] != true {
			t.Fatalf("read-only mount %q=%v, want path %q", name, mount, path)
		}
	}
	volumes := map[string]map[string]any{}
	for _, raw := range podSpec["volumes"].([]any) {
		volume := yamlMap(t, raw)
		volumes[volume["name"].(string)] = volume
	}
	token, ok := volumes["recovery-anchor-token"]
	if !ok {
		t.Fatal("optional recovery-anchor bearer Secret volume missing")
	}
	secret := yamlMap(t, token["secret"])
	if secret["secretName"] != "rhiza-recovery-anchor-token" || secret["optional"] != true {
		t.Fatalf("anchor bearer token volume must be the same optional Secret: %v", secret)
	}
	configVolume, ok := volumes["recovery-anchor-ca"]
	if !ok {
		t.Fatal("anchor CA/config volume missing")
	}
	configMap := yamlMap(t, configVolume["configMap"])
	if configMap["name"] != "rhiza-recovery-anchor-ca" || configMap["optional"] != true {
		t.Fatalf("private CA volume must use a separate optional ConfigMap: %v", configMap)
	}
}

func readYAMLDocuments(t *testing.T, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	var docs []map[string]any
	for {
		var doc map[string]any
		err := decoder.Decode(&doc)
		if err == io.EOF {
			return docs
		}
		if err != nil {
			t.Fatalf("decode YAML %s: %v", path, err)
		}
		if len(doc) != 0 {
			docs = append(docs, doc)
		}
	}
}

func yamlMap(t *testing.T, value any) map[string]any {
	t.Helper()
	got, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("YAML value has type %T, want map", value)
	}
	return got
}
