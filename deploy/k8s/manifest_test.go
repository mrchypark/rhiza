package k8s

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"

	rhiza "github.com/mrchypark/rhiza"
)

func TestThreePeerBootstrapSettings(t *testing.T) {
	statefulSet, err := os.ReadFile("statefulset.yaml")
	if err != nil {
		t.Fatal(err)
	}
	service, err := os.ReadFile("service.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(statefulSet), "podManagementPolicy: Parallel") {
		t.Fatal("three-peer StatefulSet must start peers in parallel")
	}
	if !strings.Contains(string(service), "publishNotReadyAddresses: true") {
		t.Fatal("headless service must publish peers before readiness")
	}
}

func TestReadReplicaDeploymentsSelectExplicitRoles(t *testing.T) {
	manifest, err := os.ReadFile("read-replicas.yaml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(manifest)
	for _, required := range []string{"value: object-store", "value: learner", "path: /ready", "name: rhiza-object-store", "app.kubernetes.io/name: rhiza-read-replica"} {
		if !strings.Contains(text, required) {
			t.Fatalf("read replica manifest missing %q", required)
		}
	}
	if strings.Contains(text, "app.kubernetes.io/name: rhiza\n") || strings.Contains(text, "voter token") {
		t.Fatal("read replica manifest overlaps voter selectors or requests voter credentials")
	}
}

func TestThreePeerE2EManifestsProvideDistinctVoterTokens(t *testing.T) {
	for _, manifestName := range []string{"sql-server-3peer-e2e.yaml", "graph-server-3peer-e2e.yaml"} {
		name := manifestName
		manifest, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		text := string(manifest)
		if strings.Count(text, `"public_key":"`) != 3 {
			t.Fatalf("%s must publish one voter public key per member", name)
		}
		if strings.Contains(text, `"token":"rhiza-`) {
			t.Fatalf("%s must not replicate voter tokens in the membership list", name)
		}
		members, tokens := manifestPeerCredentials(t, text)
		if len(members) != 3 || len(tokens) != 3 {
			t.Fatalf("%s must declare three members and three peer tokens, got %d and %d", name, len(members), len(tokens))
		}
		for _, member := range members {
			token, ok := tokens[member.NodeID]
			if !ok {
				t.Fatalf("%s has no peer token for %s", name, member.NodeID)
			}
			want := rhiza.PeerPublicKey(member.ClusterID, member.NodeID, token)
			got, err := base64.StdEncoding.DecodeString(member.PublicKey)
			if err != nil {
				t.Fatalf("%s member %s public key is not base64: %v", name, member.NodeID, err)
			}
			if string(got) != string(want[:]) {
				t.Fatalf("%s member %s public key does not match its peer token", name, member.NodeID)
			}
		}
	}
}

type manifestMember struct {
	ClusterID string `json:"cluster_id"`
	NodeID    string `json:"node_id"`
	PublicKey string `json:"public_key"`
}

// manifestPeerCredentials extracts the membership list and the peer token map
// from a rendered manifest so the test can prove the published public keys are
// the ones the tokens derive.
func manifestPeerCredentials(t *testing.T, manifest string) ([]manifestMember, map[string]string) {
	t.Helper()
	clusterID := manifestEnvValue(t, manifest, "RHIZA_CLUSTER_ID")
	var members []manifestMember
	if err := json.Unmarshal([]byte(manifestEnvValue(t, manifest, "RHIZA_CLUSTER_MEMBERS")), &members); err != nil {
		t.Fatalf("membership list is not valid JSON: %v", err)
	}
	for i := range members {
		members[i].ClusterID = clusterID
	}
	var tokens map[string]string
	if err := json.Unmarshal([]byte(strings.TrimSpace(secretValue(t, manifest, "peer_tokens"))), &tokens); err != nil {
		t.Fatalf("peer token map is not valid JSON: %v", err)
	}
	return members, tokens
}

// manifestEnvValue returns the inline value of a container environment variable.
func manifestEnvValue(t *testing.T, manifest, key string) string {
	t.Helper()
	marker := "- name: " + key + "\n"
	index := strings.Index(manifest, marker)
	if index < 0 {
		t.Fatalf("manifest does not set %s", key)
	}
	rest := manifest[index+len(marker):]
	const foldedPrefix = "              value: >-\n"
	const valuePrefix = "              value: "
	switch {
	case strings.HasPrefix(rest, foldedPrefix):
		rest = rest[len(foldedPrefix):]
	case strings.HasPrefix(rest, valuePrefix):
		rest = rest[len(valuePrefix):]
	default:
		t.Fatalf("%s does not have an inline value", key)
	}
	line := rest
	if end := strings.IndexByte(line, '\n'); end >= 0 {
		line = line[:end]
	}
	return strings.TrimSpace(line)
}

// secretValue returns the inline value of a Secret stringData key.
func secretValue(t *testing.T, manifest, key string) string {
	t.Helper()
	marker := "\n  " + key + ": >-\n"
	index := strings.Index(manifest, marker)
	if index < 0 {
		t.Fatalf("manifest does not set secret key %s", key)
	}
	rest := manifest[index+len(marker):]
	if end := strings.IndexByte(rest, '\n'); end >= 0 {
		rest = rest[:end]
	}
	return rest
}
