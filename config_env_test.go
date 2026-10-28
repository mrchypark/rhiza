package rhiza

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConfigFromEnvParsesOperatorEnvironment(t *testing.T) {
	for key, value := range map[string]string{
		"RHIZA_CLUSTER_ID": "recovered", "RHIZA_NODE_ID": "rhiza-r-1", "RHIZA_DATA_DIR": "/data/rhiza",
		"RHIZA_BIND_ADDR": ":8080", "RHIZA_PEER_ADDR": ":9090", "RHIZA_ADMIN_TOKEN": "admin",
		"RHIZA_CLUSTER_MEMBERS":        `[{"node_id":"rhiza-r-1","peer_url":"quic://rhiza-r-1:9090","public_key":"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="}]`,
		"RHIZA_ENABLE_RECONFIGURATION": "true",
		"RHIZA_LEARNER":                `{"node_id":"fresh-learner","peer_url":"quic://fresh-learner:9090","public_key":"AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI="}`,
		"RHIZA_PREPARED_POD_UID":       "pod-incarnation-1",
		"RHIZA_PEER_TOKEN":             "peer-secret",
		"RHIZA_OBJSTORE_PROVIDER":      "s3", "RHIZA_OBJSTORE_DIR": "/archive", "RHIZA_OBJSTORE_PREFIX": "recovered/",
		"RHIZA_OBJSTORE_ENDPOINT": "http://minio:9000", "RHIZA_OBJSTORE_BUCKET": "rhiza", "RHIZA_OBJSTORE_REGION": "us-east-1",
		"RHIZA_OBJSTORE_INSECURE": "true", "RHIZA_OBJSTORE_MAX_RETRIES": "4", "RHIZA_OBJSTORE_ACCESS_KEY": "access",
		"RHIZA_OBJSTORE_SECRET_KEY": "secret", "RHIZA_OBJSTORE_SESSION_TOKEN": "session", "RHIZA_OBJSTORE_SERVICE_ACCOUNT": "service",
		"RHIZA_OBJSTORE_AZURE_TENANT_ID": "tenant", "RHIZA_OBJSTORE_AZURE_CLIENT_ID": "client", "RHIZA_OBJSTORE_AZURE_CLIENT_SECRET": "client-secret",
		"RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT": "account", "RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT_KEY": "account-key",
		"RHIZA_OBJSTORE_AZURE_CONNECTION_STRING": "connection", "RHIZA_OBJSTORE_AZURE_USER_ASSIGNED_ID": "identity",
		"RHIZA_OBJSTORE_DURABILITY": "before-ack", "RHIZA_OBJSTORE_SYNC_INTERVAL": "2m", "RHIZA_OBJSTORE_BATCH_DELAY": "3ms",
		"RHIZA_OBJSTORE_GC_INTERVAL": "3h", "RHIZA_OBJSTORE_GC_GRACE_PERIOD": "48h", "RHIZA_CHECKPOINT_INTERVAL": "20m",
		"RHIZA_CHECKPOINT_TAIL_BYTES": "1024", "RHIZA_MAX_WAL_BYTES": "2048", "RHIZA_MAX_CONCURRENT_READS": "8", "RHIZA_MAX_LONG_POLL_READS": "2",
	} {
		t.Setenv(key, value)
	}

	config, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if config.ClusterID != "recovered" || config.NodeID != "rhiza-r-1" || config.DataDir != "/data/rhiza" || config.BindAddr != ":8080" || config.PeerAddr != ":9090" || config.AdminToken != "admin" {
		t.Fatalf("identity config=%+v", config)
	}
	if config.PeerToken != "peer-secret" || len(config.Members) != 1 || config.Members[0].ID != "rhiza-r-1" || config.Members[0].PublicKey == (PublicKey{}) {
		t.Fatalf("peer token=%q members=%+v", config.PeerToken, config.Members)
	}
	if !config.EnableReconfiguration || config.Learner == nil || config.Learner.ID != "fresh-learner" || config.PreparedPodUID != "pod-incarnation-1" {
		t.Fatalf("prepared learner config=%+v", config)
	}
	if config.ObjStoreProvider != "s3" || config.ObjStoreDir != "/archive" || config.ObjStoreDurability != ObjectStoreDurabilityBeforeAck || !config.ObjStoreInsecure || config.ObjStoreRetries != 4 {
		t.Fatalf("object store config=%+v", config)
	}
	if config.ObjStoreSyncInterval != 2*time.Minute || config.ObjStoreBatchDelay != 3*time.Millisecond || config.ObjStoreGCInterval != 3*time.Hour || config.ObjStoreGCGracePeriod != 48*time.Hour || config.CheckpointInterval != 20*time.Minute || config.CheckpointTailBytes != 1024 || config.MaxWALBytes != 2048 || config.MaxConcurrentReads != 8 || config.MaxLongPollReads != 2 {
		t.Fatalf("timing config=%+v", config)
	}
}

func TestInstalledPolicyConfigFromEnvLoadsTrustedPinAndFileSecret(t *testing.T) {
	clearInstalledPolicyEnv(t)
	secret := strings.Repeat("s", 32)
	secretFile := filepath.Join(t.TempDir(), "join-capability")
	if err := os.WriteFile(secretFile, []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RHIZA_CLUSTER_ID", "generation-7")
	t.Setenv("RHIZA_EXPECTED_INSTALLED_POLICY_HASH", strings.Repeat("ab", 32))
	t.Setenv("RHIZA_JOIN_CAPABILITY_FILE", secretFile)
	t.Setenv("RHIZA_JOIN_SEEDS", `["quic://peer-a.example:9090","quic://peer-b.example:9090"]`)

	config, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if config.ClusterID != "generation-7" || config.ExpectedInstalledPolicyHash == ([32]byte{}) || string(config.JoinCapability) != secret || len(config.JoinSeeds) != 2 {
		t.Fatalf("installed-policy config was not loaded: cluster=%q hash=%x secret-len=%d seeds=%v", config.ClusterID, config.ExpectedInstalledPolicyHash, len(config.JoinCapability), config.JoinSeeds)
	}
	runtimeConfig, err := executionConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeConfig.ExpectedInstalledPolicyHash != config.ExpectedInstalledPolicyHash || string(runtimeConfig.JoinCapability) != secret || len(runtimeConfig.JoinSeeds) != 2 {
		t.Fatal("installed-policy inputs were not propagated to runtime config")
	}
	for name, value := range map[string]any{"public config": config, "runtime config": runtimeConfig} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("%s serialization exposed the join capability", name)
		}
	}
}

func TestInstalledPolicyConfigFromEnvRejectsOversizedCapabilityFile(t *testing.T) {
	clearInstalledPolicyEnv(t)
	secret := strings.Repeat("s", maxJoinCapabilityFileBytes+1)
	secretFile := filepath.Join(t.TempDir(), "join-capability")
	if err := os.WriteFile(secretFile, []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RHIZA_CLUSTER_ID", "generation-7")
	t.Setenv("RHIZA_EXPECTED_INSTALLED_POLICY_HASH", strings.Repeat("ab", 32))
	t.Setenv("RHIZA_JOIN_CAPABILITY_FILE", secretFile)
	if _, err := ConfigFromEnv(); err == nil || !strings.Contains(err.Error(), "size limit") {
		t.Fatalf("oversized capability file error=%v", err)
	} else if strings.Contains(err.Error(), secret) {
		t.Fatal("oversized capability file error exposed secret contents")
	}
}

func TestInstalledPolicyConfigFromEnvDoesNotExposeCapabilityFileErrors(t *testing.T) {
	clearInstalledPolicyEnv(t)
	t.Setenv("RHIZA_CLUSTER_ID", "generation-7")
	t.Setenv("RHIZA_EXPECTED_INSTALLED_POLICY_HASH", strings.Repeat("ab", 32))
	t.Setenv("RHIZA_JOIN_CAPABILITY_FILE", t.TempDir())
	if _, err := ConfigFromEnv(); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("directory capability file error=%v", err)
	}
}

func TestInstalledPolicyConfigFromEnvFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*testing.T)
	}{
		{name: "malformed pin", setup: func(t *testing.T) { t.Setenv("RHIZA_EXPECTED_INSTALLED_POLICY_HASH", "not-a-hash") }},
		{name: "implicit cluster ID", setup: func(t *testing.T) {
			t.Setenv("RHIZA_EXPECTED_INSTALLED_POLICY_HASH", strings.Repeat("ab", 32))
			t.Setenv("RHIZA_CLUSTER_ID", "")
		}},
		{name: "inline secret forbidden", setup: func(t *testing.T) { t.Setenv("RHIZA_JOIN_CAPABILITY", strings.Repeat("s", 32)) }},
		{name: "seeds without policy pin", setup: func(t *testing.T) { t.Setenv("RHIZA_JOIN_SEEDS", `["quic://peer.example:9090"]`) }},
		{name: "secret file without policy pin", setup: func(t *testing.T) { t.Setenv("RHIZA_JOIN_CAPABILITY_FILE", "/must-not-be-read") }},
		{name: "malformed seed", setup: func(t *testing.T) {
			t.Setenv("RHIZA_EXPECTED_INSTALLED_POLICY_HASH", strings.Repeat("ab", 32))
			t.Setenv("RHIZA_CLUSTER_ID", "generation-7")
			t.Setenv("RHIZA_JOIN_SEEDS", `["https://peer.example:9090"]`)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			clearInstalledPolicyEnv(t)
			t.Setenv("RHIZA_CLUSTER_ID", "legacy-cluster")
			t.Setenv("RHIZA_DATA_DIR", t.TempDir())
			test.setup(t)
			if _, err := ConfigFromEnv(); err == nil {
				t.Fatal("invalid installed-policy environment was accepted")
			}
		})
	}
}

func TestExecutionConfigKeepsInstalledPolicyScopedAndPreservesLocalGC(t *testing.T) {
	pinned := Config{ClusterID: "generation-7", DataDir: t.TempDir(), ExpectedInstalledPolicyHash: [32]byte{1}}
	if _, err := executionConfig(pinned); err != nil {
		t.Fatalf("explicit policy cluster rejected: %v", err)
	}
	if _, err := executionConfig(Config{DataDir: t.TempDir(), ExpectedInstalledPolicyHash: [32]byte{1}}); err == nil || !strings.Contains(err.Error(), "explicit cluster ID") {
		t.Fatalf("policy pin without explicit cluster error=%v", err)
	}
	if _, err := executionConfig(Config{ClusterID: "generation-7", DataDir: t.TempDir(), Local: true, ExpectedInstalledPolicyHash: [32]byte{1}}); err == nil || !strings.Contains(err.Error(), "Local mode") {
		t.Fatalf("Local policy profile error=%v", err)
	}
	if _, err := executionConfig(Config{ClusterID: "generation-7", DataDir: t.TempDir(), JoinSeeds: []string{"quic://peer.example:9090"}}); err == nil {
		t.Fatal("join seeds without an installed-policy pin were accepted")
	}
	if _, err := executionConfig(Config{ClusterID: "generation-7", DataDir: t.TempDir(), JoinCapability: []byte(strings.Repeat("s", 32))}); err == nil {
		t.Fatal("join capability without an installed-policy pin was accepted")
	}
	if _, err := executionConfig(Config{ClusterID: "generation-7", DataDir: t.TempDir(), ExpectedInstalledPolicyHash: [32]byte{1}, JoinCapability: []byte("short")}); err == nil {
		t.Fatal("short join capability was accepted")
	}
	if _, err := executionConfig(Config{ClusterID: "generation-7", DataDir: t.TempDir(), ExpectedInstalledPolicyHash: [32]byte{1}, JoinSeeds: []string{"https://peer.example:9090"}}); err == nil {
		t.Fatal("non-QUIC join seed was accepted")
	}
	legacy, err := executionConfig(Config{Local: true, DataDir: t.TempDir(), ObjStoreGCInterval: time.Hour, ObjStoreGCGracePeriod: 2 * time.Hour})
	if err != nil {
		t.Fatalf("legacy Local config rejected: %v", err)
	}
	if legacy.ClusterID != "cluster-a" || legacy.ObjStoreGCInterval != time.Hour || legacy.ObjStoreGCGracePeriod != 2*time.Hour {
		t.Fatalf("legacy Local defaults or GC settings changed: %+v", legacy)
	}
}

func clearInstalledPolicyEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"RHIZA_CLUSTER_ID", "RHIZA_EXPECTED_INSTALLED_POLICY_HASH", "RHIZA_JOIN_CAPABILITY",
		"RHIZA_JOIN_CAPABILITY_FILE", "RHIZA_JOIN_SEEDS",
	} {
		t.Setenv(key, "")
	}
}

func TestConfigFromEnvRejectsInvalidInput(t *testing.T) {
	for _, test := range []struct{ key, value string }{
		{"RHIZA_CLUSTER_MEMBERS", `[{"node_id":"n1","unknown":true}]`},
		{"RHIZA_CHECKPOINT_INTERVAL", "-1s"},
		{"RHIZA_CHECKPOINT_RETAIN_COUNT", "-1"},
		{"RHIZA_CHECKPOINT_RETAIN_COUNT", "invalid"},
		{"RHIZA_MAX_LONG_POLL_READS", "1"},
		{"RHIZA_OBJSTORE_INSECURE", "treu"},
		{"RHIZA_OBJSTORE_BATCH_DELAY", "0s"},
	} {
		t.Run(test.key, func(t *testing.T) {
			t.Setenv(test.key, test.value)
			if test.key == "RHIZA_MAX_LONG_POLL_READS" {
				t.Setenv("RHIZA_MAX_CONCURRENT_READS", "0")
			}
			if _, err := ConfigFromEnv(); err == nil || !strings.Contains(err.Error(), test.key) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestConfigFromEnvCheckpointRetainCount(t *testing.T) {
	for _, test := range []struct {
		value string
		want  int
	}{{"", 3}, {"0", 3}, {"1", 1}, {"7", 7}} {
		t.Run("count="+test.value, func(t *testing.T) {
			t.Setenv("RHIZA_CHECKPOINT_RETAIN_COUNT", test.value)
			config, err := ConfigFromEnv()
			if err != nil {
				t.Fatal(err)
			}
			if config.CheckpointRetainCount != test.want {
				t.Fatalf("retain count=%d, want %d", config.CheckpointRetainCount, test.want)
			}
		})
	}
}

func TestConfigFromEnvRejectsConflictingObjectStoreDirectories(t *testing.T) {
	t.Setenv("RHIZA_OBJSTORE_DIR", "/new")
	t.Setenv("RHIZA_FILESYSTEM_DIR", "/old")
	if _, err := ConfigFromEnv(); err == nil || !strings.Contains(err.Error(), "RHIZA_OBJSTORE_DIR") {
		t.Fatalf("error=%v", err)
	}
}
