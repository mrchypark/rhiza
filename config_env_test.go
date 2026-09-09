package rhiza

import (
	"strings"
	"testing"
	"time"
)

func TestConfigFromEnvParsesOperatorEnvironment(t *testing.T) {
	for key, value := range map[string]string{
		"RHIZA_CLUSTER_ID": "recovered", "RHIZA_NODE_ID": "rhiza-r-1", "RHIZA_DATA_DIR": "/data/rhiza",
		"RHIZA_BIND_ADDR": ":8080", "RHIZA_PEER_ADDR": ":9090", "RHIZA_ADMIN_TOKEN": "admin",
		"RHIZA_CLUSTER_MEMBERS":   `[{"node_id":"rhiza-r-1","peer_url":"quic://rhiza-r-1:9090","token":"secret"}]`,
		"RHIZA_OBJSTORE_PROVIDER": "s3", "RHIZA_OBJSTORE_DIR": "/archive", "RHIZA_OBJSTORE_PREFIX": "recovered/",
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
	if len(config.Members) != 1 || config.Members[0].ID != "rhiza-r-1" || config.Members[0].Token != "secret" {
		t.Fatalf("members=%+v", config.Members)
	}
	if config.ObjStoreProvider != "s3" || config.ObjStoreDir != "/archive" || config.ObjStoreDurability != ObjectStoreDurabilityBeforeAck || !config.ObjStoreInsecure || config.ObjStoreRetries != 4 {
		t.Fatalf("object store config=%+v", config)
	}
	if config.ObjStoreSyncInterval != 2*time.Minute || config.ObjStoreBatchDelay != 3*time.Millisecond || config.ObjStoreGCInterval != 3*time.Hour || config.ObjStoreGCGracePeriod != 48*time.Hour || config.CheckpointInterval != 20*time.Minute || config.CheckpointTailBytes != 1024 || config.MaxWALBytes != 2048 || config.MaxConcurrentReads != 8 || config.MaxLongPollReads != 2 {
		t.Fatalf("timing config=%+v", config)
	}
}

func TestConfigFromEnvRejectsInvalidInput(t *testing.T) {
	for _, test := range []struct{ key, value string }{
		{"RHIZA_CLUSTER_MEMBERS", `[{"node_id":"n1","unknown":true}]`},
		{"RHIZA_CHECKPOINT_INTERVAL", "-1s"},
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

func TestConfigFromEnvRejectsConflictingObjectStoreDirectories(t *testing.T) {
	t.Setenv("RHIZA_OBJSTORE_DIR", "/new")
	t.Setenv("RHIZA_FILESYSTEM_DIR", "/old")
	if _, err := ConfigFromEnv(); err == nil || !strings.Contains(err.Error(), "RHIZA_OBJSTORE_DIR") {
		t.Fatalf("error=%v", err)
	}
}
