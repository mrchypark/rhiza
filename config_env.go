package rhiza

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// ConfigFromEnv returns a Config from the RHIZA_* environment used by the
// rhiza server binary. It is suitable for embedded hosts managed by the
// recovery operator, which updates those variables between generations.
func ConfigFromEnv() (Config, error) {
	var members []Member
	if raw := os.Getenv("RHIZA_CLUSTER_MEMBERS"); raw != "" {
		if err := decodeConfigEnvJSON(raw, &members); err != nil {
			return Config{}, fmt.Errorf("invalid RHIZA_CLUSTER_MEMBERS: %w", err)
		}
	}
	enableReconfiguration, err := parseConfigEnvBool("RHIZA_ENABLE_RECONFIGURATION")
	if err != nil {
		return Config{}, err
	}
	var learner *Member
	if raw := os.Getenv("RHIZA_LEARNER"); raw != "" {
		if err := decodeConfigEnvJSON(raw, &learner); err != nil {
			return Config{}, fmt.Errorf("invalid RHIZA_LEARNER: %w", err)
		}
	}
	checkpointInterval, err := parseConfigEnvDuration("RHIZA_CHECKPOINT_INTERVAL", "15m", func(v time.Duration) bool { return v >= 0 })
	if err != nil {
		return Config{}, err
	}
	checkpointTailBytes, err := parseConfigEnvInt64("RHIZA_CHECKPOINT_TAIL_BYTES", "536870912", func(v int64) bool { return v > 0 && v <= 2<<30 })
	if err != nil {
		return Config{}, err
	}
	maxWALBytes, err := parseConfigEnvInt64("RHIZA_MAX_WAL_BYTES", "0", func(v int64) bool { return v >= 0 })
	if err != nil {
		return Config{}, err
	}
	maxConcurrentReads, err := parseConfigEnvInt("RHIZA_MAX_CONCURRENT_READS", "0", func(v int) bool { return v >= 0 })
	if err != nil {
		return Config{}, err
	}
	maxLongPollReads, err := parseConfigEnvInt("RHIZA_MAX_LONG_POLL_READS", "0", func(v int) bool { return v >= 0 && (maxConcurrentReads != 0 || v == 0) && v <= maxConcurrentReads })
	if err != nil {
		return Config{}, err
	}
	objStoreSyncInterval, err := parseConfigEnvDuration("RHIZA_OBJSTORE_SYNC_INTERVAL", "1m", func(v time.Duration) bool { return v >= 0 })
	if err != nil {
		return Config{}, err
	}
	objStoreBatchDelay, err := parseConfigEnvDuration("RHIZA_OBJSTORE_BATCH_DELAY", "2ms", func(v time.Duration) bool { return v > 0 && v <= time.Second })
	if err != nil {
		return Config{}, err
	}
	objStoreGCInterval, err := parseConfigEnvDuration("RHIZA_OBJSTORE_GC_INTERVAL", "1h", func(v time.Duration) bool { return v >= 0 })
	if err != nil {
		return Config{}, err
	}
	objStoreGCGracePeriod, err := parseConfigEnvDuration("RHIZA_OBJSTORE_GC_GRACE_PERIOD", "24h", func(v time.Duration) bool { return v >= 0 })
	if err != nil {
		return Config{}, err
	}
	objStoreRetries, err := parseConfigEnvInt("RHIZA_OBJSTORE_MAX_RETRIES", "3", func(v int) bool { return v >= 0 })
	if err != nil {
		return Config{}, err
	}
	objStoreInsecure, err := parseConfigEnvBool("RHIZA_OBJSTORE_INSECURE")
	if err != nil {
		return Config{}, err
	}
	objStoreDir, err := configEnvObjectStoreDir()
	if err != nil {
		return Config{}, err
	}

	return Config{
		ClusterID: configEnvOrDefault("RHIZA_CLUSTER_ID", "cluster-a"), NodeID: configEnvOrDefault("RHIZA_NODE_ID", "node-1"),
		DataDir: configEnvOrDefault("RHIZA_DATA_DIR", "./rhiza-data"), BindAddr: configEnvOrDefault("RHIZA_BIND_ADDR", "127.0.0.1:8080"),
		PeerAddr: configEnvOrDefault("RHIZA_PEER_ADDR", "127.0.0.1:9090"), AdminToken: os.Getenv("RHIZA_ADMIN_TOKEN"), Members: members, EnableReconfiguration: enableReconfiguration, Learner: learner,
		ObjStoreProvider: os.Getenv("RHIZA_OBJSTORE_PROVIDER"), ObjStoreDir: objStoreDir, ObjStorePrefix: os.Getenv("RHIZA_OBJSTORE_PREFIX"),
		ObjStoreEndpoint: os.Getenv("RHIZA_OBJSTORE_ENDPOINT"), ObjStoreBucket: os.Getenv("RHIZA_OBJSTORE_BUCKET"), ObjStoreRegion: os.Getenv("RHIZA_OBJSTORE_REGION"),
		ObjStoreInsecure: objStoreInsecure, ObjStoreRetries: objStoreRetries, ObjStoreAccessKey: os.Getenv("RHIZA_OBJSTORE_ACCESS_KEY"),
		ObjStoreSecretKey: os.Getenv("RHIZA_OBJSTORE_SECRET_KEY"), ObjStoreSessionToken: os.Getenv("RHIZA_OBJSTORE_SESSION_TOKEN"),
		ObjStoreServiceAccount: os.Getenv("RHIZA_OBJSTORE_SERVICE_ACCOUNT"), ObjStoreAzureTenantID: os.Getenv("RHIZA_OBJSTORE_AZURE_TENANT_ID"),
		ObjStoreAzureClientID: os.Getenv("RHIZA_OBJSTORE_AZURE_CLIENT_ID"), ObjStoreAzureClientSecret: os.Getenv("RHIZA_OBJSTORE_AZURE_CLIENT_SECRET"),
		ObjStoreAzureStorageAccount: os.Getenv("RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT"), ObjStoreAzureStorageAccountKey: os.Getenv("RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT_KEY"),
		ObjStoreAzureConnectionString: os.Getenv("RHIZA_OBJSTORE_AZURE_CONNECTION_STRING"), ObjStoreAzureUserAssignedID: os.Getenv("RHIZA_OBJSTORE_AZURE_USER_ASSIGNED_ID"),
		ObjStoreDurability: ObjectStoreDurability(configEnvOrDefault("RHIZA_OBJSTORE_DURABILITY", "async")), ObjStoreSyncInterval: objStoreSyncInterval,
		ObjStoreBatchDelay: objStoreBatchDelay, ObjStoreGCInterval: objStoreGCInterval, ObjStoreGCGracePeriod: objStoreGCGracePeriod,
		CheckpointInterval: checkpointInterval, CheckpointTailBytes: checkpointTailBytes, MaxWALBytes: maxWALBytes,
		MaxConcurrentReads: maxConcurrentReads, MaxLongPollReads: maxLongPollReads,
	}, nil
}

func configEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func parseConfigEnvDuration(key, defaultValue string, valid func(time.Duration) bool) (time.Duration, error) {
	value, err := time.ParseDuration(configEnvOrDefault(key, defaultValue))
	if err != nil || !valid(value) {
		return 0, fmt.Errorf("invalid %s", key)
	}
	return value, nil
}

func parseConfigEnvInt64(key, defaultValue string, valid func(int64) bool) (int64, error) {
	value, err := strconv.ParseInt(configEnvOrDefault(key, defaultValue), 10, 64)
	if err != nil || !valid(value) {
		return 0, fmt.Errorf("invalid %s", key)
	}
	return value, nil
}

func parseConfigEnvInt(key, defaultValue string, valid func(int) bool) (int, error) {
	value, err := strconv.Atoi(configEnvOrDefault(key, defaultValue))
	if err != nil || !valid(value) {
		return 0, fmt.Errorf("invalid %s", key)
	}
	return value, nil
}

func parseConfigEnvBool(key string) (bool, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("invalid %s: %w", key, err)
	}
	return value, nil
}

func configEnvObjectStoreDir() (string, error) {
	value, legacy := os.Getenv("RHIZA_OBJSTORE_DIR"), os.Getenv("RHIZA_FILESYSTEM_DIR")
	if value != "" && legacy != "" && value != legacy {
		return "", errors.New("RHIZA_OBJSTORE_DIR conflicts with legacy RHIZA_FILESYSTEM_DIR")
	}
	if value != "" {
		return value, nil
	}
	return legacy, nil
}

func decodeConfigEnvJSON(raw string, value any) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}
