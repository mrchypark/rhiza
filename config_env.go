package rhiza

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// ConfigFromEnv returns a Config from the RHIZA_* environment used by the
// rhiza server binary. It is suitable for embedded hosts managed by the
// recovery operator, which updates those variables between generations.
func ConfigFromEnv() (Config, error) {
	expectedInstalledPolicyHash, err := parseInstalledPolicyHash(os.Getenv("RHIZA_EXPECTED_INSTALLED_POLICY_HASH"))
	if err != nil {
		return Config{}, err
	}
	policyPinned := expectedInstalledPolicyHash != ([32]byte{})
	clusterID := os.Getenv("RHIZA_CLUSTER_ID")
	if policyPinned && clusterID == "" {
		return Config{}, fmt.Errorf("RHIZA_CLUSTER_ID is required with RHIZA_EXPECTED_INSTALLED_POLICY_HASH")
	}
	if os.Getenv("RHIZA_JOIN_CAPABILITY") != "" {
		return Config{}, fmt.Errorf("RHIZA_JOIN_CAPABILITY is not accepted; configure RHIZA_JOIN_CAPABILITY_FILE")
	}
	joinCapabilityFile := os.Getenv("RHIZA_JOIN_CAPABILITY_FILE")
	if !policyPinned && joinCapabilityFile != "" {
		return Config{}, fmt.Errorf("RHIZA_JOIN_CAPABILITY_FILE requires RHIZA_EXPECTED_INSTALLED_POLICY_HASH")
	}
	var joinCapability []byte
	if joinCapabilityFile != "" {
		joinCapability, err = readJoinCapabilityFile(joinCapabilityFile)
		if err != nil {
			return Config{}, fmt.Errorf("read RHIZA_JOIN_CAPABILITY_FILE: %w", err)
		}
		if len(joinCapability) < 32 {
			return Config{}, fmt.Errorf("RHIZA_JOIN_CAPABILITY_FILE must contain at least 32 secret bytes")
		}
	}
	var joinSeeds []string
	if raw := os.Getenv("RHIZA_JOIN_SEEDS"); raw != "" {
		if !policyPinned {
			return Config{}, fmt.Errorf("RHIZA_JOIN_SEEDS requires RHIZA_EXPECTED_INSTALLED_POLICY_HASH")
		}
		if err := decodeConfigEnvJSON(raw, &joinSeeds); err != nil {
			return Config{}, fmt.Errorf("invalid RHIZA_JOIN_SEEDS: %w", err)
		}
		if err := validateJoinSeeds(joinSeeds); err != nil {
			return Config{}, fmt.Errorf("invalid RHIZA_JOIN_SEEDS: %w", err)
		}
	}
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
	checkpointRetainCount, err := parseConfigEnvInt("RHIZA_CHECKPOINT_RETAIN_COUNT", "3", func(v int) bool { return v >= 0 })
	if err != nil {
		return Config{}, err
	}
	if checkpointRetainCount == 0 {
		checkpointRetainCount = 3
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
	nodeID := configEnvOrDefault("RHIZA_NODE_ID", "node-1")
	peerToken, err := configEnvPeerToken(nodeID)
	if err != nil {
		return Config{}, err
	}

	return Config{
		ClusterID: configEnvOrDefault("RHIZA_CLUSTER_ID", "cluster-a"), NodeID: nodeID,
		ExpectedInstalledPolicyHash: expectedInstalledPolicyHash, JoinCapability: joinCapability, JoinSeeds: joinSeeds,
		PreparedPodUID: os.Getenv("RHIZA_PREPARED_POD_UID"),
		DataDir:        configEnvOrDefault("RHIZA_DATA_DIR", "./rhiza-data"), BindAddr: configEnvOrDefault("RHIZA_BIND_ADDR", "127.0.0.1:8080"),
		PeerAddr: configEnvOrDefault("RHIZA_PEER_ADDR", "127.0.0.1:9090"), AdminToken: os.Getenv("RHIZA_ADMIN_TOKEN"), PeerToken: peerToken, Members: members, EnableReconfiguration: enableReconfiguration, Learner: learner,
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
		CheckpointInterval: checkpointInterval, CheckpointTailBytes: checkpointTailBytes, CheckpointRetainCount: checkpointRetainCount, MaxWALBytes: maxWALBytes,
		MaxConcurrentReads: maxConcurrentReads, MaxLongPollReads: maxLongPollReads,
	}, nil
}

const maxJoinCapabilityFileBytes = 4 << 10

func readJoinCapabilityFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("could not open capability file")
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxJoinCapabilityFileBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, errors.New("could not read capability file")
	}
	if closeErr != nil {
		return nil, errors.New("could not close capability file")
	}
	if len(data) > maxJoinCapabilityFileBytes {
		return nil, errors.New("capability file exceeds size limit")
	}
	return data, nil
}

func parseInstalledPolicyHash(raw string) ([32]byte, error) {
	if raw == "" {
		return [32]byte{}, nil
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) != 32 {
		return [32]byte{}, fmt.Errorf("invalid RHIZA_EXPECTED_INSTALLED_POLICY_HASH: want 64 hexadecimal characters")
	}
	var digest [32]byte
	copy(digest[:], decoded)
	if digest == ([32]byte{}) {
		return [32]byte{}, fmt.Errorf("invalid RHIZA_EXPECTED_INSTALLED_POLICY_HASH: zero digest")
	}
	return digest, nil
}

func validateJoinSeeds(seeds []string) error {
	seen := make(map[string]struct{}, len(seeds))
	for _, seed := range seeds {
		endpoint, err := url.Parse(seed)
		if err != nil || endpoint.Scheme != "quic" || endpoint.User != nil || endpoint.Hostname() == "" || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Path != "" {
			return fmt.Errorf("join seed must be a QUIC host:port endpoint")
		}
		port, err := strconv.Atoi(endpoint.Port())
		if err != nil || port < 1 || port > 65535 {
			return fmt.Errorf("join seed must include a valid QUIC port")
		}
		if _, ok := seen[seed]; ok {
			return fmt.Errorf("join seeds must be unique")
		}
		seen[seed] = struct{}{}
	}
	return nil
}

func configEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// configEnvPeerToken resolves this process's private peer identity secret.
// RHIZA_PEER_TOKENS is the per-node map a StatefulSet needs because one Pod
// template cannot mount a distinct Secret per replica; RHIZA_PEER_TOKEN is the
// single-node form used by learners and standalone processes.
func configEnvPeerToken(nodeID string) (string, error) {
	raw := os.Getenv("RHIZA_PEER_TOKENS")
	if raw == "" {
		return os.Getenv("RHIZA_PEER_TOKEN"), nil
	}
	if os.Getenv("RHIZA_PEER_TOKEN") != "" {
		return "", errors.New("RHIZA_PEER_TOKENS conflicts with RHIZA_PEER_TOKEN")
	}
	var tokens map[string]string
	if err := decodeConfigEnvJSON(raw, &tokens); err != nil {
		return "", fmt.Errorf("invalid RHIZA_PEER_TOKENS: %w", err)
	}
	token, ok := tokens[nodeID]
	if !ok || token == "" {
		return "", fmt.Errorf("RHIZA_PEER_TOKENS has no entry for node %q", nodeID)
	}
	return token, nil
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
