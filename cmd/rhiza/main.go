package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/node"
)

func main() {
	var members []types.NodeConfig
	if raw := os.Getenv("RHIZA_CLUSTER_MEMBERS"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &members); err != nil {
			log.Fatalf("invalid RHIZA_CLUSTER_MEMBERS: %v", err)
		}
	}
	hedgeDelay, err := time.ParseDuration(getEnvOrDefault("RHIZA_HEDGE_DELAY", "5ms"))
	if err != nil || hedgeDelay < 0 {
		log.Fatalf("invalid RHIZA_HEDGE_DELAY")
	}
	checkpointInterval, err := time.ParseDuration(getEnvOrDefault("RHIZA_CHECKPOINT_INTERVAL", "1m"))
	if err != nil || checkpointInterval < 0 {
		log.Fatalf("invalid RHIZA_CHECKPOINT_INTERVAL")
	}
	objStoreRetries, err := strconv.Atoi(getEnvOrDefault("RHIZA_OBJSTORE_MAX_RETRIES", "3"))
	if err != nil || objStoreRetries < 0 {
		log.Fatalf("invalid RHIZA_OBJSTORE_MAX_RETRIES")
	}

	// Parse config from environment
	config := &types.ExecutionConfig{
		ClusterID:            types.ClusterID(getEnvOrDefault("RHIZA_CLUSTER_ID", "cluster-a")),
		NodeID:               types.NodeID(getEnvOrDefault("RHIZA_NODE_ID", "node-1")),
		Profile:              types.Profile(getEnvOrDefault("RHIZA_EXECUTION_PROFILE", string(materializer.BuildProfile()))),
		DataDir:              getEnvOrDefault("RHIZA_DATA_DIR", "./rhiza-data"),
		BindAddr:             getEnvOrDefault("RHIZA_BIND_ADDR", "127.0.0.1:8080"),
		PeerAddr:             getEnvOrDefault("RHIZA_PEER_ADDR", "127.0.0.1:9090"),
		AdminToken:           os.Getenv("RHIZA_ADMIN_TOKEN"),
		ClientToken:          os.Getenv("RHIZA_CLIENT_TOKEN"),
		Members:              members,
		HedgeDelay:           hedgeDelay,
		ObjStoreProvider:     os.Getenv("RHIZA_OBJSTORE_PROVIDER"),
		ObjStoreDir:          os.Getenv("RHIZA_FILESYSTEM_DIR"),
		ObjStorePrefix:       os.Getenv("RHIZA_OBJSTORE_PREFIX"),
		ObjStoreEndpoint:     os.Getenv("RHIZA_OBJSTORE_ENDPOINT"),
		ObjStoreBucket:       os.Getenv("RHIZA_OBJSTORE_BUCKET"),
		ObjStoreRegion:       os.Getenv("RHIZA_OBJSTORE_REGION"),
		ObjStoreInsecure:     os.Getenv("RHIZA_OBJSTORE_INSECURE") == "true",
		ObjStoreRetries:      objStoreRetries,
		ObjStoreAccessKey:    os.Getenv("RHIZA_OBJSTORE_ACCESS_KEY"),
		ObjStoreSecretKey:    os.Getenv("RHIZA_OBJSTORE_SECRET_KEY"),
		ObjStoreSessionToken: os.Getenv("RHIZA_OBJSTORE_SESSION_TOKEN"),
		CheckpointInterval:   checkpointInterval,
	}

	node := node.New(config)
	defer node.Shutdown()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := node.Start(ctx); err != nil {
		log.Fatalf("node error: %v", err)
	}

}

func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
