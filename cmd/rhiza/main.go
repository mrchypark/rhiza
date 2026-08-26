package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/mrchypark/rhiza"
)

func main() {
	var members []rhiza.Member
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
	objStoreSyncInterval, err := time.ParseDuration(getEnvOrDefault("RHIZA_OBJSTORE_SYNC_INTERVAL", "1m"))
	if err != nil || objStoreSyncInterval < 0 {
		log.Fatalf("invalid RHIZA_OBJSTORE_SYNC_INTERVAL")
	}
	objStoreGCInterval, err := time.ParseDuration(getEnvOrDefault("RHIZA_OBJSTORE_GC_INTERVAL", "1h"))
	if err != nil || objStoreGCInterval < 0 {
		log.Fatalf("invalid RHIZA_OBJSTORE_GC_INTERVAL")
	}
	objStoreGCGracePeriod, err := time.ParseDuration(getEnvOrDefault("RHIZA_OBJSTORE_GC_GRACE_PERIOD", "24h"))
	if err != nil || objStoreGCGracePeriod < 0 {
		log.Fatalf("invalid RHIZA_OBJSTORE_GC_GRACE_PERIOD")
	}
	objStoreRetries, err := strconv.Atoi(getEnvOrDefault("RHIZA_OBJSTORE_MAX_RETRIES", "3"))
	if err != nil || objStoreRetries < 0 {
		log.Fatalf("invalid RHIZA_OBJSTORE_MAX_RETRIES")
	}

	// Parse config from environment
	config := rhiza.Config{
		ClusterID:             getEnvOrDefault("RHIZA_CLUSTER_ID", "cluster-a"),
		NodeID:                getEnvOrDefault("RHIZA_NODE_ID", "node-1"),
		Profile:               rhiza.Profile(getEnvOrDefault("RHIZA_EXECUTION_PROFILE", "")),
		DataDir:               getEnvOrDefault("RHIZA_DATA_DIR", "./rhiza-data"),
		BindAddr:              getEnvOrDefault("RHIZA_BIND_ADDR", "127.0.0.1:8080"),
		PeerAddr:              getEnvOrDefault("RHIZA_PEER_ADDR", "127.0.0.1:9090"),
		AdminToken:            os.Getenv("RHIZA_ADMIN_TOKEN"),
		ClientToken:           os.Getenv("RHIZA_CLIENT_TOKEN"),
		Members:               members,
		HedgeDelay:            hedgeDelay,
		ObjStoreProvider:      os.Getenv("RHIZA_OBJSTORE_PROVIDER"),
		ObjStoreDir:           os.Getenv("RHIZA_FILESYSTEM_DIR"),
		ObjStorePrefix:        os.Getenv("RHIZA_OBJSTORE_PREFIX"),
		ObjStoreEndpoint:      os.Getenv("RHIZA_OBJSTORE_ENDPOINT"),
		ObjStoreBucket:        os.Getenv("RHIZA_OBJSTORE_BUCKET"),
		ObjStoreRegion:        os.Getenv("RHIZA_OBJSTORE_REGION"),
		ObjStoreInsecure:      os.Getenv("RHIZA_OBJSTORE_INSECURE") == "true",
		ObjStoreRetries:       objStoreRetries,
		ObjStoreAccessKey:     os.Getenv("RHIZA_OBJSTORE_ACCESS_KEY"),
		ObjStoreSecretKey:     os.Getenv("RHIZA_OBJSTORE_SECRET_KEY"),
		ObjStoreSessionToken:  os.Getenv("RHIZA_OBJSTORE_SESSION_TOKEN"),
		ObjStoreDurability:    rhiza.ObjectStoreDurability(getEnvOrDefault("RHIZA_OBJSTORE_DURABILITY", "async")),
		ObjStoreSyncInterval:  objStoreSyncInterval,
		ObjStoreGCInterval:    objStoreGCInterval,
		ObjStoreGCGracePeriod: objStoreGCGracePeriod,
		CheckpointInterval:    checkpointInterval,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	db, err := rhiza.Open(ctx, config)
	if err != nil {
		log.Fatalf("open rhiza: %v", err)
	}
	defer db.Close()

	server := &http.Server{
		Addr: config.BindAddr, Handler: db.Handler(),
		ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 120 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("Rhiza HTTP adapter listening on %s", config.BindAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("serve rhiza: %v", err)
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
