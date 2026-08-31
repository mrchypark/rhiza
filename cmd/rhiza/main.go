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
	checkpointInterval, err := time.ParseDuration(getEnvOrDefault("RHIZA_CHECKPOINT_INTERVAL", "15m"))
	if err != nil || checkpointInterval < 0 {
		log.Fatalf("invalid RHIZA_CHECKPOINT_INTERVAL")
	}
	checkpointTailBytes, err := strconv.ParseInt(getEnvOrDefault("RHIZA_CHECKPOINT_TAIL_BYTES", "536870912"), 10, 64)
	if err != nil || checkpointTailBytes <= 0 || checkpointTailBytes > 2<<30 {
		log.Fatalf("invalid RHIZA_CHECKPOINT_TAIL_BYTES")
	}
	maxWALBytes, err := strconv.ParseInt(getEnvOrDefault("RHIZA_MAX_WAL_BYTES", "0"), 10, 64)
	if err != nil || maxWALBytes < 0 {
		log.Fatalf("invalid RHIZA_MAX_WAL_BYTES")
	}
	objStoreSyncInterval, err := time.ParseDuration(getEnvOrDefault("RHIZA_OBJSTORE_SYNC_INTERVAL", "1m"))
	if err != nil || objStoreSyncInterval < 0 {
		log.Fatalf("invalid RHIZA_OBJSTORE_SYNC_INTERVAL")
	}
	objStoreBatchDelay, err := time.ParseDuration(getEnvOrDefault("RHIZA_OBJSTORE_BATCH_DELAY", "2ms"))
	if err != nil || objStoreBatchDelay <= 0 || objStoreBatchDelay > time.Second {
		log.Fatalf("invalid RHIZA_OBJSTORE_BATCH_DELAY")
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
		ClusterID:                      getEnvOrDefault("RHIZA_CLUSTER_ID", "cluster-a"),
		NodeID:                         getEnvOrDefault("RHIZA_NODE_ID", "node-1"),
		DataDir:                        getEnvOrDefault("RHIZA_DATA_DIR", "./rhiza-data"),
		BindAddr:                       getEnvOrDefault("RHIZA_BIND_ADDR", "127.0.0.1:8080"),
		PeerAddr:                       getEnvOrDefault("RHIZA_PEER_ADDR", "127.0.0.1:9090"),
		AdminToken:                     os.Getenv("RHIZA_ADMIN_TOKEN"),
		Members:                        members,
		HedgeDelay:                     &hedgeDelay,
		ObjStoreProvider:               os.Getenv("RHIZA_OBJSTORE_PROVIDER"),
		ObjStoreDir:                    os.Getenv("RHIZA_FILESYSTEM_DIR"),
		ObjStorePrefix:                 os.Getenv("RHIZA_OBJSTORE_PREFIX"),
		ObjStoreEndpoint:               os.Getenv("RHIZA_OBJSTORE_ENDPOINT"),
		ObjStoreBucket:                 os.Getenv("RHIZA_OBJSTORE_BUCKET"),
		ObjStoreRegion:                 os.Getenv("RHIZA_OBJSTORE_REGION"),
		ObjStoreInsecure:               os.Getenv("RHIZA_OBJSTORE_INSECURE") == "true",
		ObjStoreRetries:                objStoreRetries,
		ObjStoreAccessKey:              os.Getenv("RHIZA_OBJSTORE_ACCESS_KEY"),
		ObjStoreSecretKey:              os.Getenv("RHIZA_OBJSTORE_SECRET_KEY"),
		ObjStoreSessionToken:           os.Getenv("RHIZA_OBJSTORE_SESSION_TOKEN"),
		ObjStoreServiceAccount:         os.Getenv("RHIZA_OBJSTORE_SERVICE_ACCOUNT"),
		ObjStoreAzureTenantID:          os.Getenv("RHIZA_OBJSTORE_AZURE_TENANT_ID"),
		ObjStoreAzureClientID:          os.Getenv("RHIZA_OBJSTORE_AZURE_CLIENT_ID"),
		ObjStoreAzureClientSecret:      os.Getenv("RHIZA_OBJSTORE_AZURE_CLIENT_SECRET"),
		ObjStoreAzureStorageAccount:    os.Getenv("RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT"),
		ObjStoreAzureStorageAccountKey: os.Getenv("RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT_KEY"),
		ObjStoreAzureConnectionString:  os.Getenv("RHIZA_OBJSTORE_AZURE_CONNECTION_STRING"),
		ObjStoreAzureUserAssignedID:    os.Getenv("RHIZA_OBJSTORE_AZURE_USER_ASSIGNED_ID"),
		ObjStoreDurability:             rhiza.ObjectStoreDurability(getEnvOrDefault("RHIZA_OBJSTORE_DURABILITY", "async")),
		ObjStoreSyncInterval:           objStoreSyncInterval,
		ObjStoreBatchDelay:             objStoreBatchDelay,
		ObjStoreGCInterval:             objStoreGCInterval,
		ObjStoreGCGracePeriod:          objStoreGCGracePeriod,
		CheckpointInterval:             checkpointInterval,
		CheckpointTailBytes:            checkpointTailBytes,
		MaxWALBytes:                    maxWALBytes,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	role := getEnvOrDefault("RHIZA_ROLE", "voter")
	var handler http.Handler
	var closeService func() error
	switch role {
	case "voter":
		db, err := rhiza.Open(ctx, config)
		if err != nil {
			log.Fatalf("open rhiza: %v", err)
		}
		handler, closeService = db.Handler(), db.Close
	case "object-store", "learner":
		replicaMembers, err := replicaMembers(config.ClusterID, members, role == "learner")
		if err != nil {
			log.Fatalf("configure %s: %v", role, err)
		}
		syncInterval, err := time.ParseDuration(getEnvOrDefault("RHIZA_REPLICA_SYNC_INTERVAL", "0s"))
		if err != nil || syncInterval < 0 {
			log.Fatalf("invalid RHIZA_REPLICA_SYNC_INTERVAL")
		}
		replicaConfig := rhiza.ReplicaConfig{
			ClusterID: config.ClusterID, ReplicaID: config.NodeID, DataDir: config.DataDir, AdminToken: config.AdminToken,
			Members: replicaMembers, SyncInterval: syncInterval, ObjStoreEndpoint: config.ObjStoreEndpoint,
			ObjStoreBucket: config.ObjStoreBucket, ObjStoreProvider: config.ObjStoreProvider, ObjStoreDir: config.ObjStoreDir,
			ObjStorePrefix: config.ObjStorePrefix, ObjStoreRegion: config.ObjStoreRegion, ObjStoreInsecure: config.ObjStoreInsecure,
			ObjStoreRetries: config.ObjStoreRetries, ObjStoreAccessKey: config.ObjStoreAccessKey,
			ObjStoreSecretKey: config.ObjStoreSecretKey, ObjStoreSessionToken: config.ObjStoreSessionToken,
			ObjStoreServiceAccount: config.ObjStoreServiceAccount, ObjStoreAzureTenantID: config.ObjStoreAzureTenantID,
			ObjStoreAzureClientID: config.ObjStoreAzureClientID, ObjStoreAzureClientSecret: config.ObjStoreAzureClientSecret,
			ObjStoreAzureStorageAccount: config.ObjStoreAzureStorageAccount, ObjStoreAzureStorageAccountKey: config.ObjStoreAzureStorageAccountKey,
			ObjStoreAzureConnectionString: config.ObjStoreAzureConnectionString, ObjStoreAzureUserAssignedID: config.ObjStoreAzureUserAssignedID,
		}
		var replica *rhiza.ReadReplica
		if role == "learner" {
			replica, err = rhiza.OpenLearner(ctx, replicaConfig)
		} else {
			replica, err = rhiza.OpenReadReplica(ctx, replicaConfig)
		}
		if err != nil {
			log.Fatalf("open %s: %v", role, err)
		}
		handler, closeService = replica.Handler(), replica.Close
	default:
		log.Fatalf("invalid RHIZA_ROLE %q (want voter, object-store, or learner)", role)
	}
	defer closeService()

	server := &http.Server{
		Addr: config.BindAddr, Handler: handler,
		ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 120 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("Rhiza %s HTTP adapter listening on %s", role, config.BindAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("serve rhiza: %v", err)
	}
}

func replicaMembers(clusterID string, members []rhiza.Member, requirePeerIdentity bool) ([]rhiza.ReplicaMember, error) {
	result := make([]rhiza.ReplicaMember, 0, len(members))
	for _, member := range members {
		if !requirePeerIdentity {
			result = append(result, rhiza.ReplicaMember{ID: member.ID})
			continue
		}
		identity, err := rhiza.NewReplicaMember(clusterID, member)
		if err != nil {
			return nil, err
		}
		result = append(result, identity)
	}
	return result, nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
