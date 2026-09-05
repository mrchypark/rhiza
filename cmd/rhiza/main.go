package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mrchypark/rhiza"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) (resultErr error) {
	var members []rhiza.Member
	if raw := os.Getenv("RHIZA_CLUSTER_MEMBERS"); raw != "" {
		if err := decodeStrictJSON(raw, &members); err != nil {
			return fmt.Errorf("invalid RHIZA_CLUSTER_MEMBERS: %w", err)
		}
	}
	checkpointInterval, err := time.ParseDuration(getEnvOrDefault("RHIZA_CHECKPOINT_INTERVAL", "15m"))
	if err != nil || checkpointInterval < 0 {
		return errors.New("invalid RHIZA_CHECKPOINT_INTERVAL")
	}
	checkpointTailBytes, err := strconv.ParseInt(getEnvOrDefault("RHIZA_CHECKPOINT_TAIL_BYTES", "536870912"), 10, 64)
	if err != nil || checkpointTailBytes <= 0 || checkpointTailBytes > 2<<30 {
		return errors.New("invalid RHIZA_CHECKPOINT_TAIL_BYTES")
	}
	maxWALBytes, err := strconv.ParseInt(getEnvOrDefault("RHIZA_MAX_WAL_BYTES", "0"), 10, 64)
	if err != nil || maxWALBytes < 0 {
		return errors.New("invalid RHIZA_MAX_WAL_BYTES")
	}
	maxConcurrentReads, err := strconv.Atoi(getEnvOrDefault("RHIZA_MAX_CONCURRENT_READS", "0"))
	if err != nil || maxConcurrentReads < 0 {
		return errors.New("invalid RHIZA_MAX_CONCURRENT_READS")
	}
	maxLongPollReads, err := strconv.Atoi(getEnvOrDefault("RHIZA_MAX_LONG_POLL_READS", "0"))
	if err != nil || maxLongPollReads < 0 || maxConcurrentReads == 0 && maxLongPollReads != 0 || maxLongPollReads > maxConcurrentReads {
		return errors.New("invalid RHIZA_MAX_LONG_POLL_READS")
	}
	objStoreSyncInterval, err := time.ParseDuration(getEnvOrDefault("RHIZA_OBJSTORE_SYNC_INTERVAL", "1m"))
	if err != nil || objStoreSyncInterval < 0 {
		return errors.New("invalid RHIZA_OBJSTORE_SYNC_INTERVAL")
	}
	objStoreBatchDelay, err := time.ParseDuration(getEnvOrDefault("RHIZA_OBJSTORE_BATCH_DELAY", "2ms"))
	if err != nil || objStoreBatchDelay <= 0 || objStoreBatchDelay > time.Second {
		return errors.New("invalid RHIZA_OBJSTORE_BATCH_DELAY")
	}
	objStoreGCInterval, err := time.ParseDuration(getEnvOrDefault("RHIZA_OBJSTORE_GC_INTERVAL", "1h"))
	if err != nil || objStoreGCInterval < 0 {
		return errors.New("invalid RHIZA_OBJSTORE_GC_INTERVAL")
	}
	objStoreGCGracePeriod, err := time.ParseDuration(getEnvOrDefault("RHIZA_OBJSTORE_GC_GRACE_PERIOD", "24h"))
	if err != nil || objStoreGCGracePeriod < 0 {
		return errors.New("invalid RHIZA_OBJSTORE_GC_GRACE_PERIOD")
	}
	objStoreRetries, err := strconv.Atoi(getEnvOrDefault("RHIZA_OBJSTORE_MAX_RETRIES", "3"))
	if err != nil || objStoreRetries < 0 {
		return errors.New("invalid RHIZA_OBJSTORE_MAX_RETRIES")
	}
	objStoreInsecure, err := boolEnv("RHIZA_OBJSTORE_INSECURE")
	if err != nil {
		return err
	}
	objStoreDir, err := objectStoreDirEnv()
	if err != nil {
		return err
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
		ObjStoreProvider:               os.Getenv("RHIZA_OBJSTORE_PROVIDER"),
		ObjStoreDir:                    objStoreDir,
		ObjStorePrefix:                 os.Getenv("RHIZA_OBJSTORE_PREFIX"),
		ObjStoreEndpoint:               os.Getenv("RHIZA_OBJSTORE_ENDPOINT"),
		ObjStoreBucket:                 os.Getenv("RHIZA_OBJSTORE_BUCKET"),
		ObjStoreRegion:                 os.Getenv("RHIZA_OBJSTORE_REGION"),
		ObjStoreInsecure:               objStoreInsecure,
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
		MaxConcurrentReads:             maxConcurrentReads,
		MaxLongPollReads:               maxLongPollReads,
	}

	role := getEnvOrDefault("RHIZA_ROLE", "voter")
	var handler http.Handler
	var closeService func() error
	switch role {
	case "voter":
		db, err := rhiza.Open(ctx, config)
		if err != nil {
			return fmt.Errorf("open rhiza: %w", err)
		}
		handler, closeService = db.Handler(), db.Close
	case "object-store", "learner":
		replicaMembers, err := objectReplicaMembers(members)
		if role == "learner" {
			replicaMembers, err = learnerReplicaMembers(os.Getenv("RHIZA_REPLICA_MEMBERS"), members)
		}
		if err != nil {
			return fmt.Errorf("configure %s: %w", role, err)
		}
		syncInterval, err := time.ParseDuration(getEnvOrDefault("RHIZA_REPLICA_SYNC_INTERVAL", "0s"))
		if err != nil || syncInterval < 0 {
			return errors.New("invalid RHIZA_REPLICA_SYNC_INTERVAL")
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
			MaxConcurrentReads: config.MaxConcurrentReads, MaxLongPollReads: config.MaxLongPollReads,
		}
		var replica *rhiza.ReadReplica
		if role == "learner" {
			replica, err = rhiza.OpenLearner(ctx, replicaConfig)
		} else {
			replica, err = rhiza.OpenReadReplica(ctx, replicaConfig)
		}
		if err != nil {
			return fmt.Errorf("open %s: %w", role, err)
		}
		handler, closeService = replica.Handler(), replica.Close
	default:
		return fmt.Errorf("invalid RHIZA_ROLE %q (want voter, object-store, or learner)", role)
	}
	defer func() { resultErr = errors.Join(resultErr, closeService()) }()

	server := &http.Server{
		Addr: config.BindAddr, Handler: handler,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second,
		IdleTimeout: 120 * time.Second, MaxHeaderBytes: 64 << 10,
	}
	listener, err := net.Listen("tcp", config.BindAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", config.BindAddr, err)
	}
	defer listener.Close()
	serverCtx, stopServer := context.WithCancel(ctx)
	defer stopServer()
	shutdownDone := make(chan error, 1)
	go func() {
		<-serverCtx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		err := server.Shutdown(shutdownCtx)
		if err != nil {
			err = errors.Join(err, server.Close())
		}
		shutdownDone <- err
	}()
	log.Printf("Rhiza %s HTTP adapter listening on %s", role, listener.Addr())
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve rhiza: %w", err)
	}
	if ctx.Err() != nil {
		if err := <-shutdownDone; err != nil {
			return fmt.Errorf("shut down HTTP server: %w", err)
		}
	}
	return nil
}

func objectReplicaMembers(members []rhiza.Member) ([]rhiza.ReplicaMember, error) {
	result := make([]rhiza.ReplicaMember, 0, len(members))
	for _, member := range members {
		if member.ID == "" {
			return nil, errors.New("voter ID is required")
		}
		result = append(result, rhiza.ReplicaMember{ID: member.ID})
	}
	return result, nil
}

func learnerReplicaMembers(raw string, voterConfig []rhiza.Member) ([]rhiza.ReplicaMember, error) {
	for _, member := range voterConfig {
		if member.Token != "" {
			return nil, errors.New("learner must not receive voter tokens in RHIZA_CLUSTER_MEMBERS")
		}
	}
	var configured []struct {
		ID        string `json:"node_id"`
		PeerURL   string `json:"peer_url"`
		PublicKey string `json:"public_key"`
	}
	if err := decodeStrictJSON(raw, &configured); err != nil {
		return nil, fmt.Errorf("invalid RHIZA_REPLICA_MEMBERS: %w", err)
	}
	result := make([]rhiza.ReplicaMember, 0, len(configured))
	for _, member := range configured {
		key, err := base64.StdEncoding.DecodeString(member.PublicKey)
		if err != nil || len(key) != ed25519.PublicKeySize || member.ID == "" || member.PeerURL == "" {
			return nil, fmt.Errorf("learner members require node_id, peer_url, and a base64 Ed25519 public_key")
		}
		identity := rhiza.ReplicaMember{ID: rhiza.NodeID(member.ID), PeerURL: member.PeerURL}
		copy(identity.PublicKey[:], key)
		if identity.PublicKey == ([ed25519.PublicKeySize]byte{}) {
			return nil, errors.New("learner public_key must not be zero")
		}
		result = append(result, identity)
	}
	if len(result) == 0 {
		return nil, errors.New("learner voter membership is required")
	}
	return result, nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

func decodeStrictJSON(raw string, value any) error {
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

func boolEnv(key string) (bool, error) {
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

func objectStoreDirEnv() (string, error) {
	value, legacy := os.Getenv("RHIZA_OBJSTORE_DIR"), os.Getenv("RHIZA_FILESYSTEM_DIR")
	if value != "" && legacy != "" && value != legacy {
		return "", errors.New("RHIZA_OBJSTORE_DIR conflicts with legacy RHIZA_FILESYSTEM_DIR")
	}
	if value != "" {
		return value, nil
	}
	return legacy, nil
}
