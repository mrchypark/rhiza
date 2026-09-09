package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mrchypark/rhiza"
)

func main() {
	enroll := flag.Bool("enroll-existing-voter", false, "register an original pre-upgrade WAL while every voter is offline, then exit")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := runCommand(ctx, *enroll); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) (resultErr error) {
	return runCommand(ctx, false)
}

func runCommand(ctx context.Context, enroll bool) (resultErr error) {
	config, err := rhiza.ConfigFromEnv()
	if err != nil {
		return err
	}
	members := config.Members

	role := getEnvOrDefault("RHIZA_ROLE", "voter")
	if enroll {
		if role != "voter" {
			return fmt.Errorf("offline enrollment requires RHIZA_ROLE=voter")
		}
		return rhiza.EnrollExistingVoter(ctx, config)
	}
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
