package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
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
	"github.com/mrchypark/rhiza/pkg/fencing"
)

const (
	prepareContainerName = "rhiza-prepare"
	prepareTokenFile     = "/var/run/secrets/rhiza-fencer/token"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := runArgs(ctx, os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) (resultErr error) {
	return runCommand(ctx, false, false)
}

func runArgs(ctx context.Context, args []string) error {
	return runArgsWithOutput(ctx, args, os.Stderr)
}

func runArgsWithOutput(ctx context.Context, args []string, helpOutput io.Writer) error {
	flags := flag.NewFlagSet("rhiza", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	enroll := flags.Bool("enroll-existing-voter", false, "register an original pre-upgrade WAL while every voter is offline, then exit")
	prepareRestore := flags.Bool("prepare-restore", false, "materialize certified state for a fresh Pod, then exit without opening the server")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			flags.SetOutput(helpOutput)
			flags.Usage()
			return nil
		}
		return err
	}
	if *enroll && *prepareRestore {
		return errors.New("--enroll-existing-voter and --prepare-restore are mutually exclusive")
	}
	return runCommand(ctx, *enroll, *prepareRestore)
}

func runCommand(ctx context.Context, enroll, prepareRestore bool) (resultErr error) {
	var managed managedPrepareEnvironment
	managedEnabled := false
	if prepareRestore {
		var err error
		managed, managedEnabled, err = readManagedPrepareEnvironment()
		if err != nil {
			return err
		}
	}
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
	if prepareRestore {
		if managedEnabled {
			return runManagedPrepare(ctx, config, managed)
		}
		return rhiza.PrepareRestore(ctx, config)
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
		replicaMembers, err := objectReplicaMembers(config.ClusterID, members, role == "object-store" && config.EnableReconfiguration)
		if role == "learner" {
			replicaMembers, err = learnerReplicaMembers(os.Getenv("RHIZA_REPLICA_MEMBERS"))
		}
		if err != nil {
			return fmt.Errorf("configure %s: %w", role, err)
		}
		syncInterval, err := time.ParseDuration(getEnvOrDefault("RHIZA_REPLICA_SYNC_INTERVAL", "0s"))
		if err != nil || syncInterval < 0 {
			return errors.New("invalid RHIZA_REPLICA_SYNC_INTERVAL")
		}
		replicaConfig := rhiza.ReplicaConfig{
			ClusterID: config.ClusterID, ReplicaID: config.NodeID, DataDir: config.DataDir,
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
		if role == "learner" {
			replicaConfig.AdminToken = config.AdminToken
		}
		var replica *rhiza.ReadReplica
		history := rhiza.ReplicaHistoryVerificationConfig{BootstrapMembers: append([]rhiza.Member(nil), members...)}
		if role == "learner" && config.EnableReconfiguration {
			replica, err = rhiza.OpenLearnerWithHistory(ctx, replicaConfig, history)
		} else if role == "learner" {
			replica, err = rhiza.OpenLearner(ctx, replicaConfig)
		} else if config.EnableReconfiguration {
			replica, err = rhiza.OpenReadReplicaWithHistory(ctx, replicaConfig, history)
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

type managedPrepareEnvironment struct {
	operationID string
	podName     string
	podUID      string
	authority   string
	caFile      string
}

type managedAssignmentReader interface {
	BootstrapStatus(context.Context, string, fencing.Identity) (fencing.BootstrapStatusResult, error)
}

func readManagedPrepareEnvironment() (managedPrepareEnvironment, bool, error) {
	settings := managedPrepareEnvironment{
		operationID: os.Getenv("RHIZA_PREPARE_OPERATION_ID"),
		podName:     os.Getenv("POD_NAME"),
		podUID:      os.Getenv("POD_UID"),
		authority:   os.Getenv("FENCER_AUTHORITY_URL"),
		caFile:      os.Getenv("FENCER_CA_FILE"),
	}
	preparedUID := os.Getenv("RHIZA_PREPARED_POD_UID")
	any := settings.operationID != "" || settings.podName != "" || settings.podUID != "" || settings.authority != "" || settings.caFile != "" || preparedUID != ""
	if !any {
		return managedPrepareEnvironment{}, false, nil
	}
	if settings.podName == "" || settings.podUID == "" || settings.authority == "" || settings.caFile == "" || preparedUID == "" {
		return managedPrepareEnvironment{}, true, fmt.Errorf("managed prepare requires POD_NAME, POD_UID, RHIZA_PREPARED_POD_UID, FENCER_AUTHORITY_URL, and FENCER_CA_FILE")
	}
	if settings.podUID != preparedUID || strings.TrimSpace(settings.podUID) != settings.podUID {
		return managedPrepareEnvironment{}, true, fmt.Errorf("POD_UID differs from RHIZA_PREPARED_POD_UID")
	}
	for _, key := range []string{"RHIZA_ADMIN_TOKEN", "RHIZA_PEER_TOKEN", "RHIZA_PEER_TOKENS"} {
		if os.Getenv(key) != "" {
			return managedPrepareEnvironment{}, true, fmt.Errorf("managed prepare must not receive %s", key)
		}
	}
	for _, key := range []string{"RHIZA_CLUSTER_ID", "RHIZA_NODE_ID", "RHIZA_DATA_DIR", "RHIZA_CLUSTER_MEMBERS"} {
		if os.Getenv(key) == "" {
			return managedPrepareEnvironment{}, true, fmt.Errorf("managed prepare requires %s", key)
		}
	}
	return settings, true, nil
}

func runManagedPrepare(ctx context.Context, config rhiza.Config, settings managedPrepareEnvironment) error {
	if config.Local || !config.EnableReconfiguration || config.PreparedPodUID != settings.podUID || config.NodeID == "" || config.ClusterID == "" {
		return fmt.Errorf("managed prepare requires the assigned reconfigurable cluster identity")
	}
	if config.AdminToken != "" || config.PeerToken != "" || os.Getenv("RHIZA_PEER_TOKENS") != "" {
		return fmt.Errorf("managed prepare must not receive runtime credentials")
	}
	if err := fencing.CheckInitNamespaceQuiescence(ctx); err != nil {
		return fmt.Errorf("verify init process namespace before preparation: %w", err)
	}
	caPEM, err := os.ReadFile(settings.caFile)
	if err != nil {
		return fmt.Errorf("read fencing CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("fencing CA file contains no certificates")
	}
	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}}
	authority := &fencing.AuthorityClient{URL: settings.authority, TokenFile: prepareTokenFile, Client: client}
	candidate := fencing.Identity{SourceClusterID: config.ClusterID, NodeID: config.NodeID, PodName: settings.podName, PodUID: settings.podUID}
	assignment, err := resolveManagedAssignment(ctx, authority, settings.operationID, candidate)
	if err != nil {
		return err
	}
	if assignment.OperationID == "" || assignment.PodName != settings.podName || assignment.PodUID != settings.podUID || assignment.SourceClusterID != config.ClusterID {
		return fmt.Errorf("init assignment differs from this Pod configuration")
	}
	if assignment.Purpose == fencing.BootstrapLearner {
		if err := bindManagedLearnerAssignment(&config, assignment); err != nil {
			return err
		}
	} else if settings.operationID != "" && (assignment.OperationID != settings.operationID || assignment.NodeID != config.NodeID) {
		return fmt.Errorf("bootstrap assignment differs from this Pod configuration")
	}
	if assignment.Purpose != fencing.BootstrapInitial && assignment.Purpose != fencing.BootstrapLearner && assignment.Purpose != fencing.BootstrapDRSuccessor {
		return fmt.Errorf("managed preparation purpose is unsupported")
	}
	var attemptBytes [16]byte
	if _, err := rand.Read(attemptBytes[:]); err != nil {
		return fmt.Errorf("create init process nonce: %w", err)
	}
	attemptID := hex.EncodeToString(attemptBytes[:])
	admission, err := authority.BeginInitAdmission(ctx, assignment, attemptID)
	if err != nil {
		return fmt.Errorf("reserve init process: %w", err)
	}
	attempt := admission.Attempt()
	if attempt.Scope.ContainerName != prepareContainerName {
		return fmt.Errorf("authority observed init container %q, want %q", attempt.Scope.ContainerName, prepareContainerName)
	}
	cleanup, genesisDigest, prepareErr := rhiza.PrepareManagedRestoreWithAdmission(ctx, config, admission)
	quiescenceErr := fencing.CheckInitNamespaceQuiescence(ctx)
	outcome := managedInitOutcome(assignment.Purpose, cleanup, genesisDigest, prepareErr, quiescenceErr)
	if err := outcome.Validate(); err != nil {
		return errors.Join(prepareErr, quiescenceErr, fmt.Errorf("validate live init outcome: %w", err))
	}
	if err := authority.RecordInitOutcome(ctx, assignment, attemptID, outcome); err != nil {
		return errors.Join(prepareErr, quiescenceErr, fmt.Errorf("record live init outcome: %w", err))
	}
	if outcome.State != fencing.InitAttemptMaterialized {
		return errors.Join(prepareErr, quiescenceErr, fmt.Errorf("managed preparation did not establish its required cleanup proof"))
	}
	return nil
}

func resolveManagedAssignment(ctx context.Context, authority managedAssignmentReader, initialOperationID string, candidate fencing.Identity) (fencing.BootstrapAssignment, error) {
	if strings.HasPrefix(initialOperationID, "dr-successor-") {
		status, err := authority.BootstrapStatus(ctx, initialOperationID, candidate)
		if err != nil {
			return fencing.BootstrapAssignment{}, fmt.Errorf("read exact DR successor assignment: %w", err)
		}
		if status.Assignment == nil || status.Complete || status.AdmissionClosed {
			return fencing.BootstrapAssignment{}, fmt.Errorf("%w: exact DR successor assignment is unavailable or closed", fencing.ErrPending)
		}
		assignment := *status.Assignment
		if assignment.Validate() != nil || assignment.Purpose != fencing.BootstrapDRSuccessor || assignment.OperationID != initialOperationID ||
			assignment.SourceClusterID != candidate.SourceClusterID || assignment.PodName != candidate.PodName || assignment.PodUID != candidate.PodUID || assignment.NodeID != candidate.NodeID {
			return fencing.BootstrapAssignment{}, fmt.Errorf("DR successor assignment differs from exact Pod binding")
		}
		return assignment, nil
	}
	own, err := authority.BootstrapStatus(ctx, "", candidate)
	if err != nil {
		return fencing.BootstrapAssignment{}, fmt.Errorf("read own init assignment: %w", err)
	}
	if own.Assignment != nil {
		assignment := *own.Assignment
		if own.Complete || own.AdmissionClosed || assignment.Purpose != fencing.BootstrapLearner || assignment.Validate() != nil ||
			assignment.SourceClusterID != candidate.SourceClusterID || assignment.PodName != candidate.PodName || assignment.PodUID != candidate.PodUID {
			return fencing.BootstrapAssignment{}, fmt.Errorf("own-init status is not an exact active learner assignment")
		}
		return assignment, nil
	}
	if initialOperationID == "" {
		return fencing.BootstrapAssignment{}, fmt.Errorf("%w: no own learner assignment is available", fencing.ErrPending)
	}
	initial, err := authority.BootstrapStatus(ctx, initialOperationID, candidate)
	if err != nil {
		return fencing.BootstrapAssignment{}, fmt.Errorf("read exact initial init assignment: %w", err)
	}
	if initial.Assignment == nil || initial.Complete || initial.AdmissionClosed {
		return fencing.BootstrapAssignment{}, fmt.Errorf("%w: exact initial init assignment is unavailable or closed", fencing.ErrPending)
	}
	assignment := *initial.Assignment
	if assignment.Validate() != nil || assignment.OperationID != initialOperationID || assignment.SourceClusterID != candidate.SourceClusterID ||
		assignment.PodName != candidate.PodName || assignment.PodUID != candidate.PodUID {
		return fencing.BootstrapAssignment{}, fmt.Errorf("initial assignment differs from exact Pod binding")
	}
	return assignment, nil
}

func bindManagedLearnerAssignment(config *rhiza.Config, assignment fencing.BootstrapAssignment) error {
	if config == nil || assignment.Validate() != nil || assignment.Purpose != fencing.BootstrapLearner ||
		assignment.PodUID != config.PreparedPodUID || assignment.SourceClusterID != string(config.ClusterID) ||
		assignment.PublicMember.ID != assignment.NodeID || assignment.PublicMember.URL == "" || assignment.PublicMember.PeerURL == "" || assignment.PublicMember.WALIdentity != "" {
		return fmt.Errorf("own-init learner assignment lacks its exact public binding")
	}
	if assignment.MembershipHash != fencing.BootstrapMembersHash(config.Members) {
		return fmt.Errorf("learner assignment differs from immutable bootstrap membership")
	}
	if assignment.Durability != string(config.ObjStoreDurability) || assignment.StoreIdentityHash != bootstrapStoreIdentityHash(*config) {
		return fmt.Errorf("learner assignment differs from object-store profile")
	}
	key, err := hex.DecodeString(assignment.PublicMember.PublicKey)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return fmt.Errorf("learner assignment public key is invalid")
	}
	member := rhiza.Member{
		ID: rhiza.NodeID(assignment.PublicMember.ID), URL: assignment.PublicMember.URL,
		PeerURL: assignment.PublicMember.PeerURL, LogURL: assignment.PublicMember.LogURL,
	}
	copy(member.PublicKey[:], key)
	if member.ID == "" || member.PublicKey == (rhiza.PublicKey{}) {
		return fmt.Errorf("learner assignment public identity is empty")
	}
	if config.Learner != nil && *config.Learner != member {
		return fmt.Errorf("configured learner identity differs from authority assignment")
	}
	config.NodeID = assignment.NodeID
	config.Learner = &member
	config.ObjStoreDurability = rhiza.ObjectStoreDurability(assignment.Durability)
	return nil
}

func bootstrapStoreIdentityHash(config rhiza.Config) string {
	provider := config.ObjStoreProvider
	if provider == "" {
		provider = "filesystem"
	}
	connectionDigest := ""
	if raw := config.ObjStoreAzureConnectionString; raw != "" {
		encoded, _ := json.Marshal(raw)
		digest := sha256.Sum256(encoded)
		connectionDigest = hex.EncodeToString(digest[:])
	}
	return fencing.BootstrapStoreIdentityHash(map[string]string{
		"RHIZA_OBJSTORE_PROVIDER":              provider,
		"RHIZA_OBJSTORE_ENDPOINT":              config.ObjStoreEndpoint,
		"RHIZA_OBJSTORE_BUCKET":                config.ObjStoreBucket,
		"RHIZA_OBJSTORE_PREFIX":                config.ObjStorePrefix,
		"RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT": config.ObjStoreAzureStorageAccount,
		"azure_connection_string_digest":       connectionDigest,
	})
}

func managedInitOutcome(purpose fencing.BootstrapPurpose, cleanup fencing.InitLiveCleanup, genesisDigest string, prepareErr, quiescenceErr error) fencing.InitAttemptOutcome {
	cleanup.NamespaceQuiescent = quiescenceErr == nil
	outcome := fencing.InitAttemptOutcome{State: fencing.InitAttemptFailed, FailureClass: fencing.InitFailureOther, LiveCleanup: cleanup}
	if purpose == fencing.BootstrapInitial {
		outcome.FailureClass = fencing.InitFailureGenesis
	} else {
		outcome.FailureClass = fencing.InitFailureRestore
	}
	if !cleanup.CleanupSucceeded || quiescenceErr != nil {
		outcome.FailureClass = fencing.InitFailureCleanup
	} else if prepareErr == nil {
		outcome.State = fencing.InitAttemptMaterialized
		outcome.FailureClass = ""
		if purpose == fencing.BootstrapInitial {
			outcome.GenesisDigest = genesisDigest
		}
	}
	return outcome
}

func objectReplicaMembers(clusterID string, members []rhiza.Member, preservePublicIdentity bool) ([]rhiza.ReplicaMember, error) {
	result := make([]rhiza.ReplicaMember, 0, len(members))
	for _, member := range members {
		if !preservePublicIdentity {
			if member.ID == "" {
				return nil, errors.New("voter ID is required")
			}
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

// learnerReplicaMembers parses the public voter identities a learner may pin.
// Voter peer tokens are never part of a learner's configuration.
func learnerReplicaMembers(raw string) ([]rhiza.ReplicaMember, error) {
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
