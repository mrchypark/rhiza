// rhiza-fencer runs either the cooperative authority or a supervised application.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza"
	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
	thanosobjstore "github.com/thanos-io/objstore"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: rhiza-fencer serve|supervise [options]")
	}
	switch args[0] {
	case "serve":
		return serve(ctx, args[1:])
	case "supervise":
		return supervise(ctx, args[1:])
	default:
		return fmt.Errorf("unsupported fencing command")
	}
}

func supervise(ctx context.Context, args []string) error {
	f := flag.NewFlagSet("supervise", flag.ContinueOnError)
	endpoint := f.String("authority-url", os.Getenv("FENCER_AUTHORITY_URL"), "HTTPS supervisor authority endpoint")
	token := f.String("token-file", "/var/run/secrets/rhiza-fencer/token", "audience-bound Pod service account token")
	ca := f.String("ca-file", os.Getenv("FENCER_CA_FILE"), "authority TLS CA certificate")
	socket := f.String("socket", "/run/rhiza-fencer/control.sock", "private child Unix socket")
	if err := f.Parse(args); err != nil {
		return err
	}
	if len(f.Args()) == 0 {
		return fmt.Errorf("supervise requires -- application [args]")
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return err
	}
	if *ca != "" {
		data, err := os.ReadFile(*ca)
		if err != nil {
			return err
		}
		if !roots.AppendCertsFromPEM(data) {
			return fmt.Errorf("authority CA contains no certificate")
		}
	}
	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}}
	if err := os.MkdirAll(filepath.Dir(*socket), 0700); err != nil {
		return err
	}
	podName := os.Getenv("RHIZA_POD_NAME")
	if podName == "" {
		podName = os.Getenv("RHIZA_NODE_ID")
	}
	preflight, err := rhiza.EngineRepairPreflightFromEnv()
	if err != nil {
		return err
	}
	s := &fencing.Supervisor{Authority: &fencing.AuthorityClient{URL: *endpoint, TokenFile: *token, Client: client}, SocketPath: *socket, Command: f.Args(), Preflight: preflight, Identity: fencing.Identity{SourceClusterID: os.Getenv("RHIZA_CLUSTER_ID"), NodeID: os.Getenv("RHIZA_NODE_ID"), PodName: podName, PodUID: os.Getenv("POD_UID")}}
	return s.Run(ctx)
}

func serve(ctx context.Context, args []string) error {
	f := flag.NewFlagSet("serve", flag.ContinueOnError)
	namespace := f.String("namespace", os.Getenv("POD_NAMESPACE"), "single managed namespace")
	listen := f.String("listen", ":9444", "TLS listener")
	cert := f.String("tls-cert", os.Getenv("FENCER_TLS_CERT"), "TLS certificate file")
	key := f.String("tls-key", os.Getenv("FENCER_TLS_KEY"), "TLS private key file")
	attestorURL := f.String("attestor-url", os.Getenv("RHIZA_FENCE_ATTESTOR_URL"), "unsupported legacy v1 attestor endpoint")
	attestorToken := f.String("attestor-token-file", os.Getenv("RHIZA_FENCE_ATTESTOR_TOKEN_FILE"), "unsupported legacy v1 attestor token")
	attestorCA := f.String("attestor-ca-file", os.Getenv("RHIZA_FENCE_ATTESTOR_CA_FILE"), "unsupported legacy v1 attestor CA")
	stageDir := f.String("source-stage-dir", os.Getenv("FENCER_SOURCE_STAGE_DIR"), "pre-provisioned local POSIX source staging directory")
	stageIncarnation := f.String("source-stage-incarnation", os.Getenv("FENCER_SOURCE_STAGE_INCARNATION"), "pinned source staging incarnation digest")
	sourceReceiptDir := f.String("source-mutation-receipt-dir", os.Getenv("FENCER_SOURCE_MUTATION_RECEIPT_DIR"), "pre-provisioned durable source mutation receipt directory")
	sourceWriterCredentials := f.String("source-writer-credentials-file", os.Getenv("FENCER_SOURCE_WRITER_CREDENTIALS_FILE"), "separate GCS source-writer credentials file")
	sourceWriterUseADC := f.Bool("source-writer-use-adc", os.Getenv("FENCER_SOURCE_WRITER_USE_ADC") == "true", "use this authority Pod's Application Default Credentials for GCS source mutations")
	ackLossConfigFile := f.String("gcs-ack-loss-config-file", os.Getenv("FENCER_GCS_ACK_LOSS_CONFIG_FILE"), "private exact-run config for one-shot held-create GCS acknowledgement-loss qualification (disabled by default)")
	runtimeAgentCA := f.String("runtime-agent-ca-file", os.Getenv("FENCER_RUNTIME_AGENT_CA_FILE"), "dedicated node runtime-agent TLS CA; enables runtime enrollment and terminal evidence")
	runtimeAgentToken := f.String("runtime-agent-token-file", "/var/run/secrets/rhiza-fencer/runtime-agent-token", "TokenReview-audience-bound runtime-agent credential")
	runtimeAgentServer := f.String("runtime-agent-server-name", "runtime-agent", "TLS DNS identity on node runtime-agent certificates")
	runtimeAgentPort := f.String("runtime-agent-port", "9445", "node runtime-agent HTTPS host port")
	authorityStatePrefix := f.String("authority-state-prefix", os.Getenv("FENCER_AUTHORITY_STATE_PREFIX"), "canonical authority state prefix in the configured remote ObjectStorage bucket")
	independentMutation := f.Bool("independent-mutation-profile", os.Getenv("FENCER_INDEPENDENT_MUTATION_PROFILE") == "true", "require canonical remote state and source-mutation providers for the independent NoPVC mutation profile")
	authorityProfile := f.String("authority-profile", os.Getenv("FENCER_AUTHORITY_PROFILE"), "explicit authority profile (empty or logical-writer-v1)")
	controllerSA := os.Getenv("FENCER_CONTROLLER_SERVICE_ACCOUNT")
	if controllerSA == "" {
		controllerSA = "rhiza-operator"
	}
	controllerServiceAccount := f.String("controller-service-account", controllerSA, "service account allowed to register verified archive forks")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *attestorURL != "" || *attestorToken != "" || *attestorCA != "" {
		return fmt.Errorf("legacy v1 quiet-window attestors cannot authorize recovery; independent healing requires enrolled runtime and mutation evidence v2 providers, which this command does not yet configure")
	}
	if *cert == "" || *key == "" {
		return fmt.Errorf("fencing authority requires TLS certificate and key")
	}
	stateConfig := objectstore.LoadConfig()
	if err := validateSourceWriterConfig(sourceWriterConfig{
		Independent: *independentMutation, AuthorityProfile: *authorityProfile, HasCanonicalState: hasRemoteCanonicalState(stateConfig), SourceProvider: stateConfig.Provider,
		RuntimeAgentCA: *runtimeAgentCA, StageDir: *stageDir, StageIncarnation: *stageIncarnation,
		ReceiptDir: *sourceReceiptDir, CredentialsFile: *sourceWriterCredentials, UseADC: *sourceWriterUseADC,
		AckLossConfig:       *ackLossConfigFile != "",
		StorageEmulatorHost: os.Getenv("STORAGE_EMULATOR_HOST"),
	}); err != nil {
		return err
	}
	kube, err := operator.NewInCluster(*namespace)
	if err != nil {
		return err
	}
	var authorityState *operator.AuthorityStateStore
	var archiveBucket thanosobjstore.Bucket
	if hasRemoteCanonicalState(stateConfig) {
		stateBucket, err := objectstore.NewBucket(stateConfig)
		if err != nil {
			return fmt.Errorf("configure canonical authority ObjectStorage: %w", err)
		}
		defer stateBucket.Close()
		archiveBucket = stateBucket
		statePrefix := *authorityStatePrefix
		if statePrefix == "" {
			statePrefix = "rhiza-authority-state"
		}
		authorityState, err = operator.NewAuthorityStateStore(stateBucket, statePrefix)
		if err != nil {
			return err
		}
	}
	if *independentMutation {
		// Static profile validation above runs before Kubernetes access. This
		// check confirms the configured remote authority store opened correctly.
		if authorityState == nil {
			return fmt.Errorf("independent mutation profile requires canonical remote authority state")
		}
	}
	executor := newFencingExecutor(kube, authorityState, *independentMutation, *authorityProfile, *sourceReceiptDir, *sourceWriterCredentials, *sourceWriterUseADC)
	executor.ArchiveBucket = archiveBucket
	executor.AutomaticControlPrefix = stateConfig.Prefix
	executor.ArchiveStoreIdentity = operator.StoreIdentity(map[string]string{
		"RHIZA_OBJSTORE_PROVIDER":                string(stateConfig.Provider),
		"RHIZA_OBJSTORE_ENDPOINT":                stateConfig.Endpoint,
		"RHIZA_OBJSTORE_BUCKET":                  stateConfig.Bucket,
		"RHIZA_OBJSTORE_PREFIX":                  stateConfig.Prefix,
		"RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT":   stateConfig.AzureStorageAccount,
		"RHIZA_OBJSTORE_AZURE_CONNECTION_STRING": stateConfig.AzureConnectionString,
	})
	executor.ControllerServiceAccount = *controllerServiceAccount
	var ackLossFault *operator.GCSMutationAckLossFault
	if *ackLossConfigFile != "" {
		info, err := os.Lstat(*ackLossConfigFile)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
			return fmt.Errorf("GCS acknowledgement-loss config file is missing or unsafe")
		}
		data, err := os.ReadFile(*ackLossConfigFile)
		if err != nil {
			return fmt.Errorf("read GCS acknowledgement-loss config: %w", err)
		}
		config, err := operator.ParseGCSMutationAckLossConfig(data)
		if err != nil {
			return err
		}
		ackLossFault, err = operator.NewGCSMutationAckLossFault(config, func(receipt operator.GCSMutationAckLossReceipt) {
			encoded, marshalErr := json.Marshal(receipt)
			if marshalErr != nil {
				log.Printf("GCS acknowledgement-loss qualification receipt encoding failed: %v", marshalErr)
				return
			}
			log.Printf("GCS_ACK_LOSS_QUALIFICATION %s", encoded)
		})
		if err != nil {
			return err
		}
		executor.GCSMutationAckLoss = ackLossFault
	}
	if *runtimeAgentCA != "" {
		ca, err := os.ReadFile(*runtimeAgentCA)
		if err != nil {
			return fmt.Errorf("read runtime-agent CA: %w", err)
		}
		client := &operator.RuntimeAgentClient{Kube: kube, AuthorityID: "rhiza-fencer/" + *namespace, TokenFile: *runtimeAgentToken, CA: ca, ServerName: *runtimeAgentServer, Port: *runtimeAgentPort}
		executor.RuntimeAgent = client
		executor.CollectRuntimeEvidence = client.Stop
		executor.VerifyRuntimeEvidence = client.Verify
	} else if *runtimeAgentToken != "/var/run/secrets/rhiza-fencer/runtime-agent-token" || *runtimeAgentServer != "runtime-agent" || *runtimeAgentPort != "9445" {
		return fmt.Errorf("runtime-agent token, server name, and port require --runtime-agent-ca-file")
	}
	if (*stageDir == "") != (*stageIncarnation == "") {
		return fmt.Errorf("source staging requires both directory and pinned incarnation")
	}
	if *stageDir != "" {
		store, err := operator.OpenSourceStageStore(*stageDir, *stageIncarnation)
		if err != nil {
			return err
		}
		defer store.Close()
		executor.SourceStages = store
	}
	if *sourceWriterCredentials != "" {
		credentialInfo, err := os.Lstat(*sourceWriterCredentials)
		if err != nil || !credentialInfo.Mode().IsRegular() || credentialInfo.Mode()&os.ModeSymlink != 0 || credentialInfo.Mode().Perm()&0077 != 0 {
			return fmt.Errorf("source writer credentials file is missing or unsafe")
		}
		if *sourceReceiptDir != "" {
			receipts, err := operator.OpenMutationExecutionReceiptStore(*sourceReceiptDir)
			if err != nil {
				return err
			}
			defer receipts.Close()
			executor.SourceMutationReceipts = receipts
		}
		var writer *storage.Client
		if ackLossFault != nil {
			writer, err = newAckLossStorageClient(ctx, *sourceWriterCredentials, false, ackLossFault)
		} else {
			writer, err = storage.NewClient(ctx, option.WithCredentialsFile(*sourceWriterCredentials))
		}
		if err != nil {
			return fmt.Errorf("configure source writer: %w", err)
		}
		defer writer.Close()
		executor.SourceMutationStorage = writer
	} else if *sourceWriterUseADC {
		var writer *storage.Client
		if ackLossFault != nil {
			writer, err = newAckLossStorageClient(ctx, "", true, ackLossFault)
		} else {
			writer, err = storage.NewClient(ctx)
		}
		if err != nil {
			return fmt.Errorf("configure source writer from Application Default Credentials: %w", err)
		}
		defer writer.Close()
		executor.SourceMutationStorage = writer
	}
	server := &http.Server{Addr: *listen, Handler: executor.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 35 * time.Second, WriteTimeout: 35 * time.Second, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	defer server.Close()
	serving := make(chan error, 1)
	go func() { serving <- server.ListenAndServeTLS(*cert, *key) }()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-serving:
			return err
		case <-tick.C:
			if err := executor.Reconcile(ctx); err != nil {
				log.Printf("fencing reconciliation pending: %v", err)
			}
		}
	}
}

type sourceWriterConfig struct {
	Independent         bool
	AuthorityProfile    string
	HasCanonicalState   bool
	SourceProvider      objectstore.Provider
	RuntimeAgentCA      string
	StageDir            string
	StageIncarnation    string
	ReceiptDir          string
	CredentialsFile     string
	UseADC              bool
	AckLossConfig       bool
	StorageEmulatorHost string
}

func hasRemoteCanonicalState(config objectstore.Config) bool {
	return config.Provider != objectstore.ProviderFilesystem && config.Bucket != ""
}

func newAckLossStorageClient(ctx context.Context, credentialsFile string, useADC bool, fault *operator.GCSMutationAckLossFault) (*storage.Client, error) {
	const cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"
	scopes := []string{storage.ScopeFullControl, cloudPlatformScope}
	var credentials *google.Credentials
	var err error
	if useADC {
		credentials, err = google.FindDefaultCredentials(ctx, scopes...)
	} else {
		var data []byte
		data, err = os.ReadFile(credentialsFile)
		if err == nil {
			credentials, err = google.CredentialsFromJSON(ctx, data, scopes...)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("load GCS source writer credentials for acknowledgement-loss qualification: %w", err)
	}
	base := fault.WrapTransport(http.DefaultTransport)
	client := &http.Client{Transport: &oauth2.Transport{Source: credentials.TokenSource, Base: base}}
	return storage.NewClient(ctx, option.WithHTTPClient(client))
}

func legacyCooperativeControlState(independent bool, receiptDir, credentialsFile string, useADC bool) bool {
	return !independent && receiptDir == "" && credentialsFile == "" && !useADC
}

func newFencingExecutor(kube *operator.Kubernetes, authorityState *operator.AuthorityStateStore, independent bool, authorityProfile, receiptDir, credentialsFile string, useADC bool) *operator.FencingExecutor {
	profile := fencing.MutationExecutionProfile
	if independent || authorityProfile == fencing.LogicalWriterProfileV1 {
		profile = fencing.MutationHeldExecutionProfile
	}
	return &operator.FencingExecutor{
		Kube: kube, AuthorityState: authorityState,
		RequireMutationJournal:        independent || authorityProfile == fencing.LogicalWriterProfileV1,
		AuthorityProfile:              authorityProfile,
		MutationProfile:               profile,
		LegacyCooperativeControlState: legacyCooperativeControlState(independent || authorityProfile == fencing.LogicalWriterProfileV1, receiptDir, credentialsFile, useADC),
	}
}

// validateSourceWriterConfig checks the selected credential/profile mode
// without opening credentials, clients, Kubernetes, or object storage.
func validateSourceWriterConfig(c sourceWriterConfig) error {
	if c.CredentialsFile != "" && c.UseADC {
		return fmt.Errorf("source writer credentials file and Application Default Credentials are mutually exclusive")
	}
	if c.AuthorityProfile != "" && c.AuthorityProfile != fencing.LogicalWriterProfileV1 {
		return fmt.Errorf("unsupported authority profile %q", c.AuthorityProfile)
	}
	if c.Independent && c.AuthorityProfile != "" {
		return fmt.Errorf("independent mutation and logical writer authority profiles are mutually exclusive")
	}
	if c.Independent || c.AuthorityProfile != "" || c.ReceiptDir != "" || c.CredentialsFile != "" || c.UseADC {
		if c.SourceProvider != objectstore.ProviderGCS {
			return fmt.Errorf("source mutation proof profile for provider %q not implemented", c.SourceProvider)
		}
	}
	if c.AuthorityProfile == fencing.LogicalWriterProfileV1 {
		if !c.HasCanonicalState {
			return fmt.Errorf("logical writer profile requires a configured remote canonical authority store")
		}
		if c.StorageEmulatorHost != "" {
			return fmt.Errorf("logical writer profile rejects STORAGE_EMULATOR_HOST")
		}
		if (c.CredentialsFile == "") == !c.UseADC {
			return fmt.Errorf("logical writer profile requires exactly one separate source writer credential mode")
		}
		if c.ReceiptDir != "" || c.StageDir != "" || c.StageIncarnation != "" || c.AckLossConfig {
			return fmt.Errorf("logical writer profile requires canonical remote staging and receipts and does not enable acknowledgement-loss qualification")
		}
		return nil
	}
	if c.UseADC && !c.Independent {
		return fmt.Errorf("source writer Application Default Credentials require the independent mutation profile")
	}
	if c.AckLossConfig && (!c.Independent || (c.CredentialsFile == "" && !c.UseADC)) {
		return fmt.Errorf("GCS acknowledgement-loss qualification requires the independent mutation profile and its separate GCS writer credentials")
	}
	if c.Independent {
		if c.StorageEmulatorHost != "" {
			return fmt.Errorf("independent mutation profile rejects STORAGE_EMULATOR_HOST")
		}
		if !c.HasCanonicalState {
			return fmt.Errorf("independent mutation profile requires a configured remote canonical authority store; cooperative-only mode may use legacy control state but is not NoPVC-qualified")
		}
		if c.RuntimeAgentCA == "" || (c.CredentialsFile == "" && !c.UseADC) || c.StageDir != "" || c.StageIncarnation != "" || c.ReceiptDir != "" {
			return fmt.Errorf("independent mutation profile requires runtime-agent CA, exactly one separate source writer credential mode, and remote source staging and receipts (no local stage or receipt directory)")
		}
		return nil
	}
	if (c.ReceiptDir == "") != (c.CredentialsFile == "") {
		return fmt.Errorf("source mutation execution requires both a receipt directory and separate writer credentials")
	}
	if (c.ReceiptDir != "" || c.CredentialsFile != "") && !c.HasCanonicalState {
		return fmt.Errorf("source mutation execution requires canonical remote authority state; configure the independent mutation profile or disable source-mutation providers")
	}
	return nil
}
