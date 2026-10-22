// rhiza-fencer runs either the cooperative authority or a supervised application.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
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
	s := &fencing.Supervisor{Authority: &fencing.AuthorityClient{URL: *endpoint, TokenFile: *token, Client: client}, SocketPath: *socket, Command: f.Args(), Identity: fencing.Identity{SourceClusterID: os.Getenv("RHIZA_CLUSTER_ID"), NodeID: os.Getenv("RHIZA_NODE_ID"), PodUID: os.Getenv("POD_UID")}}
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
	runtimeAgentCA := f.String("runtime-agent-ca-file", os.Getenv("FENCER_RUNTIME_AGENT_CA_FILE"), "dedicated node runtime-agent TLS CA; enables runtime enrollment and terminal evidence")
	runtimeAgentToken := f.String("runtime-agent-token-file", "/var/run/secrets/rhiza-fencer/runtime-agent-token", "TokenReview-audience-bound runtime-agent credential")
	runtimeAgentServer := f.String("runtime-agent-server-name", "runtime-agent", "TLS DNS identity on node runtime-agent certificates")
	runtimeAgentPort := f.String("runtime-agent-port", "9445", "node runtime-agent HTTPS host port")
	authorityStatePrefix := f.String("authority-state-prefix", os.Getenv("FENCER_AUTHORITY_STATE_PREFIX"), "canonical authority state prefix in the configured GCS bucket")
	independentMutation := f.Bool("independent-mutation-profile", os.Getenv("FENCER_INDEPENDENT_MUTATION_PROFILE") == "true", "require canonical remote state and source-mutation providers for the independent NoPVC mutation profile")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *attestorURL != "" || *attestorToken != "" || *attestorCA != "" {
		return fmt.Errorf("legacy v1 quiet-window attestors cannot authorize recovery; independent healing requires enrolled runtime and mutation evidence v2 providers, which this command does not yet configure")
	}
	if *cert == "" || *key == "" {
		return fmt.Errorf("fencing authority requires TLS certificate and key")
	}
	kube, err := operator.NewInCluster(*namespace)
	if err != nil {
		return err
	}
	stateConfig := objectstore.LoadConfig()
	var authorityState *operator.AuthorityStateStore
	if stateConfig.Provider == objectstore.ProviderGCS && stateConfig.Bucket != "" {
		stateBucket, err := objectstore.NewBucket(stateConfig)
		if err != nil {
			return fmt.Errorf("configure canonical authority ObjectStorage: %w", err)
		}
		defer stateBucket.Close()
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
		if authorityState == nil {
			return fmt.Errorf("independent mutation profile requires RHIZA_OBJSTORE_PROVIDER=gcs and a bucket; cooperative-only mode may use legacy control state but is not NoPVC-qualified")
		}
		if *runtimeAgentCA == "" || *sourceWriterCredentials == "" || *stageDir != "" || *stageIncarnation != "" || *sourceReceiptDir != "" {
			return fmt.Errorf("independent mutation profile requires runtime-agent CA, separate source writer credentials, and remote source staging and receipts (no local stage or receipt directory)")
		}
	} else if (*sourceReceiptDir != "" || *sourceWriterCredentials != "") && authorityState == nil {
		return fmt.Errorf("source mutation execution requires canonical GCS authority state; configure the independent mutation profile or disable source-mutation providers")
	}
	executor := &operator.FencingExecutor{Kube: kube, AuthorityState: authorityState}
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
	if !*independentMutation && (*sourceReceiptDir == "") != (*sourceWriterCredentials == "") {
		return fmt.Errorf("source mutation execution requires both a receipt directory and separate writer credentials")
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
		writer, err := storage.NewClient(ctx, option.WithCredentialsFile(*sourceWriterCredentials))
		if err != nil {
			return fmt.Errorf("configure source writer: %w", err)
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
