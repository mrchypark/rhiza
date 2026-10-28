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
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
	thanosobjstore "github.com/thanos-io/objstore"
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
	s := &fencing.Supervisor{Authority: &fencing.AuthorityClient{URL: *endpoint, TokenFile: *token, Client: client}, SocketPath: *socket, Command: f.Args(), Identity: fencing.Identity{SourceClusterID: os.Getenv("RHIZA_CLUSTER_ID"), NodeID: os.Getenv("RHIZA_NODE_ID"), PodName: os.Getenv("POD_NAME"), PodUID: os.Getenv("POD_UID")}}
	return s.Run(ctx)
}

func validateAnchorEndpoint(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || raw == "" || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return fmt.Errorf("anchor: endpoint must be an HTTPS URL without credentials, query, or fragment")
	}
	return nil
}

func serve(ctx context.Context, args []string) error {
	f := flag.NewFlagSet("serve", flag.ContinueOnError)
	namespace := f.String("namespace", os.Getenv("POD_NAMESPACE"), "single managed namespace")
	listen := f.String("listen", ":9444", "TLS listener")
	cert := f.String("tls-cert", os.Getenv("FENCER_TLS_CERT"), "TLS certificate file")
	key := f.String("tls-key", os.Getenv("FENCER_TLS_KEY"), "TLS private key file")
	attestorURL := f.String("attestor-url", os.Getenv("RHIZA_FENCE_ATTESTOR_URL"), "optional external independent-evidence endpoint")
	attestorToken := f.String("attestor-token-file", os.Getenv("RHIZA_FENCE_ATTESTOR_TOKEN_FILE"), "attestor audience-bound projected token")
	attestorCA := f.String("attestor-ca-file", os.Getenv("RHIZA_FENCE_ATTESTOR_CA_FILE"), "attestor TLS CA bundle")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *cert == "" || *key == "" {
		return fmt.Errorf("fencing authority requires TLS certificate and key")
	}
	kube, err := operator.NewInCluster(*namespace)
	if err != nil {
		return err
	}
	var bucket thanosobjstore.Bucket
	var bucketConfig objectstore.Config
	if config, configErr := objectstore.LoadOperatorConfig(); configErr != nil {
		log.Printf("peer-closure durable binding reads disabled: invalid object-store config: %v", configErr)
	} else {
		bucketConfig = config
		opened, openErr := openPeerClosureBucket(ctx, config)
		if openErr != nil {
			log.Printf("peer-closure durable binding reads disabled: object-store unavailable: %v", openErr)
		} else {
			bucket = opened
		}
	}
	if closer, ok := bucket.(interface{ Close() error }); ok {
		defer func() { _ = closer.Close() }()
	}
	executor := &operator.FencingExecutor{
		Kube: kube, Bucket: bucket, Prefix: bucketConfig.Prefix,
		StoreIdentity: operator.StoreIdentity(map[string]string{
			"RHIZA_OBJSTORE_PROVIDER":                string(bucketConfig.Provider),
			"RHIZA_OBJSTORE_ENDPOINT":                bucketConfig.Endpoint,
			"RHIZA_OBJSTORE_BUCKET":                  bucketConfig.Bucket,
			"RHIZA_OBJSTORE_PREFIX":                  bucketConfig.Prefix,
			"RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT":   bucketConfig.AzureStorageAccount,
			"RHIZA_OBJSTORE_AZURE_CONNECTION_STRING": bucketConfig.AzureConnectionString,
		}),
	}
	anchorURL, anchorTokenFile := os.Getenv("RHIZA_RECOVERY_ANCHOR_URL"), os.Getenv("RHIZA_RECOVERY_ANCHOR_TOKEN_FILE")
	anchorCAFile, anchorStorageID := os.Getenv("RHIZA_RECOVERY_ANCHOR_CA_FILE"), os.Getenv("RHIZA_RECOVERY_ANCHOR_STORAGE_ID")
	if anchorURL != "" || anchorTokenFile != "" || anchorCAFile != "" || anchorStorageID != "" {
		if anchorURL == "" || anchorTokenFile == "" || anchorStorageID == "" {
			return fmt.Errorf("DR admission requires RHIZA_RECOVERY_ANCHOR_URL, RHIZA_RECOVERY_ANCHOR_TOKEN_FILE, and explicit RHIZA_RECOVERY_ANCHOR_STORAGE_ID")
		}
		if err := validateAnchorEndpoint(anchorURL); err != nil {
			return err
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			return err
		}
		if anchorCAFile != "" {
			data, err := os.ReadFile(anchorCAFile)
			if err != nil {
				return fmt.Errorf("read recovery anchor CA: %w", err)
			}
			if !roots.AppendCertsFromPEM(data) {
				return fmt.Errorf("recovery anchor CA contains no certificate")
			}
		}
		executor.Anchor = &recoveryanchor.Client{URL: anchorURL, TokenFile: anchorTokenFile, Client: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}}}
		executor.AnchorStorageID = anchorStorageID
	}
	// Independent evidence is optional and off unless the administrator configures
	// the whole trust relationship. A partial configuration fails startup.
	if *attestorURL != "" || *attestorToken != "" || *attestorCA != "" {
		attestor, err := operator.NewHTTPSAttestor(*attestorURL, *attestorToken, *attestorCA)
		if err != nil {
			return err
		}
		executor.Attestor = attestor
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

func openPeerClosureBucket(ctx context.Context, config objectstore.Config) (thanosobjstore.Bucket, error) {
	if config.Provider == objectstore.ProviderFilesystem {
		return nil, nil
	}
	bucket, err := objectstore.NewBucketForSupervisedStartup(ctx, config)
	if err != nil {
		return nil, err
	}
	return bucket, nil
}
