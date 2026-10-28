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
	"strings"
	"syscall"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

func main() {
	namespace := flag.String("namespace", os.Getenv("POD_NAMESPACE"), "namespace containing RhizaRecovery resources")
	deleteNamespaces := flag.String("delete-namespaces", os.Getenv("RHIZA_OPERATOR_DELETE_NAMESPACES"), "Pod deletion namespaces: empty uses operator Pod namespace, CSV allowlist, or * for all")
	poll := flag.Duration("poll-interval", 10*time.Second, "Kubernetes reconciliation poll interval")
	flag.Parse()
	if *poll <= 0 {
		log.Fatal("poll interval must be positive")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, *namespace, *poll, *deleteNamespaces); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func run(ctx context.Context, namespace string, poll time.Duration, deleteNamespaces string) error {
	kube, err := operator.NewInCluster(namespace)
	if err != nil {
		return fmt.Errorf("configure Kubernetes client: %w", err)
	}
	if err := configureDeletion(kube, deleteNamespaces, "/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err != nil {
		return fmt.Errorf("configure Pod deletion scope: %w", err)
	}
	bucketConfig, err := operatorBucketConfig()
	if err != nil {
		return err
	}
	bucket, err := objectstore.NewBucket(bucketConfig)
	if err != nil {
		return fmt.Errorf("open object store: %w", err)
	}
	if closer, ok := bucket.Bucket.(interface{ Close() error }); ok {
		defer func() { _ = closer.Close() }()
	}
	environment := map[string]string{}
	for _, entry := range os.Environ() {
		key, value, _ := strings.Cut(entry, "=")
		environment[key] = value
	}
	controller := &operator.Controller{
		StoreIdentity: operator.StoreIdentity(environment),
		Kube:          kube,
		Bucket:        bucket,
		Prefix:        bucketConfig.Prefix,
		HTTP:          &http.Client{Timeout: 30 * time.Second},
	}

	if os.Getenv("RHIZA_AUTOMATIC_RECOVERY") == "true" {
		backend, err := fencerBackend(os.Getenv("RHIZA_FENCER_BACKEND"), os.Getenv("RHIZA_FENCER_URL"), os.Getenv("RHIZA_FENCER_TOKEN_FILE"), os.Getenv("RHIZA_FENCER_CA_FILE"))
		if err != nil {
			return err
		}
		if os.Getenv("RHIZA_FENCER_BACKEND") == "" && backend == "http" {
			log.Print("automatic recovery uses legacy implicit HTTP fencing configuration; set RHIZA_FENCER_BACKEND=http explicitly")
		}
		if backend == "kubernetes" {
			controller.Fencer = &operator.KubernetesFencer{Kube: kube}
		} else if backend == "http" {
			endpoint, tokenFile := os.Getenv("RHIZA_FENCER_URL"), os.Getenv("RHIZA_FENCER_TOKEN_FILE")
			if endpoint == "" || tokenFile == "" {
				return fmt.Errorf("automatic recovery requires RHIZA_FENCER_URL and RHIZA_FENCER_TOKEN_FILE")
			}
			roots, err := x509.SystemCertPool()
			if err != nil {
				return err
			}
			if caFile := os.Getenv("RHIZA_FENCER_CA_FILE"); caFile != "" {
				data, err := os.ReadFile(caFile)
				if err != nil {
					return fmt.Errorf("read fencing CA: %w", err)
				}
				if !roots.AppendCertsFromPEM(data) {
					return fmt.Errorf("fencing CA contains no certificates")
				}
			}
			controller.Fencer = &operator.FencingClient{URL: endpoint, TokenFile: tokenFile, Client: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}}}
		} else {
			return fmt.Errorf("unsupported RHIZA_FENCER_BACKEND")
		}
		controller.Automatic = true
	}

	// Configure recovery anchor client from environment.
	anchorURL := os.Getenv("RHIZA_RECOVERY_ANCHOR_URL")
	anchorTokenFile := os.Getenv("RHIZA_RECOVERY_ANCHOR_TOKEN_FILE")
	anchorCAFile := os.Getenv("RHIZA_RECOVERY_ANCHOR_CA_FILE")
	anchorStorageID := os.Getenv("RHIZA_RECOVERY_ANCHOR_STORAGE_ID")
	anchorPolicy := os.Getenv("RHIZA_RECOVERY_ANCHOR_BINDING_POLICY")
	anchorConfigured, anchorBinding, err := recoveryAnchorConfigured(anchorURL, anchorTokenFile, anchorCAFile, anchorPolicy, anchorStorageID, bucketConfig)
	if err != nil {
		return err
	}
	if anchorConfigured {
		if err := validateAnchorEndpoint(anchorURL); err != nil {
			return err
		}
		anchorRoots, anchorErr := x509.SystemCertPool()
		if anchorErr != nil {
			return anchorErr
		}
		if anchorCAFile != "" {
			data, caErr := os.ReadFile(anchorCAFile)
			if caErr != nil {
				return fmt.Errorf("read anchor CA: %w", caErr)
			}
			if !anchorRoots.AppendCertsFromPEM(data) {
				return fmt.Errorf("anchor CA contains no certificates")
			}
		}
		controller.Anchor = &recoveryanchor.Client{
			URL:       anchorURL,
			TokenFile: anchorTokenFile,
			Client:    &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: anchorRoots}}},
		}
		controller.AnchorBindingConfig = anchorBinding
	}
	return controller.Run(ctx, poll)
}

func recoveryAnchorConfigured(endpoint, tokenFile, caFile, policy, storageID string, store objectstore.Config) (bool, *recoveryanchor.StorageBindingConfig, error) {
	configured := endpoint != "" || tokenFile != "" || caFile != "" || policy != "" || storageID != ""
	if !configured {
		return false, nil, nil
	}
	if endpoint == "" || tokenFile == "" {
		return false, nil, fmt.Errorf("RHIZA_RECOVERY_ANCHOR_URL and RHIZA_RECOVERY_ANCHOR_TOKEN_FILE must be set when any recovery anchor setting is configured")
	}
	if err := objectstore.ValidateConfig(store); err != nil {
		return false, nil, fmt.Errorf("recovery anchor object-store configuration: %w", err)
	}
	binding := &recoveryanchor.StorageBindingConfig{Policy: recoveryanchor.StorageBindingPolicy(policy), OpaqueStorageID: storageID}
	if store.Provider != objectstore.ProviderFilesystem {
		binding.Provider, binding.Endpoint, binding.Bucket, binding.Prefix = string(store.Provider), store.Endpoint, store.Bucket, store.Prefix
	}
	if err := binding.Validate(); err != nil {
		return false, nil, fmt.Errorf("recovery anchor storage binding: %w", err)
	}
	return true, binding, nil
}

func configureDeletion(kube *operator.Kubernetes, raw, namespaceFile string) error {
	if strings.TrimSpace(raw) != "" {
		return kube.ConfigureDeletion(raw, "")
	}
	data, err := os.ReadFile(namespaceFile)
	if err != nil {
		return fmt.Errorf("operator Pod namespace is unavailable: %w", err)
	}
	own := strings.TrimSpace(string(data))
	if supplied := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); supplied != "" && supplied != own {
		return fmt.Errorf("POD_NAMESPACE differs from mounted service account namespace")
	}
	return kube.ConfigureDeletion(raw, own)
}

// Preserve the previous implicit HTTP configuration while making Kubernetes
// the default for genuinely unconfigured installations. Explicit selection wins.
func fencerBackend(backend, endpoint, tokenFile, caFile string) (string, error) {
	if backend == "" {
		if endpoint != "" || tokenFile != "" || caFile != "" {
			backend = "http"
		} else {
			return "kubernetes", nil
		}
	}
	if backend == "http" {
		if endpoint == "" || tokenFile == "" {
			return "", fmt.Errorf("HTTP fencing requires RHIZA_FENCER_URL and RHIZA_FENCER_TOKEN_FILE; set RHIZA_FENCER_BACKEND=kubernetes explicitly to ignore legacy HTTP configuration")
		}
		return backend, nil
	}
	if backend != "kubernetes" {
		return "", fmt.Errorf("unsupported RHIZA_FENCER_BACKEND")
	}
	return backend, nil
}

func validateAnchorEndpoint(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || raw == "" {
		return fmt.Errorf("anchor: invalid endpoint")
	}
	if u.Scheme != "https" {
		return fmt.Errorf("anchor: endpoint must use https")
	}
	if u.Host == "" {
		return fmt.Errorf("anchor: endpoint host is empty")
	}
	if u.User != nil {
		return fmt.Errorf("anchor: endpoint must not contain credentials")
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return fmt.Errorf("anchor: endpoint must not contain query or fragment")
	}
	return nil
}

func operatorBucketConfig() (objectstore.Config, error) {
	return objectstore.LoadOperatorConfig()
}
