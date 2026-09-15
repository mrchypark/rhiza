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
	"strconv"
	"strings"
	"syscall"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

func main() {
	namespace := flag.String("namespace", os.Getenv("POD_NAMESPACE"), "namespace containing RhizaRecovery resources")
	poll := flag.Duration("poll-interval", 10*time.Second, "Kubernetes reconciliation poll interval")
	flag.Parse()
	if *poll <= 0 {
		log.Fatal("poll interval must be positive")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, *namespace, *poll); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func run(ctx context.Context, namespace string, poll time.Duration) error {
	kube, err := operator.NewInCluster(namespace)
	if err != nil {
		return fmt.Errorf("configure Kubernetes client: %w", err)
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
		backend := os.Getenv("RHIZA_FENCER_BACKEND")
		if backend == "kubernetes" {
			controller.Fencer = &operator.KubernetesFencer{Kube: kube}
		} else if backend == "" || backend == "http" {
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
	if anchorURL != "" || anchorTokenFile != "" {
		if anchorURL == "" || anchorTokenFile == "" {
			return fmt.Errorf("RHIZA_RECOVERY_ANCHOR_URL and RHIZA_RECOVERY_ANCHOR_TOKEN_FILE must both be set")
		}
		if err := validateAnchorEndpoint(anchorURL); err != nil {
			return err
		}
		anchorRoots, anchorErr := x509.SystemCertPool()
		if anchorErr != nil {
			return anchorErr
		}
		if caFile := os.Getenv("RHIZA_RECOVERY_ANCHOR_CA_FILE"); caFile != "" {
			data, caErr := os.ReadFile(caFile)
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
	}
	return controller.Run(ctx, poll)
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
	config := objectstore.LoadConfig()
	if directory := os.Getenv("RHIZA_OBJSTORE_DIR"); directory != "" {
		if legacy := os.Getenv("RHIZA_FILESYSTEM_DIR"); legacy != "" && legacy != directory {
			return objectstore.Config{}, fmt.Errorf("RHIZA_OBJSTORE_DIR conflicts with RHIZA_FILESYSTEM_DIR")
		}
		config.FilesystemDir = directory
	}
	if region := os.Getenv("RHIZA_OBJSTORE_REGION"); region != "" {
		config.Region = region
	}
	if raw := os.Getenv("RHIZA_OBJSTORE_INSECURE"); raw != "" {
		insecure, err := strconv.ParseBool(raw)
		if err != nil {
			return objectstore.Config{}, fmt.Errorf("invalid RHIZA_OBJSTORE_INSECURE")
		}
		config.Insecure = insecure
	}
	config.AccessKey = os.Getenv("RHIZA_OBJSTORE_ACCESS_KEY")
	config.SecretKey = os.Getenv("RHIZA_OBJSTORE_SECRET_KEY")
	config.SessionToken = os.Getenv("RHIZA_OBJSTORE_SESSION_TOKEN")
	if raw := os.Getenv("RHIZA_OBJSTORE_MAX_RETRIES"); raw != "" {
		retries, err := strconv.Atoi(raw)
		if err != nil || retries < 0 {
			return objectstore.Config{}, fmt.Errorf("invalid RHIZA_OBJSTORE_MAX_RETRIES")
		}
		config.MaxRetries = retries
	}
	return config, nil
}
