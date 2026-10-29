package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func main() {
	namespace := flag.String("namespace", os.Getenv("POD_NAMESPACE"), "namespace containing RhizaCluster resources")
	deleteNamespaces := flag.String("delete-namespaces", os.Getenv("RHIZA_OPERATOR_DELETE_NAMESPACES"), "legacy option; initialization does not delete Pods")
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
	if os.Getenv("RHIZA_AUTOMATIC_RECOVERY") == "true" {
		return fmt.Errorf("legacy automatic recovery is unavailable")
	}
	kube, err := operator.NewInCluster(namespace)
	if err != nil {
		return fmt.Errorf("configure Kubernetes client: %w", err)
	}
	return operator.RunV1Alpha2Initialization(ctx, kube, poll)
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
