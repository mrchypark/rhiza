package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
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
	return controller.Run(ctx, poll)
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
