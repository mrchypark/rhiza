package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mrchypark/rhiza/pkg/operator"
)

func main() {
	namespace := flag.String("namespace", os.Getenv("POD_NAMESPACE"), "namespace containing RhizaCluster resources")
	deleteNamespaces := flag.String("delete-namespaces", os.Getenv("RHIZA_OPERATOR_DELETE_NAMESPACES"), "legacy option; initialization does not delete Pods")
	poll := flag.Duration("poll-interval", 10*time.Second, "Kubernetes reconciliation poll interval")
	profile := flag.String("managed-runtime-profile", os.Getenv("RHIZA_MANAGED_RUNTIME_PROFILE"), "private runtime.json file selecting the managed SQL reference host")
	flag.Parse()
	if *poll <= 0 {
		log.Fatal("poll interval must be positive")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, *namespace, *poll, *deleteNamespaces, *profile); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func run(ctx context.Context, namespace string, poll time.Duration, deleteNamespaces string, profilePaths ...string) error {
	if os.Getenv("RHIZA_AUTOMATIC_RECOVERY") == "true" {
		return fmt.Errorf("legacy automatic recovery is unavailable")
	}
	profilePath := os.Getenv("RHIZA_MANAGED_RUNTIME_PROFILE")
	if len(profilePaths) > 1 {
		return fmt.Errorf("multiple managed runtime profiles")
	}
	if len(profilePaths) == 1 {
		profilePath = profilePaths[0]
	}
	var profile operator.V1Alpha2RuntimeProfile
	if profilePath != "" {
		var err error
		profile, err = readManagedRuntimeProfile(profilePath)
		if err != nil {
			return err
		}
	}
	kube, err := operator.NewInCluster(namespace)
	if err != nil {
		return fmt.Errorf("configure Kubernetes client: %w", err)
	}
	if profilePath != "" {
		return operator.RunV1Alpha2ManagedInitialization(ctx, kube, poll, profile)
	}
	return operator.RunV1Alpha2Initialization(ctx, kube, poll)
}

func readManagedRuntimeProfile(path string) (operator.V1Alpha2RuntimeProfile, error) {
	f, err := os.Open(path)
	if err != nil {
		return operator.V1Alpha2RuntimeProfile{}, fmt.Errorf("cannot open private managed runtime profile")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0027 != 0 {
		return operator.V1Alpha2RuntimeProfile{}, fmt.Errorf("managed runtime profile must be a private regular file")
	}
	return operator.DecodeV1Alpha2RuntimeProfile(f)
}
