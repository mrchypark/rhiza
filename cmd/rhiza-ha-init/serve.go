package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/mrchypark/rhiza/pkg/managedbootstrap"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func managedConfig(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, podName, podUID, identityPath, handoffPath string, profile operator.V1Alpha2RuntimeProfile) (managedsql.Config, error) {
	return managedbootstrap.ManagedConfig(ctx, kube, ref, podName, podUID, identityPath, handoffPath, profile)
}

func managedRuntimeConfig(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, podUID, identityPath string, profile operator.V1Alpha2RuntimeProfile, plan operator.V1Alpha2BootstrapPlan) (managedsql.Config, error) {
	return managedbootstrap.ManagedRuntimeConfig(ctx, kube, ref, podUID, identityPath, profile, plan)
}

func serveManaged(ctx context.Context, cfg managedsql.Config, listen string) (resultErr error) {
	host, err := managedsql.Open(ctx, cfg)
	if host != nil {
		defer func() { resultErr = errors.Join(resultErr, host.Close()) }()
	}
	if err != nil {
		return err
	}
	handler, err := managedHandler(host, cfg.Runtime.AdminToken)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second}
	serveCtx, cancelServe := context.WithCancel(ctx)
	defer cancelServe()
	repairDone := make(chan struct{})
	go func() { defer close(repairDone); runManagedReplacementLoop(serveCtx, host, cfg) }()
	defer func() { cancelServe(); <-repairDone }()
	go func() {
		<-serveCtx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func runManagedReplacementLoop(ctx context.Context, host *managedsql.Host, cfg managedsql.Config) {
	managedbootstrap.RunReplacementLoop(ctx, host, cfg)
}

func checkReady(ctx context.Context, client *http.Client, endpoint, token string) error {
	if token == "" {
		return errors.New("private API authentication token required")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return errors.New("invalid readiness endpoint")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("managed host not ready")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return errors.New("managed host not ready")
	}
	return nil
}

func managedOperationUnavailable(w http.ResponseWriter, operation string, err error) {
	managedbootstrap.ManagedOperationUnavailable(w, operation, err)
}

func managedHandler(host *managedsql.Host, token string) (http.Handler, error) {
	return managedbootstrap.ManagedHandler(host, token)
}
