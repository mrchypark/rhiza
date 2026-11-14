// Command example demonstrates embedding the managed SQL host in an application.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mrchypark/rhiza/pkg/managedbootstrap"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func run(ctx context.Context) (result error) {
	ref := operator.V1Alpha2ClusterRef{Name: os.Getenv("RHIZA_CLUSTER_NAME"), Namespace: os.Getenv("POD_NAMESPACE"), UID: os.Getenv("RHIZA_CLUSTER_UID")}
	file, err := os.Open(operator.V1Alpha2ManagedRuntimeProfilePath)
	if err != nil {
		return err
	}
	profile, err := operator.DecodeV1Alpha2RuntimeProfile(file)
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	kube, err := operator.NewInCluster(ref.Namespace)
	if err != nil {
		return err
	}
	var appReady atomic.Bool
	host, err := managedbootstrap.OpenEmbedded(ctx, managedbootstrap.ConfigRequest{
		Kube: kube, Ref: ref, PodName: os.Getenv("POD_NAME"), PodUID: os.Getenv("POD_UID"),
		IdentityPath: operator.V1Alpha2BootstrapIdentityPath, BootstrapPath: operator.V1Alpha2BootstrapHandoffPath,
		RepairPath: managedbootstrap.RepairHandoffPath, Profile: profile,
	}, func(context.Context) error {
		if !appReady.Load() {
			return errors.New("application not ready")
		}
		return nil
	})
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, host.Close()) }()
	app := http.NewServeMux()
	app.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		result, err := host.Host.Query(r.Context(), managedsql.QueryRequest{SQL: "SELECT 1", Consistency: "linearizable"})
		if err != nil {
			http.Error(w, "query unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})
	control := &http.Server{Addr: ":8080", Handler: host.Control, ReadHeaderTimeout: 5 * time.Second}
	application := &http.Server{Addr: ":8081", Handler: app, ReadHeaderTimeout: 5 * time.Second}
	done := make(chan error, 2)
	go func() { done <- control.ListenAndServe() }()
	go func() { done <- application.ListenAndServe() }()
	appReady.Store(true)
	select {
	case <-ctx.Done():
		err = nil
	case err = <-done:
	}
	appReady.Store(false)
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = errors.Join(err, application.Shutdown(shutdown), control.Shutdown(shutdown))
	return err
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
