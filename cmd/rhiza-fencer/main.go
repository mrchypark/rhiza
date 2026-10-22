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

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
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
	executor := &operator.FencingExecutor{Kube: kube}
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
