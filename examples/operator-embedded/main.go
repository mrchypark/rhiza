// Run inside a three-voter no-PVC StatefulSet configured as documented in
// docs/embedded-operator.md. The application uses db in process; this listener
// exposes only the two operator recovery endpoints.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mrchypark/rhiza"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	config, err := rhiza.ConfigFromEnv()
	if err != nil {
		return err
	}
	db, err := rhiza.Open(ctx, config)
	if err != nil {
		return err
	}
	defer db.Close()
	server := &http.Server{Addr: config.BindAddr, Handler: db.OperatorHandler(), ReadHeaderTimeout: 5 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := server.Shutdown(shutdown)
		if err != nil {
			_ = server.Close()
		}
		return err
	}
}
