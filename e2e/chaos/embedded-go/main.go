// Test-only application: SQL uses the embedded API, never db.Handler().
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/mrchypark/rhiza"
)

func main() {
	config, err := rhiza.ConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	db, err := rhiza.Open(context.Background(), config)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	management := &http.Server{Addr: config.BindAddr, Handler: db.OperatorHandler(), ReadHeaderTimeout: 5 * time.Second}
	go func() { log.Fatal(management.ListenAndServe()) }()
	app := http.NewServeMux()
	app.HandleFunc("GET /host", func(w http.ResponseWriter, r *http.Request) {
		respond(w, map[string]string{"host": "go-embedded"}, nil)
	})
	app.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, map[string]bool{"alive": true}, nil) })
	app.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if !db.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		respond(w, map[string]bool{"ready": true}, nil)
	})
	app.HandleFunc("POST /sql/execute", func(w http.ResponseWriter, r *http.Request) {
		var req rhiza.ExecuteRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536)).Decode(&req); err != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		result, err := db.Execute(r.Context(), req)
		respond(w, result, err)
	})
	app.HandleFunc("POST /sql/query", func(w http.ResponseWriter, r *http.Request) {
		var req rhiza.QueryRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536)).Decode(&req); err != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		result, err := db.Query(r.Context(), req)
		respond(w, result, err)
	})
	server := &http.Server{Addr: ":8080", Handler: app, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}

func respond(w http.ResponseWriter, value any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		value = map[string]string{"error_code": "operation_failed"}
	}
	_ = json.NewEncoder(w).Encode(value)
}
