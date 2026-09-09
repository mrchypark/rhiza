package rhiza

import "net/http"

// OperatorHandler exposes only the recovery status and authenticated archive
// capture endpoints. Serve it on a private HTTP listener reachable by the
// operator; it neither opens a listener nor exposes the database HTTP API.
// Kubernetes should name this listener's container port "recovery".
func (db *DB) OperatorHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/recovery/status", db.api)
	mux.Handle("/recovery/archive", db.api)
	return mux
}
