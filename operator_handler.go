package rhiza

import "net/http"

// OperatorHandler exposes recovery status and authenticated archive and
// membership management endpoints. Serve it on a private HTTP listener reachable by the
// operator; it neither opens a listener nor exposes the database HTTP API.
// Kubernetes should name this listener's container port "recovery".
func (db *DB) OperatorHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/recovery/status", db.api)
	mux.Handle("/recovery/archive", db.api)
	mux.Handle("/membership/status", db.api)
	mux.Handle("/membership/change", db.api)
	mux.Handle("/membership/abort", db.api)
	return mux
}
