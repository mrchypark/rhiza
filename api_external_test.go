package rhiza_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestExternalModuleCanUseEmbeddedAPI(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	repo := filepath.Dir(file)
	dir := t.TempDir()
	goMod := "module example.com/rhiza-consumer\n\ngo 1.27.0\n\nrequire github.com/mrchypark/rhiza v0.0.0\nreplace github.com/mrchypark/rhiza => " + repo + "\n"
	consumer := `package consumer

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"github.com/mrchypark/rhiza"
)

var _ http.Handler = (*rhiza.DB)(nil)
var _ http.Handler = (*rhiza.ReadReplica)(nil)
var _ = rhiza.ExecuteReturningMapOne[int64]

func TestEmbeddedLifecycle(t *testing.T) {
	_ = rhiza.Config{NodeID: "n1", DataDir: "data", Members: []rhiza.Member{{ID: "n1"}}, ObjStoreProvider: rhiza.ObjectStoreProviderFilesystem}
	_ = rhiza.NodeID("n1")
	_ = rhiza.ReplicaConfig{ReplicaID: "r1", DataDir: "replica", Members: []rhiza.ReplicaMember{{ID: "n1"}}, ObjStoreProvider: rhiza.ObjectStoreProviderS3}
	_ = rhiza.Migration{Version: 1, Name: "schema", Statements: []rhiza.SQLStatement{{SQL: "CREATE TABLE t (id INTEGER)"}}}
	_ = rhiza.RequestStatusRequest{Kind: rhiza.RequestKindSQL, RequestID: "request"}
	_ = rhiza.MutationCommitted
	_ = rhiza.RequestStateUnknownOrExpired
	_ = rhiza.HTTPErrorResponse{Code: "invalid_request", Error: "invalid request"}
	_ = rhiza.HTTPErrorCodeInvalidRequest

	ctx := context.Background()
	db, err := rhiza.Open(ctx, rhiza.Config{NodeID: "consumer", DataDir: t.TempDir()})
	if err != nil { t.Fatal(err) }
	defer db.Close()
	if err := db.Migrate(ctx, []rhiza.Migration{{Version: 1, Name: "schema", Statements: []rhiza.SQLStatement{{SQL: "CREATE TABLE t (id INTEGER PRIMARY KEY, payload BLOB NOT NULL)"}}}}); err != nil {
		t.Fatal(err)
	}
	response, id, err := rhiza.ExecuteReturningMapOne(ctx, db, rhiza.ExecuteRequest{RequestID: "insert", SQL: "INSERT INTO t(payload) VALUES (?) RETURNING id", Args: []any{[]byte{1, 2, 3}}}, func(row rhiza.SQLRow) (int64, error) {
		_, _ = row.Columns(), row.Values()
		value, err := row.Named("id")
		if err != nil { return 0, err }
		return value.(int64), nil
	})
	if err != nil || response.Status != rhiza.MutationCommitted || id != 1 {
		t.Fatalf("response=%+v id=%d err=%v", response, id, err)
	}
	result, err := db.Query(ctx, rhiza.QueryRequest{SQL: "SELECT payload FROM t WHERE id = ?", Args: []any{id}, Consistency: rhiza.ConsistencyLocal})
	if err != nil || !bytes.Equal(result.Rows[0][0].([]byte), []byte{1, 2, 3}) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
`
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(goMod), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "consumer_test.go"), []byte(consumer), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = dir
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=auto")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("external module: %v\n%s", err, output)
	}
}
