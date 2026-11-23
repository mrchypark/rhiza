package client

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCommandHTTPServiceFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	bin := filepath.Join(t.TempDir(), "rhiza")
	build := exec.CommandContext(ctx, "go", "build", "-o", bin, "../cmd/rhiza")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build rhiza: %v: %s", err, out)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	cmd := exec.CommandContext(ctx, bin)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "RHIZA_") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "RHIZA_NODE_ID=client-integration", "RHIZA_DATA_DIR="+t.TempDir(), "RHIZA_BIND_ADDR="+addr, "RHIZA_OBJSTORE_PROVIDER=filesystem", "RHIZA_OBJSTORE_DIR="+t.TempDir())
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Signal(syscall.SIGTERM); _ = cmd.Wait() })
	endpoint := "http://" + addr
	for {
		resp, err := http.Get(endpoint + "/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("rhiza readiness timeout")
		case <-time.After(50 * time.Millisecond):
		}
	}
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer dead.Close()
	c, err := New([]string{dead.URL, endpoint}, nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Read(ctx, "/sql/query", []byte(`{"sql":"SELECT 1","consistency":"local"}`))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("read: %+v %v", resp, err)
	}
	resp, err = c.Mutate(ctx, "/sql/execute", []byte(`{"request_id":"client-integration","sql":"CREATE TABLE example (id INTEGER)"}`))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("mutation: %+v %v", resp, err)
	}
}
