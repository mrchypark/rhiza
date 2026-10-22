package node

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestIndependentBindingCannotFallThroughToDirectBootstrapWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	// Short owned directory avoids macOS sockaddr_un path-length truncation.
	socketDir, err := os.MkdirTemp("", "rhiza-bind-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(socketDir); err != nil {
			t.Error(err)
		}
	})
	socket := filepath.Join(socketDir, "s")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	id := fencing.Identity{SourceClusterID: "cluster", NodeID: "a", PodUID: "pod", RunID: "run"}
	var binds atomic.Int32
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/startup":
			_ = json.NewEncoder(w).Encode(id)
		case "/v1/bind":
			var bound fencing.Identity
			if err := json.NewDecoder(r.Body).Decode(&bound); err != nil {
				http.Error(w, "bad identity", http.StatusBadRequest)
				return
			}
			want := bound
			want.WALIdentity = ""
			if want != id || bound.WALIdentity == "" {
				http.Error(w, "identity mismatch", http.StatusForbidden)
				return
			}
			binds.Add(1)
			runtime := fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "namespace", BindingUID: "binding", StatefulSetUID: "sts",
				SourceClusterID: id.SourceClusterID, NodeID: id.NodeID, PodUID: id.PodUID, RunID: id.RunID,
				NodeName: "node", NodeUID: "node-uid", MachineID: "machine", BootID: "boot", RuntimeID: "containerd",
				SandboxID: "sandbox", ContainerID: "container", ImageID: "image"}
			_ = json.NewEncoder(w).Encode(fencing.BindReceipt{Identity: bound, Producer: &fencing.ProducerBinding{
				ContractVersion: fencing.MutationIntentVersion, Runtime: runtime, ProducerUID: "producer",
				StoreScopeHash: strings.Repeat("a", 64), ExecutionProfile: fencing.MutationExecutionProfile, StageIncarnation: strings.Repeat("b", 64)}})
		default:
			http.NotFound(w, r)
		}
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close(); _ = listener.Close() })
	t.Setenv("RHIZA_FENCE_SOCKET", socket)
	t.Setenv("RHIZA_FENCE_TICKET", "test-ticket")
	config := voterTestConfig(t)
	local, err := loadVoterIdentity(config)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(filepath.Join(config.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := wal.Close(); err != nil {
			t.Error(err)
		}
	})
	n := New(config)
	n.fenceSession, err = fencing.Connect(ctx, socket, "test-ticket", "cluster", "a")
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := objectstore.NewBucket(objectstore.Config{Provider: objectstore.ProviderFilesystem, FilesystemDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	n.protectedBucket = &fencedBucket{MeteredBucket: bucket, prefix: "cluster"}
	defer n.protectedBucket.Close()
	err = ensureVoterIdentity(ctx, config, n.protectedBucket, wal, local, false, n.bindFenceWAL)
	if err == nil || !strings.Contains(err.Error(), "supervisor source HTTP") || binds.Load() != 1 {
		t.Fatalf("independent binding bypassed client gate: %v binds=%d", err, binds.Load())
	}
	if n.fenceSession == nil {
		t.Fatal("did not reach real source binding")
	}
	if _, valid := n.fenceSession.MutationBinding(); !valid {
		t.Fatal("test refused an invalid receipt instead of direct-write fallthrough")
	}
	if err := n.protectedBucket.Iter(ctx, "", func(key string) error {
		t.Errorf("bootstrap wrote provider object before durable client: %s", key)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
