package node

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestFenceStartupPartialConfigurationRejectsBeforeCreatingState(t *testing.T) {
	for _, field := range []string{"socket", "ticket"} {
		t.Run(field, func(t *testing.T) {
			t.Setenv("RHIZA_FENCE_SOCKET", "")
			t.Setenv("RHIZA_FENCE_TICKET", "")
			if field == "socket" {
				t.Setenv("RHIZA_FENCE_SOCKET", "/does/not/exist")
			} else {
				t.Setenv("RHIZA_FENCE_TICKET", "test-ticket")
			}
			dir := filepath.Join(t.TempDir(), "unopened")
			n := New(&types.ExecutionConfig{ClusterID: "cluster", NodeID: "node", DataDir: dir})
			if err := n.Open(context.Background()); err == nil {
				t.Fatal("partial supervision accepted")
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatalf("startup gate created state: %v", err)
			}
		})
	}
}

func TestFenceStartupRejectsLegacyAndRolelessAssignedResponses(t *testing.T) {
	physical := "rhiza-2"
	assigned := fencing.Identity{
		SourceClusterID: "cluster",
		NodeID:          "logical-incarnation",
		PodName:         physical,
		PodUID:          "pod-uid",
		RunID:           "run-id",
		AssignmentID:    strings.Repeat("a", 64),
	}
	assignedGrant := fencing.StartupGrant{
		Identity: assigned,
		Environment: map[string]string{
			"RHIZA_NODE_ID":     assigned.NodeID,
			"RHIZA_LEARNER":     `{"id":"logical-incarnation"}`,
			"RHIZA_PEER_TOKEN":  "private-token",
			"RHIZA_PEER_TOKENS": "",
		},
	}
	legacy, err := json.Marshal(assigned)
	if err != nil {
		t.Fatal(err)
	}
	roleless, err := json.Marshal(assignedGrant)
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{"legacy identity": legacy, "assigned envelope without role": roleless} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("RHIZA_POD_NAME", physical)
			// Keep the AF_UNIX path below macOS sockaddr_un limits; t.TempDir
			// includes this long test and subtest name in its path.
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
			server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/startup" {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(body)
			})}
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(func() { _ = server.Close(); _ = listener.Close() })
			if _, err := fencing.Connect(t.Context(), socket, "ticket", assigned.SourceClusterID, assigned.NodeID); err == nil {
				t.Fatal("legacy or role-less assigned startup response was accepted")
			}
		})
	}
}

func TestFenceWatcherRetriesOnlyPendingAndTransportUncertainty(t *testing.T) {
	for _, tc := range []struct {
		err   error
		retry bool
	}{
		{fencing.ErrPending, true},
		{fencing.ErrUnavailable, true},
		{&url.Error{Op: "Post", URL: "http://supervisor/v1/stop", Err: errors.New("connection refused")}, true},
		{context.DeadlineExceeded, true},
		{fencing.ErrClosed, false},
		{errors.New("fencing: supervisor returned HTTP 403"), false},
		{errors.New("fencing: stop request identity mismatch"), false},
		{nil, false},
	} {
		if got := retryFenceSession(tc.err); got != tc.retry {
			t.Fatalf("error=%v retry=%t want=%t", tc.err, got, tc.retry)
		}
	}
}

func TestFenceWatcherKeepsEngineAliveAcrossActualSessionOutage(t *testing.T) {
	for _, fault := range []string{"503", "connection"} {
		t.Run(fault, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			socket := filepath.Join(t.TempDir(), "session.sock")
			identity := fencing.Identity{SourceClusterID: "cluster", NodeID: "node", PodUID: "pod", RunID: "run"}
			var stopCalls atomic.Int32
			var unavailable atomic.Bool
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/startup":
					_ = json.NewEncoder(w).Encode(fencing.StartupGrant{Identity: identity, Role: fencing.StartupRoleVoterActive})
				case "/v1/bind":
					var bound fencing.Identity
					if err := json.NewDecoder(r.Body).Decode(&bound); err != nil {
						http.Error(w, "invalid identity", http.StatusBadRequest)
						return
					}
					want := identity
					want.WALIdentity = "wal"
					if bound != want {
						http.Error(w, "binding differs", http.StatusForbidden)
						return
					}
					_ = json.NewEncoder(w).Encode(fencing.BindReceipt{Identity: bound})
				case "/v1/stop":
					stopCalls.Add(1)
					if unavailable.Load() {
						w.WriteHeader(http.StatusServiceUnavailable)
					} else {
						w.WriteHeader(http.StatusAccepted)
					}
				default:
					http.NotFound(w, r)
				}
			})
			start := func() *http.Server {
				l, err := net.Listen("unix", socket)
				if err != nil {
					t.Fatal(err)
				}
				server := &http.Server{Handler: handler}
				go func() { _ = server.Serve(l) }()
				t.Cleanup(func() { _ = server.Close(); _ = l.Close() })
				return server
			}
			server := start()
			session, err := fencing.Connect(ctx, socket, "ticket", "cluster", "node")
			if err != nil {
				t.Fatal(err)
			}
			if err = session.BindWAL(ctx, "wal"); err != nil {
				t.Fatal(err)
			}
			if _, enabled := session.MutationBinding(); enabled {
				t.Fatal("cooperative fixture enabled mutation client")
			}
			if fault == "503" {
				unavailable.Store(true)
			} else {
				_ = server.Close()
			}
			if _, err = session.WaitStop(ctx); !errors.Is(err, fencing.ErrUnavailable) {
				t.Fatalf("actual session outage=%v", err)
			}
			n := &Node{fenceSession: session}
			n.opened.Store(true)
			n.startFenceWatcher(ctx)
			if fault == "connection" {
				time.Sleep(100 * time.Millisecond)
				server = start()
				_ = server
			} else {
				for stopCalls.Load() < 3 {
					select {
					case <-ctx.Done():
						t.Fatal("watcher did not retry 503")
					case <-time.After(10 * time.Millisecond):
					}
				}
				unavailable.Store(false)
			}
			before := stopCalls.Load()
			for stopCalls.Load() <= before {
				select {
				case <-ctx.Done():
					t.Fatal("watcher did not recover session polling")
				case <-time.After(10 * time.Millisecond):
				}
			}
			if !n.opened.Load() {
				t.Fatal("transient supervisor outage stopped the engine")
			}
		})
	}
}
