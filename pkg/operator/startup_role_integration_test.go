package operator

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestNodeOpenWaitsForAssignedLearnerStartupAuthority(t *testing.T) {
	physical, logical := "rhiza-2", "recovery-voter-7c41"
	requestSeen := false
	socket := filepath.Join(t.TempDir(), "startup.sock")
	startStartupAuthoritySocket(t, socket, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/startup" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		var request fencing.Identity
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode startup request: %v", err)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		requestSeen = true
		if request.SourceClusterID != "source-cluster" || request.NodeID != physical || request.PodName != physical {
			t.Errorf("startup request claimed nonphysical identity: %+v", request)
		}
		// Authority uncertainty is explicitly pending; Node.Open must not
		// create a WAL or perform startup/catch-up work before assignment.
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Setenv("RHIZA_FENCE_SOCKET", socket)
	t.Setenv("RHIZA_FENCE_TICKET", "local-ticket")
	t.Setenv("RHIZA_POD_NAME", physical)

	dataDir := filepath.Join(t.TempDir(), "node-state")
	config := assignedLearnerExecutionConfig(dataDir, logical)
	err := node.New(config).Open(context.Background())
	if !errors.Is(err, fencing.ErrPending) {
		t.Fatalf("Node.Open error=%v, want pending authority", err)
	}
	if !requestSeen {
		t.Fatal("Node.Open did not request an authenticated startup assignment")
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("pending startup created node state: %v", err)
	}
}

func TestNodeOpenBindsAssignedLearnerWALBeforeWaitingForIdentityWrite(t *testing.T) {
	physical, logical := "rhiza-2", "recovery-voter-7c41"
	assignment := strings.Repeat("a", 64)
	grant := fencing.StartupGrant{
		Identity: fencing.Identity{
			SourceClusterID: "source-cluster", NodeID: logical, PodName: physical,
			PodUID: "replacement-pod-uid", RunID: "run-9", AssignmentID: assignment,
		},
		Role: fencing.StartupRoleLearnerPendingRemove,
		Environment: map[string]string{
			"RHIZA_NODE_ID":    logical,
			"RHIZA_LEARNER":    `{"ID":"recovery-voter-7c41","PublicKey":"AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}`,
			"RHIZA_PEER_TOKEN": "fresh-assigned-peer-token",
		},
	}
	if err := grant.Validate(fencing.Identity{SourceClusterID: grant.Identity.SourceClusterID, NodeID: physical, PodName: physical, PodUID: grant.Identity.PodUID, RunID: grant.Identity.RunID}); err != nil {
		t.Fatalf("test grant invalid: %v", err)
	}
	var bindSeen bool
	socket := filepath.Join(t.TempDir(), "startup.sock")
	startStartupAuthoritySocket(t, socket, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		switch r.URL.Path {
		case "/v1/startup":
			_ = json.NewEncoder(w).Encode(grant)
		case "/v1/bind":
			var bound fencing.Identity
			if err := json.NewDecoder(r.Body).Decode(&bound); err != nil {
				t.Errorf("decode WAL bind: %v", err)
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			bindSeen = true
			if bound.SourceClusterID != grant.Identity.SourceClusterID || bound.NodeID != logical || bound.PodName != physical ||
				bound.PodUID != grant.Identity.PodUID || bound.RunID != grant.Identity.RunID || bound.AssignmentID != assignment || bound.WALIdentity == "" {
				t.Errorf("WAL bind lost authority-assigned identity: %+v", bound)
			}
			// Keep the authority pending at the durable-bind boundary. No
			// voter-identity registration or catch-up may proceed from this
			// unresolved response.
			w.WriteHeader(http.StatusAccepted)
		case "/v1/learner-identity":
			t.Error("learner identity write ran before the WAL bind was authorized")
			http.Error(w, "unexpected identity write", http.StatusForbidden)
		default:
			http.NotFound(w, r)
		}
	}))
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected pre-bind object-store request %s %s", r.Method, r.URL.Path)
			http.Error(w, "writes are not expected before bind", http.StatusForbidden)
			return
		}
		if r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<ListBucketResult><Name>archive</Name><Prefix></Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>`))
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code><Message>not found</Message></Error>`))
	}))
	defer store.Close()
	t.Setenv("RHIZA_FENCE_SOCKET", socket)
	t.Setenv("RHIZA_FENCE_TICKET", "local-ticket")
	t.Setenv("RHIZA_POD_NAME", physical)

	// This config is the bounded child configuration represented by the
	// authority grant. Node.Open is exercised directly, not a role helper.
	config := assignedLearnerExecutionConfig(filepath.Join(t.TempDir(), "node-state"), logical)
	config.ObjStoreEndpoint = strings.TrimPrefix(store.URL, "http://")
	config.PeerToken = grant.Environment["RHIZA_PEER_TOKEN"]
	config.Learner = &quepaxa.Member{ID: quepaxa.NodeID(logical), PublicKey: quepaxa.PublicKey{1}}
	err := node.New(config).Open(context.Background())
	if !errors.Is(err, fencing.ErrPending) {
		t.Fatalf("Node.Open error=%v, want pending WAL bind", err)
	}
	if !bindSeen {
		t.Fatal("Node.Open did not bind the WAL under its assigned logical identity")
	}
}

func assignedLearnerExecutionConfig(dataDir, logical string) *types.ExecutionConfig {
	return &types.ExecutionConfig{
		DataDir: dataDir, ClusterID: "source-cluster", NodeID: types.NodeID(logical),
		PeerToken: "fresh-assigned-peer-token", AdminToken: "distinct-admin-token",
		Members: []quepaxa.Member{
			{ID: "source-a", PublicKey: quepaxa.PublicKey{1}},
			{ID: "source-b", PublicKey: quepaxa.PublicKey{2}},
		},
		EnableReconfiguration: true,
		Learner:               &quepaxa.Member{ID: quepaxa.NodeID(logical), PublicKey: quepaxa.PublicKey{3}},
		ObjStoreProvider:      "s3", ObjStoreEndpoint: "http://127.0.0.1:1", ObjStoreBucket: "archive",
		ObjStoreRegion: "us-east-1", ObjStoreInsecure: true,
	}
}

func startStartupAuthoritySocket(t *testing.T, socket string, handler http.Handler) {
	t.Helper()
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.Start()
	t.Cleanup(func() {
		server.Close()
		_ = listener.Close()
	})
}
