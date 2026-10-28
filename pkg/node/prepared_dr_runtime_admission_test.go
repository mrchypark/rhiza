package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
	"github.com/thanos-io/objstore/providers/azure"
)

func TestPreparedDRRuntimeAdmissionRevocationPrecedesLocalIdentityMutation(t *testing.T) {
	ctx := context.Background()
	var storeReads, storeMutations atomic.Int32
	store := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			storeMutations.Add(1)
			http.Error(w, "unexpected storage mutation", http.StatusForbidden)
			return
		}
		storeReads.Add(1)
		writeAzurePrebindRead(w, r)
	}))
	defer store.Close()
	config, assignment := managedDRRuntimeAdmissionFixture(t, store.URL)
	writePreparedDRRuntimeMarker(t, &config, assignment)
	supervisor, socket, counts := startDRRuntimeAdmissionSupervisor(t, runtimeIdentityForAssignment(assignment), assignment, http.StatusGone)
	defer supervisor()
	t.Setenv("RHIZA_FENCE_SOCKET", socket)
	t.Setenv("RHIZA_FENCE_TICKET", "test-ticket")
	configureAzureStartupTransport(t, store)

	n := New(&config)
	err := n.Open(ctx)
	if !errors.Is(err, fencing.ErrClosed) {
		t.Fatalf("revoked runtime admission error=%v, want ErrClosed", err)
	}
	if counts.runtimeAdmission.Load() != 1 || counts.bind.Load() != 0 || storeReads.Load() != 0 || storeMutations.Load() != 0 {
		t.Fatalf("revocation ordering: admission=%d bind=%d storageReads=%d storageMutations=%d", counts.runtimeAdmission.Load(), counts.bind.Load(), storeReads.Load(), storeMutations.Load())
	}
	assertNoRuntimeVoterIdentity(t, config.DataDir)
}

func TestPreparedDRRuntimeAdmissionSuccessPrecedesWALBind(t *testing.T) {
	ctx := context.Background()
	var storeReads, storeMutations atomic.Int32
	store := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			storeMutations.Add(1)
			http.Error(w, "injected post-admission registration refusal", http.StatusForbidden)
			return
		}
		storeReads.Add(1)
		writeAzurePrebindRead(w, r)
	}))
	defer store.Close()
	config, assignment := managedDRRuntimeAdmissionFixture(t, store.URL)
	writePreparedDRRuntimeMarker(t, &config, assignment)
	supervisor, socket, counts := startDRRuntimeAdmissionSupervisor(t, runtimeIdentityForAssignment(assignment), assignment, http.StatusOK)
	defer supervisor()
	t.Setenv("RHIZA_FENCE_SOCKET", socket)
	t.Setenv("RHIZA_FENCE_TICKET", "test-ticket")
	configureAzureStartupTransport(t, store)

	n := New(&config)
	err := n.Open(ctx)
	if err == nil {
		t.Fatal("expected injected object-store registration refusal after runtime admission and WAL bind")
	}
	if counts.runtimeAdmission.Load() != 1 || counts.bind.Load() != 1 || storeReads.Load() == 0 || storeMutations.Load() == 0 {
		t.Fatalf("successful admission did not precede real WAL handoff: admission=%d bind=%d storageReads=%d storageMutations=%d err=%v", counts.runtimeAdmission.Load(), counts.bind.Load(), storeReads.Load(), storeMutations.Load(), err)
	}
	if identity, readErr := os.ReadFile(filepath.Join(config.DataDir, "qlog", "voter-identity.json")); readErr != nil || len(identity) == 0 {
		t.Fatalf("authorized handoff did not persist local voter identity: bytes=%d err=%v", len(identity), readErr)
	}
	if manifests, globErr := filepath.Glob(filepath.Join(config.DataDir, "qlog", "manifest_*.bin")); globErr != nil || len(manifests) == 0 {
		t.Fatalf("authorized handoff did not bind the runtime WAL: manifests=%v err=%v", manifests, globErr)
	}
}

func TestNonDRPreparedRuntimeSkipsDRAdmission(t *testing.T) {
	for _, mode := range []string{"genesis", "ordinary-restore"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			var storeReads, storeMutations atomic.Int32
			store := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet && r.Method != http.MethodHead {
					storeMutations.Add(1)
					http.Error(w, "injected post-admission registration refusal", http.StatusForbidden)
					return
				}
				storeReads.Add(1)
				writeAzurePrebindRead(w, r)
			}))
			defer store.Close()

			var config types.ExecutionConfig
			var identity fencing.Identity
			switch mode {
			case "genesis":
				var assignment fencing.BootstrapAssignment
				config, assignment = initialGenesisTestConfig(t)
				configureAzureRuntimeConfig(&config, store.URL)
				if _, err := prepareInitialGenesis(ctx, &config, assignment); err != nil {
					t.Fatalf("prepare initial genesis: %v", err)
				}
				identity = fencing.Identity{
					SourceClusterID: assignment.SourceClusterID, NodeID: assignment.NodeID,
					PodUID: assignment.PodUID, RunID: "runtime-run",
				}
			case "ordinary-restore":
				config, _ = managedLearnerProfileFixture()
				config.DataDir = filepath.Join(t.TempDir(), "data")
				config.EnableReconfiguration = true
				if err := os.MkdirAll(config.DataDir, 0o700); err != nil {
					t.Fatal(err)
				}
				configureAzureRuntimeConfig(&config, store.URL)
				marker, err := preparedMarkerForConfig(&config)
				if err != nil {
					t.Fatal(err)
				}
				marker.Phase = "preparing"
				if err := writePreparedRestoreIntent(config.DataDir, marker); err != nil {
					t.Fatalf("write ordinary restore intent: %v", err)
				}
				marker.Phase = "complete"
				marker.CertifiedTip = 1
				marker.TipPrefixHash = strings.Repeat("0", 64)
				if err := writePreparedRestoreMarker(config.DataDir, marker); err != nil {
					t.Fatalf("write ordinary restore marker: %v", err)
				}
				identity = fencing.Identity{
					SourceClusterID: string(config.ClusterID), NodeID: string(config.NodeID),
					PodUID: config.PreparedPodUID, RunID: "runtime-run",
				}
			}

			supervisor, socket, counts := startNonDRRuntimeAdmissionSupervisor(t, identity)
			defer supervisor()
			t.Setenv("RHIZA_FENCE_SOCKET", socket)
			t.Setenv("RHIZA_FENCE_TICKET", "test-ticket")
			configureAzureStartupTransport(t, store)
			n := New(&config)
			err := n.Open(ctx)
			if err == nil || counts.bind.Load() != 1 || counts.runtimeAdmission.Load() != 0 {
				t.Fatalf("non-DR startup did not bypass DR admission and reach the existing bind: err=%v bind=%d admission=%d", err, counts.bind.Load(), counts.runtimeAdmission.Load())
			}
			if storeReads.Load() == 0 || storeMutations.Load() == 0 {
				t.Fatalf("non-DR startup did not reach the injected post-bind storage refusal: reads=%d mutations=%d", storeReads.Load(), storeMutations.Load())
			}
		})
	}
}

type drRuntimeAdmissionCounts struct {
	runtimeAdmission atomic.Int32
	bind             atomic.Int32
}

func startDRRuntimeAdmissionSupervisor(t *testing.T, identity fencing.Identity, assignment fencing.BootstrapAssignment, admissionStatus int) (func(), string, *drRuntimeAdmissionCounts) {
	t.Helper()
	socket := shortTestUnixSocketPath(t, "dr-admission.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	counts := &drRuntimeAdmissionCounts{}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/startup":
			_ = json.NewEncoder(w).Encode(fencing.Registration{Identity: identity})
		case "/v1/bootstrap-runtime-admission":
			counts.runtimeAdmission.Add(1)
			var message fencing.AuthorityMessage
			if err := json.NewDecoder(r.Body).Decode(&message); err != nil || message.Action != "bootstrap-runtime-admission" || message.RuntimeAdmission == nil || message.RuntimeAdmission.OperationID != assignment.OperationID || message.Identity != identity {
				http.Error(w, "unexpected runtime admission request", http.StatusBadRequest)
				return
			}
			if admissionStatus != http.StatusOK {
				w.WriteHeader(admissionStatus)
				return
			}
			_ = json.NewEncoder(w).Encode(fencing.BootstrapRuntimeAdmissionResult{Assignment: assignment})
		case "/v1/bind":
			counts.bind.Add(1)
			var got fencing.Identity
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil || got.WALIdentity == "" || got.SourceClusterID != identity.SourceClusterID || got.NodeID != identity.NodeID || got.PodUID != identity.PodUID || got.CredentialUID != identity.CredentialUID {
				http.Error(w, "invalid WAL bind", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	})}
	go func() { _ = server.Serve(listener) }()
	return func() { _ = server.Close(); _ = listener.Close() }, socket, counts
}

func startNonDRRuntimeAdmissionSupervisor(t *testing.T, identity fencing.Identity) (func(), string, *drRuntimeAdmissionCounts) {
	t.Helper()
	socket := shortTestUnixSocketPath(t, "non-dr-admission.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	counts := &drRuntimeAdmissionCounts{}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/startup":
			_ = json.NewEncoder(w).Encode(fencing.Registration{Identity: identity})
		case "/v1/bootstrap-runtime-admission":
			counts.runtimeAdmission.Add(1)
			http.Error(w, "non-DR runtime must not request DR admission", http.StatusForbidden)
		case "/v1/bind":
			counts.bind.Add(1)
			var got fencing.Identity
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil || got.WALIdentity == "" || got.SourceClusterID != identity.SourceClusterID || got.NodeID != identity.NodeID || got.PodUID != identity.PodUID {
				http.Error(w, "invalid WAL bind", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	})}
	go func() { _ = server.Serve(listener) }()
	return func() { _ = server.Close(); _ = listener.Close() }, socket, counts
}

func configureAzureRuntimeConfig(config *types.ExecutionConfig, endpoint string) {
	config.ObjStoreProvider = "azure"
	config.ObjStoreEndpoint = ""
	config.ObjStoreBucket = "container"
	config.ObjStoreAzureStorageAccount = "account"
	config.ObjStoreAzureConnectionString = fmt.Sprintf("DefaultEndpointsProtocol=https;AccountName=account;AccountKey=dGVzdA==;BlobEndpoint=%s", endpoint)
	config.PeerToken, config.AdminToken = "peer-token", "admin-token"
}

func managedDRRuntimeAdmissionFixture(t *testing.T, azureEndpoint string) (types.ExecutionConfig, fencing.BootstrapAssignment) {
	t.Helper()
	config, assignment := managedDRProfileFixture()
	nodeID, err := fencing.ReplacementNodeID(config.PreparedPodUID)
	if err != nil {
		t.Fatal(err)
	}
	config.NodeID = types.NodeID(nodeID)
	config.DataDir = filepath.Join(t.TempDir(), "data")
	config.ObjStoreProvider = "azure"
	config.ObjStoreEndpoint = ""
	config.ObjStoreBucket = "container"
	config.ObjStoreAzureStorageAccount = "account"
	config.ObjStoreAzureConnectionString = fmt.Sprintf("DefaultEndpointsProtocol=https;AccountName=account;AccountKey=dGVzdA==;BlobEndpoint=%s", azureEndpoint)
	config.PeerToken, config.AdminToken = "peer-token", "admin-token"
	config.Members = []quepaxa.Member{
		peerMember(string(config.ClusterID), quepaxa.NodeID(config.NodeID), "local-key"),
		peerMember(string(config.ClusterID), "n2", "peer-key"),
	}
	request := assignment.DRApproval.Request
	request.TargetMembership = recovery.NewMembershipRecord(string(config.ClusterID), config.Members, string(config.ObjStoreDurability))
	assignment.NodeID = string(config.NodeID)
	assignment.MembershipHash = request.TargetMembership.Membership
	assignment.StoreIdentityHash = managedStoreIdentityHash(&config)
	requestHash, err := recoveryanchor.RequestHash(request)
	if err != nil {
		t.Fatal(err)
	}
	assignment.DRApproval.Request = request
	assignment.DRApproval.Receipt.RequestHash = requestHash
	assignment.AuthorizationHash = fencing.BootstrapDRAuthorizationHash(assignment)
	if err := assignment.Validate(); err != nil {
		t.Fatalf("runtime DR assignment invalid: %v", err)
	}
	if _, err := validateManagedDRSuccessorAssignment(&config, assignment); err != nil {
		t.Fatalf("runtime DR assignment differs from config: %v", err)
	}
	return config, assignment
}

func runtimeIdentityForAssignment(assignment fencing.BootstrapAssignment) fencing.Identity {
	return fencing.Identity{
		SourceClusterID: assignment.SourceClusterID, NodeID: assignment.NodeID,
		PodName: assignment.PodName, PodUID: assignment.PodUID, RunID: "runtime-run",
		CredentialName: assignment.CredentialName, CredentialUID: assignment.CredentialUID,
	}
}

func writePreparedDRRuntimeMarker(t *testing.T, config *types.ExecutionConfig, assignment fencing.BootstrapAssignment) {
	t.Helper()
	if err := os.MkdirAll(config.DataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	marker, err := preparedMarkerForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	copy := assignment
	marker.DRAssignment = &copy
	marker.Phase = "preparing"
	if err := writePreparedRestoreIntent(config.DataDir, marker); err != nil {
		t.Fatalf("write DR preparation intent: %v", err)
	}
	marker.Phase = "complete"
	marker.CertifiedTip = 1
	marker.TipPrefixHash = strings.Repeat("0", 64)
	if err := writePreparedRestoreMarker(config.DataDir, marker); err != nil {
		t.Fatalf("write DR prepared marker: %v", err)
	}
}

func configureAzureStartupTransport(t *testing.T, store *httptest.Server) {
	t.Helper()
	previous := azure.DefaultConfig
	azure.DefaultConfig.HTTPConfig.Transport = store.Client().Transport
	t.Cleanup(func() { azure.DefaultConfig = previous })
}

func writeAzurePrebindRead(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/xml")
	if r.URL.Query().Get("comp") == "list" {
		_, _ = w.Write([]byte(`<EnumerationResults><Blobs></Blobs><NextMarker></NextMarker></EnumerationResults>`))
		return
	}
	code := "BlobNotFound"
	if r.Method == http.MethodHead {
		code = "ContainerNotFound"
	}
	w.Header().Set("x-ms-error-code", code)
	w.WriteHeader(http.StatusNotFound)
	_, _ = fmt.Fprintf(w, `<Error><Code>%s</Code><Message>not found</Message></Error>`, code)
}

func assertNoRuntimeVoterIdentity(t *testing.T, dataDir string) {
	t.Helper()
	for _, path := range []string{
		filepath.Join(dataDir, "qlog", "voter-identity.json"),
	} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("revoked runtime created local identity %q: %v", path, err)
		}
	}
	manifests, err := filepath.Glob(filepath.Join(dataDir, "qlog", "manifest_*.bin"))
	if err != nil || len(manifests) != 0 {
		t.Fatalf("revoked runtime created WAL manifests %v err=%v", manifests, err)
	}
}
