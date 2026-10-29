package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestPreparedNodeDenialBeforeTargetAndServing(t *testing.T) {
	ctx := context.Background()
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{peerMember("cluster", "a", "a-token"), peerMember("cluster", "b", "b-token")}}
	transport := &membershipOperationTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), disabled: make(map[quepaxa.NodeID]bool)}
	for _, member := range cluster.Members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "source-wal"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: cluster, WAL: wal, Transport: transport, EnableReconfiguration: true})
		if err != nil {
			t.Fatal(err)
		}
		transport.cores[member.ID] = core
		transport.ids = append(transport.ids, member.ID)
	}
	core := transport.cores["a"]
	command, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "prepared-node", Events: []types.GraphStreamEvent{{Stream: "events", Kind: "created", Payload: "one"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, command); err != nil {
		t.Fatal(err)
	}
	source := objstore.NewInMemBucket()
	archive, err := recovery.NewAppendOnlyManager(source, "source", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(archive.Close)
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	view, err := recovery.OpenReadOnlyView(ctx, source, "source", 1, cluster)
	if err != nil {
		t.Fatal(err)
	}
	comparePreparedGraph(t, ctx, view)
	handoffDir := filepath.Join(t.TempDir(), "handoff")
	dataDir := filepath.Join(t.TempDir(), "verified")
	learner := peerMember("cluster", "learner", "learner-token")
	var targetRequests atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetRequests.Add(1)
		http.Error(w, "unexpected target request", http.StatusInternalServerError)
	}))
	t.Cleanup(target.Close)
	config := &types.ExecutionConfig{NodeID: "learner", ClusterID: "cluster", DataDir: dataDir, Members: cluster.Members, Learner: &learner, PeerToken: "learner-token", AdminToken: "admin-token", EnableReconfiguration: true, ObjStoreProvider: "s3", ObjStoreEndpoint: strings.TrimPrefix(target.URL, "http://"), ObjStoreInsecure: true, ObjStoreBucket: "prepared-target", ObjStoreAccessKey: "test", ObjStoreSecretKey: "test", ObjStorePrefix: "target"}
	n := New(config)
	sourceBinding, configDigest, err := n.preparedBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	identity := recovery.PreparedStartupIdentity{ClusterUID: "uid", Generation: 2, DeploymentUID: "deployment", PodUID: "pod", NodeID: "learner", SourceBinding: sourceBinding, ConfigDigest: configDigest}
	if err := recovery.PublishPreparedStartup(ctx, view, handoffDir, identity); err != nil {
		t.Fatal(err)
	}
	prepared, err := recovery.LoadPreparedStartup(ctx, source, "source", 1, cluster, handoffDir, dataDir, identity)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Directory() != dataDir || prepared.Directory() == handoffDir {
		t.Fatal("loaded data did not use the separate verified reconstruction")
	}
	var generation = 3
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","metadata":{"name":"cluster","namespace":"ns","uid":"uid","resourceVersion":"1"},"spec":{"logicalID":"logical"},"status":{"authority":{"owner":{"namespace":"ns","name":"cluster","uid":"uid","logicalID":"logical"},"generation":%d,"binding":{"deploymentUID":"deployment","storageID":"storage"},"pendingWrite":false,"evidence":{"tip":0}}}}`, generation)
	}))
	t.Cleanup(server.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("token"), 0600); err != nil {
		t.Fatal(err)
	}
	store := &operator.V1Alpha2StatusStore{Kube: &operator.Kubernetes{BaseURL: server.URL, Namespace: "ns", TokenFile: token, Client: server.Client()}}
	checker := &operator.V1Alpha2PreparedStartupChecker{Store: store, Ref: operator.V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "uid"}, Authority: operator.V1Alpha2AuthorityView{Owner: operator.V1Alpha2AuthorityOwner{Namespace: "ns", Name: "cluster", UID: "uid", LogicalID: "logical"}, Generation: 2, Binding: operator.V1Alpha2AuthorityBinding{DeploymentUID: "deployment", StorageID: "storage"}}, Identity: identity}
	err = n.OpenPrepared(ctx, PreparedStartupConfig{Prepared: prepared, Checker: checker})
	if err == nil || !strings.Contains(err.Error(), "prepared startup authority") {
		t.Fatalf("foreign live generation: %v", err)
	}
	if targetRequests.Load() != 0 {
		t.Fatalf("denied startup made %d target requests", targetRequests.Load())
	}
	if n.Ready() || n.server != nil {
		t.Fatal("denied startup exposed serving state")
	}
}

func comparePreparedGraph(t *testing.T, ctx context.Context, view *recovery.ReadOnlyView) {
	t.Helper()
	type artifact struct {
		size int
		hash [32]byte
	}
	read := func() map[string]artifact {
		dir := filepath.Join(t.TempDir(), "restore")
		if _, err := recovery.PrepareReadOnlyRestore(ctx, view, dir); err != nil {
			t.Fatal(err)
		}
		root := filepath.Join(dir, "latticedb")
		files := map[string]artifact{}
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			files[rel] = artifact{len(data), sha256.Sum256(data)}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return files
	}
	a, b := read(), read()
	if len(a) == 0 || len(b) == 0 {
		t.Fatal("fixture did not create graph artifacts")
	}
	for name, first := range a {
		second, ok := b[name]
		if !ok || first != second {
			t.Logf("graph artifact %s differs: first size=%d sha256=%x; second present=%t size=%d sha256=%x", name, first.size, first.hash, ok, second.size, second.hash)
		}
	}
	for name := range b {
		if _, ok := a[name]; !ok {
			t.Logf("graph artifact only in second preparation: %s", name)
		}
	}
}

func TestPreparedNodeAzureConnectionTrailingSemicolon(t *testing.T) {
	cfg := &types.ExecutionConfig{NodeID: "learner", ClusterID: "cluster", ObjStoreProvider: "azure", ObjStoreBucket: "bucket", ObjStoreAzureStorageAccount: "account", ObjStoreAzureConnectionString: "DefaultEndpointsProtocol=http;AccountName=account;AccountKey=key;BlobEndpoint=http://localhost:4577/account;"}
	first, _, err := New(cfg).preparedBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	cfg.ObjStoreAzureConnectionString = strings.TrimSuffix(cfg.ObjStoreAzureConnectionString, ";")
	second, _, err := New(cfg).preparedBindingDigests()
	if err != nil || first != second {
		t.Fatalf("equivalent connection binding differs: %s %s %v", first, second, err)
	}
}

func TestPreparedNodeAdoptionRestartGenuineWAL(t *testing.T) {
	ctx := context.Background()
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{peerMember("cluster", "a", "a-token"), peerMember("cluster", "b", "b-token")}}
	transport := &membershipOperationTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), disabled: make(map[quepaxa.NodeID]bool)}
	for _, member := range cluster.Members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: cluster, WAL: wal, Transport: transport, EnableReconfiguration: true})
		if err != nil {
			t.Fatal(err)
		}
		transport.cores[member.ID] = core
		transport.ids = append(transport.ids, member.ID)
	}
	value, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "adoption", Events: []types.GraphStreamEvent{{Stream: "s", Kind: "created", Payload: "v"}}})
	if err != nil {
		t.Fatal(err)
	}
	core := transport.cores["a"]
	if _, _, err := core.Propose(ctx, value); err != nil {
		t.Fatal(err)
	}
	source := objstore.NewInMemBucket()
	archive, err := recovery.NewAppendOnlyManager(source, "source", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(archive.Close)
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	view, err := recovery.OpenReadOnlyView(ctx, source, "source", 1, cluster)
	if err != nil {
		t.Fatal(err)
	}
	learner := peerMember("cluster", "learner", "learner-token")
	config := &types.ExecutionConfig{NodeID: "learner", ClusterID: "cluster", DataDir: t.TempDir(), Members: cluster.Members, Learner: &learner, PeerToken: "learner-token", AdminToken: "admin-token", EnableReconfiguration: true, ObjStoreProvider: "filesystem", ObjStoreDir: t.TempDir(), ObjStorePrefix: "target"}
	state, err := loadVoterIdentity(config)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(filepath.Join(config.DataDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	walOpen := true
	t.Cleanup(func() {
		if walOpen {
			if err := wal.Close(); err != nil {
				t.Errorf("close learner WAL: %v", err)
			}
		}
	})
	if err := ensureVoterIdentity(ctx, config, objstore.NewInMemBucket(), wal, state, false); err != nil {
		t.Fatal(err)
	}
	learnerCore, err := quepaxa.NewLearner(quepaxa.Config{NodeID: "learner", Cluster: cluster, WAL: wal, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	if err := learnerCore.AcceptCertifiedValues(view.Decisions()); err != nil {
		t.Fatal(err)
	}
	identity := recovery.PreparedStartupIdentity{ClusterUID: "uid", Generation: 2, DeploymentUID: "deployment", PodUID: "pod", NodeID: "learner", SourceBinding: "source", ConfigDigest: "config"}
	if err := persistPreparedAdoption(config.DataDir, identity, learnerCore.WALIdentity(), view); err != nil {
		t.Fatal(err)
	}
	err = wal.Close()
	walOpen = false
	if err != nil {
		t.Fatal(err)
	}
	if err := New(config).verifyPreparedAdoption(ctx, identity); err != nil {
		t.Fatalf("registered learner restart: %v", err)
	}
	t.Run("foreign bound identity preserves torn WAL", func(t *testing.T) {
		other, err := qlog.Open(filepath.Join(t.TempDir(), "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		otherOpen := true
		t.Cleanup(func() {
			if otherOpen {
				if err := other.Close(); err != nil {
					t.Errorf("close foreign WAL: %v", err)
				}
			}
		})
		otherCore, err := quepaxa.NewLearner(quepaxa.Config{NodeID: "learner", Cluster: cluster, WAL: other, Transport: transport})
		if err != nil {
			t.Fatal(err)
		}
		foreign := otherCore.WALIdentity()
		if foreign == learnerCore.WALIdentity() {
			t.Fatal("fixture identities unexpectedly match")
		}
		err = other.Close()
		otherOpen = false
		if err != nil {
			t.Fatal(err)
		}
		local, err := loadVoterIdentity(config)
		if err != nil || local.identity == nil {
			t.Fatalf("load local identity: %v", err)
		}
		local.identity.Nonce = foreign
		if err := writeVoterIdentity(config.DataDir, *local.identity); err != nil {
			t.Fatal(err)
		}
		marker := filepath.Join(config.DataDir, "prepared-adoption.json")
		data, err := os.ReadFile(marker)
		if err != nil {
			t.Fatal(err)
		}
		var adoption preparedAdoption
		if err := json.Unmarshal(data, &adoption); err != nil {
			t.Fatal(err)
		}
		adoption.WALIdentity = foreign
		data, err = json.Marshal(adoption)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(marker, data, 0600); err != nil {
			t.Fatal(err)
		}
		paths, err := filepath.Glob(filepath.Join(config.DataDir, "qlog", "seg_*.log"))
		if err != nil || len(paths) != 1 {
			t.Fatalf("segments=%v err=%v", paths, err)
		}
		f, err := os.OpenFile(paths[0], os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		fileOpen := true
		t.Cleanup(func() {
			if fileOpen {
				if err := f.Close(); err != nil {
					t.Errorf("close WAL segment: %v", err)
				}
			}
		})
		if _, err := f.Write([]byte{0x7f}); err != nil {
			t.Fatal(err)
		}
		err = f.Close()
		fileOpen = false
		if err != nil {
			t.Fatal(err)
		}
		before, err := os.ReadFile(paths[0])
		if err != nil {
			t.Fatal(err)
		}
		if err := New(config).verifyPreparedAdoption(ctx, identity); err == nil {
			t.Fatal("foreign WAL identity accepted")
		}
		after, err := os.ReadFile(paths[0])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(before, after) {
			t.Fatal("foreign WAL was modified before identity validation")
		}
	})
}
