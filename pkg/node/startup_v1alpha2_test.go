package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type preparedStartupAllowChecker struct{}

func (preparedStartupAllowChecker) ValidatePreparedStartup(context.Context, recovery.PreparedStartupIdentity) error {
	return nil
}

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
	sourceBinding, configDigest, err := New(config).preparedBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	identity := recovery.PreparedStartupIdentity{ClusterUID: "uid", Generation: 2, DeploymentUID: "deployment", PodUID: "pod", NodeID: "learner", SourceBinding: sourceBinding, ConfigDigest: configDigest}
	membership, err := learnerCore.MembershipHistory()
	if err != nil {
		t.Fatal(err)
	}
	if err := persistPreparedAdoption(config.DataDir, identity, learnerCore.WALIdentity(), view, &membership); err != nil {
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
	walSnapshot := func() [32]byte {
		t.Helper()
		files, err := filepath.Glob(filepath.Join(config.DataDir, "qlog", "*"))
		if err != nil {
			t.Fatal(err)
		}
		var contents bytes.Buffer
		for _, file := range files {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			contents.WriteString(filepath.Base(file))
			contents.Write(data)
		}
		return sha256.Sum256(contents.Bytes())
	}
	beforeRestart := walSnapshot()
	restartedNode := New(config)
	restartErr := restartedNode.OpenPrepared(ctx, PreparedStartupConfig{Identity: identity, Checker: preparedStartupAllowChecker{}})
	if !errors.Is(restartErr, ErrPreparedWriteAdmissionUnavailable) {
		t.Fatalf("legacy prepared restart without marker: got %v, want %v", restartErr, ErrPreparedWriteAdmissionUnavailable)
	}
	if afterRestart := walSnapshot(); afterRestart != beforeRestart {
		t.Fatalf("read-only prepared restart changed WAL: before=%x after=%x", beforeRestart, afterRestart)
	}
	marker := filepath.Join(config.DataDir, "prepared-adoption.json")
	validMarker, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	var validAdoption preparedAdoption
	if err := json.Unmarshal(validMarker, &validAdoption); err != nil {
		t.Fatal(err)
	}
	t.Run("missing adoption fails closed", func(t *testing.T) {
		if err := os.Remove(marker); err != nil {
			t.Fatal(err)
		}
		if err := New(config).verifyPreparedAdoption(ctx, identity); err == nil {
			t.Fatal("missing adoption accepted")
		}
		if err := os.WriteFile(marker, validMarker, 0600); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("wrong checkpoint base fails closed", func(t *testing.T) {
		bad := validAdoption
		bad.BaseSeal = &quepaxa.CheckpointSeal{Index: 1, RootHash: [32]byte{1}, PrefixHash: [32]byte{2}}
		bad.BaseDecision = &quepaxa.DecidedValue{Slot: 1}
		data, err := json.Marshal(bad)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(marker, data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := New(config).verifyPreparedAdoption(ctx, identity); err == nil {
			t.Fatal("wrong checkpoint base accepted")
		}
		if err := os.WriteFile(marker, validMarker, 0600); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("wrong membership fails closed", func(t *testing.T) {
		bad := validAdoption
		bad.Membership = &quepaxa.MembershipRecord{Genesis: quepaxa.Cluster{ConfigID: 99}}
		data, err := json.Marshal(bad)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(marker, data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := New(config).verifyPreparedAdoption(ctx, identity); err == nil {
			t.Fatal("wrong membership accepted")
		}
		if err := os.WriteFile(marker, validMarker, 0600); err != nil {
			t.Fatal(err)
		}
	})
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

type managedTestObjectStore struct {
	mu       sync.RWMutex
	objects  map[string][]byte
	requests []string
	writes   int
}

func (s *managedTestObjectStore) put(key string, body []byte) {
	s.mu.Lock()
	s.objects[key] = append([]byte(nil), body...)
	s.mu.Unlock()
}

func (s *managedTestObjectStore) writeCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.writes
}

func managedStartupDataDigest(t *testing.T, dataDir string) (string, string) {
	t.Helper()
	hash := sha256.New()
	var files []string
	err := filepath.WalkDir(dataDir, func(name string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unexpected non-regular startup data %s", name)
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(dataDir, name)
		if err != nil {
			return err
		}
		fileHash := sha256.Sum256(data)
		files = append(files, fmt.Sprintf("%s:%x", relative, fileHash))
		_, _ = io.WriteString(hash, relative+"\x00")
		_, _ = hash.Write(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), strings.Join(files, "\n")
}

func managedSQLIdempotencyRows(t *testing.T, dbPath string) string {
	t.Helper()
	dbURL := (&url.URL{Scheme: "file", Path: filepath.ToSlash(dbPath)}).String()
	db, err := sql.Open("sqlite3", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	rows, err := db.Query(`SELECT kind,hex(request_id),hex(fingerprint),commit_slot,status,error_code,rows_affected,last_insert_id,applied,hex(sql_result) FROM _rhiza_idempotency ORDER BY kind,request_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var snapshot strings.Builder
	for rows.Next() {
		var kind, commitSlot, rowsAffected, lastInsertID, applied int64
		var requestID, fingerprint, status, errorCode, sqlResult string
		if err := rows.Scan(&kind, &requestID, &fingerprint, &commitSlot, &status, &errorCode, &rowsAffected, &lastInsertID, &applied, &sqlResult); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&snapshot, "%d/%s/%s/%d/%s/%s/%d/%d/%d/%s\n", kind, requestID, fingerprint, commitSlot, status, errorCode, rowsAffected, lastInsertID, applied, sqlResult)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return snapshot.String()
}

func (s *managedTestObjectStore) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requests = append(s.requests, r.Method+" "+r.URL.RequestURI())
	s.mu.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/prepared-target/")
	if r.Method == http.MethodGet && r.URL.Query().Has("location") {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
		return
	}
	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") != "" {
		type item struct {
			Key          string
			Size         int
			ETag         string
			LastModified string
		}
		type listing struct {
			XMLName     xml.Name `xml:"ListBucketResult"`
			Name        string
			Prefix      string
			IsTruncated bool
			Contents    []item
		}
		result := listing{Name: "prepared-target", Prefix: r.URL.Query().Get("prefix")}
		s.mu.RLock()
		for name, body := range s.objects {
			if strings.HasPrefix(name, result.Prefix) {
				result.Contents = append(result.Contents, item{name, len(body), fmt.Sprintf("%q", fmt.Sprintf("%x", sha256.Sum256(body))), "2026-10-30T00:00:00Z"})
			}
		}
		s.mu.RUnlock()
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(result)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	body, exists := s.objects[key]
	if r.Method == http.MethodPut || r.Method == http.MethodDelete {
		s.writes++
	}
	etag := fmt.Sprintf("%q", fmt.Sprintf("%x", sha256.Sum256(body)))
	if r.Method == http.MethodPut {
		if r.Header.Get("If-None-Match") == "*" && exists || r.Header.Get("If-Match") != "" && (!exists || r.Header.Get("If-Match") != etag) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		if strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
			var decoded []byte
			for len(body) > 0 {
				end := bytes.Index(body, []byte("\r\n"))
				if end < 0 {
					http.Error(w, "invalid signed chunk", http.StatusBadRequest)
					return
				}
				n, parseErr := strconv.ParseInt(strings.SplitN(string(body[:end]), ";", 2)[0], 16, 64)
				body = body[end+2:]
				if parseErr != nil || n < 0 || n > int64(len(body)) {
					http.Error(w, "invalid signed chunk size", http.StatusBadRequest)
					return
				}
				if n == 0 {
					break
				}
				decoded = append(decoded, body[:int(n)]...)
				body = body[int(n)+2:]
			}
			body = decoded
		}
		s.objects[key] = body
		w.Header().Set("ETag", fmt.Sprintf("%q", fmt.Sprintf("%x", sha256.Sum256(body))))
		return
	}
	if r.Method == http.MethodDelete {
		delete(s.objects, key)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code></Error>`)
		return
	}
	w.Header().Set("ETag", etag)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	if r.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
}

func TestManagedPreparedCheckpointSQLSuffixAdoptionAndRestart(t *testing.T) {
	ctx := context.Background()
	const clusterID = "managed-restore"
	members := []quepaxa.Member{peerMember(clusterID, "voter-a", "voter-a-token"), peerMember(clusterID, "voter-b", "voter-b-token")}
	members[0].PeerURL = "quic://127.0.0.1:19001"
	members[1].PeerURL = "quic://127.0.0.1:19002"
	cluster := quepaxa.Cluster{ConfigID: 1, Members: members}
	transport := &membershipOperationTransport{cores: make(map[quepaxa.NodeID]*quepaxa.Core), ids: []quepaxa.NodeID{"voter-a", "voter-b"}, disabled: make(map[quepaxa.NodeID]bool)}
	for _, member := range members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), string(member.ID), "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: cluster, WAL: wal, Transport: transport, EnableReconfiguration: true})
		if err != nil {
			t.Fatal(err)
		}
		transport.cores[member.ID] = core
	}
	sourceCore := transport.cores["voter-a"]
	memory := objstore.NewInMemBucket()
	const storageRoot = "prepared-test-root"
	namespace, err := ManagedNamespaceFor(storageRoot, "cluster-uid", 4, "assignment", clusterID)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := recovery.NewAppendOnlyManager(memory, namespace.FinalPrefix, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(archive.Close)
	checkpoints := checkpoint.NewManager(memory, namespace.FinalPrefix, t.TempDir(), 1)
	sourceMaterial, err := materializer.Open(filepath.Join(t.TempDir(), "sqlite.db"), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sourceMaterial.Close() })
	baseSQL, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "checkpoint-base", SQL: "CREATE TABLE managed_items(id INTEGER PRIMARY KEY, value TEXT)"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := sourceCore.Propose(ctx, baseSQL); err != nil {
		t.Fatal(err)
	}
	if err := sourceMaterial.Apply(ctx, 1, baseSQL); err != nil {
		t.Fatal(err)
	}
	files, index, cleanup, err := sourceMaterial.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if index != 1 {
		t.Fatalf("checkpoint snapshot index=%d, want 1", index)
	}
	sources := make([]checkpoint.Source, 0, len(files))
	for _, file := range files {
		sources = append(sources, checkpoint.Source{Role: string(file.Role), Path: file.Path})
	}
	claim, err := checkpoints.AcquirePublisherClaim(ctx, "prepared-base", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	root, err := checkpoints.CreateFiles(ctx, claim, sources, index)
	if releaseErr := checkpoints.ReleasePublisherClaim(ctx, claim); err != nil || releaseErr != nil {
		t.Fatal(errors.Join(err, releaseErr))
	}
	root, err = checkpoints.OpenRoot(ctx, root.Index, root.RootHash)
	if err != nil {
		t.Fatal(err)
	}
	prefix, ok := sourceCore.PrefixHash(1)
	if !ok {
		t.Fatal("source checkpoint prefix is missing")
	}
	next, following, err := sourceCore.CheckpointLeaderOrders(1)
	if err != nil {
		t.Fatal(err)
	}
	membership, err := sourceCore.CheckpointMembership(1)
	if err != nil {
		t.Fatal(err)
	}
	seal := quepaxa.CheckpointSeal{ConfigID: 1, Index: 1, RootHash: root.RootHash, StateHash: root.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, Membership: &membership}
	for _, core := range transport.cores {
		core.SetCheckpointValidator(func(ctx context.Context, candidate quepaxa.CheckpointSeal) error {
			if !reflect.DeepEqual(candidate, seal) {
				return fmt.Errorf("unexpected checkpoint seal")
			}
			return checkpoints.Verify(ctx, uint64(candidate.Index), candidate.RootHash, candidate.StateHash)
		})
		if err := core.PrepareCheckpoint(ctx, seal); err != nil {
			t.Fatal(err)
		}
	}
	sealValue, err := quepaxa.EncodeCheckpointSeal(seal)
	if err != nil {
		t.Fatal(err)
	}
	sealSlot, _, err := sourceCore.Propose(ctx, sealValue)
	if err != nil {
		t.Fatal(err)
	}
	sealDecision, ok := sourceCore.CertifiedValue(sealSlot)
	if !ok {
		t.Fatal("checkpoint seal decision is missing")
	}
	if err := sourceMaterial.Apply(ctx, uint64(sealSlot), sealValue); err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, sourceCore, sealSlot); err != nil {
		t.Fatal(err)
	}
	if err := archive.TrimThrough(ctx, quepaxa.SealedCheckpoint{CheckpointSeal: seal, DecisionSlot: sealSlot}, sealDecision); err != nil {
		t.Fatal(err)
	}
	suffixSQL, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "post-checkpoint", SQL: "INSERT INTO managed_items(value) VALUES ('suffix-value')"}})
	if err != nil {
		t.Fatal(err)
	}
	suffixSlot, _, err := sourceCore.Propose(ctx, suffixSQL)
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceMaterial.Apply(ctx, uint64(suffixSlot), suffixSQL); err != nil {
		t.Fatal(err)
	}
	finalSQL, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "final-value", SQL: "UPDATE managed_items SET value = 'final-value' WHERE id = 1"}})
	if err != nil {
		t.Fatal(err)
	}
	finalSlot, _, err := sourceCore.Propose(ctx, finalSQL)
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceMaterial.Apply(ctx, uint64(finalSlot), finalSQL); err != nil {
		t.Fatal(err)
	}
	if err := archive.SyncThrough(ctx, sourceCore, sourceCore.Tip()); err != nil {
		t.Fatal(err)
	}
	view, err := recovery.OpenReadOnlyView(ctx, memory, namespace.FinalPrefix, 1, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if view.Base() != 1 || view.Tip() != 4 {
		t.Fatalf("prepared view base=%d tip=%d, want base=1 tip=4", view.Base(), view.Tip())
	}
	preparedIdentity := recovery.PreparedStartupIdentity{ClusterUID: "cluster-uid", Generation: 4, DeploymentUID: "deployment", PodUID: "prepared-pod", NodeID: "learner", SourceBinding: "pending", ConfigDigest: "pending"}
	learner := peerMember(clusterID, "learner", "learner-token")
	dataDir := filepath.Join(t.TempDir(), "prepared-data")
	objectStore := &managedTestObjectStore{objects: make(map[string][]byte)}
	objectServer := httptest.NewServer(http.HandlerFunc(objectStore.serve))
	t.Cleanup(objectServer.Close)
	membershipRecord, err := json.Marshal(recovery.NewMembershipRecord(clusterID, members, string(types.ObjectStoreDurabilityBeforeAck)))
	if err != nil {
		t.Fatal(err)
	}
	objectStore.put(strings.TrimSuffix(namespace.ConfigPrefix, "/")+"/"+clusterID+"/voters/membership.json", membershipRecord)
	config := &types.ExecutionConfig{NodeID: "learner", ClusterID: clusterID, DataDir: dataDir, Members: members, Learner: &learner, PeerToken: "learner-token", AdminToken: "admin-token", PeerAddr: "127.0.0.1:0", EnableReconfiguration: true, ObjStoreProvider: "s3", ObjStoreEndpoint: strings.TrimPrefix(objectServer.URL, "http://"), ObjStoreInsecure: true, ObjStoreBucket: "prepared-target", ObjStoreAccessKey: "test", ObjStoreSecretKey: "test", ObjStorePrefix: namespace.ConfigPrefix, ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck, ObjStoreSyncInterval: time.Hour}
	node := New(config)
	sourceBinding, configDigest, err := node.preparedBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	preparedIdentity.SourceBinding, preparedIdentity.ConfigDigest = sourceBinding, configDigest
	handoffDir := filepath.Join(t.TempDir(), "handoff")
	if err := recovery.PublishPreparedStartup(ctx, view, handoffDir, preparedIdentity); err != nil {
		t.Fatal(err)
	}
	prepared, err := recovery.LoadPreparedStartup(ctx, memory, namespace.FinalPrefix, 1, cluster, handoffDir, dataDir, preparedIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if err := memory.Iter(ctx, namespace.FinalPrefix+"/", func(name string) error {
		reader, err := memory.Get(ctx, name)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(reader)
		return errors.Join(readErr, reader.Close(), func() error {
			if readErr == nil {
				objectStore.put(name, body)
			}
			return nil
		}())
	}, objstore.WithRecursiveIter()); err != nil {
		t.Fatal(err)
	}
	bootstrapPlan, err := node.ManagedBootstrapPlanDigest(sourceBinding, "sql-v1")
	if err != nil {
		t.Fatal(err)
	}
	identity := ManagedStartupIdentity{Member: preparedIdentity, AssignmentID: "assignment", IntentHash: "intent", StorageID: "storage", PhysicalBinding: sourceBinding, BootstrapPlanDigest: bootstrapPlan, EvidenceFormat: "sql-v1"}
	checker := &managedTestChecker{}
	startup := ManagedStartupConfig{StorageRoot: storageRoot, Identity: identity, Checker: checker, Prepared: prepared}
	if err := node.OpenManaged(ctx, startup); err != nil {
		objectStore.mu.RLock()
		requests := append([]string(nil), objectStore.requests...)
		objectStore.mu.RUnlock()
		t.Fatalf("open prepared managed node: %v; requests=%v", err, requests)
	}
	if _, err := node.InspectManagedSQL(ctx, false); err != nil {
		t.Fatalf("inspect prepared SQL history: %v", err)
	}
	result, err := node.material.QueryResult(ctx, "SELECT value FROM managed_items WHERE id = 1", nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != "final-value" {
		t.Fatalf("restored SQL suffix result=%+v err=%v", result, err)
	}
	receiptsBeforeRestart := managedSQLIdempotencyRows(t, filepath.Join(dataDir, "sqlite.db"))
	if !strings.Contains(receiptsBeforeRestart, "706F73742D636865636B706F696E74") {
		t.Fatal("retained SQL receipt is missing before restart")
	}
	if err := node.Shutdown(); err != nil {
		t.Fatal(err)
	}
	preparedDBURL := (&url.URL{Scheme: "file", Path: filepath.ToSlash(filepath.Join(dataDir, "sqlite.db"))}).String()
	preparedDB, err := sql.Open("sqlite3", preparedDBURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := preparedDB.ExecContext(ctx, `UPDATE managed_items SET value='prepared-corrupt' WHERE id=1`); err != nil {
		_ = preparedDB.Close()
		t.Fatal(err)
	}
	if err := preparedDB.Close(); err != nil {
		t.Fatal(err)
	}
	beforePreparedCorrupt, beforePreparedCorruptFiles := managedStartupDataDigest(t, dataDir)
	writesBeforePreparedCorrupt := objectStore.writeCount()
	preparedCorrupt := New(config)
	if err := preparedCorrupt.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: identity, Checker: checker, Prepared: prepared}); err == nil || !strings.Contains(err.Error(), "actual SQLite contents differ") {
		t.Fatalf("prepared-target corruption result: %v", err)
	}
	if preparedCorrupt.opened.Load() || preparedCorrupt.peer != nil || preparedCorrupt.wal != nil || preparedCorrupt.lock != nil {
		t.Fatal("prepared-target preflight rejection retained runtime resources")
	}
	if after, files := managedStartupDataDigest(t, dataDir); after != beforePreparedCorrupt {
		t.Fatalf("prepared-target preflight mutated local state: before=%s after=%s\nbefore files:\n%s\nafter files:\n%s", beforePreparedCorrupt, after, beforePreparedCorruptFiles, files)
	}
	if writes := objectStore.writeCount(); writes != writesBeforePreparedCorrupt {
		t.Fatalf("prepared-target preflight wrote object storage: before=%d after=%d", writesBeforePreparedCorrupt, writes)
	}
	preparedDB, err = sql.Open("sqlite3", preparedDBURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := preparedDB.ExecContext(ctx, `UPDATE managed_items SET value='final-value' WHERE id=1`); err != nil {
		_ = preparedDB.Close()
		t.Fatal(err)
	}
	if err := preparedDB.Close(); err != nil {
		t.Fatal(err)
	}
	preparedNeighbor := New(config)
	if err := preparedNeighbor.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: identity, Checker: checker}); err != nil {
		t.Fatalf("valid prepared neighbor after rejected corruption: %v", err)
	}
	if _, err := preparedNeighbor.InspectManagedSQL(ctx, false); err != nil {
		t.Fatalf("inspect valid prepared neighbor SQL: %v", err)
	}
	if err := preparedNeighbor.Shutdown(); err != nil {
		t.Fatal(err)
	}
	restart := New(config)
	if err := restart.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: identity, Checker: checker}); err != nil {
		t.Fatalf("restart adopted prepared node: %v", err)
	}
	if got := managedSQLIdempotencyRows(t, filepath.Join(dataDir, "sqlite.db")); got != receiptsBeforeRestart {
		t.Fatalf("retained SQL receipt rows changed across restart:\nbefore:\n%s\nafter:\n%s", receiptsBeforeRestart, got)
	}
	if _, err := restart.InspectManagedSQL(ctx, false); err != nil {
		t.Fatalf("inspect restarted SQL history: %v", err)
	}
	if err := restart.Shutdown(); err != nil {
		t.Fatal(err)
	}

	marker := filepath.Join(dataDir, "prepared-adoption.json")
	validMarker, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	var validAdoption preparedAdoption
	if err := json.Unmarshal(validMarker, &validAdoption); err != nil {
		t.Fatal(err)
	}
	checkRejected := func(label string, mutate func(*preparedAdoption)) {
		t.Helper()
		t.Run(label, func(t *testing.T) {
			encoded, err := json.Marshal(validAdoption)
			if err != nil {
				t.Fatal(err)
			}
			var bad preparedAdoption
			if err := json.Unmarshal(encoded, &bad); err != nil {
				t.Fatal(err)
			}
			mutate(&bad)
			baseline, err := json.Marshal(validAdoption)
			if err != nil || !bytes.Equal(baseline, validMarker) {
				t.Fatalf("negative case mutated valid adoption baseline: err=%v", err)
			}
			data, err := json.Marshal(bad)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(marker, data, 0600); err != nil {
				t.Fatal(err)
			}
			candidate := New(config)
			if err := candidate.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: identity, Checker: checker}); err == nil {
				t.Fatal("corrupted adoption accepted")
			}
			if candidate.opened.Load() || candidate.peer != nil || candidate.wal != nil {
				t.Fatal("pre-open adoption rejection retained runtime resources")
			}
			if err := os.WriteFile(marker, validMarker, 0600); err != nil {
				t.Fatal(err)
			}
		})
	}
	checkRejected("wrong base root fails closed", func(adoption *preparedAdoption) { adoption.BaseSeal.RootHash[0] ^= 0xff })
	checkRejected("wrong membership fails closed", func(adoption *preparedAdoption) { adoption.Membership.Genesis.ConfigID++ })
	checkRejected("wrong WAL nonce fails closed", func(adoption *preparedAdoption) { adoption.WALIdentity = strings.Repeat("f", 64) })
	if err := New(config).verifyPreparedAdoption(ctx, preparedIdentity); err != nil {
		t.Fatalf("valid adoption after negative cases: %v", err)
	}

	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := New(config).OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: identity, Checker: checker}); err == nil {
		t.Fatal("missing adoption accepted")
	}
	if err := os.WriteFile(marker, validMarker, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(marker, 0700); err != nil {
		t.Fatal(err)
	}
	persistFailure := New(config)
	if err := persistFailure.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: identity, Checker: checker, Prepared: prepared}); err == nil || !strings.Contains(err.Error(), "persist verified prepared adoption") {
		t.Fatalf("prepared adoption persistence failure result: %v", err)
	}
	if persistFailure.opened.Load() || persistFailure.peer != nil || persistFailure.wal != nil || persistFailure.lock != nil {
		t.Fatal("post-open adoption persistence failure retained runtime resources")
	}
	_, persistLock, err := qlog.Acquire(filepath.Join(dataDir, "qlog"))
	if err != nil {
		t.Fatalf("post-open adoption persistence failure retained WAL lock: %v", err)
	}
	if err := persistLock.Release(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, validMarker, 0600); err != nil {
		t.Fatal(err)
	}

	// Advance the adopted WAL after the prepared view was captured, then verify
	// the exact applied SQL cut from the certified local suffix on restart.
	advanceNode := New(config)
	if err := advanceNode.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: identity, Checker: checker}); err != nil {
		t.Fatalf("open adopted node before certified WAL advance: %v", err)
	}
	advancedSQL, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "after-captured-view", SQL: "UPDATE managed_items SET value = 'beyond-view-tip' WHERE id = 1"}})
	if err != nil {
		t.Fatal(err)
	}
	advancedSlot, _, err := sourceCore.Propose(ctx, advancedSQL)
	if err != nil {
		t.Fatal(err)
	}
	advancedDecision, ok := sourceCore.CertifiedValue(advancedSlot)
	if !ok || advancedSlot <= view.Tip() {
		t.Fatalf("source did not certify a decision beyond captured view tip: slot=%d view=%d", advancedSlot, view.Tip())
	}
	if err := advanceNode.core.AcceptCertifiedValues([]quepaxa.DecidedValue{advancedDecision}); err != nil {
		t.Fatalf("advance adopted WAL with certified value: %v", err)
	}
	if err := advanceNode.material.Apply(ctx, uint64(advancedSlot), advancedSQL); err != nil {
		t.Fatalf("apply certified value beyond captured view tip: %v", err)
	}
	if advanceNode.core.Tip() != advancedSlot || advanceNode.material.Tip() != uint64(advancedSlot) {
		t.Fatalf("actual WAL/materializer tips=%d/%d, want advanced slot %d", advanceNode.core.Tip(), advanceNode.material.Tip(), advancedSlot)
	}
	advancedReceipts := managedSQLIdempotencyRows(t, filepath.Join(dataDir, "sqlite.db"))
	if !strings.Contains(advancedReceipts, "61667465722D63617074757265642D76696577") {
		t.Fatal("certified post-view SQL receipt is missing")
	}
	if err := advanceNode.Shutdown(); err != nil {
		t.Fatal(err)
	}
	dbURL := (&url.URL{Scheme: "file", Path: filepath.ToSlash(filepath.Join(dataDir, "sqlite.db"))}).String()
	db, err := sql.Open("sqlite3", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE _rhiza_meta SET value=? WHERE key='applied_slot'`, fmt.Sprint(uint64(advancedSlot)+1)); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	beforeMissingBridge, beforeMissingBridgeFiles := managedStartupDataDigest(t, dataDir)
	writesBeforeMissingBridge := objectStore.writeCount()
	missingBridge := New(config)
	if err := missingBridge.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: identity, Checker: checker}); err == nil || !strings.Contains(err.Error(), "ahead of certified WAL tip") {
		t.Fatalf("unbridged actual SQL tip result: %v", err)
	}
	if after, files := managedStartupDataDigest(t, dataDir); after != beforeMissingBridge {
		t.Fatalf("missing-bridge rejection mutated source state: before=%s after=%s\nbefore files:\n%s\nafter files:\n%s", beforeMissingBridge, after, beforeMissingBridgeFiles, files)
	}
	if writes := objectStore.writeCount(); writes != writesBeforeMissingBridge {
		t.Fatalf("missing-bridge rejection wrote object storage: before=%d after=%d", writesBeforeMissingBridge, writes)
	}
	db, err = sql.Open("sqlite3", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE _rhiza_meta SET value=? WHERE key='applied_slot'`, fmt.Sprint(uint64(advancedSlot))); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	advancedRestart := New(config)
	if err := advancedRestart.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: identity, Checker: checker}); err != nil {
		t.Fatalf("restart adopted WAL beyond captured prepared view: %v", err)
	}
	if got := managedSQLIdempotencyRows(t, filepath.Join(dataDir, "sqlite.db")); got != advancedReceipts {
		t.Fatalf("retained actual SQL receipts changed across advanced restart:\nbefore:\n%s\nafter:\n%s", advancedReceipts, got)
	}
	if err := advancedRestart.Shutdown(); err != nil {
		t.Fatal(err)
	}

	dbURL = (&url.URL{Scheme: "file", Path: filepath.ToSlash(filepath.Join(dataDir, "sqlite.db"))}).String()
	db, err = sql.Open("sqlite3", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE managed_items SET value = 'corrupt' WHERE id = 1"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	suffixHash := fmt.Sprintf("%x", sha256.Sum256(suffixSQL))
	if _, err := db.ExecContext(ctx, `UPDATE _rhiza_meta SET value=? WHERE key='applied_slot'`, fmt.Sprint(uint64(suffixSlot))); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE _rhiza_meta SET value=? WHERE key='state_slot'`, fmt.Sprint(uint64(suffixSlot))); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE _rhiza_meta SET value=? WHERE key='applied_hash'`, suffixHash); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM _rhiza_idempotency WHERE commit_slot > ?`, suffixSlot); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	corrupt := New(config)
	beforeCorruptDigest, beforeCorruptFiles := managedStartupDataDigest(t, dataDir)
	beforeCorruptWrites := objectStore.writeCount()
	existingCorrupt := New(config)
	if err := existingCorrupt.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: identity, Checker: checker}); err == nil || !strings.Contains(err.Error(), "actual SQLite contents differ") {
		t.Fatalf("existing managed-target corruption result: %v", err)
	}
	if existingCorrupt.opened.Load() || existingCorrupt.peer != nil || existingCorrupt.wal != nil || existingCorrupt.lock != nil {
		t.Fatal("prepared-target preflight rejection retained runtime resources")
	}
	if afterDigest, afterFiles := managedStartupDataDigest(t, dataDir); afterDigest != beforeCorruptDigest {
		t.Fatalf("prepared-target preflight changed local WAL/SQLite/adoption data: before=%s after=%s\nbefore files:\n%s\nafter files:\n%s", beforeCorruptDigest, afterDigest, beforeCorruptFiles, afterFiles)
	}
	if after := objectStore.writeCount(); after != beforeCorruptWrites {
		t.Fatalf("prepared-target preflight wrote object storage: before=%d after=%d", beforeCorruptWrites, after)
	}
	if err := corrupt.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: identity, Checker: checker}); err == nil || !strings.Contains(err.Error(), "actual SQLite contents differ") {
		t.Fatalf("pre-replay SQLite corruption result: %v", err)
	}
	if afterDigest, afterFiles := managedStartupDataDigest(t, dataDir); afterDigest != beforeCorruptDigest {
		t.Fatalf("preflight rejection changed local WAL/SQLite/adoption data: before=%s after=%s\nbefore files:\n%s\nafter files:\n%s", beforeCorruptDigest, afterDigest, beforeCorruptFiles, afterFiles)
	}
	if after := objectStore.writeCount(); after != beforeCorruptWrites {
		t.Fatalf("preflight rejection wrote object storage: before=%d after=%d", beforeCorruptWrites, after)
	}
	if corrupt.opened.Load() || corrupt.peer != nil || corrupt.wal != nil || corrupt.lock != nil {
		t.Fatal("pre-open SQLite rejection retained runtime resources")
	}
	_, lock, err := qlog.Acquire(filepath.Join(dataDir, "qlog"))
	if err != nil {
		t.Fatalf("pre-open rejection retained WAL lock: %v", err)
	}
	if err := lock.Release(); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite3", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	var preserved string
	if err := db.QueryRowContext(ctx, "SELECT value FROM managed_items WHERE id = 1").Scan(&preserved); err != nil || preserved != "corrupt" {
		_ = db.Close()
		t.Fatalf("pre-open rejection overwrote corrupt SQLite: value=%q err=%v", preserved, err)
	}
	var preservedTip string
	if err := db.QueryRowContext(ctx, `SELECT value FROM _rhiza_meta WHERE key='applied_slot'`).Scan(&preservedTip); err != nil || preservedTip != fmt.Sprint(uint64(suffixSlot)) {
		_ = db.Close()
		t.Fatalf("pre-replay rejection changed applied tip: tip=%q err=%v", preservedTip, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite3", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	baseHash := fmt.Sprintf("%x", sha256.Sum256(baseSQL))
	if _, err := db.ExecContext(ctx, `UPDATE managed_items SET value='base-corrupt' WHERE id=1`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	for key, value := range map[string]string{"applied_slot": "1", "state_slot": "1", "applied_hash": baseHash} {
		if _, err := db.ExecContext(ctx, `UPDATE _rhiza_meta SET value=? WHERE key=?`, value, key); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM _rhiza_idempotency WHERE commit_slot > 1`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	baseCorrupt := New(config)
	if err := baseCorrupt.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: identity, Checker: checker}); err == nil || !strings.Contains(err.Error(), "actual SQLite contents differ") {
		t.Fatalf("checkpoint-tip SQLite corruption result: %v", err)
	}
	if baseCorrupt.opened.Load() || baseCorrupt.peer != nil || baseCorrupt.wal != nil || baseCorrupt.lock != nil {
		t.Fatal("checkpoint-tip rejection retained runtime resources")
	}
	db, err = sql.Open("sqlite3", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	var basePreserved string
	if err := db.QueryRowContext(ctx, "SELECT value FROM managed_items WHERE id = 1").Scan(&basePreserved); err != nil || basePreserved != "base-corrupt" {
		_ = db.Close()
		t.Fatalf("checkpoint-tip rejection changed original SQLite: value=%q err=%v", basePreserved, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite3", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE _rhiza_meta SET value=? WHERE key='applied_slot'`, fmt.Sprint(uint64(advancedSlot)+1)); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE _rhiza_meta SET value=? WHERE key='state_slot'`, fmt.Sprint(uint64(advancedSlot)+1)); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	missingSuffix := New(config)
	if err := missingSuffix.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: storageRoot, Identity: identity, Checker: checker}); err == nil || !strings.Contains(err.Error(), "ahead of certified WAL tip") {
		t.Fatalf("missing certified SQL suffix result: %v", err)
	}
	if missingSuffix.opened.Load() || missingSuffix.peer != nil || missingSuffix.wal != nil || missingSuffix.lock != nil {
		t.Fatal("missing-suffix rejection retained runtime resources")
	}
}
