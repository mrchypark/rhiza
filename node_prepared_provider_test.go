package rhiza_test

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

//go:embed pkg/node/startup_v1alpha2.go
var preparedNodeSource string

// This static guard covers only the prepared-stage source file; it is not an
// SDK request trace and does not establish cloud-provider behavior.
func TestPreparedNodeStageAvoidsLegacyRecoverySnapshot(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "startup_v1alpha2.go", preparedNodeSource, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(file, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "BeginRecoverySnapshot" {
			t.Error("prepared stage invokes legacy recovery snapshot")
		}
		return true
	})
}

func preparedRegistrationRequest(r *http.Request, provider, bucket, account, object string) bool {
	switch provider {
	case "s3":
		return r.Method == http.MethodPut && r.URL.Path == "/"+bucket+"/"+object && r.Header.Get("If-None-Match") == "*"
	case "azure":
		return r.Method == http.MethodPut && r.URL.Path == "/"+account+"/"+bucket+"/"+object && r.Header.Get("If-None-Match") == "*"
	case "gcs":
		q := r.URL.Query()
		return r.Method == http.MethodPost && r.URL.Path == "/upload/storage/v1/b/"+bucket+"/o" && len(q["name"]) == 1 && q.Get("name") == object && len(q["ifGenerationMatch"]) == 1 && q.Get("ifGenerationMatch") == "0"
	default:
		return false
	}
}

func preparedEmulatorAuthRequest(r *http.Request, provider string) bool {
	return provider == "gcs" && r.Method == http.MethodPost && r.URL.Path == "/token" && r.URL.RawQuery == "" && r.Header.Get("Content-Type") == "application/x-www-form-urlencoded"
}

func TestPreparedNodeEmulatorAuthScope(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/token", nil)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if !preparedEmulatorAuthRequest(req, "gcs") {
		t.Fatal("emulator token request rejected")
	}
	for _, target := range []string{"/other", "/token?other=1"} {
		other := httptest.NewRequest(http.MethodPost, target, nil)
		other.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if preparedEmulatorAuthRequest(other, "gcs") {
			t.Fatal("unknown POST accepted")
		}
	}
	if preparedEmulatorAuthRequest(req, "s3") {
		t.Fatal("wrong provider accepted")
	}
	req.Header.Del("Content-Type")
	if preparedEmulatorAuthRequest(req, "gcs") {
		t.Fatal("missing content type accepted")
	}
}

func TestPreparedNodeRegistrationRequestScope(t *testing.T) {
	for _, tc := range []struct {
		provider, method, target string
		header                   bool
	}{
		{"s3", "PUT", "/bucket/prefix/voters/id.json", true},
		{"azure", "PUT", "/account/bucket/prefix/voters/id.json", true},
		{"gcs", "POST", "/upload/storage/v1/b/bucket/o?name=prefix%2Fvoters%2Fid.json&ifGenerationMatch=0", false},
	} {
		t.Run(tc.provider, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.target, nil)
			if tc.header {
				req.Header.Set("If-None-Match", "*")
			}
			accept := func(r *http.Request) bool {
				return preparedRegistrationRequest(r, tc.provider, "bucket", "account", "prefix/voters/id.json")
			}
			if !accept(req) {
				t.Fatal("valid request rejected")
			}
			for _, mutate := range []func(*http.Request){
				func(r *http.Request) { r.Method = http.MethodDelete },
				func(r *http.Request) { r.URL.Path = strings.Replace(r.URL.Path, "bucket", "other", 1) },
				func(r *http.Request) { r.URL.Path += "/other" },
				func(r *http.Request) {
					if tc.provider == "gcs" {
						q := r.URL.Query()
						q.Set("name", "other")
						r.URL.RawQuery = q.Encode()
					} else {
						r.URL.Path += "-other"
					}
				},
				func(r *http.Request) {
					r.Header.Del("If-None-Match")
					q := r.URL.Query()
					q.Del("ifGenerationMatch")
					r.URL.RawQuery = q.Encode()
				},
			} {
				copy := req.Clone(context.Background())
				u := *req.URL
				copy.URL = &u
				mutate(copy)
				if accept(copy) {
					t.Fatal("invalid request accepted")
				}
			}
		})
	}
}

// This uses actual source Nodes and SDK adapters, but an administrative HTTP
// authority fixture. It does not exercise Kubernetes or application admission.
func TestEmbeddedObjectStorageNodePreparedRestore(t *testing.T) {
	provider := os.Getenv("RHIZA_EMBEDDED_PROVIDER")
	if provider != "s3" && provider != "gcs" && provider != "azure" {
		t.Skip("requires an isolated Floci provider")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	upstream, err := url.Parse(embeddedEndpoint(t))
	if err != nil {
		t.Fatal(err)
	}
	var inspect atomic.Bool
	defer inspect.Store(false)
	var forbidden atomic.Int64
	var allowed atomic.Int64
	var authRequests atomic.Int64
	var learnerObject string
	var bucketName, accountName string
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if inspect.Load() && r.Method != http.MethodGet && r.Method != http.MethodHead {
			if preparedEmulatorAuthRequest(r, provider) {
				authRequests.Add(1)
				proxy.ServeHTTP(w, r)
				return
			} else if preparedRegistrationRequest(r, provider, bucketName, accountName, learnerObject) {
				allowed.Add(1)
			} else {
				forbidden.Add(1)
				http.Error(w, "unexpected mutation", http.StatusForbidden)
				return
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(proxyServer.Close)
	t.Setenv("RHIZA_EMBEDDED_ENDPOINT", proxyServer.URL)
	base := embeddedStoreConfig(t)
	bucketName, accountName = base.ObjStoreBucket, base.ObjStoreAzureStorageAccount
	base.Local = false
	base.ClusterID = "prepared-provider"
	base.ObjStoreDurability = rhiza.ObjectStoreDurabilityBeforeAck
	base.ObjStoreSyncInterval = 100 * time.Millisecond
	base.CheckpointInterval = 24 * time.Hour
	base.CheckpointTailBytes = 1 << 40
	base.EnableReconfiguration = true
	base.AdminToken = "provider-admin"
	members := make([]rhiza.Member, 3)
	tokens := []string{"provider-token-1", "provider-token-2", "provider-token-3"}
	for i := range members {
		id := fmt.Sprintf("v%d", i+1)
		members[i] = rhiza.Member{ID: quepaxa.NodeID(id), PeerURL: "quic://" + freeUDPAddr(t), PublicKey: rhiza.PeerPublicKey(base.ClusterID, id, tokens[i])}
	}
	base.Members = members
	var voters []*rhiza.DB
	t.Cleanup(func() {
		for _, db := range voters {
			if db != nil {
				if err := db.Close(); err != nil {
					t.Errorf("close source voter: %v", err)
				}
			}
		}
	})
	for i := range members {
		cfg := base
		if i == 0 {
			cfg.CheckpointInterval = time.Second
		}
		cfg.NodeID, cfg.PeerToken, cfg.PeerAddr, cfg.DataDir = string(members[i].ID), tokens[i], strings.TrimPrefix(members[i].PeerURL, "quic://"), t.TempDir()
		db, err := rhiza.Open(ctx, cfg)
		if err != nil {
			t.Fatalf("open source voter: %v", err)
		}
		voters = append(voters, db)
	}
	for _, db := range voters {
		membershipWaitReady(t, ctx, db)
	}
	if _, err := voters[0].Execute(ctx, rhiza.ExecuteRequest{RequestID: "provider-prepared-create", SQL: "CREATE TABLE prepared_source (id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	insert := rhiza.ExecuteRequest{RequestID: "provider-prepared-insert", SQL: "INSERT INTO prepared_source VALUES (7)"}
	applied, err := voters[0].Execute(ctx, insert)
	if err != nil {
		t.Fatal(err)
	}
	before, err := voters[0].Query(ctx, rhiza.QueryRequest{SQL: "SELECT id FROM prepared_source ORDER BY id", Consistency: rhiza.ConsistencyLinearizable})
	if err != nil || len(before.Rows) != 1 {
		t.Fatalf("initial row: %v %v", before.Rows, err)
	}
	resolved, err := voters[0].ResolveExecute(ctx, insert)
	if err != nil || resolved.State != rhiza.ResolveExecuteRetained || resolved.Durability != rhiza.ResolveExecuteDurabilityConfirmed || resolved.Receipt == nil || !reflect.DeepEqual(*resolved.Receipt, applied.MutationReceipt) {
		t.Fatalf("exact retained outcome: %+v %v", resolved, err)
	}
	changed := insert
	changed.SQL = "INSERT INTO prepared_source VALUES (9)"
	if _, err := voters[0].ResolveExecute(ctx, changed); !errors.Is(err, rhiza.ErrRequestConflict) {
		t.Fatalf("changed request: %v", err)
	}
	missing := insert
	missing.RequestID = "provider-prepared-absent"
	unknown, err := voters[0].ResolveExecute(ctx, missing)
	if err != nil || unknown.State != rhiza.ResolveExecuteUnknownOrExpired || unknown.Receipt != nil || unknown.Durability != rhiza.ResolveExecuteDurabilityUnknown {
		t.Fatalf("absent request: %+v %v", unknown, err)
	}
	after, err := voters[0].Query(ctx, rhiza.QueryRequest{SQL: "SELECT id FROM prepared_source ORDER BY id", Consistency: rhiza.ConsistencyLinearizable})
	if err != nil || !reflect.DeepEqual(before.Rows, after.Rows) {
		t.Fatalf("outcome lookup changed rows: %v %v", after.Rows, err)
	}
	bucket := embeddedBucket(t, base)
	readonly := &readonlyProviderBucket{Bucket: bucket}
	bootstrap := quepaxa.Cluster{ConfigID: 1, Members: members}
	prefix := path.Join(base.ObjStorePrefix, base.ClusterID)
	checkpointCtx, stopCheckpoint := context.WithTimeout(ctx, 2*time.Minute)
	defer stopCheckpoint()
	var view *recovery.ReadOnlyView
	var lastErr error
	for {
		view, lastErr = recovery.OpenReadOnlyView(checkpointCtx, readonly, prefix, 1, bootstrap)
		if lastErr == nil {
			seal, decision, ok := view.RecoveryBase()
			if ok && seal.Index == view.Base() && decision.Slot > seal.Index {
				break
			}
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-checkpointCtx.Done():
			timer.Stop()
			t.Fatalf("certified checkpoint base was not observed: %v (last read: %v)", checkpointCtx.Err(), lastErr)
		case <-timer.C:
		}
	}
	if err := voters[0].Close(); err != nil {
		t.Fatalf("stop checkpointing voter: %v", err)
	}
	voters[0] = nil
	view, lastErr = recovery.OpenReadOnlyView(checkpointCtx, readonly, prefix, 1, bootstrap)
	if lastErr != nil {
		t.Fatalf("read stable checkpoint: %v", lastErr)
	}
	if _, _, ok := view.RecoveryBase(); !ok {
		t.Fatal("stable checkpoint base missing")
	}
	checkpointBase := view.Base()
	if _, err := voters[1].Execute(ctx, rhiza.ExecuteRequest{RequestID: "provider-prepared-suffix", SQL: "INSERT INTO prepared_source VALUES (8)"}); err != nil {
		t.Fatal(err)
	}
	for {
		view, lastErr = recovery.OpenReadOnlyView(checkpointCtx, readonly, prefix, 1, bootstrap)
		found := false
		if lastErr == nil && view.Base() == checkpointBase {
			for _, decision := range view.Decisions() {
				commands, ok, err := types.DecodeSQLBatch(decision.Value)
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					for _, command := range commands {
						if command.RequestID == "provider-prepared-suffix" {
							found = true
						}
					}
				}
			}
		}
		if found {
			break
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-checkpointCtx.Done():
			timer.Stop()
			t.Fatalf("certified post-checkpoint application suffix not observed: %v (last read: %v)", checkpointCtx.Err(), lastErr)
		case <-timer.C:
		}
	}
	for i := 1; i < len(voters); i++ {
		if err := voters[i].Close(); err != nil {
			t.Fatalf("stop source voter: %v", err)
		}
		voters[i] = nil
	}
	learner := base
	learner.NodeID, learner.PeerToken, learner.PeerAddr = "learner", "provider-learner-token", freeUDPAddr(t)
	learnerKey := sha256.Sum256([]byte(learner.NodeID))
	learnerObject = path.Join(prefix, "voters", fmt.Sprintf("%x.json", learnerKey))
	learner.Learner = &rhiza.Member{ID: "learner", PublicKey: rhiza.PeerPublicKey(base.ClusterID, "learner", learner.PeerToken)}
	learner.DataDir = filepath.Join(t.TempDir(), "verified")
	sourceBinding, configDigest, err := rhiza.PreparedStartupBindingDigests(learner)
	if err != nil {
		t.Fatal(err)
	}
	identity := recovery.PreparedStartupIdentity{ClusterUID: "fixture-uid", Generation: 2, DeploymentUID: "fixture-deployment", PodUID: "fixture-pod", NodeID: learner.NodeID, SourceBinding: sourceBinding, ConfigDigest: configDigest}
	handoffDir := filepath.Join(t.TempDir(), "handoff")
	if err := recovery.PublishPreparedStartup(ctx, view, handoffDir, identity); err != nil {
		t.Fatal(err)
	}
	prepared, err := recovery.LoadPreparedStartup(ctx, readonly, prefix, 1, bootstrap, handoffDir, learner.DataDir, identity)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/ns/rhizaclusters/fixture" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","metadata":{"name":"fixture","namespace":"ns","uid":"fixture-uid","resourceVersion":"1"},"spec":{"logicalID":"logical"},"status":{"authority":{"owner":{"namespace":"ns","name":"fixture","uid":"fixture-uid","logicalID":"logical"},"generation":2,"binding":{"deploymentUID":"fixture-deployment","storageID":"storage"},"pendingWrite":false,"evidence":{"host":"fixture"}}}}`))
	}))
	t.Cleanup(server.Close)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("fixture-token"), 0600); err != nil {
		t.Fatal(err)
	}
	ref := operator.V1Alpha2ClusterRef{Name: "fixture", Namespace: "ns", UID: "fixture-uid"}
	store := &operator.V1Alpha2StatusStore{Kube: &operator.Kubernetes{BaseURL: server.URL, Namespace: "ns", TokenFile: tokenFile, Client: server.Client()}}
	authority := operator.V1Alpha2AuthorityView{Owner: operator.V1Alpha2AuthorityOwner{Namespace: "ns", Name: "fixture", UID: "fixture-uid", LogicalID: "logical"}, Generation: 2, Binding: operator.V1Alpha2AuthorityBinding{DeploymentUID: "fixture-deployment", StorageID: "storage"}}
	learner.PreparedStartup = &node.PreparedStartupConfig{Prepared: prepared, Checker: &operator.V1Alpha2PreparedStartupChecker{Store: store, Ref: ref, Authority: authority, Identity: identity}}
	inspect.Store(true)
	db, err := rhiza.Open(ctx, learner)
	if db != nil || !errors.Is(err, node.ErrPreparedWriteAdmissionUnavailable) {
		t.Fatalf("prepared open: db=%v err=%v", db != nil, err)
	}
	if _, err := os.Stat(filepath.Join(learner.DataDir, "prepared-adoption.json")); err != nil {
		t.Fatalf("adoption missing: %v", err)
	}
	learner.PreparedStartup.Prepared = nil
	learner.PreparedStartup.Identity = identity
	db, err = rhiza.Open(ctx, learner)
	if db != nil || !errors.Is(err, node.ErrPreparedWriteAdmissionUnavailable) {
		t.Fatalf("adopted WAL restart: db=%v err=%v", db != nil, err)
	}
	walDir := filepath.Join(learner.DataDir, "qlog")
	_, walLock, err := qlog.Acquire(walDir)
	if err != nil {
		t.Fatal(err)
	}
	lockHeld := true
	t.Cleanup(func() {
		if lockHeld {
			if err := walLock.Release(); err != nil {
				t.Error(err)
			}
		}
	})
	wal, err := qlog.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}
	walOpen := true
	t.Cleanup(func() {
		if walOpen {
			if err := wal.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if _, bound := wal.Identity(); !bound {
		t.Fatal("real learner WAL identity missing")
	}
	transport := network.NewTransport(types.ClusterID(base.ClusterID), quepaxa.NodeID(learner.NodeID), &bootstrap, learner.PeerToken)
	transportOpen := true
	t.Cleanup(func() {
		if transportOpen {
			if err := transport.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	core, err := quepaxa.NewLearner(quepaxa.Config{NodeID: quepaxa.NodeID(learner.NodeID), Cluster: bootstrap, WAL: wal, Transport: transport, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	prefixHash, ok := core.PrefixHash(view.Tip())
	if !ok || core.Tip() != view.Tip() || prefixHash != view.PrefixHash() {
		t.Fatal("recovered WAL does not match captured certified history")
	}
	transportOpen = false
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	walOpen = false
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := walLock.Release(); err != nil {
		t.Fatal(err)
	}
	lockHeld = false
	mat, err := materializer.Open(filepath.Join(learner.DataDir, "sqlite.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	matOpen := true
	t.Cleanup(func() {
		if matOpen {
			if err := mat.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	rows, err := mat.Query(ctx, "SELECT id FROM prepared_source ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	rowsOpen := true
	t.Cleanup(func() {
		if rowsOpen {
			if err := rows.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rowsOpen = false
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	matOpen = false
	if err := mat.Close(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != 7 || ids[1] != 8 {
		t.Fatalf("checkpoint/suffix rows: %v", ids)
	}
	if readonly.writes != 0 {
		t.Fatalf("source-reader mutation attempts: %d", readonly.writes)
	}
	inspect.Store(false)
	if forbidden.Load() != 0 || allowed.Load() == 0 {
		t.Fatalf("target mutation audit: forbidden=%d learner-registration=%d", forbidden.Load(), allowed.Load())
	}
	t.Logf("target mutation audit: learner-registration=%d emulator-auth=%d", allowed.Load(), authRequests.Load())
}
