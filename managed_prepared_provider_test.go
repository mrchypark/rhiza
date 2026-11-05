package rhiza_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	_ "github.com/ncruces/go-sqlite3/driver"
	"github.com/thanos-io/objstore"
)

// Freeze only a source-observed CURRENT hint, never archive HEAD or immutable
// objects. Each view must still verify its exact seal/root against real SDK
// reads. This preserves the test's checkpoint-base-plus-later-suffix scenario
// even when another admitted publisher advances the live discovery hint.
type managedProviderFixedCurrent struct {
	*readonlyProviderBucket
	key  string
	data []byte
	attr objstore.ObjectAttributes
}

func captureManagedProviderCurrent(ctx context.Context, bucket *readonlyProviderBucket, key string) (*managedProviderFixedCurrent, error) {
	before, err := bucket.Attributes(ctx, key)
	if err != nil {
		return nil, err
	}
	if before.Version == nil {
		return nil, fmt.Errorf("provider CURRENT lacks conditional version")
	}
	r, err := bucket.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	// Match the existing checkpoint locator reader's bounded object size.
	const limit = 64 << 10
	data, readErr := io.ReadAll(io.LimitReader(r, limit+1))
	if err := errors.Join(readErr, r.Close()); err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data) > limit {
		return nil, fmt.Errorf("provider CURRENT has invalid size")
	}
	after, err := bucket.Attributes(ctx, key)
	if err != nil {
		return nil, err
	}
	if after.Version == nil || *before.Version != *after.Version {
		return nil, fmt.Errorf("provider CURRENT changed during capture")
	}
	return &managedProviderFixedCurrent{readonlyProviderBucket: bucket, key: key, data: data, attr: after}, nil
}

func (b *managedProviderFixedCurrent) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if key != b.key {
		return b.readonlyProviderBucket.Get(ctx, key)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(b.data)), nil
}

func (b *managedProviderFixedCurrent) Attributes(ctx context.Context, key string) (objstore.ObjectAttributes, error) {
	if key != b.key {
		return b.readonlyProviderBucket.Attributes(ctx, key)
	}
	if err := ctx.Err(); err != nil {
		return objstore.ObjectAttributes{}, err
	}
	return b.attr, nil
}

// This test uses source voters and target materializers from the real Rhiza
// runtime plus each provider's SDK adapter. Its status endpoint is only a
// bounded authority fixture; it is not Kubernetes qualification.
func TestEmbeddedObjectStorageManagedPreparedCheckpointAdoption(t *testing.T) {
	provider := os.Getenv("RHIZA_EMBEDDED_PROVIDER")
	if provider != "s3" && provider != "gcs" && provider != "azure" {
		t.Skip("requires an isolated Floci provider")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	upstream, err := url.Parse(embeddedEndpoint(t))
	if err != nil {
		t.Fatal(err)
	}
	var audit atomic.Bool
	var rejectedMutations atomic.Int64
	var allowedRegistration atomic.Int64
	var bucketName, accountName, learnerObject string
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if audit.Load() && r.Method != http.MethodGet && r.Method != http.MethodHead {
			if preparedEmulatorAuthRequest(r, provider) {
				proxy.ServeHTTP(w, r)
				return
			}
			if preparedRegistrationRequest(r, provider, bucketName, accountName, learnerObject) {
				allowedRegistration.Add(1)
			} else {
				rejectedMutations.Add(1)
				http.Error(w, "unexpected object-store mutation", http.StatusForbidden)
				return
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(proxyServer.Close)
	t.Setenv("RHIZA_EMBEDDED_ENDPOINT", proxyServer.URL)
	base := embeddedStoreConfig(t)
	// embeddedStoreConfig registers bucket deletion before this cleanup, so the
	// audit is disabled before cleanup mutations reach the provider.
	t.Cleanup(func() { audit.Store(false) })
	bucketName, accountName = base.ObjStoreBucket, base.ObjStoreAzureStorageAccount
	base.Local = false
	base.BindAddr = "127.0.0.1:0"
	base.ObjStoreDurability = rhiza.ObjectStoreDurabilityBeforeAck
	base.ObjStoreSyncInterval = 100 * time.Millisecond
	base.CheckpointTailBytes = 1 << 40
	base.EnableReconfiguration = true

	const clusterUID = "managed-provider-fixture"
	const assignmentID = "provider-assignment"
	const storageRoot = "managed-provider-root"
	clusterID, err := types.ManagedClusterID(clusterUID, 0)
	if err != nil {
		t.Fatal(err)
	}
	namespace, err := node.ManagedNamespaceFor(storageRoot, clusterUID, 0, assignmentID, string(clusterID))
	if err != nil {
		t.Fatal(err)
	}
	base.ClusterID = string(clusterID)
	base.ObjStorePrefix = namespace.ConfigPrefix
	base.AdminToken = "provider-admin"

	const sourceCount = 3
	memberPods := []string{"source-pod-1", "source-pod-2", "source-pod-3"}
	peerTokens := []string{"source-token-1", "source-token-2", "source-token-3"}
	members := make([]rhiza.Member, sourceCount)
	for i := range members {
		id, err := types.ManagedNodeID(memberPods[i])
		if err != nil {
			t.Fatal(err)
		}
		members[i] = rhiza.Member{ID: id, PeerURL: "quic://" + freeUDPAddr(t), PublicKey: rhiza.PeerPublicKey(base.ClusterID, string(id), peerTokens[i])}
	}
	base.Members = members
	learner := base
	const learnerPod = "provider-learner-pod"
	learnerID, err := types.ManagedNodeID(learnerPod)
	if err != nil {
		t.Fatal(err)
	}
	learner.NodeID, learner.PeerToken, learner.PeerAddr = string(learnerID), "provider-learner-token", freeUDPAddr(t)
	learner.Learner = &rhiza.Member{ID: learnerID, PublicKey: rhiza.PeerPublicKey(base.ClusterID, string(learnerID), learner.PeerToken), PeerURL: "quic://" + learner.PeerAddr}
	learner.DataDir = filepath.Join(t.TempDir(), "managed-learner")
	learnerObject = path.Join(path.Join(base.ObjStorePrefix, string(clusterID)), "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(learner.NodeID))))
	managedProviderSeedSource(t, ctx, learner, learnerPod, assignmentID, clusterUID)
	sources := managedProviderSourceNodes(t, ctx, base, members, memberPods, peerTokens, storageRoot, assignmentID)
	t.Cleanup(func() {
		for _, db := range sources {
			if db != nil {
				if err := db.Shutdown(); err != nil {
					t.Errorf("close provider source voter: %v", err)
				}
			}
		}
	})
	if _, err := sources[0].ExecuteManagedSQL(ctx, network.ExecuteRequest{RequestID: "managed-provider-create", SQL: "CREATE TABLE managed_prepared_rows(id INTEGER PRIMARY KEY)"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sources[0].ExecuteManagedSQL(ctx, network.ExecuteRequest{RequestID: "managed-provider-base-row", SQL: "INSERT INTO managed_prepared_rows VALUES (7)"}); err != nil {
		t.Fatal(err)
	}
	bucket := embeddedBucket(t, base)
	readonly := &readonlyProviderBucket{Bucket: bucket}
	bootstrap := quepaxa.Cluster{ConfigID: 1, Members: members}
	finalPrefix := path.Join(base.ObjStorePrefix, string(clusterID))
	checkpointCtx, stopCheckpoint := context.WithTimeout(ctx, 2*time.Minute)
	defer stopCheckpoint()
	var view *recovery.ReadOnlyView
	var fixedCurrent *managedProviderFixedCurrent
	for {
		fixedCurrent, err = captureManagedProviderCurrent(checkpointCtx, readonly, path.Join(finalPrefix, "checkpoint/CURRENT"))
		if err == nil {
			view, err = recovery.OpenReadOnlyViewForApplicationProtocol(checkpointCtx, fixedCurrent, finalPrefix, 1, bootstrap, sqlpolicy.ManagedRequestMaterializationPolicy, types.DefaultIdempotencyWindowSlots, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, true)
		}
		if err == nil {
			seal, decision, ok := view.RecoveryBase()
			if ok && seal.Index == view.Base() && decision.Slot > seal.Index {
				break
			}
		}
		timer := time.NewTimer(400 * time.Millisecond)
		select {
		case <-checkpointCtx.Done():
			timer.Stop()
			t.Fatalf("certified checkpoint base not observed: %v (last view: %v)", checkpointCtx.Err(), err)
		case <-timer.C:
		}
	}
	checkpointBase := view.Base()
	checkpointSeal, checkpointDecision, _ := view.RecoveryBase()
	sourceState, err := sources[0].ManagedState()
	if err != nil || sourceState.WALIdentity == "" {
		t.Fatalf("actual source registration: %v", err)
	}
	if err := sources[0].Shutdown(); err != nil {
		t.Fatal(err)
	}
	sources[0] = nil
	suffix, err := sources[1].ExecuteManagedSQL(ctx, network.ExecuteRequest{RequestID: "managed-provider-suffix", SQL: "INSERT INTO managed_prepared_rows VALUES (8)"})
	if err != nil || suffix.Status != types.MutationCommitted || !suffix.Applied || suffix.Slot <= uint64(checkpointBase) {
		t.Fatalf("suffix lacks an applied ACK strictly after captured checkpoint: %+v %v", suffix, err)
	}
	lastSuccessfulBase, lastSuccessfulTip := view.Base(), view.Tip()
	for {
		view, err = recovery.OpenReadOnlyViewForApplicationProtocol(checkpointCtx, fixedCurrent, finalPrefix, 1, bootstrap, sqlpolicy.ManagedRequestMaterializationPolicy, types.DefaultIdempotencyWindowSlots, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, true)
		if err == nil {
			lastSuccessfulBase, lastSuccessfulTip = view.Base(), view.Tip()
		}
		if err == nil && view.Base() == checkpointBase && uint64(view.Tip()) >= suffix.Slot {
			break
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-checkpointCtx.Done():
			timer.Stop()
			t.Fatalf("certified SQL suffix not observed: %v (required base=%d suffix slot=%d; last successful base=%d tip=%d; last view: %v)", checkpointCtx.Err(), checkpointBase, suffix.Slot, lastSuccessfulBase, lastSuccessfulTip, err)
		case <-timer.C:
		}
	}
	seal, decision, hasBase := view.RecoveryBase()
	if !hasBase || seal.Index != checkpointBase || seal.RootHash != checkpointSeal.RootHash || decision.Slot != checkpointDecision.Slot || decision.Hash != checkpointDecision.Hash || !bytes.Equal(decision.Certificate, checkpointDecision.Certificate) {
		t.Fatal("suffix view replaced the exact captured checkpoint proof")
	}
	foundSuffix := false
	for _, decision := range view.Decisions() {
		if uint64(decision.Slot) != suffix.Slot {
			continue
		}
		commands, sqlValue, decodeErr := types.DecodeSQLBatch(decision.Value)
		if decodeErr != nil || !sqlValue || len(decision.Certificate) == 0 || sha256.Sum256(decision.Value) != decision.Hash {
			t.Fatal("suffix ACK lacks its exact certified SQL value", decodeErr)
		}
		for _, command := range commands {
			if command.RequestID == "managed-provider-suffix" && command.SQL == "INSERT INTO managed_prepared_rows VALUES (8)" {
				foundSuffix = true
			}
		}
	}
	if !foundSuffix {
		t.Fatal("captured checkpoint suffix lacks the original SQL at its ACK slot")
	}
	for i := range sources {
		if sources[i] != nil {
			if err := sources[i].Shutdown(); err != nil {
				t.Fatal(err)
			}
			sources[i] = nil
		}
	}
	if readonly.writes != 0 {
		t.Fatalf("provider recovery reader attempted %d source writes", readonly.writes)
	}

	prepared, plan, runtime, store, ref := managedProviderPreparedIdentity(t, ctx, learner, members, memberPods, learnerPod, storageRoot, assignmentID, view)
	origin := managedProviderRepairOrigin(t, ctx, store, ref, plan, runtime, learnerPod, storageRoot, sourceState.WALIdentity, view)
	managedConfig := managedsql.Config{StorageRoot: storageRoot, Runtime: runtime, Store: store, Ref: ref, Plan: plan, PodUID: learnerPod, Prepared: prepared, RepairOrigin: origin}
	first, err := managedsql.Open(ctx, managedConfig)
	if first != nil {
		t.Cleanup(func() {
			if err := first.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatalf("managed prepared restore through %s SDK: %v", provider, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	providerAssertSQLRows(t, ctx, learner.DataDir, uint64(view.Tip()))
	managedConfig.Prepared = nil
	second, err := managedsql.Open(ctx, managedConfig)
	if second != nil {
		t.Cleanup(func() {
			if err := second.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatalf("adopted managed restart through %s SDK: %v", provider, err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	providerAssertSQLRows(t, ctx, learner.DataDir, uint64(view.Tip()))

	mutatedPath := filepath.Join(learner.DataDir, "sqlite.db")
	mutateDB, err := sql.Open("sqlite3", (&url.URL{Scheme: "file", Path: filepath.ToSlash(mutatedPath)}).String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mutateDB.ExecContext(ctx, "UPDATE managed_prepared_rows SET id=70 WHERE id=7"); err != nil {
		_ = mutateDB.Close()
		t.Fatal(err)
	}
	if err := mutateDB.Close(); err != nil {
		t.Fatal(err)
	}
	beforeCorrupt, err := providerDataTreeDigest(learner.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	beforeRejectedWrites, beforeAllowed := rejectedMutations.Load(), allowedRegistration.Load()
	audit.Store(true)
	corruptHost, corruptErr := managedsql.Open(ctx, managedConfig)
	if corruptHost != nil {
		if err := corruptHost.Close(); err != nil {
			t.Errorf("close rejected corrupt managed host: %v", err)
		}
	}
	if corruptErr == nil || !strings.Contains(corruptErr.Error(), "actual SQLite contents differ") {
		t.Fatalf("same-tip SQL corruption result: %v", corruptErr)
	}
	if after, err := providerDataTreeDigest(learner.DataDir); err != nil || after != beforeCorrupt {
		t.Fatalf("corrupt prepared restart changed local WAL/SQLite/adoption: before=%s after=%s err=%v", beforeCorrupt, after, err)
	}
	if got, want := rejectedMutations.Load(), beforeRejectedWrites; got != want {
		t.Fatalf("corrupt prepared restart attempted provider writes: before=%d after=%d", want, got)
	}
	if got, want := allowedRegistration.Load(), beforeAllowed; got != want {
		t.Fatalf("corrupt prepared restart attempted voter registration: before=%d after=%d", want, got)
	}
	audit.Store(false)
	mutateDB, err = sql.Open("sqlite3", (&url.URL{Scheme: "file", Path: filepath.ToSlash(mutatedPath)}).String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mutateDB.ExecContext(ctx, "UPDATE managed_prepared_rows SET id=7 WHERE id=70"); err != nil {
		_ = mutateDB.Close()
		t.Fatal(err)
	}
	if err := mutateDB.Close(); err != nil {
		t.Fatal(err)
	}
	last, err := managedsql.Open(ctx, managedConfig)
	if last != nil {
		t.Cleanup(func() {
			if err := last.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatalf("valid managed neighbor after corruption check: %v", err)
	}
	if err := last.Close(); err != nil {
		t.Fatal(err)
	}
	providerAssertSQLRows(t, ctx, learner.DataDir, uint64(view.Tip()))
	t.Logf("provider=%s SDK exercised; authority used an HTTP fixture, not Kubernetes", provider)
}

func managedProviderSourceNodes(t *testing.T, ctx context.Context, base rhiza.Config, members []rhiza.Member, pods, tokens []string, root, assignmentID string) []*node.Node {
	t.Helper()
	ref := operator.V1Alpha2ClusterRef{Name: "fixture", Namespace: "ns", UID: "managed-provider-fixture"}
	runtime := managedProviderRuntime(base)
	n := node.New(&runtime)
	physical, _, err := n.PreparedStartupBindingDigestsForProtocol(sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := n.ManagedBootstrapPlanDigestForProtocol(physical, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := operator.NewV1Alpha2ApplicationInitPlan(ref, "managed-provider-logical", "managed-provider-deployment", assignmentID, physical, digest, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	plan.ApplicationProtocol = sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	bootstrap := operator.V1Alpha2BootstrapPlan{ClusterID: runtime.ClusterID, PhysicalBinding: physical, EvidenceFormat: plan.EvidenceFormat, Durability: types.ObjectStoreDurabilityBeforeAck, Reconfiguration: true}
	for i, member := range members {
		bootstrap.Members = append(bootstrap.Members, operator.V1Alpha2BootstrapMember{PodUID: pods[i], Member: member})
	}
	store, deployment := managedProviderStatusStore(t, ref, plan.ApplicationProtocol)
	if _, err := operator.InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, ref, deployment, plan, bootstrap); err != nil {
		t.Fatal(err)
	}
	nodes := make([]*node.Node, len(members))
	t.Cleanup(func() {
		for _, n := range nodes {
			if n != nil {
				if err := n.Shutdown(); err != nil {
					t.Error(err)
				}
			}
		}
	})
	for i, member := range members {
		cfg := managedProviderRuntime(base)
		cfg.NodeID, cfg.PeerToken, cfg.PeerAddr, cfg.BindAddr = member.ID, tokens[i], strings.TrimPrefix(member.PeerURL, "quic://"), ""
		cfg.DataDir = filepath.Join(t.TempDir(), "managed-source")
		cfg.CheckpointInterval = 24 * time.Hour
		if i == 0 {
			cfg.CheckpointInterval = time.Second
		}
		nodes[i] = node.New(&cfg)
		_, memberDigest, err := nodes[i].PreparedStartupBindingDigestsForProtocol(plan.ApplicationProtocol)
		if err != nil {
			t.Fatal(err)
		}
		identity := node.ManagedStartupIdentity{ApplicationProtocol: plan.ApplicationProtocol, EvidenceFormat: plan.EvidenceFormat, AssignmentID: plan.AssignmentID, IntentHash: plan.IntentHash, StorageID: plan.StorageID, PhysicalBinding: physical, BootstrapPlanDigest: digest, Fresh: true, Member: recovery.PreparedStartupIdentity{ClusterUID: ref.UID, DeploymentUID: plan.DeploymentUID, PodUID: pods[i], NodeID: string(member.ID), SourceBinding: physical, ConfigDigest: memberDigest}}
		checker := &mixedProviderNodeChecker{n: nodes[i], store: store, ref: ref, plan: plan, identity: identity}
		if _, err := nodes[i].PrepareManagedSQLSource(ctx, identity, true); err != nil {
			t.Fatal(err)
		}
		if err := nodes[i].OpenManaged(ctx, node.ManagedStartupConfig{StorageRoot: root, PeerRecovery: true, Identity: identity, Checker: checker, BootstrapMember: true}); err != nil {
			t.Fatal(err)
		}
	}
	initial, err := operator.LoadV1Alpha2ApplicationInit(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	state, err := nodes[0].InspectManagedKV(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := operator.CommitV1Alpha2ApplicationInit(ctx, store, initial, plan, mixedProviderEvidence(state)); err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		for {
			// Application admission does not set the catch-up/peer-ready flag
			// consumed by the real Server. Wait for both before the first write.
			if err := n.AdmitManaged(ctx); err == nil && n.Ready() {
				break
			}
			timer := time.NewTimer(30 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				t.Fatal(ctx.Err())
			case <-timer.C:
			}
		}
	}
	return nodes
}

func managedProviderSeedSource(t *testing.T, ctx context.Context, cfg rhiza.Config, podUID, assignmentID, clusterUID string) {
	t.Helper()
	runtime := managedProviderRuntime(cfg)
	n := node.New(&runtime)
	physical, member, err := n.PreparedStartupBindingDigestsForProtocol(sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := n.ManagedBootstrapPlanDigestForProtocol(physical, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	ref := operator.V1Alpha2ClusterRef{Name: "fixture", Namespace: "ns", UID: clusterUID}
	plan, err := operator.NewV1Alpha2ApplicationInitPlan(ref, "managed-provider-logical", "managed-provider-deployment", assignmentID, physical, bootstrap, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	memberIdentity := recovery.PreparedStartupIdentity{ClusterUID: clusterUID, Generation: 0, DeploymentUID: plan.DeploymentUID, PodUID: podUID, NodeID: string(runtime.NodeID), SourceBinding: physical, ConfigDigest: member}
	identity := node.ManagedStartupIdentity{ApplicationProtocol: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, Member: memberIdentity, AssignmentID: assignmentID, IntentHash: plan.IntentHash, StorageID: plan.StorageID, PhysicalBinding: physical, BootstrapPlanDigest: bootstrap, EvidenceFormat: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, Fresh: true}
	if empty, err := n.PrepareManagedSQLSource(ctx, identity, true); err != nil || !empty {
		t.Fatalf("seed exact managed SQL source identity: empty=%v err=%v", empty, err)
	}
}

func providerAssertSQLRows(t *testing.T, ctx context.Context, dataDir string, expectedTip uint64) {
	t.Helper()
	dbPath := filepath.Join(dataDir, "sqlite.db")
	state, tip, err := materializer.InspectManagedAppendOnlyRequestFile(ctx, dbPath, types.DefaultIdempotencyWindowSlots)
	if err != nil || tip != expectedTip || state.FreshEmpty {
		t.Fatalf("restored SQLite evidence state=%+v tip=%d want=%d err=%v", state, tip, expectedTip, err)
	}
	tmp, err := os.MkdirTemp(t.TempDir(), "actual-sql-read-")
	if err != nil {
		t.Fatal(err)
	}
	privateDB := filepath.Join(tmp, "application.sqlite")
	if err := copyProviderFile(dbPath, privateDB); err != nil {
		t.Fatal(err)
	}
	if err := copyProviderFileIfPresent(dbPath+"-wal", privateDB+"-wal"); err != nil {
		t.Fatal(err)
	}
	dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(privateDB)}).String() + "?mode=ro"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, "SELECT id FROM managed_prepared_rows ORDER BY id")
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			_ = db.Close()
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := errors.Join(rows.Err(), rows.Close(), db.Close()); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != 7 || ids[1] != 8 {
		t.Fatalf("restored checkpoint+certified suffix rows=%v", ids)
	}
}

func copyProviderFile(source, destination string) (resultErr error) {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, in.Close()) }()
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	return errors.Join(copyErr, out.Close())
}

func copyProviderFileIfPresent(source, destination string) error {
	if _, err := os.Stat(source); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	return copyProviderFile(source, destination)
}

func managedProviderPreparedIdentity(t *testing.T, ctx context.Context, cfg rhiza.Config, members []rhiza.Member, pods []string, learnerPod, storageRoot, assignmentID string, view *recovery.ReadOnlyView) (*recovery.VerifiedPreparedStartup, operator.V1Alpha2ApplicationInitPlan, managedsql.RuntimeConfig, *operator.V1Alpha2StatusStore, operator.V1Alpha2ClusterRef) {
	t.Helper()
	runtime := managedProviderRuntime(cfg)
	managedNode := node.New(&runtime)
	physicalBinding, memberDigest, err := managedNode.PreparedStartupBindingDigestsForProtocol(sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	commonDigest, err := managedNode.ManagedBootstrapPlanDigestForProtocol(physicalBinding, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	ref := operator.V1Alpha2ClusterRef{Name: "fixture", Namespace: "ns", UID: "managed-provider-fixture"}
	plan, err := operator.NewV1Alpha2ApplicationInitPlan(ref, "managed-provider-logical", "managed-provider-deployment", assignmentID, physicalBinding, commonDigest, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	plan.ApplicationProtocol = sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	bootstrap := operator.V1Alpha2BootstrapPlan{ClusterID: runtime.ClusterID, PhysicalBinding: physicalBinding, EvidenceFormat: plan.EvidenceFormat, Durability: types.ObjectStoreDurabilityBeforeAck, Reconfiguration: runtime.EnableReconfiguration}
	for i, member := range members {
		bootstrap.Members = append(bootstrap.Members, operator.V1Alpha2BootstrapMember{PodUID: pods[i], Member: member})
	}
	store, deploymentName := managedProviderStatusStore(t, ref, plan.ApplicationProtocol)
	if _, err := operator.InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, ref, deploymentName, plan, bootstrap); err != nil {
		t.Fatal(err)
	}
	initial, err := operator.LoadV1Alpha2ApplicationInit(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := managedProviderInitialEvidence(t, ctx, view)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := operator.CommitV1Alpha2ApplicationInit(ctx, store, initial, plan, evidence); err != nil {
		t.Fatal(err)
	}
	identity := recovery.PreparedStartupIdentity{ClusterUID: ref.UID, Generation: 0, DeploymentUID: plan.DeploymentUID, PodUID: learnerPod, NodeID: string(runtime.NodeID), SourceBinding: physicalBinding, ConfigDigest: memberDigest}
	handoffDir := filepath.Join(t.TempDir(), "managed-provider-handoff")
	if err := recovery.PublishPreparedStartupForPolicy(ctx, view, handoffDir, identity, sqlpolicy.ManagedRequestMaterializationPolicy, types.DefaultIdempotencyWindowSlots); err != nil {
		t.Fatal(err)
	}
	bucket := embeddedBucket(t, cfg)
	prepared, err := recovery.LoadPreparedStartupForApplicationProtocol(ctx, bucket, path.Join(cfg.ObjStorePrefix, string(runtime.ClusterID)), 1, quepaxa.Cluster{ConfigID: 1, Members: members}, handoffDir, runtime.DataDir, identity, sqlpolicy.ManagedRequestMaterializationPolicy, types.DefaultIdempotencyWindowSlots, plan.ApplicationProtocol, true)
	if err != nil {
		t.Fatal(err)
	}
	return prepared, plan, runtime, store, ref
}

type managedProviderRepairTransport struct {
	base      http.RoundTripper
	resources map[string]any
}

func (r *managedProviderRepairTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if object, ok := r.resources[req.URL.Path]; ok && req.Method == http.MethodGet {
		raw, err := json.Marshal(object)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(raw)), Request: req}, nil
	}
	return r.base.RoundTrip(req)
}

// Only the bounded Kubernetes owner chain is simulated. H, its prefix hash,
// checkpoint policy and the old registration nonce come from actual SDK data.
func managedProviderRepairOrigin(t *testing.T, ctx context.Context, store *operator.V1Alpha2StatusStore, ref operator.V1Alpha2ClusterRef, assignment operator.V1Alpha2ApplicationInitPlan, runtime managedsql.RuntimeConfig, pod, root, oldWAL string, view *recovery.ReadOnlyView) []byte {
	t.Helper()
	old := runtime.Members[0].ID
	ids := make([]types.NodeID, 0, len(runtime.Members))
	for _, member := range runtime.Members {
		if member.ID != old {
			ids = append(ids, member.ID)
		}
	}
	ids = append(ids, runtime.NodeID)
	slices.Sort(ids)
	plan := operator.V1Alpha2RepairPlan{OperationID: "provider-prepared-repair", Sequence: operator.V1Alpha2RepairAddThenRemove, ClusterUID: ref.UID, DeploymentUID: assignment.DeploymentUID, StorageID: assignment.StorageID, ExpectedConfigID: 1, OldNodeID: old, OldWALIdentity: oldWAL, Replacement: operator.V1Alpha2RepairMember{PodName: pod, PodUID: pod, NodeID: runtime.NodeID, PublicKey: runtime.Learner.PublicKey, PeerURL: runtime.Learner.PeerURL}, BootstrapPlanDigest: assignment.BootstrapPlanDigest, FinalVoterIDs: ids}
	deployment, err := operator.V1Alpha2GenesisDeploymentName(ref.UID)
	if err != nil {
		t.Fatal(err)
	}
	owner := func(kind, name, uid string) []any {
		return []any{map[string]any{"apiVersion": "apps/v1", "kind": kind, "name": name, "uid": uid, "controller": true}}
	}
	report, err := json.Marshal(map[string]any{"version": 1, "publicKey": base64.StdEncoding.EncodeToString(runtime.Learner.PublicKey[:])})
	if err != nil {
		t.Fatal(err)
	}
	resources := map[string]any{
		"/api/v1/namespaces/" + ref.Namespace + "/pods/" + pod: map[string]any{
			"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"name": pod, "namespace": ref.Namespace, "uid": pod, "resourceVersion": "1", "ownerReferences": owner("ReplicaSet", "provider-rs", "provider-rs-uid")},
			"status":   map[string]any{"podIP": "127.0.0.1", "containerStatuses": []any{map[string]any{"name": "sql", "restartCount": 0, "state": map[string]any{"waiting": map[string]any{}}}}, "initContainerStatuses": []any{map[string]any{"name": operator.V1Alpha2PrepareIdentityContainer, "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": string(report)}}}}},
		},
		"/apis/apps/v1/namespaces/" + ref.Namespace + "/replicasets/provider-rs": map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": map[string]any{"name": "provider-rs", "namespace": ref.Namespace, "uid": "provider-rs-uid", "resourceVersion": "1", "ownerReferences": owner("Deployment", deployment, assignment.DeploymentUID)}},
	}
	client := *store.Kube.Client
	client.Transport = &managedProviderRepairTransport{base: client.Transport, resources: resources}
	store.Kube.Client = &client
	admission, err := operator.LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	closed, _, err := operator.BeginV1Alpha2Repair(ctx, store, admission, admission.Authority(), plan)
	if err != nil {
		t.Fatal(err)
	}
	head := view.CapturedHead()
	source := quepaxa.Cluster{ConfigID: 1, Members: runtime.Members}
	handoff := operator.V1Alpha2RepairHandoff{PlanDigest: plan.Digest(), SourceRoot: root, SourcePrefix: path.Join(runtime.ObjStorePrefix, string(runtime.ClusterID)), PhysicalBinding: assignment.PhysicalBinding, Head: head, HeadHash: fmt.Sprintf("%x", sha256.Sum256(head)), PreparedConfig: &source, PreparedSlot: uint64(view.Tip()), PreparedHash: fmt.Sprintf("%x", view.PrefixHash())}
	captured, _, err := operator.CaptureV1Alpha2RepairHandoff(ctx, store, closed, closed.Authority(), handoff)
	if err != nil {
		t.Fatal(err)
	}
	repair, ok := captured.Repair()
	if !ok {
		t.Fatal("captured provider repair absent")
	}
	binding, err := operator.ValidateV1Alpha2RepairPodBinding(ctx, store, ref, plan)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(struct {
		Version int                               `json:"version"`
		Repair  operator.V1Alpha2RepairRecord     `json:"repair"`
		Binding operator.V1Alpha2RepairPodBinding `json:"binding"`
	}{1, repair, binding})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func managedProviderInitialEvidence(t *testing.T, ctx context.Context, view *recovery.ReadOnlyView) (json.RawMessage, error) {
	t.Helper()
	tmp := t.TempDir()
	checkpointDir := filepath.Join(tmp, "checkpoint")
	if err := os.Mkdir(checkpointDir, 0700); err != nil {
		return nil, err
	}
	checkpointFiles, err := view.DownloadCheckpointFiles(ctx, checkpointDir)
	if err != nil {
		return nil, fmt.Errorf("download verified provider checkpoint: %w", err)
	}
	if len(checkpointFiles) == 0 {
		return nil, fmt.Errorf("verified provider view has no checkpoint files")
	}
	files := make([]materializer.CheckpointFile, 0, len(checkpointFiles))
	for _, file := range checkpointFiles {
		files = append(files, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
	}
	expected, err := materializer.OpenManagedSQLKVWithAppendOnlyOriginals(filepath.Join(tmp, "expected.sqlite"), 4)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := expected.Close(); err != nil {
			t.Errorf("close expected provider SQL materializer: %v", err)
		}
	}()
	if err := expected.RestoreCheckpoint(ctx, files); err != nil {
		return nil, fmt.Errorf("restore expected provider checkpoint: %w", err)
	}
	last := view.Base()
	var suffix []quepaxa.DecidedValue
	for _, decision := range view.Decisions() {
		if decision.Slot <= view.Base() {
			continue
		}
		if decision.Slot != last+1 {
			return nil, fmt.Errorf("provider view suffix gap at slot %d", last+1)
		}
		suffix = append(suffix, decision)
		last = decision.Slot
	}
	if last != view.Tip() {
		return nil, fmt.Errorf("provider view suffix ends at %d, want %d", last, view.Tip())
	}
	if err := expected.ApplyBatch(ctx, suffix); err != nil {
		return nil, fmt.Errorf("apply verified provider SQL suffix: %w", err)
	}
	if expected.Tip() != uint64(view.Tip()) {
		return nil, fmt.Errorf("expected provider materializer tip %d differs from view %d", expected.Tip(), view.Tip())
	}
	state, err := expected.InspectManagedApplicationState(ctx)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Format string `json:"format"`
		Digest string `json:"digest"`
	}{Format: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, Digest: fmt.Sprintf("%x", state.Digest)})
}

func managedProviderRuntime(cfg rhiza.Config) managedsql.RuntimeConfig {
	return managedsql.RuntimeConfig{
		Local: cfg.Local, ClusterID: types.ClusterID(cfg.ClusterID), NodeID: types.NodeID(cfg.NodeID), DataDir: cfg.DataDir,
		BindAddr: cfg.BindAddr, PeerAddr: cfg.PeerAddr, AdminToken: cfg.AdminToken, PeerToken: cfg.PeerToken,
		Members: cfg.Members, EnableReconfiguration: cfg.EnableReconfiguration, Learner: cfg.Learner,
		ObjStoreEndpoint: cfg.ObjStoreEndpoint, ObjStoreBucket: cfg.ObjStoreBucket, ObjStoreProvider: cfg.ObjStoreProvider,
		ObjStoreDir: cfg.ObjStoreDir, ObjStorePrefix: cfg.ObjStorePrefix, ObjStoreRegion: cfg.ObjStoreRegion,
		ObjStoreInsecure: cfg.ObjStoreInsecure, ObjStoreRetries: cfg.ObjStoreRetries, ObjStoreAccessKey: cfg.ObjStoreAccessKey,
		ObjStoreSecretKey: cfg.ObjStoreSecretKey, ObjStoreSessionToken: cfg.ObjStoreSessionToken, ObjStoreServiceAccount: cfg.ObjStoreServiceAccount,
		ObjStoreAzureTenantID: cfg.ObjStoreAzureTenantID, ObjStoreAzureClientID: cfg.ObjStoreAzureClientID,
		ObjStoreAzureClientSecret: cfg.ObjStoreAzureClientSecret, ObjStoreAzureStorageAccount: cfg.ObjStoreAzureStorageAccount,
		ObjStoreAzureStorageAccountKey: cfg.ObjStoreAzureStorageAccountKey, ObjStoreAzureConnectionString: cfg.ObjStoreAzureConnectionString,
		ObjStoreAzureUserAssignedID: cfg.ObjStoreAzureUserAssignedID, ObjStoreDurability: types.ObjectStoreDurability(cfg.ObjStoreDurability),
		ObjStoreSyncInterval: cfg.ObjStoreSyncInterval, ObjStoreBatchDelay: cfg.ObjStoreBatchDelay,
		ObjStoreGCInterval: cfg.ObjStoreGCInterval, ObjStoreGCGracePeriod: cfg.ObjStoreGCGracePeriod,
		CheckpointInterval: cfg.CheckpointInterval, CheckpointTailBytes: cfg.CheckpointTailBytes,
		CheckpointRetainCount: cfg.CheckpointRetainCount, MaxWALBytes: cfg.MaxWALBytes,
		MaxConcurrentReads: cfg.MaxConcurrentReads, MaxLongPollReads: cfg.MaxLongPollReads,
		LocalGraphNodePropertyIndexes: cfg.LocalGraphNodePropertyIndexes,
	}
}

type managedProviderStatusFixture struct {
	mu         sync.Mutex
	cluster    map[string]any
	deployment map[string]any
}

func (s *managedProviderStatusFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if strings.Contains(r.URL.Path, "/deployments/") {
		_ = json.NewEncoder(w).Encode(s.deployment)
		return
	}
	if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/status") {
		var next map[string]any
		if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
			http.Error(w, "invalid status update", http.StatusBadRequest)
			return
		}
		oldMeta, _ := s.cluster["metadata"].(map[string]any)
		newMeta, _ := next["metadata"].(map[string]any)
		if oldMeta == nil || newMeta == nil || oldMeta["resourceVersion"] != newMeta["resourceVersion"] {
			w.WriteHeader(http.StatusConflict)
			return
		}
		version, _ := oldMeta["resourceVersion"].(string)
		var number int
		_, _ = fmt.Sscan(version, &number)
		newMeta["resourceVersion"] = fmt.Sprint(number + 1)
		s.cluster = next
	}
	_ = json.NewEncoder(w).Encode(s.cluster)
}

func managedProviderStatusStore(t *testing.T, ref operator.V1Alpha2ClusterRef, protocol ...string) (*operator.V1Alpha2StatusStore, string) {
	t.Helper()
	if len(protocol) != 1 || protocol[0] != sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol && protocol[0] != sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol {
		t.Fatal("unsupported provider fixture application protocol")
	}
	deploymentName, err := operator.V1Alpha2GenesisDeploymentName(ref.UID)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &managedProviderStatusFixture{
		cluster: map[string]any{
			"apiVersion": "rhiza.mrchypark.dev/v1alpha2", "kind": "RhizaCluster",
			"metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace, "uid": ref.UID, "resourceVersion": "1"},
			"spec":     map[string]any{"logicalID": "managed-provider-logical", "genesisIntent": "Create", "voterCount": 3, "workload": map[string]any{"podTemplate": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "fixture"}}}}}},
			"status":   map[string]any{},
		},
		deployment: map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": deploymentName, "namespace": ref.Namespace, "uid": "managed-provider-deployment", "resourceVersion": "1", "labels": map[string]string{"rhiza.mrchypark.dev/cluster-uid": ref.UID, "rhiza.mrchypark.dev/generation": "0"}, "ownerReferences": []any{map[string]any{"apiVersion": "rhiza.mrchypark.dev/v1alpha2", "kind": "RhizaCluster", "name": ref.Name, "uid": ref.UID, "controller": true}}},
		},
	}
	fixture.cluster["spec"].(map[string]any)["applicationProtocol"] = protocol[0]
	server := httptest.NewTLSServer(fixture)
	t.Cleanup(server.Close)
	tokenFile := filepath.Join(t.TempDir(), "managed-provider-token")
	if err := os.WriteFile(tokenFile, []byte("fixture-token"), 0600); err != nil {
		t.Fatal(err)
	}
	store := &operator.V1Alpha2StatusStore{Kube: &operator.Kubernetes{BaseURL: server.URL, Namespace: ref.Namespace, TokenFile: tokenFile, Client: server.Client()}}
	return store, deploymentName
}

func providerDataTreeDigest(root string) (string, error) {
	hash := sha256.New()
	err := filepath.WalkDir(root, func(name string, entry os.DirEntry, walkErr error) error {
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
			return fmt.Errorf("unexpected non-regular data file %s", name)
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		_, _ = io.WriteString(hash, relative+"\x00")
		_, _ = hash.Write(data)
		return nil
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
