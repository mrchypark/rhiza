//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	localobj "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestManagedRequestStartupRetainsSelectedSealAcrossLaterCertifiedQCKP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	objects := &managedTestObjectStore{objects: make(map[string][]byte)}
	server := httptest.NewServer(http.HandlerFunc(objects.serve))
	t.Cleanup(server.Close)
	source, _, _ := newKVWrapperFixture(t, localobj.Config{Provider: localobj.ProviderS3, Endpoint: strings.TrimPrefix(server.URL, "http://"), Bucket: "prepared-target", Region: "us-east-1", Insecure: true, AccessKey: "test", SecretKey: "test"})
	ack, err := source.ExecuteManagedKV(ctx, "put", network.KVMutationRequest{RequestID: "selected-seal-original", Key: "selected-seal", Value: []byte("unchanged")})
	if err != nil {
		t.Fatal("actual source quorum ACK", err)
	}
	original, found, err := source.material.RetainedRequestOriginal(ctx, types.MutationKV, "selected-seal-original")
	if err != nil || !found || original.Receipt != ack.MutationReceipt {
		t.Fatal("source original receipt", err)
	}
	files, cut, release, err := source.material.CheckpointFilesAt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	snapshots := make([]checkpoint.Source, len(files))
	for i, file := range files {
		snapshots[i] = checkpoint.Source{Role: string(file.Role), Path: file.Path}
	}
	rootA, err := source.checkpoints.CreateManagedAppendOnlyCandidate(ctx, snapshots, cut)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.archive.SyncThrough(ctx, source.core, quepaxa.Slot(cut)); err != nil {
		t.Fatal(err)
	}
	prefix, ok := source.core.PrefixHash(quepaxa.Slot(cut))
	if !ok {
		t.Fatal("actual checkpoint prefix unavailable")
	}
	next, following, err := source.core.CheckpointLeaderOrders(quepaxa.Slot(cut))
	if err != nil {
		t.Fatal(err)
	}
	sealA := quepaxa.CheckpointSeal{ConfigID: source.core.ConfigIDForSlot(quepaxa.Slot(cut)), Index: quepaxa.Slot(cut), RootHash: rootA.RootHash, StateHash: rootA.Hash, PrefixHash: prefix, NextLeaderOrder: next, FollowingLeaderOrder: following, GenerationAnchorHash: source.core.GenerationAnchorHash()}
	history, err := source.core.CheckpointMembership(sealA.Index)
	if err != nil {
		t.Fatal("capture exact historical membership for selected base", err)
	}
	sealA.Membership = &history
	valueA, err := quepaxa.EncodeCheckpointSeal(sealA)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.core.Propose(ctx, valueA); err != nil {
		t.Fatal("certify actual selected root A", err)
	}
	selected, exists, err := source.core.LatestManagedCheckpointSeal()
	if err != nil || !exists || selected.RootHash != rootA.RootHash {
		t.Fatal("actual selected QCKP unavailable", err)
	}
	decisionA, exists := source.core.CertifiedValue(selected.DecisionSlot)
	if !exists || source.core.ValidateCertifiedValue(ctx, decisionA) != nil {
		t.Fatal("actual A certificate unavailable")
	}
	if err := source.promoteManagedAppendOnlySeal(ctx, selected); err != nil {
		t.Fatal("publish verified A", err)
	}
	cluster := source.core.CurrentCluster()
	prefixPath := path.Join(source.config.ObjStorePrefix, string(source.config.ClusterID))
	membership, err := json.Marshal(recovery.NewMembershipRecord(string(source.config.ClusterID), cluster.Members, string(source.config.ObjStoreDurability)))
	if err != nil {
		t.Fatal(err)
	}
	if err := source.bucket.Upload(ctx, path.Join(prefixPath, "voters/membership.json"), bytes.NewReader(membership)); err != nil {
		t.Fatal(err)
	}
	view, err := recovery.OpenReadOnlyViewForApplicationProtocol(ctx, source.bucket, prefixPath, 1, cluster, sqlpolicy.ManagedRequestMaterializationPolicy, types.DefaultIdempotencyWindowSlots, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, source.config.EnableReconfiguration)
	if err != nil {
		t.Fatal("capture actual prepared H", err)
	}
	cfg := CloneManagedRuntimeConfig(*source.config)
	member := peerMember(string(cfg.ClusterID), "selected-learner", "selected-peer")
	member.PeerURL = "quic://127.0.0.1:0"
	cfg.NodeID, cfg.PeerToken, cfg.AdminToken = member.ID, "selected-peer", "selected-admin"
	cfg.PeerAddr, cfg.DataDir, cfg.Learner = "127.0.0.1:0", filepath.Join(t.TempDir(), "learner-data"), &member
	first := New(&cfg)
	physical, memberDigest, err := first.PreparedStartupBindingDigestsForProtocol(sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := first.ManagedBootstrapPlanDigestForProtocol(physical, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	identity := recovery.PreparedStartupIdentity{ClusterUID: "uid", DeploymentUID: "deployment", PodUID: "selected-pod", NodeID: string(member.ID), SourceBinding: physical, ConfigDigest: memberDigest}
	handoffDir := filepath.Join(t.TempDir(), "handoff")
	if err := recovery.PublishPreparedStartupForPolicy(ctx, view, handoffDir, identity, sqlpolicy.ManagedRequestMaterializationPolicy, types.DefaultIdempotencyWindowSlots); err != nil {
		t.Fatal(err)
	}
	prepared, err := recovery.LoadPreparedStartupForApplicationProtocol(ctx, source.bucket, prefixPath, 1, cluster, handoffDir, cfg.DataDir, identity, sqlpolicy.ManagedRequestMaterializationPolicy, types.DefaultIdempotencyWindowSlots, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, cfg.EnableReconfiguration)
	if err != nil {
		t.Fatal(err)
	}
	startup := ManagedStartupConfig{StorageRoot: "root", Identity: ManagedStartupIdentity{ApplicationProtocol: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, EvidenceFormat: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, AssignmentID: "assignment", IntentHash: "intent", StorageID: "storage", PhysicalBinding: physical, BootstrapPlanDigest: bootstrap, Member: identity}, Checker: &managedTestChecker{}, Prepared: prepared}
	t.Cleanup(func() {
		if err := first.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	if err := first.OpenManaged(ctx, startup); err != nil {
		t.Fatal("establish actual H adoption before reopen", err)
	}
	if err := first.Shutdown(); err != nil {
		t.Fatal(err)
	}
	reopened := New(&cfg)
	t.Cleanup(func() {
		if err := reopened.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	startup.Prepared = nil
	injected := false
	var once sync.Once
	restore := localtesthooks.Set(func(event string) {
		if event != "node:startup:managed-checkpoint-selected" {
			return
		}
		once.Do(func() {
			selectedRoot, selectedSeal, err := reopened.selectManagedAppendOnlyRoot(ctx)
			if err != nil || selectedRoot == nil || selectedSeal == nil || selectedRoot.RootHash != rootA.RootHash || !reflect.DeepEqual(*selectedSeal, selected) {
				t.Fatal("hook did not follow exact A selection", err)
			}
			// A distinct immutable root at the same application cut is a real
			// supported QCKP shape, also exercised by the prepared-H fixtures.
			snapshot, err := sql.Open("sqlite3", files[0].Path)
			if err != nil {
				t.Fatal(err)
			}
			_, editErr := snapshot.ExecContext(ctx, "PRAGMA user_version=1")
			if err := errors.Join(editErr, snapshot.Close()); err != nil {
				t.Fatal(err)
			}
			rootB, err := source.checkpoints.CreateManagedAppendOnlyCandidate(ctx, snapshots, cut)
			if err != nil || rootB.RootHash == rootA.RootHash {
				t.Fatal("distinct B root unavailable", err)
			}
			sealB := sealA
			sealB.RootHash, sealB.StateHash = rootB.RootHash, rootB.Hash
			valueB, err := quepaxa.EncodeCheckpointSeal(sealB)
			if err != nil {
				t.Fatal(err)
			}
			slotB, _, err := source.core.Propose(ctx, valueB)
			if err != nil || slotB <= selected.DecisionSlot {
				t.Fatal("certify later B with actual quorum", err)
			}
			decisionB, exists := source.core.CertifiedValue(slotB)
			if !exists || source.core.ValidateCertifiedValue(ctx, decisionB) != nil {
				t.Fatal("actual B certificate unavailable")
			}
			if err := source.archive.SyncThrough(ctx, source.core, slotB); err != nil {
				t.Fatal(err)
			}
			if err := reopened.core.AcceptCertifiedValues([]quepaxa.DecidedValue{decisionB}); err != nil {
				t.Fatal("receive actual later QCKP", err)
			}
			latest, exists, err := reopened.core.LatestManagedCheckpointSeal()
			old, oldExists := reopened.core.CertifiedValue(selected.DecisionSlot)
			oldPrefix, prefixOK := reopened.core.PrefixHash(sealA.Index)
			if err != nil || !exists || latest.RootHash != rootB.RootHash || latest.DecisionSlot != slotB || !oldExists || !reflect.DeepEqual(old, decisionA) || !prefixOK || oldPrefix != prefix {
				t.Fatal("later QCKP changed selected A certificate/prefix", err)
			}
			injected = true
		})
	})
	defer restore()
	openErr := reopened.OpenManaged(ctx, startup)
	if !injected {
		t.Fatal("post-selection QCKP injection was not reached", openErr)
	}
	if openErr != nil {
		t.Fatalf("later authentic QCKP invalidated selected exact root A: %v", openErr)
	}
	after, found, err := reopened.material.RetainedRequestOriginal(ctx, types.MutationKV, original.RequestID)
	if err != nil || !found || !reflect.DeepEqual(after, original) {
		t.Fatal("selected-root reopen changed original application/receipt", err)
	}
}
