package operator

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type pausePendingIdentityUploadBucket struct {
	objstore.Bucket
	key     string
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *pausePendingIdentityUploadBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	if name == b.key {
		b.once.Do(func() { close(b.reached) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.Bucket.Upload(ctx, name, reader, options...)
}

func TestPendingIdentityLateUploadCannotMutateClosedAuthority(t *testing.T) {
	runAuthenticatedLearnerPromotionRetriesExactCanonicalCompletion(t, func(t *testing.T, fixture authenticatedPendingLearnerSetup) {
		ctx := fixture.ctx
		f := fixture.f
		id := fixture.grant.Identity
		id.WALIdentity = fixture.intent.LearnerWAL
		if _, err := fixture.client.Bind(ctx, id); err != nil {
			t.Fatalf("bind canonical pending learner WAL before identity upload: %v", err)
		}

		var pod object
		if err := f.e.Kube.Get(ctx, f.e.controller().corePath("pods", id.PodName), &pod); err != nil {
			t.Fatal(err)
		}
		ctr := asObject(list(nested(pod, "spec", "containers"))[0])
		env, err := f.e.controller().environment(ctx, ctr)
		if err != nil {
			t.Fatal(err)
		}
		members, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
		if err != nil {
			t.Fatal(err)
		}
		durability := env["RHIZA_OBJSTORE_DURABILITY"]
		if durability == "" {
			durability = "async"
		}
		memberRecord := recovery.NewMembershipRecord(id.SourceClusterID, members, durability)
		storeData, err := json.Marshal([]string{env["RHIZA_OBJSTORE_PROVIDER"], env["RHIZA_OBJSTORE_ENDPOINT"], env["RHIZA_OBJSTORE_BUCKET"], path.Clean(env["RHIZA_OBJSTORE_PREFIX"]), env["RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT"]})
		if err != nil {
			t.Fatal(err)
		}
		storeDigest := sha256.Sum256(storeData)
		var secret object
		if err := f.e.Kube.Get(ctx, f.e.controller().corePath("secrets", fixture.intent.LearnerSecretName), &secret); err != nil {
			t.Fatal(err)
		}
		memberBytes, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "member")))
		if err != nil {
			t.Fatal(err)
		}
		var member quepaxa.Member
		if err := json.Unmarshal(memberBytes, &member); err != nil {
			t.Fatal(err)
		}
		record := learnerVoterIdentityRecord{Version: 2, Cluster: id.SourceClusterID, Node: id.NodeID,
			Membership: memberRecord.Membership, Store: hex.EncodeToString(storeDigest[:]), Nonce: id.WALIdentity,
			Registered: true, LearnerPublicKey: fmt.Sprintf("%x", member.PublicKey[:])}
		data, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		identityKey := path.Join(env["RHIZA_OBJSTORE_PREFIX"], id.SourceClusterID, "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(id.NodeID))))
		beforeArchive := pendingIdentityBucketSnapshot(t, ctx, f.e.ArchiveBucket)
		if _, exists := beforeArchive[identityKey]; exists {
			t.Fatal("pending learner identity object existed before the delayed upload")
		}

		pause := &pausePendingIdentityUploadBucket{Bucket: f.e.ArchiveBucket, key: identityKey, reached: make(chan struct{}), release: make(chan struct{})}
		f.e.ArchiveBucket = pause
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(pause.release) }) }
		defer release()
		uploadResult := make(chan error, 1)
		go func() {
			uploadResult <- fixture.client.RegisterLearnerIdentity(ctx, id, fencing.LearnerIdentityRegistration{Key: identityKey, Data: data})
		}()
		select {
		case <-pause.reached:
		case <-time.After(10 * time.Second):
			t.Fatal("identity upload did not pause after canonical open-root checks")
		}

		collectPrior, verifyPrior := f.e.CollectRuntimeEvidence, f.e.VerifyRuntimeEvidence
		f.e.CollectRuntimeEvidence = func(ctx context.Context, runtime fencing.RuntimeBinding, stop fencing.StopRequest) ([]byte, error) {
			if runtime.RunID != id.RunID || stop.Identity != id {
				return collectPrior(ctx, runtime, stop)
			}
			return []byte("verified-late-upload-terminal:" + id.RunID), nil
		}
		f.e.VerifyRuntimeEvidence = func(ctx context.Context, runtime fencing.RuntimeBinding, stop fencing.StopRequest, receipt []byte) error {
			if runtime.RunID != id.RunID || stop.Identity != id || string(receipt) != "verified-late-upload-terminal:"+id.RunID {
				return verifyPrior(ctx, runtime, stop, receipt)
			}
			return nil
		}
		request := FenceRequest{LogicalID: fixture.cluster.Spec.LogicalID, Namespace: f.e.Kube.Namespace,
			BindingUID: fixture.state.BindingUID, StatefulSet: fixture.cluster.Spec.StatefulSet,
			StatefulSetUID: f.runtime.StatefulSetUID, SourceClusterID: f.runtime.SourceClusterID,
			OperationID: "late-pending-identity-close", Scope: ScopeVoter,
			Targets: []FenceTarget{{NodeID: id.NodeID, Pod: id.PodName, PodUID: id.PodUID, WALIdentity: id.WALIdentity}}}
		fence := &fenceResource{Metadata: object{"name": "late-pending-identity-close", "namespace": f.e.Kube.Namespace, "uid": "late-pending-identity-close-uid"}, Spec: request}
		if err := f.e.reconcileFence(ctx, v2FenceEndpoint(), fence); err != nil {
			t.Fatalf("close the exact pending learner through the canonical fence path: %v", err)
		}
		if fence.Status.Proof == nil || fence.Status.Proof.IndependentV2 == nil || len(fence.Status.Proof.IndependentV2.Terminals) != 1 {
			t.Fatalf("pending learner closure did not retain exact independent terminal evidence: %+v", fence.Status.Proof)
		}
		terminal := fence.Status.Proof.IndependentV2.Terminals[0]
		if terminal.EvidenceVersion != 3 || terminal.ProducerUID != "" || terminal.MutationReceiptDigest != "" || terminal.Request.Identity != id {
			t.Fatalf("pending terminal proof changed identity or gained producer authority: %+v", terminal)
		}

		closedRoot, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
		if err != nil {
			t.Fatal(err)
		}
		closedControl, _, err := fixture.controller.loadAuto(ctx, fixture.controller.autoKey(&fixture.cluster))
		if err != nil {
			t.Fatal(err)
		}
		var closedChild Resource
		if err := f.e.Kube.Get(ctx, fixture.controller.resourcePath(fixture.intent.RecoveryName), &closedChild); err != nil {
			t.Fatal(err)
		}
		closedArchive := pendingIdentityBucketSnapshot(t, ctx, f.e.ArchiveBucket)
		if hashJSON(beforeArchive) != hashJSON(closedArchive) {
			t.Fatal("source archive changed before the paused identity upload resumed")
		}

		if _, err := fixture.client.RegisterStartup(ctx, fixture.physicalID); err == nil {
			t.Fatal("closed pending identity was re-registered")
		}
		if _, err := fixture.client.Bind(ctx, id); !errors.Is(err, fencing.ErrClosed) {
			t.Fatalf("closed pending identity WAL bind error=%v, want ErrClosed", err)
		}
		if _, err := fixture.client.PromoteLearner(ctx, id); !errors.Is(err, fencing.ErrClosed) {
			t.Fatalf("closed pending identity promotion error=%v, want ErrClosed", err)
		}
		stillClosedRoot, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
		if err != nil || hashJSON(closedRoot) != hashJSON(stillClosedRoot) {
			t.Fatalf("stale Register/Bind/Promote changed the closed canonical root: err=%v", err)
		}

		release()
		select {
		case err := <-uploadResult:
			if err != nil {
				t.Fatalf("delayed create-only pending identity upload failed: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("delayed identity upload did not finish after release")
		}
		afterArchive := pendingIdentityBucketSnapshot(t, ctx, f.e.ArchiveBucket)
		if len(afterArchive) != len(beforeArchive)+1 || afterArchive[identityKey] == nil || string(afterArchive[identityKey]) != string(data) {
			t.Fatalf("late identity upload changed more than its exact immutable key: before=%d after=%d", len(beforeArchive), len(afterArchive))
		}
		for key, want := range beforeArchive {
			if string(afterArchive[key]) != string(want) {
				t.Fatalf("late identity upload changed unrelated source object %q", key)
			}
		}
		afterRoot, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
		if err != nil || hashJSON(closedRoot) != hashJSON(afterRoot) {
			t.Fatalf("late identity upload changed closed authority root: err=%v", err)
		}
		afterControl, _, err := fixture.controller.loadAuto(ctx, fixture.controller.autoKey(&fixture.cluster))
		if err != nil || hashJSON(closedControl) != hashJSON(afterControl) {
			t.Fatalf("late identity upload changed canonical target generation: err=%v", err)
		}
		var afterChild Resource
		if err := f.e.Kube.Get(ctx, fixture.controller.resourcePath(fixture.intent.RecoveryName), &afterChild); err != nil || hashJSON(closedChild) != hashJSON(afterChild) {
			t.Fatalf("late identity upload changed recovery target resource: err=%v", err)
		}
	})
}

func pendingIdentityBucketSnapshot(t *testing.T, ctx context.Context, bucket objstore.Bucket) map[string][]byte {
	t.Helper()
	objects := make(map[string][]byte)
	if err := bucket.Iter(ctx, "", func(key string) error {
		reader, err := bucket.Get(ctx, key)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(reader)
		return errors.Join(readErr, reader.Close(), func() error {
			if readErr == nil {
				objects[key] = data
			}
			return nil
		}())
	}, objstore.WithRecursiveIter()); err != nil {
		t.Fatalf("snapshot source archive: %v", err)
	}
	return objects
}
