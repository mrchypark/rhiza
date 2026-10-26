package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type archiveGenesisFixtureClient struct {
	bucket      objstore.Bucket
	prefix      string
	status      fencing.ArchiveGenesisStatus
	calls       int
	uploads     int
	afterUpload func(*archiveGenesisFixtureClient)
}

func (c *archiveGenesisFixtureClient) ArchiveGenesisStatus(_ context.Context, _ string, _ uint) (fencing.ArchiveGenesisStatus, error) {
	c.calls++
	return c.status, nil
}

func (c *archiveGenesisFixtureClient) Upload(ctx context.Context, key string, condition fencing.MutationCondition, payload []byte) error {
	c.uploads++
	if key != "archive/head.bin" || condition.Mode != fencing.MutationCreateOnly {
		return errors.New("genesis upload was not create-only")
	}
	if err := c.bucket.Upload(ctx, path.Join(c.prefix, key), bytes.NewReader(payload), objstore.WithIfNotExists()); err != nil {
		return err
	}
	digest := sha256.Sum256(payload)
	digestHex := hex.EncodeToString(digest[:])
	intent := fencing.MutationIntent{
		ContractVersion: fencing.MutationIntentVersion,
		StoreScopeHash:  c.status.MutationScopeHash,
		OwnerStep:       fencing.MutationOwnerStep{Owner: "producer", Workflow: "stage", Step: "archive-genesis", Attempt: 1},
		Condition:       fencing.MutationCondition{Mode: fencing.MutationCreateOnly},
		OperationID:     fencing.MutationRequestID("producer", 1),
		Kind:            "upload", Key: key, PayloadRef: "staged-genesis", PayloadDigest: digestHex, PayloadSize: int64(len(payload)),
	}
	ref := fencing.ArchiveGenesisRef{Version: fencing.ArchiveGenesisVersion, ConfigID: recovery.ArchiveConfigID, ScopeHash: c.status.ScopeHash,
		Mutation: &fencing.ArchiveGenesisMutation{ProducerUID: "producer", Sequence: 1, Intent: intent}}
	c.status.CanInitializeEmpty = false
	c.status.Ref = &ref
	c.status.Mutation = &fencing.MutationLookupResult{Found: true, Entry: &fencing.MutationEntry{Intent: intent, Resolution: &fencing.MutationResolution{
		Outcome: fencing.MutationCommitted, PreparationHash: strings.Repeat("a", 64), ReceiptDigest: strings.Repeat("b", 64),
		PayloadDigest: digestHex, AllCopiesTerminal: true,
	}}}
	if c.afterUpload != nil {
		c.afterUpload(c)
	}
	return nil
}

func TestEnsureArchiveGenesisCreatesOnlyClaimAuthorizedEmptyHead(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	if err := bucket.Upload(ctx, "cluster/voters/membership.json", strings.NewReader("bootstrap membership"), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: "bucket", Prefix: "cluster", ClusterID: "cluster"}
	scopeHash, err := scope.Hash()
	if err != nil {
		t.Fatal(err)
	}
	client := &archiveGenesisFixtureClient{bucket: bucket, prefix: scope.Prefix, status: fencing.ArchiveGenesisStatus{
		ScopeHash: scopeHash, MutationScopeHash: strings.Repeat("c", 64), ConfigID: recovery.ArchiveConfigID, CanInitializeEmpty: true,
	}}
	if err := ensureArchiveGenesis(ctx, client, bucket, scope, client.status.MutationScopeHash, recovery.MembershipRecord{}); err != nil {
		t.Fatal(err)
	}
	if client.calls != 2 || client.uploads != 1 {
		t.Fatalf("status calls=%d uploads=%d, want 2 and 1", client.calls, client.uploads)
	}
	archive := recovery.NewManager(bucket, scope.Prefix, recovery.ArchiveConfigID)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := archive.HeadVersion(); !ok || archive.Tip() != 0 || archive.Sealed() {
		t.Fatal("genesis HEAD was not a persisted empty unsealed archive")
	}
}

func TestEmptyRecoveryAuthorizationMustRemainIdenticalAcrossGenesisUpload(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: "bucket", Prefix: "target", ClusterID: "target"}
	scopeHash, err := scope.Hash()
	if err != nil {
		t.Fatal(err)
	}
	membership := recovery.MembershipRecord{Cluster: "target", Durability: "async"}
	membershipBytes, err := json.Marshal(membership)
	if err != nil {
		t.Fatal(err)
	}
	digest := func(value string) string { return archiveGenesisBytesDigest([]byte(value)) }
	authorization := fencing.ArchiveEmptyRecoveryAuthorization{
		Version: fencing.ArchiveGenesisVersion, StatefulSetUID: "sts", TargetClusterID: "target",
		OperationID: "recovery-op", SourceFenceOperationID: "fence-op", ConfigID: recovery.ArchiveConfigID,
		SourceTip: 0, SourceScopeHash: digest("source-scope"), TargetScopeHash: scopeHash,
		SourceGenesisRefDigest: digest("source-genesis"), SourceHeadDigest: digest("sealed-source-head"),
		SourceHeadVersion:     objstore.ObjectVersion{Type: objstore.ETag, Value: "sealed-source-etag"},
		TargetEmptyHeadDigest: archiveGenesisBytesDigest(mustEmptyArchiveHead(t)), SourceRootDigest: digest("source-root"),
		SourceFenceDigest: digest("source-fence"), SourceMembershipHash: digest("source-membership"),
		SourceDurability: "async", TargetMembershipHash: digest(string(membershipBytes)), TargetDurability: "async",
		AllowDataLoss: true, PolicyDigest: digest("persisted-policy"),
	}
	client := &archiveGenesisFixtureClient{bucket: bucket, prefix: scope.Prefix, status: fencing.ArchiveGenesisStatus{
		ScopeHash: scopeHash, MutationScopeHash: digest("mutation-scope"), ConfigID: recovery.ArchiveConfigID,
		EmptyRecoveryAuthorization: &authorization, EmptyRecoveryState: fencing.EmptyRecoveryAuthorized,
	}}
	client.afterUpload = func(client *archiveGenesisFixtureClient) {
		client.status.EmptyRecoveryAuthorization = nil
		client.status.EmptyRecoveryState = ""
	}
	if err := ensureArchiveGenesis(ctx, client, bucket, scope, client.status.MutationScopeHash, membership); err != nil {
		if !strings.Contains(err.Error(), "authorization changed") {
			t.Fatalf("missing original authorization was not rejected explicitly: %v", err)
		}
	} else {
		t.Fatal("accepted a changed or missing authorization after publishing target genesis")
	}
}

func mustEmptyArchiveHead(t *testing.T) []byte {
	t.Helper()
	data, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestEnsureArchiveGenesisDoesNotAdoptAnExistingHead(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: "bucket", Prefix: "cluster", ClusterID: "cluster"}
	scopeHash, err := scope.Hash()
	if err != nil {
		t.Fatal(err)
	}
	head, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, path.Join(scope.Prefix, "archive/head.bin"), bytes.NewReader(head), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	client := &archiveGenesisFixtureClient{bucket: bucket, prefix: scope.Prefix, status: fencing.ArchiveGenesisStatus{
		ScopeHash: scopeHash, MutationScopeHash: strings.Repeat("c", 64), ConfigID: recovery.ArchiveConfigID, CanInitializeEmpty: true,
	}}
	if err := ensureArchiveGenesis(ctx, client, bucket, scope, client.status.MutationScopeHash, recovery.MembershipRecord{}); err == nil {
		t.Fatal("adopted an existing HEAD without a canonical genesis reference")
	}
	if client.uploads != 0 {
		t.Fatalf("attempted %d genesis uploads over an existing HEAD", client.uploads)
	}
}

func TestEnsureArchiveGenesisRejectsHeadlessRecoveryEvidence(t *testing.T) {
	tests := []struct {
		name string
		keys []string
	}{
		{name: "fork intent only", keys: []string{"cluster/fork/intent.json"}},
		{name: "fork result only", keys: []string{"cluster/fork/result.json"}},
		{name: "checkpoint current only", keys: []string{"cluster/checkpoint/CURRENT"}},
		{name: "checkpoint root only", keys: []string{"cluster/checkpoint/roots/00000000000000000001_deadbeef.json"}},
		{name: "lineage only", keys: []string{"cluster/recovery/generation.json"}},
		{name: "anchor and lineage without head", keys: []string{"cluster/recovery/anchor.json", "cluster/recovery/generation.json"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			bucket := objstore.NewInMemBucket()
			for _, key := range tt.keys {
				if err := bucket.Upload(ctx, key, strings.NewReader("interrupted recovery evidence"), objstore.WithIfNotExists()); err != nil {
					t.Fatal(err)
				}
			}
			scope := fencing.ArchiveScope{Provider: "gcs", Bucket: "bucket", Prefix: "cluster", ClusterID: "cluster"}
			scopeHash, err := scope.Hash()
			if err != nil {
				t.Fatal(err)
			}
			client := &archiveGenesisFixtureClient{bucket: bucket, prefix: scope.Prefix, status: fencing.ArchiveGenesisStatus{
				ScopeHash: scopeHash, MutationScopeHash: strings.Repeat("c", 64), ConfigID: recovery.ArchiveConfigID, CanInitializeEmpty: true,
			}}
			if err := ensureArchiveGenesis(ctx, client, bucket, scope, client.status.MutationScopeHash, recovery.MembershipRecord{}); err == nil {
				t.Fatal("initialized an empty HEAD over interrupted recovery evidence")
			}
			if client.uploads != 0 {
				t.Fatalf("attempted %d genesis uploads over interrupted recovery evidence", client.uploads)
			}
			if _, err := bucket.Attributes(ctx, path.Join(scope.Prefix, "archive/head.bin")); !bucket.IsObjNotFoundErr(err) {
				t.Fatalf("genesis HEAD was created despite prior evidence: %v", err)
			}
		})
	}
}

func TestEnsureArchiveGenesisVerifiesForkAndAcceptsEvolvedHead(t *testing.T) {
	ctx := context.Background()
	bucket, scope, membership, status := buildForkArchiveGenesisFixture(t)
	client := &archiveGenesisFixtureClient{bucket: bucket, prefix: scope.Prefix, status: status}
	if err := ensureArchiveGenesis(ctx, client, bucket, scope, status.MutationScopeHash, membership); err != nil {
		t.Fatalf("valid fork genesis with evolved HEAD: %v", err)
	}
	if client.uploads != 0 {
		t.Fatalf("valid existing fork genesis caused %d uploads", client.uploads)
	}
	archive := recovery.NewManager(bucket, scope.Prefix, recovery.ArchiveConfigID)
	defer archive.Close()
	if err := archive.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if archive.Tip() <= quepaxa.Slot(status.Ref.Fork.BaseTip) || archive.Sealed() {
		t.Fatalf("current HEAD was not legitimately evolved: tip=%d base=%d sealed=%v", archive.Tip(), status.Ref.Fork.BaseTip, archive.Sealed())
	}
}

func TestEnsureArchiveGenesisRejectsForkReferenceMismatches(t *testing.T) {
	ctx := context.Background()
	bucket, scope, membership, status := buildForkArchiveGenesisFixture(t)
	tests := []struct {
		name    string
		mutate  func(*fencing.ArchiveGenesisStatus, *recovery.MembershipRecord)
		corrupt func(objstore.Bucket, string) error
	}{
		{name: "scope", mutate: func(status *fencing.ArchiveGenesisStatus, _ *recovery.MembershipRecord) {
			status.ScopeHash = strings.Repeat("d", 64)
		}},
		{name: "mutation scope", mutate: func(status *fencing.ArchiveGenesisStatus, _ *recovery.MembershipRecord) {
			status.MutationScopeHash = strings.Repeat("d", 64)
		}},
		{name: "config", mutate: func(status *fencing.ArchiveGenesisStatus, _ *recovery.MembershipRecord) { status.ConfigID++ }},
		{name: "anchor key", mutate: func(status *fencing.ArchiveGenesisStatus, _ *recovery.MembershipRecord) {
			status.Ref.Fork.AnchorKey = "other/anchor.json"
		}},
		{name: "anchor digest", mutate: func(status *fencing.ArchiveGenesisStatus, _ *recovery.MembershipRecord) {
			status.Ref.Fork.AnchorDigest = strings.Repeat("d", 64)
		}},
		{name: "intent digest", mutate: func(status *fencing.ArchiveGenesisStatus, _ *recovery.MembershipRecord) {
			status.Ref.Fork.IntentDigest = strings.Repeat("d", 64)
		}},
		{name: "result digest", mutate: func(status *fencing.ArchiveGenesisStatus, _ *recovery.MembershipRecord) {
			status.Ref.Fork.ResultDigest = strings.Repeat("d", 64)
		}},
		{name: "lineage digest", mutate: func(status *fencing.ArchiveGenesisStatus, _ *recovery.MembershipRecord) {
			status.Ref.Fork.LineageDigest = strings.Repeat("d", 64)
		}},
		{name: "target prefix", mutate: func(status *fencing.ArchiveGenesisStatus, _ *recovery.MembershipRecord) {
			status.Ref.Fork.TargetPrefix = "other-target"
		}},
		{name: "membership hash", mutate: func(status *fencing.ArchiveGenesisStatus, _ *recovery.MembershipRecord) {
			status.Ref.Fork.MembershipHash = strings.Repeat("d", 64)
		}},
		{name: "base tip", mutate: func(status *fencing.ArchiveGenesisStatus, _ *recovery.MembershipRecord) { status.Ref.Fork.BaseTip++ }},
		{name: "base prefix", mutate: func(status *fencing.ArchiveGenesisStatus, _ *recovery.MembershipRecord) {
			status.Ref.Fork.BasePrefixHash = strings.Repeat("d", 64)
		}},
		{name: "anchor hash", mutate: func(status *fencing.ArchiveGenesisStatus, _ *recovery.MembershipRecord) {
			status.Ref.Fork.AnchorHash = strings.Repeat("d", 64)
		}},
		{name: "proof config", mutate: func(status *fencing.ArchiveGenesisStatus, _ *recovery.MembershipRecord) { status.Ref.Fork.ConfigID++ }},
		{name: "target membership", mutate: func(_ *fencing.ArchiveGenesisStatus, membership *recovery.MembershipRecord) {
			membership.Membership = strings.Repeat("d", 64)
		}},
		{name: "changed lineage object", corrupt: func(bucket objstore.Bucket, key string) error {
			return bucket.Upload(ctx, key, bytes.NewReader([]byte(`{"fork":{}}`)))
		}},
		{name: "missing head with orphaned fork data", corrupt: func(bucket objstore.Bucket, key string) error {
			return bucket.Delete(ctx, key)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := status
			current.Ref = cloneArchiveGenesisRef(status.Ref)
			currentMembership := membership
			if tt.mutate != nil {
				tt.mutate(&current, &currentMembership)
			}
			if tt.corrupt != nil {
				key := path.Join(scope.Prefix, "recovery/generation.json")
				if tt.name == "missing head with orphaned fork data" {
					key = path.Join(scope.Prefix, "archive/head.bin")
				}
				if err := tt.corrupt(bucket, key); err != nil {
					t.Fatal(err)
				}
			}
			client := &archiveGenesisFixtureClient{bucket: bucket, prefix: scope.Prefix, status: current}
			if err := ensureArchiveGenesis(ctx, client, bucket, scope, status.MutationScopeHash, currentMembership); err == nil {
				t.Fatal("accepted mismatched or missing fork evidence")
			}
			if client.uploads != 0 {
				t.Fatalf("invalid fork genesis caused %d uploads", client.uploads)
			}
		})
	}
}

func buildForkArchiveGenesisFixture(t *testing.T) (objstore.Bucket, fencing.ArchiveScope, recovery.MembershipRecord, fencing.ArchiveGenesisStatus) {
	t.Helper()
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	sourcePrefix, targetPrefix, sourceID, targetID, operationID := "archives/source", "archives/target", "source", "target", "recovery-op"
	sourceMember := quepaxa.Member{ID: "source", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("source-key")))}
	sourceMembers := []quepaxa.Member{sourceMember}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: sourceMember.ID, Cluster: quepaxa.Cluster{ConfigID: 1, Members: sourceMembers}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeGraphCommand(types.GraphCommand{RequestID: "genesis-fixture", Events: []types.GraphStreamEvent{{Stream: "genesis", Kind: "created", Payload: "verified"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(ctx, value); err != nil {
		t.Fatal(err)
	}
	sourceArchive := recovery.NewManager(bucket, sourcePrefix, recovery.ArchiveConfigID)
	if err := sourceArchive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	sourceArchive.Close()
	if err := recovery.Seal(ctx, bucket, sourcePrefix, operationID); err != nil {
		t.Fatal(err)
	}
	targetMember := quepaxa.Member{ID: "target", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("target-key")))}
	membership := recovery.NewMembershipRecord(targetID, []quepaxa.Member{targetMember}, "async")
	result, err := recovery.Fork(ctx, bucket, recovery.ForkOptions{
		SourcePrefix: sourcePrefix, TargetPrefix: targetPrefix,
		SourceBootstrap: quepaxa.Cluster{ConfigID: 1, Members: sourceMembers}, TargetMembers: []quepaxa.Member{targetMember},
		TargetMembership: membership, OperationID: operationID,
	})
	if err != nil {
		t.Fatal(err)
	}
	anchor, anchorHash, err := recovery.ReadGenerationAnchor(ctx, bucket, targetPrefix)
	if err != nil {
		t.Fatal(err)
	}
	lineage := map[string]any{
		"version": 1, "operation_id": operationID, "source": sourceID,
		"source_durability": "async", "source_membership": strings.Repeat("e", 64),
		"target_membership": membership, "fork": result,
	}
	lineageBytes, err := json.Marshal(lineage)
	if err != nil {
		t.Fatal(err)
	}
	membershipBytes, err := json.Marshal(membership)
	if err != nil {
		t.Fatal(err)
	}
	for key, payload := range map[string][]byte{
		path.Join(targetPrefix, "voters/membership.json"):   membershipBytes,
		path.Join(targetPrefix, "recovery/generation.json"): lineageBytes,
	} {
		if err := bucket.Upload(ctx, key, bytes.NewReader(payload), objstore.WithIfNotExists()); err != nil {
			t.Fatal(err)
		}
	}
	lineageDigest := archiveGenesisBytesDigest(lineageBytes)
	intentBytes, err := readArchiveGenesisObject(ctx, bucket, path.Join(targetPrefix, "fork/intent.json"))
	if err != nil {
		t.Fatal(err)
	}
	resultBytes, err := readArchiveGenesisObject(ctx, bucket, path.Join(targetPrefix, "fork/result.json"))
	if err != nil {
		t.Fatal(err)
	}
	membershipBytes, err = json.Marshal(membership)
	if err != nil {
		t.Fatal(err)
	}
	membershipHash := sha256.Sum256(membershipBytes)
	anchorHashHex := hex.EncodeToString(anchorHash[:])
	proof := fencing.ArchiveGenesisFork{
		AnchorKey: recovery.GenerationAnchorKey(targetPrefix), AnchorDigest: anchorHashHex, AnchorHash: anchorHashHex,
		IntentDigest: archiveGenesisBytesDigest(intentBytes), ResultDigest: archiveGenesisBytesDigest(resultBytes),
		LineageDigest: lineageDigest, TargetPrefix: targetPrefix,
		MembershipHash: hex.EncodeToString(membershipHash[:]), ConfigID: recovery.ArchiveConfigID,
		BaseTip: anchor.SourceTip, BasePrefixHash: anchor.SourcePrefixHash,
	}
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: "bucket", Prefix: targetPrefix, ClusterID: targetID}
	scopeHash, err := scope.Hash()
	if err != nil {
		t.Fatal(err)
	}
	status := fencing.ArchiveGenesisStatus{ScopeHash: scopeHash, MutationScopeHash: strings.Repeat("c", 64), ConfigID: recovery.ArchiveConfigID, Ref: &fencing.ArchiveGenesisRef{
		Version: fencing.ArchiveGenesisVersion, ConfigID: recovery.ArchiveConfigID, ScopeHash: scopeHash, Fork: &proof,
	}}
	// Advance the actual forked target HEAD. The genesis payload is a verified
	// base, not the only byte representation the live archive may ever contain.
	targetWAL, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer targetWAL.Close()
	targetCore, err := quepaxa.New(quepaxa.Config{NodeID: targetMember.ID, Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{targetMember}}, WAL: targetWAL, EnableReconfiguration: true})
	if err != nil {
		t.Fatal(err)
	}
	var anchorPrefix, rootHash [32]byte
	if _, err := hex.Decode(anchorPrefix[:], []byte(anchor.SourcePrefixHash)); err != nil {
		t.Fatal(err)
	}
	if _, err := hex.Decode(rootHash[:], []byte(anchor.Checkpoint.RootHash)); err != nil {
		t.Fatal(err)
	}
	if err := targetCore.InstallFencedGenerationBase(ctx, quepaxa.FencedGenerationBase{ConfigID: 1, Index: quepaxa.Slot(anchor.SourceTip), PrefixHash: anchorPrefix, RootHash: rootHash, AnchorHash: anchorHash}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := targetCore.Propose(ctx, quepaxa.EncodeReadBarrier([quepaxa.ReadBarrierNonceSize]byte{1})); err != nil {
		t.Fatal(err)
	}
	targetArchive := recovery.NewManager(bucket, targetPrefix, recovery.ArchiveConfigID)
	if err := targetArchive.SyncThrough(ctx, targetCore, targetCore.Tip()); err != nil {
		t.Fatal(err)
	}
	targetArchive.Close()
	return bucket, scope, membership, status
}

func cloneArchiveGenesisRef(ref *fencing.ArchiveGenesisRef) *fencing.ArchiveGenesisRef {
	if ref == nil {
		return nil
	}
	copy := *ref
	if ref.Fork != nil {
		fork := *ref.Fork
		copy.Fork = &fork
	}
	if ref.Mutation != nil {
		mutation := *ref.Mutation
		copy.Mutation = &mutation
	}
	return &copy
}
