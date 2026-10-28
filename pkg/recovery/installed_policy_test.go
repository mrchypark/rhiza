package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

func validInstalledPolicy(t *testing.T) (InstalledPolicyRecord, []byte) {
	t.Helper()
	cluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{
		{ID: "voter-a", PeerURL: "quic://voter-a", PublicKey: quepaxa.PublicKey{1}},
		{ID: "voter-b", PeerURL: "quic://voter-b", PublicKey: quepaxa.PublicKey{2}},
	}}
	secret := bytes.Repeat([]byte("s"), 32)
	verifier, err := InstalledJoinCapabilityVerifier("generation-a", secret)
	if err != nil {
		t.Fatal(err)
	}
	genesis, err := InstalledGenesisDigest(cluster, InstalledPolicyProfileAppendOnly, "before-ack")
	if err != nil {
		t.Fatal(err)
	}
	return InstalledPolicyRecord{
		ClusterID:                 "generation-a",
		StorageBindingID:          "public-storage-binding",
		InitialCluster:            cluster,
		Profile:                   InstalledPolicyProfileAppendOnly,
		Durability:                "before-ack",
		JoinCapabilityVerifierHex: fmtHex(verifier),
		Base: InstalledPolicyBase{
			Kind:          InstalledPolicyBaseGenesis,
			GenesisDigest: fmtHex(genesis),
		},
	}, secret
}

func TestInstalledPolicyInstallerAndReadonlyVerifier(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	record, secret := validInstalledPolicy(t)
	digest, err := InstallInstalledPolicy(ctx, bucket, "", record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := InstallInstalledPolicy(ctx, bucket, "", record); err != nil {
		t.Fatalf("identical installer retry: %v", err)
	}
	racingBucket := objstore.NewInMemBucket()
	start := make(chan struct{})
	errCh := make(chan error, 2)
	var installers sync.WaitGroup
	for range 2 {
		installers.Add(1)
		go func() {
			defer installers.Done()
			<-start
			_, installErr := InstallInstalledPolicy(ctx, racingBucket, "", record)
			errCh <- installErr
		}()
	}
	close(start)
	installers.Wait()
	close(errCh)
	for installErr := range errCh {
		if installErr != nil {
			t.Fatalf("concurrent identical installer: %v", installErr)
		}
	}
	conflict := record
	conflict.StorageBindingID = "different-storage-binding"
	if _, err := InstallInstalledPolicy(ctx, bucket, "", conflict); err == nil {
		t.Fatal("second installer replaced a different retained policy")
	}

	expectation := InstalledPolicyExpectation{
		ExpectedDigest: digest, ClusterID: record.ClusterID, StorageBindingID: record.StorageBindingID,
		InitialCluster: record.InitialCluster, Durability: record.Durability,
	}
	verified, err := VerifyInstalledPolicy(ctx, bucket, "", expectation)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Digest() != digest || verified.ClusterID() != record.ClusterID || verified.StorageBindingID() != record.StorageBindingID || verified.Profile() != InstalledPolicyProfileAppendOnly || verified.Durability() != "before-ack" || verified.Base() != record.Base {
		t.Fatal("verified handle getters differ from installed policy")
	}
	if !verified.VerifyJoinCapability(secret) {
		t.Fatal("verified handle rejected the installed generation capability")
	}
	if verified.VerifyJoinCapability([]byte(record.JoinCapabilityVerifierHex)) {
		t.Fatal("public verifier was accepted as the capability secret")
	}
	if verified.VerifyJoinCapability(bytes.Repeat([]byte("x"), len(secret))) {
		t.Fatal("wrong generation capability was accepted")
	}
	mutated := verified.InitialCluster()
	mutated.Members[0].ID = "changed"
	if verified.InitialCluster().Members[0].ID != "voter-a" {
		t.Fatal("verified handle exposed mutable cluster storage")
	}
	stored, err := readInstalledPolicyBytes(ctx, bucket, mustInstalledPolicyKey(t, "", record.ClusterID))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(stored, secret) || bytes.Contains(stored, []byte("approved")) {
		t.Fatal("policy contains a capability secret or approval assertion")
	}
}

func TestGenerateInstalledJoinCapabilityForExplicitInstaller(t *testing.T) {
	clusterID := "generated-generation"
	secret, verifier, err := GenerateInstalledJoinCapability(clusterID)
	if err != nil {
		t.Fatal(err)
	}
	secondSecret, secondVerifier, err := GenerateInstalledJoinCapability(clusterID)
	if err != nil {
		t.Fatal(err)
	}
	if len(secret) == 0 || bytes.Equal(secret, secondSecret) {
		t.Fatal("fresh capability generation returned an empty or repeated secret")
	}
	raw, err := base64.RawURLEncoding.DecodeString(string(secret))
	if err != nil || len(raw) != 32 {
		t.Fatalf("generated capability is not unpadded URL-safe Base64 of 32 random bytes: decoded=%d err=%v", len(raw), err)
	}
	computed, err := InstalledJoinCapabilityVerifier(clusterID, secret)
	if err != nil || computed != verifier {
		t.Fatalf("generated verifier does not bind exact returned secret bytes: got=%x want=%x err=%v", computed, verifier, err)
	}
	if bytes.Equal(verifier[:], secondVerifier[:]) {
		t.Fatal("distinct generated capabilities produced the same verifier")
	}

	record, _ := validInstalledPolicy(t)
	record.ClusterID = clusterID
	record.JoinCapabilityVerifierHex = fmtHex(verifier)
	genesis, err := InstalledGenesisDigest(record.InitialCluster, record.Profile, record.Durability)
	if err != nil {
		t.Fatal(err)
	}
	record.Base.GenesisDigest = fmtHex(genesis)
	bucket := objstore.NewInMemBucket()
	if _, err := InstallInstalledPolicy(context.Background(), bucket, "", record); err != nil {
		t.Fatalf("explicit installer could not install generated verifier: %v", err)
	}
	stored, err := readInstalledPolicyBytes(context.Background(), bucket, mustInstalledPolicyKey(t, "", clusterID))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(stored, secret) {
		t.Fatal("installed policy serialized the generated secret")
	}
	digest, err := InstalledPolicyDigest(record)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyInstalledPolicy(context.Background(), bucket, "", InstalledPolicyExpectation{
		ExpectedDigest: digest, ClusterID: clusterID, StorageBindingID: record.StorageBindingID,
		InitialCluster: record.InitialCluster, Durability: record.Durability,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !verified.VerifyJoinCapability(secret) || verified.VerifyJoinCapability(secondSecret) {
		t.Fatal("verified policy accepted the wrong generated capability")
	}
}

func TestInstalledPolicyVerifyRequiresIndependentExactExpectations(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	record, _ := validInstalledPolicy(t)
	digest, err := InstallInstalledPolicy(ctx, bucket, "", record)
	if err != nil {
		t.Fatal(err)
	}
	base := InstalledPolicyExpectation{
		ExpectedDigest: digest, ClusterID: record.ClusterID, StorageBindingID: record.StorageBindingID,
		InitialCluster: record.InitialCluster, Durability: record.Durability,
	}
	tests := []struct {
		name   string
		mutate func(*InstalledPolicyExpectation)
	}{
		{"missing expected digest", func(e *InstalledPolicyExpectation) { e.ExpectedDigest = [32]byte{} }},
		{"wrong digest", func(e *InstalledPolicyExpectation) { e.ExpectedDigest[0] ^= 0xff }},
		{"wrong cluster", func(e *InstalledPolicyExpectation) { e.ClusterID = "generation-b" }},
		{"wrong storage mapping", func(e *InstalledPolicyExpectation) { e.StorageBindingID = "other-storage" }},
		{"wrong initial membership", func(e *InstalledPolicyExpectation) { e.InitialCluster.Members[0].PublicKey[0] ^= 0xff }},
		{"wrong durability", func(e *InstalledPolicyExpectation) { e.Durability = "async" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expected := base
			expected.InitialCluster = cloneCluster(base.InitialCluster)
			test.mutate(&expected)
			if _, err := VerifyInstalledPolicy(ctx, bucket, "", expected); err == nil {
				t.Fatal("mismatched expectation was accepted")
			}
		})
	}
	if _, err := VerifyInstalledPolicy(ctx, bucket, "", InstalledPolicyExpectation{ClusterID: record.ClusterID}); err == nil {
		t.Fatal("verifier inferred an expected digest from the policy object")
	}
}

func TestVerifyMissingInstalledPolicyFailsWithoutCreatingIt(t *testing.T) {
	record, _ := validInstalledPolicy(t)
	digest, err := InstalledPolicyDigest(record)
	if err != nil {
		t.Fatal(err)
	}
	bucket := objstore.NewInMemBucket()
	_, err = VerifyInstalledPolicy(context.Background(), bucket, "", InstalledPolicyExpectation{
		ExpectedDigest: digest, ClusterID: record.ClusterID, StorageBindingID: record.StorageBindingID,
		InitialCluster: record.InitialCluster, Durability: record.Durability,
	})
	if err == nil {
		t.Fatal("missing installed policy was accepted")
	}
	exists, existsErr := bucket.Exists(context.Background(), mustInstalledPolicyKey(t, "", record.ClusterID))
	if existsErr != nil || exists {
		t.Fatalf("readonly verifier created the missing policy: exists=%v err=%v", exists, existsErr)
	}
}

func TestInstalledPolicyRejectsUnsupportedProfileAndConfusedBase(t *testing.T) {
	record, _ := validInstalledPolicy(t)
	tests := []struct {
		name   string
		mutate func(*InstalledPolicyRecord)
	}{
		{"unknown profile", func(p *InstalledPolicyRecord) { p.Profile = "other" }},
		{"unknown durability", func(p *InstalledPolicyRecord) { p.Durability = "unknown" }},
		{"genesis carrying DR anchor", func(p *InstalledPolicyRecord) { p.Base.GenerationAnchorHash = strings.Repeat("a", 64) }},
		{"DR without anchor", func(p *InstalledPolicyRecord) { p.Base = InstalledPolicyBase{Kind: InstalledPolicyBaseDR} }},
		{"DR with genesis digest", func(p *InstalledPolicyRecord) {
			p.Base = InstalledPolicyBase{Kind: InstalledPolicyBaseDR, GenesisDigest: p.Base.GenesisDigest, GenerationAnchorHash: strings.Repeat("a", 64)}
		}},
		{"unknown base kind", func(p *InstalledPolicyRecord) { p.Base.Kind = "other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneInstalledPolicy(record)
			test.mutate(&candidate)
			if _, err := InstalledPolicyDigest(candidate); err == nil {
				t.Fatal("unsupported policy shape was accepted")
			}
		})
	}
}

func TestInstalledPolicyRejectsDuplicatePublicKeyEvenWhenHashPinned(t *testing.T) {
	ctx := context.Background()
	record, _ := validInstalledPolicy(t)
	record.InitialCluster.Members = append(record.InitialCluster.Members, quepaxa.Member{
		ID: "voter-c", PeerURL: "quic://voter-c", PublicKey: quepaxa.PublicKey{3},
	})
	genesis, err := InstalledGenesisDigest(record.InitialCluster, record.Profile, record.Durability)
	if err != nil {
		t.Fatal(err)
	}
	record.Base.GenesisDigest = fmtHex(genesis)
	if _, err := InstalledPolicyDigest(record); err != nil {
		t.Fatalf("three voters with distinct public keys were rejected: %v", err)
	}
	record.InitialCluster.Members[2].PublicKey = record.InitialCluster.Members[0].PublicKey
	if _, err := InstalledPolicyDigest(record); err == nil {
		t.Fatal("digest accepted distinct voter IDs sharing one public key")
	}
	bucket := objstore.NewInMemBucket()
	if _, err := InstallInstalledPolicy(ctx, bucket, "", record); err == nil {
		t.Fatal("installer accepted distinct voter IDs sharing one public key")
	}

	// Pin the exact malformed bytes independently to exercise verifier-side
	// schema validation rather than relying on digest construction to reject it.
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, mustInstalledPolicyKey(t, "", record.ClusterID), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	_, err = VerifyInstalledPolicy(ctx, bucket, "", InstalledPolicyExpectation{
		ExpectedDigest: sha256.Sum256(data), ClusterID: record.ClusterID,
		StorageBindingID: record.StorageBindingID, InitialCluster: record.InitialCluster,
		Durability: record.Durability,
	})
	if err == nil {
		t.Fatal("verifier accepted a hash-pinned initial cluster with duplicate public keys")
	}
}

func TestVerifyRejectsHashPinnedInvalidProfileOrBaseAndAcceptsDRBase(t *testing.T) {
	ctx := context.Background()
	record, _ := validInstalledPolicy(t)
	tests := []struct {
		name   string
		mutate func(*InstalledPolicyRecord)
	}{
		{"profile", func(p *InstalledPolicyRecord) { p.Profile = "unrecognized" }},
		{"genesis-anchor-confusion", func(p *InstalledPolicyRecord) { p.Base.GenerationAnchorHash = strings.Repeat("a", 64) }},
		{"bad-genesis-digest", func(p *InstalledPolicyRecord) { p.Base.GenesisDigest = strings.Repeat("b", 64) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneInstalledPolicy(record)
			test.mutate(&candidate)
			data, err := json.Marshal(candidate)
			if err != nil {
				t.Fatal(err)
			}
			bucket := objstore.NewInMemBucket()
			if err := bucket.Upload(ctx, mustInstalledPolicyKey(t, "", record.ClusterID), bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			expected := InstalledPolicyExpectation{
				ExpectedDigest: sha256.Sum256(data), ClusterID: record.ClusterID,
				StorageBindingID: record.StorageBindingID, InitialCluster: record.InitialCluster, Durability: record.Durability,
			}
			if _, err := VerifyInstalledPolicy(ctx, bucket, "", expected); err == nil {
				t.Fatal("hash-pinned invalid profile/base was accepted")
			}
		})
	}

	drRecord := cloneInstalledPolicy(record)
	drRecord.ClusterID = "generation-dr"
	drRecord.Base = InstalledPolicyBase{Kind: InstalledPolicyBaseDR, GenerationAnchorHash: strings.Repeat("c", 64)}
	secret := bytes.Repeat([]byte("d"), 32)
	verifier, err := InstalledJoinCapabilityVerifier(drRecord.ClusterID, secret)
	if err != nil {
		t.Fatal(err)
	}
	drRecord.JoinCapabilityVerifierHex = fmtHex(verifier)
	bucket := objstore.NewInMemBucket()
	digest, err := InstallInstalledPolicy(ctx, bucket, "", drRecord)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyInstalledPolicy(ctx, bucket, "", InstalledPolicyExpectation{
		ExpectedDigest: digest, ClusterID: drRecord.ClusterID, StorageBindingID: drRecord.StorageBindingID,
		InitialCluster: drRecord.InitialCluster, Durability: drRecord.Durability,
	})
	if err != nil {
		t.Fatal(err)
	}
	if verified.Base() != drRecord.Base {
		t.Fatalf("valid explicit DR base not retained: base=%+v", verified.Base())
	}
}

func TestInstalledPolicyRejectsMalformedCanonicalObject(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	record, _ := validInstalledPolicy(t)
	_, err := InstalledPolicyDigest(record)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := encodeInstalledPolicy(record)
	if err != nil {
		t.Fatal(err)
	}
	noncanonical := append([]byte(" "), canonical...)
	if err := bucket.Upload(ctx, mustInstalledPolicyKey(t, "", record.ClusterID), bytes.NewReader(noncanonical)); err != nil {
		t.Fatal(err)
	}
	expected := InstalledPolicyExpectation{ExpectedDigest: sha256Sum(noncanonical), ClusterID: record.ClusterID, StorageBindingID: record.StorageBindingID, InitialCluster: record.InitialCluster, Durability: record.Durability}
	if _, err := VerifyInstalledPolicy(ctx, bucket, "", expected); err == nil {
		t.Fatal("noncanonical object with the expected canonical digest was accepted")
	}

	bucket2 := objstore.NewInMemBucket()
	unknown := append(append([]byte(nil), canonical[:len(canonical)-1]...), []byte(`,"unknown":true}`)...)
	if err := bucket2.Upload(ctx, mustInstalledPolicyKey(t, "", record.ClusterID), bytes.NewReader(unknown)); err != nil {
		t.Fatal(err)
	}
	expected.ExpectedDigest = sha256Sum(unknown)
	if _, err := VerifyInstalledPolicy(ctx, bucket2, "", expected); err == nil {
		t.Fatal("unknown policy field was accepted after digest verification")
	}
}

func TestInstalledPolicyUnknownUploadErrorRemainsUnknownAndRetryVerifiesBytes(t *testing.T) {
	ctx := context.Background()
	record, _ := validInstalledPolicy(t)
	unknown := errors.New("write result unknown")
	bucket := &installedPolicyTestBucket{Bucket: objstore.NewInMemBucket(), commitThenError: true, uploadErr: unknown}
	digest, err := InstallInstalledPolicy(ctx, bucket, "", record)
	if digest != ([32]byte{}) || err != unknown {
		t.Fatalf("unknown upload result digest=%x err=%v; expected exact error", digest, err)
	}
	if _, err := InstallInstalledPolicy(ctx, bucket, "", record); err != nil {
		t.Fatalf("later exact-byte retry did not verify installed policy: %v", err)
	}
	if bucket.uploads != 1 {
		t.Fatalf("upload calls=%d, want no replacement on exact retry", bucket.uploads)
	}
}

func TestInstalledPolicyKeyAndCapabilityAreGenerationScoped(t *testing.T) {
	key, err := InstalledPolicyKey("archive", "generation-a")
	if err != nil || key != "archive/generation-a/recovery/installed-policy.json" {
		t.Fatalf("policy key=%q err=%v", key, err)
	}
	for _, prefix := range []string{"../escape", "nested/../escape"} {
		if _, err := InstalledPolicyKey(prefix, "generation-a"); err == nil {
			t.Fatalf("unsafe prefix %q accepted", prefix)
		}
	}
	if normalized, err := InstalledPolicyKey("/archive/", "generation-a"); err != nil || normalized != "archive/generation-a/recovery/installed-policy.json" {
		t.Fatalf("existing prefix normalization changed: key=%q err=%v", normalized, err)
	}
	if _, err := InstalledPolicyKey("", "../escape"); err == nil {
		t.Fatal("unsafe cluster ID accepted in policy key")
	}
	secret := bytes.Repeat([]byte("j"), 32)
	one, err := InstalledJoinCapabilityVerifier("generation-a", secret)
	if err != nil {
		t.Fatal(err)
	}
	two, err := InstalledJoinCapabilityVerifier("generation-b", secret)
	if err != nil || one == two {
		t.Fatal("capability verifier was not generation-scoped")
	}
	for name, candidate := range map[string][]byte{
		"empty": nil,
		"short": secret[:31],
	} {
		if _, err := InstalledJoinCapabilityVerifier("generation-a", candidate); err == nil {
			t.Fatalf("%s capability was accepted", name)
		}
	}
}

type installedPolicyTestBucket struct {
	objstore.Bucket
	uploads         int
	commitThenError bool
	uploadErr       error
}

func (b *installedPolicyTestBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	b.uploads++
	if b.commitThenError {
		if err := b.Bucket.Upload(ctx, name, reader, options...); err != nil {
			return err
		}
		return b.uploadErr
	}
	if b.uploadErr != nil {
		return b.uploadErr
	}
	return b.Bucket.Upload(ctx, name, reader, options...)
}

func mustInstalledPolicyKey(t *testing.T, prefix, clusterID string) string {
	t.Helper()
	key, err := InstalledPolicyKey(prefix, clusterID)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func fmtHex(value [32]byte) string { return hex.EncodeToString(value[:]) }

func sha256Sum(value []byte) [32]byte { return sha256.Sum256(value) }
