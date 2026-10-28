package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
	"github.com/thanos-io/objstore"
)

func TestVerifyInstalledPolicyUsesIndependentRuntimeExpectations(t *testing.T) {
	ctx := context.Background()
	config, bucket, secret := installedPolicyNodeFixture(t)
	verified, err := verifyInstalledPolicy(ctx, &config, bucket)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Digest() != config.ExpectedInstalledPolicyHash || verified.ClusterID() != string(config.ClusterID) || verified.Profile() != recovery.InstalledPolicyProfileAppendOnly || verified.Durability() != config.ObjStoreDurability {
		t.Fatal("verified policy does not match its independent runtime expectation")
	}
	if !verified.VerifyJoinCapability(secret) || verified.VerifyJoinCapability([]byte("wrong capability")) {
		t.Fatal("verified policy accepted the wrong enrollment capability")
	}
	cluster := verified.InitialCluster()
	cluster.Members[0].PublicKey[0]++
	if verified.InitialCluster().Members[0].PublicKey == cluster.Members[0].PublicKey {
		t.Fatal("verified policy exposed mutable initial membership")
	}
}

func TestVerifyInstalledPolicyRejectsMissingOrChangedExpectations(t *testing.T) {
	ctx := context.Background()
	config, bucket, _ := installedPolicyNodeFixture(t)
	tests := []struct {
		name   string
		mutate func(*types.ExecutionConfig)
	}{
		{name: "missing independent digest", mutate: func(c *types.ExecutionConfig) { c.ExpectedInstalledPolicyHash = [32]byte{} }},
		{name: "different digest", mutate: func(c *types.ExecutionConfig) { c.ExpectedInstalledPolicyHash = sha256.Sum256([]byte("wrong policy")) }},
		{name: "different initial membership", mutate: func(c *types.ExecutionConfig) { c.Members[0].PublicKey[0]++ }},
		{name: "different durability", mutate: func(c *types.ExecutionConfig) { c.ObjStoreDurability = types.ObjectStoreDurabilityAsync }},
		{name: "different storage prefix", mutate: func(c *types.ExecutionConfig) { c.ObjStorePrefix += "/other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := config
			changed.Members = append([]quepaxa.Member(nil), config.Members...)
			test.mutate(&changed)
			if _, err := verifyInstalledPolicy(ctx, &changed, bucket); err == nil {
				t.Fatal("changed independent policy expectation was accepted")
			}
		})
	}
	if _, err := verifyInstalledPolicy(ctx, &config, objstore.NewInMemBucket()); err == nil {
		t.Fatal("missing installed policy was accepted")
	}
	wrongNamespace := objstore.NewInMemBucket()
	if _, err := verifyInstalledPolicy(ctx, &config, wrongNamespace); err == nil {
		t.Fatal("installed policy in a different bucket namespace was accepted")
	}
}

func TestInstalledPolicyExpectationRejectsLocalAndImplicitIdentity(t *testing.T) {
	for _, config := range []*types.ExecutionConfig{
		{ExpectedInstalledPolicyHash: [32]byte{1}, ClusterID: "generation-7", Local: true},
		{ExpectedInstalledPolicyHash: [32]byte{1}},
		{ExpectedInstalledPolicyHash: [32]byte{1}, ClusterID: "generation-7"},
	} {
		if _, err := installedPolicyExpectation(config); err == nil {
			t.Fatalf("invalid installed-policy config accepted: %+v", config)
		}
	}
}

func TestInstalledPolicyExpectationUsesRuntimeS3Default(t *testing.T) {
	config, _, _ := installedPolicyNodeFixture(t)
	want, err := installedPolicyExpectation(&config)
	if err != nil {
		t.Fatal(err)
	}
	config.ObjStoreProvider = ""
	got, err := installedPolicyExpectation(&config)
	if err != nil {
		t.Fatalf("runtime-default S3 provider rejected: %v", err)
	}
	if got.StorageBindingID != want.StorageBindingID {
		t.Fatalf("empty provider binding=%q, explicit S3 binding=%q", got.StorageBindingID, want.StorageBindingID)
	}
	config.ObjStoreProvider = "s3"
	config.ObjStoreDurability = ""
	got, err = installedPolicyExpectation(&config)
	if err != nil {
		t.Fatalf("runtime-default async durability rejected: %v", err)
	}
	if got.Durability != types.ObjectStoreDurabilityAsync {
		t.Fatalf("empty durability=%q, want runtime async default", got.Durability)
	}
}

func TestInstalledPolicyExpectationRejectsUnsupportedDerivedAzureBinding(t *testing.T) {
	config, _, _ := installedPolicyNodeFixture(t)
	config.ObjStoreProvider = "azure"
	if _, err := installedPolicyExpectation(&config); err == nil {
		t.Fatal("unsupported derived Azure binding was accepted")
	}
}

func TestNodeOpenFailsClosedForPinnedInstalledPolicy(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "node-data")
	n := New(&types.ExecutionConfig{
		ClusterID: "generation-7", NodeID: "node-a", DataDir: dataDir,
		ExpectedInstalledPolicyHash: [32]byte{1},
		ObjStoreProvider:            "s3", ObjStoreEndpoint: "http://127.0.0.1:1", ObjStoreBucket: "archive",
	})
	err := n.Open(context.Background())
	const want = "installed-policy runtime is not wired; refusing legacy Node.Open"
	if err == nil || err.Error() != want {
		t.Fatalf("Node.Open error=%v, want %q", err, want)
	}
	if _, statErr := os.Stat(dataDir); !os.IsNotExist(statErr) {
		t.Fatalf("pinned Node.Open created data directory before refusing: stat error=%v", statErr)
	}
	if n.wal != nil || n.bucket != nil || n.opened.Load() {
		t.Fatal("pinned Node.Open mutated runtime state before refusing")
	}
}

func TestNodeOpenLegacyLocalWithoutPolicyPinStillWorks(t *testing.T) {
	n := New(&types.ExecutionConfig{Local: true, NodeID: "local", DataDir: t.TempDir()})
	if err := n.Open(context.Background()); err != nil {
		t.Fatalf("legacy Local Node.Open: %v", err)
	}
	if !n.ready.Load() {
		t.Fatal("legacy Local node did not become ready")
	}
	if err := n.Shutdown(); err != nil {
		t.Fatalf("legacy Local Node.Shutdown: %v", err)
	}
}

func installedPolicyNodeFixture(t *testing.T) (types.ExecutionConfig, objstore.Bucket, []byte) {
	t.Helper()
	ctx := context.Background()
	secret := []byte(strings.Repeat("join-secret-", 4))
	member := quepaxa.Member{ID: "node-a", PeerURL: "quic://node-a.example:9090", PublicKey: quepaxa.PublicKey{1}}
	config := types.ExecutionConfig{
		ClusterID: "generation-7", Members: []quepaxa.Member{member},
		ExpectedInstalledPolicyHash: [32]byte{1}, ObjStoreProvider: "s3", ObjStoreEndpoint: "https://s3.example",
		ObjStoreBucket: "archive", ObjStorePrefix: "rhiza/root", ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck,
	}
	binding, err := (recoveryanchor.StorageBindingConfig{
		Policy: recoveryanchor.StorageBindingDerived, Provider: config.ObjStoreProvider,
		Endpoint: config.ObjStoreEndpoint, Bucket: config.ObjStoreBucket, Prefix: config.ObjStorePrefix,
	}).ExpectedBinding(string(config.ClusterID))
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := recovery.InstalledJoinCapabilityVerifier(string(config.ClusterID), secret)
	if err != nil {
		t.Fatal(err)
	}
	genesis, err := recovery.InstalledGenesisDigest(quepaxa.Cluster{ConfigID: 1, Members: config.Members}, recovery.InstalledPolicyProfileAppendOnly, config.ObjStoreDurability)
	if err != nil {
		t.Fatal(err)
	}
	record := recovery.InstalledPolicyRecord{
		ClusterID: string(config.ClusterID), StorageBindingID: binding.StorageID,
		InitialCluster: quepaxa.Cluster{ConfigID: 1, Members: config.Members},
		Profile:        recovery.InstalledPolicyProfileAppendOnly, Durability: config.ObjStoreDurability,
		JoinCapabilityVerifierHex: hex.EncodeToString(verifier[:]),
		Base:                      recovery.InstalledPolicyBase{Kind: recovery.InstalledPolicyBaseGenesis, GenesisDigest: hex.EncodeToString(genesis[:])},
	}
	// This matches Node.open: the factory returns an unprefixed provider bucket,
	// while callers construct object keys from ObjStorePrefix themselves.
	bucket := objstore.NewInMemBucket()
	digest, err := recovery.InstallInstalledPolicy(ctx, bucket, config.ObjStorePrefix, record)
	if err != nil {
		t.Fatal(err)
	}
	config.ExpectedInstalledPolicyHash = digest
	return config, bucket, secret
}
