package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func prepareInitialRegistrationFixture(t *testing.T, f *supervisorRegistrationFixture, candidateOrdinal ...int) fencing.ArchiveScope {
	t.Helper()
	ctx := t.Context()
	identity := f.identity
	ordinal := 0
	if len(candidateOrdinal) != 0 {
		ordinal = candidateOrdinal[0]
	}
	identity.NodeID, identity.PodName, identity.RunID = fmt.Sprintf("rhiza-%d", ordinal), fmt.Sprintf("rhiza-%d", ordinal), "initial-run"
	identity.PodUID = fmt.Sprintf("pod-%d", ordinal)
	f.identity = identity

	tokens := map[string]string{"rhiza-0": "peer-token-0", "rhiza-1": "peer-token-1", "rhiza-2": "peer-token-2"}
	members := make([]quepaxa.Member, 0, len(tokens))
	for _, id := range []string{"rhiza-0", "rhiza-1", "rhiza-2"} {
		members = append(members, quepaxa.Member{ID: quepaxa.NodeID(id), PublicKey: quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(identity.SourceClusterID), quepaxa.NodeID(id), tokens[id]))})
	}
	membersJSON, err := json.Marshal(members)
	if err != nil {
		t.Fatal(err)
	}
	tokensJSON, err := json.Marshal(tokens)
	if err != nil {
		t.Fatal(err)
	}

	func() {
		f.api.mu.Lock()
		defer f.api.mu.Unlock()
		setStartupEnv := func(container object) {
			setOperatorTestEnv(container, "RHIZA_CLUSTER_MEMBERS", string(membersJSON))
		}
		pod := f.api.authPod
		metadata := asObject(pod["metadata"])
		metadata["name"], metadata["uid"] = identity.PodName, identity.PodUID
		setStartupEnv(asObject(list(asObject(pod["spec"])["containers"])[0]))
		f.api.authPod, f.api.authPodName, f.api.authPodUID = pod, identity.PodName, identity.PodUID
		sts := f.api.authStatefulSet
		setStartupEnv(asObject(list(nested(sts, "spec", "template", "spec", "containers"))[0]))
		f.api.authStatefulSet = sts
		f.api.objects["/apis/apps/v1/namespaces/test/statefulsets"] = object{"items": []any{sts}}
		secret := f.api.objects["/api/v1/namespaces/test/secrets/peer-credentials"]
		asObject(secret["data"])["RHIZA_PEER_TOKENS"] = base64.StdEncoding.EncodeToString(tokensJSON)
	}()

	if err := f.e.initializeRoot(ctx, "sts-uid", identity.SourceClusterID); err != nil {
		t.Fatal(err)
	}
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: "archive", Prefix: path.Join("root", identity.SourceClusterID), ClusterID: identity.SourceClusterID}
	scopeHash, err := scope.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.state.ClaimArchiveScope(ctx, scope, recovery.ArchiveConfigID, "sts-uid", identity.SourceClusterID, hashJSON([]string{"initial-genesis-registration"})); err != nil {
		t.Fatal(err)
	}
	genesisRef := initialRegistrationTestGenesisRef(scopeHash)
	if err := f.state.SetArchiveScopeGenesisRef(ctx, scope, genesisRef); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.change(ctx, "sts-uid", identity.SourceClusterID, func(ledger *fencing.Ledger) error {
		ledger.ArchiveGenesisState = fencing.ArchiveGenesisCommitted
		ledger.ArchiveScopeHash = scopeHash
		ledger.ArchiveGenesisRef = &genesisRef
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	genesis, err := json.Marshal(recovery.NewMembershipRecord(identity.SourceClusterID, members, "async"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.e.ArchiveBucket.Upload(ctx, path.Join(scope.Prefix, "voters/membership.json"), bytes.NewReader(genesis), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	return scope
}

func setOperatorTestEnv(container object, name, value string) {
	env := list(container["env"])
	for _, raw := range env {
		entry := asObject(raw)
		if str(entry["name"]) == name {
			entry["value"] = value
			container["env"] = env
			return
		}
	}
	container["env"] = append(env, object{"name": name, "value": value})
}

func initialRegistrationTestGenesisRef(scopeHash string) fencing.ArchiveGenesisRef {
	producer := "genesis-producer"
	intent := fencing.MutationIntent{
		ContractVersion: fencing.MutationIntentVersion,
		StoreScopeHash:  hashJSON([]string{"genesis-store-scope"}),
		Condition:       fencing.MutationCondition{Mode: fencing.MutationCreateOnly},
		OperationID:     fencing.MutationRequestID(producer, 1),
		Kind:            "upload", Key: "archive/head.bin", PayloadRef: "genesis-payload",
		PayloadDigest: strings.Repeat("a", 64), PayloadSize: 1,
	}
	return fencing.ArchiveGenesisRef{Version: fencing.ArchiveGenesisVersion, ConfigID: recovery.ArchiveConfigID, ScopeHash: scopeHash,
		Mutation: &fencing.ArchiveGenesisMutation{ProducerUID: producer, Sequence: 1, Intent: intent}}
}

func initialRegistrationIdentityKey(identity fencing.Identity) string {
	hash := sha256.Sum256([]byte(identity.NodeID))
	return path.Join("root", identity.SourceClusterID, "voters", fmt.Sprintf("%x.json", hash[:]))
}

func TestAuthenticatedInitialRegistrationRequiresCanonicalGenesisAndRunBoundEvidence(t *testing.T) {
	f := newSupervisorRegistrationFixture(t, true)
	scope := prepareInitialRegistrationFixture(t, &f)
	ctx := t.Context()
	client := fencing.AuthorityClient{URL: f.server.URL + "/v1/supervisor", TokenFile: f.token, Client: f.server.Client()}
	if _, err := client.RegisterStartup(ctx, f.identity); err == nil {
		t.Fatal("ordinary active-voter registration bypassed initial-registration preflight")
	}
	root, _, err := f.state.ReadRoot(ctx, "sts-uid", f.identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if len(root.Ledger.Runs) != 0 {
		t.Fatalf("unmarked registration created a Run: %+v", root.Ledger.Runs)
	}

	grant, err := client.RegisterInitialStartup(ctx, f.identity)
	if err != nil {
		t.Fatalf("authenticated initial registration: %v", err)
	}
	if grant.Identity != f.identity || grant.InitialRegistration == nil || grant.InitialRegistration.ValidateForIdentity(f.identity) != nil {
		t.Fatalf("grant lacks exact authority-pinned initial registration evidence: %+v", grant)
	}
	root, _, err = f.state.ReadRoot(ctx, "sts-uid", f.identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run, ok := root.Ledger.Runs[f.identity.RunID]
	if !ok || run.Identity != f.identity || run.Identity.WALIdentity != "" || run.InitialRegistration == nil || *run.InitialRegistration != *grant.InitialRegistration || run.Runtime == nil {
		t.Fatalf("initial Run was not durably pinned before child WAL binding: %+v", run)
	}
	producer, producerUID, _, err := f.state.ReadProducer(ctx, *run.Runtime)
	if err != nil || producerUID == "" || producer.Phase != fencing.MutationReserved || producer.WALIdentity != "" {
		t.Fatalf("initial registration did not reserve an unused producer after pinning: uid=%q producer=%+v err=%v", producerUID, producer, err)
	}
	identityBytes, err := readInitialRegistrationObject(ctx, f.e.ArchiveBucket, initialRegistrationIdentityKey(f.identity))
	if !errors.Is(err, errInitialRegistrationObjectNotFound) || len(identityBytes) != 0 {
		t.Fatalf("initial admission unexpectedly created or reused voter identity: bytes=%d err=%v", len(identityBytes), err)
	}
	if grant.InitialRegistration.MembershipHash == "" || grant.InitialRegistration.StoreScopeHash != mustInitialScopeHash(t, scope) {
		t.Fatalf("grant did not pin exact canonical source/store evidence: %+v", grant.InitialRegistration)
	}
	firstRunSnapshot := hashJSON(run)
	firstProducerSnapshot := hashJSON(producer)
	retryGrant, err := client.RegisterInitialStartup(ctx, f.identity)
	if err != nil || hashJSON(retryGrant) != hashJSON(grant) {
		t.Fatalf("same physical Run retry after an uncertain response was not idempotent: grant=%+v retry=%+v err=%v", grant, retryGrant, err)
	}
	root, _, err = f.state.ReadRoot(ctx, "sts-uid", f.identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	retriedRun := root.Ledger.Runs[f.identity.RunID]
	retriedProducer, _, _, err := f.state.ReadProducer(ctx, *retriedRun.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	if hashJSON(retriedRun) != firstRunSnapshot || hashJSON(retriedProducer) != firstProducerSnapshot {
		t.Fatal("same-Run retry changed authority-pinned identity or producer state")
	}
}

func TestInitialRegistrationRejectsPresentEmptyIdentityObject(t *testing.T) {
	f := newSupervisorRegistrationFixture(t, true)
	_ = prepareInitialRegistrationFixture(t, &f)
	key := initialRegistrationIdentityKey(f.identity)
	if err := f.e.ArchiveBucket.Upload(t.Context(), key, bytes.NewReader(nil), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	client := fencing.AuthorityClient{URL: f.server.URL + "/v1/supervisor", TokenFile: f.token, Client: f.server.Client()}
	if _, err := client.RegisterInitialStartup(t.Context(), f.identity); err == nil {
		t.Fatal("present empty identity object was treated as authoritative NotFound")
	}
	root, _, err := f.state.ReadRoot(t.Context(), "sts-uid", f.identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if len(root.Ledger.Runs) != 0 {
		t.Fatalf("malformed identity object created a Run: %+v", root.Ledger.Runs)
	}
}

func TestInitialRegistrationConcurrentWALBindingHasOneWinner(t *testing.T) {
	f := newSupervisorRegistrationFixture(t, true)
	prepareInitialRegistrationFixture(t, &f)
	ctx := t.Context()
	client := fencing.AuthorityClient{URL: f.server.URL + "/v1/supervisor", TokenFile: f.token, Client: f.server.Client()}
	if _, err := client.RegisterInitialStartup(ctx, f.identity); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		identity fencing.Identity
		err      error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	for _, nonce := range []string{strings.Repeat("a", 64), strings.Repeat("b", 64)} {
		identity := f.identity
		identity.WALIdentity = nonce
		// Competing children have independent client-side binding caches.
		bindingClient := client
		go func() {
			<-start
			_, err := bindingClient.Bind(ctx, identity)
			results <- outcome{identity: identity, err: err}
		}()
	}
	close(start)
	var winner fencing.Identity
	successes := 0
	for range 2 {
		result := <-results
		if result.err == nil {
			successes++
			winner = result.identity
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent WAL bindings succeeded %d times, want exactly one", successes)
	}
	if _, err := client.Bind(ctx, winner); err != nil {
		t.Fatalf("exact winning binding retry failed: %v", err)
	}
	root, _, err := f.state.ReadRoot(ctx, "sts-uid", f.identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run := root.Ledger.Runs[f.identity.RunID]
	if run.Identity != winner || run.Runtime == nil {
		t.Fatal("canonical Run does not retain the winning WAL identity")
	}
	producer, _, _, err := f.state.ReadProducer(ctx, *run.Runtime)
	if err != nil || producer.WALIdentity != winner.WALIdentity || producer.Phase != fencing.MutationOpen {
		t.Fatalf("producer disagrees with winning WAL binding: err=%v", err)
	}
	// The remote registration is still absent: a lost local WAL after binding
	// must not regain first-use authority merely because publication never ran.
	if _, err := client.RegisterInitialStartup(ctx, f.identity); err == nil {
		t.Fatal("bound Run with absent remote registration regained first-use authority")
	}
}

func TestInitialRegistrationRejectsMissingRootMismatchedGenesisAndExistingRegistration(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *supervisorRegistrationFixture, fencing.ArchiveScope)
	}{
		{
			name: "lost canonical root",
			mutate: func(t *testing.T, f *supervisorRegistrationFixture, _ fencing.ArchiveScope) {
				t.Helper()
				key, _, err := f.state.rootKey("sts-uid", f.identity.SourceClusterID)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.state.bucket.Delete(t.Context(), key); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "genesis membership mismatch",
			mutate: func(t *testing.T, f *supervisorRegistrationFixture, scope fencing.ArchiveScope) {
				t.Helper()
				key := path.Join(scope.Prefix, "voters/membership.json")
				if err := f.e.ArchiveBucket.Delete(t.Context(), key); err != nil {
					t.Fatal(err)
				}
				wrong, err := json.Marshal(recovery.NewMembershipRecord("different-source", nil, "async"))
				if err != nil {
					t.Fatal(err)
				}
				if err := f.e.ArchiveBucket.Upload(t.Context(), key, bytes.NewReader(wrong), objstore.WithIfNotExists()); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "own registration already exists",
			mutate: func(t *testing.T, f *supervisorRegistrationFixture, _ fencing.ArchiveScope) {
				t.Helper()
				if err := f.e.ArchiveBucket.Upload(t.Context(), initialRegistrationIdentityKey(f.identity), bytes.NewReader([]byte("existing")), objstore.WithIfNotExists()); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newSupervisorRegistrationFixture(t, true)
			scope := prepareInitialRegistrationFixture(t, &f)
			tt.mutate(t, &f, scope)
			client := fencing.AuthorityClient{URL: f.server.URL + "/v1/supervisor", TokenFile: f.token, Client: f.server.Client()}
			if _, err := client.RegisterInitialStartup(t.Context(), f.identity); err == nil {
				t.Fatal("invalid or previously used initial-registration state was admitted")
			}
			if tt.name == "lost canonical root" {
				if _, _, err := f.state.ReadRoot(t.Context(), "sts-uid", f.identity.SourceClusterID); !errors.Is(err, errAuthorityRootLost) {
					t.Fatalf("fixture did not preserve root-loss evidence: %v", err)
				}
				return
			}
			root, _, err := f.state.ReadRoot(t.Context(), "sts-uid", f.identity.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			if len(root.Ledger.Runs) != 0 {
				t.Fatalf("rejected initial registration created a Run: %+v", root.Ledger.Runs)
			}
		})
	}
}

func TestAuthenticatedInitialRegistrationAcceptsLateStatefulSetOrdinal(t *testing.T) {
	f := newSupervisorRegistrationFixture(t, true)
	_ = prepareInitialRegistrationFixture(t, &f, 2)
	client := fencing.AuthorityClient{URL: f.server.URL + "/v1/supervisor", TokenFile: f.token, Client: f.server.Client()}
	grant, err := client.RegisterInitialStartup(t.Context(), f.identity)
	if err != nil {
		t.Fatalf("authenticated initial registration for ordinal 2: %v", err)
	}
	if grant.Identity != f.identity || grant.InitialRegistration == nil || grant.InitialRegistration.ValidateForIdentity(f.identity) != nil {
		t.Fatalf("late ordinal grant does not bind the physical identity: %+v", grant)
	}
	root, _, err := f.state.ReadRoot(t.Context(), "sts-uid", f.identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run, ok := root.Ledger.Runs[f.identity.RunID]
	if !ok || run.Identity != f.identity || run.InitialRegistration == nil || *run.InitialRegistration != *grant.InitialRegistration {
		t.Fatalf("late ordinal was not pinned in canonical Run evidence: %+v", run)
	}
}

func TestInitialRegistrationCloseBeforeCASDoesNotIssueGrantOrRun(t *testing.T) {
	f := newSupervisorRegistrationFixture(t, true)
	_ = prepareInitialRegistrationFixture(t, &f)
	f.backend.beforeReturn = func(ctx context.Context, _ fencing.RuntimeBinding) error {
		_, err := f.e.change(ctx, "sts-uid", f.identity.SourceClusterID, func(ledger *fencing.Ledger) error {
			return ledger.Close("close-during-enrollment", strings.Repeat("c", 64))
		})
		return err
	}
	client := fencing.AuthorityClient{URL: f.server.URL + "/v1/supervisor", TokenFile: f.token, Client: f.server.Client()}
	if _, err := client.RegisterInitialStartup(t.Context(), f.identity); !errors.Is(err, fencing.ErrClosed) {
		t.Fatalf("root closure racing enrollment should deny the grant, got %v", err)
	}
	root, _, err := f.state.ReadRoot(t.Context(), "sts-uid", f.identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if root.Ledger.ClosedBy != "close-during-enrollment" || len(root.Ledger.Runs) != 0 {
		t.Fatalf("closed-root CAS race registered a Run: closedBy=%q runs=%+v", root.Ledger.ClosedBy, root.Ledger.Runs)
	}
	if _, _, _, err := f.state.ReadProducer(t.Context(), fencing.RuntimeBinding{RunID: f.identity.RunID, StatefulSetUID: "sts-uid", SourceClusterID: f.identity.SourceClusterID}); err == nil {
		t.Fatal("closed-root CAS race reserved a producer")
	}
}

func mustInitialScopeHash(t *testing.T, scope fencing.ArchiveScope) string {
	t.Helper()
	hash, err := scope.Hash()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := hex.DecodeString(hash)
	if err != nil || len(decoded) != sha256.Size {
		t.Fatalf("scope hash is invalid: %q", hash)
	}
	return hash
}
