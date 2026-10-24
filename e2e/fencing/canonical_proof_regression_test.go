package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/thanos-io/objstore"
)

const (
	canonicalTestSTS       = "canonical-proof-sts"
	canonicalTestSource    = "chaos-source"
	canonicalTestBinding   = "canonical-proof-binding"
	canonicalTestPrefix    = "canonical-proof-authority"
	canonicalTestOperation = "canonical-voter-operation"
)

var errCanonicalTestPermission = errors.New("injected canonical read permission failure")

type canonicalProofBucket struct {
	objstore.Bucket
	denyRootRead    bool
	corruptRootRead bool
}

func (b *canonicalProofBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if b.denyRootRead && strings.Contains(name, "/rhiza-authority/v1/roots/") {
		return objstore.ObjectAttributes{}, errCanonicalTestPermission
	}
	return b.Bucket.Attributes(ctx, name)
}

func (b *canonicalProofBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if b.corruptRootRead && strings.Contains(name, "/rhiza-authority/v1/roots/") {
		return io.NopCloser(strings.NewReader("not-a-canonical-envelope")), nil
	}
	return b.Bucket.Get(ctx, name)
}

func seedCanonicalRoot(t *testing.T, store *operator.AuthorityStateStore, state fenceLedgerState) {
	t.Helper()
	root, _, _ := store.ReadRoot(t.Context(), state.StatefulSetUID, state.Ledger.SourceClusterID)
	root.StatefulSetUID = state.StatefulSetUID
	root.Ledger = state.Ledger
	if err := store.WriteRoot(t.Context(), state.StatefulSetUID, state.Ledger.SourceClusterID, root, nil); err != nil {
		t.Fatalf("write in-memory canonical root: %v", err)
	}
}

func canonicalVoterProof() (operator.FenceRequest, operator.FenceProof, fenceLedgerState, []operator.FenceTarget, []string) {
	target := operator.FenceTarget{NodeID: "rhiza-2", Pod: "rhiza-2", PodUID: "old-voter-uid", WALIdentity: "old-voter-wal"}
	request := operator.FenceRequest{
		LogicalID: "fencing-e2e", BindingUID: canonicalTestBinding, Namespace: namespace,
		StatefulSet: "rhiza", StatefulSetUID: canonicalTestSTS, SourceClusterID: canonicalTestSource,
		OperationID: canonicalTestOperation, Scope: operator.ScopeVoter, Targets: []operator.FenceTarget{target},
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(encoded(request)))
	stop := fencing.StopRequest{OperationID: request.OperationID, RequestHash: hash, Identity: fencing.Identity{
		SourceClusterID: request.SourceClusterID, NodeID: target.NodeID, PodUID: target.PodUID,
		RunID: "old-voter-run", WALIdentity: target.WALIdentity,
	}}
	state := fenceLedgerState{StatefulSetUID: request.StatefulSetUID, Ledger: fencing.Ledger{
		SourceClusterID: request.SourceClusterID,
		ClosedNodes:     map[string]fencing.StopRequest{target.NodeID: stop},
		Runs: map[string]fencing.Run{stop.Identity.RunID: {
			Identity: stop.Identity, Stop: &stop,
			Drain: &fencing.DrainReceipt{Request: stop, WritersJoined: true}, Exited: true,
		}},
	}}
	proof := operator.FenceProof{
		OperationID: request.OperationID, SourceClusterID: request.SourceClusterID,
		BindingUID: request.BindingUID, RequestHash: hash, ProofID: "cooperative-" + strings.Repeat("a", 64),
		ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true,
	}
	return request, proof, state, []operator.FenceTarget{target}, []string{target.PodUID}
}

func TestCanonicalRootReadVerifiesVoterProofWithoutLedgerConfigMap(t *testing.T) {
	ctx := context.Background()
	base := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = base.Close() })
	store, err := operator.NewAuthorityStateStore(base, canonicalTestPrefix)
	if err != nil {
		t.Fatal(err)
	}
	request, proof, state, targets, oldUIDs := canonicalVoterProof()
	seedCanonicalRoot(t, store, state)
	legacyCalls := 0
	legacy := func(context.Context, string, string) (fenceLedgerState, error) {
		legacyCalls++
		return state, nil
	}
	reader := configuredProofLedgerReader(store, true, legacy)
	out := t.TempDir()
	item := obj{"spec": request, "status": obj{"proof": proof}}
	if err := verifyScopedProofWithReader(ctx, item, canonicalTestSTS, canonicalTestBinding, oldUIDs, operator.ScopeVoter, targets, out, reader); err != nil {
		t.Fatalf("canonical ObjectStorage root did not validate exact voter proof: %v", err)
	}
	if legacyCalls != 0 {
		t.Fatalf("canonical read unexpectedly consulted legacy ledger %d times", legacyCalls)
	}
	if _, err := os.Stat(filepath.Join(out, "validated-voter-proof.json")); err != nil {
		t.Fatalf("validated voter proof artifact missing: %v", err)
	}
}

func TestCanonicalRootErrorsNeverFallBackToTemptingLegacyLedger(t *testing.T) {
	ctx := context.Background()
	_, _, state, _, _ := canonicalVoterProof()
	tests := []struct {
		name   string
		prefix string
		seed   bool
		wrap   func(objstore.Bucket) objstore.Bucket
	}{
		{name: "missing root", prefix: canonicalTestPrefix},
		{name: "corrupt root", prefix: canonicalTestPrefix, seed: true, wrap: func(bucket objstore.Bucket) objstore.Bucket {
			return &canonicalProofBucket{Bucket: bucket, corruptRootRead: true}
		}},
		{name: "permission denied", prefix: canonicalTestPrefix, seed: true, wrap: func(bucket objstore.Bucket) objstore.Bucket {
			return &canonicalProofBucket{Bucket: bucket, denyRootRead: true}
		}},
		{name: "wrong prefix", prefix: canonicalTestPrefix + "-wrong", seed: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := objstore.NewInMemBucket()
			t.Cleanup(func() { _ = base.Close() })
			seedStore, err := operator.NewAuthorityStateStore(base, canonicalTestPrefix)
			if err != nil {
				t.Fatal(err)
			}
			if test.seed {
				seedCanonicalRoot(t, seedStore, state)
			}
			var bucket objstore.Bucket = base
			if test.wrap != nil {
				bucket = test.wrap(base)
			}
			store, err := operator.NewAuthorityStateStore(bucket, test.prefix)
			if err != nil {
				t.Fatal(err)
			}
			legacyCalls := 0
			legacy := func(context.Context, string, string) (fenceLedgerState, error) {
				legacyCalls++
				return state, nil
			}
			reader := configuredProofLedgerReader(store, true, legacy)
			if _, err := reader(ctx, canonicalTestSTS, canonicalTestSource); err == nil {
				t.Fatal("canonical root failure was accepted")
			}
			if legacyCalls != 0 {
				t.Fatalf("canonical error fell back to tempting legacy ledger %d times", legacyCalls)
			}
		})
	}
}

func TestLegacyLedgerIsUsedOnlyWhenExplicitAndCanonicalStoreIsAbsent(t *testing.T) {
	_, _, state, _, _ := canonicalVoterProof()
	legacyCalls := 0
	legacy := func(context.Context, string, string) (fenceLedgerState, error) {
		legacyCalls++
		return state, nil
	}
	reader := configuredProofLedgerReader(nil, true, legacy)
	got, err := reader(context.Background(), canonicalTestSTS, canonicalTestSource)
	if err != nil || got.StatefulSetUID != state.StatefulSetUID || legacyCalls != 1 {
		t.Fatalf("explicit legacy-only reader did not use legacy source: state=%+v calls=%d err=%v", got, legacyCalls, err)
	}
	legacyCalls = 0
	reader = configuredProofLedgerReader(nil, false, legacy)
	if _, err := reader(context.Background(), canonicalTestSTS, canonicalTestSource); err == nil {
		t.Fatal("unconfigured reader treated legacy ledger as authority")
	}
	if legacyCalls != 0 {
		t.Fatalf("implicit legacy lookup ran %d times", legacyCalls)
	}
}

func activeRegistrationLedger() fenceLedgerState {
	state := fenceLedgerState{StatefulSetUID: canonicalTestSTS, Ledger: fencing.Ledger{
		SourceClusterID: canonicalTestSource,
		Runs:            make(map[string]fencing.Run),
	}}
	for i, node := range []string{"rhiza-0", "rhiza-1", "learner"} {
		runID := fmt.Sprintf("active-run-%d", i)
		identity := fencing.Identity{SourceClusterID: canonicalTestSource, NodeID: node, PodUID: node + "-uid", RunID: runID, WALIdentity: node + "-wal"}
		state.Ledger.Runs[runID] = fencing.Run{Identity: identity}
	}
	return state
}

func TestRegisteredTargetsReadCanonicalRootAndRequireEachExactActiveVoter(t *testing.T) {
	ctx := context.Background()
	targets := []operator.FenceTarget{
		{NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: "rhiza-0-uid", WALIdentity: "rhiza-0-wal"},
		{NodeID: "rhiza-1", Pod: "rhiza-1", PodUID: "rhiza-1-uid", WALIdentity: "rhiza-1-wal"},
		{NodeID: "learner", Pod: "learner", PodUID: "learner-uid", WALIdentity: "learner-wal"},
	}
	for _, name := range []string{"exact active set", "captured subset while another active voter remains"} {
		name := name
		t.Run(name, func(t *testing.T) {
			base := objstore.NewInMemBucket()
			t.Cleanup(func() { _ = base.Close() })
			store, err := operator.NewAuthorityStateStore(base, canonicalTestPrefix)
			if err != nil {
				t.Fatal(err)
			}
			seedCanonicalRoot(t, store, activeRegistrationLedger())
			reader := configuredProofLedgerReader(store, true, nil)
			want := targets
			if name == "captured subset while another active voter remains" {
				want = targets[:2]
			}
			if err := verifyRegisteredTargets(ctx, want, canonicalTestSTS, t.TempDir(), reader); err != nil {
				t.Fatalf("canonical active registrations were rejected: %v", err)
			}
		})
	}
	for _, test := range []struct {
		name    string
		targets []operator.FenceTarget
		mutate  func(*fenceLedgerState)
	}{
		{name: "stale uid", targets: []operator.FenceTarget{targets[0], targets[1], {NodeID: "learner", Pod: "learner", PodUID: "stale-uid", WALIdentity: "learner-wal"}}},
		{name: "stale wal", targets: []operator.FenceTarget{targets[0], targets[1], {NodeID: "learner", Pod: "learner", PodUID: "learner-uid", WALIdentity: "stale-wal"}}},
		{name: "missing learner ledger entry", targets: targets, mutate: func(state *fenceLedgerState) {
			delete(state.Ledger.Runs, "active-run-2")
		}},
		{name: "stopped run", targets: targets, mutate: func(state *fenceLedgerState) {
			run := state.Ledger.Runs["active-run-2"]
			stop := fencing.StopRequest{OperationID: "stopped", RequestHash: "hash", Identity: run.Identity}
			run.Stop = &stop
			state.Ledger.Runs["active-run-2"] = run
		}},
		{name: "exited run", targets: targets, mutate: func(state *fenceLedgerState) {
			run := state.Ledger.Runs["active-run-2"]
			run.Exited = true
			state.Ledger.Runs["active-run-2"] = run
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := objstore.NewInMemBucket()
			t.Cleanup(func() { _ = base.Close() })
			store, err := operator.NewAuthorityStateStore(base, canonicalTestPrefix)
			if err != nil {
				t.Fatal(err)
			}
			state := activeRegistrationLedger()
			if test.mutate != nil {
				test.mutate(&state)
			}
			seedCanonicalRoot(t, store, state)
			reader := configuredProofLedgerReader(store, true, nil)
			if err := verifyRegisteredTargets(ctx, test.targets, canonicalTestSTS, t.TempDir(), reader); err == nil {
				t.Fatal("accepted incomplete or non-current active registration")
			}
		})
	}
}

func TestScopedProofRejectsInvalidBindingBeforeCanonicalRead(t *testing.T) {
	request, proof, _, targets, oldUIDs := canonicalVoterProof()
	request.BindingUID = "different-binding"
	proof.RequestHash = fmt.Sprintf("%x", sha256.Sum256(encoded(request)))
	reads := 0
	reader := proofLedgerReader(func(context.Context, string, string) (fenceLedgerState, error) {
		reads++
		return fenceLedgerState{}, nil
	})
	err := verifyScopedProofWithReader(context.Background(), obj{"spec": request, "status": obj{"proof": proof}}, canonicalTestSTS, canonicalTestBinding, oldUIDs, operator.ScopeVoter, targets, t.TempDir(), reader)
	if err == nil {
		t.Fatal("accepted request with incorrect binding")
	}
	if reads != 0 {
		t.Fatalf("invalid binding reached canonical store %d times", reads)
	}
}

func TestAuthorityStatePrefixUsesActualFencerArgsAndDefault(t *testing.T) {
	deployment := func(args, env []any) obj {
		return obj{"spec": obj{"template": obj{"spec": obj{"containers": []any{obj{
			"name": "authority", "args": args, "env": env,
			"envFrom": []any{obj{"configMapRef": obj{"name": "rhiza-config"}}, obj{"secretRef": obj{"name": "rhiza-object-store"}}},
		}}}}}}
	}
	for _, test := range []struct {
		name string
		args []any
		env  []any
		want string
	}{
		{name: "default", want: "rhiza-authority-state"},
		{name: "environment override", env: []any{obj{"name": "FENCER_AUTHORITY_STATE_PREFIX", "value": "env-prefix"}}, want: "env-prefix"},
		{name: "argument overrides environment", args: []any{"serve", "--authority-state-prefix=arg-prefix"}, env: []any{obj{"name": "FENCER_AUTHORITY_STATE_PREFIX", "value": "env-prefix"}}, want: "arg-prefix"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := fencerAuthorityStatePrefix(deployment(test.args, test.env))
			if err != nil || got != test.want {
				t.Fatalf("authority prefix = %q, want %q: %v", got, test.want, err)
			}
		})
	}
}
