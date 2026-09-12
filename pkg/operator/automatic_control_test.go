package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

// --- helpers ---

func autoBucket(t *testing.T) *objstore.InMemBucket {
	t.Helper()
	return objstore.NewInMemBucket()
}

func autoController(bucket *objstore.InMemBucket) *Controller {
	return &Controller{Bucket: bucket, Prefix: "root", Kube: &Kubernetes{Namespace: "default"}}
}

func autoClusterResource(name, uid, logicalID string) *ClusterResource {
	return &ClusterResource{
		APIVersion: "rhiza.mrchypark.dev/v1alpha1",
		Kind:       "RhizaCluster",
		Metadata:   map[string]any{"name": name, "uid": uid, "generation": float64(1)},
		Spec: ClusterSpec{
			LogicalID:      logicalID,
			StatefulSet:    "rhiza",
			Container:      "rhiza",
			AdoptClusterID: "source",
			VoterPods:      []VoterPod{{NodeID: "n1", Pod: "rhiza-0"}, {NodeID: "n2", Pod: "rhiza-1"}, {NodeID: "n3", Pod: "rhiza-2"}},
		},
	}
}

func validAutoControl() *autoControl {
	return &autoControl{
		Version:         1,
		BindingUID:      "binding-uid-1",
		BindingHash:     "hash-1",
		StatefulSetUID:  "sts-uid-1",
		ActiveClusterID: "source",
		Voters:          []VoterPod{{NodeID: "n1", Pod: "rhiza-0"}, {NodeID: "n2", Pod: "rhiza-1"}, {NodeID: "n3", Pod: "rhiza-2"}},
		Identities:      map[quepaxa.NodeID]FenceTarget{},
		Sequence:        0,
	}
}

func TestLoadAutoSaveAutoRoundTrip(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	ctx := context.Background()
	key := c.autoKey(r)

	// Save initial record (version=nil → IfNotExists).
	state := validAutoControl()
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}

	// Load it back.
	loaded, version, err := c.loadAuto(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version != 1 || loaded.BindingUID != "binding-uid-1" || loaded.ActiveClusterID != "source" {
		t.Fatalf("loaded fields mismatch: %+v", loaded)
	}
	if version == nil {
		t.Fatal("version should not be nil after save")
	}
}

func TestSaveAutoCASWithCorrectVersion(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	ctx := context.Background()

	// Create.
	state := validAutoControl()
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}

	// Load to get version.
	_, version, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}

	// CAS with correct version — should succeed.
	state.Sequence = 1
	if err := c.saveAuto(ctx, r, state, version); err != nil {
		t.Fatalf("CAS with correct version should succeed: %v", err)
	}

	// Verify the update stuck.
	loaded, _, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Sequence != 1 {
		t.Fatalf("expected Sequence=1 after CAS, got %d", loaded.Sequence)
	}
}

func TestSaveAutoCASRejectsStaleVersion(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	ctx := context.Background()

	// Create.
	state := validAutoControl()
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}

	// Load v1.
	_, v1, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}

	// CAS with v1 → succeeds, bumps to v2.
	state.Sequence = 1
	if err := c.saveAuto(ctx, r, state, v1); err != nil {
		t.Fatal(err)
	}

	// CAS with stale v1 again → should fail.
	state.Sequence = 2
	err = c.saveAuto(ctx, r, state, v1)
	if err == nil {
		t.Fatal("CAS with stale version should fail")
	}
	if !bucket.IsConditionNotMetErr(err) {
		t.Fatalf("expected condition-not-met error, got: %v", err)
	}
}

func TestLoadAutoRejectsMalformedJSON(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	ctx := context.Background()
	key := c.autoKey(r)

	// Write garbage directly.
	if err := bucket.Upload(ctx, key, bytes.NewReader([]byte("{not json")), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}

	_, _, err := c.loadAuto(ctx, key)
	if err == nil {
		t.Fatal("loadAuto should reject malformed JSON")
	}
}

func TestLoadAutoRejectsUnknownFields(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	ctx := context.Background()
	key := c.autoKey(r)

	// Write JSON with unknown fields.
	record := map[string]any{
		"version":         float64(1),
		"bindingUID":      "b1",
		"bindingHash":     "h1",
		"statefulSetUID":  "s1",
		"activeClusterID": "source",
		"identities":      map[string]any{},
		"unknownField":    "should-fail",
	}
	data, _ := json.Marshal(record)
	if err := bucket.Upload(ctx, key, bytes.NewReader(data), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}

	_, _, err := c.loadAuto(ctx, key)
	if err == nil {
		t.Fatal("loadAuto should reject unknown fields")
	}
}

func TestLoadAutoRejectsWrongVersion(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	ctx := context.Background()
	key := c.autoKey(r)

	record := validAutoControl()
	record.Version = 2 // invalid — must be 1
	data, _ := json.Marshal(record)
	if err := bucket.Upload(ctx, key, bytes.NewReader(data), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}

	_, _, err := c.loadAuto(ctx, key)
	if err == nil {
		t.Fatal("loadAuto should reject version != 1")
	}
}

func TestLoadAutoRejectsMissingBindingUID(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	ctx := context.Background()
	key := c.autoKey(r)

	record := validAutoControl()
	record.BindingUID = "" // required
	data, _ := json.Marshal(record)
	if err := bucket.Upload(ctx, key, bytes.NewReader(data), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}

	_, _, err := c.loadAuto(ctx, key)
	if err == nil {
		t.Fatal("loadAuto should reject empty BindingUID")
	}
}

func TestLoadAutoRejectsMissingActiveClusterID(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	ctx := context.Background()
	key := c.autoKey(r)

	record := validAutoControl()
	record.ActiveClusterID = "" // required
	data, _ := json.Marshal(record)
	if err := bucket.Upload(ctx, key, bytes.NewReader(data), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}

	_, _, err := c.loadAuto(ctx, key)
	if err == nil {
		t.Fatal("loadAuto should reject empty ActiveClusterID")
	}
}

func TestLoadAutoRejectsNilIdentities(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	ctx := context.Background()
	key := c.autoKey(r)

	record := validAutoControl()
	record.Identities = nil // required non-nil
	data, _ := json.Marshal(record)
	if err := bucket.Upload(ctx, key, bytes.NewReader(data), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}

	_, _, err := c.loadAuto(ctx, key)
	if err == nil {
		t.Fatal("loadAuto should reject nil Identities")
	}
}

func TestAuthorizeAutomaticChildRejectsWrongChildUID(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	ctx := context.Background()

	// Persist a control record with a valid intent.
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	state := validAutoControl()
	desiredSpec := Spec{StatefulSet: "rhiza", SourceClusterID: "source", Durability: "before-ack", RecoveryID: "auto-r1"}
	state.Intent = &autoIntent{
		RecoveryName: "auto-r1",
		RecoveryUID:  "child-uid-exact",
		SpecHash:     hashJSON(desiredSpec),
		Proof:        &FenceProof{ProofID: "proof-1"},
	}
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}

	// Resource with matching owner but wrong UID.
	child := &Resource{
		Metadata: map[string]any{
			"name": "auto-r1",
			"uid":  "wrong-uid",
			"annotations": map[string]any{
				automaticOwner:   state.BindingUID,
				automaticLogical: "logical-1",
			},
		},
		Spec: desiredSpec,
	}

	ok, err := c.authorizeAutomaticChild(ctx, child)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should reject wrong child UID")
	}
}

func TestAuthorizeAutomaticChildRejectsWrongSpecHash(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	ctx := context.Background()

	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	state := validAutoControl()
	desiredSpec := Spec{StatefulSet: "rhiza", SourceClusterID: "source", Durability: "before-ack", RecoveryID: "auto-r1"}
	state.Intent = &autoIntent{
		RecoveryName: "auto-r1",
		RecoveryUID:  "child-uid-exact",
		SpecHash:     hashJSON(desiredSpec),
		Proof:        &FenceProof{ProofID: "proof-1"},
	}
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}

	// Resource with matching owner and UID but different spec.
	child := &Resource{
		Metadata: map[string]any{
			"name": "auto-r1",
			"uid":  "child-uid-exact",
			"annotations": map[string]any{
				automaticOwner:   state.BindingUID,
				automaticLogical: "logical-1",
			},
		},
		Spec: Spec{StatefulSet: "other-set", SourceClusterID: "source", Durability: "before-ack", RecoveryID: "auto-r1"},
	}

	ok, err := c.authorizeAutomaticChild(ctx, child)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should reject wrong spec hash")
	}
}

func TestAuthorizeAutomaticChildRejectsWrongOwner(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	ctx := context.Background()

	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	state := validAutoControl()
	desiredSpec := Spec{StatefulSet: "rhiza", SourceClusterID: "source", Durability: "before-ack", RecoveryID: "auto-r1"}
	state.Intent = &autoIntent{
		RecoveryName: "auto-r1",
		RecoveryUID:  "child-uid-exact",
		SpecHash:     hashJSON(desiredSpec),
		Proof:        &FenceProof{ProofID: "proof-1"},
	}
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}

	// Resource with wrong owner binding UID.
	child := &Resource{
		Metadata: map[string]any{
			"name": "auto-r1",
			"uid":  "child-uid-exact",
			"annotations": map[string]any{
				automaticOwner:   "wrong-binding-uid",
				automaticLogical: "logical-1",
			},
		},
		Spec: desiredSpec,
	}

	ok, err := c.authorizeAutomaticChild(ctx, child)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should reject wrong owner")
	}
}

func TestAuthorizeAutomaticChildAcceptsExactPersistedIntent(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	ctx := context.Background()

	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	state := validAutoControl()
	desiredSpec := Spec{StatefulSet: "rhiza", SourceClusterID: "source", Durability: "before-ack", RecoveryID: "auto-r1"}
	request := FenceRequest{
		LogicalID:       "logical-1",
		BindingUID:      state.BindingUID,
		Namespace:       "default",
		StatefulSet:     "rhiza",
		StatefulSetUID:  "sts-uid-1",
		SourceClusterID: "source",
		OperationID:     "op-1",
		Scope:           ScopeGeneration,
		Targets:         []FenceTarget{{NodeID: "n1", Pod: "rhiza-0"}, {NodeID: "n2", Pod: "rhiza-1"}, {NodeID: "n3", Pod: "rhiza-2"}},
	}
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	state.Intent = &autoIntent{
		RecoveryName: "auto-r1",
		RecoveryUID:  "child-uid-exact",
		SpecHash:     hashJSON(desiredSpec),
		Request:      request,
		Proof: &FenceProof{
			OperationID:         request.OperationID,
			SourceClusterID:     request.SourceClusterID,
			BindingUID:          request.BindingUID,
			RequestHash:         hash,
			ProofID:             "proof-1",
			ProcessesTerminated: true,
			RecreationBlocked:   true,
			StorageQuiesced:     true,
		},
	}
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}

	// Exact match: same owner, UID, name, spec.
	child := &Resource{
		Metadata: map[string]any{
			"name": "auto-r1",
			"uid":  "child-uid-exact",
			"annotations": map[string]any{
				automaticOwner:   state.BindingUID,
				automaticLogical: "logical-1",
			},
		},
		Spec: desiredSpec,
	}

	ok, err := c.authorizeAutomaticChild(ctx, child)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("should accept exact persisted intent")
	}
}

func TestAuthorizeAutomaticChildReturnsTrueWithoutAnnotation(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	ctx := context.Background()

	// No automaticOwner annotation → not an automatic child → allowed.
	child := &Resource{
		Metadata: map[string]any{"name": "manual-recovery", "uid": "uid-1"},
		Spec:     Spec{StatefulSet: "rhiza", SourceClusterID: "source"},
	}

	ok, err := c.authorizeAutomaticChild(ctx, child)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("non-automatic child should be allowed")
	}
}

func TestAuthorizeAutomaticChildRejectsNoProof(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	ctx := context.Background()

	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	state := validAutoControl()
	desiredSpec := Spec{StatefulSet: "rhiza", SourceClusterID: "source", Durability: "before-ack", RecoveryID: "auto-r1"}
	state.Intent = &autoIntent{
		RecoveryName: "auto-r1",
		RecoveryUID:  "child-uid-exact",
		SpecHash:     hashJSON(desiredSpec),
		Proof:        nil, // no proof yet
	}
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}

	child := &Resource{
		Metadata: map[string]any{
			"name": "auto-r1",
			"uid":  "child-uid-exact",
			"annotations": map[string]any{
				automaticOwner:   state.BindingUID,
				automaticLogical: "logical-1",
			},
		},
		Spec: desiredSpec,
	}

	ok, err := c.authorizeAutomaticChild(ctx, child)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should reject intent without proof")
	}
}

func TestAuthorizeAutomaticChildRejectsIntentNil(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	ctx := context.Background()

	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	state := validAutoControl()
	state.Intent = nil // no intent
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}

	child := &Resource{
		Metadata: map[string]any{
			"name": "auto-r1",
			"uid":  "child-uid-exact",
			"annotations": map[string]any{
				automaticOwner:   state.BindingUID,
				automaticLogical: "logical-1",
			},
		},
		Spec: Spec{StatefulSet: "rhiza", SourceClusterID: "source"},
	}

	ok, err := c.authorizeAutomaticChild(ctx, child)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should reject nil intent")
	}
}

func TestAuthorizeAutomaticChildRejectsWrongRecoveryName(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	ctx := context.Background()

	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	state := validAutoControl()
	desiredSpec := Spec{StatefulSet: "rhiza", SourceClusterID: "source", Durability: "before-ack", RecoveryID: "auto-r1"}
	state.Intent = &autoIntent{
		RecoveryName: "auto-r1",
		RecoveryUID:  "child-uid-exact",
		SpecHash:     hashJSON(desiredSpec),
		Proof:        &FenceProof{ProofID: "proof-1"},
	}
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}

	// Resource name differs from intent RecoveryName.
	child := &Resource{
		Metadata: map[string]any{
			"name": "different-name",
			"uid":  "child-uid-exact",
			"annotations": map[string]any{
				automaticOwner:   state.BindingUID,
				automaticLogical: "logical-1",
			},
		},
		Spec: desiredSpec,
	}

	ok, err := c.authorizeAutomaticChild(ctx, child)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("should reject wrong recovery name")
	}
}

func TestLoadAutoTraversesCorrectKeyPath(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	ctx := context.Background()

	state := validAutoControl()
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}

	// Verify the key path matches expected structure.
	expected := "root/automatic/default/logical-1/control.json"
	exists, err := bucket.Exists(ctx, expected)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatalf("expected key %q not found in bucket", expected)
	}
}

func TestLoadAutoDetectsConcurrentWrite(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	ctx := context.Background()
	key := c.autoKey(r)

	// Save v1.
	state := validAutoControl()
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}

	// Load to get v1.
	_, v1, err := c.loadAuto(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	// Another writer CAS with v1 → gets v2.
	state.Sequence = 10
	if err := c.saveAuto(ctx, r, state, v1); err != nil {
		t.Fatal(err)
	}

	// loadAuto should see v2 and not v1.
	loaded, v2, err := c.loadAuto(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Sequence != 10 {
		t.Fatalf("expected Sequence=10 after concurrent write, got %d", loaded.Sequence)
	}
	if v2 == nil || v1 == nil {
		t.Fatal("versions should not be nil")
	}
	if v2.Value == v1.Value {
		t.Fatal("version should have changed after CAS")
	}
}

func TestAutoKeyPathIncludesNamespaceAndLogicalID(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	ctx := context.Background()

	// Save to one key.
	state := validAutoControl()
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}

	// Different logicalID → different key.
	r2 := autoClusterResource("cluster-2", "uid-2", "logical-2")
	state2 := validAutoControl()
	state2.BindingUID = "binding-uid-2"
	if err := c.saveAuto(ctx, r2, state2, nil); err != nil {
		t.Fatal(err)
	}

	// Both should exist independently.
	_, v1, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	_, v2, err := c.loadAuto(ctx, c.autoKey(r2))
	if err != nil {
		t.Fatal(err)
	}
	if v1 == nil || v2 == nil {
		t.Fatal("both keys should exist")
	}
}

func TestSaveAutoPersistedTimeFields(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	ctx := context.Background()

	suspected := time.Date(2026, 9, 11, 10, 0, 0, 0, time.UTC)
	completed1 := time.Date(2026, 9, 11, 8, 0, 0, 0, time.UTC)
	completed2 := time.Date(2026, 9, 11, 9, 0, 0, 0, time.UTC)

	state := validAutoControl()
	state.SuspectedSince = suspected
	state.Completions = []time.Time{completed1, completed2}
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}

	loaded, _, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.SuspectedSince.Equal(suspected) {
		t.Fatalf("SuspectedSince=%v, want %v", loaded.SuspectedSince, suspected)
	}
	if len(loaded.Completions) != 2 || !loaded.Completions[0].Equal(completed1) || !loaded.Completions[1].Equal(completed2) {
		t.Fatalf("Completions=%v, want [%v, %v]", loaded.Completions, completed1, completed2)
	}
}

func TestAuthorizeAutomaticChildRejectsIncompleteProof(t *testing.T) {
	bucket := autoBucket(t)
	c := autoController(bucket)
	ctx := context.Background()

	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	state := validAutoControl()
	desiredSpec := Spec{StatefulSet: "rhiza", SourceClusterID: "source", Durability: "before-ack", RecoveryID: "auto-r1"}
	request := FenceRequest{
		LogicalID:       "logical-1",
		BindingUID:      state.BindingUID,
		Namespace:       "default",
		StatefulSet:     "rhiza",
		StatefulSetUID:  "sts-uid-1",
		SourceClusterID: "source",
		OperationID:     "op-1",
		Scope:           ScopeGeneration,
		Targets:         []FenceTarget{{NodeID: "n1", Pod: "rhiza-0"}, {NodeID: "n2", Pod: "rhiza-1"}, {NodeID: "n3", Pod: "rhiza-2"}},
	}
	state.Intent = &autoIntent{
		RecoveryName: "auto-r1",
		RecoveryUID:  "child-uid-exact",
		SpecHash:     hashJSON(desiredSpec),
		Request:      request,
		Proof: &FenceProof{
			OperationID:         request.OperationID,
			SourceClusterID:     request.SourceClusterID,
			BindingUID:          request.BindingUID,
			RequestHash:         "wrong-hash",
			ProofID:             "proof-1",
			ProcessesTerminated: true,
			RecreationBlocked:   true,
			StorageQuiesced:     true,
		},
	}
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}

	child := &Resource{
		Metadata: map[string]any{
			"name": "auto-r1",
			"uid":  "child-uid-exact",
			"annotations": map[string]any{
				automaticOwner:   state.BindingUID,
				automaticLogical: "logical-1",
			},
		},
		Spec: desiredSpec,
	}

	ok, err := c.authorizeAutomaticChild(ctx, child)
	if err == nil {
		t.Fatal("should reject proof with wrong requestHash")
	}
	if ok {
		t.Fatal("should not authorize child with incomplete proof")
	}
}
