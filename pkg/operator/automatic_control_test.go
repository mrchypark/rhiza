package operator

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
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
			VoterCount:     3,
		},
	}
}

func validAutoControl() *autoControl {
	return &autoControl{
		Version:         1,
		BindingUID:      "binding-uid-1",
		BindingHash:     "hash-1",
		BindingVersion:  autoBindingVersion,
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

// The declared count, the workload environment, and the replica count are one
// contract; a mismatch must be rejected instead of reconciled as an outage.
func TestReconcileClusterRequiresMatchingVoterCount(t *testing.T) {
	ctx := context.Background()
	sts := fakeSTS("before-ack", voterMembers(3))
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	r.Spec.Container, r.Spec.Automatic = "rhiza", true
	normalizeClusterSpec(r)
	adopted := adoptedClusterMembers(t, r, sts)
	ctr, err := container(sts, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_CLUSTER_MEMBERS", object{"value": string(jsonBytes(adopted))})
	handler := http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if q.Method == http.MethodGet && strings.Contains(q.URL.Path, "/statefulsets/") {
			json.NewEncoder(w).Encode(sts)
			return
		}
		if q.Method == http.MethodGet && q.URL.Path == "/api/v1/namespaces/default/services/rhiza-headless" {
			json.NewEncoder(w).Encode(object{"spec": object{"clusterIP": "None"}})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{})
	})
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("tok"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := autoController(autoBucket(t))
	c.Kube = &Kubernetes{BaseURL: server.URL, Namespace: "default", TokenFile: token, Client: server.Client()}
	state := validAutoControl()
	state.BindingUID, state.BindingHash, state.StatefulSetUID = "uid-1", autoBinding(r), "sts-uid"
	state.Voters, _ = derivedVoters(r, sts)
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}

	asObject(sts["spec"])["replicas"] = float64(4)
	if err := c.reconcileCluster(ctx, r); err != nil {
		t.Fatal(err)
	}
	if want := "StatefulSet has 4 desired replicas but voterCount is 3"; r.Status.Phase != "Blocked" || r.Status.Message != want {
		t.Fatalf("status=%+v want=%q", r.Status, want)
	}

	asObject(sts["spec"])["replicas"] = float64(3)
	raw, err := json.Marshal(voterMembers(4))
	if err != nil {
		t.Fatal(err)
	}
	ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_CLUSTER_MEMBERS", object{"value": string(raw)})
	if err := c.reconcileCluster(ctx, r); err != nil {
		t.Fatal(err)
	}
	if want := "RHIZA_CLUSTER_MEMBERS lists 4 voters but voterCount is 3"; r.Status.Phase != "Blocked" || r.Status.Message != want {
		t.Fatalf("status=%+v want=%q", r.Status, want)
	}

	r.Spec.VoterCount = 2
	if err := c.reconcileCluster(ctx, r); err != nil {
		t.Fatal(err)
	}
	if want := "spec.voterCount=2 is below the minimum 3; set it to the StatefulSet desired replicas and exported RHIZA_CLUSTER_MEMBERS count (3)"; r.Status.Phase != "Blocked" || r.Status.Message != want {
		t.Fatalf("status=%+v want=%q", r.Status, want)
	}
}

// The count contract must hold before an initial binding freezes the inventory;
// otherwise a typo could only be answered by editing the workload.
func TestReconcileClusterValidatesVoterCountBeforeBinding(t *testing.T) {
	ctx := context.Background()
	c, r, _ := boundClusterFixture(t, voterMembers(3))
	r.Spec.VoterCount++
	if err := c.reconcileCluster(ctx, r); err != nil {
		t.Fatal(err)
	}
	if want := "StatefulSet has 3 desired replicas but voterCount is 4"; r.Status.Phase != "Blocked" || r.Status.Message != want {
		t.Fatalf("status=%+v want=%q", r.Status, want)
	}
	if _, _, err := c.loadAuto(ctx, c.autoKey(r)); !c.Bucket.IsObjNotFoundErr(err) {
		t.Fatalf("mismatched voter count froze a control record: %v", err)
	}
	if exists, err := c.Bucket.Exists(ctx, bindingPath(c)); err != nil || exists {
		t.Fatalf("mismatched voter count froze the workload binding: exists=%t err=%v", exists, err)
	}

	r.Spec.VoterCount--
	if err := c.reconcileCluster(ctx, r); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Observing" || r.Status.Message != "durable cluster binding created" {
		t.Fatalf("corrected voter count was not accepted: %+v", r.Status)
	}
	state, _, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil || len(state.Voters) != 3 || state.BindingHash != autoBinding(r) {
		t.Fatalf("corrected voter count was not bound: %+v err=%v", state, err)
	}
}

// A voter count longer than one fencing request admits must be refused before it
// is frozen; binding it would block the first generation-wide incident forever.
func TestReconcileClusterRejectsUnfenceableVoterCount(t *testing.T) {
	ctx := context.Background()
	c, r, _ := boundClusterFixture(t, voterMembers(fenceTargetCapacity+1))
	if err := c.reconcileCluster(ctx, r); err != nil {
		t.Fatal(err)
	}
	if want := "spec.voterCount=65 exceeds the maximum 64"; r.Status.Phase != "Blocked" || r.Status.Message != want {
		t.Fatalf("status=%+v want=%q", r.Status, want)
	}
	if _, _, err := c.loadAuto(ctx, c.autoKey(r)); !c.Bucket.IsObjNotFoundErr(err) {
		t.Fatalf("unfenceable voter count froze a control record: %v", err)
	}
	if exists, err := c.Bucket.Exists(ctx, bindingPath(c)); err != nil || exists {
		t.Fatalf("unfenceable voter count froze the workload binding: exists=%t err=%v", exists, err)
	}
}

func bindingPath(c *Controller) string {
	return path.Join(c.Prefix, "automatic-workloads", c.Kube.Namespace, "sts-uid", "binding.json")
}

// boundClusterFixture builds a bootstrap-ready controller and cluster resource
// whose StatefulSet declares one replica and one member per voter.
func boundClusterFixture(t *testing.T, voters []quepaxa.Member) (*Controller, *ClusterResource, object) {
	t.Helper()
	sts := fakeSTS("before-ack", voters)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	r.Spec.Container, r.Spec.Automatic = "rhiza", true
	r.Spec.VoterCount = len(voters)
	normalizeClusterSpec(r)
	ctr, err := container(sts, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	ctr["env"] = append(list(ctr["env"]), object{"name": "RHIZA_ENABLE_RECONFIGURATION", "value": "true"})
	ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_CLUSTER_MEMBERS", object{"value": string(jsonBytes(adoptedClusterMembers(t, r, sts)))})
	handler := http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if q.Method == http.MethodGet && strings.Contains(q.URL.Path, "/statefulsets/") {
			json.NewEncoder(w).Encode(sts)
			return
		}
		if q.Method == http.MethodGet && q.URL.Path == "/api/v1/namespaces/default/services/rhiza-headless" {
			json.NewEncoder(w).Encode(object{"spec": object{"clusterIP": "None"}})
			return
		}
		if q.Method == http.MethodPut && strings.HasSuffix(q.URL.Path, "/status") {
			var body map[string]any
			json.NewDecoder(q.Body).Decode(&body)
			json.NewEncoder(w).Encode(body)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{})
	})
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("tok"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := autoController(autoBucket(t))
	c.StoreIdentity = storeIdentity()
	c.Kube = &Kubernetes{BaseURL: server.URL, Namespace: "default", TokenFile: token, Client: server.Client()}
	return c, r, sts
}

func adoptedClusterMembers(t *testing.T, r *ClusterResource, sts object) []quepaxa.Member {
	t.Helper()
	members, err := clusterMemberSources(r, sts, "default")
	if err != nil {
		t.Fatal(err)
	}
	for i := range members {
		members[i] = voterMember("source", string(members[i].ID), "token-"+string(members[i].ID))
		members[i].URL = fmt.Sprintf("http://%s.%s.default.svc.cluster.local:%d", members[i].ID, str(nested(sts, "spec", "serviceName")), r.Spec.HTTPPort)
		members[i].PeerURL = fmt.Sprintf("quic://%s.%s.default.svc.cluster.local:%d", members[i].ID, str(nested(sts, "spec", "serviceName")), r.Spec.PeerPort)
	}
	return members
}

func clusterVoterMembers(count int) []quepaxa.Member {
	voters := make([]quepaxa.Member, 0, count)
	for i := range count {
		id := fmt.Sprintf("rhiza-%d", i)
		voters = append(voters, voterMember("source", id, "token-"+id))
	}
	return voters
}

type failingBucket struct{ objstore.Bucket }

func (f failingBucket) Exists(context.Context, string) (bool, error) {
	return false, errors.New("object store offline")
}

func TestClusterBootstrapRequiresOptInAndUnregisteredStore(t *testing.T) {
	for _, tc := range []struct {
		name       string
		bootstrap  bool
		members    string
		commitment []byte
		bucket     bool
		want       string
	}{
		{name: "empty without opt-in", want: "set spec.bootstrap: true for a fresh workload or export RHIZA_CLUSTER_MEMBERS"},
		{name: "whitespace without opt-in", members: " \n\t", want: "set spec.bootstrap: true for a fresh workload or export RHIZA_CLUSTER_MEMBERS"},
		{name: "whitespace with opt-in", bootstrap: true, members: " \n\t"},
		{name: "registered commitment", bootstrap: true, commitment: []byte("registered"), want: "already registered in object storage"},
		{name: "unreadable commitment", bootstrap: true, commitment: []byte("{"), want: "registered cluster membership is unreadable"},
		{name: "unreachable store", bootstrap: true, bucket: true, want: "object store unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			api := &clusterAPI{sts: freshClusterSTS(3)}
			c := clusterController(t, api)
			r := autoClusterResource("cluster-1", "uid-1", "logical-1")
			r.Spec.VoterCount, r.Spec.Bootstrap = 3, tc.bootstrap
			normalizeClusterSpec(r)
			ctr, _ := container(api.sts, "rhiza")
			if tc.members != "" {
				ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_CLUSTER_MEMBERS", object{"value": tc.members})
			}
			if tc.commitment != nil {
				if bytes.Equal(tc.commitment, []byte("registered")) {
					members := voterMembers(3)
					tc.commitment = jsonBytes(recovery.NewMembershipRecord(r.Spec.AdoptClusterID, members, "before-ack"))
				}
				if err := c.Bucket.Upload(ctx, path.Join(c.Prefix, r.Spec.AdoptClusterID, "voters/membership.json"), bytes.NewReader(tc.commitment)); err != nil {
					t.Fatal(err)
				}
			}
			if tc.bucket {
				c.Bucket = failingBucket{Bucket: c.Bucket}
			}
			if err := c.reconcileCluster(ctx, r); err != nil {
				t.Fatal(err)
			}
			if tc.want != "" {
				if r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, tc.want) || api.secretCreates != 0 {
					t.Fatalf("status=%+v creates=%d want=%q", r.Status, api.secretCreates, tc.want)
				}
				return
			}
			if r.Status.Phase != "Observing" || api.secretCreates != 1 {
				t.Fatalf("fresh bootstrap failed: status=%+v creates=%d", r.Status, api.secretCreates)
			}
		})
	}
}

func TestClusterFreshBootstrapValidatesRuntimeIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		fix  func(object)
		want string
	}{
		{name: "missing node id", fix: func(ctr object) { ctr["env"] = removeEnv(list(ctr["env"]), "RHIZA_NODE_ID") }, want: "RHIZA_NODE_ID must use fieldRef metadata.name; observed missing"},
		{name: "constant node id", fix: func(ctr object) {
			ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_NODE_ID", object{"value": "rhiza-0"})
		}, want: "observed rhiza-0"},
		{name: "peer port mismatch", fix: func(ctr object) {
			ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_PEER_ADDR", object{"value": "0.0.0.0:9091"})
		}, want: "RHIZA_PEER_ADDR port is 9091 but expected 9090"},
		{name: "http bind mismatch", fix: func(ctr object) {
			ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_BIND_ADDR", object{"value": "0.0.0.0:8081"})
		}, want: "RHIZA_BIND_ADDR port is 8081 but expected 8080"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := &clusterAPI{sts: freshClusterSTS(3)}
			c := clusterController(t, api)
			r := autoClusterResource("cluster-1", "uid-1", "logical-1")
			r.Spec.VoterCount, r.Spec.Bootstrap = 3, true
			normalizeClusterSpec(r)
			ctr, _ := container(api.sts, "rhiza")
			tc.fix(ctr)
			if err := c.reconcileCluster(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			if r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, tc.want) || api.secretCreates != 0 {
				t.Fatalf("status=%+v creates=%d want=%q", r.Status, api.secretCreates, tc.want)
			}
		})
	}
}

func TestClusterServiceAndDomainValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		service object
		omit    bool
		domain  string
		want    string
	}{
		{name: "missing service", omit: true, want: "does not name an existing Service"},
		{name: "non-headless service", service: object{"spec": object{"clusterIP": "10.0.0.1"}}, want: "must be headless"},
		{name: "invalid domain", domain: "Bad_Domain", want: "is not a DNS name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := &clusterAPI{sts: freshClusterSTS(3), service: tc.service, omitService: tc.omit}
			c := clusterController(t, api)
			r := autoClusterResource("cluster-1", "uid-1", "logical-1")
			r.Spec.VoterCount, r.Spec.ClusterDomain = 3, tc.domain
			normalizeClusterSpec(r)
			if err := c.reconcileCluster(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			if r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, tc.want) {
				t.Fatalf("status=%+v want=%q", r.Status, tc.want)
			}
		})
	}

	t.Run("custom cluster domain", func(t *testing.T) {
		api := &clusterAPI{sts: freshClusterSTS(3)}
		c := clusterController(t, api)
		r := autoClusterResource("cluster-1", "uid-1", "logical-1")
		r.Spec.VoterCount, r.Spec.Bootstrap, r.Spec.ClusterDomain = 3, true, "corp.internal"
		normalizeClusterSpec(r)
		if err := c.reconcileCluster(t.Context(), r); err != nil {
			t.Fatal(err)
		}
		if r.Status.Phase != "Observing" || api.secretCreates != 1 {
			t.Fatalf("status=%+v creates=%d", r.Status, api.secretCreates)
		}
		var secret object
		for _, candidate := range api.secrets {
			secret = candidate
		}
		members, _ := decodedClusterSecret(t, secret)
		for _, member := range members {
			if !strings.Contains(member.URL, ".svc.corp.internal:") || !strings.Contains(member.PeerURL, ".svc.corp.internal:") {
				t.Fatalf("custom domain missing from member URLs: %+v", member)
			}
		}
	})
}

func TestLegacyBindingMigration(t *testing.T) {
	ctx := t.Context()
	api := &clusterAPI{sts: fakeSTS("before-ack", voterMembers(3))}
	c := clusterController(t, api)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	r.Spec.VoterCount, r.Spec.Container = 3, "rhiza"
	normalizeClusterSpec(r)
	state := validAutoControl()
	state.BindingUID, state.StatefulSetUID = "uid-1", "sts-uid"
	state.BindingVersion = 0
	state.Voters = []VoterPod{{NodeID: "n1", Pod: "n1"}, {NodeID: "n2", Pod: "n2"}, {NodeID: "n3", Pod: "n3"}}
	state.BindingHash = legacyAutoBinding(r, state.Voters)
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcileCluster(ctx, r); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Observing" || !strings.Contains(r.Status.Message, "legacy automatic binding migrated") {
		t.Fatalf("migration status=%+v", r.Status)
	}
	loaded, _, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil || loaded.BindingVersion != autoBindingVersion || loaded.BindingHash != autoBinding(r) {
		t.Fatalf("migrated record=%+v err=%v", loaded, err)
	}
	if err := c.reconcileCluster(ctx, r); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase == "Blocked" {
		t.Fatalf("migrated binding blocked on the next reconcile: %+v", r.Status)
	}
}

func TestLegacyBindingMigrationFailsClosed(t *testing.T) {
	ctx := t.Context()
	for _, tc := range []struct {
		name  string
		count int
		hash  string
		want  string
	}{
		{name: "missing voter count", count: 0, want: "spec.voterCount is required"},
		{name: "wrong replicas", count: 4, want: "StatefulSet has 3 desired replicas but voterCount is 4"},
		{name: "hash changed", count: 3, hash: "wrong", want: "legacy automatic binding hash no longer matches"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := &clusterAPI{sts: fakeSTS("before-ack", voterMembers(3))}
			c := clusterController(t, api)
			r := autoClusterResource("cluster-1", "uid-1", "logical-1")
			r.Spec.VoterCount, r.Spec.Container = tc.count, "rhiza"
			normalizeClusterSpec(r)
			state := validAutoControl()
			state.BindingUID, state.StatefulSetUID = "uid-1", "sts-uid"
			state.BindingVersion = 0
			state.Voters = []VoterPod{{NodeID: "n1", Pod: "n1"}, {NodeID: "n2", Pod: "n2"}, {NodeID: "n3", Pod: "n3"}}
			state.BindingHash = legacyAutoBinding(r, state.Voters)
			if tc.hash != "" {
				state.BindingHash = tc.hash
			}
			if err := c.saveAuto(ctx, r, state, nil); err != nil {
				t.Fatal(err)
			}
			if err := c.reconcileCluster(ctx, r); err != nil {
				t.Fatal(err)
			}
			if r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, tc.want) {
				t.Fatalf("status=%+v want=%q", r.Status, tc.want)
			}
		})
	}
}

type clusterAPI struct {
	sts           object
	service       object
	omitService   bool
	secrets       map[string]object
	secretCreates int
}

func (a *clusterAPI) serve(w http.ResponseWriter, q *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case q.URL.Path == "/apis/apps/v1/namespaces/default/statefulsets/rhiza" && q.Method == http.MethodGet:
		_ = json.NewEncoder(w).Encode(a.sts)
	case q.URL.Path == "/apis/apps/v1/namespaces/default/statefulsets/rhiza" && q.Method == http.MethodPut:
		var next object
		if json.NewDecoder(q.Body).Decode(&next) == nil {
			a.sts = next
			apiserverNormalize(a.sts)
			_ = json.NewEncoder(w).Encode(a.sts)
		}
	case q.URL.Path == "/api/v1/namespaces/default/services/rhiza-headless" && q.Method == http.MethodGet:
		if a.omitService {
			http.NotFound(w, q)
			return
		}
		_ = json.NewEncoder(w).Encode(a.service)
	case q.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizaclusters/cluster-1/status" && q.Method == http.MethodPut:
		var next ClusterResource
		if json.NewDecoder(q.Body).Decode(&next) == nil {
			_ = json.NewEncoder(w).Encode(next)
		}
	case q.URL.Path == "/api/v1/namespaces/default/secrets" && q.Method == http.MethodPost:
		var next object
		if json.NewDecoder(q.Body).Decode(&next) == nil {
			if a.secrets == nil {
				a.secrets = map[string]object{}
			}
			name := str(nested(next, "metadata", "name"))
			if a.secrets[name] == nil {
				a.secrets[name] = next
				a.secretCreates++
			}
			_ = json.NewEncoder(w).Encode(a.secrets[name])
		}
	case strings.HasPrefix(q.URL.Path, "/api/v1/namespaces/default/secrets/") && q.Method == http.MethodGet:
		name := strings.TrimPrefix(q.URL.Path, "/api/v1/namespaces/default/secrets/")
		if secret := a.secrets[name]; secret != nil {
			_ = json.NewEncoder(w).Encode(secret)
			return
		}
		http.NotFound(w, q)
	default:
		http.NotFound(w, q)
	}
}

func clusterController(t *testing.T, api *clusterAPI) *Controller {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(api.serve))
	t.Cleanup(server.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("tok"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := autoController(autoBucket(t))
	c.StoreIdentity = storeIdentity()
	c.Kube = &Kubernetes{BaseURL: server.URL, Namespace: "default", TokenFile: token, Client: server.Client()}
	if api.service == nil {
		api.service = object{"spec": object{"clusterIP": "None"}}
	}
	return c
}

func freshClusterSTS(count int) object {
	sts := fakeSTS("before-ack", make([]quepaxa.Member, count))
	ctr, _ := container(sts, "rhiza")
	ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_CLUSTER_MEMBERS", object{"value": ""})
	ctr["env"] = append(list(ctr["env"]), object{"name": "RHIZA_ENABLE_RECONFIGURATION", "value": "true"})
	ctr["env"] = append(list(ctr["env"]), object{"name": "RHIZA_NODE_ID", "valueFrom": object{"fieldRef": object{"fieldPath": "metadata.name"}}}, object{"name": "RHIZA_PEER_ADDR", "value": "0.0.0.0:9090"}, object{"name": "RHIZA_BIND_ADDR", "value": "0.0.0.0:8080"})
	return sts
}

func TestClusterVoterCountAdoptsDerivedInventory(t *testing.T) {
	for _, count := range []int{3, 4, 5} {
		t.Run(fmt.Sprintf("voters=%d", count), func(t *testing.T) {
			ctx := context.Background()
			api := &clusterAPI{sts: freshClusterSTS(count)}
			c := clusterController(t, api)
			r := autoClusterResource("cluster-1", "uid-1", "logical-1")
			r.Spec.VoterCount, r.Spec.Automatic = count, true
			normalizeClusterSpec(r)
			members := adoptedClusterMembers(t, r, api.sts)
			ctr, _ := container(api.sts, "rhiza")
			ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_CLUSTER_MEMBERS", object{"value": string(jsonBytes(members))})

			if err := c.reconcileCluster(ctx, r); err != nil {
				t.Fatal(err)
			}
			state, _, err := c.loadAuto(ctx, c.autoKey(r))
			if err != nil || state.BindingHash != autoBinding(r) || state.BindingVersion != autoBindingVersion || len(state.Voters) != count {
				t.Fatalf("count=%d state=%+v err=%v", count, state, err)
			}
			for i, voter := range state.Voters {
				if voter.NodeID != quepaxa.NodeID(fmt.Sprintf("rhiza-%d", i)) || voter.Pod != fmt.Sprintf("rhiza-%d", i) {
					t.Fatalf("count=%d voter[%d]=%+v", count, i, voter)
				}
			}
			if api.secretCreates != 0 {
				t.Fatalf("adoption regenerated %d secrets", api.secretCreates)
			}
		})
	}
}

func TestClusterFreshBootstrapGeneratesImmutableIdentitiesOnce(t *testing.T) {
	ctx := context.Background()
	const count = 5
	api := &clusterAPI{sts: freshClusterSTS(count)}
	c := clusterController(t, api)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	r.Spec.VoterCount, r.Spec.Automatic, r.Spec.Bootstrap = count, true, true
	normalizeClusterSpec(r)

	if err := c.reconcileCluster(ctx, r); err != nil {
		t.Fatal(err)
	}
	if r.Status.Message != "fresh voter credentials created; waiting for workload" || api.secretCreates != 1 {
		t.Fatalf("bootstrap status=%+v creates=%d", r.Status, api.secretCreates)
	}
	if exists, err := c.Bucket.Exists(ctx, bindingPath(c)); err != nil || exists {
		t.Fatalf("fresh bootstrap froze a binding before adoption: exists=%t err=%v", exists, err)
	}
	if replicas := int(number(nested(api.sts, "spec", "replicas"))); replicas != count {
		t.Fatalf("bootstrap replicas=%d want=%d", replicas, count)
	}
	secretName := ""
	var secret object
	for name, candidate := range api.secrets {
		secretName, secret = name, candidate
	}
	if secretName == "" || secret["immutable"] != true {
		t.Fatalf("bootstrap secret=%v", secret)
	}
	members, tokens := decodedClusterSecret(t, secret)
	if len(members) != count || len(tokens) != count {
		t.Fatalf("members=%d tokens=%d want=%d", len(members), len(tokens), count)
	}
	for i, member := range members {
		pod := fmt.Sprintf("rhiza-%d", i)
		if string(member.ID) != pod || member.URL != "http://"+pod+".rhiza-headless.default.svc.cluster.local:8080" || member.PeerURL != "quic://"+pod+".rhiza-headless.default.svc.cluster.local:9090" {
			t.Fatalf("member[%d]=%+v", i, member)
		}
		want := network.PeerPublicKey(types.ClusterID("source"), member.ID, tokens[string(member.ID)])
		if string(member.PublicKey[:]) != string(want) {
			t.Fatalf("member %s public key drifted from the engine derivation", member.ID)
		}
	}
	ctr, _ := container(api.sts, "rhiza")
	for name, want := range map[string]string{"RHIZA_CLUSTER_MEMBERS": "members", "RHIZA_ADMIN_TOKEN": "admin", "RHIZA_PEER_TOKENS": "peer_tokens"} {
		entry := envEntry(ctr, name)
		if str(nested(entry, "valueFrom", "secretKeyRef", "name")) != secretName || str(nested(entry, "valueFrom", "secretKeyRef", "key")) != want {
			t.Fatalf("%s env=%+v", name, entry)
		}
	}
	if token := envEntry(ctr, "RHIZA_PEER_TOKEN"); token == nil || str(token["value"]) != "" {
		t.Fatalf("inherited scalar token was not cleared: %+v", token)
	}

	if err := c.reconcileCluster(ctx, r); err != nil {
		t.Fatal(err)
	}
	state, _, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil || len(state.Voters) != count || api.secretCreates != 1 {
		t.Fatalf("fresh adoption state=%+v creates=%d err=%v", state, api.secretCreates, err)
	}
	members2, tokens2 := decodedClusterSecret(t, api.secrets[secretName])
	if !bytes.Equal(jsonBytes(members), jsonBytes(members2)) || !bytes.Equal(jsonBytes(tokens), jsonBytes(tokens2)) {
		t.Fatal("fresh identities changed after the workload adopted them")
	}
	wrong := clusterVoterMembers(count)
	for i := range wrong {
		wrong[i].ID = quepaxa.NodeID(fmt.Sprintf("wrong-%d", i))
	}
	ctr, _ = container(api.sts, "rhiza")
	ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_CLUSTER_MEMBERS", object{"value": string(jsonBytes(wrong))})
	if err := c.reconcileCluster(ctx, r); err != nil {
		t.Fatal(err)
	}
	if want := "RHIZA_CLUSTER_MEMBERS node IDs must be rhiza-0 through rhiza-4"; r.Status.Message != want {
		t.Fatalf("bound workload accepted changed node IDs: %+v want=%q", r.Status, want)
	}
}

func decodedClusterSecret(t *testing.T, secret object) ([]quepaxa.Member, map[string]string) {
	t.Helper()
	memberData, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "members")))
	if err != nil {
		t.Fatal(err)
	}
	var members []quepaxa.Member
	if err := json.Unmarshal(memberData, &members); err != nil {
		t.Fatal(err)
	}
	tokenData, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "peer_tokens")))
	if err != nil {
		t.Fatal(err)
	}
	var tokens map[string]string
	if err := json.Unmarshal(tokenData, &tokens); err != nil {
		t.Fatal(err)
	}
	return members, tokens
}

func envEntry(ctr object, name string) object {
	for _, raw := range list(ctr["env"]) {
		entry := asObject(raw)
		if str(entry["name"]) == name {
			return entry
		}
	}
	return nil
}

func TestClusterVoterCountRejectsAdoptionMismatch(t *testing.T) {
	ctx := context.Background()
	api := &clusterAPI{sts: freshClusterSTS(3)}
	c := clusterController(t, api)
	r := autoClusterResource("cluster-1", "uid-1", "logical-1")
	r.Spec.VoterCount = 3
	normalizeClusterSpec(r)
	ctr, _ := container(api.sts, "rhiza")

	asObject(api.sts["spec"])["replicas"] = float64(4)
	if err := c.reconcileCluster(ctx, r); err != nil {
		t.Fatal(err)
	}
	if want := "StatefulSet has 4 desired replicas but voterCount is 3"; r.Status.Message != want {
		t.Fatalf("status=%+v want=%q", r.Status, want)
	}

	asObject(api.sts["spec"])["replicas"] = float64(3)
	ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_CLUSTER_MEMBERS", object{"value": string(jsonBytes(clusterVoterMembers(4)))})
	if err := c.reconcileCluster(ctx, r); err != nil {
		t.Fatal(err)
	}
	if want := "RHIZA_CLUSTER_MEMBERS lists 4 voters but voterCount is 3"; r.Status.Message != want {
		t.Fatalf("status=%+v want=%q", r.Status, want)
	}

	wrong := clusterVoterMembers(3)
	for i := range wrong {
		wrong[i].ID = quepaxa.NodeID(fmt.Sprintf("wrong-%d", i))
	}
	ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_CLUSTER_MEMBERS", object{"value": string(jsonBytes(wrong))})
	if err := c.reconcileCluster(ctx, r); err != nil {
		t.Fatal(err)
	}
	if want := "RHIZA_CLUSTER_MEMBERS node IDs must be rhiza-0 through rhiza-2"; r.Status.Message != want {
		t.Fatalf("status=%+v want=%q", r.Status, want)
	}
}
