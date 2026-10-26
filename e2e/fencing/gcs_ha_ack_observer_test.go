package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/thanos-io/objstore"
)

func TestValidateGCSHAAckReceiptBindsFullTupleAndLiveScope(t *testing.T) {
	source := gcsRecoverySource{StatefulSetUID: "sts-uid", ClusterID: "source", NamespaceUID: "namespace-uid", BindingUID: "binding-uid", Bucket: "archive-bucket", Prefix: "cluster/source"}
	scope := gcsHAAckSourceScope(source)
	scopeHash, err := gcsHAAckScopeHash(scope)
	if err != nil {
		t.Fatal(err)
	}
	runtime := fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "namespace-uid", BindingUID: "binding-uid", StatefulSetUID: source.StatefulSetUID,
		SourceClusterID: source.ClusterID, NodeID: "node", PodUID: "pod-uid", RunID: "run", NodeName: "node-name", NodeUID: "node-uid",
		MachineID: "machine", BootID: "boot", RuntimeID: "runtime", SandboxID: "sandbox", ContainerID: "container", ImageID: "image"}
	identity := fencing.Identity{SourceClusterID: source.ClusterID, NodeID: runtime.NodeID, PodUID: runtime.PodUID, RunID: runtime.RunID, WALIdentity: "wal-identity"}
	intent := fencing.MutationIntent{ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: scopeHash,
		OwnerStep: fencing.MutationOwnerStep{Owner: "producer", Workflow: "workflow", Step: "bucket-1", Attempt: 1},
		Condition: fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, OperationID: "op", Kind: "upload",
		Key: "archive/blocks/" + strings.Repeat("a", 64) + "_00000000000000000001.bin", PayloadDigest: strings.Repeat("a", 64), PayloadSize: 10}
	receipt := operator.GCSMutationAckLossReceipt{Version: 1, Injected: true, WireRequests: 1, SourceIdentity: identity, Runtime: runtime,
		ProducerUID: "producer", Sequence: 1, DispatchID: "dispatch", OperationID: intent.OperationID, Intent: intent,
		Preparation: fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: scopeHash, GenerationSet: true, Generation: 0},
		Scope:       scope, TupleDigest: "", Bucket: source.Bucket, Prefix: source.Prefix, ObjectKey: intent.Key, PayloadDigest: intent.PayloadDigest, PayloadSize: intent.PayloadSize}
	receipt.TupleDigest = gcsHAAckTupleDigest(receipt)
	if err := validateGCSHAAckReceipt(source, receipt); err != nil {
		t.Fatalf("valid exact receipt rejected: %v", err)
	}

	t.Run("changed tuple with stale digest", func(t *testing.T) {
		changed := receipt
		changed.Intent.Key += ".other"
		changed.ObjectKey = changed.Intent.Key
		if err := validateGCSHAAckReceipt(source, changed); err == nil {
			t.Fatal("mutated receipt tuple was accepted")
		}
	})
	t.Run("cross-scope", func(t *testing.T) {
		changed := receipt
		changed.Scope.Prefix = "cluster/other"
		changed.TupleDigest = gcsHAAckTupleDigest(changed)
		if err := validateGCSHAAckReceipt(source, changed); err == nil {
			t.Fatal("receipt for another source scope was accepted")
		}
	})
}

func TestGCSAckLossWriterObserverOptInIsExplicitAndIdentityBound(t *testing.T) {
	for _, test := range []struct {
		name    string
		value   string
		want    bool
		wantErr bool
	}{
		{name: "default off"},
		{name: "explicit no", value: "no"},
		{name: "explicit yes", value: "yes", want: true},
		{name: "reject malformed opt-in", value: "true", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseGCSAckLossWriterObserverOptIn(test.value)
			if (err != nil) != test.wantErr || got != test.want {
				t.Fatalf("opt-in %q = %t, %v; want %t, error=%t", test.value, got, err, test.want, test.wantErr)
			}
		})
	}

	const writerEmail = "rhiza-data@example.iam.gserviceaccount.com"
	writerIdentity := gcsAckLossCredentialIdentity{Type: "service_account", ClientEmail: writerEmail}
	if shared, err := validateGCSAckLossObserverIdentity(writerIdentity, writerEmail, writerEmail, false); err == nil || shared {
		t.Fatalf("same writer identity passed with opt-in off: shared=%t err=%v", shared, err)
	}
	if shared, err := validateGCSAckLossObserverIdentity(writerIdentity, writerEmail, writerEmail, true); err != nil || !shared {
		t.Fatalf("explicit opt-in rejected the exact writer credential: shared=%t err=%v", shared, err)
	}
	wrongIdentity := writerIdentity
	wrongIdentity.ClientEmail = "other@example.iam.gserviceaccount.com"
	if shared, err := validateGCSAckLossObserverIdentity(wrongIdentity, writerEmail, writerEmail, true); err == nil || shared {
		t.Fatalf("opt-in accepted a credential for a different identity: shared=%t err=%v", shared, err)
	}
	readerIdentity := gcsAckLossCredentialIdentity{Type: "service_account", ClientEmail: "reader@example.iam.gserviceaccount.com"}
	if shared, err := validateGCSAckLossObserverIdentity(readerIdentity, readerIdentity.ClientEmail, writerEmail, false); err != nil || shared {
		t.Fatalf("distinct reader identity behavior changed: shared=%t err=%v", shared, err)
	}
}

func TestObserveGCSHAAckReceiptRejectsCanonicalBindingNamespaceAndGuardDigestDrift(t *testing.T) {
	for _, test := range []struct {
		name           string
		rootBindingUID string
		rootNamespace  string
		wrongGuardHash bool
		wantError      bool
	}{
		{name: "exact store-backed tuple"},
		{name: "canonical binding mismatch", rootBindingUID: "other-binding", wantError: true},
		{name: "canonical namespace mismatch", rootNamespace: "other-namespace", wantError: true},
		{name: "canonical guard digest mismatch", wrongGuardHash: true, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			bucket := objstore.NewInMemBucket()
			t.Cleanup(func() { _ = bucket.Close() })
			const prefix = "authority"
			stateStore, err := operator.NewAuthorityStateStore(bucket, prefix)
			if err != nil {
				t.Fatal(err)
			}
			source, runtime, identity, producer, producerUID, receipt, guardBytes := gcsHAAckCanonicalFixture(t, test.wrongGuardHash)
			evidenceStore, err := operator.NewAuthorityEvidenceStore(bucket, prefix, source.StatefulSetUID, source.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			guarded := *producer.Entries[receipt.Sequence].GuardedLogicalResolution
			if _, err := evidenceStore.CreateImmutable(ctx, guarded.GuardRef, guardBytes); err != nil {
				t.Fatal(err)
			}
			rootRuntime := runtime
			if test.rootBindingUID != "" {
				rootRuntime.BindingUID = test.rootBindingUID
			}
			if test.rootNamespace != "" {
				rootRuntime.NamespaceUID = test.rootNamespace
			}
			ledgerRun := fencing.Run{Identity: identity, Runtime: &rootRuntime, MutationProducerUID: producerUID,
				MutationScopeHash: receipt.Intent.StoreScopeHash, MutationProfile: fencing.MutationHeldExecutionProfile,
				MutationStageIncarnation: "stage-incarnation"}
			ledger := fencing.Ledger{SourceClusterID: source.ClusterID, RequireRuntimeBinding: true, RequireMutationJournal: true,
				Runs: map[string]fencing.Run{identity.RunID: ledgerRun}}
			storeGCSHAObserverCanonicalObjects(t, bucket, prefix, source.StatefulSetUID, source.ClusterID, runtime, producerUID, ledger, *producer)
			if _, err := observeGCSAckLossReceipt(ctx, stateStore, evidenceStore, source, receipt); err != nil && !test.wantError {
				t.Fatalf("valid store-backed canonical tuple rejected: %v", err)
			} else if err == nil && test.wantError {
				t.Fatal("observer accepted mismatched canonical binding, namespace, or guard digest")
			} else if test.wrongGuardHash && !strings.Contains(err.Error(), "immutable guard digest differs") {
				t.Fatalf("wrong guard digest failed before the byte-to-canonical digest check: %v", err)
			}
		})
	}
}

func gcsHAAckCanonicalFixture(t *testing.T, wrongGuardHash bool) (gcsRecoverySource, fencing.RuntimeBinding, fencing.Identity, *fencing.MutationProducer, string, operator.GCSMutationAckLossReceipt, []byte) {
	t.Helper()
	source := gcsRecoverySource{StatefulSetUID: "sts-uid", ClusterID: "source", NamespaceUID: "namespace-uid", BindingUID: "binding-uid", Bucket: "archive-bucket", Prefix: "cluster/source"}
	scope := gcsHAAckSourceScope(source)
	scopeHash, err := gcsHAAckScopeHash(scope)
	if err != nil {
		t.Fatal(err)
	}
	runtime := fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: source.NamespaceUID, BindingUID: source.BindingUID, StatefulSetUID: source.StatefulSetUID,
		SourceClusterID: source.ClusterID, NodeID: "node", PodUID: "pod-uid", RunID: "run", NodeName: "node-name", NodeUID: "node-uid",
		MachineID: "machine", BootID: "boot", RuntimeID: "runtime", SandboxID: "sandbox", ContainerID: "container", ImageID: "image"}
	identity := fencing.Identity{SourceClusterID: source.ClusterID, NodeID: runtime.NodeID, PodUID: runtime.PodUID, RunID: runtime.RunID, WALIdentity: "wal-identity"}
	intent := fencing.MutationIntent{ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: scopeHash,
		OwnerStep: fencing.MutationOwnerStep{Owner: "producer-placeholder", Workflow: "workflow", Step: "bucket-1", Attempt: 1},
		Condition: fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, OperationID: fencing.MutationRequestID("producer-placeholder", 1),
		Kind: "upload", Key: "archive/blocks/" + strings.Repeat("a", 64) + "_00000000000000000001.bin", PayloadRef: "stages/payload",
		PayloadDigest: strings.Repeat("a", 64), PayloadSize: 10}
	producer, err := fencing.NewMutationProducer(identity, runtime)
	if err != nil {
		t.Fatal(err)
	}
	producerUID := "producer-" + gcsHAAckDigestJSON([]any{"rhiza-mutation-producer-v1", runtime})
	intent.OwnerStep.Owner = producerUID
	intent.OperationID = fencing.MutationRequestID(producerUID, 1)
	if err := producer.Activate(identity); err != nil {
		t.Fatal(err)
	}
	if err := producer.Admit(1, intent); err != nil {
		t.Fatal(err)
	}
	preparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: scopeHash, GenerationSet: true, Generation: 0}
	if err := producer.Prepare(1, preparation); err != nil {
		t.Fatal(err)
	}
	if err := producer.GrantDispatch(1, "dispatch"); err != nil {
		t.Fatal(err)
	}
	if err := producer.SealDispatch(1); err != nil {
		t.Fatal(err)
	}
	if err := producer.Close("close", strings.Repeat("c", 64)); err != nil {
		t.Fatal(err)
	}
	preparationHash, err := producer.PreparationHash(1)
	if err != nil {
		t.Fatal(err)
	}
	rootIdentity := gcsHAAuthorityRootIdentity(source.StatefulSetUID, source.ClusterID)
	guarded := fencing.MutationGuardedLogicalResolution{Version: fencing.MutationGuardedLogicalResolutionVersion, Runtime: runtime, WALIdentity: identity.WALIdentity,
		ProducerUID: producerUID, Sequence: 1, Intent: intent, Preparation: preparation, PreparationHash: preparationHash, DispatchID: "dispatch",
		Profile: fencing.MutationHeldExecutionProfile, GuardRootIdentity: rootIdentity, GuardVersion: fencing.MutationGuardedLogicalResolutionVersion,
		GuardRef: path.Join("mutation-guards", strings.Repeat("d", 64)+".json"), ObjectGeneration: 7, ObjectMetageneration: 1, ObjectSize: intent.PayloadSize, PayloadDigest: intent.PayloadDigest}
	record := gcsHAHeldGuardRecord{Version: guarded.GuardVersion, RootIdentity: rootIdentity, Runtime: runtime, WALIdentity: identity.WALIdentity,
		ProducerUID: producerUID, Sequence: 1, Intent: intent, Preparation: preparation, PreparationHash: preparationHash, DispatchID: "dispatch"}
	guardBytes, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	guardHash := sha256.Sum256(guardBytes)
	guarded.GuardDigest = fmt.Sprintf("%x", guardHash)
	if wrongGuardHash {
		guarded.GuardDigest = strings.Repeat("f", 64)
	}
	if err := producer.ResolveGuardedLogical(producerUID, 1, guarded); err != nil {
		t.Fatal(err)
	}
	receipt := operator.GCSMutationAckLossReceipt{Version: 1, Injected: true, WireRequests: 1, SourceIdentity: identity, Runtime: runtime,
		ProducerUID: producerUID, Sequence: 1, DispatchID: "dispatch", OperationID: intent.OperationID, Intent: intent, Preparation: preparation,
		Scope: scope, Bucket: source.Bucket, Prefix: source.Prefix, ObjectKey: intent.Key, PayloadDigest: intent.PayloadDigest, PayloadSize: intent.PayloadSize}
	receipt.TupleDigest = gcsHAAckTupleDigest(receipt)
	return source, runtime, identity, producer, producerUID, receipt, guardBytes
}

func storeGCSHAObserverCanonicalObjects(t *testing.T, bucket objstore.Bucket, prefix, statefulSetUID, sourceID string, runtime fencing.RuntimeBinding, producerUID string, ledger fencing.Ledger, producer fencing.MutationProducer) {
	t.Helper()
	rootIdentity := gcsHAAuthorityRootIdentity(statefulSetUID, sourceID)
	rootValue, err := json.Marshal(struct {
		StatefulSetUID string         `json:"statefulSetUID"`
		Ledger         fencing.Ledger `json:"ledger"`
	}{StatefulSetUID: statefulSetUID, Ledger: ledger})
	if err != nil {
		t.Fatal(err)
	}
	rootKey := path.Join(prefix, "rhiza-authority/v1/roots", rootIdentity+".json")
	putGCSHAObserverEnvelope(t, bucket, rootKey, rootIdentity, rootValue)
	producerIdentity := gcsHAAckDigestJSON([]any{"rhiza-mutation-producer-v1", runtime})
	producerValue, err := json.Marshal(struct {
		ProducerUID string                   `json:"producerUID"`
		Producer    fencing.MutationProducer `json:"producer"`
	}{ProducerUID: producerUID, Producer: producer})
	if err != nil {
		t.Fatal(err)
	}
	putGCSHAObserverEnvelope(t, bucket, path.Join(prefix, "rhiza-authority/v1/producers", producerIdentity+".json"), producerIdentity, producerValue)
}

func putGCSHAObserverEnvelope(t *testing.T, bucket objstore.Bucket, key, identity string, value []byte) {
	t.Helper()
	digest := sha256.Sum256(value)
	envelope, err := json.Marshal(struct {
		Version  int             `json:"version"`
		Identity string          `json:"identity"`
		Digest   string          `json:"digest"`
		Value    json.RawMessage `json:"value"`
	}{Version: 1, Identity: identity, Digest: fmt.Sprintf("%x", digest), Value: value})
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(context.Background(), key, bytes.NewReader(envelope), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
}

func gcsHAAckDigestJSON(value any) string {
	data, _ := json.Marshal(value)
	digest := sha256.Sum256(data)
	return fmt.Sprintf("%x", digest)
}

func TestValidateGCSHAAckGuardRequiresExactRootAndTupleBytes(t *testing.T) {
	guarded := fencing.MutationGuardedLogicalResolution{
		Version:     fencing.MutationGuardedLogicalResolutionVersion,
		Runtime:     fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "namespace-uid", BindingUID: "binding-uid", StatefulSetUID: "sts-uid", SourceClusterID: "source", NodeID: "node", PodUID: "pod", RunID: "run", NodeName: "node-name", NodeUID: "node-uid", MachineID: "machine", BootID: "boot", RuntimeID: "runtime", SandboxID: "sandbox", ContainerID: "container", ImageID: "image"},
		WALIdentity: "wal", ProducerUID: "producer", Sequence: 1,
		Intent:          fencing.MutationIntent{ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: strings.Repeat("a", 64), OperationID: "op", Kind: "upload", Key: "extent", PayloadDigest: strings.Repeat("b", 64), PayloadSize: 4},
		Preparation:     fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: strings.Repeat("a", 64), GenerationSet: true},
		PreparationHash: strings.Repeat("c", 64), DispatchID: "dispatch", Profile: fencing.MutationHeldExecutionProfile,
		GuardRootIdentity: strings.Repeat("d", 64), GuardVersion: fencing.MutationGuardedLogicalResolutionVersion,
	}
	record := gcsHAHeldGuardRecord{Version: guarded.GuardVersion, RootIdentity: guarded.GuardRootIdentity, Runtime: guarded.Runtime,
		WALIdentity: guarded.WALIdentity, ProducerUID: guarded.ProducerUID, Sequence: guarded.Sequence, Intent: guarded.Intent,
		Preparation: guarded.Preparation, PreparationHash: guarded.PreparationHash, DispatchID: guarded.DispatchID}
	body, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateGCSHAAckGuard(body, guarded, guarded.GuardRootIdentity); err != nil {
		t.Fatalf("valid guard rejected: %v", err)
	}
	if err := validateGCSHAAckGuard(body, guarded, strings.Repeat("e", 64)); err == nil {
		t.Fatal("guard from another canonical root was accepted")
	}
	changed := record
	changed.Intent.Key = "mutated"
	mutated, err := json.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateGCSHAAckGuard(mutated, guarded, guarded.GuardRootIdentity); err == nil {
		t.Fatal("guard with a mutated canonical intent was accepted")
	}
}
