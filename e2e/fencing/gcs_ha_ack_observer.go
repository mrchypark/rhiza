package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
)

// gcsHAAckObservation is derived from the canonical root, producer journal,
// and immutable guard object. The private fault receipt is only a selector.
type gcsHAAckObservation struct {
	Runtime     fencing.RuntimeBinding
	ProducerUID string
	Sequence    uint64
	DispatchID  string
	Guarded     fencing.MutationGuardedLogicalResolution
	GuardSHA256 string
}

type gcsHAHeldGuardRecord struct {
	Version         int                         `json:"version"`
	RootIdentity    string                      `json:"rootIdentity"`
	Runtime         fencing.RuntimeBinding      `json:"runtime"`
	WALIdentity     string                      `json:"walIdentity"`
	ProducerUID     string                      `json:"producerUID"`
	Sequence        uint64                      `json:"sequence"`
	Intent          fencing.MutationIntent      `json:"intent"`
	Preparation     fencing.MutationPreparation `json:"preparation"`
	PreparationHash string                      `json:"preparationHash"`
	DispatchID      string                      `json:"dispatchID"`
}

// captureGCSAckLossRuntime resolves the one enrolled held-v2 runtime whose
// immutable Pod UID was captured before arming the qualification fault.
func captureGCSAckLossRuntime(ctx context.Context, store *operator.AuthorityStateStore, source gcsRecoverySource, podUID string) (fencing.RuntimeBinding, string, error) {
	if ctx == nil || store == nil || source.StatefulSetUID == "" || source.ClusterID == "" || podUID == "" {
		return fencing.RuntimeBinding{}, "", fmt.Errorf("GCS ACK observer: source root and captured Pod UID are required")
	}
	state, _, err := store.ReadRoot(ctx, source.StatefulSetUID, source.ClusterID)
	if err != nil {
		return fencing.RuntimeBinding{}, "", fmt.Errorf("read canonical GCS source root: %w", err)
	}
	if !state.Ledger.RequireRuntimeBinding || !state.Ledger.RequireMutationJournal || state.StatefulSetUID != source.StatefulSetUID || state.Ledger.SourceClusterID != source.ClusterID {
		return fencing.RuntimeBinding{}, "", fmt.Errorf("GCS ACK observer: canonical source root is not bound to runtime mutation authority")
	}
	if state.Ledger.ClosedBy != "" {
		return fencing.RuntimeBinding{}, "", fmt.Errorf("GCS ACK observer: canonical source root is already closed")
	}
	scope := gcsHAAckSourceScope(source)
	wantScopeHash, err := gcsHAAckScopeHash(scope)
	if err != nil {
		return fencing.RuntimeBinding{}, "", err
	}
	var found *fencing.Run
	for _, run := range state.Ledger.Runs {
		if run.Identity.PodUID != podUID || run.Runtime == nil || run.Stop != nil || run.Exited || run.MutationProfile != fencing.MutationHeldExecutionProfile || run.MutationScopeHash != wantScopeHash || run.MutationProducerUID == "" || run.Identity.WALIdentity == "" {
			continue
		}
		if run.Runtime.StatefulSetUID != source.StatefulSetUID || run.Runtime.SourceClusterID != source.ClusterID || run.Runtime.NamespaceUID != source.NamespaceUID || run.Runtime.BindingUID != source.BindingUID || run.Runtime.PodUID != podUID || run.Runtime.RunID != run.Identity.RunID || run.Runtime.NodeID != run.Identity.NodeID || run.Identity.SourceClusterID != source.ClusterID || run.Runtime.Validate(run.Identity) != nil {
			return fencing.RuntimeBinding{}, "", fmt.Errorf("GCS ACK observer: captured Pod UID has a malformed enrolled runtime")
		}
		if found != nil {
			return fencing.RuntimeBinding{}, "", fmt.Errorf("GCS ACK observer: captured Pod UID has multiple held-v2 enrolled runs")
		}
		copy := run
		found = &copy
	}
	if found == nil {
		return fencing.RuntimeBinding{}, "", fmt.Errorf("GCS ACK observer: no held-v2 enrolled run matches the captured Pod UID and live GCS scope")
	}
	producer, producerUID, _, err := store.ReadProducer(ctx, *found.Runtime)
	if err != nil {
		return fencing.RuntimeBinding{}, "", fmt.Errorf("read enrolled GCS mutation producer: %w", err)
	}
	if producerUID != found.MutationProducerUID || producer.Runtime != *found.Runtime || producer.WALIdentity != found.Identity.WALIdentity || producer.Phase != fencing.MutationOpen {
		return fencing.RuntimeBinding{}, "", fmt.Errorf("GCS ACK observer: producer differs from the exact enrolled source run")
	}
	return *found.Runtime, producerUID, nil
}

func gcsHAAckSourceScope(source gcsRecoverySource) operator.AttestationStoreScope {
	return operator.AttestationStoreScope{Provider: "gcs", Bucket: source.Bucket, Prefix: source.Prefix}
}

func gcsHAAckScopeHash(scope operator.AttestationStoreScope) (string, error) {
	encoded, err := json.Marshal(scope)
	if err != nil {
		return "", fmt.Errorf("encode GCS source scope: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// observeGCSAckLossReceipt binds the log receipt to the still-live canonical
// journal entry and reads the complete immutable guard record. Compacted or
// otherwise unavailable journal entries are inconclusive, never inferred.
func observeGCSAckLossReceipt(ctx context.Context, stateStore *operator.AuthorityStateStore, evidenceStore *operator.AuthorityEvidenceStore, source gcsRecoverySource, receipt operator.GCSMutationAckLossReceipt) (gcsHAAckObservation, error) {
	if ctx == nil || stateStore == nil || evidenceStore == nil {
		return gcsHAAckObservation{}, fmt.Errorf("GCS ACK observer: canonical read stores and context are required")
	}
	if err := validateGCSHAAckReceipt(source, receipt); err != nil {
		return gcsHAAckObservation{}, err
	}
	state, _, err := stateStore.ReadRoot(ctx, receipt.Runtime.StatefulSetUID, receipt.Runtime.SourceClusterID)
	if err != nil {
		return gcsHAAckObservation{}, fmt.Errorf("read canonical root for ACK receipt: %w", err)
	}
	run, ok := state.Ledger.Runs[receipt.SourceIdentity.RunID]
	if !ok || state.StatefulSetUID != receipt.Runtime.StatefulSetUID || state.Ledger.SourceClusterID != receipt.Runtime.SourceClusterID || run.Identity != receipt.SourceIdentity || run.Runtime == nil || *run.Runtime != receipt.Runtime || run.MutationProducerUID != receipt.ProducerUID || run.MutationScopeHash != receipt.Intent.StoreScopeHash || run.MutationProfile != fencing.MutationHeldExecutionProfile || receipt.Runtime.NamespaceUID != source.NamespaceUID || receipt.Runtime.BindingUID != source.BindingUID {
		return gcsHAAckObservation{}, fmt.Errorf("GCS ACK observer: receipt does not match the exact canonical source run")
	}
	wantRoot := gcsHAAuthorityRootIdentity(receipt.Runtime.StatefulSetUID, receipt.Runtime.SourceClusterID)
	if evidenceStore.Identity() == "" || evidenceStore.Identity() != wantRoot {
		return gcsHAAckObservation{}, fmt.Errorf("GCS ACK observer: evidence store is not scoped to the canonical source root")
	}
	producer, producerUID, _, err := stateStore.ReadProducer(ctx, receipt.Runtime)
	if err != nil {
		return gcsHAAckObservation{}, fmt.Errorf("read canonical producer for ACK receipt: %w", err)
	}
	if producerUID != receipt.ProducerUID || producer.Runtime != receipt.Runtime || producer.WALIdentity != receipt.SourceIdentity.WALIdentity {
		return gcsHAAckObservation{}, fmt.Errorf("GCS ACK observer: canonical producer identity differs from receipt")
	}
	if receipt.Sequence <= producer.CompactedThrough {
		return gcsHAAckObservation{}, fmt.Errorf("GCS ACK observer: receipt sequence %d is compacted through %d; exact live entry unavailable", receipt.Sequence, producer.CompactedThrough)
	}
	entry, ok := producer.Entries[receipt.Sequence]
	if !ok || entry.Preparation == nil || entry.Intent != receipt.Intent || *entry.Preparation != receipt.Preparation || entry.Intent.OperationID != receipt.OperationID || entry.DispatchClosed != true || len(entry.Dispatches) != 1 {
		return gcsHAAckObservation{}, fmt.Errorf("GCS ACK observer: canonical entry does not match receipt intent, preparation, or sealed dispatch")
	}
	dispatch, ok := entry.Dispatches[receipt.DispatchID]
	if !ok || receipt.DispatchID == "" || dispatch.ReceiptDigest != "" {
		return gcsHAAckObservation{}, fmt.Errorf("GCS ACK observer: receipt does not identify the unresolved original dispatch")
	}
	if err := producer.ValidateGuardedLogicalResolution(producerUID, receipt.Sequence, entry); err != nil {
		return gcsHAAckObservation{}, fmt.Errorf("validate canonical guarded logical resolution: %w", err)
	}
	guarded := *entry.GuardedLogicalResolution
	if guarded.Runtime != receipt.Runtime || guarded.WALIdentity != receipt.SourceIdentity.WALIdentity || guarded.ProducerUID != receipt.ProducerUID || guarded.Sequence != receipt.Sequence || guarded.Intent != receipt.Intent || guarded.Preparation != receipt.Preparation || guarded.DispatchID != receipt.DispatchID || guarded.GuardRootIdentity != evidenceStore.Identity() || guarded.ObjectSize != receipt.PayloadSize || guarded.PayloadDigest != receipt.PayloadDigest {
		return gcsHAAckObservation{}, fmt.Errorf("GCS ACK observer: guarded resolution differs from exact receipt tuple or evidence root")
	}
	reader, _, err := evidenceStore.Read(ctx, guarded.GuardRef)
	if err != nil {
		return gcsHAAckObservation{}, fmt.Errorf("read immutable held-create guard: %w", err)
	}
	guardBytes, readErr := io.ReadAll(io.LimitReader(reader, 1<<20))
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return gcsHAAckObservation{}, fmt.Errorf("read immutable held-create guard bytes: %w", err)
	}
	if len(guardBytes) == 1<<20 {
		return gcsHAAckObservation{}, fmt.Errorf("GCS ACK observer: held-create guard exceeds the bounded record size")
	}
	if err := validateGCSHAAckGuard(guardBytes, guarded, evidenceStore.Identity()); err != nil {
		return gcsHAAckObservation{}, err
	}
	digest := sha256.Sum256(guardBytes)
	if hex.EncodeToString(digest[:]) != guarded.GuardDigest {
		return gcsHAAckObservation{}, fmt.Errorf("GCS ACK observer: immutable guard digest differs from canonical guarded resolution")
	}
	return gcsHAAckObservation{Runtime: receipt.Runtime, ProducerUID: producerUID, Sequence: receipt.Sequence, DispatchID: receipt.DispatchID, Guarded: guarded, GuardSHA256: hex.EncodeToString(digest[:])}, nil
}

func gcsHAAuthorityRootIdentity(statefulSetUID, sourceClusterID string) string {
	data, _ := json.Marshal([]string{"rhiza-authority-root-v1", statefulSetUID, sourceClusterID})
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func validateGCSHAAckReceipt(source gcsRecoverySource, receipt operator.GCSMutationAckLossReceipt) error {
	if receipt.Version != 1 || !receipt.Injected || receipt.Reason != "" || receipt.WireRequests != 1 || receipt.Runtime.Validate(receipt.SourceIdentity) != nil || receipt.SourceIdentity.WALIdentity == "" ||
		receipt.Runtime.StatefulSetUID != source.StatefulSetUID || receipt.Runtime.SourceClusterID != source.ClusterID || receipt.Runtime.NamespaceUID != source.NamespaceUID || receipt.Runtime.BindingUID != source.BindingUID || !fencing.IsHeldCreateEligible(receipt.Intent, receipt.Preparation) {
		return fmt.Errorf("GCS ACK observer: receipt is not one successful-response-loss event for a complete runtime")
	}
	scope := gcsHAAckSourceScope(source)
	if receipt.Scope != scope || receipt.Bucket != source.Bucket || receipt.Prefix != source.Prefix || receipt.ObjectKey != receipt.Intent.Key || receipt.PayloadDigest != receipt.Intent.PayloadDigest || receipt.PayloadSize != receipt.Intent.PayloadSize || receipt.OperationID != receipt.Intent.OperationID || receipt.Scope.Provider != "gcs" || receipt.Scope.Endpoint != "" || receipt.Scope.Account != "" || receipt.Scope.ConnectionStringDigest != "" {
		return fmt.Errorf("GCS ACK observer: receipt storage scope or object tuple differs from live source configuration")
	}
	scopeHash, err := gcsHAAckScopeHash(scope)
	if err != nil || receipt.Intent.StoreScopeHash != scopeHash {
		return fmt.Errorf("GCS ACK observer: receipt intent is not bound to live source scope")
	}
	wantTuple := gcsHAAckTupleDigest(receipt)
	if receipt.TupleDigest == "" || receipt.TupleDigest != wantTuple {
		return fmt.Errorf("GCS ACK observer: receipt tuple digest does not match its full fields")
	}
	return nil
}

func gcsHAAckTupleDigest(receipt operator.GCSMutationAckLossReceipt) string {
	data, _ := json.Marshal([]any{receipt.Runtime, receipt.SourceIdentity, receipt.ProducerUID, receipt.Sequence, receipt.DispatchID, receipt.Intent, receipt.Preparation, receipt.Scope})
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func validateGCSHAAckGuard(data []byte, guarded fencing.MutationGuardedLogicalResolution, rootIdentity string) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var record gcsHAHeldGuardRecord
	if err := decoder.Decode(&record); err != nil {
		return fmt.Errorf("decode immutable held-create guard: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return fmt.Errorf("immutable held-create guard has trailing data")
	}
	want := gcsHAHeldGuardRecord{Version: guarded.GuardVersion, RootIdentity: rootIdentity, Runtime: guarded.Runtime, WALIdentity: guarded.WALIdentity,
		ProducerUID: guarded.ProducerUID, Sequence: guarded.Sequence, Intent: guarded.Intent, Preparation: guarded.Preparation,
		PreparationHash: guarded.PreparationHash, DispatchID: guarded.DispatchID}
	canonical, err := json.Marshal(want)
	if err != nil || !bytes.Equal(canonical, data) || record != want {
		return fmt.Errorf("immutable held-create guard bytes differ from the full canonical resolution tuple")
	}
	return nil
}
