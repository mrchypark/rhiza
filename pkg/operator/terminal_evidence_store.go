package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
)

// Retain the actual evidence, not a replacement cooperative drain receipt.
// Mutation archives and provider receipts referenced here must remain retained.
type terminalEvidence struct {
	Version        int                      `json:"version"`
	Request        fencing.StopRequest      `json:"request"`
	Runtime        fencing.RuntimeBinding   `json:"runtime"`
	ProducerUID    string                   `json:"producerUID"`
	Storage        AttestationStoreScope    `json:"storage"`
	RuntimeReceipt []byte                   `json:"runtimeReceipt"`
	Producer       fencing.MutationProducer `json:"producer"`
}

func receiptDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func terminalProducer(producer fencing.MutationProducer) fencing.MutationProducer {
	// A resumable verification cursor is not part of the terminal storage result.
	producer.ArchiveScan = nil
	return producer
}

func (e terminalEvidence) reference(objectVersionID string) fencing.TerminalReference {
	digest := hashJSON(e)
	return fencing.TerminalReference{
		EvidenceVersion: 2, Request: e.Request,
		RuntimeHash: fencing.RuntimeBindingDigest(e.Runtime), ProducerUID: e.ProducerUID,
		ProofName: "rhiza-terminal-" + digest[:40], ProofUID: objectVersionID, ProofDigest: digest,
		RuntimeReceiptDigest:  receiptDigest(e.RuntimeReceipt),
		MutationReceiptDigest: hashJSON(e.Producer), StorageScopeDigest: hashJSON(e.Storage),
	}
}

func (e terminalEvidence) validate() error {
	p := e.Producer
	if e.Version != 2 || e.Runtime.Validate(e.Request.Identity) != nil || e.ProducerUID == "" ||
		e.Storage.Provider == "" || e.Storage.Provider == "filesystem" || e.Storage.Bucket == "" || e.Storage.Prefix == "" ||
		len(e.RuntimeReceipt) == 0 || len(e.RuntimeReceipt) > 65536 || p.ArchiveScan != nil ||
		p.Validate() != nil || p.Runtime != e.Runtime || p.WALIdentity != e.Request.Identity.WALIdentity ||
		p.CloseOperationID != e.Request.OperationID || p.CloseRequestHash != e.Request.RequestHash || !p.BarrierComplete() {
		return fmt.Errorf("fencing: incomplete or mismatched terminal evidence")
	}
	return nil
}

func terminalArtifactKey(name string) (string, error) {
	if len(name) != len("rhiza-terminal-")+40 || !strings.HasPrefix(name, "rhiza-terminal-") {
		return "", fmt.Errorf("fencing: invalid terminal evidence object name")
	}
	return "terminal/" + name, nil
}

func terminalObjectVersionID(version *objstore.ObjectVersion) (string, error) {
	if !validAuthorityObjectVersion(version) {
		return "", fmt.Errorf("fencing: terminal evidence object version is unavailable")
	}
	return hashJSON([]any{"rhiza-terminal-object-version-v1", version.Type, version.Value}), nil
}

func decodeTerminalEvidence(data []byte, ref fencing.TerminalReference, version *objstore.ObjectVersion) (terminalEvidence, error) {
	var evidence terminalEvidence
	versionID, err := terminalObjectVersionID(version)
	if err != nil || ref.ProofUID == "" || versionID != ref.ProofUID {
		return evidence, errors.Join(err, fmt.Errorf("fencing: retained terminal evidence object version differs"))
	}
	if len(data) > 700000 {
		return evidence, fmt.Errorf("fencing: terminal evidence exceeds capacity")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&evidence) != nil || decoder.Decode(new(any)) != io.EOF || evidence.validate() != nil || evidence.reference(versionID) != ref {
		return terminalEvidence{}, fmt.Errorf("fencing: retained terminal evidence contents differ")
	}
	canonical, err := json.Marshal(evidence)
	if err != nil || !bytes.Equal(canonical, data) {
		return terminalEvidence{}, errors.Join(err, fmt.Errorf("fencing: retained terminal evidence bytes are not canonical"))
	}
	return evidence, nil
}

func readTerminalEvidence(ctx context.Context, store *AuthorityEvidenceStore, ref fencing.TerminalReference) (terminalEvidence, error) {
	key, err := terminalArtifactKey(ref.ProofName)
	if err != nil {
		return terminalEvidence{}, err
	}
	raw, version, err := store.Read(ctx, key)
	if err != nil {
		return terminalEvidence{}, err
	}
	data, readErr := io.ReadAll(io.LimitReader(raw, 700001))
	closeErr := raw.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return terminalEvidence{}, err
	}
	return decodeTerminalEvidence(data, ref, version)
}

// A stored digest is not evidence of termination. Recheck trusted runtime and
// mutation receipts, and the entire committed archive chain for this operation.
func (e *FencingExecutor) verifyTerminalEvidence(ctx context.Context, evidence terminalEvidence, verificationID string) error {
	if err := evidence.validate(); err != nil {
		return err
	}
	if len(verificationID) != 64 || e.VerifyRuntimeEvidence == nil {
		return fmt.Errorf("fencing: runtime evidence verifier or verification identity is unavailable")
	}
	if err := e.VerifyRuntimeEvidence(ctx, evidence.Runtime, evidence.Request, evidence.RuntimeReceipt); err != nil {
		return err
	}
	producer, err := e.reserveMutationProducer(ctx, evidence.Runtime)
	if err != nil {
		return err
	}
	// Compaction may advance after retention, but neither closure nor the frozen
	// admitted interval can change. Never substitute another producer object.
	run, err := e.enrolledMutationRun(ctx, evidence.Runtime)
	if err != nil {
		return err
	}
	if run.Stop == nil || *run.Stop != evidence.Request || run.MutationProducerUID != evidence.ProducerUID ||
		producer.WALIdentity != evidence.Producer.WALIdentity || producer.CloseOperationID != evidence.Request.OperationID || producer.CloseRequestHash != evidence.Request.RequestHash ||
		producer.LastAdmitted != evidence.Producer.LastAdmitted || producer.ClosedThrough != evidence.Producer.ClosedThrough || !producer.BarrierComplete() {
		return fmt.Errorf("fencing: terminal producer closure or coverage differs")
	}
	// Bind every prepared descriptor, including archived work, to this exact
	// workload store. A valid receipt for another bucket/prefix cannot compose.
	verifier := *e
	verifier.VerifyMutationEvidence = func(ctx context.Context, runtime fencing.RuntimeBinding, sequence uint64, entry fencing.MutationEntry) error {
		if entry.Preparation == nil || entry.Preparation.StoreScopeHash != hashJSON(evidence.Storage) {
			return fmt.Errorf("fencing: terminal mutation names another storage scope")
		}
		if e.VerifyMutationEvidence == nil {
			return fmt.Errorf("fencing: mutation evidence verifier is unavailable")
		}
		return e.VerifyMutationEvidence(ctx, runtime, sequence, entry)
	}
	if err := verifier.verifyMutationArchiveReceipts(ctx, fencing.MutationCompactionPlan{Runtime: evidence.Runtime, Entries: evidence.Producer.Entries}); err != nil {
		return err
	}
	if err := verifier.verifyMutationArchiveReceipts(ctx, fencing.MutationCompactionPlan{Runtime: evidence.Runtime, Entries: producer.Entries}); err != nil {
		return err
	}
	// If the retained snapshot had an archive head, a subsequently advanced
	// watermark alone is insufficient: its exact UID/digest must be an ancestor.
	verificationID = hashJSON([]any{"terminal-mutations", verificationID, evidence.reference("").ProofDigest})
	done, found, err := verifier.scanMutationArchiveHistory(ctx, evidence.Runtime, verificationID, 32, evidence.Producer.ArchiveHead, 0)
	if err != nil {
		return err
	}
	if !done {
		return ErrFencePending
	}
	if evidence.Producer.ArchiveHead != nil && !found {
		return fmt.Errorf("fencing: retained terminal archive is outside committed history")
	}
	return nil
}

func (e *FencingExecutor) readIndependentTerminal(ctx context.Context, runtime fencing.RuntimeBinding, storage AttestationStoreScope, verificationID string) (terminalEvidence, fencing.TerminalReference, error) {
	run, err := e.enrolledMutationRun(ctx, runtime)
	if err != nil {
		return terminalEvidence{}, fencing.TerminalReference{}, err
	}
	state, err := e.read(ctx, runtime.StatefulSetUID, runtime.SourceClusterID)
	if err != nil {
		return terminalEvidence{}, fencing.TerminalReference{}, err
	}
	ref, err := state.Ledger.IndependentTerminal(run.Identity.RunID, hashJSON(storage))
	if err != nil {
		return terminalEvidence{}, ref, err
	}
	store, err := e.authorityEvidenceFor(runtime)
	if err != nil {
		return terminalEvidence{}, ref, err
	}
	evidence, err := readTerminalEvidence(ctx, store, ref)
	if err == nil && (evidence.Runtime != runtime || evidence.Storage != storage) {
		err = fmt.Errorf("fencing: retained terminal evidence names another requested incarnation or store")
	}
	if err == nil {
		err = e.verifyTerminalEvidence(ctx, evidence, verificationID)
	}
	return evidence, ref, err
}

// Persist/read back immutable evidence before binding it into the root CAS.
// A lost response is resolved using the exact original reference, not by
// manufacturing another proof or marking a dead run cooperatively Exited.
func (e *FencingExecutor) retainIndependentTerminal(ctx context.Context, runtime fencing.RuntimeBinding, storage AttestationStoreScope, runtimeReceipt []byte) (fencing.TerminalReference, error) {
	run, err := e.enrolledMutationRun(ctx, runtime)
	if err != nil || run.Stop == nil {
		return fencing.TerminalReference{}, errors.Join(err, fmt.Errorf("fencing: terminal run is not closed"))
	}
	verificationID := hashJSON([]any{"retain-terminal", run.Stop, storage})
	if run.IndependentTerminal != nil {
		_, ref, err := e.readIndependentTerminal(ctx, runtime, storage, verificationID)
		return ref, err
	}
	producer, err := e.reserveMutationProducer(ctx, runtime)
	if err != nil {
		return fencing.TerminalReference{}, err
	}
	// Reserving a never-bound producer can pin its UID in the root. Do not
	// retain the pre-reservation enrollment snapshot with an empty producer UID.
	run, err = e.enrolledMutationRun(ctx, runtime)
	if err != nil || run.Stop == nil {
		return fencing.TerminalReference{}, errors.Join(err, fmt.Errorf("fencing: terminal enrollment changed"))
	}
	evidence := terminalEvidence{Version: 2, Request: *run.Stop, Runtime: runtime, ProducerUID: run.MutationProducerUID, Storage: storage,
		RuntimeReceipt: append([]byte(nil), runtimeReceipt...), Producer: terminalProducer(producer)}
	if err := e.verifyTerminalEvidence(ctx, evidence, verificationID); err != nil {
		return fencing.TerminalReference{}, err
	}
	data, err := json.Marshal(evidence)
	if err != nil || len(data) > 700000 {
		return fencing.TerminalReference{}, errors.Join(err, fmt.Errorf("fencing: terminal evidence exceeds capacity"))
	}
	store, err := e.authorityEvidenceFor(runtime)
	if err != nil {
		return fencing.TerminalReference{}, err
	}
	key, err := terminalArtifactKey(evidence.reference("pending").ProofName)
	if err != nil {
		return fencing.TerminalReference{}, err
	}
	version, err := store.CreateImmutable(ctx, key, data)
	if err != nil {
		return fencing.TerminalReference{}, err
	}
	versionID, err := terminalObjectVersionID(version)
	if err != nil {
		return fencing.TerminalReference{}, err
	}
	ref := evidence.reference(versionID)
	if _, err := readTerminalEvidence(ctx, store, ref); err != nil {
		return ref, errors.Join(err, fmt.Errorf("fencing: terminal evidence object readback differs"))
	}
	_, commitErr := e.change(ctx, runtime.StatefulSetUID, runtime.SourceClusterID, func(l *fencing.Ledger) error { return l.RecordIndependentTerminal(ref) })
	_, persisted, readErr := e.readIndependentTerminal(ctx, runtime, storage, verificationID)
	if readErr != nil || persisted != ref {
		return ref, errors.Join(commitErr, readErr, fmt.Errorf("fencing: terminal reference commit is unconfirmed"))
	}
	return ref, nil
}
