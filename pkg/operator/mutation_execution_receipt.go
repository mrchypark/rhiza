package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

type mutationExecutionReceipt struct {
	Runtime           fencing.RuntimeBinding      `json:"runtime"`
	WALIdentity       string                      `json:"walIdentity"`
	ProducerUID       string                      `json:"producerUID"`
	Sequence          uint64                      `json:"sequence"`
	Intent            fencing.MutationIntent      `json:"intent"`
	Preparation       fencing.MutationPreparation `json:"preparation"`
	DispatchID        string                      `json:"dispatchID"`
	Outcome           fencing.MutationOutcome     `json:"outcome"`
	ConditionFailed   bool                        `json:"conditionFailed,omitempty"`
	Generation        int64                       `json:"generation"`
	Metageneration    int64                       `json:"metageneration,omitempty"`
	PayloadDigest     string                      `json:"payloadDigest,omitempty"`
	AllCopiesTerminal bool                        `json:"allCopiesTerminal,omitempty"`
	ReceiptDigest     string                      `json:"receiptDigest"`
}

func (r mutationExecutionReceipt) digest() string {
	r.ReceiptDigest = ""
	return hashJSON(r)
}

type MutationExecutionReceiptStore struct {
	mu       sync.Mutex
	dir      string
	lock     *os.File
	evidence *AuthorityEvidenceStore
}

const maxMutationExecutionReceipts = 8192

// openMutationExecutionReceiptStore requires a separately provisioned private
// directory so the stage manifest's strict file inventory remains unchanged.
func OpenMutationExecutionReceiptStore(dir string) (*MutationExecutionReceiptStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("fencing: mutation receipt directory is required")
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("fencing: mutation receipt directory is missing or unsafe")
	}
	fd, err := syscall.Open(filepath.Join(dir, ".lock"), syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), filepath.Join(dir, ".lock"))
	lockInfo, err := lock.Stat()
	if err != nil || !lockInfo.Mode().IsRegular() || lockInfo.Mode().Perm()&0077 != 0 {
		_ = lock.Close()
		return nil, fmt.Errorf("fencing: mutation receipt lock is unsafe")
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("fencing: mutation receipt store already owned: %w", err)
	}
	s := &MutationExecutionReceiptStore{dir: dir, lock: lock}
	entries, err := os.ReadDir(dir)
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	for _, entry := range entries {
		if entry.Name() == ".lock" {
			continue
		}
		if !entry.Type().IsRegular() || !strings.HasPrefix(entry.Name(), "receipt-") || len(entry.Name()) != len("receipt-")+64 {
			_ = s.Close()
			return nil, fmt.Errorf("fencing: unaccounted mutation receipt entry")
		}
		receipt, err := readMutationExecutionReceipt(filepath.Join(dir, entry.Name()))
		if err != nil || entry.Name() != mutationExecutionReceiptName(receipt) {
			_ = s.Close()
			return nil, fmt.Errorf("fencing: invalid or misnamed mutation receipt")
		}
	}
	if len(entries)-1 > maxMutationExecutionReceipts {
		_ = s.Close()
		return nil, fmt.Errorf("fencing: mutation receipt inventory exceeds capacity")
	}
	return s, nil
}

// NewRemoteMutationExecutionReceiptStore makes immutable provider receipts
// durable in the root-bound object store rather than on the operator's disk.
func NewRemoteMutationExecutionReceiptStore(evidence *AuthorityEvidenceStore) (*MutationExecutionReceiptStore, error) {
	if evidence == nil || evidence.Identity() == "" {
		return nil, fmt.Errorf("fencing: remote mutation receipts require root-bound authority evidence")
	}
	return &MutationExecutionReceiptStore{evidence: evidence}, nil
}

// receiptsForRuntime keeps production receipts under the authenticated root's
// object-store evidence prefix. The local store is a compatibility path only
// for legacy/unit executors without canonical authority state.
func (e *FencingExecutor) receiptsForRuntime(runtime fencing.RuntimeBinding) (*MutationExecutionReceiptStore, error) {
	if e != nil && e.AuthorityState != nil {
		evidence, err := e.authorityEvidenceFor(runtime)
		if err != nil {
			return nil, err
		}
		return NewRemoteMutationExecutionReceiptStore(evidence)
	}
	if e == nil || e.SourceMutationReceipts == nil {
		return nil, fmt.Errorf("fencing: mutation receipt store is not configured")
	}
	return e.SourceMutationReceipts, nil
}

func (s *MutationExecutionReceiptStore) Close() error {
	if s != nil && s.evidence != nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return nil
	}
	err := s.lock.Close()
	s.lock = nil
	return err
}

func (s *MutationExecutionReceiptStore) put(receipt mutationExecutionReceipt) (string, error) {
	if s != nil && s.evidence != nil {
		if receipt.Sequence == 0 || receipt.ProducerUID == "" || receipt.WALIdentity == "" || receipt.DispatchID == "" || receipt.ReceiptDigest != receipt.digest() {
			return "", fmt.Errorf("fencing: incomplete mutation execution receipt")
		}
		data, err := json.Marshal(receipt)
		if err != nil {
			return "", err
		}
		if _, err := s.evidence.CreateImmutable(context.Background(), mutationExecutionReceiptName(receipt), data); err != nil {
			return "", err
		}
		return receipt.ReceiptDigest, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil || receipt.Sequence == 0 || receipt.ProducerUID == "" || receipt.WALIdentity == "" || receipt.DispatchID == "" || receipt.ReceiptDigest != receipt.digest() {
		return "", fmt.Errorf("fencing: incomplete mutation execution receipt")
	}
	name := mutationExecutionReceiptName(receipt)
	path := filepath.Join(s.dir, name)
	if saved, err := readMutationExecutionReceipt(path); err == nil {
		if saved != receipt {
			return "", fmt.Errorf("fencing: original mutation receipt is immutable")
		}
		return receipt.ReceiptDigest, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return "", err
	}
	if len(entries)-1 >= maxMutationExecutionReceipts {
		return "", fmt.Errorf("fencing: mutation receipt capacity exhausted")
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		return "", err
	}
	f, err := os.CreateTemp(s.dir, ".receipt-")
	if err != nil {
		return "", err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if err := f.Chmod(0600); err != nil {
		_ = f.Close()
		return "", err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return "", err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	if err := os.Link(temp, path); err != nil {
		if saved, readErr := readMutationExecutionReceipt(path); readErr == nil && saved == receipt {
			return receipt.ReceiptDigest, nil
		}
		return "", err
	}
	if err := syncSourceStageDir(s.dir); err != nil {
		return "", err
	}
	return receipt.ReceiptDigest, nil
}

func mutationExecutionReceiptName(receipt mutationExecutionReceipt) string {
	return "receipt-" + hashJSON([]any{receipt.Runtime, receipt.WALIdentity, receipt.ProducerUID, receipt.Sequence, receipt.Intent.OperationID})
}

// VerifySourceMutationEvidence checks an archived producer entry against its
// exact immutable receipt in the same root-bound store used during execution.
func (e *FencingExecutor) VerifySourceMutationEvidence(ctx context.Context, runtime fencing.RuntimeBinding, sequence uint64, entry fencing.MutationEntry) error {
	if ctx == nil || e == nil || entry.Preparation == nil || entry.Resolution == nil || sequence == 0 {
		return fmt.Errorf("fencing: prepared mutation receipt identity is incomplete")
	}
	run, err := e.enrolledMutationRun(ctx, runtime)
	if err != nil {
		return err
	}
	if entry.GuardedLogicalResolution != nil {
		if run.MutationProfile != fencing.MutationHeldExecutionProfile || e.SourceMutationStorage == nil || e.Kube == nil {
			return fmt.Errorf("fencing: guarded mutation evidence is outside the held GCS profile")
		}
		producer := fencing.MutationProducer{Runtime: runtime, WALIdentity: run.Identity.WALIdentity, Phase: fencing.MutationClosed, ClosedThrough: sequence}
		if err := producer.ValidateGuardedLogicalResolution(run.MutationProducerUID, sequence, entry); err != nil {
			return err
		}
		scope, err := e.sourceMutationStorageScope(ctx, runtime)
		if err != nil || hashJSON(scope) != entry.Intent.StoreScopeHash {
			return errors.Join(err, fmt.Errorf("fencing: guarded mutation storage scope differs"))
		}
		provider, err := newGCSMutationProviderForProfile(e.SourceMutationStorage, scope, run.MutationProfile)
		if err != nil {
			return err
		}
		return e.verifyGuardedLogicalResolution(ctx, runtime, run, run.MutationProducerUID, sequence, entry, provider)
	}
	receipts, err := e.receiptsForRuntime(runtime)
	if err != nil {
		return err
	}
	owner := hashJSON([]any{runtime, run.Identity.WALIdentity, run.MutationProducerUID, sequence, entry.Intent.OperationID})
	receipt, err := receipts.get("receipt-" + owner)
	if err != nil {
		return err
	}
	dispatch, dispatched := entry.Dispatches[receipt.DispatchID]
	resolution := entry.Resolution
	if receipt.Runtime != runtime || receipt.WALIdentity != run.Identity.WALIdentity || receipt.ProducerUID != run.MutationProducerUID ||
		receipt.Sequence != sequence || receipt.Intent != entry.Intent || receipt.Preparation != *entry.Preparation || !mutationReceiptMatchesDispatch(entry, receipt) || !dispatched ||
		receipt.ReceiptDigest != resolution.ReceiptDigest || receipt.Outcome != resolution.Outcome || receipt.ConditionFailed != resolution.ConditionFailed ||
		receipt.PayloadDigest != resolution.PayloadDigest || receipt.AllCopiesTerminal != resolution.AllCopiesTerminal ||
		(resolution.AllCopiesTerminal && dispatch.ReceiptDigest != receipt.ReceiptDigest) {
		return fmt.Errorf("fencing: prepared mutation receipt does not match archived tuple")
	}
	return nil
}

func mutationReceiptMatchesDispatch(entry fencing.MutationEntry, receipt mutationExecutionReceipt) bool {
	if receipt.DispatchID == "" || len(entry.Dispatches) != 1 {
		return false
	}
	_, exists := entry.Dispatches[receipt.DispatchID]
	return exists
}

func (s *MutationExecutionReceiptStore) get(name string) (mutationExecutionReceipt, error) {
	if s != nil && s.evidence != nil {
		var receipt mutationExecutionReceipt
		if !strings.HasPrefix(name, "receipt-") || strings.Contains(name, "/") || len(name) != len("receipt-")+64 {
			return receipt, fmt.Errorf("fencing: invalid mutation receipt reference")
		}
		r, _, err := s.evidence.Read(context.Background(), name)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || s.evidence.IsNotFound(err) {
				return receipt, fmt.Errorf("%w: %v", os.ErrNotExist, err)
			}
			return receipt, err
		}
		data, readErr := io.ReadAll(io.LimitReader(r, (1<<20)+1))
		if err := errors.Join(readErr, r.Close()); err != nil {
			return receipt, err
		}
		if len(data) > 1<<20 {
			return receipt, fmt.Errorf("fencing: mutation execution receipt exceeds capacity")
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&receipt); err != nil {
			return receipt, err
		}
		if err := decoder.Decode(new(any)); err != io.EOF || receipt.ReceiptDigest != receipt.digest() || mutationExecutionReceiptName(receipt) != name {
			return receipt, fmt.Errorf("fencing: mutation execution receipt is corrupt")
		}
		return receipt, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil || !strings.HasPrefix(name, "receipt-") || strings.Contains(name, "/") {
		return mutationExecutionReceipt{}, fmt.Errorf("fencing: invalid mutation receipt reference")
	}
	return readMutationExecutionReceipt(filepath.Join(s.dir, name))
}

func readMutationExecutionReceipt(path string) (mutationExecutionReceipt, error) {
	var receipt mutationExecutionReceipt
	info, err := os.Lstat(path)
	if err != nil {
		return receipt, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 1<<20 {
		return receipt, fmt.Errorf("fencing: mutation execution receipt is unsafe")
	}
	f, err := os.Open(path)
	if err != nil {
		return receipt, err
	}
	data, readErr := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	err = errors.Join(readErr, f.Sync(), f.Close())
	if err != nil {
		return receipt, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF || receipt.ReceiptDigest != receipt.digest() {
		return receipt, fmt.Errorf("fencing: mutation execution receipt is corrupt")
	}
	return receipt, nil
}
