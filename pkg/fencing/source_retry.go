package fencing

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const maxSourceRetryBytes = 1 << 20

// sourceRetryState is caller retention, not an admission or execution authority.
// It must stay disabled at Node startup until staging and the authority client
// are wired. Never create it as a fallback for lost state of an old producer.
type sourceRetryState struct {
	mu       sync.Mutex
	dir      string
	lock     *os.File
	data     sourceRetryData
	fatal    error
	syncFile func(*os.File) error
	syncDir  func(string) error
}

type sourceRetryData struct {
	Binding BindReceipt       `json:"binding"`
	Steps   []sourceRetryStep `json:"steps"`
}

type sourceRetryStep struct {
	Sequence uint64        `json:"sequence"`
	Entry    MutationEntry `json:"entry"`
	Admitted bool          `json:"admitted"`
	Advanced bool          `json:"advanced"`
}

type sourceRetryFile struct {
	Data   sourceRetryData `json:"data"`
	Digest string          `json:"digest"`
}

// createSourceRetryState requires a fresh, never-used producer. Existing paths
// are rejected even if their retry file is missing. Recovery uses open only.
func createSourceRetryState(dir string, binding BindReceipt) (*sourceRetryState, error) {
	return loadSourceRetryState(dir, binding, true)
}

func openSourceRetryState(dir string, binding BindReceipt) (*sourceRetryState, error) {
	return loadSourceRetryState(dir, binding, false)
}

func loadSourceRetryState(dir string, binding BindReceipt, create bool) (*sourceRetryState, error) {
	if err := binding.Validate(binding.Identity); err != nil {
		return nil, err
	}
	if binding.Producer == nil {
		return nil, fmt.Errorf("fencing: source retry requires a mutation binding")
	}
	if create {
		if err := os.Mkdir(dir, 0700); err != nil {
			return nil, err
		}
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, fmt.Errorf("fencing: original source retry directory missing: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("fencing: original source retry directory invalid")
	}
	lock, err := os.OpenFile(filepath.Join(dir, "retry.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("fencing: source retry state already owned: %w", err)
	}
	s := &sourceRetryState{dir: dir, lock: lock, syncFile: (*os.File).Sync, syncDir: syncSourceRetryDir}
	failed := true
	defer func() {
		if failed {
			_ = s.Close()
		}
	}()
	if create {
		if err := s.persist(sourceRetryData{Binding: binding.clone()}); err != nil {
			return nil, err
		}
	} else {
		path := filepath.Join(dir, "retry.json")
		info, err := os.Lstat(path)
		if err != nil {
			return nil, fmt.Errorf("fencing: original source retry file missing: %w", err)
		}
		if !info.Mode().IsRegular() || info.Size() > maxSourceRetryBytes {
			return nil, fmt.Errorf("fencing: original source retry file invalid")
		}
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(io.LimitReader(file, maxSourceRetryBytes+1))
		// A previous rename/fsync may have failed. Re-establish durability before
		// exposing recovered tuples to a possible first admission.
		err = errors.Join(readErr, file.Sync(), file.Close())
		if err != nil {
			return nil, err
		}
		if len(data) > maxSourceRetryBytes {
			return nil, fmt.Errorf("fencing: source retry file exceeds byte capacity")
		}
		var saved sourceRetryFile
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&saved); err != nil {
			return nil, err
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return nil, fmt.Errorf("fencing: trailing source retry state")
		}
		if sourceRetryDigest(saved.Data) != saved.Digest || !binding.SameBinding(saved.Data.Binding) {
			return nil, fmt.Errorf("fencing: source retry binding or checksum differs")
		}
		if err := saved.Data.validate(); err != nil {
			return nil, err
		}
		s.data = saved.Data
	}
	for _, directory := range []string{dir, filepath.Dir(dir)} {
		if err := s.syncDir(directory); err != nil {
			return nil, err
		}
	}
	failed = false
	return s, nil
}

func (d sourceRetryData) validate() error {
	if err := d.Binding.Validate(d.Binding.Identity); err != nil {
		return err
	}
	if d.Binding.Producer == nil || len(d.Steps) > MaxMutationJournalEntries {
		return fmt.Errorf("fencing: source retry binding or capacity invalid")
	}
	owners := make(map[MutationOwnerStep]bool, len(d.Steps))
	for i, step := range d.Steps {
		intent := step.Entry.Intent
		if step.Sequence != uint64(i)+1 || intent.OperationID != MutationRequestID(d.Binding.Producer.ProducerUID, step.Sequence) || intent.StoreScopeHash != d.Binding.Producer.StoreScopeHash || owners[intent.OwnerStep] {
			return fmt.Errorf("fencing: source retry original tuple/owner differs")
		}
		owners[intent.OwnerStep] = true
		p := MutationProducer{Runtime: d.Binding.Producer.Runtime, WALIdentity: d.Binding.Identity.WALIdentity}
		if err := p.validateEntry(step.Sequence, step.Entry); err != nil {
			return err
		}
		if (!step.Admitted && (i != len(d.Steps)-1 || step.Entry.Resolution != nil)) || (step.Advanced && step.Entry.Resolution == nil) {
			return fmt.Errorf("fencing: source advanced past uncertain admission or result")
		}
	}
	return nil
}

// allocate is called only after external payload sealing (or exact delete
// selection). It fsyncs the original tuple before returning it to an Admit caller.
func (s *sourceRetryState) allocate(intent MutationIntent) (sourceRetryStep, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return sourceRetryStep{}, err
	}
	for _, step := range s.data.Steps {
		if step.Entry.Intent.OwnerStep == intent.OwnerStep {
			if intent.OperationID != "" && intent.OperationID != step.Entry.Intent.OperationID {
				return sourceRetryStep{}, fmt.Errorf("fencing: retry changed original request identity")
			}
			intent.OperationID = step.Entry.Intent.OperationID
			if intent != step.Entry.Intent {
				return sourceRetryStep{}, fmt.Errorf("fencing: retry changed original owner intent")
			}
			return cloneSourceRetryStep(step), nil
		}
	}
	// shortcut: retain 64 owner tombstones; reclaim only after a separately
	// specified durable owner-retirement contract, never on a TTL or snapshot.
	if len(s.data.Steps) == MaxMutationJournalEntries {
		return sourceRetryStep{}, fmt.Errorf("fencing: source retry capacity exhausted; retain original owner history")
	}
	if len(s.data.Steps) > 0 && !s.data.Steps[len(s.data.Steps)-1].Admitted {
		return sourceRetryStep{}, ErrPending
	}
	sequence := uint64(len(s.data.Steps)) + 1
	if intent.OperationID != "" {
		return sourceRetryStep{}, fmt.Errorf("fencing: new owner cannot supply a different request identity")
	}
	intent.OperationID = MutationRequestID(s.data.Binding.Producer.ProducerUID, sequence)
	step := sourceRetryStep{Sequence: sequence, Entry: MutationEntry{Intent: intent}}
	next := s.data
	next.Steps = append(append([]sourceRetryStep(nil), s.data.Steps...), step)
	if err := s.persist(next); err != nil {
		return sourceRetryStep{}, err
	}
	return step, nil
}

// admitted records a durable authority admission response, not a provider ACK.
func (s *sourceRetryState) admitted(sequence uint64, intent MutationIntent) error {
	return s.changeStep(sequence, intent, func(step *sourceRetryStep) error {
		step.Admitted = true
		return nil
	})
}

// resolved accepts only the original entry from the authenticated, verified
// authority result path. Local validation is not authentication of provider proof.
func (s *sourceRetryState) resolved(sequence uint64, entry MutationEntry) error {
	return s.changeStep(sequence, entry.Intent, func(step *sourceRetryStep) error {
		if entry.Resolution == nil {
			return ErrPending
		}
		if step.Entry.Resolution != nil && *step.Entry.Resolution != *entry.Resolution {
			return fmt.Errorf("fencing: original mutation result changed")
		}
		step.Entry = cloneSourceRetryStep(sourceRetryStep{Entry: entry}).Entry
		step.Admitted = true
		return nil
	})
}

// advance retains the owner tombstone. It does not free capacity by forgetting
// an old owner, nor turn Excluded/Rejected into a successful mutation return.
func (s *sourceRetryState) advance(sequence uint64, intent MutationIntent) error {
	return s.changeStep(sequence, intent, func(step *sourceRetryStep) error {
		if step.Entry.Resolution == nil {
			return ErrPending
		}
		step.Advanced = true
		return nil
	})
}

func (s *sourceRetryState) changeStep(sequence uint64, intent MutationIntent, change func(*sourceRetryStep) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	if sequence == 0 || sequence > uint64(len(s.data.Steps)) || s.data.Steps[sequence-1].Entry.Intent != intent {
		return fmt.Errorf("fencing: source retry original tuple differs")
	}
	next := s.data
	next.Steps = append([]sourceRetryStep(nil), s.data.Steps...)
	if err := change(&next.Steps[sequence-1]); err != nil {
		return err
	}
	return s.persist(next)
}

func (s *sourceRetryState) usable() error {
	if s.lock == nil {
		return fmt.Errorf("fencing: source retry state closed")
	}
	return s.fatal
}

func (s *sourceRetryState) persist(next sourceRetryData) error {
	if err := next.validate(); err != nil {
		return err
	}
	data, err := json.Marshal(sourceRetryFile{Data: next, Digest: sourceRetryDigest(next)})
	if err != nil {
		return err
	}
	if len(data) > maxSourceRetryBytes {
		return fmt.Errorf("fencing: source retry byte capacity exhausted")
	}
	if err := s.writeAtomic(data); err != nil {
		s.fatal = fmt.Errorf("fencing: source retry persistence uncertain; reopen original state: %w", err)
		return s.fatal
	}
	s.data = next
	return nil
}

func (s *sourceRetryState) writeAtomic(data []byte) error {
	f, err := os.CreateTemp(s.dir, ".retry-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if n, err := f.Write(data); err != nil || n != len(data) {
		_ = f.Close()
		return errors.Join(err, io.ErrShortWrite)
	}
	if err := s.syncFile(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), filepath.Join(s.dir, "retry.json")); err != nil {
		return err
	}
	return s.syncDir(s.dir)
}

func (s *sourceRetryState) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return nil
	}
	err := syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
	err = errors.Join(err, s.lock.Close())
	s.lock = nil
	return err
}

func sourceRetryDigest(data sourceRetryData) string {
	encoded, _ := json.Marshal(data)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func syncSourceRetryDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func cloneSourceRetryStep(step sourceRetryStep) sourceRetryStep {
	data, _ := json.Marshal(step)
	var copy sourceRetryStep
	_ = json.Unmarshal(data, &copy)
	return copy
}
