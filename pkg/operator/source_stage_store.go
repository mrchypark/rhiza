package operator

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
	"strings"
	"sync"
	"syscall"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

const (
	maxSourceStageTickets = 64
	maxSourceStageBytes   = 64 << 20
	maxSourceStageTotal   = maxSourceStageTickets * maxSourceStageBytes
)

type sourceStageDescriptor struct {
	Ref    string `json:"ref"`
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

type sourceStageRecord struct {
	ProducerUID string                 `json:"producerUID"`
	Ticket      string                 `json:"ticket"`
	Draft       fencing.MutationIntent `json:"draft"`
	Receiving   *sourceStageDescriptor `json:"receiving,omitempty"`
	Payload     *sourceStageDescriptor `json:"payload,omitempty"`
	Cancelled   bool                   `json:"cancelled,omitempty"`
}

type sourceStageManifest struct {
	Version     int                 `json:"version"`
	Incarnation string              `json:"incarnation"`
	Records     []sourceStageRecord `json:"records"`
	Digest      string              `json:"digest"`
}

// sourceStageStore owns immutable bytes for one explicitly provisioned local
// POSIX spool incarnation. It deliberately refuses to initialize missing state.
type SourceStageStore struct {
	mu          sync.Mutex
	dir         string
	incarnation string
	lock        *os.File
	fatal       error
	manifest    sourceStageManifest
	syncFile    func(*os.File) error
	syncDir     func(string) error
}

func OpenSourceStageStore(dir, incarnation string) (*SourceStageStore, error) {
	if dir == "" || !validStageDigest(incarnation) {
		return nil, fmt.Errorf("fencing: explicit staging directory and incarnation required")
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, fmt.Errorf("fencing: source staging directory missing or unsafe: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("fencing: source staging directory missing or unsafe")
	}
	fd, err := syscall.Open(filepath.Join(dir, ".lock"), syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), filepath.Join(dir, ".lock"))
	lockInfo, err := lock.Stat()
	if err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("fencing: source staging lock file unsafe: %w", err)
	}
	if !lockInfo.Mode().IsRegular() || lockInfo.Mode().Perm()&0077 != 0 {
		_ = lock.Close()
		return nil, fmt.Errorf("fencing: source staging lock file unsafe")
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("fencing: source staging spool already owned: %w", err)
	}
	s := &SourceStageStore{dir: dir, incarnation: incarnation, lock: lock, syncFile: (*os.File).Sync, syncDir: syncSourceStageDir}
	if err := s.readManifest(); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func (s *SourceStageStore) readManifest() error {
	path := filepath.Join(s.dir, "stages.json")
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("fencing: original source staging manifest missing: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 1<<20 {
		return fmt.Errorf("fencing: source staging manifest invalid")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	data, readErr := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	err = errors.Join(readErr, s.syncFile(f), f.Close())
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return fmt.Errorf("fencing: source staging manifest exceeds capacity")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&s.manifest); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("fencing: trailing source staging manifest")
	}
	if s.manifest.Version != 1 || s.manifest.Incarnation != s.incarnation || s.manifest.Records == nil || len(s.manifest.Records) > maxSourceStageTickets || sourceStageManifestDigest(s.manifest) != s.manifest.Digest {
		return fmt.Errorf("fencing: source staging incarnation or capacity differs")
	}
	type ownerKey struct {
		Producer string
		Owner    fencing.MutationOwnerStep
	}
	seen := make(map[string]bool, len(s.manifest.Records))
	seenOwner := make(map[ownerKey]bool, len(s.manifest.Records))
	var retained int64
	for _, record := range s.manifest.Records {
		owner := ownerKey{Producer: record.ProducerUID, Owner: record.Draft.OwnerStep}
		if !validStageTicket(record.Ticket) || record.ProducerUID == "" || !validStageDraft(record.Draft) || seen[record.Ticket] || seenOwner[owner] {
			return fmt.Errorf("fencing: invalid source staging record")
		}
		seen[record.Ticket] = true
		seenOwner[owner] = true
		if record.Receiving != nil && (record.Receiving.Ref != "payload-"+record.Ticket || record.Receiving.Size < 0 || record.Receiving.Size > maxSourceStageBytes || !validStageDigest(record.Receiving.Digest)) {
			return fmt.Errorf("fencing: invalid receiving source stage descriptor")
		}
		if record.Payload != nil {
			if record.Cancelled || record.Receiving == nil || *record.Receiving != *record.Payload {
				return fmt.Errorf("fencing: cancelled source stage retains payload")
			}
			if record.Payload.Ref != "payload-"+record.Ticket || record.Payload.Size < 0 || record.Payload.Size > maxSourceStageBytes || !validStageDigest(record.Payload.Digest) {
				return fmt.Errorf("fencing: invalid source staging descriptor")
			}
			if err := s.verify(*record.Payload); err != nil {
				return err
			}
			retained += record.Payload.Size
		} else if record.Receiving != nil {
			path := filepath.Join(s.dir, record.Receiving.Ref)
			if _, err := os.Lstat(path); err == nil {
				if err := s.verify(*record.Receiving); err != nil {
					return err
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			retained += record.Receiving.Size
		}
		if record.Cancelled && record.Receiving != nil {
			if _, err := os.Lstat(filepath.Join(s.dir, record.Receiving.Ref)); err == nil {
				return fmt.Errorf("fencing: cancelled source stage has finalized bytes")
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	if retained > maxSourceStageTotal {
		return fmt.Errorf("fencing: source staging inventory exceeds byte capacity")
	}
	allowed := map[string]bool{".lock": true, "stages.json": true}
	receiving := make(map[string]bool, len(s.manifest.Records))
	for _, record := range s.manifest.Records {
		if record.Payload != nil {
			allowed[record.Payload.Ref] = true
		} else if record.Receiving != nil {
			allowed[record.Receiving.Ref] = true
			receiving[record.Ticket] = true
		}
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	var stale []string
	for _, entry := range entries {
		name := entry.Name()
		if allowed[name] {
			continue
		}
		ticket, recognized := sourceStageTempTicket(name)
		if !recognized || ticket != "" && !receiving[ticket] {
			return fmt.Errorf("fencing: unaccounted source staging file %q", name)
		}
		info, err := os.Lstat(filepath.Join(s.dir, name))
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return fmt.Errorf("fencing: unsafe source staging temporary %q", name)
		}
		stale = append(stale, name)
	}
	var removeErr error
	for _, name := range stale {
		removeErr = errors.Join(removeErr, os.Remove(filepath.Join(s.dir, name)))
	}
	if err := errors.Join(removeErr, s.syncDir(s.dir)); err != nil {
		return err
	}
	return nil
}

// Only temporary names emitted by CreateTemp are recoverable. A receiving
// payload temp is eligible only while its exact ticket has a durable descriptor.
func sourceStageTempTicket(name string) (string, bool) {
	var suffix string
	if strings.HasPrefix(name, ".manifest-") {
		suffix = strings.TrimPrefix(name, ".manifest-")
	} else if strings.HasPrefix(name, ".receiving-") {
		rest := strings.TrimPrefix(name, ".receiving-")
		ticket, random, ok := strings.Cut(rest, "-")
		if !ok || !validStageTicket(ticket) {
			return "", false
		}
		return ticket, validStageTempSuffix(random)
	} else {
		return "", false
	}
	return "", validStageTempSuffix(suffix)
}

func validStageTempSuffix(suffix string) bool {
	if suffix == "" {
		return false
	}
	for _, c := range suffix {
		if c < 'a' || c > 'z' {
			if c < '0' || c > '9' {
				return false
			}
		}
	}
	return true
}

func (s *SourceStageStore) Reserve(producerUID, ticket string, draft fencing.MutationIntent) error {
	if producerUID == "" || !validStageTicket(ticket) || !validStageDraft(draft) {
		return fmt.Errorf("fencing: invalid source staging reservation")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil || s.fatal != nil {
		return fmt.Errorf("fencing: source staging store closed")
	}
	for _, current := range s.manifest.Records {
		if current.Ticket == ticket {
			if current.ProducerUID == producerUID && current.Draft == draft && !current.Cancelled {
				return nil
			}
			return fmt.Errorf("fencing: source staging ticket identity changed")
		}
		if current.ProducerUID == producerUID && current.Draft.OwnerStep == draft.OwnerStep {
			return fmt.Errorf("fencing: source staging owner already has another ticket")
		}
	}
	if len(s.manifest.Records) >= maxSourceStageTickets {
		return fmt.Errorf("fencing: source staging ticket capacity exhausted")
	}
	next := s.manifest
	next.Records = append(append([]sourceStageRecord(nil), s.manifest.Records...), sourceStageRecord{ProducerUID: producerUID, Ticket: ticket, Draft: draft})
	return s.persist(next)
}

func (s *SourceStageStore) Seal(producerUID, ticket, digest string, size int64, input io.Reader) (sourceStageDescriptor, error) {
	if producerUID == "" || !validStageTicket(ticket) || !validStageDigest(digest) || size < 0 || size > maxSourceStageBytes || input == nil {
		return sourceStageDescriptor{}, fmt.Errorf("fencing: invalid or oversized source stage")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil || s.fatal != nil {
		return sourceStageDescriptor{}, fmt.Errorf("fencing: source staging store closed")
	}
	i := s.recordIndex(producerUID, ticket)
	if i < 0 {
		return sourceStageDescriptor{}, fmt.Errorf("fencing: source stage was not reserved")
	}
	if s.manifest.Records[i].Cancelled {
		return sourceStageDescriptor{}, fmt.Errorf("fencing: source stage was cancelled")
	}
	record := s.manifest.Records[i]
	descriptor := sourceStageDescriptor{Ref: "payload-" + ticket, Digest: digest, Size: size}
	if record.Receiving != nil && *record.Receiving != descriptor {
		return sourceStageDescriptor{}, fmt.Errorf("fencing: source stage retry changed original digest or length")
	}
	if s.manifest.Records[i].Payload != nil {
		p := *s.manifest.Records[i].Payload
		if p != descriptor {
			return sourceStageDescriptor{}, fmt.Errorf("fencing: original source stage descriptor changed")
		}
		if err := s.verify(p); err != nil {
			return sourceStageDescriptor{}, err
		}
		return p, nil
	}
	var used int64
	for _, stage := range s.manifest.Records {
		if stage.Ticket == ticket {
			continue
		}
		if stage.Payload != nil {
			used += stage.Payload.Size
		} else if stage.Receiving != nil {
			used += stage.Receiving.Size
		}
	}
	if used+size > maxSourceStageTotal {
		return sourceStageDescriptor{}, fmt.Errorf("fencing: source staging byte capacity exhausted")
	}
	if record.Receiving == nil {
		if _, err := os.Lstat(filepath.Join(s.dir, descriptor.Ref)); err == nil {
			return sourceStageDescriptor{}, fmt.Errorf("fencing: unaccounted source staging payload exists")
		} else if !errors.Is(err, os.ErrNotExist) {
			return sourceStageDescriptor{}, err
		}
		next := s.manifest
		next.Records = append([]sourceStageRecord(nil), s.manifest.Records...)
		receiving := descriptor
		next.Records[i].Receiving = &receiving
		if err := s.persist(next); err != nil {
			return sourceStageDescriptor{}, err
		}
	}
	final := filepath.Join(s.dir, descriptor.Ref)
	if _, err := os.Lstat(final); err == nil {
		if err := s.verify(descriptor); err != nil {
			return sourceStageDescriptor{}, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return sourceStageDescriptor{}, err
	} else if err := s.writePayload(final, descriptor, input); err != nil {
		return sourceStageDescriptor{}, err
	}
	next := s.manifest
	next.Records = append([]sourceStageRecord(nil), s.manifest.Records...)
	p := descriptor
	next.Records[i].Payload = &p
	if err := s.persist(next); err != nil {
		return sourceStageDescriptor{}, err
	}
	return descriptor, nil
}

func (s *SourceStageStore) Cancel(producerUID, ticket string) error {
	if producerUID == "" || !validStageTicket(ticket) {
		return fmt.Errorf("fencing: invalid source stage cancellation")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil || s.fatal != nil {
		return fmt.Errorf("fencing: source staging store closed")
	}
	i := s.recordIndex(producerUID, ticket)
	if i < 0 {
		return nil
	}
	record := s.manifest.Records[i]
	if record.Payload != nil {
		return fmt.Errorf("fencing: sealed source stage cannot be cancelled")
	}
	if record.Receiving != nil {
		if _, err := os.Lstat(filepath.Join(s.dir, record.Receiving.Ref)); err == nil {
			return fmt.Errorf("fencing: finalized source stage cannot be cancelled")
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if record.Cancelled {
		return nil
	}
	next := s.manifest
	next.Records = append([]sourceStageRecord(nil), s.manifest.Records...)
	next.Records[i].Cancelled = true
	next.Records[i].Receiving = nil
	return s.persist(next)
}

func validStageDraft(draft fencing.MutationIntent) bool {
	owner := draft.OwnerStep
	return draft.ContractVersion == fencing.MutationIntentVersion && draft.Kind == "upload" && draft.OperationID == "" && draft.PayloadRef == "" && draft.PayloadDigest == "" && draft.PayloadSize == 0 && validStageDigest(draft.StoreScopeHash) && len(draft.Key) <= 1024 && draft.Key != "" && len(owner.Owner) <= 128 && owner.Owner != "" && len(owner.Workflow) <= 128 && owner.Workflow != "" && len(owner.Step) <= 128 && owner.Step != "" && owner.Attempt > 0 && validStageCondition(draft.Condition)
}

func (s *SourceStageStore) recordIndex(producerUID, ticket string) int {
	for i, record := range s.manifest.Records {
		if record.Ticket == ticket && record.ProducerUID == producerUID {
			return i
		}
	}
	return -1
}

func (s *SourceStageStore) writePayload(final string, descriptor sourceStageDescriptor, input io.Reader) error {
	f, err := os.CreateTemp(s.dir, ".receiving-"+strings.TrimPrefix(descriptor.Ref, "payload-")+"-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, hash), io.LimitReader(input, descriptor.Size))
	if copyErr == nil && n == descriptor.Size {
		var extra [1]byte
		if n, err := io.ReadFull(input, extra[:]); n != 0 || err != io.EOF {
			copyErr = errors.Join(err, fmt.Errorf("fencing: source stage exceeds reserved length"))
		}
	}
	if copyErr != nil || n != descriptor.Size || hex.EncodeToString(hash.Sum(nil)) != descriptor.Digest {
		_ = f.Close()
		return errors.Join(copyErr, fmt.Errorf("fencing: source stage length or digest differs"))
	}
	if err := f.Chmod(0400); err != nil {
		_ = f.Close()
		return err
	}
	if err := s.syncFile(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), final); err != nil {
		return err
	}
	if err := s.syncDir(s.dir); err != nil {
		return err
	}
	return s.verify(descriptor)
}

func (s *SourceStageStore) verify(descriptor sourceStageDescriptor) error {
	if !strings.HasPrefix(descriptor.Ref, "payload-") || !validStageTicket(strings.TrimPrefix(descriptor.Ref, "payload-")) || !validStageDigest(descriptor.Digest) || descriptor.Size < 0 || descriptor.Size > maxSourceStageBytes {
		return fmt.Errorf("fencing: invalid source stage reference")
	}
	path := filepath.Join(s.dir, descriptor.Ref)
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("fencing: original staged payload missing: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() != descriptor.Size {
		return fmt.Errorf("fencing: original staged payload differs")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	hash := sha256.New()
	n, readErr := io.Copy(hash, io.LimitReader(f, descriptor.Size+1))
	if readErr != nil || n != descriptor.Size || hex.EncodeToString(hash.Sum(nil)) != descriptor.Digest {
		_ = f.Close()
		return errors.Join(readErr, fmt.Errorf("fencing: original staged payload digest differs"))
	}
	return errors.Join(s.syncFile(f), f.Close())
}

func (s *SourceStageStore) persist(next sourceStageManifest) error {
	if s.fatal != nil {
		return s.fatal
	}
	next.Digest = sourceStageManifestDigest(next)
	data, err := json.Marshal(next)
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return fmt.Errorf("fencing: source staging manifest capacity exhausted")
	}
	f, err := os.CreateTemp(s.dir, ".manifest-")
	if err != nil {
		return s.fail(err)
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(0600); err != nil {
		_ = f.Close()
		return s.fail(err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return s.fail(err)
	}
	if err := s.syncFile(f); err != nil {
		_ = f.Close()
		return s.fail(err)
	}
	if err := f.Close(); err != nil {
		return s.fail(err)
	}
	if err := os.Rename(f.Name(), filepath.Join(s.dir, "stages.json")); err != nil {
		return s.fail(err)
	}
	if err := s.syncDir(s.dir); err != nil {
		return s.fail(err)
	}
	s.manifest = next
	return nil
}

func sourceStageManifestDigest(manifest sourceStageManifest) string {
	manifest.Digest = ""
	data, _ := json.Marshal(manifest)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func (s *SourceStageStore) fail(err error) error {
	s.fatal = err
	return err
}

func (s *SourceStageStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return nil
	}
	lock := s.lock
	s.lock = nil
	return errors.Join(syscall.Flock(int(lock.Fd()), syscall.LOCK_UN), lock.Close())
}

func validStageTicket(value string) bool { return validStageDigest(value) }

func validStageDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.ToLower(value) == value
}

func validStageCondition(condition fencing.MutationCondition) bool {
	switch condition.Mode {
	case fencing.MutationUnconditional, fencing.MutationCreateOnly:
		return condition.Generation == 0
	case fencing.MutationMatchGeneration:
		return condition.Generation > 0
	default:
		return false
	}
}

func syncSourceStageDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
