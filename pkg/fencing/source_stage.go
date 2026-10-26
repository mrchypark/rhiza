package fencing

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type sourceSealedPayload struct {
	Ref    string `json:"ref"`
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

type sourceRetryStage struct {
	Ticket        string               `json:"ticket"`
	Draft         MutationIntent       `json:"draft"`
	LocalPayload  *sourceSealedPayload `json:"localPayload,omitempty"`
	RemoteStarted bool                 `json:"remoteStarted,omitempty"`
	Payload       *sourceSealedPayload `json:"payload,omitempty"`
}

func uploadStageDraft(intent MutationIntent) MutationIntent {
	intent.OperationID = ""
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = "", "", 0
	return intent
}

func validateSourceStage(stage sourceRetryStage, scope string) error {
	if !mutationDigestValid(stage.Ticket) || stage.Draft.Kind != "upload" || stage.Draft != uploadStageDraft(stage.Draft) || stage.Draft.StoreScopeHash != scope {
		return fmt.Errorf("fencing: staging ticket original owner/target invalid")
	}
	intent := stage.Draft
	intent.OperationID = stage.Ticket
	if stage.Payload == nil {
		// Only validate the prospective caller fields; this placeholder is never
		// admitted, persisted as payload evidence or sent to the authority.
		intent.PayloadRef, intent.PayloadDigest = "receiving", stage.Ticket
	} else {
		intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = stage.Payload.Ref, stage.Payload.Digest, stage.Payload.Size
	}
	return validateMutationIntent(intent)
}

func (d sourceRetryData) validateStages() error {
	if len(d.Stages) > MaxMutationJournalEntries {
		return fmt.Errorf("fencing: source staging ticket capacity exceeded")
	}
	owners := make(map[MutationOwnerStep]bool, len(d.Stages))
	tickets := make(map[string]bool, len(d.Stages))
	for _, stage := range d.Stages {
		if err := validateSourceStage(stage, d.Binding.Producer.StoreScopeHash); err != nil {
			return err
		}
		if stage.LocalPayload != nil && (stage.LocalPayload.Ref != "payload-"+stage.Ticket || stage.LocalPayload.Size < 0 || stage.LocalPayload.Size > maxStagePayloadBytes || !mutationDigestValid(stage.LocalPayload.Digest)) {
			return fmt.Errorf("fencing: invalid local source retry payload descriptor")
		}
		if stage.RemoteStarted && stage.LocalPayload == nil {
			return fmt.Errorf("fencing: source transport started without retained original bytes")
		}
		if owners[stage.Draft.OwnerStep] || tickets[stage.Ticket] {
			return fmt.Errorf("fencing: duplicate source staging owner or ticket")
		}
		owners[stage.Draft.OwnerStep], tickets[stage.Ticket] = true, true
	}
	return nil
}

func (d sourceRetryData) hasSealedStage(intent MutationIntent) bool {
	for _, stage := range d.Stages {
		if stage.Draft == uploadStageDraft(intent) && stage.Payload != nil {
			return *stage.Payload == (sourceSealedPayload{Ref: intent.PayloadRef, Digest: intent.PayloadDigest, Size: intent.PayloadSize})
		}
	}
	return false
}

// reserveUpload fsyncs the original ticket and target before any reservation or
// payload transmission to the authority. It does not reserve a mutation sequence.
func (s *sourceRetryState) reserveUpload(draft MutationIntent) (sourceRetryStage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return sourceRetryStage{}, err
	}
	for _, stage := range s.data.Stages {
		if stage.Draft.OwnerStep == draft.OwnerStep {
			if stage.Draft != draft {
				return sourceRetryStage{}, fmt.Errorf("fencing: staging retry changed original target")
			}
			return cloneSourceStage(stage), nil
		}
	}
	if len(s.data.Stages) == MaxMutationJournalEntries {
		return sourceRetryStage{}, fmt.Errorf("fencing: source staging ticket capacity exhausted")
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return sourceRetryStage{}, err
	}
	stage := sourceRetryStage{Ticket: hex.EncodeToString(nonce[:]), Draft: draft}
	if err := validateSourceStage(stage, s.data.Binding.Producer.StoreScopeHash); err != nil {
		return sourceRetryStage{}, err
	}
	next := s.data
	next.Stages = append(append([]sourceRetryStage(nil), next.Stages...), stage)
	if err := s.persist(next); err != nil {
		return sourceRetryStage{}, err
	}
	return stage, nil
}

// reserveUploadWithPayload fsyncs the original bytes and their digest/length
// before any staging authority request can be sent. Missing or corrupt bytes
// are a recovery error, never permission to use a new buffer for the old slot.
func (s *sourceRetryState) reserveUploadWithPayload(draft MutationIntent, payload []byte) (sourceRetryStage, error) {
	if int64(len(payload)) > maxStagePayloadBytes {
		return sourceRetryStage{}, fmt.Errorf("fencing: source retry payload exceeds capacity")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return sourceRetryStage{}, err
	}
	for _, saved := range s.data.Stages {
		if saved.Draft.OwnerStep != draft.OwnerStep {
			continue
		}
		if saved.Draft != draft {
			return sourceRetryStage{}, fmt.Errorf("fencing: original source owner has no durable payload")
		}
		if saved.LocalPayload == nil {
			if saved.RemoteStarted || saved.Payload != nil {
				return sourceRetryStage{}, fmt.Errorf("fencing: original source owner has no durable payload")
			}
			digest := sha256.Sum256(payload)
			descriptor := sourceSealedPayload{Ref: "payload-" + saved.Ticket, Digest: hex.EncodeToString(digest[:]), Size: int64(len(payload))}
			next := s.data
			next.Stages = append([]sourceRetryStage(nil), s.data.Stages...)
			var recovered sourceRetryStage
			for i := range next.Stages {
				if next.Stages[i].Ticket == saved.Ticket {
					copyDescriptor := descriptor
					next.Stages[i].LocalPayload = &copyDescriptor
					recovered = cloneSourceStage(next.Stages[i])
					break
				}
			}
			if err := s.persist(next); err != nil {
				return sourceRetryStage{}, err
			}
			if err := s.writeLocalPayload(descriptor, payload); err != nil {
				s.fatal = fmt.Errorf("fencing: local source payload recovery uncertain; reopen original state: %w", err)
				return sourceRetryStage{}, s.fatal
			}
			return recovered, nil
		}
		stored, err := s.readLocalPayload(*saved.LocalPayload)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) || saved.RemoteStarted || saved.Payload != nil {
				return sourceRetryStage{}, err
			}
			digest := sha256.Sum256(payload)
			if int64(len(payload)) != saved.LocalPayload.Size || hex.EncodeToString(digest[:]) != saved.LocalPayload.Digest {
				return sourceRetryStage{}, fmt.Errorf("fencing: retry changed original upload bytes")
			}
			if err := s.writeLocalPayload(*saved.LocalPayload, payload); err != nil {
				return sourceRetryStage{}, err
			}
			return cloneSourceStage(saved), nil
		}
		if !bytes.Equal(stored, payload) {
			return sourceRetryStage{}, fmt.Errorf("fencing: retry changed original upload bytes")
		}
		return cloneSourceStage(saved), nil
	}
	if len(s.data.Stages) == MaxMutationJournalEntries {
		return sourceRetryStage{}, fmt.Errorf("fencing: source staging ticket capacity exhausted")
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return sourceRetryStage{}, err
	}
	ticket := hex.EncodeToString(nonce[:])
	digest := sha256.Sum256(payload)
	descriptor := sourceSealedPayload{Ref: "payload-" + ticket, Digest: hex.EncodeToString(digest[:]), Size: int64(len(payload))}
	stage := sourceRetryStage{Ticket: ticket, Draft: draft, LocalPayload: &descriptor}
	if err := validateSourceStage(stage, s.data.Binding.Producer.StoreScopeHash); err != nil {
		return sourceRetryStage{}, err
	}
	next := s.data
	next.Stages = append(append([]sourceRetryStage(nil), s.data.Stages...), stage)
	if err := s.persist(next); err != nil {
		return sourceRetryStage{}, err
	}
	if err := s.writeLocalPayload(descriptor, payload); err != nil {
		s.fatal = fmt.Errorf("fencing: local source payload persistence uncertain; reopen original state: %w", err)
		return sourceRetryStage{}, s.fatal
	}
	return stage, nil
}

func (s *sourceRetryState) markStageRemoteStarted(ticket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	next := s.data
	next.Stages = append([]sourceRetryStage(nil), s.data.Stages...)
	for i := range next.Stages {
		if next.Stages[i].Ticket == ticket {
			if next.Stages[i].LocalPayload == nil {
				return fmt.Errorf("fencing: source transport cannot start without durable payload")
			}
			next.Stages[i].RemoteStarted = true
			return s.persist(next)
		}
	}
	return fmt.Errorf("fencing: original source stage ticket missing")
}

func (s *sourceRetryState) readLocalPayload(descriptor sourceSealedPayload) ([]byte, error) {
	path := filepath.Join(s.dir, descriptor.Ref)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("fencing: original source retry payload missing: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 || info.Size() != descriptor.Size {
		return nil, fmt.Errorf("fencing: original source retry payload invalid")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, descriptor.Size+1))
	err = errors.Join(readErr, file.Sync(), file.Close())
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	if int64(len(data)) != descriptor.Size || hex.EncodeToString(digest[:]) != descriptor.Digest {
		return nil, fmt.Errorf("fencing: original source retry payload digest or length differs")
	}
	return data, nil
}

func (s *sourceRetryState) writeLocalPayload(descriptor sourceSealedPayload, payload []byte) error {
	final := filepath.Join(s.dir, descriptor.Ref)
	if _, err := os.Lstat(final); err == nil {
		return fmt.Errorf("fencing: unaccounted source retry payload exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.CreateTemp(s.dir, ".payload-"+strings.TrimPrefix(descriptor.Ref, "payload-")+"-")
	if err != nil {
		return err
	}
	if n, err := file.Write(payload); err != nil || n != len(payload) {
		_ = file.Close()
		return errors.Join(err, io.ErrShortWrite)
	}
	if err := s.syncFile(file); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), final); err != nil {
		return err
	}
	return s.syncDir(s.dir)
}

func (s *sourceRetryState) recoverPayloadFiles() error {
	known := make(map[string]sourceSealedPayload, len(s.data.Stages))
	retired := make(map[string]bool, len(s.data.PendingPayloadGC))
	for _, stage := range s.data.Stages {
		if stage.LocalPayload != nil {
			known[stage.LocalPayload.Ref] = *stage.LocalPayload
		}
	}
	for _, ref := range s.data.PendingPayloadGC {
		retired[ref] = true
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	removedTemp := false
	for _, entry := range entries {
		name := entry.Name()
		if name == "retry.lock" || name == "retry.json" {
			continue
		}
		if retired[name] {
			if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
				return fmt.Errorf("fencing: unsafe retired source payload")
			}
			if err := os.Remove(filepath.Join(s.dir, name)); err != nil {
				return err
			}
			removedTemp = true
			continue
		}
		if strings.HasPrefix(name, ".retry-") {
			if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
				return fmt.Errorf("fencing: unsafe source retry temporary file")
			}
			if err := os.Remove(filepath.Join(s.dir, name)); err != nil {
				return err
			}
			removedTemp = true
			continue
		}
		if strings.HasPrefix(name, ".payload-") {
			associated := false
			for _, stage := range s.data.Stages {
				if stage.Ticket != "" && stage.LocalPayload != nil && strings.HasPrefix(name, ".payload-"+stage.Ticket+"-") {
					associated = true
					break
				}
			}
			if !associated || entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
				return fmt.Errorf("fencing: unassociated source retry payload temporary")
			}
			if err := os.Remove(filepath.Join(s.dir, name)); err != nil {
				return err
			}
			removedTemp = true
			continue
		}
		if _, ok := known[name]; !ok || entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return fmt.Errorf("fencing: unaccounted source retry file")
		}
	}
	if removedTemp {
		if err := s.syncDir(s.dir); err != nil {
			return err
		}
	}
	for _, descriptor := range known {
		if _, err := s.readLocalPayload(descriptor); err != nil {
			recoverable := false
			for _, stage := range s.data.Stages {
				if stage.LocalPayload != nil && stage.LocalPayload.Ref == descriptor.Ref && !stage.RemoteStarted && stage.Payload == nil {
					recoverable = true
					break
				}
			}
			if errors.Is(err, os.ErrNotExist) && recoverable {
				continue
			}
			return err
		}
	}
	if len(s.data.PendingPayloadGC) != 0 {
		next := s.data
		next.PendingPayloadGC = nil
		if err := s.persist(next); err != nil {
			return err
		}
	}
	return nil
}

// sealedUpload retains a descriptor obtained from the authenticated authority
// sealing response. It is not physical sealing or verification of that receipt.
func (s *sourceRetryState) sealedUpload(ticket string, payload sourceSealedPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	next := s.data
	next.Stages = append([]sourceRetryStage(nil), s.data.Stages...)
	for i := range next.Stages {
		stage := &next.Stages[i]
		if stage.Ticket != ticket {
			continue
		}
		if stage.LocalPayload != nil && (stage.LocalPayload.Digest != payload.Digest || stage.LocalPayload.Size != payload.Size) {
			return fmt.Errorf("fencing: authority sealed payload differs from durable original bytes")
		}
		if stage.Payload != nil {
			if *stage.Payload != payload {
				return fmt.Errorf("fencing: original sealed payload changed")
			}
			return nil
		}
		stage.Payload = &payload
		return s.persist(next)
	}
	return fmt.Errorf("fencing: original staging ticket missing")
}

func cloneSourceStage(stage sourceRetryStage) sourceRetryStage {
	if stage.Payload != nil {
		copy := *stage.Payload
		stage.Payload = &copy
	}
	return stage
}
