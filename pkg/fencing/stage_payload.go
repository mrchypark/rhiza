package fencing

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// This is the physical sealing boundary only, not a staging authority. Before
// using it the authority must durably reserve the ticket and its byte budget,
// bind producer/owner, and hold exclusive spool ownership. No production caller
// may use it until that inventory and the reviewed deployment contract exist.
// There is deliberately no create-directory, takeover or deletion fallback.
const maxStagePayloadBytes int64 = 64 << 20

type stagePayloadFiles struct {
	dir      string
	syncFile func(*os.File) error
	syncDir  func(string) error
}

func openStagePayloadFiles(dir string) (*stagePayloadFiles, error) {
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, fmt.Errorf("fencing: original staging directory missing: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("fencing: invalid staging directory")
	}
	return &stagePayloadFiles{dir: dir, syncFile: (*os.File).Sync, syncDir: syncSourceRetryDir}, nil
}

// seal receives only a durably reserved, still-receiving ticket. Previously
// sealed tickets use recover, never this receive path, even after file loss.
// It returns a descriptor only after bytes and the immutable final name are
// durable. An uncertain finalization leaves a discoverable final file; it never
// overwrites it or silently initializes a replacement. Descriptor persistence
// and admission remain subsequent, separate authority transactions.
//
// The byte budget here bounds stored bytes only. It is not a liveness bound:
// seal imposes no deadline on input, and both the copy and the trailing EOF
// probe can block indefinitely on a reader whose Read blocks or never returns.
// The caller transport must bound that time itself, so the supplied reader is
// deadline- and cancellation-aware and its reads are actually unblocked when
// that bound expires. Cancelling a surrounding context does not by itself
// revoke a provider operation that has already been issued; that revocation
// remains a separate deployment contract under the pending Pro consultation.
func (s *stagePayloadFiles) seal(ticket, digest string, size int64, input io.Reader) (sourceSealedPayload, error) {
	if !mutationDigestValid(ticket) || !mutationDigestValid(digest) || size < 0 || size > maxStagePayloadBytes || input == nil {
		return sourceSealedPayload{}, fmt.Errorf("fencing: invalid or oversized staging payload")
	}
	payload := sourceSealedPayload{Ref: "payload-" + ticket, Digest: digest, Size: size}
	if _, err := os.Lstat(filepath.Join(s.dir, payload.Ref)); err == nil {
		return s.recover(ticket, digest, size)
	} else if !errors.Is(err, os.ErrNotExist) {
		return sourceSealedPayload{}, err
	}
	f, err := os.CreateTemp(s.dir, ".receiving-"+ticket+"-*")
	if err != nil {
		return sourceSealedPayload{}, err
	}
	defer os.Remove(f.Name())
	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, hash), io.LimitReader(input, size))
	if copyErr == nil && n == size {
		// Probe without storing an extra byte beyond the reserved byte budget.
		var extra [1]byte
		if n, err := io.ReadFull(input, extra[:]); n != 0 || err != io.EOF {
			copyErr = errors.Join(err, fmt.Errorf("fencing: staging payload exceeds reserved length"))
		}
	}
	if copyErr != nil || n != size || hex.EncodeToString(hash.Sum(nil)) != digest {
		_ = f.Close()
		return sourceSealedPayload{}, errors.Join(copyErr, fmt.Errorf("fencing: staging payload length or digest differs"))
	}
	if err := f.Chmod(0400); err != nil {
		_ = f.Close()
		return sourceSealedPayload{}, err
	}
	if err := s.syncFile(f); err != nil {
		_ = f.Close()
		return sourceSealedPayload{}, err
	}
	if err := f.Close(); err != nil {
		return sourceSealedPayload{}, err
	}
	// Link, unlike Rename, cannot replace a finalized ticket. Concurrent attempts
	// must verify the winner, including its exact content, before any descriptor.
	if err := os.Link(f.Name(), filepath.Join(s.dir, payload.Ref)); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return sourceSealedPayload{}, err
		}
		return s.recover(ticket, digest, size)
	}
	if err := s.syncDir(s.dir); err != nil {
		return sourceSealedPayload{}, err
	}
	return payload, nil
}

// recover is lookup-only: a previously sealed payload lost from disk cannot be
// made valid again by silently receiving replacement bytes under its identity.
func (s *stagePayloadFiles) recover(ticket, digest string, size int64) (sourceSealedPayload, error) {
	if !mutationDigestValid(ticket) || !mutationDigestValid(digest) || size < 0 || size > maxStagePayloadBytes {
		return sourceSealedPayload{}, fmt.Errorf("fencing: invalid sealed payload descriptor")
	}
	payload := sourceSealedPayload{Ref: "payload-" + ticket, Digest: digest, Size: size}
	if err := s.verify(payload); err != nil {
		return sourceSealedPayload{}, err
	}
	return payload, nil
}

func (s *stagePayloadFiles) verify(payload sourceSealedPayload) error {
	path := filepath.Join(s.dir, payload.Ref)
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != payload.Size {
		return fmt.Errorf("fencing: original sealed payload missing or replaced")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	hash := sha256.New()
	n, readErr := io.Copy(hash, io.LimitReader(f, payload.Size+1))
	if readErr != nil || n != payload.Size || hex.EncodeToString(hash.Sum(nil)) != payload.Digest {
		_ = f.Close()
		return errors.Join(readErr, fmt.Errorf("fencing: original sealed payload digest differs"))
	}
	// Recovery may observe a link whose directory sync failed. Re-establish both
	// durability barriers before returning it as a sealed descriptor.
	if err := errors.Join(s.syncFile(f), f.Close()); err != nil {
		return err
	}
	return s.syncDir(s.dir)
}
