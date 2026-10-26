package fencing

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func payloadDigest(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func newStageFilesTest(t *testing.T) *stagePayloadFiles {
	t.Helper()
	s, err := openStagePayloadFiles(t.TempDir())
	mustJournal(t, err)
	return s
}

func TestStagePayloadPhysicalSealingPrecedesSourceAllocation(t *testing.T) {
	source, binding, dir := newSourceRetryTest(t)
	files := newStageFilesTest(t) // Separate from source WAL/retry state.
	intent := sourceRetryIntent("bootstrap-registration")
	ticket, err := source.reserveUpload(uploadStageDraft(intent))
	mustJournal(t, err)
	data := []byte("original registration")
	payload, err := files.seal(ticket.Ticket, payloadDigest(data), int64(len(data)), bytes.NewReader(data))
	mustJournal(t, err)
	if len(source.data.Steps) != 0 || source.data.Stages[0].Payload != nil {
		t.Fatal("physical sealing allocated or admitted a source mutation")
	}
	// Lost sealing response: reopen original source metadata and recover exact
	// physical bytes, not a mocked seal descriptor or reconstructed sequence.
	source = reopenSourceRetryTest(t, source, binding, dir)
	files, err = openStagePayloadFiles(files.dir)
	mustJournal(t, err)
	recovered, err := files.recover(ticket.Ticket, payload.Digest, payload.Size)
	mustJournal(t, err)
	if recovered != payload {
		t.Fatal("sealing response loss changed original descriptor")
	}
	mustJournal(t, source.sealedUpload(ticket.Ticket, recovered))
	intent.PayloadRef, intent.PayloadDigest, intent.PayloadSize = payload.Ref, payload.Digest, payload.Size
	step, err := source.allocate(intent)
	mustJournal(t, err)
	if step.Sequence != 1 || step.Entry.Intent.PayloadDigest != payloadDigest(data) {
		t.Fatal("original source tuple did not retain actual sealed bytes")
	}
	// This is not durable authority admission or a provider acknowledgement.
}

func TestStagePayloadRejectsInvalidBytesWithoutFinalIdentity(t *testing.T) {
	for name, tc := range map[string]struct {
		data   string
		digest string
		size   int64
	}{
		"short":          {"x", payloadDigest([]byte("xx")), 2},
		"long":           {"xx", payloadDigest([]byte("x")), 1},
		"hash":           {"x", payloadDigest([]byte("y")), 1},
		"negative":       {"", payloadDigest(nil), -1},
		"oversize":       {"", payloadDigest(nil), maxStagePayloadBytes + 1},
		"invalid digest": {"", "bad", 0},
	} {
		t.Run(name, func(t *testing.T) {
			s := newStageFilesTest(t)
			got, err := s.seal(journalTestDigest, tc.digest, tc.size, strings.NewReader(tc.data))
			if err == nil || got != (sourceSealedPayload{}) {
				t.Fatal("invalid payload returned a sealed descriptor")
			}
			entries, err := os.ReadDir(s.dir)
			mustJournal(t, err)
			if len(entries) != 0 {
				t.Fatal("rejected receive left final or temporary bytes")
			}
		})
	}
}

func TestStagePayloadSyncFailureCannotAcknowledgeOrOverwrite(t *testing.T) {
	for _, barrier := range []string{"file", "directory"} {
		t.Run(barrier, func(t *testing.T) {
			s := newStageFilesTest(t)
			fault := errors.New("injected " + barrier + " sync failure")
			if barrier == "file" {
				s.syncFile = func(*os.File) error { return fault }
			} else {
				s.syncDir = func(string) error { return fault }
			}
			data := []byte("first")
			got, err := s.seal(journalTestDigest, payloadDigest(data), int64(len(data)), bytes.NewReader(data))
			if !errors.Is(err, fault) || got != (sourceSealedPayload{}) {
				t.Fatal("uncertain durability returned a descriptor")
			}
			reopened, err := openStagePayloadFiles(s.dir)
			mustJournal(t, err)
			if barrier == "file" {
				if _, err := reopened.recover(journalTestDigest, payloadDigest(data), int64(len(data))); err == nil {
					t.Fatal("unfinalized bytes recovered as sealed")
				}
				return
			}
			original, err := reopened.recover(journalTestDigest, payloadDigest(data), int64(len(data)))
			mustJournal(t, err)
			if _, err := reopened.seal(journalTestDigest, payloadDigest([]byte("other")), 5, strings.NewReader("other")); err == nil {
				t.Fatal("retry overwrote immutable orphan after uncertain directory sync")
			}
			actual, err := os.ReadFile(filepath.Join(s.dir, original.Ref))
			mustJournal(t, err)
			if !bytes.Equal(actual, data) {
				t.Fatal("original finalized bytes changed")
			}
		})
	}
}

func TestStagePayloadConcurrentFinalizersCannotReplaceWinner(t *testing.T) {
	s := newStageFilesTest(t)
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, content := range []string{"first", "other"} {
		wg.Go(func() {
			_, err := s.seal(journalTestDigest, payloadDigest([]byte(content)), int64(len(content)), strings.NewReader(content))
			results <- err
		})
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("different payloads both sealed under one identity: successes=%d", successes)
	}
	entries, err := os.ReadDir(s.dir)
	mustJournal(t, err)
	if len(entries) != 1 || entries[0].Name() != "payload-"+journalTestDigest {
		t.Fatal("concurrent finalization left temporary data or multiple identities")
	}
}

func TestStagePayloadMissingRecoveryDoesNotInitialize(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := openStagePayloadFiles(missing); err == nil {
		t.Fatal("lost staging directory silently recreated")
	}
	s := newStageFilesTest(t)
	if _, err := s.recover(journalTestDigest, payloadDigest(nil), 0); err == nil {
		t.Fatal("missing recorded payload silently initialized")
	}
	if _, err := os.Stat(filepath.Join(s.dir, "payload-"+journalTestDigest)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("lookup-only recovery created replacement bytes")
	}
}

func TestStagePayloadRejectsSymlinkAndUnsafeTicket(t *testing.T) {
	s := newStageFilesTest(t)
	external := filepath.Join(t.TempDir(), "external")
	mustJournal(t, os.WriteFile(external, []byte("original"), 0600))
	mustJournal(t, os.Symlink(external, filepath.Join(s.dir, "payload-"+journalTestDigest)))
	if _, err := s.seal(journalTestDigest, payloadDigest([]byte("original")), 8, strings.NewReader("original")); err == nil {
		t.Fatal("symlink accepted as immutable payload")
	}
	for _, ticket := range []string{"", "../external", strings.Repeat("x", 64)} {
		if _, err := s.seal(ticket, payloadDigest(nil), 0, strings.NewReader("")); err == nil {
			t.Fatal("unsafe ticket accepted")
		}
	}
	data, err := os.ReadFile(external)
	mustJournal(t, err)
	if string(data) != "original" {
		t.Fatal("external target changed")
	}
}

func TestStagePayloadEmptyAndIdempotentRecovery(t *testing.T) {
	s := newStageFilesTest(t)
	payload, err := s.seal(journalTestDigest, payloadDigest(nil), 0, strings.NewReader(""))
	mustJournal(t, err)
	again, err := s.seal(journalTestDigest, payload.Digest, 0, strings.NewReader("not replacement bytes"))
	mustJournal(t, err)
	if again != payload {
		t.Fatal("same reserved ticket did not return original immutable bytes")
	}
}

type stageZeroReader struct{}

func (stageZeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func TestStagePayloadSupportsExisting64MiBCheckpointBlock(t *testing.T) {
	s := newStageFilesTest(t)
	hash := sha256.New()
	_, err := io.Copy(hash, io.LimitReader(stageZeroReader{}, maxStagePayloadBytes))
	mustJournal(t, err)
	digest := hex.EncodeToString(hash.Sum(nil))
	payload, err := s.seal(journalTestDigest, digest, maxStagePayloadBytes, io.LimitReader(stageZeroReader{}, maxStagePayloadBytes))
	mustJournal(t, err)
	_, err = s.recover(journalTestDigest, payload.Digest, payload.Size)
	mustJournal(t, err)
}
