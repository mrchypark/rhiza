package fencing

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSourceUploadTicketRetainedBeforeSequenceAndResponseLoss(t *testing.T) {
	s, binding, dir := newSourceRetryTest(t)
	intent := sourceRetryIntent("block")
	draft := uploadStageDraft(intent)
	stage, err := s.reserveUpload(draft)
	mustJournal(t, err)
	data, err := os.ReadFile(filepath.Join(dir, "retry.json"))
	mustJournal(t, err)
	var persisted sourceRetryFile
	mustJournal(t, json.Unmarshal(data, &persisted))
	if len(persisted.Data.Steps) != 0 || len(persisted.Data.Stages) != 1 || persisted.Data.Stages[0].Ticket != stage.Ticket || persisted.Data.Stages[0].Payload != nil {
		t.Fatal("ticket was not durably retained before streaming or consumed a mutation sequence")
	}
	if _, err := s.allocate(intent); err == nil {
		t.Fatal("receiving ticket became an admissible mutation")
	}
	after, err := os.ReadFile(filepath.Join(dir, "retry.json"))
	mustJournal(t, err)
	if !bytes.Equal(data, after) {
		t.Fatal("failed pre-seal allocation changed retained ticket")
	}
	// A lost remote reservation response resumes this same local ticket.
	s = reopenSourceRetryTest(t, s, binding, dir)
	retry, err := s.reserveUpload(draft)
	mustJournal(t, err)
	if retry.Ticket != stage.Ticket || retry.Draft != stage.Draft {
		t.Fatal("staging retry created a different payload identity or target")
	}
	draft.Key = "replacement-key"
	if _, err := s.reserveUpload(draft); err == nil {
		t.Fatal("same owner changed target before sequence assignment")
	}
	mustJournal(t, s.sealedUpload(stage.Ticket, sourceSealedPayload{Ref: intent.PayloadRef, Digest: intent.PayloadDigest, Size: intent.PayloadSize}))
	step, err := s.allocate(intent)
	mustJournal(t, err)
	if step.Sequence != 1 {
		t.Fatal("staging reservation consumed or skipped a sequence")
	}
}

func TestSourceUploadSealingUncertaintyKeepsOriginalDescriptor(t *testing.T) {
	s, binding, dir := newSourceRetryTest(t)
	intent := sourceRetryIntent("block")
	stage, err := s.reserveUpload(uploadStageDraft(intent))
	mustJournal(t, err)
	payload := sourceSealedPayload{Ref: intent.PayloadRef, Digest: intent.PayloadDigest, Size: intent.PayloadSize}
	fault := errors.New("injected sealing descriptor directory sync")
	s.syncDir = func(string) error { return fault }
	if err := s.sealedUpload(stage.Ticket, payload); !errors.Is(err, fault) {
		t.Fatal("uncertain seal retention returned success")
	}
	if _, err := s.allocate(intent); !errors.Is(err, fault) {
		t.Fatal("source allocated before recovering uncertain seal retention")
	}
	s = reopenSourceRetryTest(t, s, binding, dir)
	retry, err := s.reserveUpload(uploadStageDraft(intent))
	mustJournal(t, err)
	if retry.Ticket != stage.Ticket || retry.Payload == nil || *retry.Payload != payload {
		t.Fatal("recovery lost original seal descriptor")
	}
	for _, changed := range []sourceSealedPayload{
		{Ref: "replacement", Digest: payload.Digest, Size: payload.Size},
		{Ref: payload.Ref, Digest: strings.Repeat("b", 64), Size: payload.Size},
		{Ref: payload.Ref, Digest: payload.Digest, Size: payload.Size + 1},
	} {
		if err := s.sealedUpload(stage.Ticket, changed); err == nil {
			t.Fatal("retry refreshed sealed payload identity, digest or length")
		}
	}
	retry.Payload.Ref = "caller-mutated-copy"
	mustJournal(t, s.sealedUpload(stage.Ticket, payload))
	step, err := s.allocate(intent)
	mustJournal(t, err)
	if step.Entry.Intent.PayloadRef != payload.Ref {
		t.Fatal("caller mutated retained sealed descriptor")
	}
}

func TestSourceUploadTicketPressureDoesNotPreventSealingAccountedWork(t *testing.T) {
	s, _, _ := newSourceRetryTest(t)
	intent := sourceRetryIntent("block")
	var first sourceRetryStage
	for i := range MaxMutationJournalEntries {
		draft := uploadStageDraft(intent)
		draft.OwnerStep.Attempt = uint64(i) + 1
		stage, err := s.reserveUpload(draft)
		mustJournal(t, err)
		if i == 0 {
			first = stage
		}
	}
	if _, err := s.reserveUpload(uploadStageDraft(sourceRetryIntent("unaccounted"))); err == nil {
		t.Fatal("unbounded receiving tickets accepted")
	}
	mustJournal(t, s.sealedUpload(first.Ticket, sourceSealedPayload{Ref: intent.PayloadRef, Digest: intent.PayloadDigest, Size: intent.PayloadSize}))
	step, err := s.allocate(intent)
	mustJournal(t, err)
	if step.Sequence != 1 {
		t.Fatal("ticket quota prevented finalization of already-accounted work")
	}
}
