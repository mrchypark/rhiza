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
	"os"
	"strings"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
)

type remoteSourceStageRecord struct {
	Version   int                    `json:"version"`
	Producer  string                 `json:"producer"`
	Ticket    string                 `json:"ticket"`
	Draft     fencing.MutationIntent `json:"draft"`
	Receiving *sourceStageDescriptor `json:"receiving,omitempty"`
	Payload   *sourceStageDescriptor `json:"payload,omitempty"`
	Cancelled bool                   `json:"cancelled,omitempty"`
	Retired   bool                   `json:"retired,omitempty"`
	Digest    string                 `json:"digest"`
}

const maxRemoteSourceStageTombstones = 8192

type remoteSourceStageQuota struct {
	Version  int              `json:"version"`
	Producer string           `json:"producer"`
	Tickets  map[string]int64 `json:"tickets"`
	Digest   string           `json:"digest"`
}

func (q remoteSourceStageQuota) digest() string {
	q.Digest = ""
	return hashJSON(q)
}

// reserveRemoteStageQuota holds capacity until an independently verified
// archive-retirement path exists. Cancellation alone is not archival proof.
func (s *SourceStageStore) reserveRemoteStageQuota(ctx context.Context, producerUID, ticket string, size *int64) error {
	key := sourceStageQuotaKey(producerUID)
	for range 32 {
		var quota remoteSourceStageQuota
		r, generation, err := s.evidence.Read(ctx, key)
		if err != nil {
			if !s.evidence.IsNotFound(err) {
				return err
			}
			quota = remoteSourceStageQuota{Version: 1, Producer: producerUID, Tickets: map[string]int64{ticket: -1}}
			if size != nil {
				quota.Tickets[ticket] = *size
			}
			if quotaExceedsLimit(quota.Tickets) {
				return fmt.Errorf("fencing: remote source staging quota exceeded")
			}
			quota.Digest = quota.digest()
			data, marshalErr := json.Marshal(quota)
			if marshalErr != nil {
				return marshalErr
			}
			if _, err := s.evidence.CreateImmutable(ctx, key, data); err == nil || errors.Is(err, errAuthorityStateConflict) {
				return nil
			}
			// A conflicting create means another ticket won initialization; retry
			// against its generation so concurrent reservations remain bounded.
			continue
		}
		data, readErr := io.ReadAll(io.LimitReader(r, 1<<20+1))
		closeErr := r.Close()
		if readErr != nil || closeErr != nil || len(data) > 1<<20 {
			return errors.Join(readErr, closeErr, fmt.Errorf("fencing: remote source staging quota is unreadable"))
		}
		d := json.NewDecoder(bytes.NewReader(data))
		d.DisallowUnknownFields()
		if err := d.Decode(&quota); err != nil || d.Decode(new(any)) != io.EOF || quota.Version != 1 || quota.Producer != producerUID || quota.Tickets == nil || len(quota.Tickets) > maxRemoteSourceStageTombstones || quota.Digest != quota.digest() || quotaExceedsLimit(quota.Tickets) {
			return fmt.Errorf("fencing: remote source staging quota is corrupt")
		}
		prior, exists := quota.Tickets[ticket]
		if exists {
			if prior == -2 {
				return fmt.Errorf("fencing: archived source stage ticket cannot be reopened")
			}
			if size == nil || prior == *size {
				return nil
			}
			if prior != -1 {
				return fmt.Errorf("fencing: remote source stage retry changed reserved size")
			}
			quota.Tickets[ticket] = *size
		} else {
			if activeSourceStageTickets(quota.Tickets) >= maxSourceStageTickets {
				return fmt.Errorf("fencing: remote source staging ticket quota exceeded")
			}
			if len(quota.Tickets) >= maxRemoteSourceStageTombstones {
				return fmt.Errorf("fencing: remote source staging tombstone capacity exhausted")
			}
			reservedSize := int64(-1)
			if size != nil {
				reservedSize = *size
			}
			quota.Tickets[ticket] = reservedSize
		}
		if quotaExceedsLimit(quota.Tickets) {
			return fmt.Errorf("fencing: remote source staging byte quota exceeded")
		}
		quota.Digest = quota.digest()
		body, err := json.Marshal(quota)
		if err != nil {
			return err
		}
		if _, err := s.evidence.CompareAndSwap(ctx, key, generation, body); err == nil {
			return nil
		}
	}
	return fmt.Errorf("fencing: remote source staging quota CAS remained uncertain")
}

func quotaExceedsLimit(tickets map[string]int64) bool {
	if activeSourceStageTickets(tickets) > maxSourceStageTickets {
		return true
	}
	var total int64
	for ticket, size := range tickets {
		if !validStageTicket(ticket) {
			return true
		}
		if size < -2 || size > maxSourceStageBytes {
			return true
		}
		if size > 0 {
			total += size
		}
	}
	return total > maxSourceStageTotal
}

func activeSourceStageTickets(tickets map[string]int64) int {
	active := 0
	for _, size := range tickets {
		if size != -2 {
			active++
		}
	}
	return active
}

func (s *SourceStageStore) retireRemoteStageQuota(ctx context.Context, producerUID, ticket string, size int64) error {
	key := sourceStageQuotaKey(producerUID)
	for range 32 {
		var quota remoteSourceStageQuota
		r, generation, err := s.evidence.Read(ctx, key)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(io.LimitReader(r, 1<<20+1))
		closeErr := r.Close()
		if readErr != nil || closeErr != nil || len(data) > 1<<20 {
			return errors.Join(readErr, closeErr, fmt.Errorf("fencing: remote source staging quota is unreadable"))
		}
		d := json.NewDecoder(bytes.NewReader(data))
		d.DisallowUnknownFields()
		if err := d.Decode(&quota); err != nil || d.Decode(new(any)) != io.EOF || quota.Version != 1 || quota.Producer != producerUID || quota.Tickets == nil || len(quota.Tickets) > maxRemoteSourceStageTombstones || quota.Digest != quota.digest() || quotaExceedsLimit(quota.Tickets) {
			return fmt.Errorf("fencing: remote source staging quota is corrupt")
		}
		reserved, ok := quota.Tickets[ticket]
		if !ok {
			return fmt.Errorf("fencing: remote source stage quota reservation is missing")
		}
		if reserved == -2 {
			return nil
		}
		if reserved != size {
			return fmt.Errorf("fencing: remote source stage quota reservation differs")
		}
		quota.Tickets[ticket] = -2
		quota.Digest = quota.digest()
		body, err := json.Marshal(quota)
		if err != nil {
			return err
		}
		if _, err := s.evidence.CompareAndSwap(ctx, key, generation, body); err == nil {
			return nil
		}
	}
	return fmt.Errorf("fencing: remote source staging retirement CAS remained uncertain")
}

// releaseCancelledRemoteStageQuota frees capacity only after the stage record
// is durably cancelled with no receive in progress. The -2 value is a retained
// tombstone, so neither retries nor delayed seal requests can reopen the slot.
func (s *SourceStageStore) releaseCancelledRemoteStageQuota(ctx context.Context, producerUID, ticket string) error {
	key := sourceStageQuotaKey(producerUID)
	for range 32 {
		var quota remoteSourceStageQuota
		r, generation, err := s.evidence.Read(ctx, key)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(io.LimitReader(r, 1<<20+1))
		closeErr := r.Close()
		if readErr != nil || closeErr != nil || len(data) > 1<<20 {
			return errors.Join(readErr, closeErr, fmt.Errorf("fencing: remote source staging quota is unreadable"))
		}
		d := json.NewDecoder(bytes.NewReader(data))
		d.DisallowUnknownFields()
		if err := d.Decode(&quota); err != nil || d.Decode(new(any)) != io.EOF || quota.Version != 1 || quota.Producer != producerUID || quota.Tickets == nil || len(quota.Tickets) > maxRemoteSourceStageTombstones || quota.Digest != quota.digest() || quotaExceedsLimit(quota.Tickets) {
			return fmt.Errorf("fencing: remote source staging quota is corrupt")
		}
		reserved, ok := quota.Tickets[ticket]
		if !ok {
			return fmt.Errorf("fencing: cancelled source stage quota reservation is missing")
		}
		if reserved == -2 {
			return nil
		}
		quota.Tickets[ticket] = -2
		quota.Digest = quota.digest()
		body, err := json.Marshal(quota)
		if err != nil {
			return err
		}
		if _, err := s.evidence.CompareAndSwap(ctx, key, generation, body); err == nil {
			return nil
		}
	}
	return fmt.Errorf("fencing: cancelled source stage quota CAS remained uncertain")
}

// retireArchivedSourceStage reclaims remote stage capacity only after the
// exact mutation is verified in committed archive history and its receipts.
func (e *FencingExecutor) retireArchivedSourceStage(ctx context.Context, runtime fencing.RuntimeBinding, producerUID string, sequence uint64, intent fencing.MutationIntent, archiveRef fencing.MutationArchiveReference) error {
	if ctx == nil || e == nil || e.AuthorityState == nil || !archiveRef.Valid() || intent.Kind != "upload" || !strings.HasPrefix(intent.PayloadRef, "payload-") {
		return fmt.Errorf("fencing: archived source-stage retirement proof is incomplete")
	}
	producer, storedUID, _, err := e.AuthorityState.ReadProducer(ctx, runtime)
	if err != nil {
		return err
	}
	if storedUID != producerUID || producer.CompactedThrough < sequence || producer.ArchiveHead == nil {
		return fmt.Errorf("fencing: source stage has no committed archive coverage")
	}
	entry, err := e.lookupArchivedMutation(ctx, runtime, producerUID, sequence, intent)
	if err != nil {
		return err
	}
	if entry.Intent != intent {
		return fmt.Errorf("fencing: archived source-stage intent differs")
	}
	verificationID := hashJSON([]any{"lookup-archived-mutation", producerUID, sequence, intent})
	producer, storedUID, _, err = e.AuthorityState.ReadProducer(ctx, runtime)
	if err != nil {
		return err
	}
	if storedUID != producerUID || producer.ArchiveScan == nil || producer.ArchiveScan.VerificationID != verificationID || producer.ArchiveScan.Sequence != sequence || producer.ArchiveScan.ResultArchive == nil || *producer.ArchiveScan.ResultArchive != archiveRef {
		return ErrFencePending
	}
	stages, err := e.sourceStagesFor(ctx, runtime)
	if err != nil {
		return err
	}
	if stages.evidence == nil {
		return nil
	}
	ticket := strings.TrimPrefix(intent.PayloadRef, "payload-")
	if !validStageTicket(ticket) {
		return fmt.Errorf("fencing: archived source-stage ticket is invalid")
	}
	draft := intent
	draft.OperationID, draft.PayloadRef, draft.PayloadDigest, draft.PayloadSize = "", "", "", 0
	want := sourceStageDescriptor{Ref: intent.PayloadRef, Digest: intent.PayloadDigest, Size: intent.PayloadSize}
	record, generation, err := stages.readRemoteStage(ctx, producerUID, ticket)
	if err != nil {
		return err
	}
	if record.Draft != draft || record.Payload == nil || *record.Payload != want || record.Cancelled {
		return fmt.Errorf("fencing: archived source stage differs from its reservation")
	}
	if !record.Retired {
		payloadKey := sourceStagePayloadKey(producerUID, ticket)
		payload, payloadVersion, err := stages.evidence.Read(ctx, payloadKey)
		if err != nil && !stages.evidence.IsNotFound(err) {
			return err
		}
		if err == nil {
			verified, verifyErr := verifiedStageTemp(payload, want.Digest, want.Size)
			closeErr := payload.Close()
			if verifyErr != nil || closeErr != nil || payloadVersion == nil || payloadVersion.Type != objstore.Generation {
				if verified != nil {
					_ = verified.Close()
					_ = os.Remove(verified.Name())
				}
				return errors.Join(verifyErr, closeErr, fmt.Errorf("fencing: archived source payload failed exact-version verification"))
			}
			_ = verified.Close()
			_ = os.Remove(verified.Name())
			if err := stages.evidence.DeleteVersion(ctx, payloadKey, *payloadVersion); err != nil {
				return err
			}
		}
		record.Retired = true
		if _, err := stages.writeRemoteStage(ctx, producerUID, ticket, generation, record); err != nil {
			return err
		}
	}
	return stages.retireRemoteStageQuota(ctx, producerUID, ticket, want.Size)
}

func (r remoteSourceStageRecord) digest() string {
	r.Digest = ""
	return hashJSON(r)
}

func (s *SourceStageStore) ReserveContext(ctx context.Context, producerUID, ticket string, draft fencing.MutationIntent) error {
	if ctx == nil || s == nil || s.evidence == nil || producerUID == "" || !validStageTicket(ticket) || !validStageDraft(draft) {
		return fmt.Errorf("fencing: invalid remote source staging reservation")
	}
	if err := s.reserveRemoteStageQuota(ctx, producerUID, ticket, nil); err != nil {
		return err
	}
	want := remoteSourceStageRecord{Version: 1, Producer: producerUID, Ticket: ticket, Draft: draft}
	want.Digest = want.digest()
	data, err := json.Marshal(want)
	if err != nil {
		return err
	}
	_, createErr := s.evidence.CreateImmutable(ctx, sourceStageRecordKey(producerUID, ticket), data)
	if createErr == nil {
		return nil
	}
	got, _, readErr := s.readRemoteStage(ctx, producerUID, ticket)
	if readErr == nil && got.Producer == producerUID && got.Ticket == ticket && got.Draft == draft && !got.Cancelled && !got.Retired {
		return nil
	}
	return errors.Join(createErr, readErr, fmt.Errorf("fencing: source staging ticket identity changed"))
}

func (s *SourceStageStore) SealContext(ctx context.Context, producerUID, ticket, digest string, size int64, input io.Reader) (sourceStageDescriptor, error) {
	if ctx == nil || s == nil || s.evidence == nil || producerUID == "" || !validStageTicket(ticket) || !validStageDigest(digest) || size < 0 || size > maxSourceStageBytes || input == nil {
		return sourceStageDescriptor{}, fmt.Errorf("fencing: invalid remote source stage")
	}
	// This temporary file is only a bounded upload buffer. It is not recovery
	// state: an interrupted upload is retried from the source and never admitted.
	inputFile, err := verifiedStageTemp(input, digest, size)
	if err != nil {
		return sourceStageDescriptor{}, err
	}
	defer func() { _ = inputFile.Close(); _ = os.Remove(inputFile.Name()) }()
	if _, err := inputFile.Seek(0, io.SeekStart); err != nil {
		return sourceStageDescriptor{}, err
	}

	current, generation, err := s.readRemoteStage(ctx, producerUID, ticket)
	if err != nil {
		return sourceStageDescriptor{}, err
	}
	want := sourceStageDescriptor{Ref: "payload-" + ticket, Digest: digest, Size: size}
	draft := current.Draft
	if current.Cancelled || current.Retired {
		return sourceStageDescriptor{}, fmt.Errorf("fencing: source stage was cancelled or retired")
	}
	if current.Receiving != nil && *current.Receiving != want {
		return sourceStageDescriptor{}, fmt.Errorf("fencing: source stage retry changed original digest or length")
	}
	if current.Payload != nil {
		if *current.Payload != want {
			return sourceStageDescriptor{}, fmt.Errorf("fencing: original source stage descriptor changed")
		}
		if err := s.verifyRemotePayload(ctx, producerUID, ticket, want); err != nil {
			return sourceStageDescriptor{}, err
		}
		return want, nil
	}
	if err := s.reserveRemoteStageQuota(ctx, producerUID, ticket, &size); err != nil {
		return sourceStageDescriptor{}, err
	}
	if current.Receiving == nil {
		next := current
		next.Receiving = &want
		generation, err = s.writeRemoteStage(ctx, producerUID, ticket, generation, next)
		if err != nil {
			return sourceStageDescriptor{}, err
		}
	}

	key := sourceStagePayloadKey(producerUID, ticket)
	if err := s.evidence.CreateImmutableFrom(ctx, key, inputFile, size); err != nil {
		// A timeout can occur after GCS committed. Accept only the original bytes
		// at this deterministic key; never replace a conflicting payload.
		if verifyErr := s.verifyRemotePayload(ctx, producerUID, ticket, want); verifyErr != nil {
			return sourceStageDescriptor{}, errors.Join(err, verifyErr)
		}
	} else if err := s.verifyRemotePayload(ctx, producerUID, ticket, want); err != nil {
		return sourceStageDescriptor{}, err
	}

	for range 8 {
		current, generation, err = s.readRemoteStage(ctx, producerUID, ticket)
		if err != nil {
			return sourceStageDescriptor{}, err
		}
		if current.Cancelled || current.Retired {
			return sourceStageDescriptor{}, fmt.Errorf("fencing: source stage was cancelled or retired")
		}
		if current.Draft != draft || current.Receiving == nil || *current.Receiving != want {
			return sourceStageDescriptor{}, fmt.Errorf("fencing: source stage reservation changed")
		}
		if current.Payload != nil {
			if *current.Payload != want {
				return sourceStageDescriptor{}, fmt.Errorf("fencing: original source stage descriptor changed")
			}
			return want, nil
		}
		next := current
		next.Payload, next.Receiving = &want, nil
		if _, err := s.writeRemoteStage(ctx, producerUID, ticket, generation, next); err == nil {
			return want, nil
		}
	}
	return sourceStageDescriptor{}, fmt.Errorf("fencing: source stage metadata CAS remained uncertain")
}

func (s *SourceStageStore) CancelContext(ctx context.Context, producerUID, ticket string) error {
	if ctx == nil || s == nil || s.evidence == nil || producerUID == "" || !validStageTicket(ticket) {
		return fmt.Errorf("fencing: invalid remote source stage cancellation")
	}
	for range 8 {
		record, generation, err := s.readRemoteStage(ctx, producerUID, ticket)
		if err != nil {
			return err
		}
		if record.Retired {
			return nil
		}
		if record.Cancelled {
			if record.Receiving != nil || record.Payload != nil {
				return ErrFencePending
			}
			return s.releaseCancelledRemoteStageQuota(ctx, producerUID, ticket)
		}
		if record.Payload != nil {
			return fmt.Errorf("fencing: sealed source stage cannot be cancelled")
		}
		if record.Receiving != nil {
			// The upload may already be in flight or may have committed with an
			// uncertain response. Keep quota reserved until sealing reaches a
			// terminal state; clearing Receiving here could orphan late bytes.
			return ErrFencePending
		}
		next := record
		next.Cancelled = true
		if _, err := s.writeRemoteStage(ctx, producerUID, ticket, generation, next); err == nil {
			return s.releaseCancelledRemoteStageQuota(ctx, producerUID, ticket)
		}
	}
	return fmt.Errorf("fencing: source stage cancellation CAS remained uncertain")
}

func (s *SourceStageStore) readRemoteStage(ctx context.Context, producerUID, ticket string) (remoteSourceStageRecord, *objstore.ObjectVersion, error) {
	var record remoteSourceStageRecord
	r, generation, err := s.evidence.Read(ctx, sourceStageRecordKey(producerUID, ticket))
	if err != nil {
		return record, nil, err
	}
	data, err := io.ReadAll(io.LimitReader(r, 1<<20+1))
	closeErr := r.Close()
	if err != nil {
		return record, nil, errors.Join(err, closeErr)
	}
	if closeErr != nil {
		return record, nil, closeErr
	}
	if len(data) > 1<<20 {
		return record, nil, fmt.Errorf("fencing: remote source stage record exceeds capacity")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&record); err != nil {
		return record, nil, err
	}
	if d.Decode(new(any)) != io.EOF || record.Version != 1 || record.Producer != producerUID || record.Ticket != ticket || !validStageDraft(record.Draft) || record.Digest != record.digest() {
		return record, nil, fmt.Errorf("fencing: remote source stage record is corrupt or stale")
	}
	if record.Payload != nil && (record.Receiving != nil || !validRemoteStageDescriptor(ticket, *record.Payload)) {
		return record, nil, fmt.Errorf("fencing: remote sealed stage record is invalid")
	}
	if record.Receiving != nil && !validRemoteStageDescriptor(ticket, *record.Receiving) {
		return record, nil, fmt.Errorf("fencing: remote receiving stage record is invalid")
	}
	if record.Cancelled && (record.Payload != nil || record.Receiving != nil) || record.Retired && (record.Payload == nil || record.Receiving != nil || record.Cancelled) {
		return record, nil, fmt.Errorf("fencing: cancelled remote stage retains payload state")
	}
	return record, generation, nil
}

func (s *SourceStageStore) writeRemoteStage(ctx context.Context, producerUID, ticket string, generation *objstore.ObjectVersion, record remoteSourceStageRecord) (*objstore.ObjectVersion, error) {
	record.Digest = record.digest()
	data, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	newGeneration, err := s.evidence.CompareAndSwap(ctx, sourceStageRecordKey(producerUID, ticket), generation, data)
	if err == nil {
		return newGeneration, nil
	}
	current, gotGeneration, readErr := s.readRemoteStage(ctx, producerUID, ticket)
	if readErr == nil && current.Digest == record.Digest && current.digest() == record.Digest {
		return gotGeneration, nil
	}
	return nil, errors.Join(err, readErr)
}

func (s *SourceStageStore) openRemoteMutationPayload(ctx context.Context, producerUID string, intent fencing.MutationIntent) (io.ReadCloser, error) {
	if ctx == nil || producerUID == "" || intent.Kind != "upload" || !strings.HasPrefix(intent.PayloadRef, "payload-") {
		return nil, fmt.Errorf("fencing: mutation has no sealed remote payload")
	}
	ticket := strings.TrimPrefix(intent.PayloadRef, "payload-")
	if !validStageTicket(ticket) {
		return nil, fmt.Errorf("fencing: invalid remote payload reference")
	}
	record, _, err := s.readRemoteStage(ctx, producerUID, ticket)
	if err != nil {
		return nil, err
	}
	draft := intent
	draft.OperationID, draft.PayloadRef, draft.PayloadDigest, draft.PayloadSize = "", "", "", 0
	want := sourceStageDescriptor{Ref: intent.PayloadRef, Digest: intent.PayloadDigest, Size: intent.PayloadSize}
	if record.Cancelled || record.Retired || record.Draft != draft || record.Payload == nil || *record.Payload != want {
		return nil, fmt.Errorf("fencing: remote stage differs from admitted mutation")
	}
	r, _, err := s.evidence.Read(ctx, sourceStagePayloadKey(producerUID, ticket))
	if err != nil {
		return nil, err
	}
	verified, err := verifiedStageTemp(r, want.Digest, want.Size)
	closeErr := r.Close()
	if err != nil {
		return nil, errors.Join(err, closeErr)
	}
	return &removeOnClose{File: verified}, closeErr
}

func (s *SourceStageStore) verifyRemotePayload(ctx context.Context, producerUID, ticket string, want sourceStageDescriptor) error {
	r, _, err := s.evidence.Read(ctx, sourceStagePayloadKey(producerUID, ticket))
	if err != nil {
		return err
	}
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(r, want.Size+1))
	closeErr := r.Close()
	if err != nil || n != want.Size || hex.EncodeToString(hash.Sum(nil)) != want.Digest {
		return errors.Join(err, closeErr, fmt.Errorf("fencing: remote source payload digest or size differs"))
	}
	var extra [1]byte
	// The bounded copy above reads at most size+1 bytes, so n > size is the
	// complete overlength check; the reader is already closed here.
	_ = extra
	if closeErr != nil {
		return closeErr
	}
	return nil
}

func verifiedStageTemp(input io.Reader, digest string, size int64) (*os.File, error) {
	f, err := os.CreateTemp("", "rhiza-stage-")
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*os.File, error) { _ = f.Close(); _ = os.Remove(f.Name()); return nil, err }
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, hash), io.LimitReader(input, size+1))
	if err != nil || n != size || hex.EncodeToString(hash.Sum(nil)) != digest {
		return fail(errors.Join(err, fmt.Errorf("fencing: staged payload length or digest differs")))
	}
	var extra [1]byte
	if n, err := io.ReadFull(input, extra[:]); n != 0 || err != io.EOF {
		return fail(errors.Join(err, fmt.Errorf("fencing: source stage exceeds reserved length")))
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}
	return f, nil
}

type removeOnClose struct{ *os.File }

func (f *removeOnClose) Close() error {
	if f == nil || f.File == nil {
		return nil
	}
	name := f.Name()
	return errors.Join(f.File.Close(), os.Remove(name))
}

func sourceStageRecordKey(producerUID, ticket string) string {
	return "stages/" + hashJSON(producerUID) + "/" + ticket + ".json"
}

func sourceStagePayloadKey(producerUID, ticket string) string {
	return "stages/" + hashJSON(producerUID) + "/" + ticket + "/payload"
}

func sourceStageQuotaKey(producerUID string) string {
	return "stages/" + hashJSON(producerUID) + "/quota.json"
}

func validRemoteStageDescriptor(ticket string, descriptor sourceStageDescriptor) bool {
	return descriptor.Ref == "payload-"+ticket && validStageDigest(descriptor.Digest) && descriptor.Size >= 0 && descriptor.Size <= maxSourceStageBytes
}
