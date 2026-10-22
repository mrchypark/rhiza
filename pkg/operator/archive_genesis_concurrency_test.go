package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
	"google.golang.org/api/option"
)

type pauseArchiveGenesisRootCASBucket struct {
	objstore.Bucket
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *pauseArchiveGenesisRootCASBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	if strings.Contains(name, "/rhiza-authority/v1/roots/") {
		b.once.Do(func() { close(b.started) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.Bucket.Upload(ctx, name, reader, options...)
}

func TestConcurrentArchiveGenesisCandidatesHaveOneCASWinnerAndTerminalLoser(t *testing.T) {
	var executor *FencingExecutor
	var runtime fencing.RuntimeBinding
	var uploads atomic.Int32
	payload := recoveryEmptyHeadBytes(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost || r.URL.Query().Get("ifGenerationMatch") != "0" || r.URL.Query().Get("name") != "root/source/archive/head.bin" {
			t.Errorf("unexpected genesis upload request: %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		scope, _, err := executor.archiveGenesisScope(r.Context(), runtime)
		if err != nil {
			t.Errorf("derive archive scope: %v", err)
			http.Error(w, "scope unavailable", http.StatusInternalServerError)
			return
		}
		claim, _, err := executor.AuthorityState.ReadArchiveScopeClaim(r.Context(), scope)
		if err != nil || claim.GenesisRef == nil || claim.GenesisRef.Mutation == nil {
			t.Errorf("provider dispatch preceded winning claim: %+v %v", claim, err)
			http.Error(w, "claim unavailable", http.StatusInternalServerError)
			return
		}
		root, err := executor.read(r.Context(), claim.OwnerStatefulSetUID, claim.OwnerSourceClusterID)
		if err != nil || root.Ledger.ArchiveGenesisRef == nil || !sameArchiveGenesisRef(root.Ledger.ArchiveGenesisRef, claim.GenesisRef) {
			t.Errorf("provider dispatch preceded matching root CAS: %+v %v", root.Ledger.ArchiveGenesisRef, err)
			http.Error(w, "root reference unavailable", http.StatusInternalServerError)
			return
		}
		producer, _, _, err := executor.AuthorityState.ReadProducer(r.Context(), runtime)
		entry, exists := producer.Entries[claim.GenesisRef.Mutation.Sequence]
		if err != nil || !exists || entry.Intent != claim.GenesisRef.Mutation.Intent {
			t.Errorf("provider dispatch lacks exact admitted tuple: entry=%+v exists=%v err=%v", entry, exists, err)
			http.Error(w, "admission unavailable", http.StatusInternalServerError)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Contains(body, payload) {
			t.Errorf("provider request omitted staged genesis payload: %v", err)
			http.Error(w, "payload mismatch", http.StatusBadRequest)
			return
		}
		uploads.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind": "storage#object", "bucket": "archive", "name": "root/source/archive/head.bin",
			"generation": "31", "metageneration": "1", "size": strconv.Itoa(len(payload)),
			"metadata": map[string]string{
				"rhiza-operation-id":   claim.GenesisRef.Mutation.Intent.OperationID,
				"rhiza-payload-sha256": claim.GenesisRef.Mutation.Intent.PayloadDigest,
			},
		})
	}))
	t.Cleanup(server.Close)
	f := newArchiveGenesisFixture(t)
	executor, runtime = f.e, f.runtime
	storageClient, err := storage.NewClient(t.Context(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storageClient.Close() })
	f.e.SourceMutationStorage = storageClient

	first := f.intent
	second := first
	second.OperationID = fencing.MutationRequestID(f.producer, 2)
	second.OwnerStep.Step = "genesis-candidate-two"
	second.PayloadRef, second.PayloadDigest, second.PayloadSize = "", "", 0
	secondTicket := strings.Repeat("8", 64)
	if err := f.stages.Reserve(f.producer, secondTicket, second); err != nil {
		t.Fatal(err)
	}
	sealed, err := f.stages.Seal(f.producer, secondTicket, hashBytes(payload), int64(len(payload)), bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	second.PayloadRef, second.PayloadDigest, second.PayloadSize = sealed.Ref, sealed.Digest, sealed.Size
	f.admit(t, 1, first)
	f.admit(t, 2, second)
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		return p.Close("closed-before-concurrent-genesis-claim", strings.Repeat("d", 64))
	}); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, candidate := range []struct {
		sequence uint64
		intent   fencing.MutationIntent
	}{{1, first}, {2, second}} {
		candidate := candidate
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- f.e.prepareArchiveGenesisMutation(t.Context(), f.runtime.StatefulSetUID, f.runtime, f.producer, candidate.sequence, candidate.intent)
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil && !errors.Is(err, fencing.ErrPending) && !errors.Is(err, errArchiveGenesisClaimed) {
			t.Fatalf("concurrent candidate preparation failed unexpectedly: %v", err)
		}
	}

	scope, _, err := f.e.archiveGenesisScope(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	claim, _, err := f.e.AuthorityState.ReadArchiveScopeClaim(t.Context(), scope)
	if err != nil || claim.GenesisRef == nil || claim.GenesisRef.Mutation == nil {
		t.Fatalf("concurrent CAS did not select one genesis tuple: %+v %v", claim, err)
	}
	if err := f.e.setRootArchiveGenesisRef(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID, *claim.GenesisRef); err != nil {
		t.Fatalf("could not repair the exact CAS winner's root reference: %v", err)
	}
	winner := claim.GenesisRef.Mutation
	loser := first
	loserSequence := uint64(1)
	if winner.Sequence == 1 {
		loser, loserSequence = second, 2
	} else if winner.Sequence != 2 {
		t.Fatalf("claim selected unexpected sequence %d", winner.Sequence)
	}
	if err := f.e.cancelArchiveGenesisLoser(t.Context(), f.runtime, f.producer, loserSequence, loser); err != nil {
		t.Fatalf("admitted losing tuple was not terminally cancelled after CAS winner was durable: %v", err)
	}
	producer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	losingEntry := producer.Entries[loserSequence]
	if losingEntry.Intent != loser || losingEntry.Preparation != nil || len(losingEntry.Dispatches) != 0 || losingEntry.Resolution == nil ||
		losingEntry.Resolution.Outcome != fencing.MutationRejected || !losingEntry.Resolution.AllCopiesTerminal {
		t.Fatalf("losing tuple was not excluded before preparation or dispatch: %+v", losingEntry)
	}
	evidence, err := f.e.authorityEvidenceFor(f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	proofReader, _, err := evidence.Read(t.Context(), "archive-genesis-cancellations/"+losingEntry.Resolution.ReceiptDigest)
	if err != nil {
		t.Fatalf("loser lacks exact durable cancellation proof: %v", err)
	}
	proofBytes, readErr := io.ReadAll(proofReader)
	closeErr := proofReader.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		t.Fatal(err)
	}
	var proof archiveGenesisCancellationProof
	if err := json.Unmarshal(proofBytes, &proof); err != nil || proof.Sequence != loserSequence || proof.Intent != loser ||
		proof.WinningGenesis.Mutation == nil || proof.WinningGenesis.Mutation.Sequence != winner.Sequence || proof.WinningGenesis.Mutation.Intent != winner.Intent {
		t.Fatalf("loser proof does not bind both exact candidates: %+v %v", proof, err)
	}

	status, code := postArchiveGenesisStatus(t, f.e, f.id)
	if code != http.StatusOK || status.Mutation == nil || status.Mutation.Entry == nil || status.Mutation.Entry.Resolution == nil ||
		status.Mutation.Entry.Resolution.Outcome != fencing.MutationCommitted {
		t.Fatalf("closed producer could not resume the exact CAS winner: status=%+v code=%d", status, code)
	}
	if uploads.Load() != 1 {
		t.Fatalf("concurrent genesis candidates caused %d provider effects, want one", uploads.Load())
	}
	producer, _, _, err = f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	winnerEntry := producer.Entries[winner.Sequence]
	if winnerEntry.Resolution == nil || winnerEntry.Resolution.Outcome != fencing.MutationCommitted || len(winnerEntry.Dispatches) != 1 {
		t.Fatalf("CAS winner did not settle exactly once: %+v", winnerEntry)
	}
}

func TestArchiveGenesisStatusCancelsLostLoserAfterClaimRootCASGapAndClosure(t *testing.T) {
	var executor *FencingExecutor
	var runtime fencing.RuntimeBinding
	var uploads atomic.Int32
	payload := recoveryEmptyHeadBytes(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost || r.URL.Query().Get("ifGenerationMatch") != "0" || r.URL.Query().Get("name") != "root/source/archive/head.bin" {
			t.Errorf("unexpected genesis upload request: %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		scope, _, err := executor.archiveGenesisScope(r.Context(), runtime)
		claim, _, claimErr := executor.AuthorityState.ReadArchiveScopeClaim(r.Context(), scope)
		root, rootErr := executor.read(r.Context(), runtime.StatefulSetUID, runtime.SourceClusterID)
		if err != nil || claimErr != nil || rootErr != nil || claim.GenesisRef == nil || root.Ledger.ArchiveGenesisRef == nil || !sameArchiveGenesisRef(claim.GenesisRef, root.Ledger.ArchiveGenesisRef) {
			t.Errorf("provider effect preceded matching claim/root references: claim=%+v root=%+v errors=%v/%v/%v", claim.GenesisRef, root.Ledger.ArchiveGenesisRef, err, claimErr, rootErr)
			http.Error(w, "reference unavailable", http.StatusInternalServerError)
			return
		}
		producer, _, _, err := executor.AuthorityState.ReadProducer(r.Context(), runtime)
		entry := producer.Entries[claim.GenesisRef.Mutation.Sequence]
		if err != nil || entry.Intent != claim.GenesisRef.Mutation.Intent {
			t.Errorf("provider effect lacked exact admitted winner: %+v %v", entry, err)
			http.Error(w, "winner unavailable", http.StatusInternalServerError)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Contains(body, payload) {
			t.Errorf("provider request omitted genesis payload: %v", err)
			http.Error(w, "payload mismatch", http.StatusBadRequest)
			return
		}
		uploads.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind": "storage#object", "bucket": "archive", "name": "root/source/archive/head.bin",
			"generation": "41", "metageneration": "1", "size": strconv.Itoa(len(payload)),
			"metadata": map[string]string{
				"rhiza-operation-id":   claim.GenesisRef.Mutation.Intent.OperationID,
				"rhiza-payload-sha256": claim.GenesisRef.Mutation.Intent.PayloadDigest,
			},
		})
	}))
	t.Cleanup(server.Close)
	f := newArchiveGenesisFixture(t)
	executor, runtime = f.e, f.runtime
	storageClient, err := storage.NewClient(t.Context(), option.WithEndpoint(server.URL+"/storage/v1/"), option.WithHTTPClient(server.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storageClient.Close() })
	f.e.SourceMutationStorage = storageClient

	first := f.intent
	second := first
	second.OperationID = fencing.MutationRequestID(f.producer, 2)
	second.OwnerStep.Step = "genesis-candidate-two"
	second.PayloadRef, second.PayloadDigest, second.PayloadSize = "", "", 0
	secondTicket := strings.Repeat("9", 64)
	if err := f.stages.Reserve(f.producer, secondTicket, second); err != nil {
		t.Fatal(err)
	}
	sealed, err := f.stages.Seal(f.producer, secondTicket, hashBytes(payload), int64(len(payload)), bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	second.PayloadRef, second.PayloadDigest, second.PayloadSize = sealed.Ref, sealed.Digest, sealed.Size
	f.admit(t, 1, first)
	f.admit(t, 2, second)

	baseBucket := f.e.AuthorityState.bucket
	pausedBucket := &pauseArchiveGenesisRootCASBucket{Bucket: baseBucket, started: make(chan struct{}), release: make(chan struct{})}
	reopened, err := NewAuthorityStateStore(pausedBucket, f.e.AuthorityState.prefix)
	if err != nil {
		t.Fatal(err)
	}
	f.e.AuthorityState = reopened
	prepareResult := make(chan error, 1)
	go func() {
		prepareResult <- f.e.prepareArchiveGenesisMutation(t.Context(), f.runtime.StatefulSetUID, f.runtime, f.producer, 1, first)
	}()
	select {
	case <-pausedBucket.started:
	case <-t.Context().Done():
		t.Fatal("timed out waiting for root-reference CAS pause")
	}
	scope, _, err := f.e.archiveGenesisScope(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	claim, _, err := f.e.AuthorityState.ReadArchiveScopeClaim(t.Context(), scope)
	root, _, rootErr := f.e.AuthorityState.ReadRoot(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil || rootErr != nil || claim.GenesisRef == nil || root.Ledger.ArchiveGenesisRef != nil {
		t.Fatalf("test did not stop between claim and root CAS: claim=%+v root=%+v errors=%v/%v", claim.GenesisRef, root.Ledger.ArchiveGenesisRef, err, rootErr)
	}
	// The losing request observes the winner's claim while the root CAS is
	// paused. Its cancellation is necessarily pending, and this response is
	// intentionally never retried by the test caller.
	err = f.e.executeSourceMutationRequest(t.Context(), "Bearer test", f.id,
		sourceMutationRequest{ProducerUID: f.producer, Sequence: 2, Intent: second}, true)
	if !errors.Is(err, fencing.ErrPending) {
		t.Fatalf("loser was not held pending while root reference was absent: %v", err)
	}
	if _, err := f.e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error {
		return p.Close("closed-during-claim-root-gap", strings.Repeat("e", 64))
	}); err != nil {
		t.Fatal(err)
	}
	close(pausedBucket.release)
	if err := <-prepareResult; err != nil {
		t.Fatalf("winning reference CAS did not resume after release: %v", err)
	}

	status, code := postArchiveGenesisStatus(t, f.e, f.id)
	if code != http.StatusOK || status.Mutation == nil || status.Mutation.Entry == nil || status.Mutation.Entry.Resolution == nil ||
		status.Mutation.Entry.Resolution.Outcome != fencing.MutationCommitted {
		t.Fatalf("status failed to reconcile closed producer after lost loser response: code=%d status=%+v", code, status)
	}
	producer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	loser := producer.Entries[2]
	if loser.Preparation != nil || len(loser.Dispatches) != 0 || loser.Resolution == nil || loser.Resolution.Outcome != fencing.MutationRejected || !loser.Resolution.AllCopiesTerminal {
		t.Fatalf("status did not terminally cancel the lost loser: %+v", loser)
	}
	if uploads.Load() != 1 {
		t.Fatalf("status reconciliation caused %d provider effects, want one", uploads.Load())
	}
}
