package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
	"google.golang.org/api/option"
)

type failOneLogicalProducerWriteBucket struct {
	objstore.Bucket
	fail atomic.Bool
}

func (b *failOneLogicalProducerWriteBucket) Upload(ctx context.Context, name string, input io.Reader, options ...objstore.ObjectUploadOption) error {
	body, err := io.ReadAll(input)
	if err != nil {
		return err
	}
	if strings.Contains(name, "/logical-producers/") && b.fail.CompareAndSwap(true, false) {
		return errors.New("injected logical producer CAS failure after cancellation proof persistence")
	}
	return b.Bucket.Upload(ctx, name, bytes.NewReader(body), options...)
}

func logicalGenesisStorageServer(t *testing.T, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	objects := map[string]map[string]any{}
	var generation int64 = 20
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		name := r.URL.Query().Get("name")
		if r.Method == http.MethodGet {
			if object, ok := objects[name]; ok {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(object)
				return
			}
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost || name == "" || r.URL.Query().Get("ifGenerationMatch") != "0" {
			t.Errorf("unexpected logical GCS mutation: %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "multipart/related" || params["boundary"] == "" {
			t.Errorf("invalid GCS multipart request: %v", err)
			http.Error(w, "invalid multipart request", http.StatusBadRequest)
			return
		}
		reader := multipart.NewReader(r.Body, params["boundary"])
		metadataPart, err := reader.NextPart()
		if err != nil {
			t.Errorf("read GCS metadata: %v", err)
			http.Error(w, "missing metadata", http.StatusBadRequest)
			return
		}
		var metadata struct {
			Name     string            `json:"name"`
			Metadata map[string]string `json:"metadata"`
		}
		if err := json.NewDecoder(metadataPart).Decode(&metadata); err != nil {
			t.Errorf("decode GCS metadata: %v", err)
			http.Error(w, "bad metadata", http.StatusBadRequest)
			return
		}
		_ = metadataPart.Close()
		mediaPart, err := reader.NextPart()
		if err != nil {
			t.Errorf("read GCS payload: %v", err)
			http.Error(w, "missing payload", http.StatusBadRequest)
			return
		}
		payload, readErr := io.ReadAll(mediaPart)
		_ = mediaPart.Close()
		if readErr != nil || metadata.Name != name || metadata.Metadata["rhiza-payload-sha256"] != hashBytes(payload) {
			t.Errorf("GCS mutation metadata/payload mismatch: name=%q query=%q readErr=%v", metadata.Name, name, readErr)
			http.Error(w, "payload mismatch", http.StatusBadRequest)
			return
		}
		if _, exists := objects[name]; exists {
			http.Error(w, "object already exists", http.StatusPreconditionFailed)
			return
		}
		generation++
		object := map[string]any{"kind": "storage#object", "bucket": "logical-manifest-archive", "name": name,
			"generation": fmt.Sprint(generation), "metageneration": "1", "size": fmt.Sprint(len(payload)), "metadata": metadata.Metadata}
		objects[name] = object
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(object)
	}))
}

func TestLogicalGenesisHTTPOrdinaryMutationRequiresRealCommittedWinner(t *testing.T) {
	fixture := newLogicalManifestHTTPFixture(t, nil)
	ctx := context.Background()
	authority := &fencing.AuthorityClient{URL: fixture.server.URL + "/v1/supervisor", TokenFile: fixture.podTokenFile,
		Client: fixture.server.Client(), AuthorityProfile: fencing.LogicalWriterProfileV1}
	if _, err := authority.RegisterStartup(ctx, fixture.identity); err != nil {
		t.Fatal(err)
	}
	id := fixture.identity
	id.WALIdentity = hashJSON("logical-genesis-missing-winner-wal")
	receipt, err := authority.Bind(ctx, id)
	if err != nil || receipt.LogicalBinding == nil || receipt.Producer == nil {
		t.Fatalf("bind logical Run for no-winner negative: receipt=%+v err=%v", receipt, err)
	}
	intent := completeStoreIntent(fencing.MutationIntent{Kind: "upload", Key: "ordinary/before-genesis.bin"})
	intent.StoreScopeHash = receipt.Producer.StoreScopeHash
	intent.OperationID = fencing.MutationRequestID(receipt.Producer.ProducerUID, 1)
	root, err := fixture.e.read(ctx, logicalManifestSTSUID, id.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run := root.Ledger.Runs[id.RunID]
	err = fixture.e.requireLogicalArchiveGenesisCommitted(ctx, logicalManifestSTSUID, id, run, intent)
	if !errors.Is(err, fencing.ErrPending) {
		t.Fatalf("ordinary mutation without a committed canonical winner was not held: %v", err)
	}
}

func TestLogicalGenesisHTTPAdmissionRecoversCancellationProofWrittenBeforeProducerCAS(t *testing.T) {
	wal := hashJSON("logical-genesis-loser-wal")
	fixture := newLogicalManifestHTTPFixture(t, nil)
	ctx := context.Background()
	fixture.state.bucket = &failOneLogicalProducerWriteBucket{Bucket: fixture.bucket}

	identity := fixture.identity
	authority := &fencing.AuthorityClient{URL: fixture.server.URL + "/v1/supervisor", TokenFile: fixture.podTokenFile,
		Client: fixture.server.Client(), AuthorityProfile: fencing.LogicalWriterProfileV1}
	if _, err := authority.RegisterStartup(ctx, identity); err != nil {
		t.Fatalf("idempotent logical startup registration over authenticated HTTP: %v", err)
	}
	identity.WALIdentity = wal
	receipt, err := authority.Bind(ctx, identity)
	if err != nil || receipt.LogicalBinding == nil || receipt.Producer == nil {
		t.Fatalf("logical bind omitted its canonical producer: receipt=%+v err=%v", receipt, err)
	}
	binding := *receipt.LogicalBinding
	producerUID := receipt.Producer.ProducerUID
	rootUID := logicalManifestSTSUID
	emptyHead, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	baseBucket := fixture.state.bucket.(*failOneLogicalProducerWriteBucket)
	var storageCalls atomic.Int32
	storageServer := logicalGenesisStorageServer(t, &storageCalls)
	t.Cleanup(storageServer.Close)
	storageClient, err := storage.NewClient(ctx, option.WithEndpoint(storageServer.URL+"/storage/v1/"), option.WithHTTPClient(storageServer.Client()), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storageClient.Close() })
	fixture.e.SourceMutationStorage = storageClient
	retryClient, err := fencing.CreateAuthoritySourceMutationClient(t.TempDir()+"/source-retry", authority, receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = retryClient.Close() })
	if err := retryClient.Upload(ctx, "archive/head.bin", fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, emptyHead); err != nil {
		t.Fatalf("first logical writer did not commit its actual archive genesis tuple: %v", err)
	}
	root, err := fixture.e.read(ctx, rootUID, identity.SourceClusterID)
	if err != nil || root.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisCommitted || root.Ledger.ArchiveGenesisRef == nil || root.Ledger.ArchiveGenesisRef.Mutation == nil {
		t.Fatalf("first source writer did not establish canonical committed genesis evidence: ledger=%+v err=%v", root.Ledger, err)
	}
	winner := *root.Ledger.ArchiveGenesisRef
	baseBucket.fail.Store(true)
	if err := retryClient.Upload(ctx, "archive/head.bin", fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, emptyHead); err == nil {
		t.Fatal("losing tuple unexpectedly survived the injected cancellation-proof CAS failure")
	}
	firstProducer, firstUID, _, err := fixture.state.ReadLogicalProducer(ctx, rootUID, binding)
	if err != nil || firstUID != producerUID || firstProducer.LastAdmitted != 1 || len(firstProducer.Entries) != 1 || firstProducer.Entries[1].Resolution == nil || firstProducer.Entries[1].Resolution.Outcome != fencing.MutationCommitted {
		t.Fatalf("failed loser CAS changed producer beyond the committed winner: uid=%q producer=%+v err=%v", firstUID, firstProducer, err)
	}
	proof := logicalArchiveGenesisCancellationProof{Version: 1, StatefulSetUID: rootUID, SourceClusterID: identity.SourceClusterID,
		Binding: binding, WALIdentity: wal, ProducerUID: producerUID, Sequence: 2, WinningGenesis: winner}
	// A second ordinary upload drives recoverPending: it first retries the exact
	// loser tuple, then allocates and dispatches the next sequence for a distinct
	// key. The test provider commits it so the same client can prove it advanced.
	nextErr := retryClient.Upload(ctx, "ordinary/next.bin", fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, []byte("next mutation"))
	if nextErr != nil {
		producer, _, _, readErr := fixture.state.ReadLogicalProducer(ctx, rootUID, binding)
		t.Fatalf("next ordinary mutation did not recover the loser and succeed: %v; producer=%+v readErr=%v storageCalls=%d", nextErr, producer, readErr, storageCalls.Load())
	}
	producer, actualUID, _, err := fixture.state.ReadLogicalProducer(ctx, rootUID, binding)
	if err != nil || actualUID != producerUID || producer.LastAdmitted != 3 || producer.Entries[2].Intent.Key != "archive/head.bin" ||
		producer.Entries[2].Resolution == nil || producer.Entries[2].Resolution.Outcome != fencing.MutationRejected ||
		producer.Entries[3].Intent.Key != "ordinary/next.bin" || producer.Entries[3].Resolution == nil || producer.Entries[3].Resolution.Outcome != fencing.MutationCommitted {
		t.Fatalf("client did not recover the loser and advance to the next ordinary tuple: producer=%+v err=%v", producer, err)
	}
	entry := producer.Entries[2]
	proof.Intent = entry.Intent
	proofBytes, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	proofDigest := authorityStateDigest(proofBytes)
	evidence, err := fixture.e.authorityEvidenceForRoot(rootUID, identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	proofReader, _, err := evidence.Read(ctx, "archive-genesis-cancellations/"+proofDigest)
	if err != nil {
		t.Fatalf("immutable loser proof was not persisted before the failed CAS: %v", err)
	}
	storedProof, readErr := io.ReadAll(proofReader)
	closeErr := proofReader.Close()
	if err := errors.Join(readErr, closeErr); err != nil || !bytes.Equal(storedProof, proofBytes) {
		t.Fatalf("persisted proof is not the exact expected winner/loser binding: %v", err)
	}
	if entry.Intent.Key != "archive/head.bin" || entry.Preparation != nil ||
		len(entry.Dispatches) != 0 || !entry.DispatchClosed || entry.Resolution == nil || entry.Resolution.Outcome != fencing.MutationRejected ||
		!entry.Resolution.AllCopiesTerminal || entry.Resolution.ReceiptDigest != proofDigest {
		t.Fatalf("retry did not consume the exact loser tuple without dispatch: producer=%+v entry=%+v err=%v", producer, entry, err)
	}
	request := object{"identity": identity, "producerUID": producerUID, "sequence": uint64(2), "intent": entry.Intent}
	if repeated := logicalManifestHTTPPost(t, fixture, "/v1/source-mutations/admit", request); repeated.Code != http.StatusOK {
		t.Fatalf("exact terminal loser replay was not idempotent: status=%d body=%s", repeated.Code, repeated.Body.String())
	}
	altered := entry.Intent
	altered.PayloadDigest = strings.Repeat("e", 64)
	if changed := logicalManifestHTTPPost(t, fixture, "/v1/source-mutations/admit", object{"identity": identity, "producerUID": producerUID, "sequence": uint64(2), "intent": altered}); changed.Code == http.StatusOK {
		t.Fatalf("altered terminal intent reused the loser tuple: status=%d body=%q", changed.Code, changed.Body.String())
	}
	if changedOwner := logicalManifestHTTPPost(t, fixture, "/v1/source-mutations/admit", object{"identity": identity, "producerUID": "other-producer", "sequence": uint64(2), "intent": entry.Intent}); changedOwner.Code == http.StatusOK {
		t.Fatal("altered terminal owner reused the loser tuple")
	}
	if storageCalls.Load() < 2 {
		t.Fatalf("expected actual GCS provider requests for the winner and next ordinary mutation, got %d", storageCalls.Load())
	}
}
