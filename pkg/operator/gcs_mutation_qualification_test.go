package operator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

type qualificationRoundTripper func(*http.Request) (*http.Response, error)

func (f qualificationRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type qualificationFixture struct {
	id          fencing.Identity
	runtime     fencing.RuntimeBinding
	scope       AttestationStoreScope
	config      GCSMutationAckLossConfig
	intent      fencing.MutationIntent
	preparation fencing.MutationPreparation
	run         fencing.Run
}

func newQualificationFixture() qualificationFixture {
	id := fencing.Identity{SourceClusterID: "source", NodeID: "node-1", PodUID: "pod-uid", RunID: "run-1", WALIdentity: "wal-identity"}
	runtime := fencing.RuntimeBinding{
		AuthorityID: "authority", NamespaceUID: "namespace-uid", BindingUID: "binding-uid", StatefulSetUID: "sts-uid",
		SourceClusterID: id.SourceClusterID, NodeID: id.NodeID, PodUID: id.PodUID, RunID: id.RunID,
		NodeName: "node", NodeUID: "node-uid", MachineID: "machine", BootID: "boot", RuntimeID: "runtime",
		SandboxID: "sandbox", ContainerID: "container", ImageID: "image",
	}
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive-bucket", Prefix: "clusters/source"}
	digest := strings.Repeat("a", 64)
	key := fmt.Sprintf("archive/blocks/%s_%020d.bin", digest, 3)
	config := GCSMutationAckLossConfig{Version: 1, Selector: GCSMutationAckLossSelectorFirstArchiveExtent, Runtime: runtime, Identity: id, Bucket: scope.Bucket, Prefix: scope.Prefix}
	intent := fencing.MutationIntent{
		ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: hashJSON(scope),
		OwnerStep: fencing.MutationOwnerStep{Owner: "producer-uid", Workflow: "stage-incarnation", Step: "bucket-7", Attempt: 1},
		Condition: fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, OperationID: "operation-7",
		Kind: "upload", Key: key, PayloadDigest: digest, PayloadSize: 128,
	}
	preparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash, GenerationSet: true, Generation: 0}
	run := fencing.Run{Identity: id, Runtime: &runtime, MutationProducerUID: "producer-uid", MutationScopeHash: intent.StoreScopeHash,
		MutationProfile: fencing.MutationHeldExecutionProfile, MutationStageIncarnation: intent.OwnerStep.Workflow}
	return qualificationFixture{id: id, runtime: runtime, scope: scope, config: config, intent: intent, preparation: preparation, run: run}
}

func TestGCSMutationAckLossSelectsFirstCanonicalOrdinaryExtent(t *testing.T) {
	f := newQualificationFixture()
	var receipts []GCSMutationAckLossReceipt
	fault, err := NewGCSMutationAckLossFault(f.config, func(receipt GCSMutationAckLossReceipt) { receipts = append(receipts, receipt) })
	if err != nil {
		t.Fatal(err)
	}
	ctx := fault.arm(context.Background(), f.runtime, f.run, f.run.MutationProducerUID, 7, "dispatch-7", f.intent, f.preparation, f.scope)
	armed, ok := ctx.Value(gcsMutationAckLossContextKey{}).(*gcsMutationAckLossRequest)
	if !ok || armed.intent != f.intent || armed.dispatchID != "dispatch-7" {
		t.Fatal("first eligible ordinary extent was not pinned to its complete original dispatch tuple")
	}
	query := url.Values{"uploadType": {"multipart"}, "ifGenerationMatch": {"0"}, "name": {f.scope.Prefix + "/" + f.intent.Key}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://storage.googleapis.com/upload/storage/v1/b/"+f.scope.Bucket+"/o?"+query.Encode(), strings.NewReader("request"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "multipart/related; boundary=abc")
	responseBody := &countingReadCloser{Reader: strings.NewReader("success response")}
	transport := fault.WrapTransport(qualificationRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: responseBody}, nil
	}))
	response, err := transport.RoundTrip(request)
	if response != nil || !errors.Is(err, errGCSMutationAckLost) || responseBody.reads == 0 || !responseBody.closed {
		t.Fatalf("RoundTrip() = (%v, %v), body reads=%d closed=%v", response, err, responseBody.reads, responseBody.closed)
	}
	if len(receipts) != 1 || !receipts[0].Injected || receipts[0].WireRequests != 1 || receipts[0].DispatchID != "dispatch-7" ||
		receipts[0].Intent != f.intent || receipts[0].Preparation != f.preparation || receipts[0].Scope != f.scope || receipts[0].TupleDigest == "" {
		t.Fatalf("unexpected private fault receipt: %#v", receipts)
	}

	// A second wire is surfaced with a count of two, making the qualification
	// fail closed instead of presenting the first receipt as a one-wire result.
	second, err := transport.RoundTrip(request)
	if err != nil || second == nil || len(receipts) != 2 || receipts[1].WireRequests != 2 || !strings.Contains(receipts[1].Reason, "additional wire") {
		t.Fatalf("second wire = (%v, %v), receipts=%#v", second, err, receipts)
	}

	// The selected tuple stays pinned: another otherwise-valid extent is not
	// eligible for injection during this configured run.
	otherIntent := f.intent
	otherIntent.PayloadDigest = strings.Repeat("b", 64)
	otherIntent.Key = fmt.Sprintf("archive/blocks/%s_%020d.bin", otherIntent.PayloadDigest, 4)
	otherIntent.OwnerStep.Step = "bucket-8"
	otherCtx := fault.arm(context.Background(), f.runtime, f.run, f.run.MutationProducerUID, 8, "dispatch-8", otherIntent, f.preparation, f.scope)
	if _, ok := otherCtx.Value(gcsMutationAckLossContextKey{}).(*gcsMutationAckLossRequest); ok {
		t.Fatal("fault selected more than one source dispatch")
	}
}

func TestGCSMutationAckLossRejectsWrongOwnerPathControlAndProfile(t *testing.T) {
	f := newQualificationFixture()
	fault, err := NewGCSMutationAckLossFault(f.config, func(GCSMutationAckLossReceipt) {})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name        string
		run         fencing.Run
		intent      fencing.MutationIntent
		preparation fencing.MutationPreparation
	}{
		{name: "wrong owner", run: f.run, intent: func() fencing.MutationIntent { i := f.intent; i.OwnerStep.Owner = "different-producer"; return i }(), preparation: f.preparation},
		{name: "non-extent path", run: f.run, intent: func() fencing.MutationIntent { i := f.intent; i.Key = "chunks/fake-object"; return i }(), preparation: f.preparation},
		{name: "control path", run: f.run, intent: func() fencing.MutationIntent { i := f.intent; i.Key = "archive/head.bin"; return i }(), preparation: f.preparation},
		{name: "legacy profile", run: func() fencing.Run { r := f.run; r.MutationProfile = fencing.MutationExecutionProfile; return r }(), intent: f.intent, preparation: f.preparation},
		{name: "nonzero generation", run: f.run, intent: f.intent, preparation: func() fencing.MutationPreparation { p := f.preparation; p.Generation = 4; return p }()},
		{name: "unconditional ordinary upload", run: f.run, intent: func() fencing.MutationIntent {
			i := f.intent
			i.Condition.Mode = fencing.MutationUnconditional
			return i
		}(), preparation: f.preparation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := fault.arm(context.Background(), f.runtime, tc.run, f.run.MutationProducerUID, 7, "dispatch", tc.intent, tc.preparation, f.scope)
			if _, ok := ctx.Value(gcsMutationAckLossContextKey{}).(*gcsMutationAckLossRequest); ok {
				t.Fatal("nonqualifying source mutation was armed")
			}
		})
	}
	if !isGCSMutationControlKey("voters/membership.json") || !isGCSMutationControlKey("recovery/generation.json") || !isGCSMutationControlKey("checkpoint/CURRENT") {
		t.Fatal("known control paths were not excluded")
	}
}

type countingReadCloser struct {
	io.Reader
	reads  int
	closed bool
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	r.reads++
	return r.Reader.Read(p)
}

func (r *countingReadCloser) Close() error { r.closed = true; return nil }

func TestGCSMutationAckLossRejectsMismatchedHTTPRequestAndLegacyConfig(t *testing.T) {
	f := newQualificationFixture()
	fault, err := NewGCSMutationAckLossFault(f.config, func(GCSMutationAckLossReceipt) {})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	responseBody := &countingReadCloser{Reader: strings.NewReader("response")}
	transport := fault.WrapTransport(qualificationRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: responseBody}, nil
	}))
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://storage.googleapis.com/upload/storage/v1/b/"+f.scope.Bucket+"/o?uploadType=multipart&ifGenerationMatch=1&name=wrong", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "multipart/related; boundary=abc")
	response, err := transport.RoundTrip(request)
	if err != nil || response == nil || responseBody.reads != 0 {
		t.Fatalf("mismatched request altered: response=%v err=%v bodyReads=%d", response, err, responseBody.reads)
	}

	legacyConfig := `{"runtime":{},"identity":{},"bucket":"bucket-123","prefix":"source","objectKey":"archive/blocks/fake"}`
	if _, err := ParseGCSMutationAckLossConfig([]byte(legacyConfig)); err == nil {
		t.Fatal("legacy preselected object-key config was accepted")
	}
}
