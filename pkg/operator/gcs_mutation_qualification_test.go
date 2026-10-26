package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
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

func newInitialHeadQualificationFixture(t *testing.T) qualificationFixture {
	t.Helper()
	f := newQualificationFixture()
	payload, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	f.config.Selector = GCSMutationAckLossSelectorInitialArchiveHead
	f.config.Runtime = fencing.RuntimeBinding{}
	f.config.Identity = fencing.Identity{}
	f.config.Bootstrap = &GCSMutationAckLossBootstrapBinding{NamespaceUID: f.runtime.NamespaceUID, SourceClusterID: f.runtime.SourceClusterID}
	f.intent = fencing.MutationIntent{
		ContractVersion: fencing.MutationIntentVersion, StoreScopeHash: hashJSON(f.scope),
		OwnerStep: fencing.MutationOwnerStep{Owner: f.run.MutationProducerUID, Workflow: f.run.MutationStageIncarnation, Step: "genesis-head", Attempt: 1},
		Condition: fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, OperationID: "genesis-operation",
		Kind: "upload", Key: "archive/head.bin", PayloadDigest: authorityStateDigest(payload), PayloadSize: int64(len(payload)),
	}
	f.preparation = fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: f.intent.StoreScopeHash, GenerationSet: true}
	return f
}

func TestGCSMutationAckLossInitialHeadConfigAndCanonicalDispatch(t *testing.T) {
	f := newInitialHeadQualificationFixture(t)
	configBytes, err := json.Marshal(f.config)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseGCSMutationAckLossConfig(configBytes)
	if err != nil || parsed.Selector != GCSMutationAckLossSelectorInitialArchiveHead || parsed.Bootstrap == nil {
		t.Fatalf("parse initial HEAD selector: config=%+v err=%v", parsed, err)
	}
	var receipts []GCSMutationAckLossReceipt
	fault, err := NewGCSMutationAckLossFault(parsed, func(receipt GCSMutationAckLossReceipt) { receipts = append(receipts, receipt) })
	if err != nil || !fault.RequiresBootstrapGenesisAuthorization() {
		t.Fatalf("initial HEAD selector did not require CONTROL authorization: fault=%v err=%v", fault, err)
	}
	ctx := fault.arm(context.Background(), f.runtime, f.run, f.run.MutationProducerUID, 1, "dispatch-genesis", f.intent, f.preparation, f.scope, true)
	armed, ok := ctx.Value(gcsMutationAckLossContextKey{}).(*gcsMutationAckLossRequest)
	if !ok || armed.intent != f.intent || armed.runtime != f.runtime || armed.identity != f.id || armed.sequence != 1 || armed.dispatchID != "dispatch-genesis" {
		t.Fatal("canonical initial HEAD dispatch was not pinned to its exact CONTROL-derived tuple")
	}
	query := url.Values{"uploadType": {"multipart"}, "ifGenerationMatch": {"0"}, "name": {f.scope.Prefix + "/archive/head.bin"}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://storage.googleapis.com/upload/storage/v1/b/"+f.scope.Bucket+"/o?"+query.Encode(), strings.NewReader("multipart payload"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "multipart/related; boundary=bootstrap")
	body := &countingReadCloser{Reader: strings.NewReader("provider success response")}
	transport := fault.WrapTransport(qualificationRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
	}))
	response, err := transport.RoundTrip(request)
	if response != nil || !errors.Is(err, errGCSMutationAckLost) || !body.closed {
		t.Fatalf("initial HEAD response loss = (%v, %v), bodyClosed=%t", response, err, body.closed)
	}
	if len(receipts) != 1 || !receipts[0].Injected || receipts[0].WireRequests != 1 || receipts[0].Intent != f.intent ||
		receipts[0].Preparation != f.preparation || receipts[0].Runtime != f.runtime || receipts[0].SourceIdentity != f.id ||
		receipts[0].ObjectKey != "archive/head.bin" || receipts[0].PayloadDigest != f.intent.PayloadDigest || receipts[0].PayloadSize != f.intent.PayloadSize {
		t.Fatalf("initial HEAD response-loss receipt differs from the exact selected dispatch: %#v", receipts)
	}
}

func TestGCSMutationAckLossInitialHeadRejectsIneligibleDispatches(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(*qualificationFixture)
		withControl bool
	}{
		{name: "no canonical CONTROL authorization"},
		{name: "wrong namespace UID", mutate: func(f *qualificationFixture) { f.runtime.NamespaceUID = "other-namespace" }, withControl: true},
		{name: "wrong source cluster", mutate: func(f *qualificationFixture) { f.runtime.SourceClusterID = "other-source" }, withControl: true},
		{name: "source run mismatch", mutate: func(f *qualificationFixture) { f.run.Identity.SourceClusterID = "other-source" }, withControl: true},
		{name: "wrong object key", mutate: func(f *qualificationFixture) { f.intent.Key = "archive/blocks/fake" }, withControl: true},
		{name: "wrong mutation kind", mutate: func(f *qualificationFixture) { f.intent.Kind = "delete" }, withControl: true},
		{name: "unconditional intent", mutate: func(f *qualificationFixture) { f.intent.Condition.Mode = fencing.MutationUnconditional }, withControl: true},
		{name: "noncanonical HEAD bytes", mutate: func(f *qualificationFixture) { f.intent.PayloadDigest = strings.Repeat("c", 64) }, withControl: true},
		{name: "wrong payload size", mutate: func(f *qualificationFixture) { f.intent.PayloadSize++ }, withControl: true},
		{name: "nonzero generation", mutate: func(f *qualificationFixture) { f.preparation.Generation = 1 }, withControl: true},
		{name: "legacy profile", mutate: func(f *qualificationFixture) { f.run.MutationProfile = fencing.MutationExecutionProfile }, withControl: true},
		{name: "wrong scope", mutate: func(f *qualificationFixture) { f.scope.Prefix = "other/source" }, withControl: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newInitialHeadQualificationFixture(t)
			if test.mutate != nil {
				test.mutate(&f)
			}
			fault, err := NewGCSMutationAckLossFault(f.config, func(GCSMutationAckLossReceipt) {})
			if err != nil {
				t.Fatal(err)
			}
			var ctx context.Context
			if test.withControl {
				ctx = fault.arm(context.Background(), f.runtime, f.run, f.run.MutationProducerUID, 1, "dispatch", f.intent, f.preparation, f.scope, true)
			} else {
				ctx = fault.arm(context.Background(), f.runtime, f.run, f.run.MutationProducerUID, 1, "dispatch", f.intent, f.preparation, f.scope)
			}
			if _, ok := ctx.Value(gcsMutationAckLossContextKey{}).(*gcsMutationAckLossRequest); ok {
				t.Fatal("ineligible initial HEAD dispatch was armed")
			}
		})
	}
}

func TestGCSMutationAckLossInitialHeadSelectorIsOptInAndConfigIsStrict(t *testing.T) {
	extent := newQualificationFixture()
	extentFault, err := NewGCSMutationAckLossFault(extent.config, func(GCSMutationAckLossReceipt) {})
	if err != nil || extentFault.RequiresBootstrapGenesisAuthorization() {
		t.Fatalf("ordinary extent selector changed its default authorization contract: fault=%v err=%v", extentFault, err)
	}
	initial := newInitialHeadQualificationFixture(t)
	for _, tc := range []struct {
		name   string
		change func(*GCSMutationAckLossConfig)
	}{
		{name: "missing bootstrap", change: func(c *GCSMutationAckLossConfig) { c.Bootstrap = nil }},
		{name: "missing namespace UID", change: func(c *GCSMutationAckLossConfig) { c.Bootstrap.NamespaceUID = "" }},
		{name: "missing source cluster", change: func(c *GCSMutationAckLossConfig) { c.Bootstrap.SourceClusterID = "" }},
		{name: "identity must be resolved from CONTROL", change: func(c *GCSMutationAckLossConfig) { c.Identity = initial.id }},
		{name: "runtime must be resolved from CONTROL", change: func(c *GCSMutationAckLossConfig) { c.Runtime = initial.runtime }},
		{name: "bootstrap is not valid for extent selector", change: func(c *GCSMutationAckLossConfig) { c.Selector = GCSMutationAckLossSelectorFirstArchiveExtent }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := initial.config
			tc.change(&config)
			if err := config.Validate(); err == nil {
				t.Fatal("accepted malformed or mismatched bootstrap selector config")
			}
		})
	}
}

func TestGCSMutationAckLossInitialHeadRequiresExactCanonicalControlGenesis(t *testing.T) {
	f := newAutomaticBootstrapGenesisFixture(t)
	ctx := t.Context()
	state, err := f.e.read(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if !f.e.canonicalBootstrapGenesisDispatch(ctx, f.runtime, state, f.producer, 1, f.intent, f.scope) {
		t.Fatal("exact root- and claim-referenced initial HEAD dispatch was not authorized")
	}
	for _, tc := range []struct {
		name   string
		change func(*fenceLedger, *string, *uint64, *fencing.MutationIntent, *AttestationStoreScope)
	}{
		{name: "missing committed root reference", change: func(state *fenceLedger, _ *string, _ *uint64, _ *fencing.MutationIntent, _ *AttestationStoreScope) {
			state.Ledger.ArchiveGenesisRef = nil
		}},
		{name: "wrong root cluster", change: func(state *fenceLedger, _ *string, _ *uint64, _ *fencing.MutationIntent, _ *AttestationStoreScope) {
			state.Ledger.SourceClusterID = "other-source"
		}},
		{name: "wrong producer", change: func(_ *fenceLedger, producer *string, _ *uint64, _ *fencing.MutationIntent, _ *AttestationStoreScope) {
			*producer = "other-producer"
		}},
		{name: "wrong sequence", change: func(_ *fenceLedger, _ *string, sequence *uint64, _ *fencing.MutationIntent, _ *AttestationStoreScope) {
			*sequence = 2
		}},
		{name: "different intent", change: func(_ *fenceLedger, _ *string, _ *uint64, intent *fencing.MutationIntent, _ *AttestationStoreScope) {
			intent.Key = "archive/blocks/unrelated"
		}},
		{name: "different object-store scope", change: func(_ *fenceLedger, _ *string, _ *uint64, _ *fencing.MutationIntent, scope *AttestationStoreScope) {
			scope.Prefix = "root/other"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := state
			candidate.Ledger = state.Ledger
			if state.Ledger.ArchiveGenesisRef != nil {
				ref := *state.Ledger.ArchiveGenesisRef
				candidate.Ledger.ArchiveGenesisRef = &ref
			}
			producer, sequence, intent, scope := f.producer, uint64(1), f.intent, f.scope
			tc.change(&candidate, &producer, &sequence, &intent, &scope)
			if f.e.canonicalBootstrapGenesisDispatch(ctx, f.runtime, candidate, producer, sequence, intent, scope) {
				t.Fatal("fault selector accepted a tuple not referenced by canonical CONTROL root and scope claim")
			}
		})
	}
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
