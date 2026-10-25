package operator

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

const GCSMutationAckLossSelectorFirstArchiveExtent = "first-ordinary-archive-extent-v1"
const GCSMutationAckLossSelectorInitialArchiveHead = "initial-canonical-archive-head-v1"

// GCSMutationAckLossBootstrapBinding pins a pre-apply bootstrap run to its
// namespace and declared source. Runtime and producer identities are resolved
// only from canonical CONTROL state when the genesis dispatch is admitted.
type GCSMutationAckLossBootstrapBinding struct {
	NamespaceUID    string `json:"namespaceUID"`
	SourceClusterID string `json:"sourceClusterID"`
}

// GCSMutationPreWireGate enables one private, exact-dispatch pause before the
// authenticated transport sends a selected request.
type GCSMutationPreWireGate struct {
	Nonce       string `json:"nonce"`
	ReleaseFile string `json:"releaseFile"`
}

// GCSMutationAckLossConfig opts one exact source GCS scope into a one-shot
// qualification fault. The ordinary-extent selector binds a complete runtime
// in advance; the initial-HEAD selector binds namespace/source and resolves
// the admitted runtime from canonical CONTROL state at dispatch.
type GCSMutationAckLossConfig struct {
	Version     int                                 `json:"version"`
	Selector    string                              `json:"selector"`
	Runtime     fencing.RuntimeBinding              `json:"runtime"`
	Identity    fencing.Identity                    `json:"identity"`
	Bucket      string                              `json:"bucket"`
	Prefix      string                              `json:"prefix"`
	Bootstrap   *GCSMutationAckLossBootstrapBinding `json:"bootstrap,omitempty"`
	PreWireGate *GCSMutationPreWireGate             `json:"preWireGate,omitempty"`
}

// GCSMutationAckLossReceipt is a private log event correlating a transport
// fault or pre-wire gate to the canonical producer tuple. It contains no
// credentials and is not recovery evidence.
type GCSMutationAckLossReceipt struct {
	Version        int                         `json:"version"`
	Selector       string                      `json:"selector"`
	Injected       bool                        `json:"injected"`
	Reason         string                      `json:"reason,omitempty"`
	GatePhase      string                      `json:"gatePhase,omitempty"`
	GateNonce      string                      `json:"gateNonce,omitempty"`
	ProcessID      string                      `json:"processID,omitempty"`
	HTTPStatus     int                         `json:"httpStatus,omitempty"`
	WireRequests   uint64                      `json:"wireRequests"`
	SourceIdentity fencing.Identity            `json:"sourceIdentity"`
	Runtime        fencing.RuntimeBinding      `json:"runtime"`
	ProducerUID    string                      `json:"producerUID"`
	Sequence       uint64                      `json:"sequence"`
	DispatchID     string                      `json:"dispatchID"`
	OperationID    string                      `json:"operationID"`
	Intent         fencing.MutationIntent      `json:"intent"`
	Preparation    fencing.MutationPreparation `json:"preparation"`
	Scope          AttestationStoreScope       `json:"scope"`
	TupleDigest    string                      `json:"tupleDigest"`
	Bucket         string                      `json:"bucket"`
	Prefix         string                      `json:"prefix"`
	ObjectKey      string                      `json:"objectKey"`
	PayloadDigest  string                      `json:"payloadDigest"`
	PayloadSize    int64                       `json:"payloadSize"`
	ObservedAt     time.Time                   `json:"observedAt"`
}

type gcsMutationAckLossRequest struct {
	runtime     fencing.RuntimeBinding
	identity    fencing.Identity
	producerUID string
	sequence    uint64
	dispatchID  string
	intent      fencing.MutationIntent
	preparation fencing.MutationPreparation
	scope       AttestationStoreScope
	wireCount   uint64
}

type gcsMutationAckLossContextKey struct{}

// GCSMutationAckLossFault is armed only by a canonical, original held-v2
// source mutation dispatch matching its exact run, storage scope and key.
type GCSMutationAckLossFault struct {
	config GCSMutationAckLossConfig
	log    func(GCSMutationAckLossReceipt)

	mu        sync.Mutex
	selected  *gcsMutationAckLossRequest
	fired     bool
	inFlight  bool
	gateSeen  bool
	processID string
}

func ParseGCSMutationAckLossConfig(data []byte) (GCSMutationAckLossConfig, error) {
	var config GCSMutationAckLossConfig
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return GCSMutationAckLossConfig{}, fmt.Errorf("decode GCS acknowledgement-loss config: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return GCSMutationAckLossConfig{}, fmt.Errorf("decode GCS acknowledgement-loss config: trailing data")
	}
	if err := config.Validate(); err != nil {
		return GCSMutationAckLossConfig{}, err
	}
	return config, nil
}

func (c GCSMutationAckLossConfig) Validate() error {
	if c.Version != 1 {
		return fmt.Errorf("GCS acknowledgement-loss config requires version 1")
	}
	switch c.Selector {
	case GCSMutationAckLossSelectorFirstArchiveExtent:
		if c.Bootstrap != nil || c.Identity.WALIdentity == "" || c.Runtime.Validate(c.Identity) != nil {
			return fmt.Errorf("ordinary-extent ACK-loss config requires an exact enrolled runtime and WAL identity")
		}
	case GCSMutationAckLossSelectorInitialArchiveHead:
		if c.Bootstrap == nil || c.Bootstrap.NamespaceUID == "" || c.Bootstrap.SourceClusterID == "" ||
			c.Identity != (fencing.Identity{}) || c.Runtime != (fencing.RuntimeBinding{}) {
			return fmt.Errorf("initial HEAD ACK-loss config requires only an exact bootstrap namespace and source binding")
		}
	default:
		return fmt.Errorf("GCS acknowledgement-loss config requires an exact enrolled runtime and WAL identity")
	}
	if len(c.Bucket) < 3 || len(c.Bucket) > 63 || !isBucketName(c.Bucket) || c.Prefix == "" || path.Clean(c.Prefix) != c.Prefix || c.Prefix == "." || strings.HasPrefix(c.Prefix, "/") {
		return fmt.Errorf("GCS acknowledgement-loss config requires an exact bucket and canonical prefix")
	}
	if c.PreWireGate != nil && (!validGCSQualificationNonce(c.PreWireGate.Nonce) || !filepath.IsAbs(c.PreWireGate.ReleaseFile) || filepath.Clean(c.PreWireGate.ReleaseFile) != c.PreWireGate.ReleaseFile) {
		return fmt.Errorf("GCS pre-wire gate requires a 32-byte lowercase-hex nonce and absolute canonical release file")
	}
	return nil
}

func validGCSQualificationNonce(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func isBucketName(bucket string) bool {
	for i, r := range bucket {
		valid := r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.'
		if !valid || (i == 0 || i == len(bucket)-1) && !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return !strings.Contains(bucket, "..")
}

func isGCSMutationControlKey(key string) bool {
	for _, control := range []string{
		"archive/head.bin", "archive/seal.json", "archive/PUBLISH_LOCK",
		"voters/membership.json", "recovery/generation.json", "recovery/successor.json",
		"checkpoint/CURRENT", "checkpoint/PUBLISHER",
	} {
		if key == control {
			return true
		}
	}
	for _, prefix := range []string{"voters/", "recovery/", "checkpoint/", "archive/recovery-pins/", "archive/gc-candidates/"} {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func isGCSOrdinaryArchiveExtentKey(key, payloadDigest string) bool {
	const prefix = "archive/blocks/"
	if !strings.HasPrefix(key, prefix) {
		return false
	}
	base := strings.TrimPrefix(key, prefix)
	if len(base) != 64+1+20+len(".bin") || !strings.HasSuffix(base, ".bin") || base[64] != '_' || base[:64] != payloadDigest {
		return false
	}
	generationText := base[65 : 65+20]
	generation, err := strconv.ParseUint(generationText, 10, 64)
	return err == nil && generation > 0 && generationText == fmt.Sprintf("%020d", generation)
}

func NewGCSMutationAckLossFault(config GCSMutationAckLossConfig, report func(GCSMutationAckLossReceipt)) (*GCSMutationAckLossFault, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if report == nil {
		return nil, fmt.Errorf("GCS acknowledgement-loss config requires a private receipt logger")
	}
	fault := &GCSMutationAckLossFault{config: config, log: report}
	if config.PreWireGate != nil {
		var processNonce [16]byte
		if _, err := rand.Read(processNonce[:]); err != nil {
			return nil, fmt.Errorf("create qualification process identity: %w", err)
		}
		fault.processID = strconv.Itoa(os.Getpid()) + "-" + hex.EncodeToString(processNonce[:])
	}
	return fault, nil
}

func (f *GCSMutationAckLossFault) RequiresBootstrapGenesisAuthorization() bool {
	return f != nil && f.config.Selector == GCSMutationAckLossSelectorInitialArchiveHead
}

func (f *GCSMutationAckLossFault) arm(ctx context.Context, runtime fencing.RuntimeBinding, run fencing.Run, producerUID string, sequence uint64, dispatchID string,
	intent fencing.MutationIntent, preparation fencing.MutationPreparation, scope AttestationStoreScope, canonicalBootstrapGenesis ...bool) context.Context {
	if f == nil || ctx == nil || dispatchID == "" || producerUID == "" || sequence == 0 || run.MutationProfile != fencing.MutationHeldExecutionProfile ||
		run.MutationProducerUID != producerUID || run.MutationStageIncarnation == "" ||
		intent.OwnerStep.Owner != producerUID || intent.OwnerStep.Workflow != run.MutationStageIncarnation || intent.OwnerStep.Attempt != 1 ||
		intent.Condition.Mode != fencing.MutationCreateOnly || intent.PayloadSize <= 0 || !fencing.IsHeldCreateEligible(intent, preparation) ||
		scope.Provider != "gcs" || scope.Bucket != f.config.Bucket || scope.Prefix != f.config.Prefix || scope.Endpoint != "" || scope.Account != "" || scope.ConnectionStringDigest != "" {
		return ctx
	}
	switch f.config.Selector {
	case GCSMutationAckLossSelectorFirstArchiveExtent:
		if f.config.Bootstrap != nil || run.Identity != f.config.Identity || run.Runtime == nil || *run.Runtime != f.config.Runtime || runtime != f.config.Runtime ||
			intent.OwnerStep.Step != fmt.Sprintf("bucket-%d", sequence) || !isGCSOrdinaryArchiveExtentKey(intent.Key, intent.PayloadDigest) {
			return ctx
		}
	case GCSMutationAckLossSelectorInitialArchiveHead:
		if f.config.Bootstrap == nil || runtime.NamespaceUID != f.config.Bootstrap.NamespaceUID || runtime.SourceClusterID != f.config.Bootstrap.SourceClusterID ||
			run.Identity.SourceClusterID != f.config.Bootstrap.SourceClusterID || run.Identity.WALIdentity == "" || run.Runtime == nil || *run.Runtime != runtime || runtime.Validate(run.Identity) != nil ||
			run.MutationScopeHash != intent.StoreScopeHash || !isCanonicalEmptyArchiveHeadIntent(intent) ||
			len(canonicalBootstrapGenesis) != 1 || !canonicalBootstrapGenesis[0] {
			return ctx
		}
	default:
		return ctx
	}
	request := &gcsMutationAckLossRequest{
		runtime: runtime, identity: run.Identity, producerUID: producerUID, sequence: sequence,
		dispatchID: dispatchID, intent: intent, preparation: preparation, scope: scope,
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.selected != nil {
		return ctx
	}
	f.selected = request // The canonical grant above is durable before any wire call.
	return context.WithValue(ctx, gcsMutationAckLossContextKey{}, request)
}

func isCanonicalEmptyArchiveHeadIntent(intent fencing.MutationIntent) bool {
	if intent.Key != "archive/head.bin" || intent.Kind != "upload" || intent.Condition.Mode != fencing.MutationCreateOnly {
		return false
	}
	head, err := recovery.EmptyArchiveHeadBytes()
	return err == nil && int64(len(head)) == intent.PayloadSize && authorityStateDigest(head) == intent.PayloadDigest
}

// WrapTransport wraps the authenticated HTTP transport used by the Storage
// client. It does not inspect or buffer upload bodies.
func (f *GCSMutationAckLossFault) WrapTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return gcsMutationAckLossRoundTripper{fault: f, base: base}
}

type gcsMutationAckLossRoundTripper struct {
	fault *GCSMutationAckLossFault
	base  http.RoundTripper
}

func (t gcsMutationAckLossRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	fault := t.fault
	armed, ok := request.Context().Value(gcsMutationAckLossContextKey{}).(*gcsMutationAckLossRequest)
	if fault == nil || !ok || !fault.matchesRequest(request, armed) {
		return t.base.RoundTrip(request)
	}
	fault.mu.Lock()
	if fault.selected != armed {
		fault.mu.Unlock()
		return t.base.RoundTrip(request)
	}
	if fault.config.PreWireGate != nil {
		if fault.gateSeen || fault.inFlight {
			fault.mu.Unlock()
			fault.reportGate(armed, "duplicate", fmt.Errorf("pre-wire gate is one-shot"), 0)
			return nil, fmt.Errorf("GCS qualification: selected pre-wire request is one-shot")
		}
		fault.gateSeen = true
		fault.inFlight = true
		fault.mu.Unlock()
		if err := fault.awaitPreWireRelease(request.Context(), armed); err != nil {
			fault.mu.Lock()
			fault.inFlight = false
			fault.mu.Unlock()
			return nil, err
		}
		if err := request.Context().Err(); err != nil {
			fault.mu.Lock()
			fault.inFlight = false
			fault.mu.Unlock()
			fault.reportGate(armed, "cancelled", err, 0)
			return nil, fmt.Errorf("GCS qualification pre-wire gate cancelled: %w", err)
		}
		fault.mu.Lock()
		armed.wireCount++
		fault.mu.Unlock()
		response, err := t.base.RoundTrip(request)
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		fault.mu.Lock()
		fault.inFlight = false
		fault.mu.Unlock()
		fault.reportGate(armed, "wire-result", err, status)
		return response, err
	}
	if fault.inFlight {
		fault.mu.Unlock()
		fault.report(armed, false, "concurrent wire request for selected dispatch")
		return nil, fmt.Errorf("GCS qualification: selected dispatch already has an in-flight request")
	}
	if fault.fired {
		armed.wireCount++
		fault.mu.Unlock()
		fault.report(armed, true, "additional wire request after injected response loss")
		return t.base.RoundTrip(request)
	}
	armed.wireCount++
	wireCount := armed.wireCount
	fault.inFlight = true
	fault.mu.Unlock()
	response, err := t.base.RoundTrip(request)
	if err != nil || response == nil || response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		fault.mu.Lock()
		fault.inFlight = false
		fault.mu.Unlock()
		return response, err
	}
	if wireCount != 1 {
		fault.mu.Lock()
		fault.inFlight = false
		fault.mu.Unlock()
		fault.report(armed, false, "multiple wire requests for the selected dispatch")
		return response, nil
	}
	if response.Body == nil {
		fault.mu.Lock()
		fault.inFlight = false
		fault.mu.Unlock()
		return nil, fmt.Errorf("GCS acknowledgement-loss fault could not drain an empty response body")
	}
	_, copyErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		fault.mu.Lock()
		fault.inFlight = false
		fault.mu.Unlock()
		return nil, fmt.Errorf("GCS acknowledgement-loss fault could not consume the successful response: %w", err)
	}
	fault.mu.Lock()
	fault.fired = true
	fault.inFlight = false
	fault.mu.Unlock()
	fault.report(armed, true, "")
	return nil, errGCSMutationAckLost
}

type gcsMutationPreWireRelease struct {
	Version     int    `json:"version"`
	Nonce       string `json:"nonce"`
	ProcessID   string `json:"processID"`
	TupleDigest string `json:"tupleDigest"`
}

func (f *GCSMutationAckLossFault) awaitPreWireRelease(ctx context.Context, request *gcsMutationAckLossRequest) error {
	f.reportGate(request, "entered", nil, 0)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			f.reportGate(request, "cancelled", err, 0)
			return fmt.Errorf("GCS qualification pre-wire gate cancelled: %w", err)
		}
		_, found, err := f.readPreWireRelease(request)
		if err != nil {
			f.reportGate(request, "invalid-release", err, 0)
			return fmt.Errorf("GCS qualification pre-wire release rejected: %w", err)
		}
		if found {
			if err := ctx.Err(); err != nil {
				f.reportGate(request, "cancelled", err, 0)
				return fmt.Errorf("GCS qualification pre-wire gate cancelled: %w", err)
			}
			if err := os.Remove(f.config.PreWireGate.ReleaseFile); err != nil {
				f.reportGate(request, "release-remove-failed", err, 0)
				return fmt.Errorf("consume GCS qualification pre-wire release: %w", err)
			}
			f.reportGate(request, "released", nil, 0)
			return nil
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
}

func (f *GCSMutationAckLossFault) readPreWireRelease(request *gcsMutationAckLossRequest) (gcsMutationPreWireRelease, bool, error) {
	info, err := os.Lstat(f.config.PreWireGate.ReleaseFile)
	if errors.Is(err, os.ErrNotExist) {
		return gcsMutationPreWireRelease{}, false, nil
	}
	if err != nil {
		return gcsMutationPreWireRelease{}, false, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 || info.Size() <= 0 || info.Size() > 1024 {
		return gcsMutationPreWireRelease{}, false, fmt.Errorf("release file is not a private bounded regular file")
	}
	data, err := os.ReadFile(f.config.PreWireGate.ReleaseFile)
	if err != nil {
		return gcsMutationPreWireRelease{}, false, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var release gcsMutationPreWireRelease
	if err := decoder.Decode(&release); err != nil {
		return gcsMutationPreWireRelease{}, false, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return gcsMutationPreWireRelease{}, false, fmt.Errorf("release file has trailing data")
	}
	if release.Version != 1 || release.Nonce != f.config.PreWireGate.Nonce || release.ProcessID != f.processID || release.TupleDigest != request.tupleDigest() {
		return gcsMutationPreWireRelease{}, false, fmt.Errorf("release does not match the selected nonce, process, and dispatch tuple")
	}
	return release, true, nil
}

func (request *gcsMutationAckLossRequest) tupleDigest() string {
	return hashJSON([]any{request.runtime, request.identity, request.producerUID, request.sequence, request.dispatchID, request.intent, request.preparation, request.scope})
}

func (f *GCSMutationAckLossFault) reportGate(request *gcsMutationAckLossRequest, phase string, err error, status int) {
	reason := ""
	if err != nil {
		reason = err.Error()
	}
	f.mu.Lock()
	wireRequests := request.wireCount
	f.mu.Unlock()
	f.log(GCSMutationAckLossReceipt{
		Version: 1, Selector: f.config.Selector, Reason: reason, GatePhase: phase,
		GateNonce: f.config.PreWireGate.Nonce, ProcessID: f.processID, WireRequests: wireRequests,
		SourceIdentity: request.identity, Runtime: request.runtime, ProducerUID: request.producerUID,
		Sequence: request.sequence, DispatchID: request.dispatchID, OperationID: request.intent.OperationID,
		Intent: request.intent, Preparation: request.preparation, Scope: request.scope, TupleDigest: request.tupleDigest(),
		Bucket: request.scope.Bucket, Prefix: request.scope.Prefix, ObjectKey: request.intent.Key,
		PayloadDigest: request.intent.PayloadDigest, PayloadSize: request.intent.PayloadSize, HTTPStatus: status, ObservedAt: time.Now().UTC(),
	})
}

func (f *GCSMutationAckLossFault) matchesRequest(request *http.Request, armed *gcsMutationAckLossRequest) bool {
	if request == nil || armed == nil || request.Method != http.MethodPost || request.URL == nil || request.URL.Scheme != "https" || request.URL.Host != "storage.googleapis.com" {
		return false
	}
	urlValues := request.URL.Query()
	if request.URL.EscapedPath() != "/upload/storage/v1/b/"+f.config.Bucket+"/o" ||
		!oneQueryValue(urlValues, "uploadType", "multipart") || !oneQueryValue(urlValues, "ifGenerationMatch", "0") ||
		!oneQueryValue(urlValues, "name", path.Join(armed.scope.Prefix, armed.intent.Key)) {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	return err == nil && mediaType == "multipart/related"
}

func oneQueryValue(values map[string][]string, key, want string) bool {
	got, ok := values[key]
	return ok && len(got) == 1 && got[0] == want
}

func (f *GCSMutationAckLossFault) report(request *gcsMutationAckLossRequest, injected bool, reason string) {
	f.mu.Lock()
	wireRequests := request.wireCount
	f.mu.Unlock()
	f.log(GCSMutationAckLossReceipt{
		Version: 1, Selector: f.config.Selector, Injected: injected, Reason: reason, WireRequests: wireRequests,
		SourceIdentity: request.identity, Runtime: request.runtime, ProducerUID: request.producerUID,
		Sequence: request.sequence, DispatchID: request.dispatchID, OperationID: request.intent.OperationID,
		Intent: request.intent, Preparation: request.preparation, Scope: request.scope,
		TupleDigest: hashJSON([]any{request.runtime, request.identity, request.producerUID, request.sequence, request.dispatchID, request.intent, request.preparation, request.scope}),
		Bucket:      request.scope.Bucket, Prefix: request.scope.Prefix, ObjectKey: request.intent.Key,
		PayloadDigest: request.intent.PayloadDigest, PayloadSize: request.intent.PayloadSize, ObservedAt: time.Now().UTC(),
	})
}

var errGCSMutationAckLost = errors.New("GCS qualification: successful upload response was intentionally discarded")
