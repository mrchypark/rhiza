package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

const fencingProtocolAnnotation = "rhiza.mrchypark.dev/cooperative-fencing"
const fencingRootAnnotation = "rhiza.mrchypark.dev/fencing-root-uid"
const fencingCredentialAnnotation = "rhiza.mrchypark.dev/fencing-credential-uid"

// FencingExecutor is a namespaced cooperative authority. Its credentials are
// separate from the operator and application accounts. Applications may request
// only transitions for their TokenReview-authenticated Pod incarnation.
//
// Independent completion uses enrolled runtime/mutation evidence v2 after the
// cooperative ledger is durably closed. The legacy Attestor transport is retained
// for diagnostics, never as an alternative recovery authorization.
type FencingExecutor struct {
	Kube *Kubernetes
	// AuthorityState is canonical ObjectStorage state; Kubernetes resources are
	// only optional indexes and are never consulted to reconstruct authority.
	AuthorityState *AuthorityStateStore
	// ArchiveBucket is the configured shared object store used to independently
	// verify fork evidence before target-root registration.
	ArchiveBucket objstore.Bucket
	// AutomaticControlPrefix matches the operator's canonical automatic-control
	// object prefix. Learner grants require reading that durable control record.
	AutomaticControlPrefix string
	// ArchiveStoreIdentity is the non-secret identity derived from the
	// fencer's configured archive bucket, not from workload environment.
	ArchiveStoreIdentity map[string]string
	// ControllerServiceAccount is the only TokenReview identity allowed to
	// register a nonzero archive fork. Source Pod credentials are never accepted.
	ControllerServiceAccount string
	// LegacyCooperativeControlState explicitly opts into the Kubernetes CAS
	// ledger for cooperative-only operation. It is never a fallback from a
	// configured canonical store and source-mutation APIs still require
	// AuthorityState.
	LegacyCooperativeControlState bool
	// RequireMutationJournal pins this executor to the independently configured
	// source-mutation profile. The persisted root claim is immutable: this option
	// may create a required root, but never upgrades an existing cooperative root.
	RequireMutationJournal bool
	// MutationProfile selects the profile for newly enrolled source producers.
	// Existing root-bound profiles are always preserved on rebind.
	MutationProfile string
	Attestor        FenceAttestor
	// SourceStages is an optional local fallback for isolated tests or development.
	// Production selects root-bound ObjectStorage staging through sourceStagesFor.
	SourceStages *SourceStageStore
	// Source mutations stay unavailable unless the authority receives separate
	// writer credentials and a durable receipt directory.
	SourceMutationStorage  *storage.Client
	SourceMutationReceipts *MutationExecutionReceiptStore
	// GCSMutationAckLoss is an optional, exact-dispatch qualification fault.
	// It is nil in production unless an explicit run-scoped config is supplied.
	GCSMutationAckLoss *GCSMutationAckLossFault
	// Prepared work needs trusted receipt/session verification, not just valid
	// digest strings. A missing provider verifier cannot qualify archived work.
	VerifyMutationEvidence func(context.Context, fencing.RuntimeBinding, uint64, fencing.MutationEntry) error
	// The trusted runtime provider must verify the complete enrolled incarnation
	// and original closure, including descendants and prevention of its return.
	VerifyRuntimeEvidence func(context.Context, fencing.RuntimeBinding, fencing.StopRequest, []byte) error
	// Only a trusted runtime/power provider can collect a terminal receipt. A
	// missing or unreachable provider leaves the operation pending.
	CollectRuntimeEvidence func(context.Context, fencing.RuntimeBinding, fencing.StopRequest) ([]byte, error)
	// observeEmptyRecoveryError is an in-process diagnostic hook for controlled
	// tests. It never changes the HTTP response or authorization decision and is
	// nil in production.
	observeEmptyRecoveryError func(error)
	// RuntimeAgent captures the exact containerd/cgroup incarnation before
	// RegisterEnrolled is durably persisted and before supervisor child startup.
	RuntimeAgent *RuntimeAgentClient
	// manualRecoveryNow is a deterministic clock seam for manual-attestation
	// lifecycle tests. Production leaves it nil and uses wall-clock UTC.
	manualRecoveryNow func() time.Time
}

type fenceLedger struct {
	StatefulSetUID string         `json:"statefulSetUID"`
	Ledger         fencing.Ledger `json:"ledger"`
}

func (e *FencingExecutor) controller() *Controller { return &Controller{Kube: e.Kube} }

func registerStageError(stage string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("fencing: register %s rejected: %w", stage, err)
}

// emitEarlyClosedRegistrationDenial keeps authenticated registration failures
// from repair preflight resolvers on the same structured diagnostic path as
// canonical-root closure discovered later in handle. The grant is unresolved
// at this point by construction.
func (e *FencingExecutor) emitEarlyClosedRegistrationDenial(uid string, identity fencing.Identity) {
	if e == nil || e.Kube == nil {
		return
	}
	_ = emitStartupRegistrationDenial(startupRegistrationDenialEvent{
		Event: startupRegistrationDenialEventName, Action: "register", Result: "closed", HTTPStatus: http.StatusGone,
		Stage: startupRegistrationStageGrant, Namespace: e.Kube.Namespace, StatefulSetUID: uid,
		PodUID: identity.PodUID, SourceClusterID: identity.SourceClusterID,
		AttemptedRunID: identity.RunID, RequestedNodeID: identity.NodeID,
	})
}

func (e *FencingExecutor) ledgerPath(uid, source string) string {
	return e.controller().corePath("configmaps", "rhiza-fence-ledger-"+hashJSON([]string{uid, source})[:40])
}

// change retries only an ObjectStorage generation conflict. The transition is
// pure and must tolerate a response lost after the conditional write commits.
func (e *FencingExecutor) change(ctx context.Context, uid, source string, transition func(*fencing.Ledger) error) (fenceLedger, error) {
	if e.AuthorityState == nil {
		if e != nil && e.RequireMutationJournal {
			return fenceLedger{}, fmt.Errorf("fencing: required mutation journal needs canonical ObjectStorage authority state")
		}
		if e != nil && e.LegacyCooperativeControlState {
			return e.changeLegacyCooperative(ctx, uid, source, transition)
		}
		return fenceLedger{}, fmt.Errorf("fencing: canonical ObjectStorage authority state is not configured")
	}
	for attempt := 0; attempt < 8; attempt++ {
		state, version, err := e.AuthorityState.ReadRoot(ctx, uid, source)
		if err != nil {
			if errors.Is(err, errAuthorityStateNotFound) {
				return fenceLedger{}, fmt.Errorf("fencing: canonical authority root is missing; refusing to bootstrap")
			}
			return fenceLedger{}, err
		}
		if err := transition(&state.Ledger); err != nil {
			return state, err
		}
		writeErr := e.AuthorityState.WriteRoot(ctx, uid, source, state, version)
		if writeErr == nil {
			return state, nil
		}
		if errors.Is(writeErr, errAuthorityStateConflict) {
			continue
		}
		// WriteRoot confirms uncertain responses only when this attempt's fresh
		// revision is present. Logical equality alone can describe an older value.
		return state, writeErr
	}
	return fenceLedger{}, fencing.ErrUnavailable
}

// initializeRoot is called only for authenticated source registration. Other
// transitions never interpret a missing canonical root as permission to open it.
func (e *FencingExecutor) initializeRoot(ctx context.Context, uid, source string) error {
	if e != nil && e.RequireMutationJournal && e.AuthorityState == nil {
		return fmt.Errorf("fencing: required mutation journal needs canonical ObjectStorage authority state")
	}
	if e.AuthorityState == nil {
		if e != nil && e.LegacyCooperativeControlState {
			_, err := e.readLegacyCooperative(ctx, uid, source)
			if apiNotFound(err) {
				return nil
			}
			return err
		}
		return fmt.Errorf("fencing: canonical ObjectStorage authority state is not configured")
	}
	if existing, _, err := e.AuthorityState.ReadRoot(ctx, uid, source); err == nil {
		if err := e.validateMutationJournalProfile(existing); err != nil {
			return err
		}
		_, identity, _ := e.AuthorityState.rootKey(uid, source)
		return e.AuthorityState.ensureRootGenesis(ctx, identity)
	} else if !errors.Is(err, errAuthorityStateNotFound) {
		return err
	}
	// A missing canonical root is not permission to forget an older
	// cooperative ledger for the same StatefulSet/source identity. Check the
	// existing Kubernetes ledger before creating genesis; only an authoritative
	// NotFound permits a genuinely new canonical root. In particular, unreadable
	// or malformed legacy state must fail closed rather than be treated as empty.
	if _, legacyErr := e.readLegacyCooperative(ctx, uid, source); legacyErr == nil {
		return fmt.Errorf("fencing: root-init phase=legacy_check cause=legacy_exists: legacy cooperative ledger exists; refusing canonical root bootstrap")
	} else if !apiNotFound(legacyErr) {
		return errors.Join(fmt.Errorf("fencing: root-init phase=legacy_check cause=legacy_check_unavailable: cannot verify legacy cooperative ledger before canonical root bootstrap"), legacyErr)
	}
	initial := fenceLedger{StatefulSetUID: uid, Ledger: fencing.Ledger{
		SourceClusterID: source, RequireRuntimeBinding: e != nil && e.RequireMutationJournal,
		RequireMutationJournal: e != nil && e.RequireMutationJournal,
		ArchiveGenesisState: func() string {
			if e != nil && e.RequireMutationJournal {
				return fencing.ArchiveGenesisRegistrationPending
			}
			return ""
		}(),
	}}
	err := e.AuthorityState.WriteRoot(ctx, uid, source, initial, nil)
	if err != nil && !errors.Is(err, errAuthorityStateConflict) {
		created, _, readErr := e.AuthorityState.ReadRoot(ctx, uid, source)
		if readErr == nil {
			if profileErr := e.validateMutationJournalProfile(created); profileErr != nil {
				return profileErr
			}
			_, identity, _ := e.AuthorityState.rootKey(uid, source)
			return e.AuthorityState.ensureRootGenesis(ctx, identity)
		}
		return errors.Join(err, readErr)
	}
	created, _, err := e.AuthorityState.ReadRoot(ctx, uid, source)
	if err != nil {
		return err
	}
	if err := e.validateMutationJournalProfile(created); err != nil {
		return err
	}
	_, identity, _ := e.AuthorityState.rootKey(uid, source)
	return e.AuthorityState.ensureRootGenesis(ctx, identity)
}

// rejectClosedCanonicalStartup applies the global generation closure before a
// register request resolves any learner or voter grant. It deliberately does
// not treat per-node closure as generation-wide closure; the final ledger CAS
// remains the admission boundary for races after this read.
func (e *FencingExecutor) rejectClosedCanonicalStartup(ctx context.Context, uid, source string) error {
	if e == nil || e.AuthorityState == nil {
		// Explicit legacy cooperative mode has no canonical root to inspect. Its
		// existing path remains unchanged; configured canonical stores never fall
		// back here after a read error.
		return nil
	}
	state, _, err := e.AuthorityState.ReadRoot(ctx, uid, source)
	if err != nil {
		return fmt.Errorf("fencing: canonical startup authority unavailable: %w", err)
	}
	if state.StatefulSetUID != uid || state.Ledger.SourceClusterID != source {
		return fmt.Errorf("fencing: canonical startup authority identity differs")
	}
	if state.Ledger.ClosedBy == "" && state.Ledger.ClosedHash == "" {
		return nil
	}
	if state.Ledger.ClosedBy == "" || !validEvidenceDigest(state.Ledger.ClosedHash) {
		return fmt.Errorf("fencing: canonical generation closure is malformed")
	}
	return fencing.ErrClosed
}

func (e *FencingExecutor) validateMutationJournalProfile(state fenceLedger) error {
	if e != nil && e.RequireMutationJournal && !fencing.MutationProfileSupported(e.sourceMutationProfile()) {
		return fmt.Errorf("fencing: configured source mutation profile is unsupported")
	}
	if e != nil && e.RequireMutationJournal && !state.Ledger.RequireMutationJournal {
		return fmt.Errorf("fencing: existing authority root is cooperative-only; refusing mutation-journal profile upgrade")
	}
	if state.Ledger.RequireMutationJournal && !state.Ledger.RequireRuntimeBinding {
		return fmt.Errorf("fencing: authority root requires mutation journal without runtime binding")
	}
	if state.Ledger.RequireMutationJournal {
		if state.Ledger.ArchiveGenesisState == fencing.ArchiveGenesisRegistrationPending {
			if state.Ledger.ArchiveScopeHash != "" || state.Ledger.ArchiveGenesisRef != nil || state.Ledger.EmptyRecoveryAuthorization != nil {
				return fmt.Errorf("fencing: pending archive genesis root has unexpected scope state")
			}
		} else if state.Ledger.ArchiveGenesisState == fencing.ArchiveGenesisEmptyRecoveryPending {
			if !validEvidenceDigest(state.Ledger.ArchiveScopeHash) || state.Ledger.ArchiveGenesisRef != nil || state.Ledger.EmptyRecoveryAuthorization != nil {
				return fmt.Errorf("fencing: pending empty recovery root has unexpected scope state")
			}
		} else if state.Ledger.ArchiveGenesisState == fencing.ArchiveGenesisAwaitingRef {
			if !validEvidenceDigest(state.Ledger.ArchiveScopeHash) || state.Ledger.ArchiveGenesisRef != nil {
				return fmt.Errorf("fencing: archive genesis scope claim differs")
			}
		} else if state.Ledger.ArchiveGenesisState == fencing.ArchiveGenesisCommitted {
			if !validEvidenceDigest(state.Ledger.ArchiveScopeHash) || state.Ledger.ArchiveGenesisRef == nil ||
				state.Ledger.ArchiveGenesisRef.ScopeHash != state.Ledger.ArchiveScopeHash || state.Ledger.ArchiveGenesisRef.Validate() != nil {
				return fmt.Errorf("fencing: committed archive genesis reference differs")
			}
		} else {
			return fmt.Errorf("fencing: required archive genesis state is missing")
		}
	} else if state.Ledger.ArchiveGenesisState != "" || state.Ledger.ArchiveScopeHash != "" || state.Ledger.ArchiveGenesisRef != nil || state.Ledger.EmptyRecoveryAuthorization != nil {
		return fmt.Errorf("fencing: cooperative root carries independent archive genesis state")
	}
	if state.Ledger.EmptyRecoveryAuthorization != nil {
		a := state.Ledger.EmptyRecoveryAuthorization
		if err := a.Validate(); err != nil || a.StatefulSetUID != state.StatefulSetUID || a.TargetClusterID != state.Ledger.SourceClusterID ||
			a.TargetScopeHash != state.Ledger.ArchiveScopeHash || !state.Ledger.RequireMutationJournal ||
			(state.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisAwaitingRef && state.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisCommitted) {
			return fmt.Errorf("fencing: root empty recovery authorization differs")
		}
		if state.Ledger.ArchiveGenesisRef != nil && (state.Ledger.ArchiveGenesisRef.Mutation == nil || state.Ledger.ArchiveGenesisRef.Mutation.Intent.PayloadDigest != a.TargetEmptyHeadDigest) {
			return fmt.Errorf("fencing: root empty recovery authorization differs from target genesis mutation")
		}
	}
	return nil
}

func (e *FencingExecutor) read(ctx context.Context, uid, source string) (fenceLedger, error) {
	if e.AuthorityState == nil {
		if e != nil && e.RequireMutationJournal {
			return fenceLedger{}, fmt.Errorf("fencing: required mutation journal needs canonical ObjectStorage authority state")
		}
		if e != nil && e.LegacyCooperativeControlState {
			return e.readLegacyCooperative(ctx, uid, source)
		}
		return fenceLedger{}, fmt.Errorf("fencing: canonical ObjectStorage authority state is not configured")
	}
	state, _, err := e.AuthorityState.ReadRoot(ctx, uid, source)
	if err != nil {
		return fenceLedger{}, err
	}
	return state, nil
}

func (e *FencingExecutor) changeLegacyCooperative(ctx context.Context, uid, source string, transition func(*fencing.Ledger) error) (fenceLedger, error) {
	if e == nil || e.Kube == nil || transition == nil {
		return fenceLedger{}, fmt.Errorf("fencing: legacy cooperative Kubernetes control state is unavailable")
	}
	endpoint := e.ledgerPath(uid, source)
	for attempt := 0; attempt < 8; attempt++ {
		var resource object
		err := e.Kube.Get(ctx, endpoint, &resource)
		creating := apiNotFound(err)
		if err != nil && !creating {
			return fenceLedger{}, err
		}
		state := fenceLedger{StatefulSetUID: uid, Ledger: fencing.Ledger{SourceClusterID: source}}
		if !creating {
			if nested(resource, "metadata", "deletionTimestamp") != nil {
				return state, fmt.Errorf("fencing: ledger deleting")
			}
			if err := json.Unmarshal([]byte(str(nested(resource, "data", "ledger.json"))), &state); err != nil {
				return state, err
			}
			if state.StatefulSetUID != uid || state.Ledger.SourceClusterID != source {
				return state, fmt.Errorf("fencing: ledger identity differs")
			}
		}
		if err := transition(&state.Ledger); err != nil {
			return state, err
		}
		data, err := json.Marshal(state)
		if err != nil {
			return state, err
		}
		if len(data) > 700000 {
			return state, fmt.Errorf("fencing: ledger capacity exhausted; old evidence cannot be discarded")
		}
		if creating {
			_, name, _ := strings.Cut(endpoint, e.controller().corePath("configmaps", "")+"/")
			resource = object{"apiVersion": "v1", "kind": "ConfigMap", "metadata": object{"name": name, "namespace": e.Kube.Namespace}, "data": object{"ledger.json": string(data)}}
			err = e.Kube.Post(ctx, e.controller().corePath("configmaps", ""), resource, nil)
		} else {
			resource["data"] = object{"ledger.json": string(data)}
			err = e.Kube.Put(ctx, endpoint, resource, nil)
		}
		if err == nil {
			return state, nil
		}
		var api *APIError
		if !errors.As(err, &api) || api.StatusCode != http.StatusConflict {
			return state, err
		}
	}
	return fenceLedger{}, fencing.ErrUnavailable
}

func (e *FencingExecutor) readLegacyCooperative(ctx context.Context, uid, source string) (fenceLedger, error) {
	if e == nil || e.Kube == nil {
		return fenceLedger{}, fmt.Errorf("fencing: legacy cooperative Kubernetes control state is unavailable")
	}
	var resource object
	if err := e.Kube.Get(ctx, e.ledgerPath(uid, source), &resource); err != nil {
		return fenceLedger{}, err
	}
	var state fenceLedger
	if nested(resource, "metadata", "deletionTimestamp") != nil {
		return state, fmt.Errorf("fencing: ledger deleting")
	}
	if err := json.Unmarshal([]byte(str(nested(resource, "data", "ledger.json"))), &state); err != nil {
		return state, err
	}
	if state.StatefulSetUID != uid || state.Ledger.SourceClusterID != source {
		return state, fmt.Errorf("fencing: ledger identity differs")
	}
	return state, nil
}

// Handler must be served with TLS. Only audience-bound, Pod-bound service
// account tokens are accepted; arbitrary identity fields are never authority.
func (e *FencingExecutor) Handler() http.Handler { return http.HandlerFunc(e.handle) }

func (e *FencingExecutor) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == httpFenceRequestPath {
		e.handleHTTPFenceRequest(w, r)
		return
	}
	if r.URL.Path == "/v1/archive-genesis/empty-recovery/register" {
		e.handleArchiveEmptyRecovery(w, r)
		return
	}
	if r.URL.Path == frozenSourcePreflightPath {
		e.handleFrozenSourcePreflight(w, r)
		return
	}
	if r.URL.Path == bootstrapGenesisPreflightPath {
		e.handleBootstrapGenesisPreflight(w, r)
		return
	}
	if r.URL.Path == bootstrapGenesisFenceConfirmPath {
		e.handleBootstrapGenesisFenceConfirmation(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/archive-genesis/") {
		e.handleArchiveForkGenesis(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/source-mutations/") {
		e.handleSourceMutation(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/source-stages/") {
		e.handleSourceStage(w, r)
		return
	}
	if r.Method != http.MethodPost || r.URL.Path != "/v1/supervisor" {
		http.NotFound(w, r)
		return
	}
	var message fencing.AuthorityMessage
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536))
	d.DisallowUnknownFields()
	if err := d.Decode(&message); err != nil {
		http.Error(w, "invalid request", 400)
		return
	}
	if err := d.Decode(new(any)); err != io.EOF {
		http.Error(w, "trailing request", 400)
		return
	}
	uid, err := e.authenticate(r.Context(), r.Header.Get("Authorization"), message.Identity)
	if err != nil {
		if transientFenceAuthority(err) {
			http.Error(w, "authority unavailable", http.StatusServiceUnavailable)
			return
		}
		log.Printf("fencing supervisor authentication rejected: action=%s podUID=%s error=%s", safeFenceLogField(message.Action, 128), safeFenceLogField(message.Identity.PodUID, 256), safeFenceLogField(err.Error(), 512))
		http.Error(w, "supervisor identity rejected", http.StatusForbidden)
		return
	}
	if message.PreflightProposal != nil && message.Action != "engine-repair-preflight" {
		http.Error(w, "unexpected engine repair preflight payload", http.StatusBadRequest)
		return
	}
	if message.InitialRegistration && message.Action != "register" {
		http.Error(w, "unexpected initial registration marker", http.StatusBadRequest)
		return
	}
	if message.RegistrationWALIdentity != "" && (message.Action != "register" || message.InitialRegistration || e.AuthorityState == nil) {
		http.Error(w, "unexpected registration continuation", http.StatusBadRequest)
		return
	}
	if message.Action == "engine-repair-preflight" {
		if message.PreflightProposal == nil || message.EngineRepair != nil || message.Drain != nil ||
			message.Request != nil || message.MutationLookup != nil || message.ArchiveGenesis != nil || message.LearnerIdentity != nil {
			http.Error(w, "malformed engine repair preflight", http.StatusBadRequest)
			return
		}
		if err := e.createPendingEngineRepairPreflight(r.Context(), uid, message.Identity, *message.PreflightProposal); err != nil {
			writeEngineRepairStartupError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	if message.Action == "register" && (message.Drain != nil || message.Request != nil || message.MutationLookup != nil || message.ArchiveGenesis != nil || message.LearnerIdentity != nil || message.PreflightProposal != nil) {
		http.Error(w, "malformed registration", http.StatusBadRequest)
		return
	}
	if strings.HasPrefix(message.Action, "engine-repair-") {
		if message.EngineRepair == nil || message.PreflightProposal != nil || message.Drain != nil || message.Request != nil || message.MutationLookup != nil || message.ArchiveGenesis != nil || message.LearnerIdentity != nil {
			http.Error(w, "malformed engine repair request", http.StatusBadRequest)
			return
		}
		response, requestErr := e.engineRepairRunnerRequest(r.Context(), r.Header.Get("Authorization"), uid, message.Identity, *message.EngineRepair, message.Action)
		if requestErr != nil {
			if transientFenceAuthority(requestErr) {
				http.Error(w, "engine repair authority unavailable", http.StatusServiceUnavailable)
			} else if errors.Is(requestErr, fencing.ErrPending) || errors.Is(requestErr, ErrFencePending) {
				w.WriteHeader(http.StatusAccepted)
			} else if errors.Is(requestErr, fencing.ErrClosed) {
				http.Error(w, "engine repair operation closed", http.StatusGone)
			} else {
				http.Error(w, "engine repair request rejected", http.StatusConflict)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
		return
	}
	if message.EngineRepair != nil {
		http.Error(w, "unexpected engine repair request", http.StatusBadRequest)
		return
	}
	var activeRestartGrant *fencing.StartupGrant
	var initialRegistrationGrant *fencing.StartupGrant
	var initialRegistrationProvenance *fencing.InitialRegistrationProvenance
	if message.Action == "register" {
		if message.InitialRegistration {
			grant, provenance, err := e.resolveInitialRegistrationGrant(r.Context(), uid, message.Identity)
			if err != nil {
				writeEngineRepairStartupError(w, err)
				return
			}
			initialRegistrationGrant, initialRegistrationProvenance = &grant, &provenance
		} else if message.RegistrationWALIdentity == "" {
			grant, assigned, restartErr := e.resolveEngineRepairActiveRestartRegistration(r.Context(), uid, message.Identity)
			if restartErr != nil {
				if errors.Is(restartErr, fencing.ErrClosed) {
					e.emitEarlyClosedRegistrationDenial(uid, message.Identity)
				}
				writeEngineRepairStartupError(w, restartErr)
				return
			}
			if assigned {
				activeRestartGrant = &grant
			} else {
				grant, assigned, bootstrapErr := e.resolveEngineRepairBootstrapRegistration(r.Context(), uid, message.Identity)
				if bootstrapErr != nil {
					if errors.Is(bootstrapErr, fencing.ErrClosed) {
						e.emitEarlyClosedRegistrationDenial(uid, message.Identity)
					}
					writeEngineRepairStartupError(w, bootstrapErr)
					return
				}
				if assigned {
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(grant)
					return
				}
			}
		}
	}
	registrationStage := ""
	grantResolved := false
	if message.Action == "register" {
		if initialRegistrationGrant == nil {
			registrationStage = startupRegistrationStageRootInit
			if message.RegistrationWALIdentity != "" {
				// Continuation consumes existing authority; it must never create genesis.
				var root fenceLedger
				root, _, err = e.AuthorityState.ReadRoot(r.Context(), uid, message.Identity.SourceClusterID)
				if err == nil {
					err = e.validateMutationJournalProfile(root)
				}
				err = registerStageError("root-read", err)
			} else {
				// Persist the immutable profile claim before enrollment or admission.
				err = registerStageError("root-init", e.initializeRoot(r.Context(), uid, message.Identity.SourceClusterID))
			}
		}
	}
	startupGrant := fencing.StartupGrant{Identity: message.Identity, Role: fencing.StartupRoleVoterActive}
	if startupGrant.Identity.PodName == "" {
		startupGrant.Identity.PodName = message.Identity.NodeID
	}
	if err == nil && message.Action == "register" {
		registrationStage = startupRegistrationStageGrant
		if initialRegistrationGrant == nil {
			err = registerStageError("startup-grant", e.rejectClosedCanonicalStartup(r.Context(), uid, message.Identity.SourceClusterID))
		}
		if err == nil {
			if activeRestartGrant != nil {
				startupGrant = *activeRestartGrant
			} else if initialRegistrationGrant != nil {
				startupGrant = *initialRegistrationGrant
			} else {
				if message.RegistrationWALIdentity != "" {
					var static bool
					startupGrant, static, err = e.resolveStaticVoterStartupGrant(r.Context(), uid, message.Identity)
					if err == nil && (!static || startupGrant.Identity != message.Identity || startupGrant.Role != fencing.StartupRoleVoterActive) {
						err = fencing.ErrClosed
					}
				} else {
					startupGrant, err = e.resolveStartupGrant(r.Context(), uid, message.Identity)
				}
				if err == nil {
					var required bool
					required, err = e.initialRegistrationMarkerRequired(r.Context(), uid, message.Identity)
					if err == nil && message.RegistrationWALIdentity != "" {
						var root fenceLedger
						root, _, err = e.AuthorityState.ReadRoot(r.Context(), uid, message.Identity.SourceClusterID)
						if err == nil {
							err = validateRegistrationContinuation(&root.Ledger, message.Identity, message.RegistrationWALIdentity, uid)
						}
					} else if err == nil && required {
						err = fmt.Errorf("fencing: initial registration authority evidence required")
					}
				}
				err = registerStageError("startup-grant", err)
			}
			grantResolved = err == nil
		}
	}
	var enrolled *fencing.RuntimeBinding
	if err == nil && message.Action == "register" && e.RuntimeAgent != nil {
		base, enrollErr := e.RuntimeAgent.enrollmentBase(r.Context(), e.Kube.Namespace, uid, startupGrant.Identity)
		if enrollErr == nil {
			var binding fencing.RuntimeBinding
			binding, enrollErr = e.RuntimeAgent.Enroll(r.Context(), base)
			if enrollErr == nil {
				enrolled = &binding
			}
		}
		if enrollErr != nil {
			log.Printf("runtime enrollment pending: podUID=%s runID=%s error=%s", safeFenceLogField(message.Identity.PodUID, 256), safeFenceLogField(message.Identity.RunID, 128), safeFenceLogField(enrollErr.Error(), 512))
			http.Error(w, "runtime enrollment unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	if message.Action == "learner-identity" {
		if message.Drain != nil || message.Request != nil || message.MutationLookup != nil || message.ArchiveGenesis != nil || message.LearnerIdentity == nil {
			http.Error(w, "malformed learner identity registration", http.StatusBadRequest)
			return
		}
		if err == nil {
			var engineOwned bool
			engineOwned, err = e.registerEngineRepairLearnerIdentity(r.Context(), uid, message.Identity, *message.LearnerIdentity)
			if err == nil && !engineOwned {
				err = e.registerPendingLearnerIdentity(r.Context(), uid, message.Identity, *message.LearnerIdentity)
			}
		}
		if errors.Is(err, fencing.ErrPending) || errors.Is(err, ErrFencePending) {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		if errors.Is(err, fencing.ErrClosed) {
			http.Error(w, "learner identity registration closed", http.StatusGone)
			return
		}
		if err != nil {
			if transientFenceAuthority(err) {
				http.Error(w, "authority unavailable", http.StatusServiceUnavailable)
				return
			}
			log.Printf("fencing learner identity registration rejected: podUID=%s error=%s", safeFenceLogField(message.Identity.PodUID, 256), safeFenceLogField(err.Error(), 512))
			http.Error(w, "learner identity registration rejected", http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}\n"))
		return
	}
	var stop fencing.StopRequest
	var registrationPredecessor *fencing.Run
	if err == nil && message.Action == "register" && message.RegistrationWALIdentity != "" {
		registrationPredecessor, err = e.prepareRegistrationPredecessor(r.Context(), uid, message.Identity, message.RegistrationWALIdentity)
	}
	var binding fencing.BindReceipt
	var mutationLookup fencing.MutationLookupResult
	if message.Action == "mutation-lookup" {
		if message.MutationLookup == nil || message.Drain != nil || message.Request != nil {
			err = fmt.Errorf("fencing: malformed mutation lookup request")
		} else {
			mutationLookup, err = e.lookupMutation(r.Context(), uid, message.Identity, *message.MutationLookup)
		}
	} else if message.MutationLookup != nil {
		err = fmt.Errorf("fencing: unexpected mutation lookup request")
	} else if message.Action == "bind" {
		var engineOwned bool
		binding, engineOwned, err = e.bindEngineRepairCandidate(r.Context(), uid, message.Identity)
		if err == nil && !engineOwned {
			binding, err = e.bindSupervisorWAL(r.Context(), uid, message.Identity)
		}
	} else if message.Action == "promote-learner" {
		if message.Drain != nil || message.Request != nil || message.MutationLookup != nil || message.ArchiveGenesis != nil || message.LearnerIdentity != nil {
			err = fmt.Errorf("fencing: malformed learner promotion request")
		} else {
			binding, err = e.promoteSupervisorLearner(r.Context(), uid, message.Identity)
		}
	} else if message.Action == "stop" {
		var state fenceLedger
		state, err = e.read(r.Context(), uid, message.Identity.SourceClusterID)
		if err == nil {
			run, exists := state.Ledger.Runs[message.Identity.RunID]
			if !exists || run.Identity != message.Identity {
				err = fmt.Errorf("fencing: incarnation differs")
			} else if run.Stop == nil {
				err = fencing.ErrPending
			} else {
				stop = *run.Stop
			}
		}
	} else {
		if err == nil {
			if message.Action == "register" {
				registrationStage = startupRegistrationStageLedgerRegister
			}
			var changed fenceLedger
			changed, err = e.change(r.Context(), uid, message.Identity.SourceClusterID, func(l *fencing.Ledger) error {
				switch message.Action {
				case "register":
					if message.RegistrationWALIdentity != "" {
						if enrolled == nil {
							return fmt.Errorf("fencing: registration continuation requires runtime enrollment")
						}
						if err := validateRegistrationContinuation(l, message.Identity, message.RegistrationWALIdentity, uid); err != nil {
							return err
						}
						if err := validatePreparedRegistrationPredecessor(l, registrationPredecessor); err != nil {
							return err
						}
					}
					if e.RequireMutationJournal && !l.RequireMutationJournal {
						return fmt.Errorf("fencing: authority root does not require mutation journal")
					}
					if l.RequireMutationJournal {
						if !l.RequireRuntimeBinding {
							return fmt.Errorf("fencing: authority root requires mutation journal without runtime binding")
						}
						if enrolled == nil {
							return fmt.Errorf("fencing: runtime agent is required for mutation-journal registration")
						}
					}
					if activeRestartGrant != nil {
						return nil
					}
					if enrolled != nil {
						l.RequireRuntimeBinding = true
						if message.RegistrationWALIdentity != "" {
							boundHistory := false
							for _, prior := range l.Runs {
								if prior.Identity.NodeID == message.Identity.NodeID && prior.Identity.WALIdentity != "" {
									boundHistory = true
								}
							}
							if !boundHistory {
								return l.ContinueUnboundRegistration(startupGrant.Identity, *enrolled, message.RegistrationWALIdentity)
							}
						}
						if initialRegistrationProvenance != nil {
							ordinal, ordinalErr := engineRepairPodOrdinal(startupGrant.Identity.PodName)
							if ordinalErr != nil {
								return fencing.ErrClosed
							}
							if err := validateInitialRegistrationLedger(l, startupGrant.Identity, *initialRegistrationProvenance, ordinal, uid); err != nil {
								return err
							}
							if err := l.RegisterEnrolledWithRole(startupGrant.Identity, *enrolled, startupGrant.Role); err != nil {
								return registerStageError("ledger-register", err)
							}
							run := l.Runs[startupGrant.Identity.RunID]
							provenance := *initialRegistrationProvenance
							run.InitialRegistration = &provenance
							l.Runs[startupGrant.Identity.RunID] = run
							return nil
						}
						if err := l.RegisterEnrolledWithRole(startupGrant.Identity, *enrolled, startupGrant.Role); err != nil {
							return registerStageError("ledger-register", err)
						}
						return nil
					}
					if l.RequireRuntimeBinding {
						return fmt.Errorf("fencing: runtime agent is required")
					}
					return registerStageError("ledger-register", l.RegisterWithRole(startupGrant.Identity, startupGrant.Role))
				case "drain":
					if message.Drain == nil || message.Drain.Request.Identity != message.Identity {
						return fmt.Errorf("fencing: drain binding differs")
					}
					return l.RecordDrain(*message.Drain)
				case "exit":
					if message.Request == nil || message.Request.Identity != message.Identity {
						return fmt.Errorf("fencing: exit binding differs")
					}
					return l.RecordExit(*message.Request)
				case "never-bound-exit":
					if message.Request == nil || message.Request.Identity != message.Identity {
						return fmt.Errorf("fencing: never-bound exit binding differs")
					}
					return l.RecordNeverBoundExit(*message.Request)
				default:
					return fmt.Errorf("fencing: unknown action")
				}
			})
			if err == nil && message.Action == "register" && changed.Ledger.RequireMutationJournal && startupGrant.Role == fencing.StartupRoleVoterActive {
				if enrolled == nil {
					err = fmt.Errorf("fencing: required archive genesis registration lacks runtime binding")
				} else {
					registrationStage = startupRegistrationStageProducerReserve
					_, err = e.reserveMutationProducer(r.Context(), *enrolled)
					err = registerStageError("producer-reserve", err)
					if err == nil {
						registrationStage = startupRegistrationStageArchiveScope
						err = registerStageError("archive-scope", e.claimArchiveScopeForRegistration(r.Context(), uid, startupGrant.Identity, *enrolled))
					}
				}
			}
		}
	}
	if err == nil && message.Action == "register" && activeRestartGrant != nil {
		if enrolled == nil {
			err = fmt.Errorf("fencing: active restart grant lacks trusted runtime enrollment")
		} else {
			finalState, finalErr := e.read(r.Context(), uid, message.Identity.SourceClusterID)
			if finalErr != nil {
				err = finalErr
			} else {
				run, exists := finalState.Ledger.Runs[startupGrant.Identity.RunID]
				if finalState.Ledger.ClosedBy != "" || finalState.Ledger.ClosedHash != "" || !exists || run.Runtime == nil || run.EngineRepairProvenance == nil ||
					run.Identity.RunID != startupGrant.Identity.RunID || *run.Runtime != *enrolled {
					err = fencing.ErrClosed
				} else {
					store, storeErr := NewEngineRepairStore(e.AuthorityState, uid)
					if storeErr != nil {
						err = storeErr
					} else {
						record, current, currentRuntime, currentErr := store.ReadEngineRepairCurrentCandidate(r.Context(), message.Identity.SourceClusterID, run.EngineRepairProvenance.OperationID)
						if currentErr != nil || current != run.Identity || currentRuntime != *enrolled {
							err = errors.Join(currentErr, fencing.ErrClosed)
						} else {
							err = e.verifyEngineRepairActiveRestartCurrent(r.Context(), uid, record, current, currentRuntime)
						}
					}
				}
			}
		}
	}
	if errors.Is(err, fencing.ErrPending) || errors.Is(err, ErrFencePending) {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if errors.Is(err, fencing.ErrClosed) {
		if message.Action == "register" {
			var runtimeContainerID string
			if enrolled != nil {
				runtimeContainerID = enrolled.ContainerID
			}
			_ = emitStartupRegistrationDenial(startupRegistrationDenialEvent{
				Event: startupRegistrationDenialEventName, Action: "register", Result: "closed", HTTPStatus: http.StatusGone,
				Stage: registrationStage, Namespace: e.Kube.Namespace, StatefulSetUID: uid, PodUID: message.Identity.PodUID,
				SourceClusterID: message.Identity.SourceClusterID, AttemptedRunID: message.Identity.RunID,
				RequestedNodeID: message.Identity.NodeID, GrantResolved: grantResolved,
				ResolvedNodeID: func() string {
					if grantResolved {
						return startupGrant.Identity.NodeID
					}
					return ""
				}(),
				AssignmentID: func() string {
					if grantResolved {
						return startupGrant.Identity.AssignmentID
					}
					return ""
				}(),
				RuntimeContainerID: runtimeContainerID,
			})
		}
		w.WriteHeader(http.StatusGone)
		return
	}
	if err != nil {
		if transientFenceAuthority(err) {
			if message.Action == "register" && strings.HasPrefix(err.Error(), "fencing: register root-init rejected: fencing: root-init phase=") {
				log.Printf("fencing supervisor transition rejected: action=%s podUID=%s error=%s", safeFenceLogField(message.Action, 128), safeFenceLogField(message.Identity.PodUID, 256), safeFenceLogField(err.Error(), 512))
			}
			http.Error(w, "authority unavailable", http.StatusServiceUnavailable)
			return
		}
		log.Printf("fencing supervisor transition rejected: action=%s podUID=%s error=%s", safeFenceLogField(message.Action, 128), safeFenceLogField(message.Identity.PodUID, 256), safeFenceLogField(err.Error(), 512))
		http.Error(w, "fencing transition rejected", http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if message.Action == "stop" {
		_ = json.NewEncoder(w).Encode(stop)
	} else if message.Action == "register" {
		_ = json.NewEncoder(w).Encode(startupGrant)
	} else if message.Action == "bind" || message.Action == "promote-learner" {
		_ = json.NewEncoder(w).Encode(binding)
	} else if message.Action == "mutation-lookup" {
		_ = json.NewEncoder(w).Encode(mutationLookup)
	} else {
		_, _ = w.Write([]byte("{}\n"))
	}
}

func transientFenceAuthority(err error) bool {
	if errors.Is(err, fencing.ErrUnavailable) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var api *APIError
	if errors.As(err, &api) && (api.StatusCode >= 500 || api.StatusCode == 429 || api.StatusCode == 409) {
		return true
	}
	var network net.Error
	return errors.As(err, &network)
}

func (e *FencingExecutor) authenticate(ctx context.Context, authorization string, id fencing.Identity) (string, error) {
	token, ok := strings.CutPrefix(authorization, "Bearer ")
	if !ok || token == "" || id.PodUID == "" || id.RunID == "" || id.SourceClusterID == "" {
		return "", fmt.Errorf("fencing: bound token required")
	}
	var review object
	err := e.Kube.Post(ctx, "/apis/authentication.k8s.io/v1/tokenreviews", object{"apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview", "spec": object{"token": token, "audiences": []string{"rhiza-fencer"}}}, &review)
	if err != nil {
		return "", err
	}
	if nested(review, "status", "authenticated") != true {
		return "", fmt.Errorf("fencing: unauthenticated token")
	}
	audienceOK := false
	for _, audience := range list(nested(review, "status", "audiences")) {
		if str(audience) == "rhiza-fencer" {
			audienceOK = true
		}
	}
	if !audienceOK {
		return "", fmt.Errorf("fencing: token audience differs")
	}
	extra := asObject(nested(review, "status", "user", "extra"))
	first := func(key string) string {
		v := list(extra[key])
		if len(v) != 1 {
			return ""
		}
		return str(v[0])
	}
	if first("authentication.kubernetes.io/pod-uid") != id.PodUID {
		return "", fmt.Errorf("fencing: token Pod UID differs")
	}
	name := first("authentication.kubernetes.io/pod-name")
	claimedPodName := id.PodName
	if claimedPodName == "" {
		claimedPodName = id.NodeID
	}
	if name == "" || claimedPodName != name {
		return "", fmt.Errorf("fencing: authenticated physical Pod name differs")
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", name), &pod); err != nil {
		return "", err
	}
	if str(nested(pod, "metadata", "uid")) != id.PodUID || str(nested(pod, "metadata", "name")) != name || nested(pod, "metadata", "deletionTimestamp") != nil {
		return "", fmt.Errorf("fencing: Pod incarnation differs")
	}
	sa := str(nested(pod, "spec", "serviceAccountName"))
	if sa == "" {
		sa = "default"
	}
	if str(nested(review, "status", "user", "username")) != "system:serviceaccount:"+e.Kube.Namespace+":"+sa {
		return "", fmt.Errorf("fencing: token namespace differs")
	}
	if err := validateSupervisedPod(pod); err != nil {
		return "", err
	}
	var uid, ownerName string
	for _, ref := range list(nested(pod, "metadata", "ownerReferences")) {
		owner := asObject(ref)
		if str(owner["kind"]) == "StatefulSet" && (owner["controller"] == true || str(nested(pod, "metadata", "annotations", fencingRootAnnotation)) != "") {
			uid, ownerName = str(owner["uid"]), str(owner["name"])
		}
	}
	if uid == "" || ownerName == "" {
		return "", fmt.Errorf("fencing: StatefulSet controller required")
	}
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(ownerName), &sts); err != nil {
		return "", err
	}
	if str(nested(sts, "metadata", "uid")) != uid {
		return "", fmt.Errorf("fencing: workload UID differs")
	}
	if str(nested(pod, "metadata", "labels", "rhiza.mrchypark.dev/automatic-learner")) != "" {
		if err := e.validateLearnerBinding(ctx, pod, ownerName); err != nil {
			return "", err
		}
	}
	ctr := asObject(list(nested(pod, "spec", "containers"))[0])
	env, err := e.controller().environment(ctx, ctr)
	if err != nil || env["RHIZA_CLUSTER_ID"] != id.SourceClusterID {
		return "", fmt.Errorf("fencing: Pod source differs")
	}
	return uid, nil
}

func (e *FencingExecutor) validateLearnerBinding(ctx context.Context, pod object, stsName string) error {
	name := str(nested(pod, "metadata", "name"))
	operation := str(nested(pod, "metadata", "annotations", ownerAnnotation))
	root := str(nested(pod, "metadata", "annotations", fencingRootAnnotation))
	if operation == "" || root == "" || str(nested(pod, "metadata", "labels", "rhiza.mrchypark.dev/automatic-learner")) != name {
		return fmt.Errorf("fencing: learner binding missing")
	}
	var roots struct {
		Items []ClusterResource `json:"items"`
	}
	if err := e.Kube.Get(ctx, "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/"+url.PathEscape(e.Kube.Namespace)+"/rhizaclusters", &roots); err != nil {
		return err
	}
	found := false
	for _, candidate := range roots.Items {
		if str(candidate.Metadata["uid"]) == root && candidate.Spec.StatefulSet == stsName && candidate.Metadata["deletionTimestamp"] == nil {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("fencing: learner root binding differs")
	}
	var secret object
	if err := e.Kube.Get(ctx, e.controller().corePath("secrets", name+"-credentials"), &secret); err != nil {
		return err
	}
	if secret["immutable"] != true || str(nested(secret, "metadata", "uid")) != str(nested(pod, "metadata", "annotations", fencingCredentialAnnotation)) || str(nested(secret, "metadata", "annotations", ownerAnnotation)) != operation {
		return fmt.Errorf("fencing: learner credentials changed")
	}
	data, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "member")))
	if err != nil {
		return err
	}
	var member quepaxa.Member
	if err := json.Unmarshal(data, &member); err != nil || string(member.ID) != name || member.PublicKey == (quepaxa.PublicKey{}) {
		return fmt.Errorf("fencing: learner member differs")
	}
	return nil
}

type learnerVoterIdentityRecord struct {
	Version          int    `json:"version"`
	Cluster          string `json:"cluster"`
	Node             string `json:"node"`
	Membership       string `json:"membership"`
	Store            string `json:"store"`
	Nonce            string `json:"nonce"`
	Registered       bool   `json:"registered"`
	LearnerPublicKey string `json:"learner_public_key,omitempty"`
}

// registerPendingLearnerIdentity is deliberately not a general mutation API.
// It permits exactly the immutable per-node voter identity object, after the
// authenticated pending learner has bound its WAL, and only if its bytes agree
// with the live Pod configuration and immutable credential.
func (e *FencingExecutor) registerPendingLearnerIdentity(ctx context.Context, statefulSetUID string, id fencing.Identity, registration fencing.LearnerIdentityRegistration) error {
	if e.ArchiveBucket == nil || !slices.Contains(e.ArchiveBucket.SupportedObjectUploadOptions(), objstore.IfNotExists) ||
		id.NodeID == "" || id.PodName == "" || id.PodUID == "" || id.RunID == "" || id.WALIdentity == "" ||
		len(registration.Data) == 0 || len(registration.Data) > 64<<10 {
		return fmt.Errorf("fencing: pending learner identity registration is incomplete")
	}
	role, err := e.resolveStartupRole(ctx, statefulSetUID, id)
	if err != nil || role != fencing.StartupRoleLearnerPendingRemove {
		return errors.Join(err, fmt.Errorf("fencing: canonical pending learner assignment is unavailable"))
	}
	state, err := e.read(ctx, statefulSetUID, id.SourceClusterID)
	if err != nil {
		return err
	}
	run, ok := state.Ledger.Runs[id.RunID]
	if !ok || run.Identity != id || run.Stop != nil || run.Runtime == nil || run.Runtime.Validate(id) != nil ||
		fencing.RunRole(run) != fencing.StartupRoleLearnerPendingRemove {
		return fmt.Errorf("fencing: learner run is not pending and WAL-bound")
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", id.PodName), &pod); err != nil {
		return err
	}
	ctr := asObject(list(nested(pod, "spec", "containers"))[0])
	env, err := e.controller().environment(ctx, ctr)
	if err != nil {
		return err
	}
	members, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
	if err != nil || len(members) < 2 {
		return fmt.Errorf("fencing: learner source membership is unavailable")
	}
	durability := env["RHIZA_OBJSTORE_DURABILITY"]
	if durability == "" {
		durability = "async"
	}
	memberRecord := recovery.NewMembershipRecord(id.SourceClusterID, members, durability)
	provider := env["RHIZA_OBJSTORE_PROVIDER"]
	if provider == "" {
		provider = "filesystem"
	}
	storeData, _ := json.Marshal([]string{provider, env["RHIZA_OBJSTORE_ENDPOINT"], env["RHIZA_OBJSTORE_BUCKET"], path.Clean(env["RHIZA_OBJSTORE_PREFIX"]), env["RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT"]})
	storeDigest := sha256.Sum256(storeData)
	secretName := id.PodName + "-credentials"
	var assignedGrant *fencing.StartupGrant
	if id.AssignmentID != "" {
		grant, assigned, grantErr := e.resolveSameOrdinalStartupGrant(ctx, statefulSetUID, id)
		if grantErr != nil || !assigned || grant.Identity.AssignmentID != id.AssignmentID {
			return errors.Join(grantErr, fmt.Errorf("fencing: same-ordinal credential assignment is unavailable"))
		}
		secretName = sameOrdinalSecretName(str(nested(pod, "metadata", "annotations", fencingRootAnnotation)), id.PodUID)
		assignedGrant = &grant
	}
	var secret object
	if err := e.Kube.Get(ctx, e.controller().corePath("secrets", secretName), &secret); err != nil {
		return err
	}
	if secret["immutable"] != true || id.AssignmentID == "" && str(nested(secret, "metadata", "annotations", ownerAnnotation)) != str(nested(pod, "metadata", "annotations", ownerAnnotation)) ||
		id.AssignmentID == "" && str(nested(secret, "metadata", "uid")) != str(nested(pod, "metadata", "annotations", fencingCredentialAnnotation)) ||
		id.AssignmentID != "" && (str(nested(secret, "metadata", "uid")) == "" || str(nested(secret, "metadata", "resourceVersion")) == "") {
		return fmt.Errorf("fencing: learner credential identity changed")
	}
	memberBytes, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "member")))
	if err != nil {
		return fmt.Errorf("fencing: learner credential is invalid")
	}
	var member quepaxa.Member
	decoder := json.NewDecoder(bytes.NewReader(memberBytes))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&member) != nil || member.ID != quepaxa.NodeID(id.NodeID) || member.PublicKey == (quepaxa.PublicKey{}) {
		return fmt.Errorf("fencing: learner credential differs from its identity")
	}
	if assignedGrant != nil {
		canonicalGrant, assigned, grantErr := e.resolveSameOrdinalStartupGrant(ctx, statefulSetUID, id)
		if grantErr != nil || !assigned || canonicalGrant.Identity.AssignmentID != id.AssignmentID ||
			string(memberBytes) != canonicalGrant.Environment["RHIZA_LEARNER"] ||
			str(nested(secret, "data", "RHIZA_PEER_TOKEN")) != base64.StdEncoding.EncodeToString([]byte(canonicalGrant.Environment["RHIZA_PEER_TOKEN"])) {
			return errors.Join(grantErr, fmt.Errorf("fencing: same-ordinal credential changed during identity registration"))
		}
	}
	want := learnerVoterIdentityRecord{Version: 2, Cluster: id.SourceClusterID, Node: id.NodeID,
		Membership: memberRecord.Membership, Store: hex.EncodeToString(storeDigest[:]), Nonce: id.WALIdentity,
		Registered: true, LearnerPublicKey: fmt.Sprintf("%x", member.PublicKey[:])}
	var got learnerVoterIdentityRecord
	decoder = json.NewDecoder(bytes.NewReader(registration.Data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&got) != nil || decoder.Decode(new(any)) != io.EOF || got != want {
		return fmt.Errorf("fencing: learner identity payload differs from canonical configuration")
	}
	canonical, _ := json.Marshal(want)
	if !bytes.Equal(canonical, registration.Data) {
		return fmt.Errorf("fencing: learner identity payload is not canonical")
	}
	key := path.Join(env["RHIZA_OBJSTORE_PREFIX"], id.SourceClusterID, "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(id.NodeID))))
	if registration.Key != key {
		return fmt.Errorf("fencing: learner identity key differs")
	}
	if existing, readErr := readAuthorityObject(ctx, e.ArchiveBucket, key); readErr == nil {
		if !bytes.Equal(existing, registration.Data) {
			return fmt.Errorf("fencing: immutable learner identity already differs")
		}
		return nil
	} else if !e.ArchiveBucket.IsObjNotFoundErr(readErr) {
		return readErr
	}
	writeErr := e.ArchiveBucket.Upload(ctx, key, bytes.NewReader(registration.Data), objstore.WithIfNotExists())
	if writeErr != nil && !e.ArchiveBucket.IsConditionNotMetErr(writeErr) {
		return writeErr
	}
	readback, readErr := readAuthorityObject(ctx, e.ArchiveBucket, key)
	if readErr != nil || !bytes.Equal(readback, registration.Data) {
		return errors.Join(readErr, fmt.Errorf("fencing: learner identity conditional-write readback differs"))
	}
	return nil
}

func readAuthorityObject(ctx context.Context, bucket objstore.Bucket, key string) ([]byte, error) {
	r, err := bucket.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(r, (64<<10)+1))
	closeErr := r.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	if len(data) > 64<<10 {
		return nil, fmt.Errorf("fencing: learner identity object exceeds capacity")
	}
	return data, nil
}

// resolveStartupRole grants the non-voting learner role only from the
// authenticated Pod plus the operator's versioned canonical automatic-control
// record and its exact persisted recovery child. Any ambiguous/missing record
// remains unavailable; metadata labels and annotations are never authority.
func (e *FencingExecutor) resolveStartupRole(ctx context.Context, statefulSetUID string, id fencing.Identity) (fencing.StartupRole, error) {
	if repair, found, err := e.engineRepairRecordForRuntime(ctx, statefulSetUID, id); err != nil {
		return "", err
	} else if found {
		state, err := e.read(ctx, statefulSetUID, id.SourceClusterID)
		if err != nil {
			return "", err
		}
		candidate := repair.CurrentCandidateIdentity()
		runtime := repair.CurrentCandidateRuntime()
		run, ok := state.Ledger.Runs[id.RunID]
		if !ok || id != candidate || run.Identity != id || run.Runtime == nil || runtime == nil ||
			*run.Runtime != *runtime || run.Stop != nil {
			return "", fencing.ErrClosed
		}
		role := fencing.RunRole(run)
		if repair.Phase == fencing.RepairAdding && (role == fencing.StartupRoleLearnerPendingRemove || role == fencing.StartupRoleVoterActive) ||
			repair.Phase == fencing.RepairActive && role == fencing.StartupRoleVoterActive {
			return role, nil
		}
		return "", fencing.ErrClosed
	}
	grant, err := e.resolveStartupGrant(ctx, statefulSetUID, id)
	return grant.Role, err
}

func (e *FencingExecutor) resolveLegacyStartupRole(ctx context.Context, statefulSetUID string, id fencing.Identity) (fencing.StartupRole, error) {
	role := fencing.StartupRoleVoterActive
	if e == nil || e.Kube == nil {
		return "", fmt.Errorf("fencing: startup authority unavailable")
	}
	podName := id.PodName
	if podName == "" {
		podName = id.NodeID
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", podName), &pod); err != nil {
		return "", err
	}
	if str(nested(pod, "metadata", "uid")) != id.PodUID || str(nested(pod, "metadata", "name")) != podName || nested(pod, "metadata", "deletionTimestamp") != nil {
		return "", fmt.Errorf("fencing: startup Pod incarnation changed")
	}
	if str(nested(pod, "metadata", "labels", "rhiza.mrchypark.dev/automatic-learner")) == "" {
		return role, nil
	}
	if str(nested(pod, "metadata", "labels", "rhiza.mrchypark.dev/automatic-learner")) != podName {
		return "", fmt.Errorf("fencing: learner identity label differs")
	}
	if id.NodeID != podName {
		return "", fmt.Errorf("fencing: learner logical identity is not yet assigned by canonical authority")
	}
	if e.ArchiveBucket == nil {
		return "", fmt.Errorf("fencing: canonical automatic control store is unavailable")
	}
	ownerName := ""
	for _, ref := range list(nested(pod, "metadata", "ownerReferences")) {
		owner := asObject(ref)
		if str(owner["kind"]) == "StatefulSet" && str(owner["uid"]) == statefulSetUID && owner["controller"] == false {
			ownerName = str(owner["name"])
		}
	}
	if ownerName == "" {
		return "", fmt.Errorf("fencing: learner StatefulSet owner differs")
	}
	if err := e.validateLearnerBinding(ctx, pod, ownerName); err != nil {
		return "", err
	}
	var roots struct {
		Items []ClusterResource `json:"items"`
	}
	if err := e.Kube.Get(ctx, "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/"+url.PathEscape(e.Kube.Namespace)+"/rhizaclusters", &roots); err != nil {
		return "", err
	}
	var root *ClusterResource
	for i := range roots.Items {
		candidate := &roots.Items[i]
		if str(candidate.Metadata["uid"]) == str(nested(pod, "metadata", "annotations", fencingRootAnnotation)) &&
			candidate.Spec.StatefulSet == ownerName && candidate.Spec.Automatic && candidate.Spec.AdoptClusterID == id.SourceClusterID && candidate.Spec.LogicalID != "" && candidate.Metadata["deletionTimestamp"] == nil {
			if root != nil {
				return "", fmt.Errorf("fencing: learner root identity is ambiguous")
			}
			root = candidate
		}
	}
	if root == nil {
		return "", fmt.Errorf("fencing: learner automatic root is unavailable")
	}
	c := &Controller{Kube: e.Kube, Bucket: e.ArchiveBucket, Prefix: e.AutomaticControlPrefix}
	state, _, err := c.loadAuto(ctx, c.autoKey(root))
	if err != nil {
		return "", fmt.Errorf("fencing: canonical learner control record unavailable: %w", err)
	}
	currentBinding := autoBinding(root)
	if state.BindingHash != currentBinding && (state.LegacyBindingHash == "" || state.LegacyBindingHash != currentBinding) {
		return "", fmt.Errorf("fencing: automatic learner binding hash differs")
	}
	intent := state.Intent
	if state.BindingUID != str(root.Metadata["uid"]) || state.StatefulSetUID != statefulSetUID || state.ActiveClusterID != id.SourceClusterID || intent == nil || intent.Aborting || intent.Proof == nil || intent.Failed == "" || intent.LearnerName != podName || intent.LearnerUID != id.PodUID || intent.RecoveryName == "" || ((intent.RecoveryUID == "") != (intent.SpecHash == "")) {
		return "", fmt.Errorf("fencing: learner is not in a durable pending-removal assignment")
	}
	failedTarget, targetKnown := state.Identities[intent.Failed]
	if !targetKnown || failedTarget.NodeID != string(intent.Failed) || intent.Request.BindingUID != state.BindingUID || intent.Request.SourceClusterID != state.ActiveClusterID || intent.Request.StatefulSetUID != state.StatefulSetUID || intent.Request.Scope != ScopeVoter || len(intent.Request.Targets) != 1 || intent.Request.Targets[0] != failedTarget || validateFenceProof(intent.Request, intent.Proof) != nil {
		return "", fmt.Errorf("fencing: learner removal proof is not bound to the canonical cohort")
	}
	if err := e.validateCanonicalLearnerFence(ctx, intent.Request, intent.Proof, failedTarget); err != nil {
		return "", err
	}
	var child Resource
	err = e.Kube.Get(ctx, c.resourcePath(intent.RecoveryName), &child)
	if intent.RecoveryUID == "" {
		if err == nil || !apiNotFound(err) {
			return "", fmt.Errorf("fencing: unjournaled learner recovery child is ambiguous")
		}
	} else {
		if err != nil {
			return "", err
		}
		if str(child.Metadata["uid"]) != intent.RecoveryUID || hashJSON(child.Spec) != intent.SpecHash || child.Spec.Membership == nil || child.Spec.Membership.AbortAddition || child.Spec.Membership.OperationID != intent.RecoveryName || child.Spec.Membership.Remove != intent.Failed || child.Spec.Membership.ReplacementPod != podName || child.Spec.Membership.ReplacementSecret != podName+"-credentials" || string(child.Spec.Membership.Fence.NodeID) != string(intent.Failed) || child.Spec.Membership.Fence.WALIdentity != failedTarget.WALIdentity || child.Spec.Membership.Fence.WorkloadUID != failedTarget.PodUID || !child.Spec.Membership.Fence.Confirmed || child.Spec.Membership.Fence.Evidence != intent.Proof.ProofID {
			return "", fmt.Errorf("fencing: learner recovery child differs from canonical intent")
		}
		if child.Status.Phase == "Complete" || (child.Status.Membership != nil && child.Status.Membership.Phase == "Complete") {
			var sts object
			if err := e.Kube.Get(ctx, c.stsPath(root.Spec.StatefulSet), &sts); err != nil {
				return "", err
			}
			var installed installedGeneration
			if err := c.verifyAutomaticCompletion(ctx, root, state, &child, sts, &installed); err != nil {
				return "", fmt.Errorf("fencing: completed learner membership is not canonical: %w", err)
			}
			return fencing.StartupRoleVoterActive, nil
		}
	}
	if str(nested(pod, "metadata", "annotations", ownerAnnotation)) != intent.Request.OperationID {
		return "", fmt.Errorf("fencing: learner operation ownership differs")
	}
	return fencing.StartupRoleLearnerPendingRemove, nil
}

func (e *FencingExecutor) validateCanonicalLearnerFence(ctx context.Context, request FenceRequest, proof *FenceProof, target FenceTarget) error {
	if e == nil || e.AuthorityState == nil || proof == nil || proof.IndependentV2 == nil {
		return fmt.Errorf("fencing: canonical terminal authority is unavailable")
	}
	requestDigest, err := requestHash(&request)
	if err != nil {
		return err
	}
	canonical, _, err := e.AuthorityState.ReadRoot(ctx, request.StatefulSetUID, request.SourceClusterID)
	if err != nil {
		return fmt.Errorf("fencing: canonical terminal authority is unavailable: %w", err)
	}
	closed, ok := canonical.Ledger.ClosedNodes[target.NodeID]
	if !ok || closed.OperationID != request.OperationID || closed.RequestHash != requestDigest ||
		canonical.StatefulSetUID != request.StatefulSetUID || canonical.Ledger.SourceClusterID != request.SourceClusterID {
		return fmt.Errorf("fencing: canonical old-voter closure differs")
	}
	if err := validateIndependentV2(request, proof.IndependentV2); err != nil {
		return fmt.Errorf("fencing: canonical old-voter terminal proof differs: %w", err)
	}
	if err := verifyIndependentVoterProofCoverage(canonical, request, proof.IndependentV2); err != nil {
		return fmt.Errorf("fencing: canonical old-voter terminal coverage differs: %w", err)
	}
	storage := proof.IndependentV2.StorageScope
	found := false
	for _, run := range canonical.Ledger.Runs {
		if run.Identity.NodeID != target.NodeID {
			continue
		}
		if run.Identity.PodUID == target.PodUID && run.Identity.WALIdentity == target.WALIdentity {
			if run.Stop == nil || run.Stop.OperationID != request.OperationID || run.Stop.RequestHash != requestDigest || run.IndependentTerminal == nil {
				return fmt.Errorf("fencing: canonical old-voter run is not terminally closed")
			}
			verificationID := hashJSON([]any{"compose-independent-terminal", requestDigest, storage, run.Identity.RunID})
			ref, err := e.readIndependentTerminal(ctx, *run.Runtime, storage, verificationID)
			if err != nil || ref != *run.IndependentTerminal {
				return errors.Join(err, fmt.Errorf("fencing: canonical old-voter terminal evidence is unavailable"))
			}
			found = true
		}
	}
	if !found {
		return fmt.Errorf("fencing: canonical old-voter terminal run is missing")
	}
	return nil
}

func validateSupervisedPod(pod object) error {
	if str(nested(pod, "metadata", "annotations", fencingProtocolAnnotation)) != "1" {
		return fmt.Errorf("fencing: workload is not enrolled")
	}
	spec := asObject(pod["spec"])
	if spec["hostPID"] == true || spec["shareProcessNamespace"] == true || len(list(spec["containers"])) != 1 || len(list(spec["initContainers"])) != 0 || len(list(spec["ephemeralContainers"])) != 0 {
		return fmt.Errorf("fencing: dedicated private PID namespace required")
	}
	ctr := asObject(list(spec["containers"])[0])
	if ctr["lifecycle"] != nil {
		return fmt.Errorf("fencing: untracked lifecycle processes are unsupported")
	}
	for _, probe := range []string{"readinessProbe", "livenessProbe", "startupProbe"} {
		if nested(ctr, probe, "exec") != nil {
			return fmt.Errorf("fencing: exec probes may create untracked processes")
		}
	}
	for _, mount := range list(ctr["volumeMounts"]) {
		p := path.Clean(str(asObject(mount)["mountPath"]))
		if p == "/" || p == "/proc" || strings.HasPrefix(p, "/proc/") {
			return fmt.Errorf("fencing: procfs must remain kernel-provided")
		}
	}
	command := list(ctr["command"])
	args := list(ctr["args"])
	if len(command) != 1 || filepath.Base(str(command[0])) != "rhiza-fencer" || len(args) < 2 || str(args[0]) != "supervise" {
		return fmt.Errorf("fencing: supervised launch required")
	}
	security := asObject(ctr["securityContext"])
	if mode := str(security["procMount"]); mode != "" && mode != "Default" {
		return fmt.Errorf("fencing: default procfs required")
	}
	if security["allowPrivilegeEscalation"] != false || security["privileged"] == true {
		return fmt.Errorf("fencing: unprivileged supervisor required")
	}
	dropped := false
	for _, capability := range list(nested(security, "capabilities", "drop")) {
		if str(capability) == "ALL" {
			dropped = true
		}
	}
	if !dropped || len(list(nested(security, "capabilities", "add"))) != 0 {
		return fmt.Errorf("fencing: capabilities must be dropped")
	}
	for _, volume := range list(spec["volumes"]) {
		if asObject(volume)["hostPath"] != nil {
			return fmt.Errorf("fencing: host mounts unsupported")
		}
	}
	return nil
}

// Reconcile evaluates durable proof, never status-only assertions. The current
// inventory is re-read after closing admission; unknown Pods remain blocking.
func (e *FencingExecutor) Reconcile(ctx context.Context) error {
	endpoint := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + url.PathEscape(e.Kube.Namespace) + "/rhizafences"
	var records struct {
		Items []fenceResource `json:"items"`
	}
	if err := e.Kube.Get(ctx, endpoint, &records); err != nil {
		return err
	}
	var failures []error
	for _, record := range records.Items {
		if record.Status.Proof != nil || record.Metadata["deletionTimestamp"] != nil {
			continue
		}
		if err := e.reconcileFence(ctx, endpoint, &record); err != nil && !errors.Is(err, ErrFencePending) {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (e *FencingExecutor) reconcileFence(ctx context.Context, endpoint string, record *fenceResource) error {
	r := record.Spec
	if err := validateRequest(&r); err != nil {
		return err
	}
	if r.Namespace != e.Kube.Namespace {
		return fmt.Errorf("fencing: namespace differs")
	}
	if r.Scope == ScopeVoter && len(r.Targets) == 0 {
		return fmt.Errorf("fencing: voter scope requires targets")
	}
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(r.StatefulSet), &sts); err != nil {
		return err
	}
	if str(nested(sts, "metadata", "uid")) != r.StatefulSetUID {
		return fmt.Errorf("fencing: StatefulSet UID differs")
	}
	hash, err := requestHash(&r)
	if err != nil {
		return err
	}
	var nodes []string
	if r.Scope == ScopeVoter {
		for _, target := range r.Targets {
			nodes = append(nodes, target.NodeID)
		}
	}
	state, err := e.change(ctx, r.StatefulSetUID, r.SourceClusterID, func(l *fencing.Ledger) error {
		if binding := r.BootstrapGenesis; binding != nil {
			if l.ClosedBy != "" {
				if l.ClosedBy != r.OperationID || l.ClosedHash != hash {
					return fencing.ErrClosed
				}
			} else {
				current := fenceLedger{StatefulSetUID: r.StatefulSetUID, Ledger: *l}
				candidate, targets, err := e.bootstrapGenesisFenceCandidate(ctx, r, current, false)
				if err != nil {
					return err
				}
				if binding.ClusterUID != candidate.ClusterUID || binding.CandidateDigest != hashJSON(candidate) || hashJSON(targets) != hashJSON(r.Targets) {
					return fmt.Errorf("fencing: automatic bootstrap candidate or target inventory changed")
				}
			}
		}
		return l.CloseNodes(r.OperationID, hash, nodes)
	})
	if err != nil {
		return err
	}
	current, err := e.currentInventory(ctx, r.StatefulSetUID, nodes)
	if err != nil {
		return err
	}
	for _, target := range r.Targets {
		found := false
		for _, run := range state.Ledger.Runs {
			if run.Identity.NodeID == target.NodeID && (target.PodUID == "" || target.PodUID == run.Identity.PodUID) && (target.WALIdentity == "" || target.WALIdentity == run.Identity.WALIdentity) {
				found = true
			}
		}
		if !found {
			return ErrFencePending
		}
	}
	// A clean source-process drain is not independent termination evidence. The
	// required mutation-journal profile must always complete through verified v2
	// evidence, even when the cooperative ledger also reports every process exited.
	if state.Ledger.RequireMutationJournal {
		settled, err := e.closeMutationProducers(ctx, state, nodes)
		if err != nil {
			return err
		}
		if !settled {
			return ErrFencePending
		}
		if r.BootstrapGenesis != nil {
			closed, err := e.read(ctx, r.StatefulSetUID, r.SourceClusterID)
			if err != nil {
				return err
			}
			if closed.Ledger.ClosedBy != r.OperationID || closed.Ledger.ClosedHash != hash {
				return fencing.ErrPending
			}
			candidate, targets, err := e.bootstrapGenesisFenceCandidate(ctx, r, closed, true)
			if err != nil || r.BootstrapGenesis.CandidateDigest != hashJSON(candidate) || r.BootstrapGenesis.ClusterUID != candidate.ClusterUID || hashJSON(targets) != hashJSON(r.Targets) {
				return errors.Join(err, fencing.ErrPending)
			}
		}
		return e.independentFence(ctx, endpoint, record, r, sts, state, hash, nodes, current)
	}
	if !state.Ledger.CompleteNodes(r.OperationID, hash, nodes, current) {
		return e.independentFence(ctx, endpoint, record, r, sts, state, hash, nodes, current)
	}
	proof := &FenceProof{OperationID: r.OperationID, SourceClusterID: r.SourceClusterID, BindingUID: r.BindingUID, RequestHash: hash, ProofID: "cooperative-" + hashJSON(state), ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}
	record.Status.Proof = proof
	return e.Kube.Put(ctx, endpoint+"/"+url.PathEscape(str(record.Metadata["name"]))+"/status", record, nil)
}

// currentInventory lists the Pod UIDs in the fenced scope. An unsupported Pod
// shape blocks independently of which backend would report termination.
func (e *FencingExecutor) currentInventory(ctx context.Context, uid string, nodes []string) ([]string, error) {
	pods, err := e.controller().ownedPods(ctx, uid)
	if err != nil {
		return nil, err
	}
	wanted := map[string]bool{}
	for _, node := range nodes {
		wanted[node] = true
	}
	var current []string
	for _, pod := range pods {
		name := str(nested(pod, "metadata", "name"))
		if len(wanted) > 0 && !wanted[name] {
			continue
		}
		if err := validateSupervisedPod(pod); err != nil {
			return nil, ErrFencePending
		}
		current = append(current, str(nested(pod, "metadata", "uid")))
	}
	return current, nil
}

// ledgerHash binds the exact durable closed ledger, including the namespace that
// owns it, so evidence bound to one closed scope cannot authorize another.
func (e *FencingExecutor) ledgerHash(state fenceLedger) string {
	return hashJSON([]any{e.Kube.Namespace, state})
}

// storageScope binds the exact object-store location of the fenced generation.
// It comes from the supervised workload configuration, never from a global
// bucket guess or from the attestor's own claim.
func (e *FencingExecutor) storageScope(ctx context.Context, sts object, source string) (AttestationStoreScope, error) {
	containers := list(nested(sts, "spec", "template", "spec", "containers"))
	if len(containers) == 0 {
		return AttestationStoreScope{}, fmt.Errorf("fencing: workload container is unavailable")
	}
	// shortcut: the supervised contract is one application container; a
	// multi-container template would have to name its object-store container.
	env, err := e.controller().environment(ctx, asObject(containers[0]))
	if err != nil {
		return AttestationStoreScope{}, err
	}
	identity := StoreIdentity(env)
	// Carry every non-secret field StoreIdentity compares, so evidence for a
	// different store that merely shares a bucket and prefix cannot bind.
	scope := AttestationStoreScope{
		Provider:               identity["RHIZA_OBJSTORE_PROVIDER"],
		Endpoint:               identity["RHIZA_OBJSTORE_ENDPOINT"],
		Bucket:                 identity["RHIZA_OBJSTORE_BUCKET"],
		Prefix:                 path.Join(identity["RHIZA_OBJSTORE_PREFIX"], source),
		Account:                identity["RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT"],
		ConnectionStringDigest: identity["azure_connection_string_digest"],
	}
	if scope.Provider == "" || scope.Bucket == "" || scope.Provider == "filesystem" {
		return scope, fmt.Errorf("fencing: workload object store is not a shared store")
	}
	return scope, nil
}
