package operator

import (
	"context"
	"encoding/base64"
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
	"strings"

	"cloud.google.com/go/storage"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
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
	// RuntimeAgent captures the exact containerd/cgroup incarnation before
	// RegisterEnrolled is durably persisted and before supervisor child startup.
	RuntimeAgent *RuntimeAgentClient
}

type fenceLedger struct {
	StatefulSetUID string         `json:"statefulSetUID"`
	Ledger         fencing.Ledger `json:"ledger"`
}

func (e *FencingExecutor) controller() *Controller { return &Controller{Kube: e.Kube} }

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
		log.Printf("fencing supervisor authentication rejected: action=%s podUID=%s error=%v", message.Action, message.Identity.PodUID, err)
		http.Error(w, "supervisor identity rejected", http.StatusForbidden)
		return
	}
	if message.Action == "register" {
		// Persist the immutable profile claim before enrollment or any admission
		// side effect. A failed enrollment leaves a required, retryable root.
		err = e.initializeRoot(r.Context(), uid, message.Identity.SourceClusterID)
	}
	var enrolled *fencing.RuntimeBinding
	if err == nil && message.Action == "register" && e.RuntimeAgent != nil {
		if message.Drain != nil || message.Request != nil || message.MutationLookup != nil {
			http.Error(w, "malformed runtime registration", http.StatusBadRequest)
			return
		}
		base, enrollErr := e.RuntimeAgent.enrollmentBase(r.Context(), e.Kube.Namespace, uid, message.Identity)
		if enrollErr == nil {
			var binding fencing.RuntimeBinding
			binding, enrollErr = e.RuntimeAgent.Enroll(r.Context(), base)
			if enrollErr == nil {
				enrolled = &binding
			}
		}
		if enrollErr != nil {
			log.Printf("runtime enrollment pending: podUID=%s runID=%s error=%v", message.Identity.PodUID, message.Identity.RunID, enrollErr)
			http.Error(w, "runtime enrollment unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	var stop fencing.StopRequest
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
		binding, err = e.bindSupervisorWAL(r.Context(), uid, message.Identity)
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
			var changed fenceLedger
			changed, err = e.change(r.Context(), uid, message.Identity.SourceClusterID, func(l *fencing.Ledger) error {
				switch message.Action {
				case "register":
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
					if enrolled != nil {
						l.RequireRuntimeBinding = true
						return l.RegisterEnrolled(message.Identity, *enrolled)
					}
					if l.RequireRuntimeBinding {
						return fmt.Errorf("fencing: runtime agent is required")
					}
					return l.Register(message.Identity)
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
			if err == nil && message.Action == "register" && changed.Ledger.RequireMutationJournal {
				if enrolled == nil {
					err = fmt.Errorf("fencing: required archive genesis registration lacks runtime binding")
				} else {
					err = e.claimArchiveScopeForRegistration(r.Context(), uid, message.Identity, *enrolled)
				}
			}
		}
	}
	if errors.Is(err, fencing.ErrPending) || errors.Is(err, ErrFencePending) {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if errors.Is(err, fencing.ErrClosed) {
		w.WriteHeader(http.StatusGone)
		return
	}
	if err != nil {
		if transientFenceAuthority(err) {
			http.Error(w, "authority unavailable", http.StatusServiceUnavailable)
			return
		}
		log.Printf("fencing supervisor transition rejected: action=%s podUID=%s error=%v", message.Action, message.Identity.PodUID, err)
		http.Error(w, "fencing transition rejected", http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if message.Action == "stop" {
		_ = json.NewEncoder(w).Encode(stop)
	} else if message.Action == "bind" {
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
	if name == "" || id.NodeID != name {
		return "", fmt.Errorf("fencing: stable Pod name required as node ID")
	}
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", name), &pod); err != nil {
		return "", err
	}
	if str(nested(pod, "metadata", "uid")) != id.PodUID || nested(pod, "metadata", "deletionTimestamp") != nil {
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
	state, err := e.change(ctx, r.StatefulSetUID, r.SourceClusterID, func(l *fencing.Ledger) error { return l.CloseNodes(r.OperationID, hash, nodes) })
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
