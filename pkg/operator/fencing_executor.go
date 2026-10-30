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
	"strconv"
	"strings"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
	"github.com/thanos-io/objstore"
)

const fencingProtocolAnnotation = "rhiza.mrchypark.dev/cooperative-fencing"
const fencingRootAnnotation = "rhiza.mrchypark.dev/fencing-root-uid"
const fencingCredentialAnnotation = "rhiza.mrchypark.dev/fencing-credential-uid"
const fencingBindingAnnotation = "rhiza.mrchypark.dev/fencing-incarnation-binding"
const fencingPrepareOperationAnnotation = "rhiza.mrchypark.dev/prepare-operation"

// FencingExecutor is a namespaced cooperative authority. Its credentials are
// separate from the operator and application accounts. Applications may request
// only transitions for their TokenReview-authenticated Pod incarnation.
//
// Attestor is optional external evidence for an incarnation no supervisor can
// report on. It is consulted only after the cooperative ledger is durably closed
// and only when the cooperative proof is still incomplete.
type FencingExecutor struct {
	Kube          *Kubernetes
	Attestor      FenceAttestor
	Bucket        objstore.Bucket
	Prefix        string
	StoreIdentity map[string]string
	// Anchor and AnchorStorageID are configured independently of the archive
	// prefix and StoreIdentity digest. DR admission fails closed when either is
	// absent; the target cluster component is checked against the authenticated
	// Pod's source cluster and the retained assignment.
	Anchor              AnchorClient
	AnchorStorageID     string
	AnchorBindingConfig *recoveryanchor.StorageBindingConfig
}

type fenceLedger struct {
	StatefulSetUID string         `json:"statefulSetUID"`
	Ledger         fencing.Ledger `json:"ledger"`
}

func (e *FencingExecutor) controller() *Controller {
	return &Controller{Kube: e.Kube, Bucket: e.Bucket, Prefix: e.Prefix, StoreIdentity: e.StoreIdentity, Anchor: e.Anchor, AnchorBindingConfig: e.AnchorBindingConfig}
}

func (e *FencingExecutor) ledgerPath(uid, source string) string {
	return e.controller().corePath("configmaps", "rhiza-fence-ledger-"+hashJSON([]string{uid, source})[:40])
}

// change retries only a Kubernetes CAS conflict, not failed executions. The
// transition is pure and must tolerate a previously committed response loss.
func (e *FencingExecutor) change(ctx context.Context, uid, source string, transition func(*fencing.Ledger) error) (fenceLedger, error) {
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

func (e *FencingExecutor) read(ctx context.Context, uid, source string) (fenceLedger, error) {
	state, _, _, err := e.readWithUID(ctx, uid, source)
	return state, err
}

func (e *FencingExecutor) readWithUID(ctx context.Context, uid, source string) (fenceLedger, string, object, error) {
	var resource object
	if err := e.Kube.Get(ctx, e.ledgerPath(uid, source), &resource); err != nil {
		return fenceLedger{}, "", nil, err
	}
	var state fenceLedger
	if nested(resource, "metadata", "deletionTimestamp") != nil {
		return state, "", nil, fmt.Errorf("fencing: ledger deleting")
	}
	if err := json.Unmarshal([]byte(str(nested(resource, "data", "ledger.json"))), &state); err != nil {
		return state, "", nil, err
	}
	if state.StatefulSetUID != uid || state.Ledger.SourceClusterID != source {
		return state, "", nil, fmt.Errorf("fencing: ledger differs")
	}
	return state, str(nested(resource, "metadata", "uid")), resource, nil
}

func (e *FencingExecutor) registrationLedgerObjectUID(resource object, uid, source string) (string, error) {
	meta := asObject(resource["metadata"])
	wantName := path.Base(e.ledgerPath(uid, source))
	if str(meta["name"]) != wantName || str(meta["namespace"]) != e.Kube.Namespace {
		return "", fmt.Errorf("fencing: registration ledger object scope differs")
	}
	objectUID := str(meta["uid"])
	if objectUID == "" {
		return "", fmt.Errorf("fencing: registration ledger UID is unavailable")
	}
	return objectUID, nil
}

// changeExisting is the registration-only CAS path. Unlike change, it never
// creates a ledger and it pins the Kubernetes object UID across every retry.
// A 404 or replacement object revokes admission instead of becoming a new
// history root.
func (e *FencingExecutor) changeExisting(ctx context.Context, uid, source, expectedObjectUID string, validate func(*fencing.Ledger) error, transition func(*fencing.Ledger) error) (fenceLedger, error) {
	if expectedObjectUID == "" {
		return fenceLedger{}, fmt.Errorf("fencing: registration ledger UID is unavailable")
	}
	endpoint := e.ledgerPath(uid, source)
	for attempt := 0; attempt < 8; attempt++ {
		var resource object
		if err := e.Kube.Get(ctx, endpoint, &resource); err != nil {
			if apiNotFound(err) {
				return fenceLedger{}, fmt.Errorf("fencing: registration ledger disappeared; admission refused")
			}
			return fenceLedger{}, err
		}
		if nested(resource, "metadata", "deletionTimestamp") != nil {
			return fenceLedger{}, fmt.Errorf("fencing: ledger deleting")
		}
		got, err := e.registrationLedgerObjectUID(resource, uid, source)
		if err != nil || got != expectedObjectUID {
			return fenceLedger{}, fmt.Errorf("fencing: registration ledger object identity changed")
		}
		var state fenceLedger
		if err := json.Unmarshal([]byte(str(nested(resource, "data", "ledger.json"))), &state); err != nil {
			return state, err
		}
		if state.StatefulSetUID != uid || state.Ledger.SourceClusterID != source {
			return state, fmt.Errorf("fencing: ledger identity differs")
		}
		if validate != nil {
			if err := validate(&state.Ledger); err != nil {
				return state, err
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
		resource["data"] = object{"ledger.json": string(data)}
		err = e.Kube.Put(ctx, endpoint, resource, nil)
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

// registerIncarnation admits only a slot already present in the retained
// ledger. Legacy IDs require current exact managed enrollment; replacements
// receive a UID-derived consensus ID and immutable managed credentials.
func (e *FencingExecutor) registerIncarnation(ctx context.Context, candidate fencing.Identity, workloadUID string) (fencing.Registration, error) {
	if candidate.PodName == "" {
		return fencing.Registration{}, fmt.Errorf("fencing: registration requires the authenticated Pod name")
	}
	canonical, err := fencing.ReplacementNodeID(candidate.PodUID)
	if err != nil || (candidate.NodeID != candidate.PodName && candidate.NodeID != canonical) {
		return fencing.Registration{}, fmt.Errorf("fencing: candidate NodeID is not bound to the authenticated Pod UID")
	}
	state, ledgerUID, ledgerObject, err := e.readWithUID(ctx, workloadUID, candidate.SourceClusterID)
	if apiNotFound(err) {
		return fencing.Registration{}, fmt.Errorf("fencing: registration ledger is missing; cold bootstrap is unsupported")
	}
	if err != nil {
		return fencing.Registration{}, err
	}
	verifiedUID, err := e.registrationLedgerObjectUID(ledgerObject, workloadUID, candidate.SourceClusterID)
	if err != nil || verifiedUID != ledgerUID {
		if err == nil {
			err = fmt.Errorf("fencing: registration ledger object identity changed")
		}
		return fencing.Registration{}, err
	}
	if state.Ledger.ClosedBy != "" {
		return fencing.Registration{}, fencing.ErrClosed
	}
	var sameUID *fencing.Identity
	sameUIDWAL := ""
	seenSlot := false
	var slotEnrollment []fencing.Identity
	for _, run := range state.Ledger.Runs {
		old := run.Identity
		if old.PodUID == candidate.PodUID {
			if sameUID != nil && (sameUID.NodeID != old.NodeID || fencing.PodNameForIdentity(*sameUID) != fencing.PodNameForIdentity(old) || sameUID.CredentialUID != old.CredentialUID || sameUID.CredentialName != old.CredentialName) {
				return fencing.Registration{}, fmt.Errorf("fencing: retained Pod UID identity history conflicts")
			}
			if old.WALIdentity != "" {
				if sameUIDWAL != "" && sameUIDWAL != old.WALIdentity {
					return fencing.Registration{}, fmt.Errorf("fencing: retained Pod UID WAL history conflicts")
				}
				sameUIDWAL = old.WALIdentity
			}
			copy := old
			sameUID = &copy
		}
		if fencing.PodNameForIdentity(old) == candidate.PodName {
			seenSlot = true
			slotEnrollment = append(slotEnrollment, old)
		}
	}
	if sameUID != nil {
		if fencing.PodNameForIdentity(*sameUID) != candidate.PodName {
			return fencing.Registration{}, fmt.Errorf("fencing: Pod UID is already bound to a different name")
		}
		if sameUID.PodName == "" {
			binding, managed, err := e.managedWorkloadBinding(ctx, candidate, workloadUID, true)
			if err != nil {
				return fencing.Registration{}, err
			}
			if !managed {
				return fencing.Registration{}, fmt.Errorf("fencing: legacy registration lacks verified managed enrollment")
			}
			if err := e.validateManagedLegacyIdentity(ctx, binding, candidate, sameUIDWAL); err != nil {
				return fencing.Registration{}, err
			}
			legacy := candidate
			legacy.PodName = ""
			if err := e.registerLedgerIdentity(ctx, workloadUID, candidate.SourceClusterID, ledgerUID, slotEnrollment, legacy); err != nil {
				return fencing.Registration{}, err
			}
			return fencing.Registration{Identity: legacy}, nil
		}
		if err := sameUID.Validate(); err != nil {
			return fencing.Registration{}, err
		}
		for _, run := range state.Ledger.Runs {
			if run.Identity.PodUID == candidate.PodUID && run.Identity.RunID != candidate.RunID && !terminatedFencingRun(run) {
				return fencing.Registration{}, fmt.Errorf("fencing: previous run for this Pod UID has no terminal receipt")
			}
		}
		identity := *sameUID
		identity.RunID, identity.WALIdentity = candidate.RunID, ""
		reg, err := e.incarnationCredential(ctx, candidate, workloadUID, identity, false)
		if err != nil {
			return fencing.Registration{}, err
		}
		if err := e.registerLedgerIdentity(ctx, workloadUID, candidate.SourceClusterID, ledgerUID, slotEnrollment, reg.Identity); err != nil {
			return fencing.Registration{}, err
		}
		return reg, nil
	}
	if seenSlot {
		registration, handled, err := e.registerMembershipRepairLearner(ctx, candidate, workloadUID, ledgerUID, slotEnrollment)
		if handled || err != nil {
			return registration, err
		}
	}

	if !seenSlot {
		if state.Ledger.Bootstrap != nil {
			return e.registerBootstrapAssignment(ctx, candidate, workloadUID)
		}
		return fencing.Registration{}, fmt.Errorf("fencing: no retained enrollment for this StatefulSet slot; cold registration is unsupported")
	}

	if state.Ledger.ClosedNodes[candidate.PodName].OperationID != "" || state.Ledger.ClosedNodes[canonical].OperationID != "" {
		return fencing.Registration{}, fencing.ErrClosed
	}
	for _, run := range state.Ledger.Runs {
		old := run.Identity
		if old.NodeID == canonical && (old.PodUID != candidate.PodUID || fencing.PodNameForIdentity(old) != candidate.PodName) {
			return fencing.Registration{}, fmt.Errorf("fencing: replacement NodeID already belongs to another incarnation")
		}
		if fencing.PodNameForIdentity(old) == candidate.PodName && old.PodUID != candidate.PodUID && !terminatedFencingRun(run) {
			return fencing.Registration{}, fmt.Errorf("fencing: previous Pod incarnation has no terminal receipt")
		}
	}

	identity := candidate
	identity.NodeID = canonical
	reg, err := e.incarnationCredential(ctx, candidate, workloadUID, identity, true)
	if err != nil {
		return fencing.Registration{}, err
	}
	if err := e.registerLedgerIdentity(ctx, workloadUID, candidate.SourceClusterID, ledgerUID, slotEnrollment, reg.Identity); err != nil {
		return fencing.Registration{}, err
	}
	return reg, nil
}

func (e *FencingExecutor) registerMembershipRepairLearner(ctx context.Context, candidate fencing.Identity, workloadUID, ledgerUID string, enrolled []fencing.Identity) (fencing.Registration, bool, error) {
	state, currentLedgerUID, _, err := e.readWithUID(ctx, workloadUID, candidate.SourceClusterID)
	if err != nil {
		return fencing.Registration{}, true, err
	}
	if currentLedgerUID != ledgerUID || state.Ledger.MembershipRepair == nil {
		return fencing.Registration{}, false, nil
	}
	record := state.Ledger.MembershipRepair
	if record.Progress.Stage != fencing.MembershipRepairAdmitted {
		return fencing.Registration{}, false, nil
	}
	var assignment *fencing.BootstrapAssignment
	for i := range record.Progress.Candidates {
		candidateAssignment := &record.Progress.Candidates[i].Assignment
		if candidateAssignment.PodUID == candidate.PodUID {
			assignment = candidateAssignment
			break
		}
	}
	if assignment == nil {
		return fencing.Registration{}, false, nil
	}
	a := record.Authorization
	canonical, canonicalErr := fencing.ReplacementNodeID(candidate.PodUID)
	if assignment.Validate() != nil || canonicalErr != nil || assignment.Purpose != fencing.BootstrapLearner ||
		assignment.RootUID != a.RootUID || assignment.StatefulSetUID != workloadUID || assignment.LedgerUID != ledgerUID ||
		assignment.SourceClusterID != candidate.SourceClusterID || assignment.PodName != candidate.PodName ||
		assignment.PodUID != candidate.PodUID || assignment.NodeID != canonical || assignment.NodeID != candidate.NodeID ||
		assignment.MembershipHash != a.BootstrapMembershipHash || assignment.CredentialName == "" || assignment.CredentialUID == "" ||
		(candidate.CredentialName != "" && candidate.CredentialName != assignment.CredentialName) ||
		(candidate.CredentialUID != "" && candidate.CredentialUID != assignment.CredentialUID) ||
		fencing.PodNameForIdentity(a.OldIdentity) != candidate.PodName || candidate.PodUID == a.OldIdentity.PodUID {
		return fencing.Registration{}, true, fmt.Errorf("fencing: candidate differs from retained ordinary-repair assignment")
	}
	binding, err := e.peerClosureBinding(ctx, candidate, workloadUID)
	if err != nil {
		return fencing.Registration{}, true, err
	}
	view := fencing.MembershipRepairView{Authorization: record.Authorization, Progress: record.Progress}
	if err := e.verifyMembershipRepairArchive(ctx, binding, view); err != nil {
		return fencing.Registration{}, true, err
	}
	identity := candidate
	identity.CredentialName, identity.CredentialUID = assignment.CredentialName, assignment.CredentialUID
	registration, err := e.incarnationCredential(ctx, candidate, workloadUID, identity, false)
	if err != nil {
		return fencing.Registration{}, true, err
	}
	if registration.Identity.CredentialName != assignment.CredentialName || registration.Identity.CredentialUID != assignment.CredentialUID {
		return fencing.Registration{}, true, fmt.Errorf("fencing: candidate credential differs from retained assignment")
	}
	if err := e.registerLedgerIdentity(ctx, workloadUID, candidate.SourceClusterID, ledgerUID, enrolled, registration.Identity); err != nil {
		return fencing.Registration{}, true, err
	}
	return registration, true, nil
}

// managedWorkloadBinding resolves the trusted managed root and durable
// workload binding. Missing or unavailable evidence is not proof that the
// caller is unmanaged.
func (e *FencingExecutor) managedWorkloadBinding(ctx context.Context, id fencing.Identity, workloadUID string, loadBinding bool) (peerClosureBinding, bool, error) {
	name := fencing.PodNameForIdentity(id)
	var pod object
	if err := e.Kube.Get(ctx, e.controller().corePath("pods", name), &pod); err != nil {
		return peerClosureBinding{}, false, err
	}
	if str(nested(pod, "metadata", "uid")) != id.PodUID || nested(pod, "metadata", "deletionTimestamp") != nil {
		return peerClosureBinding{}, false, fmt.Errorf("fencing: managed binding Pod incarnation changed")
	}
	ownerName, ownerUID := "", ""
	for _, raw := range list(nested(pod, "metadata", "ownerReferences")) {
		owner := asObject(raw)
		if str(owner["apiVersion"]) == "apps/v1" && str(owner["kind"]) == "StatefulSet" && owner["controller"] == true {
			ownerName, ownerUID = str(owner["name"]), str(owner["uid"])
			break
		}
	}
	if ownerName == "" || ownerUID != workloadUID {
		return peerClosureBinding{}, false, fmt.Errorf("fencing: managed binding StatefulSet differs")
	}
	var roots struct {
		Items []ClusterResource `json:"items"`
	}
	if err := e.Kube.Get(ctx, e.controller().clusterPath(""), &roots); err != nil {
		return peerClosureBinding{}, false, err
	}
	managed := false
	for _, root := range roots.Items {
		if root.Spec.StatefulSet == ownerName && root.Spec.Automatic && root.Metadata["deletionTimestamp"] == nil {
			managed = true
		}
	}
	if managed {
		if !loadBinding {
			return peerClosureBinding{}, true, nil
		}
		binding, err := e.peerClosureBinding(ctx, id, workloadUID)
		return binding, true, err
	}
	if e.Bucket == nil {
		return peerClosureBinding{}, false, nil
	}
	key := path.Join(e.Prefix, "automatic-workloads", e.Kube.Namespace, workloadUID, "binding.json")
	exists, err := e.Bucket.Exists(ctx, key)
	if err != nil {
		return peerClosureBinding{}, false, fmt.Errorf("fencing: durable workload binding status unavailable: %w", err)
	}
	if exists {
		return peerClosureBinding{}, true, fmt.Errorf("fencing: managed workload root is missing; ledger reconstruction is refused")
	}
	return peerClosureBinding{}, false, nil
}

func (e *FencingExecutor) validateManagedLegacyIdentity(ctx context.Context, binding peerClosureBinding, id fencing.Identity, walIdentity string) error {
	members, err := e.configuredSourceMembers(ctx, binding, id.SourceClusterID)
	if err != nil {
		return err
	}
	found := false
	for _, member := range members {
		if string(member.ID) == id.NodeID {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("fencing: legacy NodeID is absent from configured voters")
	}
	control, _, err := e.controller().loadAuto(ctx, e.controller().autoKey(&binding.root))
	if err != nil {
		return fmt.Errorf("fencing: durable voter inventory unavailable: %w", err)
	}
	target, ok := control.Identities[quepaxa.NodeID(id.NodeID)]
	if !ok || target.NodeID != id.NodeID || target.Pod != fencing.PodNameForIdentity(id) || target.PodUID != id.PodUID || target.WALIdentity == "" {
		return fmt.Errorf("fencing: legacy registration does not match the exact observed Pod and WAL identity")
	}
	if walIdentity != "" && target.WALIdentity != walIdentity {
		return fmt.Errorf("fencing: legacy WAL identity differs from the durable voter observation")
	}
	return nil
}

func (e *FencingExecutor) configuredSourceMembers(ctx context.Context, binding peerClosureBinding, source string) ([]quepaxa.Member, error) {
	archived, err := e.controller().sourceMembership(ctx, path.Join(e.Prefix, source))
	if err != nil {
		return nil, fmt.Errorf("fencing: immutable source membership unavailable: %w", err)
	}
	members, err := recoveryMembers(binding.environment["RHIZA_CLUSTER_MEMBERS"])
	if err != nil {
		return nil, fmt.Errorf("fencing: configured voter identity unavailable: %w", err)
	}
	durability := binding.environment["RHIZA_OBJSTORE_DURABILITY"]
	if durability == "" {
		durability = "async"
	}
	if archived != recovery.NewMembershipRecord(source, members, durability) {
		return nil, fmt.Errorf("fencing: configured voters differ from immutable source membership")
	}
	return members, nil
}

func terminatedFencingRun(run fencing.Run) bool {
	if !run.Exited || run.Stop == nil {
		return false
	}
	if run.Identity.WALIdentity == "" {
		return run.NeverBoundExit != nil && *run.NeverBoundExit == *run.Stop && run.Drain == nil
	}
	return run.Drain != nil && run.Drain.Request == *run.Stop && run.Drain.Clean()
}

func (e *FencingExecutor) registerLedgerIdentity(ctx context.Context, workloadUID, source, ledgerUID string, enrolled []fencing.Identity, identity fencing.Identity) error {
	validateEnrollment := func(ledger *fencing.Ledger) error {
		if len(enrolled) == 0 {
			return fmt.Errorf("fencing: registration has no retained slot enrollment")
		}
		for _, expected := range enrolled {
			run, ok := ledger.Runs[expected.RunID]
			if !ok || run.Identity != expected {
				return fmt.Errorf("fencing: retained slot enrollment changed during registration")
			}
		}
		return nil
	}
	_, err := e.changeExisting(ctx, workloadUID, source, ledgerUID, validateEnrollment, func(ledger *fencing.Ledger) error {
		return ledger.Register(identity)
	})
	return err
}

func (e *FencingExecutor) incarnationSecretName(rootUID, workloadUID string, id fencing.Identity) string {
	return "rhiza-inc-" + hashJSON([]string{e.Kube.Namespace, rootUID, workloadUID, id.SourceClusterID, id.PodName, id.PodUID, id.NodeID})[:40]
}

func (e *FencingExecutor) incarnationCredential(ctx context.Context, candidate fencing.Identity, workloadUID string, identity fencing.Identity, createAllowed bool) (fencing.Registration, error) {
	return e.incarnationCredentialFor(ctx, candidate, workloadUID, identity, createAllowed, false)
}

func (e *FencingExecutor) incarnationCredentialForInit(ctx context.Context, candidate fencing.Identity, workloadUID string, identity fencing.Identity, createAllowed bool) (fencing.Registration, error) {
	if identity.RunID != "" || identity.WALIdentity != "" || identity.PodUID == "" || identity.PodName == "" || identity.SourceClusterID == "" || identity.NodeID == "" {
		return fencing.Registration{}, fmt.Errorf("fencing: invalid pre-runtime learner credential identity")
	}
	return e.incarnationCredentialFor(ctx, candidate, workloadUID, identity, createAllowed, true)
}

func (e *FencingExecutor) incarnationCredentialFor(ctx context.Context, candidate fencing.Identity, workloadUID string, identity fencing.Identity, createAllowed, preRuntime bool) (fencing.Registration, error) {
	binding, err := e.peerClosureBinding(ctx, candidate, workloadUID)
	if err != nil {
		return fencing.Registration{}, err
	}
	var sts object
	if err := e.Kube.Get(ctx, e.controller().stsPath(binding.root.Spec.StatefulSet), &sts); err != nil {
		return fencing.Registration{}, err
	}
	if str(nested(sts, "metadata", "uid")) != workloadUID || nested(sts, "metadata", "deletionTimestamp") != nil {
		return fencing.Registration{}, fmt.Errorf("fencing: replacement StatefulSet changed")
	}
	service := str(nested(sts, "spec", "serviceName"))
	if service == "" {
		return fencing.Registration{}, fmt.Errorf("fencing: StatefulSet service name unavailable")
	}
	domain := binding.root.Spec.ClusterDomain
	if domain == "" {
		domain = "cluster.local"
	}
	if !clusterDomainRE.MatchString(domain) {
		return fencing.Registration{}, fmt.Errorf("fencing: invalid cluster DNS domain")
	}
	httpPort, err := listenerPort(binding.environment["RHIZA_BIND_ADDR"])
	if err != nil {
		return fencing.Registration{}, fmt.Errorf("fencing: HTTP listener binding unavailable: %w", err)
	}
	peerPort, err := listenerPort(binding.environment["RHIZA_PEER_ADDR"])
	if err != nil {
		return fencing.Registration{}, fmt.Errorf("fencing: peer listener binding unavailable: %w", err)
	}
	host := identity.PodName + "." + service + "." + e.Kube.Namespace + ".svc." + domain
	member := quepaxa.Member{
		ID: quepaxa.NodeID(identity.NodeID), URL: "http://" + net.JoinHostPort(host, fmt.Sprint(httpPort)),
		PeerURL: "quic://" + net.JoinHostPort(host, fmt.Sprint(peerPort)),
	}
	if binding.rootUID == "" || str(nested(binding.callerPod, "metadata", "uid")) != identity.PodUID || str(nested(binding.callerPod, "metadata", "name")) != identity.PodName {
		return fencing.Registration{}, fmt.Errorf("fencing: replacement binding changed")
	}
	archived, err := e.controller().sourceMembership(ctx, path.Join(e.Prefix, identity.SourceClusterID))
	if err != nil {
		return fencing.Registration{}, fmt.Errorf("fencing: immutable source membership unavailable: %w", err)
	}
	staticMembers, err := recoveryMembers(binding.environment["RHIZA_CLUSTER_MEMBERS"])
	if err != nil {
		return fencing.Registration{}, fmt.Errorf("fencing: configured voter identity unavailable: %w", err)
	}
	durability := binding.environment["RHIZA_OBJSTORE_DURABILITY"]
	if durability == "" {
		durability = "async"
	}
	if archived != recovery.NewMembershipRecord(identity.SourceClusterID, staticMembers, durability) {
		return fencing.Registration{}, fmt.Errorf("fencing: configured voters differ from immutable source membership")
	}
	for _, static := range staticMembers {
		if string(static.ID) == identity.NodeID {
			return fencing.Registration{}, fmt.Errorf("fencing: replacement NodeID collides with configured voter identity")
		}
	}
	control, _, err := e.controller().loadAuto(ctx, e.controller().autoKey(&binding.root))
	if err != nil {
		return fencing.Registration{}, fmt.Errorf("fencing: durable voter inventory unavailable: %w", err)
	}
	if control.BindingUID != binding.rootUID || control.BindingVersion != autoBindingVersion ||
		control.BindingHash != autoBinding(&binding.root) || control.StatefulSetUID != workloadUID ||
		control.ActiveClusterID != identity.SourceClusterID {
		return fencing.Registration{}, fmt.Errorf("fencing: durable workload binding differs")
	}
	if _, err := e.controller().readAutoBinding(ctx, &binding.root, control, workloadUID); err != nil {
		return fencing.Registration{}, fmt.Errorf("fencing: immutable workload binding is unavailable: %w", err)
	}
	slotFound := false
	for _, voter := range control.Voters {
		if voter.Pod == identity.PodName {
			if slotFound {
				return fencing.Registration{}, fmt.Errorf("fencing: durable voter inventory repeats the StatefulSet slot")
			}
			slotFound = true
		}
	}
	if !slotFound {
		return fencing.Registration{}, fmt.Errorf("fencing: StatefulSet slot is not in the retained voter inventory")
	}
	for _, voter := range control.Voters {
		if string(voter.NodeID) != identity.NodeID {
			continue
		}
		target, ok := control.Identities[voter.NodeID]
		if !ok || target.Pod != identity.PodName || target.PodUID != identity.PodUID {
			return fencing.Registration{}, fmt.Errorf("fencing: replacement NodeID collides with a different or unknown voter")
		}
	}
	if target, ok := control.Identities[quepaxa.NodeID(identity.NodeID)]; ok && (target.Pod != identity.PodName || target.PodUID != identity.PodUID) {
		return fencing.Registration{}, fmt.Errorf("fencing: replacement NodeID is already bound to another Pod incarnation")
	}
	identity.PodName = candidate.PodName
	name := e.incarnationSecretName(binding.rootUID, workloadUID, identity)
	identity.CredentialName = name
	annotations := object{
		fencingRootAnnotation: binding.rootUID,
		fencingBindingAnnotation: hashJSON([]string{binding.rootUID, workloadUID, identity.SourceClusterID,
			identity.PodName, identity.PodUID, identity.NodeID}),
	}
	var secret object
	err = e.Kube.Get(ctx, e.controller().corePath("secrets", name), &secret)
	if apiNotFound(err) {
		if !createAllowed {
			return fencing.Registration{}, fmt.Errorf("fencing: registered incarnation credential is missing")
		}
		token, tokenErr := randomToken()
		if tokenErr != nil {
			return fencing.Registration{}, tokenErr
		}
		member.PublicKey = quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(identity.SourceClusterID), member.ID, token))
		secret = object{
			"apiVersion": "v1", "kind": "Secret", "type": "Opaque", "immutable": true,
			"metadata": object{"name": name, "annotations": annotations},
			"data": object{
				"member":           base64.StdEncoding.EncodeToString(jsonBytes(member)),
				"RHIZA_PEER_TOKEN": base64.StdEncoding.EncodeToString([]byte(token)),
			},
		}
		var created object
		if err := e.Kube.Post(ctx, e.controller().corePath("secrets", ""), secret, &created); err != nil {
			var api *APIError
			if !errors.As(err, &api) || api.StatusCode != http.StatusConflict {
				return fencing.Registration{}, fmt.Errorf("fencing: create incarnation credential: %w", err)
			}
			if err := e.Kube.Get(ctx, e.controller().corePath("secrets", name), &secret); err != nil {
				return fencing.Registration{}, fmt.Errorf("fencing: resolve concurrent credential creation: %w", err)
			}
		} else {
			secret = created
		}
	} else if err != nil {
		return fencing.Registration{}, fmt.Errorf("fencing: read incarnation credential: %w", err)
	}
	if secret["immutable"] != true || secret["type"] != "Opaque" ||
		str(nested(secret, "metadata", "name")) != name || str(nested(secret, "metadata", "namespace")) != e.Kube.Namespace ||
		str(nested(secret, "metadata", "uid")) == "" {
		return fencing.Registration{}, fmt.Errorf("fencing: incarnation credential metadata is incomplete")
	}
	for key, want := range annotations {
		if str(nested(secret, "metadata", "annotations", key)) != want {
			return fencing.Registration{}, fmt.Errorf("fencing: incarnation credential binding differs")
		}
	}
	identity.CredentialUID = str(nested(secret, "metadata", "uid"))
	if preRuntime {
		if fencing.ValidatePodBinding(identity.NodeID, identity.PodName, identity.PodUID) != nil || identity.CredentialName == "" || identity.CredentialUID == "" {
			return fencing.Registration{}, fmt.Errorf("fencing: pre-runtime learner identity binding is incomplete")
		}
	} else if err := identity.Validate(); err != nil {
		return fencing.Registration{}, err
	}
	memberBytes, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "member")))
	if err != nil {
		return fencing.Registration{}, fmt.Errorf("fencing: invalid incarnation member encoding")
	}
	var stored quepaxa.Member
	decoder := json.NewDecoder(strings.NewReader(string(memberBytes)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&stored) != nil || decoder.Decode(new(any)) != io.EOF {
		return fencing.Registration{}, fmt.Errorf("fencing: invalid incarnation member")
	}
	tokenBytes, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "RHIZA_PEER_TOKEN")))
	if err != nil || len(tokenBytes) == 0 {
		return fencing.Registration{}, fmt.Errorf("fencing: incarnation peer token unavailable")
	}
	member.PublicKey = quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(identity.SourceClusterID), quepaxa.NodeID(identity.NodeID), string(tokenBytes)))
	if stored.ID != member.ID || stored.URL != member.URL || stored.PeerURL != member.PeerURL || stored.LogURL != "" || stored.WALIdentity != "" || stored.PublicKey != member.PublicKey {
		return fencing.Registration{}, fmt.Errorf("fencing: incarnation credential member differs")
	}
	memberBytes, err = json.Marshal(stored)
	if err != nil {
		return fencing.Registration{}, err
	}
	return fencing.Registration{Identity: identity, PeerToken: string(tokenBytes), Learner: memberBytes}, nil
}

func listenerPort(address string) (int, error) {
	_, raw, err := net.SplitHostPort(address)
	if err != nil {
		return 0, err
	}
	port, err := strconv.Atoi(raw)
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("invalid listener port")
	}
	return port, nil
}

// Handler must be served with TLS. Only audience-bound, Pod-bound service
// account tokens are accepted; arbitrary identity fields are never authority.
func (e *FencingExecutor) Handler() http.Handler { return http.HandlerFunc(e.handle) }

func (e *FencingExecutor) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || (r.URL.Path != "/v1/supervisor" && r.URL.Path != "/v1/peer-closure/request" && r.URL.Path != "/v1/peer-closure/status") {
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
	if err := fencing.ValidateAuthorityMessageShape(message); err != nil {
		http.Error(w, "invalid action payload shape", http.StatusBadRequest)
		return
	}
	var authenticatedPodName string
	preRuntime := message.Action == "bootstrap-status" || message.Action == "bootstrap-runtime-admission" || message.Action == "init-begin" || message.Action == "init-outcome"
	uid, err := e.authenticateMode(r.Context(), r.Header.Get("Authorization"), message.Identity, message.Action == "register" || preRuntime, preRuntime, &authenticatedPodName)
	if err != nil {
		if transientFenceAuthority(err) {
			http.Error(w, "authority unavailable", http.StatusServiceUnavailable)
			return
		}
		log.Printf("fencing supervisor authentication rejected: action=%s podUID=%s error=%v", message.Action, message.Identity.PodUID, err)
		http.Error(w, "supervisor identity rejected", http.StatusForbidden)
		return
	}
	var stop fencing.StopRequest
	var peerReceipt fencing.PeerClosureReceipt
	var registration fencing.Registration
	if r.URL.Path == "/v1/peer-closure/request" || r.URL.Path == "/v1/peer-closure/status" {
		if message.Identity.NodeID == "" || message.PeerClosure == nil || message.Drain != nil || message.Request != nil {
			err = fmt.Errorf("fencing: peer-closure request shape differs")
		} else if r.URL.Path == "/v1/peer-closure/request" && message.Action == "peer-closure-request" {
			peerReceipt, err = e.requestPeerClosure(r.Context(), message.Identity, *message.PeerClosure, uid)
		} else if r.URL.Path == "/v1/peer-closure/status" && message.Action == "peer-closure-status" {
			peerReceipt, err = e.readPeerClosure(r.Context(), message.Identity, *message.PeerClosure, uid)
		} else {
			err = fmt.Errorf("fencing: peer-closure action differs")
		}
	} else if message.PeerClosure != nil {
		err = fmt.Errorf("fencing: unexpected peer-closure request")
	} else if preRuntime {
		shapeOK := message.Drain == nil && message.Request == nil && message.Completion == nil && message.RepairStatus == nil && message.RepairPhase == nil && message.RepairReserve == nil && message.PeerClosure == nil
		switch message.Action {
		case "bootstrap-status":
			shapeOK = shapeOK && message.BootstrapStatus != nil && message.RuntimeAdmission == nil && message.BeginInit == nil && message.InitOutcome == nil
		case "bootstrap-runtime-admission":
			shapeOK = shapeOK && message.RuntimeAdmission != nil && message.BootstrapStatus == nil && message.BeginInit == nil && message.InitOutcome == nil
		case "init-begin":
			shapeOK = shapeOK && message.BootstrapStatus == nil && message.RuntimeAdmission == nil && message.BeginInit != nil && message.InitOutcome == nil
		case "init-outcome":
			shapeOK = shapeOK && message.BootstrapStatus == nil && message.RuntimeAdmission == nil && message.BeginInit == nil && message.InitOutcome != nil
		default:
			shapeOK = false
		}
		if !shapeOK {
			err = fmt.Errorf("fencing: bootstrap request shape differs")
		} else {
			switch message.Action {
			case "bootstrap-status":
				var result fencing.BootstrapStatusResult
				result, err = e.bootstrapStatus(r.Context(), uid, authenticatedPodName, message.Identity, *message.BootstrapStatus)
				if err == nil {
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(result)
					return
				}
			case "bootstrap-runtime-admission":
				var result fencing.BootstrapRuntimeAdmissionResult
				result, err = e.bootstrapRuntimeAdmission(r.Context(), authenticatedPodName, message.Identity, *message.RuntimeAdmission)
				if err == nil {
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(result)
					return
				}
			case "init-begin":
				var attempt fencing.InitAttempt
				attempt, err = e.beginInit(r.Context(), uid, authenticatedPodName, message.Identity, *message.BeginInit)
				if err == nil {
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(attempt)
					return
				}
			case "init-outcome":
				err = e.recordInitOutcome(r.Context(), uid, authenticatedPodName, message.Identity, *message.InitOutcome)
			default:
				err = fmt.Errorf("fencing: unknown pre-runtime action")
			}
		}
	} else if message.Action == "register" {
		if message.Drain != nil || message.Request != nil {
			err = fmt.Errorf("fencing: unexpected registration fields")
		} else {
			if message.Identity.PodName == "" {
				message.Identity.PodName = authenticatedPodName
			}
			err = e.recordObservedInitTerminal(r.Context(), message.Identity, uid)
			if err == nil {
				registration, err = e.registerIncarnation(r.Context(), message.Identity, uid)
			}
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
	} else if message.Action == "bootstrap-complete" {
		if message.Completion == nil || message.Drain != nil || message.Request != nil || message.BeginInit != nil || message.InitOutcome != nil || message.BootstrapStatus != nil || message.RepairStatus != nil || message.RepairPhase != nil || message.RepairReserve != nil || message.PeerClosure != nil {
			err = fmt.Errorf("fencing: bootstrap completion request shape differs")
		} else {
			err = e.completeBootstrap(r.Context(), uid, message.Identity, *message.Completion)
		}
	} else if message.Action == "membership-repair-status" {
		if message.RepairStatus == nil || message.RepairReserve != nil || message.RepairPhase != nil || message.Drain != nil || message.Request != nil || message.Completion != nil || message.BeginInit != nil || message.InitOutcome != nil || message.BootstrapStatus != nil || message.PeerClosure != nil {
			err = fmt.Errorf("fencing: membership-repair status request shape differs")
		} else {
			var result fencing.MembershipRepairStatusResult
			result, err = e.readMembershipRepair(r.Context(), message.Identity, uid, *message.RepairStatus)
			if err == nil {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(result)
				return
			}
		}
	} else if message.Action == "membership-repair-reserve" {
		if message.RepairReserve == nil || message.RepairStatus != nil || message.RepairPhase != nil || message.Drain != nil || message.Request != nil || message.Completion != nil || message.BeginInit != nil || message.InitOutcome != nil || message.BootstrapStatus != nil || message.PeerClosure != nil {
			err = fmt.Errorf("fencing: membership-repair reservation request shape differs")
		} else {
			var result fencing.MembershipRepairReserveResult
			result, err = e.reserveMembershipRepair(r.Context(), message.Identity, uid, *message.RepairReserve)
			if err == nil {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(result)
				return
			}
		}
	} else if message.Action == "membership-repair-authorize" {
		if message.RepairPhase == nil || message.RepairStatus != nil || message.RepairReserve != nil || message.Drain != nil || message.Request != nil || message.Completion != nil || message.BeginInit != nil || message.InitOutcome != nil || message.BootstrapStatus != nil || message.PeerClosure != nil {
			err = fmt.Errorf("fencing: membership-repair authorization request shape differs")
		} else {
			err = e.authorizeMembershipRepairPhase(r.Context(), message.Identity, message.RepairPhase.Phase, uid)
		}
	} else if message.Action == "membership-repair-qualify" {
		if message.RepairQualification == nil || message.RepairStatus != nil || message.RepairReserve != nil || message.RepairPhase != nil || message.RepairPhasePrepare != nil || message.RepairPhaseObserve != nil || message.Drain != nil || message.Request != nil || message.Completion != nil || message.BeginInit != nil || message.InitOutcome != nil || message.BootstrapStatus != nil || message.PeerClosure != nil {
			err = fmt.Errorf("fencing: membership-repair qualification request shape differs")
		} else {
			var result fencing.MembershipRepairQualificationResult
			result, err = e.qualifyMembershipRepair(r.Context(), message.Identity, uid, *message.RepairQualification)
			if err == nil {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(result)
				return
			}
		}
	} else if message.Action == "membership-repair-phase-prepare" {
		if message.RepairPhasePrepare == nil || message.RepairStatus != nil || message.RepairReserve != nil || message.RepairPhase != nil || message.RepairQualification != nil || message.RepairPhaseObserve != nil || message.Drain != nil || message.Request != nil || message.Completion != nil || message.BeginInit != nil || message.InitOutcome != nil || message.BootstrapStatus != nil || message.PeerClosure != nil {
			err = fmt.Errorf("fencing: membership-repair phase-preparation request shape differs")
		} else {
			var result fencing.MembershipRepairPhasePrepareResult
			result, err = e.prepareMembershipRepairPhase(r.Context(), message.Identity, uid, *message.RepairPhasePrepare)
			if err == nil {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(result)
				return
			}
		}
	} else if message.Action == "membership-repair-phase-observe" {
		if message.RepairPhaseObserve == nil || message.RepairStatus != nil || message.RepairReserve != nil || message.RepairPhase != nil || message.RepairQualification != nil || message.RepairPhasePrepare != nil || message.RepairPhaseComplete != nil || message.Drain != nil || message.Request != nil || message.Completion != nil || message.BeginInit != nil || message.InitOutcome != nil || message.BootstrapStatus != nil || message.RuntimeAdmission != nil || message.PeerClosure != nil {
			err = fmt.Errorf("fencing: membership-repair phase-observation request shape differs")
		} else {
			var result fencing.MembershipRepairPhaseObserveResult
			result, err = e.observeMembershipRepairPhase(r.Context(), message.Identity, uid, *message.RepairPhaseObserve)
			if err == nil {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(result)
				return
			}
		}
	} else if message.Action == "membership-repair-phase-complete" {
		if message.RepairPhaseComplete == nil || message.RepairStatus != nil || message.RepairReserve != nil || message.RepairPhase != nil || message.RepairQualification != nil || message.RepairPhasePrepare != nil || message.RepairPhaseObserve != nil || message.Drain != nil || message.Request != nil || message.Completion != nil || message.BeginInit != nil || message.InitOutcome != nil || message.BootstrapStatus != nil || message.RuntimeAdmission != nil || message.PeerClosure != nil {
			err = fmt.Errorf("fencing: membership-repair phase-completion request shape differs")
		} else {
			var result fencing.MembershipRepairPhaseObserveResult
			result, err = e.completeMembershipRepairPhase(r.Context(), message.Identity, uid, *message.RepairPhaseComplete)
			if err == nil {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(result)
				return
			}
		}
	} else {
		if message.Action == "bind" && message.Identity.PodName == "" {
			var pod object
			err = e.Kube.Get(r.Context(), e.controller().corePath("pods", message.Identity.NodeID), &pod)
			if err == nil && str(nested(pod, "metadata", "annotations", fencingPrepareOperationAnnotation)) == "" {
				var binding peerClosureBinding
				var managed bool
				binding, managed, err = e.managedWorkloadBinding(r.Context(), message.Identity, uid, true)
				if err == nil && !managed {
					err = fmt.Errorf("fencing: legacy WAL binding lacks verified managed enrollment")
				}
				if err == nil {
					err = e.validateManagedLegacyIdentity(r.Context(), binding, message.Identity, message.Identity.WALIdentity)
				}
			}
		}
		if err == nil && message.Action == "bind" {
			err = e.recordObservedInitTerminal(r.Context(), message.Identity, uid)
		}
		if err == nil {
			if message.Action == "bind" {
				err = e.bindRuntime(r.Context(), uid, message.Identity)
				if err == nil {
					err = e.recordMembershipRepairBind(r.Context(), uid, message.Identity)
				}
			} else {
				_, err = e.change(r.Context(), uid, message.Identity.SourceClusterID, func(l *fencing.Ledger) error {
					switch message.Action {
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
			}
		}
	}
	if errors.Is(err, fencing.ErrPending) {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if errors.Is(err, fencing.ErrClosed) {
		w.WriteHeader(http.StatusGone)
		return
	}
	if errors.Is(err, fencing.ErrNotFound) {
		http.Error(w, "peer-closure request not found", http.StatusNotFound)
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
	if r.URL.Path == "/v1/peer-closure/request" || r.URL.Path == "/v1/peer-closure/status" {
		_ = json.NewEncoder(w).Encode(peerReceipt)
	} else if message.Action == "register" {
		_ = json.NewEncoder(w).Encode(registration)
	} else if message.Action == "stop" {
		_ = json.NewEncoder(w).Encode(stop)
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

func (e *FencingExecutor) authenticate(ctx context.Context, authorization string, id fencing.Identity, registration bool, authenticatedPodName *string) (string, error) {
	return e.authenticateMode(ctx, authorization, id, registration, false, authenticatedPodName)
}

func (e *FencingExecutor) authenticateMode(ctx context.Context, authorization string, id fencing.Identity, registration, initAction bool, authenticatedPodName *string) (string, error) {
	token, ok := strings.CutPrefix(authorization, "Bearer ")
	if !ok || token == "" || id.PodUID == "" || (!registration && id.RunID == "") || id.SourceClusterID == "" {
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
	if name == "" {
		return "", fmt.Errorf("fencing: authenticated Pod name unavailable")
	}
	if authenticatedPodName != nil {
		*authenticatedPodName = name
	}
	if id.PodName == "" {
		if id.NodeID != name || id.CredentialUID != "" || id.CredentialName != "" {
			return "", fmt.Errorf("fencing: legacy identity must retain its exact Pod name")
		}
	} else if id.PodName != name {
		return "", fmt.Errorf("fencing: replacement Pod name differs from its authenticated binding")
	} else if registration {
		canonical, canonicalErr := fencing.ReplacementNodeID(id.PodUID)
		if id.NodeID != name && (canonicalErr != nil || id.NodeID != canonical) {
			return "", fmt.Errorf("fencing: registration candidate is not bound to the authenticated Pod")
		}
		if id.CredentialUID != "" || id.CredentialName != "" {
			if initAction && id.RunID == "" {
				if id.RunID != "" || id.WALIdentity != "" || id.CredentialUID == "" || id.CredentialName == "" || fencing.ValidatePodBinding(id.NodeID, name, id.PodUID) != nil {
					return "", fmt.Errorf("fencing: pre-runtime credential binding differs")
				}
			} else if id.Validate() != nil {
				return "", fmt.Errorf("fencing: registered credential binding differs")
			}
		}
	} else if id.Validate() != nil {
		return "", fmt.Errorf("fencing: replacement identity differs from its authenticated Pod binding")
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
	if registration && id.RunID == "" {
		if err := validatePreparePod(pod, id.PodUID); err != nil {
			return "", err
		}
	} else if err := validateSupervisedPod(pod); err != nil {
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
	_, _, err := e.learnerCredentialBinding(ctx, pod, stsName)
	return err
}

// learnerCredentialBinding resolves the credential that the supervised
// incarnation registered. Pod names are addresses, not consensus identities;
// the deterministic secret name and its immutable binding use the canonical
// UID-derived NodeID and exact workload/root UIDs.
func (e *FencingExecutor) learnerCredentialBinding(ctx context.Context, pod object, stsName string) (string, string, error) {
	credentialName, credentialUID, _, err := e.learnerCredentialBindingWithLedgerUID(ctx, pod, stsName)
	return credentialName, credentialUID, err
}

func (e *FencingExecutor) learnerCredentialBindingWithLedgerUID(ctx context.Context, pod object, stsName string) (string, string, string, error) {
	scope, err := e.resolveLearnerBindingScope(ctx, pod, stsName)
	if err != nil {
		return "", "", "", err
	}
	ledger, ledgerUID, ledgerObject, err := e.readWithUID(ctx, scope.statefulSetUID, scope.sourceClusterID)
	if err != nil {
		return "", "", "", fmt.Errorf("fencing: learner registration ledger unavailable: %w", err)
	}
	verifiedLedgerUID, err := e.registrationLedgerObjectUID(ledgerObject, scope.statefulSetUID, scope.sourceClusterID)
	if err != nil || verifiedLedgerUID != ledgerUID {
		return "", "", "", fmt.Errorf("fencing: learner registration ledger identity differs")
	}
	var identity *fencing.Identity
	activeRun := false
	for _, run := range ledger.Ledger.Runs {
		registered := run.Identity
		if registered.SourceClusterID != scope.sourceClusterID || registered.NodeID != scope.nodeID || fencing.PodNameForIdentity(registered) != scope.podName || registered.PodUID != scope.podUID {
			continue
		}
		if err := registered.Validate(); err != nil || registered.CredentialName == "" || registered.CredentialUID == "" {
			return "", "", "", fmt.Errorf("fencing: learner registration identity is incomplete")
		}
		if identity != nil && (identity.CredentialName != registered.CredentialName || identity.CredentialUID != registered.CredentialUID) {
			return "", "", "", fmt.Errorf("fencing: learner registration credential history conflicts")
		}
		copy := registered
		identity = &copy
		if run.Stop == nil && run.Drain == nil && !run.Exited {
			activeRun = true
		}
	}
	if identity == nil || !activeRun {
		return "", "", "", fmt.Errorf("fencing: learner has no live retained registration")
	}
	if err := e.validateLearnerCredential(ctx, scope, *identity); err != nil {
		return "", "", "", err
	}
	return identity.CredentialName, identity.CredentialUID, ledgerUID, nil
}

func validateSupervisedPod(pod object) error {
	if str(nested(pod, "metadata", "annotations", fencingProtocolAnnotation)) != "1" {
		return fmt.Errorf("fencing: workload is not enrolled")
	}
	spec := asObject(pod["spec"])
	if spec["hostPID"] == true || spec["shareProcessNamespace"] == true || len(list(spec["containers"])) != 1 || len(list(spec["initContainers"])) > 1 || len(list(spec["ephemeralContainers"])) != 0 {
		return fmt.Errorf("fencing: dedicated private PID namespace required")
	}
	if len(list(spec["initContainers"])) == 1 {
		if err := validatePreparePod(pod, str(nested(pod, "metadata", "uid"))); err != nil {
			return fmt.Errorf("fencing: prepare init profile differs: %w", err)
		}
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
	current, err := e.currentInventory(ctx, r.StatefulSetUID, nodes, r.Targets)
	if err != nil {
		return err
	}
	for _, target := range r.Targets {
		found := false
		for _, run := range state.Ledger.Runs {
			if run.Identity.NodeID == target.NodeID && fencing.PodNameForIdentity(run.Identity) == target.Pod && (target.PodUID == "" || target.PodUID == run.Identity.PodUID) && (target.WALIdentity == "" || target.WALIdentity == run.Identity.WALIdentity) {
				found = true
			}
		}
		if !found {
			return ErrFencePending
		}
	}
	if !state.Ledger.CompleteNodes(r.OperationID, hash, nodes, current) {
		if !state.Ledger.BootstrapInitComplete(nodes) {
			return ErrFencePending
		}
		return e.independentFence(ctx, endpoint, record, r, sts, state, hash, nodes, current)
	}
	proof := &FenceProof{OperationID: r.OperationID, SourceClusterID: r.SourceClusterID, BindingUID: r.BindingUID, RequestHash: hash, ProofID: "cooperative-" + hashJSON(state), ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}
	record.Status.Proof = proof
	return e.Kube.Put(ctx, endpoint+"/"+url.PathEscape(str(record.Metadata["name"]))+"/status", record, nil)
}

// currentInventory lists the Pod UIDs in the fenced scope. An unsupported Pod
// shape blocks independently of which backend would report termination.
func (e *FencingExecutor) currentInventory(ctx context.Context, uid string, nodes []string, targets []FenceTarget) ([]string, error) {
	pods, err := e.controller().ownedPods(ctx, uid)
	if err != nil {
		return nil, err
	}
	// Existing voter-scope requests encode NodeIDs, while Kubernetes indexes
	// the live inventory by Pod name. Translate only from the immutable target
	// binding when checking a voter scope; an unknown mapping must block.
	wanted := map[string]bool{}
	if len(nodes) > 0 {
		wantedNodes := map[string]bool{}
		for _, node := range nodes {
			wantedNodes[node] = true
		}
		for _, target := range targets {
			if !wantedNodes[target.NodeID] || fencing.ValidatePodBinding(target.NodeID, target.Pod, target.PodUID) != nil {
				return nil, fmt.Errorf("fencing: voter inventory has an unknown Pod binding")
			}
			wanted[target.Pod] = true
		}
		seenNodes := map[string]bool{}
		for _, target := range targets {
			seenNodes[target.NodeID] = true
		}
		if len(seenNodes) != len(wantedNodes) {
			return nil, fmt.Errorf("fencing: voter inventory omits a target Pod")
		}
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

// independentFence asks the configured attestor to prove that the exact closed
// incarnation is terminally gone. Every other outcome leaves the fence pending:
// no attestor, an unverifiable response, a ledger that moved during attestation,
// or a Pod inventory that changed. Pod absence, a DELETE response and a failed
// probe are never accepted as termination proof.
func (e *FencingExecutor) independentFence(ctx context.Context, endpoint string, record *fenceResource, r FenceRequest, sts object, state fenceLedger, hash string, nodes, current []string) error {
	if e.Attestor == nil {
		return ErrFencePending
	}
	storage, err := e.storageScope(ctx, sts, r.SourceClusterID)
	if err != nil {
		return err
	}
	scope, err := buildIndependentScope(state, r, nodes, hash, e.ledgerHash(state), current, storage)
	if err != nil {
		return ErrFencePending
	}
	response, err := e.Attestor.Attest(ctx, scope.attestationRequest())
	if err != nil {
		return err
	}
	proof, err := validateIndependent(scope, response)
	if err != nil {
		return ErrFencePending
	}
	// Evidence is only valid for the ledger and Pod inventory it was issued
	// against. Never persist a proof that went stale while the attestor worked.
	rechecked, err := e.read(ctx, state.StatefulSetUID, state.Ledger.SourceClusterID)
	if err != nil {
		return err
	}
	again, err := e.currentInventory(ctx, r.StatefulSetUID, nodes, r.Targets)
	if err != nil {
		return err
	}
	if e.ledgerHash(rechecked) != scope.ledgerHash || !sameStrings(again, current) {
		return ErrFencePending
	}
	record.Status.Proof = proof
	return e.Kube.Put(ctx, endpoint+"/"+url.PathEscape(str(record.Metadata["name"]))+"/status", record, nil)
}
