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

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const fencingProtocolAnnotation = "rhiza.mrchypark.dev/cooperative-fencing"
const fencingRootAnnotation = "rhiza.mrchypark.dev/fencing-root-uid"
const fencingCredentialAnnotation = "rhiza.mrchypark.dev/fencing-credential-uid"

// FencingExecutor is a namespaced cooperative authority. Its credentials are
// separate from the operator and application accounts. Applications may request
// only transitions for their TokenReview-authenticated Pod incarnation.
//
// Attestor is optional external evidence for an incarnation no supervisor can
// report on. It is consulted only after the cooperative ledger is durably closed
// and only when the cooperative proof is still incomplete.
type FencingExecutor struct {
	Kube     *Kubernetes
	Attestor FenceAttestor
}

type fenceLedger struct {
	StatefulSetUID string         `json:"statefulSetUID"`
	Ledger         fencing.Ledger `json:"ledger"`
}

func (e *FencingExecutor) controller() *Controller { return &Controller{Kube: e.Kube} }

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
		return state, fmt.Errorf("fencing: ledger differs")
	}
	return state, nil
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
	var stop fencing.StopRequest
	var peerReceipt fencing.PeerClosureReceipt
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
		_, err = e.change(r.Context(), uid, message.Identity.SourceClusterID, func(l *fencing.Ledger) error {
			switch message.Action {
			case "register":
				return l.Register(message.Identity)
			case "bind":
				return l.Bind(message.Identity)
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
	again, err := e.currentInventory(ctx, r.StatefulSetUID, nodes)
	if err != nil {
		return err
	}
	if e.ledgerHash(rechecked) != scope.ledgerHash || !sameStrings(again, current) {
		return ErrFencePending
	}
	record.Status.Proof = proof
	return e.Kube.Put(ctx, endpoint+"/"+url.PathEscape(str(record.Metadata["name"]))+"/status", record, nil)
}
