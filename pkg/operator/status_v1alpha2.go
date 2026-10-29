package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

var (
	ErrV1Alpha2StatusConflict       = errors.New("v1alpha2 status conflict")
	ErrV1Alpha2StatusOutcomeUnknown = errors.New("v1alpha2 status update outcome unknown")
)

type V1Alpha2ClusterRef struct{ Name, Namespace, UID string }
type V1Alpha2StatusStore struct{ Kube *Kubernetes }

type V1Alpha2AuthorityOwner struct{ Namespace, Name, UID, LogicalID string }
type V1Alpha2AuthorityBinding struct{ DeploymentUID, StorageID string }
type V1Alpha2AuthorityView struct {
	Owner      V1Alpha2AuthorityOwner
	Generation int64
	Binding    V1Alpha2AuthorityBinding
}

// V1Alpha2StatusSnapshot is obtained only from Load or a successful CAS.
// Its accessors return copies; the private preconditions cannot be forged by callers.
type V1Alpha2StatusSnapshot struct {
	ref             V1Alpha2ClusterRef
	resourceVersion string
	authority       V1Alpha2AuthorityView
	authorityRaw    json.RawMessage
	queue           json.RawMessage
	object          map[string]json.RawMessage
	store           *V1Alpha2StatusStore
}

func (s V1Alpha2StatusSnapshot) Ref() V1Alpha2ClusterRef          { return s.ref }
func (s V1Alpha2StatusSnapshot) ResourceVersion() string          { return s.resourceVersion }
func (s V1Alpha2StatusSnapshot) Authority() V1Alpha2AuthorityView { return s.authority }
func (s V1Alpha2StatusSnapshot) Queue() json.RawMessage           { return bytes.Clone(s.queue) }

func (s *V1Alpha2StatusStore) path(ref V1Alpha2ClusterRef) (string, error) {
	if s == nil || s.Kube == nil || !validNamespace(ref.Namespace) || s.Kube.Namespace != ref.Namespace || len(ref.Name) == 0 || len(ref.Name) > 253 || !v1alpha2Name.MatchString(ref.Name) || !v1alpha2NonBlank.MatchString(ref.UID) {
		return "", fmt.Errorf("status store requires an exact same-namespace cluster identity")
	}
	return "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/" + ref.Namespace + "/rhizaclusters/" + ref.Name, nil
}

func (s *V1Alpha2StatusStore) Load(ctx context.Context, ref V1Alpha2ClusterRef) (V1Alpha2StatusSnapshot, error) {
	p, err := s.path(ref)
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	var obj map[string]json.RawMessage
	if err := s.Kube.Get(ctx, p, &obj); err != nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("load cluster status: %w", err)
	}
	return s.snapshot(ref, obj)
}

func jsonObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	var value map[string]json.RawMessage
	if err := json.Unmarshal(raw, &value); err != nil || value == nil {
		return nil, fmt.Errorf("expected JSON object")
	}
	return value, nil
}

func (s *V1Alpha2StatusStore) snapshot(ref V1Alpha2ClusterRef, obj map[string]json.RawMessage) (V1Alpha2StatusSnapshot, error) {
	var version, kind string
	_ = json.Unmarshal(obj["apiVersion"], &version)
	_ = json.Unmarshal(obj["kind"], &kind)
	meta, err := jsonObject(obj["metadata"])
	if err != nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("invalid cluster metadata")
	}
	var name, ns, uid, rv string
	_ = json.Unmarshal(meta["name"], &name)
	_ = json.Unmarshal(meta["namespace"], &ns)
	_ = json.Unmarshal(meta["uid"], &uid)
	_ = json.Unmarshal(meta["resourceVersion"], &rv)
	if version != V1Alpha2APIVersion || kind != "RhizaCluster" || name != ref.Name || ns != ref.Namespace || uid != ref.UID || !v1alpha2NonBlank.MatchString(rv) {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("cluster identity or resourceVersion mismatch")
	}
	if raw, ok := meta["deletionTimestamp"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("cluster is deleting")
	}
	spec, err := jsonObject(obj["spec"])
	if err != nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("missing or invalid cluster spec")
	}
	var logicalID string
	if json.Unmarshal(spec["logicalID"], &logicalID) != nil || !v1alpha2NonBlank.MatchString(logicalID) {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("invalid cluster logicalID")
	}
	status, err := jsonObject(obj["status"])
	if err != nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("missing or invalid cluster status")
	}
	auth, err := jsonObject(status["authority"])
	if err != nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("missing or invalid authority")
	}
	owner, err := jsonObject(auth["owner"])
	if err != nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("invalid authority owner")
	}
	binding, err := jsonObject(auth["binding"])
	if err != nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("invalid authority binding")
	}
	var view V1Alpha2AuthorityView
	for key, dst := range map[string]*string{"namespace": &view.Owner.Namespace, "name": &view.Owner.Name, "uid": &view.Owner.UID, "logicalID": &view.Owner.LogicalID} {
		if json.Unmarshal(owner[key], dst) != nil || !v1alpha2NonBlank.MatchString(*dst) {
			return V1Alpha2StatusSnapshot{}, fmt.Errorf("invalid authority owner %s", key)
		}
	}
	for key, dst := range map[string]*string{"deploymentUID": &view.Binding.DeploymentUID, "storageID": &view.Binding.StorageID} {
		if json.Unmarshal(binding[key], dst) != nil || !v1alpha2NonBlank.MatchString(*dst) {
			return V1Alpha2StatusSnapshot{}, fmt.Errorf("invalid authority binding %s", key)
		}
	}
	if view.Owner.Namespace != ref.Namespace || view.Owner.Name != ref.Name || view.Owner.UID != ref.UID || view.Owner.LogicalID != logicalID || len(auth["generation"]) == 0 || bytes.Equal(bytes.TrimSpace(auth["generation"]), []byte("null")) || json.Unmarshal(auth["generation"], &view.Generation) != nil || view.Generation < 0 {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("invalid authority identity or generation")
	}
	if _, intent := spec["genesisIntent"]; intent {
		var value string
		if json.Unmarshal(spec["genesisIntent"], &value) != nil || value != "Create" || validateGenesisReceipt(status["genesis"], ref, logicalID) != nil {
			return V1Alpha2StatusSnapshot{}, fmt.Errorf("missing or invalid genesis receipt")
		}
		var receipt v1alpha2Genesis
		_ = json.Unmarshal(status["genesis"], &receipt)
		if view.Generation == 0 && (view.Binding.DeploymentUID != receipt.DeploymentUID || view.Binding.StorageID != receipt.StorageID) {
			return V1Alpha2StatusSnapshot{}, fmt.Errorf("genesis binding mismatch")
		}
	} else if _, present := status["genesis"]; present {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("genesis receipt without creation intent")
	}
	return V1Alpha2StatusSnapshot{ref: ref, resourceVersion: rv, authority: view, authorityRaw: bytes.Clone(status["authority"]), queue: bytes.Clone(status["recoveryQueue"]), object: obj, store: s}, nil
}

func sameJSON(a, b json.RawMessage) bool {
	var x, y any
	d := json.NewDecoder(bytes.NewReader(a))
	d.UseNumber()
	if d.Decode(&x) != nil {
		return false
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if d.Decode(&y) != nil {
		return false
	}
	xb, e1 := json.Marshal(x)
	yb, e2 := json.Marshal(y)
	return e1 == nil && e2 == nil && bytes.Equal(xb, yb)
}

// compareAndSwapQueueObserved changes only status.recoveryQueue. Queue JSON is opaque
// bookkeeping; successful storage does not validate policy or authorize work.
// Matching readback proves only the logical postcondition, not which write applied it.
func (s *V1Alpha2StatusStore) compareAndSwapQueueObserved(ctx context.Context, old V1Alpha2StatusSnapshot, next json.RawMessage) (V1Alpha2StatusSnapshot, error) {
	return s.compareAndSwapQueue(ctx, old, next, true)
}

func (s *V1Alpha2StatusStore) compareAndSwapQueue(ctx context.Context, old V1Alpha2StatusSnapshot, next json.RawMessage, allowReadback bool) (V1Alpha2StatusSnapshot, error) {
	if old.store != s || old.object == nil || old.resourceVersion == "" {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("invalid status snapshot")
	}
	if next != nil {
		if _, err := jsonObject(next); err != nil {
			return V1Alpha2StatusSnapshot{}, fmt.Errorf("queue must be a JSON object")
		}
	}
	p, err := s.path(old.ref)
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	status, err := jsonObject(old.object["status"])
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	if next == nil {
		delete(status, "recoveryQueue")
	} else {
		status["recoveryQueue"] = bytes.Clone(next)
	}
	statusRaw, err := json.Marshal(status)
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	obj := make(map[string]json.RawMessage, len(old.object))
	for k, v := range old.object {
		obj[k] = v
	}
	obj["status"] = statusRaw
	var response map[string]json.RawMessage
	err = s.Kube.Put(ctx, p+"/status", obj, &response)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("%w: %v", ErrV1Alpha2StatusConflict, err)
	}
	if err == nil {
		result, e := s.snapshot(old.ref, response)
		if e == nil && sameJSON(result.object["status"], statusRaw) && result.resourceVersion != old.resourceVersion {
			return result, nil
		}
	}
	if !allowReadback {
		live, readErr := s.Load(ctx, old.ref)
		if readErr == nil && live.resourceVersion != old.resourceVersion && sameJSON(live.object["status"], statusRaw) {
			return V1Alpha2StatusSnapshot{}, fmt.Errorf("%w: matching readback cannot identify the writer", ErrV1Alpha2StatusOutcomeUnknown)
		}
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("%w: update and diagnostic readback did not establish the result", ErrV1Alpha2StatusOutcomeUnknown)
	}
	// A failed or malformed response may follow an applied write. Never retry it.
	live, readErr := s.Load(ctx, old.ref)
	if readErr == nil && live.resourceVersion != old.resourceVersion && sameJSON(live.object["status"], statusRaw) {
		return live, nil
	}
	return V1Alpha2StatusSnapshot{}, fmt.Errorf("%w: update or readback could not establish the result", ErrV1Alpha2StatusOutcomeUnknown)
}
