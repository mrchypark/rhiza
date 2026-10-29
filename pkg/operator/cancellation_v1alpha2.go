package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type V1Alpha2CancellationResult string

const (
	V1Alpha2CancellationPending  V1Alpha2CancellationResult = "Pending"
	V1Alpha2CancellationComplete V1Alpha2CancellationResult = "Complete"
)

type v1alpha2CanceledDecision struct {
	Claim        V1Alpha2QueueClaim       `json:"claim"`
	Binding      V1Alpha2AuthorityBinding `json:"binding"`
	Outcome      string                   `json:"outcome"`
	Acknowledged bool                     `json:"acknowledged,omitempty"`
}

// ReconcileV1Alpha2Cancellation is valid only while this package has no
// effectful v1alpha2 producers. Preparing and all unknown phases remain blocked.
// It performs at most one write per call; uncertain outcomes require readback.
func ReconcileV1Alpha2Cancellation(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, name string, claim V1Alpha2QueueClaim) (V1Alpha2CancellationResult, error) {
	pending := V1Alpha2CancellationPending
	if claim.Phase != "CancelRequested" || len(name) > 253 || !v1alpha2Name.MatchString(name) {
		return pending, fmt.Errorf("exact cancellation claim required")
	}
	snap, err := store.Load(ctx, ref)
	if err != nil {
		return pending, err
	}
	if err = checkQueueAuthority(snap, claim.ExpectedGeneration); err != nil {
		return pending, err
	}
	claimJSON, _ := json.Marshal(claim)
	decision := v1alpha2CanceledDecision{Claim: claim, Binding: snap.Authority().Binding, Outcome: "Canceled"}
	decisionJSON, _ := json.Marshal(decision)
	decision.Acknowledged = true
	acknowledgedJSON, _ := json.Marshal(decision)
	acknowledged := sameJSON(snap.Queue(), acknowledgedJSON)
	if sameJSON(snap.Queue(), claimJSON) {
		obj, err := cancellationRequest(ctx, store, ref, name, claim)
		if err != nil {
			return pending, err
		}
		if err := cancellationDeletingProtected(obj, true); err != nil {
			return pending, err
		}
		if status := obj["status"]; len(bytes.TrimSpace(status)) != 0 && !sameJSON(status, json.RawMessage("{}")) {
			return pending, fmt.Errorf("request status unknown")
		}
		_, err = store.compareAndSwapQueue(ctx, snap, decisionJSON, false)
		return pending, err
	}
	if !sameJSON(snap.Queue(), decisionJSON) && !acknowledged {
		return pending, fmt.Errorf("cancellation decision or binding changed")
	}

	present, err := cancellationUIDPresent(ctx, store, ref, claim.RequestUID)
	if err != nil {
		return pending, err
	}
	if !present {
		if !acknowledged {
			return pending, fmt.Errorf("request disappeared before acknowledgment proof")
		}
		_, err = store.compareAndSwapQueue(ctx, snap, nil, true)
		if err != nil {
			return pending, err
		}
		return V1Alpha2CancellationComplete, nil
	}
	obj, err := cancellationRequest(ctx, store, ref, name, claim)
	if err != nil {
		return pending, err
	}
	if err := cancellationDeletingProtected(obj, !acknowledged); err != nil {
		return pending, err
	}
	status := obj["status"]
	if len(bytes.TrimSpace(status)) == 0 || sameJSON(status, json.RawMessage("{}")) {
		if acknowledged {
			return pending, fmt.Errorf("recorded acknowledgment is missing")
		}
		obj["status"] = decisionJSON
		return pending, cancellationPut(ctx, store, ref, name, obj, true, decisionJSON)
	}
	if !sameJSON(status, decisionJSON) {
		return pending, fmt.Errorf("request acknowledgment changed or unknown")
	}
	if !acknowledged {
		_, err = store.compareAndSwapQueue(ctx, snap, acknowledgedJSON, false)
		return pending, err
	}
	live, err := store.Load(ctx, ref)
	if err != nil || !sameJSON(live.Queue(), acknowledgedJSON) || !sameJSON(live.authorityRaw, snap.authorityRaw) {
		return pending, fmt.Errorf("cancellation decision or authority changed")
	}
	meta, protected, err := finalizerMetadata(obj, V1Alpha2RecoveryRef{Namespace: ref.Namespace, Name: name, UID: claim.RequestUID})
	if err != nil {
		return pending, err
	}
	if !protected {
		return pending, nil
	}
	var finalizers []string
	if json.Unmarshal(meta["finalizers"], &finalizers) != nil {
		return pending, fmt.Errorf("invalid finalizers")
	}
	filtered := make([]string, 0, len(finalizers))
	for _, f := range finalizers {
		if f != V1Alpha2RecoveryRecordFinalizer {
			filtered = append(filtered, f)
		}
	}
	meta["finalizers"], _ = json.Marshal(filtered)
	obj["metadata"], _ = json.Marshal(meta)
	return pending, cancellationPut(ctx, store, ref, name, obj, false, decisionJSON)
}

func cancellationRequest(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, name string, claim V1Alpha2QueueClaim) (map[string]json.RawMessage, error) {
	var obj map[string]json.RawMessage
	p := "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/" + ref.Namespace + "/rhizarecoveries/" + name
	if err := store.Kube.Get(ctx, p, &obj); err != nil {
		return nil, fmt.Errorf("read exact request: %w", err)
	}
	raw, _ := json.Marshal(obj)
	r, err := DecodeV1Alpha2Recovery(raw)
	if err != nil {
		return nil, err
	}
	meta, _, err := finalizerMetadata(obj, V1Alpha2RecoveryRef{Namespace: ref.Namespace, Name: name, UID: claim.RequestUID})
	if err != nil {
		return nil, err
	}
	var created string
	_ = json.Unmarshal(meta["creationTimestamp"], &created)
	timestamp, err := time.Parse(time.RFC3339Nano, created)
	intent := struct {
		Target             V1Alpha2Target `json:"target"`
		ExpectedGeneration int64          `json:"expectedGeneration"`
		Backoff            int32          `json:"backoff"`
		Deadline           *int64         `json:"deadline"`
	}{r.Spec.Target, r.Spec.ExpectedGeneration, effectiveBackoff(r.Spec.BackoffLimit), r.Spec.ActiveDeadlineSeconds}
	encoded, _ := json.Marshal(intent)
	digest := sha256.Sum256(encoded)
	hash := hex.EncodeToString(digest[:])
	op := sha256.Sum256([]byte("rhiza-queue-v1\x00" + ref.UID + "\x00" + claim.RequestUID + "\x00" + hash))
	if err != nil || !timestamp.Equal(claim.CreatedAt) || r.Spec.Target.Name != ref.Name || r.Spec.Target.UID != ref.UID || r.Spec.ExpectedGeneration != claim.ExpectedGeneration || hash != claim.IntentHash || hex.EncodeToString(op[:]) != claim.OperationID {
		return nil, fmt.Errorf("request identity or intent changed")
	}
	return obj, nil
}

func cancellationDeletingProtected(obj map[string]json.RawMessage, requireProtected bool) error {
	meta, err := jsonObject(obj["metadata"])
	if err != nil {
		return err
	}
	var deletion string
	if json.Unmarshal(meta["deletionTimestamp"], &deletion) != nil {
		return fmt.Errorf("deletion is unknown")
	}
	t, err := time.Parse(time.RFC3339Nano, deletion)
	if err != nil || t.IsZero() {
		return fmt.Errorf("invalid deletion timestamp")
	}
	var finalizers []string
	raw := bytes.TrimSpace(meta["finalizers"])
	if !requireProtected && (len(raw) == 0 || bytes.Equal(raw, []byte("null"))) {
		return nil
	}
	if json.Unmarshal(raw, &finalizers) != nil || finalizers == nil {
		return fmt.Errorf("finalizers unknown")
	}
	if requireProtected {
		for _, f := range finalizers {
			if f == V1Alpha2RecoveryRecordFinalizer {
				return nil
			}
		}
		return fmt.Errorf("request is not protected")
	}
	return nil
}

func cancellationUIDPresent(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, uid string) (bool, error) {
	var list struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			ResourceVersion string `json:"resourceVersion"`
			Continue        string `json:"continue"`
		} `json:"metadata"`
		Items []json.RawMessage `json:"items"`
	}
	p := "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/" + ref.Namespace + "/rhizarecoveries"
	if err := store.Kube.Get(ctx, p, &list); err != nil {
		return false, err
	}
	if list.APIVersion != V1Alpha2APIVersion || list.Kind != "RhizaRecoveryList" || !v1alpha2NonBlank.MatchString(list.Metadata.ResourceVersion) || list.Metadata.Continue != "" || list.Items == nil {
		return false, fmt.Errorf("incomplete recovery list")
	}
	seen := map[string]bool{}
	names := map[string]bool{}
	present := false
	for _, raw := range list.Items {
		r, err := DecodeV1Alpha2Recovery(raw)
		if err != nil {
			return false, fmt.Errorf("invalid recovery list item")
		}
		var id, ns, name, rv, created string
		_ = json.Unmarshal(r.Metadata["uid"], &id)
		_ = json.Unmarshal(r.Metadata["namespace"], &ns)
		_ = json.Unmarshal(r.Metadata["name"], &name)
		_ = json.Unmarshal(r.Metadata["resourceVersion"], &rv)
		_ = json.Unmarshal(r.Metadata["creationTimestamp"], &created)
		t, err := time.Parse(time.RFC3339Nano, created)
		if !v1alpha2NonBlank.MatchString(id) || seen[id] || names[name] || ns != ref.Namespace || len(name) > 253 || !v1alpha2Name.MatchString(name) || !v1alpha2NonBlank.MatchString(rv) || err != nil || t.IsZero() {
			return false, fmt.Errorf("invalid recovery list identity")
		}
		seen[id] = true
		names[name] = true
		if id == uid {
			present = true
		}
	}
	return present, nil
}

func cancellationPut(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, name string, obj map[string]json.RawMessage, status bool, decision json.RawMessage) error {
	p := "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/" + ref.Namespace + "/rhizarecoveries/" + name
	if status {
		p += "/status"
	}
	var response map[string]json.RawMessage
	err := store.Kube.Put(ctx, p, obj, &response)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict {
		return ErrV1Alpha2StatusConflict
	}
	// A later reconciliation always performs exact fresh reads; never replay here.
	if err != nil {
		return ErrV1Alpha2StatusOutcomeUnknown
	}
	oldMeta, e1 := jsonObject(obj["metadata"])
	var oldUID, oldRV string
	_ = json.Unmarshal(oldMeta["uid"], &oldUID)
	_ = json.Unmarshal(oldMeta["resourceVersion"], &oldRV)
	exact := V1Alpha2RecoveryRef{Namespace: ref.Namespace, Name: name, UID: oldUID}
	_, _, oldErr := finalizerMetadata(obj, exact)
	newMeta, _, newErr := finalizerMetadata(response, exact)
	var newRV string
	_ = json.Unmarshal(newMeta["resourceVersion"], &newRV)
	if e1 != nil || oldErr != nil || newErr != nil || !v1alpha2NonBlank.MatchString(oldUID) || !v1alpha2NonBlank.MatchString(oldRV) || oldRV == newRV || !sameJSON(response["status"], decision) {
		return ErrV1Alpha2StatusOutcomeUnknown
	}
	// Only the server-managed revision and managedFields may differ.
	delete(oldMeta, "managedFields")
	delete(newMeta, "managedFields")
	newMeta["resourceVersion"] = bytes.Clone(oldMeta["resourceVersion"])
	if !status {
		var intended []string
		if json.Unmarshal(oldMeta["finalizers"], &intended) == nil && len(intended) == 0 {
			var actual []string
			raw := bytes.TrimSpace(newMeta["finalizers"])
			if len(raw) == 0 || bytes.Equal(raw, []byte("null")) || (json.Unmarshal(raw, &actual) == nil && len(actual) == 0) {
				oldMeta["finalizers"] = json.RawMessage("[]")
				newMeta["finalizers"] = json.RawMessage("[]")
			}
		}
	}
	expected := make(map[string]json.RawMessage, len(obj))
	actual := make(map[string]json.RawMessage, len(response))
	for key, value := range obj {
		expected[key] = value
	}
	for key, value := range response {
		actual[key] = value
	}
	expected["metadata"], _ = json.Marshal(oldMeta)
	actual["metadata"], _ = json.Marshal(newMeta)
	expectedJSON, e1 := json.Marshal(expected)
	actualJSON, e2 := json.Marshal(actual)
	if e1 != nil || e2 != nil || !sameJSON(expectedJSON, actualJSON) {
		return ErrV1Alpha2StatusOutcomeUnknown
	}
	return nil
}
