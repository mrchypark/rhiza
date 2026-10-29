package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// BeginV1Alpha2RecoveryPreparation records intent only; it grants no effect
// authority. Future producers must enforce a complete effects contract.
func BeginV1Alpha2RecoveryPreparation(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, name string, claim V1Alpha2QueueClaim) error {
	return transitionV1Alpha2ClaimIntent(ctx, store, ref, name, claim, false)
}

// RegisterV1Alpha2RecoveryCancellation records a request, not a terminal
// decision. The claim and request finalizer remain in place. Cancellation
// after preparation begins is blocked pending the effects contract.
func RegisterV1Alpha2RecoveryCancellation(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, name string, claim V1Alpha2QueueClaim) error {
	return transitionV1Alpha2ClaimIntent(ctx, store, ref, name, claim, true)
}

func transitionV1Alpha2ClaimIntent(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, name string, claim V1Alpha2QueueClaim, cancel bool) error {
	if claim.Phase != "Claimed" || len(name) > 253 || !v1alpha2Name.MatchString(name) {
		return fmt.Errorf("exact claimed request required")
	}
	snapshot, err := store.Load(ctx, ref)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(claim)
	if err != nil || !sameJSON(snapshot.Queue(), encoded) {
		return fmt.Errorf("claim changed or unknown")
	}
	if err := checkQueueAuthority(snapshot, claim.ExpectedGeneration); err != nil {
		return err
	}
	var raw json.RawMessage
	p := "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/" + ref.Namespace + "/rhizarecoveries/" + name
	if err := store.Kube.Get(ctx, p, &raw); err != nil {
		return fmt.Errorf("read request: %w", err)
	}
	r, err := DecodeV1Alpha2Recovery(raw)
	if err != nil {
		return fmt.Errorf("invalid request: %w", err)
	}
	var uid, ns, requestName, rv, created string
	_ = json.Unmarshal(r.Metadata["uid"], &uid)
	_ = json.Unmarshal(r.Metadata["namespace"], &ns)
	_ = json.Unmarshal(r.Metadata["name"], &requestName)
	_ = json.Unmarshal(r.Metadata["resourceVersion"], &rv)
	_ = json.Unmarshal(r.Metadata["creationTimestamp"], &created)
	timestamp, err := time.Parse(time.RFC3339Nano, created)
	intent := struct {
		Target             V1Alpha2Target `json:"target"`
		ExpectedGeneration int64          `json:"expectedGeneration"`
		Backoff            int32          `json:"backoff"`
		Deadline           *int64         `json:"deadline"`
	}{r.Spec.Target, r.Spec.ExpectedGeneration, effectiveBackoff(r.Spec.BackoffLimit), r.Spec.ActiveDeadlineSeconds}
	intentJSON, _ := json.Marshal(intent)
	digest := sha256.Sum256(intentJSON)
	op := sha256.Sum256([]byte("rhiza-queue-v1\x00" + ref.UID + "\x00" + uid + "\x00" + hex.EncodeToString(digest[:])))
	if err != nil || !timestamp.Equal(claim.CreatedAt) || uid != claim.RequestUID || ns != ref.Namespace || requestName != name || !v1alpha2NonBlank.MatchString(rv) || r.Spec.Target.UID != ref.UID || r.Spec.Target.Name != ref.Name || r.Spec.ExpectedGeneration != claim.ExpectedGeneration || hex.EncodeToString(digest[:]) != claim.IntentHash || hex.EncodeToString(op[:]) != claim.OperationID {
		return fmt.Errorf("request identity or immutable intent changed")
	}
	var finalizers []string
	if json.Unmarshal(r.Metadata["finalizers"], &finalizers) != nil {
		return fmt.Errorf("request finalizer unknown")
	}
	protected := false
	for _, f := range finalizers {
		if f == V1Alpha2RecoveryRecordFinalizer {
			protected = true
		}
	}
	if !protected || (len(bytes.TrimSpace(r.Status)) != 0 && !sameJSON(r.Status, json.RawMessage("{}"))) {
		return fmt.Errorf("request protection or status unknown")
	}
	deletion, exists := r.Metadata["deletionTimestamp"]
	deleting := exists && !bytes.Equal(bytes.TrimSpace(deletion), []byte("null"))
	if cancel != deleting {
		return fmt.Errorf("request deletion state does not match transition")
	}
	if cancel {
		var value string
		if json.Unmarshal(deletion, &value) != nil {
			return fmt.Errorf("invalid deletion timestamp")
		}
		if parsed, err := time.Parse(time.RFC3339Nano, value); err != nil || parsed.IsZero() {
			return fmt.Errorf("invalid deletion timestamp")
		}
		claim.Phase = "CancelRequested"
	} else {
		claim.Phase = "Preparing"
	}
	next, err := json.Marshal(claim)
	if err != nil {
		return err
	}
	_, err = store.compareAndSwapQueue(ctx, snapshot, next, false)
	return err
}
