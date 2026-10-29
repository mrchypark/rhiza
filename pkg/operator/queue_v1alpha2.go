package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// V1Alpha2QueueClaim is exclusive bookkeeping, not permission to execute.
type V1Alpha2QueueClaim struct {
	RequestUID         string    `json:"requestUID"`
	OperationID        string    `json:"operationID"`
	IntentHash         string    `json:"intentHash"`
	ExpectedGeneration int64     `json:"expectedGeneration"`
	CreatedAt          time.Time `json:"createdAt"`
	Phase              string    `json:"phase,omitempty"`
}

const V1Alpha2RecoveryRecordFinalizer = "rhiza.mrchypark.dev/recovery-record"

type V1Alpha2QueueBlock string

const (
	V1Alpha2QueueIncomplete        V1Alpha2QueueBlock = "IncompleteSnapshot"
	V1Alpha2QueueActiveMissing     V1Alpha2QueueBlock = "ActiveRequestMissingOrChanged"
	V1Alpha2QueueGenerationChanged V1Alpha2QueueBlock = "TargetGenerationChanged"
	V1Alpha2QueueAuthorityBusy     V1Alpha2QueueBlock = "AuthorityBusyOrUnknown"
	V1Alpha2QueueRequestBlocked    V1Alpha2QueueBlock = "RequestRequiresReconciliation"
)

type V1Alpha2QueueError struct {
	Reason V1Alpha2QueueBlock
	Detail string
}

func (e *V1Alpha2QueueError) Error() string { return string(e.Reason) + ": " + e.Detail }
func queueError(reason V1Alpha2QueueBlock, detail string) error {
	return &V1Alpha2QueueError{Reason: reason, Detail: detail}
}

type v1alpha2QueueEntry struct {
	request V1Alpha2Recovery
	uid     string
	name    string
	rv      string
	created time.Time
	hash    string
}

// ClaimV1Alpha2Recovery assumes all participating controllers protect requests
// before effects and retain the finalizer until an authoritative decision.
// Forced deletion outside that trusted scope and producer integration are not
// proved here. A stale claim is never cleared by this library. Separate reads
// of two resources are not atomic: a returned claim is bookkeeping observation,
// not readiness or permission for effects. Downstream fresh authority and
// storage-generation gates and the trusted finalizer protocol remain required.
func ClaimV1Alpha2Recovery(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef) (V1Alpha2QueueClaim, error) {
	snapshot, err := store.Load(ctx, ref)
	if err != nil {
		return V1Alpha2QueueClaim{}, err
	}
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
		return V1Alpha2QueueClaim{}, fmt.Errorf("list recoveries: %w", err)
	}
	if list.APIVersion != V1Alpha2APIVersion || list.Kind != "RhizaRecoveryList" || !v1alpha2NonBlank.MatchString(list.Metadata.ResourceVersion) || list.Metadata.Continue != "" || list.Items == nil {
		return V1Alpha2QueueClaim{}, queueError(V1Alpha2QueueIncomplete, "invalid or incomplete recovery list")
	}
	entries := make([]v1alpha2QueueEntry, 0, len(list.Items))
	seen := map[string]bool{}
	for _, raw := range list.Items {
		r, err := DecodeV1Alpha2Recovery(raw)
		if err != nil {
			return V1Alpha2QueueClaim{}, queueError(V1Alpha2QueueIncomplete, "invalid recovery in list")
		}
		var uid, ns, name, timestamp, rv string
		if json.Unmarshal(r.Metadata["uid"], &uid) != nil || json.Unmarshal(r.Metadata["namespace"], &ns) != nil || json.Unmarshal(r.Metadata["name"], &name) != nil || json.Unmarshal(r.Metadata["resourceVersion"], &rv) != nil || json.Unmarshal(r.Metadata["creationTimestamp"], &timestamp) != nil || !v1alpha2NonBlank.MatchString(uid) || ns != ref.Namespace || len(name) > 253 || !v1alpha2Name.MatchString(name) || !v1alpha2NonBlank.MatchString(rv) || seen[uid] {
			return V1Alpha2QueueClaim{}, queueError(V1Alpha2QueueIncomplete, "invalid or duplicate recovery identity")
		}
		created, err := time.Parse(time.RFC3339Nano, timestamp)
		if err != nil || created.IsZero() {
			return V1Alpha2QueueClaim{}, queueError(V1Alpha2QueueIncomplete, "invalid creation timestamp")
		}
		seen[uid] = true
		if r.Spec.Target.UID != ref.UID {
			continue
		}
		if r.Spec.Target.Name != ref.Name {
			return V1Alpha2QueueClaim{}, queueError(V1Alpha2QueueIncomplete, "target UID and name disagree")
		}
		// TTL is mutable retention policy, not immutable execution intent.
		intent := struct {
			Target             V1Alpha2Target `json:"target"`
			ExpectedGeneration int64          `json:"expectedGeneration"`
			Backoff            int32          `json:"backoff"`
			Deadline           *int64         `json:"deadline"`
		}{r.Spec.Target, r.Spec.ExpectedGeneration, effectiveBackoff(r.Spec.BackoffLimit), r.Spec.ActiveDeadlineSeconds}
		encoded, _ := json.Marshal(intent)
		digest := sha256.Sum256(encoded)
		entries = append(entries, v1alpha2QueueEntry{r, uid, name, rv, created, hex.EncodeToString(digest[:])})
	}
	// Equal server timestamps have a deterministic UID tie-break, not proof of
	// hidden chronological order or a global wall clock.
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].created.Equal(entries[j].created) {
			return entries[i].uid < entries[j].uid
		}
		return entries[i].created.Before(entries[j].created)
	})
	var existing V1Alpha2QueueClaim
	raw := bytes.TrimSpace(snapshot.Queue())
	if len(raw) > 0 {
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(raw, &fields)
		if err := decodeV1Alpha2(raw, &existing); err != nil || len(fields["expectedGeneration"]) == 0 || bytes.Equal(bytes.TrimSpace(fields["expectedGeneration"]), []byte("null")) || existing.RequestUID == "" || existing.OperationID == "" || existing.IntentHash == "" || existing.CreatedAt.IsZero() || existing.ExpectedGeneration < 0 {
			return V1Alpha2QueueClaim{}, queueError(V1Alpha2QueueIncomplete, "unsupported or invalid queue claim")
		}
		if existing.Phase != "" && existing.Phase != "Claimed" {
			return V1Alpha2QueueClaim{}, queueError(V1Alpha2QueueRequestBlocked, "claim requires lifecycle reconciliation")
		}
		for _, entry := range entries {
			op := sha256.Sum256([]byte("rhiza-queue-v1\x00" + ref.UID + "\x00" + entry.uid + "\x00" + entry.hash))
			if entry.uid == existing.RequestUID && entry.hash == existing.IntentHash && existing.OperationID == hex.EncodeToString(op[:]) && entry.request.Spec.ExpectedGeneration == existing.ExpectedGeneration && entry.created.Equal(existing.CreatedAt) {
				if err := checkQueueRequest(ctx, store, ref, entry); err != nil {
					return V1Alpha2QueueClaim{}, err
				}
				if err := checkQueueAuthority(snapshot, existing.ExpectedGeneration); err != nil {
					return V1Alpha2QueueClaim{}, err
				}
				if err := observeQueueClaim(ctx, store, snapshot, entry, existing); err != nil {
					return V1Alpha2QueueClaim{}, err
				}
				return existing, nil
			}
		}
		return V1Alpha2QueueClaim{}, queueError(V1Alpha2QueueActiveMissing, "active request absent or immutable intent changed")
	}
	if len(entries) == 0 {
		return V1Alpha2QueueClaim{}, queueError(V1Alpha2QueueIncomplete, "no request available")
	}
	head := entries[0]
	if err := checkQueueRequest(ctx, store, ref, head); err != nil {
		return V1Alpha2QueueClaim{}, err
	}
	if err := checkQueueAuthority(snapshot, head.request.Spec.ExpectedGeneration); err != nil {
		return V1Alpha2QueueClaim{}, err
	}
	op := sha256.Sum256([]byte("rhiza-queue-v1\x00" + ref.UID + "\x00" + head.uid + "\x00" + head.hash))
	claim := V1Alpha2QueueClaim{RequestUID: head.uid, OperationID: hex.EncodeToString(op[:]), IntentHash: head.hash, ExpectedGeneration: head.request.Spec.ExpectedGeneration, CreatedAt: head.created, Phase: "Claimed"}
	next, err := json.Marshal(claim)
	if err != nil {
		return V1Alpha2QueueClaim{}, err
	}
	if _, err := store.CompareAndSwapQueue(ctx, snapshot, next); err != nil {
		return V1Alpha2QueueClaim{}, err
	}
	if err := observeQueueClaim(ctx, store, snapshot, head, claim); err != nil {
		return V1Alpha2QueueClaim{}, err
	}
	return claim, nil
}

// An observation failure leaves the claim untouched for exact reconciliation.
func observeQueueClaim(ctx context.Context, store *V1Alpha2StatusStore, initial V1Alpha2StatusSnapshot, entry v1alpha2QueueEntry, claim V1Alpha2QueueClaim) error {
	if err := checkQueueRequest(ctx, store, initial.Ref(), entry); err != nil {
		return err
	}
	live, err := store.Load(ctx, initial.Ref())
	if err != nil {
		return queueError(V1Alpha2QueueAuthorityBusy, "post-claim authority read failed")
	}
	if live.Ref() != initial.Ref() || !sameJSON(live.authorityRaw, initial.authorityRaw) {
		return queueError(V1Alpha2QueueAuthorityBusy, "authority changed during claim observation")
	}
	if err := checkQueueAuthority(live, claim.ExpectedGeneration); err != nil {
		return err
	}
	expected, err := json.Marshal(claim)
	if err != nil || !sameJSON(live.Queue(), expected) {
		return queueError(V1Alpha2QueueRequestBlocked, "claim changed or is unknown during observation")
	}
	return nil
}

func checkQueueAuthority(snapshot V1Alpha2StatusSnapshot, expected int64) error {
	var authority map[string]json.RawMessage
	if json.Unmarshal(snapshot.authorityRaw, &authority) != nil {
		return queueError(V1Alpha2QueueAuthorityBusy, "invalid authority")
	}
	if !bytes.Equal(bytes.TrimSpace(authority["pendingWrite"]), []byte("false")) {
		return queueError(V1Alpha2QueueAuthorityBusy, "pendingWrite is not explicitly false")
	}
	if value, ok := authority["transition"]; ok && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return queueError(V1Alpha2QueueAuthorityBusy, "transition is present or invalid")
	}
	if expected != snapshot.Authority().Generation {
		return queueError(V1Alpha2QueueGenerationChanged, "fresh independent classification or own-commit reconciliation required")
	}
	return nil
}

func checkQueueRequest(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, entry v1alpha2QueueEntry) error {
	if err := queueRequestReady(entry.request); err != nil {
		return err
	}
	var raw json.RawMessage
	p := "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/" + ref.Namespace + "/rhizarecoveries/" + entry.name
	if err := store.Kube.Get(ctx, p, &raw); err != nil {
		return queueError(V1Alpha2QueueRequestBlocked, "fresh request read failed")
	}
	live, err := DecodeV1Alpha2Recovery(raw)
	if err != nil {
		return queueError(V1Alpha2QueueRequestBlocked, "fresh request is invalid")
	}
	var uid, rv, name, ns string
	_ = json.Unmarshal(live.Metadata["uid"], &uid)
	_ = json.Unmarshal(live.Metadata["resourceVersion"], &rv)
	_ = json.Unmarshal(live.Metadata["name"], &name)
	_ = json.Unmarshal(live.Metadata["namespace"], &ns)
	if uid != entry.uid || rv != entry.rv || name != entry.name || ns != ref.Namespace || ValidateV1Alpha2RecoveryUpdate(entry.request, live) != nil || !sameJSON(live.Metadata["creationTimestamp"], entry.request.Metadata["creationTimestamp"]) {
		return queueError(V1Alpha2QueueRequestBlocked, "fresh request identity, revision or intent changed")
	}
	return queueRequestReady(live)
}

func queueRequestReady(r V1Alpha2Recovery) error {
	if raw, ok := r.Metadata["deletionTimestamp"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return queueError(V1Alpha2QueueRequestBlocked, "request deletion requires reconciliation")
	}
	var finalizers []string
	if json.Unmarshal(r.Metadata["finalizers"], &finalizers) != nil {
		return queueError(V1Alpha2QueueRequestBlocked, "request finalizer missing or invalid")
	}
	found := false
	for _, f := range finalizers {
		if f == V1Alpha2RecoveryRecordFinalizer {
			found = true
		}
	}
	if !found {
		return queueError(V1Alpha2QueueRequestBlocked, "request finalizer missing")
	}
	// No progress status schema is selected yet; any populated status might be
	// terminal or represent unknown effects and therefore needs reconciliation.
	if len(bytes.TrimSpace(r.Status)) > 0 && !sameJSON(r.Status, json.RawMessage("{}")) {
		return queueError(V1Alpha2QueueRequestBlocked, "request status requires reconciliation")
	}
	return nil
}
