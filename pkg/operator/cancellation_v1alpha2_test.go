package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

// cancellationServer models only the Kubernetes objects; no recovery effects
// are available to the reconciler through this transport.
type cancellationServer struct {
	t                *testing.T
	cluster, request string
	later            string
	writes           []string
	version          int
	missing          bool
	fault            string
	faultAt          int
	puts             int
	staleList        bool
	incompleteList   bool
	responseMutation func(string) string
	failReadback     bool
	failNextGet      bool
}

func newCancellationServer(t *testing.T, claim V1Alpha2QueueClaim) *cancellationServer {
	t.Helper()
	claim.Phase = "CancelRequested"
	q, err := json.Marshal(claim)
	if err != nil {
		t.Fatal(err)
	}
	cluster := strings.Replace(phase2IdleObject(), `"unrelated":`, `"recoveryQueue":`+string(q)+`,"unrelated":`, 1)
	request := phase2Recovery("old", "2026-01-01T00:00:00Z", 2)
	request = strings.Replace(request, `"resourceVersion":"7"`, `"resourceVersion":"7","deletionTimestamp":"2026-01-02T00:00:00Z"`, 1)
	request = strings.Replace(request, `["rhiza.mrchypark.dev/recovery-record"]`, `["other","rhiza.mrchypark.dev/recovery-record"]`, 1)
	return &cancellationServer{t: t, cluster: cluster, request: request, version: 10}
}

func (s *cancellationServer) roundTrip(r *http.Request) (*http.Response, error) {
	s.t.Helper()
	cluster := strings.Contains(r.URL.Path, "/rhizaclusters/")
	list := strings.HasSuffix(r.URL.Path, "/rhizarecoveries")
	request := strings.Contains(r.URL.Path, "/rhizarecoveries/")
	if r.Method == http.MethodGet {
		switch {
		case cluster:
			if s.failNextGet {
				s.failNextGet = false
				return nil, fmt.Errorf("readback unavailable")
			}
			return phase2Response(200, s.cluster), nil
		case list:
			if s.incompleteList {
				return phase2Response(200, phase2List(`{"resourceVersion":"100","continue":"next"}`)), nil
			}
			if s.staleList {
				if s.later != "" {
					return phase2Response(200, phase2List(`{"resourceVersion":"100"}`, s.request, s.later)), nil
				}
				return phase2Response(200, phase2List(`{"resourceVersion":"100"}`, s.request)), nil
			}
			if s.missing {
				if s.later != "" {
					return phase2Response(200, phase2List(`{"resourceVersion":"100"}`, s.later)), nil
				}
				return phase2Response(200, phase2List(`{"resourceVersion":"100"}`)), nil
			}
			if s.later != "" {
				return phase2Response(200, phase2List(`{"resourceVersion":"100"}`, s.request, s.later)), nil
			}
			return phase2Response(200, phase2List(`{"resourceVersion":"100"}`, s.request)), nil
		case request:
			if strings.HasSuffix(r.URL.Path, "/later") && s.later != "" {
				return phase2Response(200, s.later), nil
			}
			if s.missing {
				return phase2Response(404, "not found"), nil
			}
			return phase2Response(200, s.request), nil
		}
	}
	if r.Method != http.MethodPut || (!cluster && !request) {
		s.t.Fatalf("unexpected operation %s %s", r.Method, r.URL.Path)
	}
	s.puts++
	fault := s.puts == s.faultAt
	if fault && s.fault == "conflict" {
		return phase2Response(409, "stale"), nil
	}
	if fault && s.fault == "before" {
		return nil, fmt.Errorf("connection lost before apply")
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.t.Fatal(err)
	}
	var object, metadata map[string]json.RawMessage
	if json.Unmarshal(body, &object) != nil || json.Unmarshal(object["metadata"], &metadata) != nil {
		s.t.Fatalf("invalid PUT: %s", body)
	}
	current := s.request
	if cluster {
		current = s.cluster
	}
	var old map[string]json.RawMessage
	var oldMeta map[string]json.RawMessage
	_ = json.Unmarshal([]byte(current), &old)
	_ = json.Unmarshal(old["metadata"], &oldMeta)
	if !sameJSON(metadata["resourceVersion"], oldMeta["resourceVersion"]) {
		return phase2Response(409, "stale"), nil
	}
	s.version++
	metadata["resourceVersion"], _ = json.Marshal(fmt.Sprint(s.version))
	object["metadata"], _ = json.Marshal(metadata)
	updated, _ := json.Marshal(object)
	if cluster {
		var before, after map[string]json.RawMessage
		_ = json.Unmarshal(old["status"], &before)
		_ = json.Unmarshal(object["status"], &after)
		if !sameJSON(before["authority"], after["authority"]) || !sameJSON(before["unrelated"], after["unrelated"]) {
			s.t.Fatal("cancellation mutated authority or unrelated status")
		}
		s.cluster = string(updated)
		s.writes = append(s.writes, "cluster")
	} else {
		s.request = string(updated)
		if strings.HasSuffix(r.URL.Path, "/status") {
			s.writes = append(s.writes, "ack")
		} else {
			s.writes = append(s.writes, "finalizer")
		}
	}
	if fault && s.fault == "after" {
		if cluster && s.failReadback {
			s.failNextGet = true
		}
		return nil, fmt.Errorf("connection lost after apply")
	}
	if fault && s.responseMutation != nil {
		return phase2Response(200, s.responseMutation(string(updated))), nil
	}
	return phase2Response(200, string(updated)), nil
}

func TestPhase4CancellationMissingFinalizersAfterAcknowledged(t *testing.T) {
	for _, field := range []string{"omitted", "null"} {
		t.Run(field, func(t *testing.T) {
			claim := phase3Claim()
			claim.Phase = "CancelRequested"
			s := newCancellationServer(t, claim)
			s.request = strings.Replace(s.request, `["other","rhiza.mrchypark.dev/recovery-record"]`, `["rhiza.mrchypark.dev/recovery-record"]`, 1)
			store := phase2Store(t, s.roundTrip)
			ctx := context.Background()
			for i := 0; i < 4; i++ {
				if _, err := ReconcileV1Alpha2Cancellation(ctx, store, phase2Ref, "old", claim); err != nil {
					t.Fatalf("step %d: %v", i, err)
				}
			}
			var obj, meta map[string]json.RawMessage
			_ = json.Unmarshal([]byte(s.request), &obj)
			_ = json.Unmarshal(obj["metadata"], &meta)
			if field == "omitted" {
				delete(meta, "finalizers")
			} else {
				meta["finalizers"] = json.RawMessage("null")
			}
			obj["metadata"], _ = json.Marshal(meta)
			encoded, _ := json.Marshal(obj)
			s.request = string(encoded)
			decision := s.cluster
			result, err := ReconcileV1Alpha2Cancellation(ctx, store, phase2Ref, "old", claim)
			if err != nil || result != V1Alpha2CancellationPending || s.cluster != decision || len(s.writes) != 4 {
				t.Fatalf("still listed: %s %v %v", result, err, s.writes)
			}
			s.missing = true
			result, err = ReconcileV1Alpha2Cancellation(ctx, store, phase2Ref, "old", claim)
			if err != nil || result != V1Alpha2CancellationComplete || len(s.writes) != 5 {
				t.Fatalf("absence: %s %v %v", result, err, s.writes)
			}
		})
	}
}

func TestPhase4CancellationUnknownClearReadbackNeverInfersComplete(t *testing.T) {
	claim := phase3Claim()
	claim.Phase = "CancelRequested"
	s := newCancellationServer(t, claim)
	store := phase2Store(t, s.roundTrip)
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if _, err := ReconcileV1Alpha2Cancellation(ctx, store, phase2Ref, "old", claim); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	s.missing, s.fault, s.faultAt, s.failReadback = true, "after", 5, true
	result, err := ReconcileV1Alpha2Cancellation(ctx, store, phase2Ref, "old", claim)
	if !errors.Is(err, ErrV1Alpha2StatusOutcomeUnknown) || result == V1Alpha2CancellationComplete {
		t.Fatalf("unknown clear: %s %v", result, err)
	}
	result, err = ReconcileV1Alpha2Cancellation(ctx, store, phase2Ref, "old", claim)
	if err == nil || result == V1Alpha2CancellationComplete || len(s.writes) != 5 {
		t.Fatalf("missing evidence inferred complete: %s %v %v", result, err, s.writes)
	}
}

func TestPhase4CancellationRejectsMalformedWriteResponses(t *testing.T) {
	mutations := map[string]func(string) string{
		"malformed": func(string) string { return `{}` },
		"kind":      func(s string) string { return strings.Replace(s, `"kind":"RhizaRecovery"`, `"kind":"Wrong"`, 1) },
		"namespace": func(s string) string { return strings.Replace(s, `"namespace":"ns"`, `"namespace":"other"`, 1) },
		"uid":       func(s string) string { return strings.Replace(s, `"uid":"old"`, `"uid":"other"`, 1) },
		"resource version": func(s string) string {
			var obj map[string]json.RawMessage
			var meta map[string]json.RawMessage
			_ = json.Unmarshal([]byte(s), &obj)
			_ = json.Unmarshal(obj["metadata"], &meta)
			meta["resourceVersion"] = json.RawMessage(`""`)
			obj["metadata"], _ = json.Marshal(meta)
			out, _ := json.Marshal(obj)
			return string(out)
		},
		"spec": func(s string) string {
			return strings.Replace(s, `"expectedGeneration":2`, `"expectedGeneration":3`, 1)
		},
	}
	for name, mutate := range mutations {
		for _, boundary := range []int{2, 4} {
			t.Run(fmt.Sprintf("%s/%d", name, boundary), func(t *testing.T) {
				claim := phase3Claim()
				claim.Phase = "CancelRequested"
				s := newCancellationServer(t, claim)
				s.faultAt, s.responseMutation = boundary, mutate
				store := phase2Store(t, s.roundTrip)
				for i := 1; i <= boundary; i++ {
					_, err := ReconcileV1Alpha2Cancellation(context.Background(), store, phase2Ref, "old", claim)
					if i < boundary && err != nil {
						t.Fatalf("step %d: %v", i, err)
					}
					if i == boundary && !errors.Is(err, ErrV1Alpha2StatusOutcomeUnknown) {
						t.Fatalf("untrusted response accepted: %v", err)
					}
				}
			})
		}
	}
}

func TestPhase4CancellationFinalizerResponseEquivalence(t *testing.T) {
	for _, field := range []string{"omitted", "null"} {
		for _, keepOther := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/other=%t", field, keepOther), func(t *testing.T) {
				claim := phase3Claim()
				claim.Phase = "CancelRequested"
				s := newCancellationServer(t, claim)
				if !keepOther {
					s.request = strings.Replace(s.request, `["other","rhiza.mrchypark.dev/recovery-record"]`, `["rhiza.mrchypark.dev/recovery-record"]`, 1)
				}
				s.faultAt = 4
				s.responseMutation = func(response string) string {
					var obj, meta map[string]json.RawMessage
					_ = json.Unmarshal([]byte(response), &obj)
					_ = json.Unmarshal(obj["metadata"], &meta)
					if field == "omitted" {
						delete(meta, "finalizers")
					} else {
						meta["finalizers"] = json.RawMessage("null")
					}
					obj["metadata"], _ = json.Marshal(meta)
					out, _ := json.Marshal(obj)
					return string(out)
				}
				store := phase2Store(t, s.roundTrip)
				for i := 1; i <= 4; i++ {
					_, err := ReconcileV1Alpha2Cancellation(context.Background(), store, phase2Ref, "old", claim)
					if i < 4 && err != nil {
						t.Fatalf("step %d: %v", i, err)
					}
					if i == 4 && keepOther && !errors.Is(err, ErrV1Alpha2StatusOutcomeUnknown) {
						t.Fatalf("lost finalizer accepted: %v", err)
					}
					if i == 4 && !keepOther && err != nil {
						t.Fatalf("equivalent empty finalizers rejected: %v", err)
					}
				}
			})
		}
	}
}

func TestPhase4CancellationWaitingFIFO(t *testing.T) {
	claim := phase3Claim()
	s := newCancellationServer(t, claim)
	s.cluster = phase2IdleObject()
	s.later = phase2Recovery("later", "2026-01-02T00:00:00Z", 2)
	store := phase2Store(t, s.roundTrip)
	ctx := context.Background()
	got, err := ClaimV1Alpha2RecoveryCancellation(ctx, store, phase2Ref)
	if err != nil || got.RequestUID != "old" || got.Phase != "CancelRequested" {
		t.Fatalf("waiting claim: %+v %v", got, err)
	}
	if len(s.writes) != 1 || s.writes[0] != "cluster" {
		t.Fatalf("claim writes: %v", s.writes)
	}
	resumed, err := ClaimV1Alpha2RecoveryCancellation(ctx, store, phase2Ref)
	if err != nil || resumed != got || len(s.writes) != 1 {
		t.Fatalf("resume: %+v %v writes=%v", resumed, err, s.writes)
	}
	if _, err := ClaimV1Alpha2Recovery(ctx, store, phase2Ref); err == nil || len(s.writes) != 1 {
		t.Fatalf("normal claim bypassed cancellation: %v %v", err, s.writes)
	}
	for i := 0; i < 4; i++ {
		if _, err := ReconcileV1Alpha2Cancellation(ctx, store, phase2Ref, "old", got); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
	s.missing = true
	result, err := ReconcileV1Alpha2Cancellation(ctx, store, phase2Ref, "old", got)
	if err != nil || result != V1Alpha2CancellationComplete {
		t.Fatalf("complete: %s %v", result, err)
	}
	s.staleList = true
	if _, err := ClaimV1Alpha2RecoveryCancellation(ctx, store, phase2Ref); err == nil {
		t.Fatal("stale list resurrected canceled request")
	}
	s.staleList = false
	next, err := ClaimV1Alpha2Recovery(ctx, store, phase2Ref)
	if err != nil || next.RequestUID != "later" || next.Phase != "Claimed" {
		t.Fatalf("next head: %+v %v", next, err)
	}
}

func TestPhase4CancellationWaitingBlocksUnsafeHead(t *testing.T) {
	for name, mutate := range map[string]func(*cancellationServer){
		"older healthy": func(s *cancellationServer) {
			s.request = strings.Replace(s.request, `,"deletionTimestamp":"2026-01-02T00:00:00Z"`, "", 1)
		},
		"older unprotected": func(s *cancellationServer) {
			s.request = strings.Replace(s.request, `["other","rhiza.mrchypark.dev/recovery-record"]`, `["other"]`, 1)
		},
		"nonempty status": func(s *cancellationServer) {
			s.request = strings.TrimSuffix(s.request, "}") + `,"status":{"phase":"Failed"}}`
		},
		"pending write": func(s *cancellationServer) {
			s.cluster = strings.Replace(s.cluster, `"pendingWrite":false`, `"pendingWrite":true`, 1)
		},
		"transition": func(s *cancellationServer) {
			s.cluster = strings.Replace(s.cluster, `"pendingWrite":false`, `"pendingWrite":false,"transition":{"id":"other"}`, 1)
		},
		"incomplete list": func(s *cancellationServer) { s.incompleteList = true },
		"other active claim": func(s *cancellationServer) {
			c := phase3Claim()
			c.Phase = "Preparing"
			q, _ := json.Marshal(c)
			s.cluster = strings.Replace(s.cluster, `"unrelated":`, `"recoveryQueue":`+string(q)+`,"unrelated":`, 1)
		},
		"unknown active phase": func(s *cancellationServer) {
			c := phase3Claim()
			c.Phase = "unknown"
			q, _ := json.Marshal(c)
			s.cluster = strings.Replace(s.cluster, `"unrelated":`, `"recoveryQueue":`+string(q)+`,"unrelated":`, 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := newCancellationServer(t, phase3Claim())
			s.cluster = phase2IdleObject()
			s.later = phase2Recovery("later", "2026-01-02T00:00:00Z", 2)
			s.later = strings.Replace(s.later, `"resourceVersion":"7"`, `"resourceVersion":"7","deletionTimestamp":"2026-01-03T00:00:00Z"`, 1)
			mutate(s)
			store := phase2Store(t, s.roundTrip)
			if _, err := ClaimV1Alpha2RecoveryCancellation(context.Background(), store, phase2Ref); err == nil {
				t.Fatal("unsafe head accepted")
			}
			if len(s.writes) != 0 {
				t.Fatalf("unsafe writes: %v", s.writes)
			}
		})
	}
}

func TestPhase4CancellationWaitingCompetingClaimAndTerminal(t *testing.T) {
	ctx := context.Background()
	t.Run("stale CAS", func(t *testing.T) {
		s := newCancellationServer(t, phase3Claim())
		s.cluster, s.fault, s.faultAt = phase2IdleObject(), "conflict", 1
		store := phase2Store(t, s.roundTrip)
		if _, err := ClaimV1Alpha2RecoveryCancellation(ctx, store, phase2Ref); !errors.Is(err, ErrV1Alpha2StatusConflict) {
			t.Fatalf("stale CAS: %v", err)
		}
		if len(s.writes) != 0 {
			t.Fatalf("conflict wrote: %v", s.writes)
		}
		if _, err := ClaimV1Alpha2RecoveryCancellation(ctx, store, phase2Ref); err != nil {
			t.Fatalf("fresh retry: %v", err)
		}
	})
	t.Run("normal wins", func(t *testing.T) {
		s := newCancellationServer(t, phase3Claim())
		s.cluster = phase2IdleObject()
		s.request = strings.Replace(s.request, `,"deletionTimestamp":"2026-01-02T00:00:00Z"`, "", 1)
		store := phase2Store(t, s.roundTrip)
		if _, err := ClaimV1Alpha2Recovery(ctx, store, phase2Ref); err != nil {
			t.Fatal(err)
		}
		s.request = strings.Replace(s.request, `"resourceVersion":"7"`, `"resourceVersion":"7","deletionTimestamp":"2026-01-02T00:00:00Z"`, 1)
		if _, err := ClaimV1Alpha2RecoveryCancellation(ctx, store, phase2Ref); err == nil || len(s.writes) != 1 {
			t.Fatalf("overwrote normal claim: %v %v", err, s.writes)
		}
	})
	t.Run("terminal decision", func(t *testing.T) {
		s := newCancellationServer(t, phase3Claim())
		claim := phase3Claim()
		claim.Phase = "CancelRequested"
		store := phase2Store(t, s.roundTrip)
		if _, err := ReconcileV1Alpha2Cancellation(ctx, store, phase2Ref, "old", claim); err != nil {
			t.Fatal(err)
		}
		if _, err := ClaimV1Alpha2RecoveryCancellation(ctx, store, phase2Ref); err == nil || len(s.writes) != 1 {
			t.Fatalf("terminal decision overwritten: %v %v", err, s.writes)
		}
	})
}

func TestPhase4CancellationRequiresCompleteAbsenceList(t *testing.T) {
	claim := phase3Claim()
	claim.Phase = "CancelRequested"
	s := newCancellationServer(t, claim)
	store := phase2Store(t, s.roundTrip)
	for i := 0; i < 4; i++ {
		if _, err := ReconcileV1Alpha2Cancellation(context.Background(), store, phase2Ref, "old", claim); err != nil {
			t.Fatal(err)
		}
	}
	s.missing, s.incompleteList = true, true
	if result, err := ReconcileV1Alpha2Cancellation(context.Background(), store, phase2Ref, "old", claim); err == nil || result == V1Alpha2CancellationComplete {
		t.Fatalf("incomplete list accepted: %s %v", result, err)
	}
	if len(s.writes) != 4 {
		t.Fatalf("queue cleared: %v", s.writes)
	}
}

func TestPhase4CancellationStaleListCannotResurrect(t *testing.T) {
	claim := phase3Claim()
	claim.Phase = "CancelRequested"
	s := newCancellationServer(t, claim)
	store := phase2Store(t, s.roundTrip)
	for i := 0; i < 4; i++ {
		if _, err := ReconcileV1Alpha2Cancellation(context.Background(), store, phase2Ref, "old", claim); err != nil {
			t.Fatal(err)
		}
	}
	s.missing = true
	if _, err := ReconcileV1Alpha2Cancellation(context.Background(), store, phase2Ref, "old", claim); err != nil {
		t.Fatal(err)
	}
	s.staleList = true
	if _, err := ClaimV1Alpha2Recovery(context.Background(), store, phase2Ref); err == nil {
		t.Fatal("stale list resurrected the old claim")
	}
	if len(s.writes) != 5 {
		t.Fatalf("unexpected writes: %v", s.writes)
	}
}

func TestPhase4CancellationReplacementNeverMutated(t *testing.T) {
	claim := phase3Claim()
	claim.Phase = "CancelRequested"
	s := newCancellationServer(t, claim)
	store := phase2Store(t, s.roundTrip)
	for i := 0; i < 4; i++ {
		if _, err := ReconcileV1Alpha2Cancellation(context.Background(), store, phase2Ref, "old", claim); err != nil {
			t.Fatal(err)
		}
	}
	s.request = strings.Replace(s.request, `"uid":"old"`, `"uid":"replacement"`, 1)
	replacement := s.request
	result, err := ReconcileV1Alpha2Cancellation(context.Background(), store, phase2Ref, "old", claim)
	if err != nil || result != V1Alpha2CancellationComplete {
		t.Fatalf("replacement: %s %v", result, err)
	}
	if s.request != replacement || strings.Join(s.writes, ",") != "cluster,ack,cluster,finalizer,cluster" {
		t.Fatalf("replacement changed: %v", s.writes)
	}
}

func TestPhase4CancellationOrderRetentionAndAbsence(t *testing.T) {
	claim := phase3Claim()
	claim.Phase = "CancelRequested"
	s := newCancellationServer(t, claim)
	store := phase2Store(t, s.roundTrip)
	for i, want := range []string{"cluster", "ack", "cluster", "finalizer"} {
		result, err := ReconcileV1Alpha2Cancellation(context.Background(), store, phase2Ref, "old", claim)
		if err != nil || result != V1Alpha2CancellationPending {
			t.Fatalf("step %d: %s %v", i, result, err)
		}
		if len(s.writes) != i+1 || s.writes[i] != want {
			t.Fatalf("step %d: writes %v", i, s.writes)
		}
	}
	if !strings.Contains(s.request, `"finalizers":["other"]`) {
		t.Fatalf("other finalizer was not retained: %s", s.request)
	}
	result, err := ReconcileV1Alpha2Cancellation(context.Background(), store, phase2Ref, "old", claim)
	if err != nil || result != V1Alpha2CancellationPending || len(s.writes) != 4 {
		t.Fatalf("still present: %s %v %v", result, err, s.writes)
	}
	s.missing = true
	result, err = ReconcileV1Alpha2Cancellation(context.Background(), store, phase2Ref, "old", claim)
	if err != nil || result != V1Alpha2CancellationComplete || len(s.writes) != 5 || s.writes[4] != "cluster" {
		t.Fatalf("absence: %s %v %v", result, err, s.writes)
	}
}

func TestPhase4CancellationUncertainWritesResume(t *testing.T) {
	for _, fault := range []string{"before", "after", "conflict"} {
		for boundary := 1; boundary <= 5; boundary++ {
			t.Run(fmt.Sprintf("%s/%d", fault, boundary), func(t *testing.T) {
				claim := phase3Claim()
				claim.Phase = "CancelRequested"
				s := newCancellationServer(t, claim)
				s.fault, s.faultAt = fault, boundary
				store := phase2Store(t, s.roundTrip)
				faultSeen := false
				for i := 0; i < 15; i++ {
					if len(s.writes) >= 4 {
						s.missing = true
					}
					result, err := ReconcileV1Alpha2Cancellation(context.Background(), store, phase2Ref, "old", claim)
					if s.puts >= boundary && !faultSeen {
						faultSeen = true
						if fault != "after" && err == nil {
							t.Fatalf("%s write accepted without error", fault)
						}
						if err != nil && result == V1Alpha2CancellationComplete {
							t.Fatalf("error reported completion: %v", err)
						}
					} else if err != nil {
						t.Fatalf("unexpected retry error: %v", err)
					}
					if err == nil && result == V1Alpha2CancellationComplete {
						if strings.Join(s.writes, ",") != "cluster,ack,cluster,finalizer,cluster" {
							t.Fatalf("writes: %v", s.writes)
						}
						return
					}
				}
				t.Fatalf("did not converge; writes=%v", s.writes)
			})
		}
	}
}

func TestPhase4CancellationBlocksChangedAuthorityAndIdentity(t *testing.T) {
	claim := phase3Claim()
	claim.Phase = "CancelRequested"
	for name, mutate := range map[string]func(*cancellationServer){
		"cluster uid": func(s *cancellationServer) { s.cluster = strings.Replace(s.cluster, `"uid":"uid"`, `"uid":"other"`, 1) },
		"generation": func(s *cancellationServer) {
			s.cluster = strings.Replace(s.cluster, `"generation":2`, `"generation":3`, 1)
		},
		"pending write": func(s *cancellationServer) {
			s.cluster = strings.Replace(s.cluster, `"pendingWrite":false`, `"pendingWrite":true`, 1)
		},
		"transition": func(s *cancellationServer) {
			s.cluster = strings.Replace(s.cluster, `"pendingWrite":false`, `"pendingWrite":false,"transition":{"id":"other"}`, 1)
		},
		"request replacement": func(s *cancellationServer) {
			s.request = strings.Replace(s.request, `"uid":"old"`, `"uid":"replacement"`, 1)
		},
		"unknown phase": func(s *cancellationServer) {
			s.cluster = strings.Replace(s.cluster, `"phase":"CancelRequested"`, `"phase":"unknown"`, 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := newCancellationServer(t, claim)
			mutate(s)
			store := phase2Store(t, s.roundTrip)
			_, err := ReconcileV1Alpha2Cancellation(context.Background(), store, phase2Ref, "old", claim)
			if err == nil {
				t.Fatal("unsafe state accepted")
			}
			if len(s.writes) != 0 {
				t.Fatalf("unsafe writes: %v", s.writes)
			}
		})
	}
}
