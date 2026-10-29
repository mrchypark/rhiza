package operator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

var authorityTestRequest = V1Alpha2WriteRequest{Kind: "sql", ID: "request", Fingerprint: "fingerprint"}
var authorityTestEvidence = json.RawMessage(`{"tip":7}`)

func authorityTestSetup(t *testing.T) (*phase8Server, *V1Alpha2StatusStore, V1Alpha2AdmissionSnapshot) {
	t.Helper()
	cluster := strings.Replace(phase2Object, `"pendingWrite":{"keep":1},"transition":{"keep":2}`, `"pendingWrite":false,"evidence":{"tip":7}`, 1)
	cluster = strings.Replace(cluster, `"resourceVersion":"1"`, `"resourceVersion":"47"`, 1)
	s := &phase8Server{t: t, cluster: cluster, apply: true}
	store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		second := r.Method == http.MethodPut && strings.Contains(s.cluster, `"resourceVersion":"48"`)
		response, err := s.roundTrip(r)
		if second && err == nil && response.StatusCode == http.StatusOK {
			s.cluster = strings.Replace(s.cluster, `"resourceVersion":"48"`, `"resourceVersion":"49"`, 1)
			_ = response.Body.Close()
			response.Body = io.NopCloser(strings.NewReader(s.cluster))
		}
		return response, err
	})
	snap, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	return s, store, snap
}

func TestAuthorityV1Alpha2ReserveFinalizeAndRecoveryRead(t *testing.T) {
	s, store, initial := authorityTestSetup(t)
	ctx := context.Background()
	reserved, observation, err := ReserveV1Alpha2Write(ctx, store, initial, initial.Authority(), authorityTestEvidence, authorityTestRequest)
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("reserve: %s %v", observation, err)
	}
	if req, ok := reserved.Pending(); !ok || req != authorityTestRequest {
		t.Fatalf("pending: %+v %v", req, ok)
	}
	loaded, err := LoadV1Alpha2Admission(ctx, store, phase2Ref)
	if err != nil {
		t.Fatalf("pending recovery read: %v", err)
	}
	if req, ok := loaded.Pending(); !ok || req != authorityTestRequest {
		t.Fatalf("recovery pending: %+v %v", req, ok)
	}
	if _, _, err := ReserveV1Alpha2Write(ctx, store, loaded, loaded.Authority(), authorityTestEvidence, authorityTestRequest); err == nil {
		t.Fatal("new write accepted during pending")
	}
	foreign := authorityTestRequest
	foreign.Fingerprint = "foreign"
	if _, _, err := FinalizeV1Alpha2Write(ctx, store, loaded, loaded.Authority(), foreign, V1Alpha2WriteCommitted, json.RawMessage(`{"tip":8}`)); err == nil {
		t.Fatal("foreign finalization accepted")
	}
	if _, _, err := FinalizeV1Alpha2Write(ctx, store, loaded, loaded.Authority(), authorityTestRequest, "not-found", authorityTestEvidence); err == nil {
		t.Fatal("unknown outcome finalized")
	}
	if _, _, err := FinalizeV1Alpha2Write(ctx, store, loaded, loaded.Authority(), authorityTestRequest, V1Alpha2WriteNoEffect, json.RawMessage(`{"tip":8}`)); err == nil {
		t.Fatal("no-effect changed evidence")
	}
	if s.puts != 1 {
		t.Fatalf("invalid operations wrote status: %d", s.puts)
	}
	final, observation, err := FinalizeV1Alpha2Write(ctx, store, loaded, loaded.Authority(), authorityTestRequest, V1Alpha2WriteCommitted, json.RawMessage(`{"tip":8}`))
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("finalize: %s %v", observation, err)
	}
	if _, ok := final.Pending(); ok || !sameJSON(final.Evidence(), json.RawMessage(`{"tip":8}`)) {
		t.Fatalf("final state pending=%v evidence=%s", ok, final.Evidence())
	}
	if !strings.Contains(s.cluster, `"unrelated":{"keep":true}`) || !strings.Contains(s.cluster, `"recoveryQueue":{"version":1}`) {
		t.Fatal("unrelated status changed")
	}
}

func TestAuthorityV1Alpha2StaleSnapshotConflicts(t *testing.T) {
	s, store, snap := authorityTestSetup(t)
	s.cluster = strings.Replace(s.cluster, `"resourceVersion":"47"`, `"resourceVersion":"48"`, 1)
	s.cluster = strings.Replace(s.cluster, `"generation":2`, `"generation":3`, 1)
	_, _, err := ReserveV1Alpha2Write(context.Background(), store, snap, snap.Authority(), authorityTestEvidence, authorityTestRequest)
	if !errors.Is(err, ErrV1Alpha2StatusConflict) {
		t.Fatalf("stale CAS: %v", err)
	}
	live, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref)
	if err != nil || live.Authority().Generation != 3 {
		t.Fatalf("live authority: %+v %v", live.Authority(), err)
	}
	if _, pending := live.Pending(); pending {
		t.Fatal("stale CAS reserved a write")
	}
}

func TestAuthorityV1Alpha2ProvenNoEffect(t *testing.T) {
	s, store, snap := authorityTestSetup(t)
	ctx := context.Background()
	reserved, _, err := ReserveV1Alpha2Write(ctx, store, snap, snap.Authority(), authorityTestEvidence, authorityTestRequest)
	if err != nil {
		t.Fatal(err)
	}
	final, observation, err := FinalizeV1Alpha2Write(ctx, store, reserved, snap.Authority(), authorityTestRequest, V1Alpha2WriteNoEffect, authorityTestEvidence)
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("no-effect finalize: %s %v", observation, err)
	}
	if _, pending := final.Pending(); pending || !sameJSON(final.Evidence(), authorityTestEvidence) || s.puts != 2 {
		t.Fatalf("pending=%v evidence=%s puts=%d", pending, final.Evidence(), s.puts)
	}
}

func TestAuthorityV1Alpha2IdenticalCompetingReservationDoesNotProveOwnership(t *testing.T) {
	s, store, old := authorityTestSetup(t)
	ctx := context.Background()
	if _, _, err := ReserveV1Alpha2Write(ctx, store, old, old.Authority(), authorityTestEvidence, authorityTestRequest); err != nil {
		t.Fatal(err)
	}
	// Simulate another writer installing the identical status while this CAS
	// returns an ambiguous transport failure, not a successful response.
	current := s.cluster
	store.Kube.Client.Transport = phase2Transport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPut {
			return nil, errors.New("lost response")
		}
		return phase2Response(http.StatusOK, current), nil
	})
	observed, result, err := ReserveV1Alpha2Write(ctx, store, old, old.Authority(), authorityTestEvidence, authorityTestRequest)
	if !errors.Is(err, ErrV1Alpha2StatusOutcomeUnknown) || result != V1Alpha2CASMatchingReadback {
		t.Fatalf("stale ambiguous CAS: %s %v", result, err)
	}
	if s.cluster != current {
		t.Fatal("ambiguous attempt changed competing pending")
	}
	if req, pending := observed.Pending(); !pending || req != authorityTestRequest {
		t.Fatalf("readback pending: %+v %v", req, pending)
	}
}

func TestAuthorityV1Alpha2ReserveRejectsMismatches(t *testing.T) {
	for _, test := range []string{"owner", "generation", "binding", "evidence", "request", "transition"} {
		t.Run(test, func(t *testing.T) {
			s, store, snap := authorityTestSetup(t)
			expected, evidence, request := snap.Authority(), json.RawMessage(`{"tip":7}`), authorityTestRequest
			switch test {
			case "owner":
				expected.Owner.UID = "other"
			case "generation":
				expected.Generation++
			case "binding":
				expected.Binding.DeploymentUID = "other"
			case "evidence":
				evidence = json.RawMessage(`{"tip":8}`)
			case "request":
				request.Fingerprint = ""
			case "transition":
				s.cluster = strings.Replace(s.cluster, `"pendingWrite":false`, `"transition":{"id":"other"},"pendingWrite":false`, 1)
				var err error
				snap, err = LoadV1Alpha2Admission(context.Background(), store, phase2Ref)
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := ReserveV1Alpha2Write(context.Background(), store, snap, expected, evidence, request); err == nil {
				t.Fatal("invalid reservation accepted")
			}
			if s.puts != 0 {
				t.Fatalf("writes: %d", s.puts)
			}
		})
	}
}

func TestAuthorityV1Alpha2CASConflictAndUnknown(t *testing.T) {
	for _, mode := range []string{"conflict", "lost applied", "lost not applied"} {
		t.Run(mode, func(t *testing.T) {
			s, store, snap := authorityTestSetup(t)
			s.conflict = mode == "conflict"
			s.timeout = mode != "conflict"
			s.apply = mode == "lost applied"
			observed, result, err := ReserveV1Alpha2Write(context.Background(), store, snap, snap.Authority(), authorityTestEvidence, authorityTestRequest)
			if mode == "conflict" {
				if !errors.Is(err, ErrV1Alpha2StatusConflict) {
					t.Fatalf("conflict: %v", err)
				}
			} else {
				if !errors.Is(err, ErrV1Alpha2StatusOutcomeUnknown) {
					t.Fatalf("unknown outcome: %v", err)
				}
				if mode == "lost applied" {
					if result != V1Alpha2CASMatchingReadback {
						t.Fatalf("observation: %s", result)
					}
					if req, ok := observed.Pending(); !ok || req != authorityTestRequest {
						t.Fatalf("pending readback: %+v %v", req, ok)
					}
				}
				if mode == "lost not applied" && result != V1Alpha2CASUnknown {
					t.Fatalf("observation: %s", result)
				}
			}
			if s.puts != 1 {
				t.Fatalf("CAS attempts: %d", s.puts)
			}
		})
	}
}
