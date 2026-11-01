package operator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func phase3Claim() V1Alpha2QueueClaim {
	intent := []byte(`{"target":{"name":"cluster","uid":"uid"},"expectedGeneration":2,"backoff":6,"deadline":null}`)
	digest := sha256.Sum256(intent)
	hash := hex.EncodeToString(digest[:])
	op := sha256.Sum256([]byte("rhiza-queue-v1\x00uid\x00old\x00" + hash))
	created, _ := time.Parse(time.RFC3339, "2026-01-01T00:00:00Z")
	return V1Alpha2QueueClaim{RequestUID: "old", OperationID: hex.EncodeToString(op[:]), IntentHash: hash, ExpectedGeneration: 2, CreatedAt: created, Phase: "Claimed"}
}

func TestPhase3ClaimRegistration(t *testing.T) {
	claim := phase3Claim()
	for _, tc := range []struct {
		name, phase                 string
		cancel, deleting, wantWrite bool
	}{
		{"cancel", "Claimed", true, true, true},
		{"reserve", "Claimed", false, false, true},
		{"legacy unknown", "", true, true, false},
		{"preparing", "Preparing", true, true, false},
		{"no deletion", "Claimed", true, false, false},
		{"deleting cannot reserve", "Claimed", false, true, false},
		{"pending", "Claimed", true, true, false},
		{"transition", "Claimed", true, true, false},
		{"generation", "Claimed", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := claim
			c.Phase = tc.phase
			queue, _ := json.Marshal(c)
			object := strings.Replace(phase2IdleObject(), `"unrelated":`, `"recoveryQueue":`+string(queue)+`,"unrelated":`, 1)
			switch tc.name {
			case "pending":
				object = strings.Replace(object, `"pendingWrite":false`, `"pendingWrite":true`, 1)
			case "transition":
				object = strings.Replace(object, `"pendingWrite":false`, `"pendingWrite":false,"transition":{"id":"other"}`, 1)
			case "generation":
				object = strings.Replace(object, `"generation":2`, `"generation":3`, 1)
			}
			request := phase2Recovery("old", "2026-01-01T00:00:00Z", 2)
			if tc.deleting {
				request = strings.Replace(request, `"resourceVersion":"7"`, `"resourceVersion":"7","deletionTimestamp":"2026-01-02T00:00:00Z"`, 1)
			}
			writes := 0
			store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodPut {
					writes++
					body, _ := io.ReadAll(r.Body)
					return phase2Response(200, strings.Replace(string(body), `"resourceVersion":"1"`, `"resourceVersion":"2"`, 1)), nil
				}
				if strings.Contains(r.URL.Path, "/rhizarecoveries/") {
					return phase2Response(200, request), nil
				}
				return phase2Response(200, object), nil
			})
			var err error
			if tc.cancel {
				err = RegisterV1Alpha2RecoveryCancellation(context.Background(), store, phase2Ref, "old", c)
			} else {
				err = BeginV1Alpha2RecoveryPreparation(context.Background(), store, phase2Ref, "old", c)
			}
			if (err == nil) != tc.wantWrite || (writes == 1) != tc.wantWrite {
				t.Fatalf("err=%v writes=%d", err, writes)
			}
		})
	}
}

func TestPhase3CASUncertainAndConflict(t *testing.T) {
	for _, mode := range []string{"conflict", "malformed", "timeout-applied", "timeout-not-applied"} {
		t.Run(mode, func(t *testing.T) {
			claim := phase3Claim()
			queue, _ := json.Marshal(claim)
			object := strings.Replace(phase2IdleObject(), `"unrelated":`, `"recoveryQueue":`+string(queue)+`,"unrelated":`, 1)
			request := strings.Replace(phase2Recovery("old", "2026-01-01T00:00:00Z", 2), `"resourceVersion":"7"`, `"resourceVersion":"7","deletionTimestamp":"2026-01-02T00:00:00Z"`, 1)
			gets := 0
			store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodPut {
					body, _ := io.ReadAll(r.Body)
					if mode == "conflict" {
						return phase2Response(http.StatusConflict, "conflict"), nil
					}
					if mode == "timeout-applied" {
						object = strings.Replace(string(body), `"resourceVersion":"1"`, `"resourceVersion":"2"`, 1)
					}
					if mode == "malformed" {
						return phase2Response(http.StatusOK, `{}`), nil
					}
					return nil, fmt.Errorf("connection lost")
				}
				if strings.Contains(r.URL.Path, "/rhizarecoveries/") {
					return phase2Response(200, request), nil
				}
				gets++
				return phase2Response(200, object), nil
			})
			err := RegisterV1Alpha2RecoveryCancellation(context.Background(), store, phase2Ref, "old", claim)
			if mode == "conflict" {
				if !errors.Is(err, ErrV1Alpha2StatusConflict) || gets != 1 {
					t.Fatalf("err=%v gets=%d", err, gets)
				}
			} else if !errors.Is(err, ErrV1Alpha2StatusOutcomeUnknown) || gets != 2 {
				t.Fatalf("err=%v gets=%d", err, gets)
			}
		})
	}
}

func TestPhase3CompetingCAS(t *testing.T) {
	claim := phase3Claim()
	queue, _ := json.Marshal(claim)
	object := strings.Replace(phase2IdleObject(), `"unrelated":`, `"recoveryQueue":`+string(queue)+`,"unrelated":`, 1)
	writes := 0
	store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet {
			return phase2Response(200, object), nil
		}
		writes++
		if writes > 1 {
			return phase2Response(http.StatusConflict, "stale revision"), nil
		}
		body, _ := io.ReadAll(r.Body)
		object = strings.Replace(string(body), `"resourceVersion":"1"`, `"resourceVersion":"2"`, 1)
		return phase2Response(200, object), nil
	})
	a, err := store.Load(context.Background(), phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.Load(context.Background(), phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	claim.Phase = "Preparing"
	preparing, _ := json.Marshal(claim)
	claim.Phase = "CancelRequested"
	cancel, _ := json.Marshal(claim)
	if _, err := store.compareAndSwapQueue(context.Background(), a, preparing, false); err != nil {
		t.Fatal(err)
	}
	if _, err := store.compareAndSwapQueue(context.Background(), b, cancel, false); !errors.Is(err, ErrV1Alpha2StatusConflict) {
		t.Fatalf("losing CAS: %v", err)
	}
}

func TestPhase3DeletionRacesPreparationBookkeeping(t *testing.T) {
	claim := phase3Claim()
	queue, _ := json.Marshal(claim)
	object := strings.Replace(phase2IdleObject(), `"unrelated":`, `"recoveryQueue":`+string(queue)+`,"unrelated":`, 1)
	request := phase2Recovery("old", "2026-01-01T00:00:00Z", 2)
	var original map[string]json.RawMessage
	_ = json.Unmarshal([]byte(object), &original)
	var oldStatus map[string]json.RawMessage
	_ = json.Unmarshal(original["status"], &oldStatus)
	writes := 0
	store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPut {
			writes++
			request = strings.Replace(request, `"resourceVersion":"7"`, `"resourceVersion":"8","deletionTimestamp":"2026-01-02T00:00:00Z"`, 1)
			body, _ := io.ReadAll(r.Body)
			object = strings.Replace(string(body), `"resourceVersion":"1"`, `"resourceVersion":"2"`, 1)
			return phase2Response(200, object), nil
		}
		if strings.Contains(r.URL.Path, "/rhizarecoveries/") {
			return phase2Response(200, request), nil
		}
		return phase2Response(200, object), nil
	})
	ctx := context.Background()
	if err := BeginV1Alpha2RecoveryPreparation(ctx, store, phase2Ref, "old", claim); err != nil {
		t.Fatal(err)
	}
	var result map[string]json.RawMessage
	_ = json.Unmarshal([]byte(object), &result)
	var status map[string]json.RawMessage
	_ = json.Unmarshal(result["status"], &status)
	claim.Phase = "Preparing"
	want, _ := json.Marshal(claim)
	if !sameJSON(status["authority"], oldStatus["authority"]) || !sameJSON(status["unrelated"], oldStatus["unrelated"]) || !sameJSON(status["recoveryQueue"], want) {
		t.Fatalf("preparation changed authority or lost claim: %s", result["status"])
	}
	if !strings.Contains(request, `"deletionTimestamp"`) || !strings.Contains(request, V1Alpha2RecoveryRecordFinalizer) {
		t.Fatalf("deleting request or finalizer lost: %s", request)
	}
	if err := RegisterV1Alpha2RecoveryCancellation(ctx, store, phase2Ref, "old", claim); err == nil || writes != 1 {
		t.Fatalf("cancellation after preparation: err=%v writes=%d", err, writes)
	}
}
