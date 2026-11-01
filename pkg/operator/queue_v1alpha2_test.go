package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

func phase2Recovery(uid, created string, generation int) string {
	return fmt.Sprintf(`{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaRecovery","metadata":{"name":%q,"namespace":"ns","uid":%q,"resourceVersion":"7","finalizers":["rhiza.mrchypark.dev/recovery-record"],"creationTimestamp":%q},"spec":{"target":{"name":"cluster","uid":"uid"},"expectedGeneration":%d}}`, uid, uid, created, generation)
}

func TestPhase2QueueBlocksUnsafeHeadAndAuthority(t *testing.T) {
	head := phase2Recovery("old", "2026-01-01T00:00:00Z", 2)
	idle := phase2IdleObject()
	tests := []struct{ name, object, listed, live string }{
		{"missing pending write", strings.Replace(idle, `,"pendingWrite":false`, "", 1), head, head},
		{"null pending write", strings.Replace(idle, `"pendingWrite":false`, `"pendingWrite":null`, 1), head, head},
		{"true pending write", strings.Replace(idle, `"pendingWrite":false`, `"pendingWrite":true`, 1), head, head},
		{"transition", strings.Replace(idle, `"pendingWrite":false`, `"pendingWrite":false,"transition":{"id":"x"}`, 1), head, head},
		{"null queue", strings.Replace(idle, `"unrelated":`, `"recoveryQueue":null,"unrelated":`, 1), head, head},
		{"unknown queue version", strings.Replace(idle, `"unrelated":`, `"recoveryQueue":{"version":9},"unrelated":`, 1), head, head},
		{"no finalizer", idle, strings.Replace(head, `"finalizers":["rhiza.mrchypark.dev/recovery-record"],`, "", 1), head},
		{"no request version", idle, strings.Replace(head, `"resourceVersion":"7",`, "", 1), head},
		{"head uid changed", idle, head, strings.Replace(head, `"uid":"old"`, `"uid":"replacement"`, 1)},
		{"head version changed", idle, head, strings.Replace(head, `"resourceVersion":"7"`, `"resourceVersion":"8"`, 1)},
		{"head intent changed", idle, head, strings.Replace(head, `"expectedGeneration":2`, `"expectedGeneration":3`, 1)},
		{"head finalizer removed", idle, head, strings.Replace(head, `"finalizers":["rhiza.mrchypark.dev/recovery-record"]`, `"finalizers":[]`, 1)},
		{"head deleting", idle, strings.Replace(head, `"resourceVersion":"7"`, `"deletionTimestamp":"2026-01-02T00:00:00Z","resourceVersion":"7"`, 1), head},
		{"head complete", idle, strings.TrimSuffix(head, "}") + `,"status":{"phase":"Complete"}}`, head},
		{"head failed", idle, strings.TrimSuffix(head, "}") + `,"status":{"phase":"Failed"}}`, head},
		{"head canceled", idle, strings.TrimSuffix(head, "}") + `,"status":{"phase":"Canceled"}}`, head},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writes := 0
			store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				switch {
				case r.Method != http.MethodGet:
					writes++
					return phase2Response(500, ""), nil
				case strings.HasSuffix(r.URL.Path, "/rhizarecoveries"):
					return phase2Response(200, phase2List(`{"resourceVersion":"7"}`, tt.listed, phase2Recovery("later", "2026-01-02T00:00:00Z", 2))), nil
				case strings.Contains(r.URL.Path, "/rhizarecoveries/"):
					return phase2Response(200, tt.live), nil
				default:
					return phase2Response(200, tt.object), nil
				}
			})
			if _, err := ClaimV1Alpha2Recovery(context.Background(), store, phase2Ref); err == nil {
				t.Fatal("unsafe claim accepted")
			}
			if writes != 0 {
				t.Fatalf("unexpected writes: %d", writes)
			}
		})
	}
}

func TestPhase2QueueCompetingClaimsAndUnknownReadback(t *testing.T) {
	head := phase2Recovery("old", "2026-01-01T00:00:00Z", 2)
	list := phase2List(`{"resourceVersion":"7"}`, head)
	for _, mode := range []string{"conflict", "timeout-after-apply", "timeout-before-apply"} {
		t.Run(mode, func(t *testing.T) {
			object := phase2IdleObject()
			puts, clusterGets := 0, 0
			store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				switch {
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/rhizarecoveries"):
					return phase2Response(200, list), nil
				case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/rhizarecoveries/"):
					return phase2Response(200, head), nil
				case r.Method == http.MethodGet:
					clusterGets++
					return phase2Response(200, object), nil
				case r.Method == http.MethodPut:
					puts++
					if mode == "conflict" {
						return phase2Response(409, ""), nil
					}
					if mode == "timeout-after-apply" {
						body, err := io.ReadAll(r.Body)
						if err != nil {
							t.Fatal(err)
						}
						object = strings.Replace(string(body), `"resourceVersion":"1"`, `"resourceVersion":"2"`, 1)
					}
					return nil, fmt.Errorf("timeout")
				default:
					t.Fatalf("unexpected method %s", r.Method)
					return nil, nil
				}
			})
			_, err := ClaimV1Alpha2Recovery(context.Background(), store, phase2Ref)
			switch mode {
			case "conflict":
				if !errors.Is(err, ErrV1Alpha2StatusConflict) {
					t.Fatalf("conflict: %v", err)
				}
				if clusterGets != 1 {
					t.Fatalf("blind readback: %d", clusterGets)
				}
			case "timeout-after-apply":
				if err != nil || clusterGets < 2 {
					t.Fatalf("readback: %v, gets %d", err, clusterGets)
				}
			case "timeout-before-apply":
				if !errors.Is(err, ErrV1Alpha2StatusOutcomeUnknown) || clusterGets != 2 {
					t.Fatalf("unknown: %v, gets %d", err, clusterGets)
				}
			}
			if puts != 1 {
				t.Fatalf("puts = %d", puts)
			}
		})
	}
}

func TestPhase2QueuePostCASRequestChangesBlock(t *testing.T) {
	head := phase2Recovery("old", "2026-01-01T00:00:00Z", 2)
	mutations := map[string]string{
		"deleting":          strings.Replace(head, `"resourceVersion":"7"`, `"deletionTimestamp":"2026-01-02T00:00:00Z","resourceVersion":"8"`, 1),
		"finalizer removed": strings.Replace(strings.Replace(head, `"finalizers":["rhiza.mrchypark.dev/recovery-record"]`, `"finalizers":[]`, 1), `"resourceVersion":"7"`, `"resourceVersion":"8"`, 1),
		"intent changed":    strings.Replace(strings.Replace(head, `"expectedGeneration":2`, `"expectedGeneration":3`, 1), `"resourceVersion":"7"`, `"resourceVersion":"8"`, 1),
	}
	for name, changed := range mutations {
		for _, timeout := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/timeout=%t", name, timeout), func(t *testing.T) {
				object, live := phase2IdleObject(), head
				puts, headGets := 0, 0
				store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
					switch {
					case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/rhizarecoveries"):
						return phase2Response(200, phase2List(`{"resourceVersion":"7"}`, head)), nil
					case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/rhizarecoveries/"):
						headGets++
						return phase2Response(200, live), nil
					case r.Method == http.MethodGet:
						return phase2Response(200, object), nil
					case r.Method == http.MethodPut:
						puts++
						body, err := io.ReadAll(r.Body)
						if err != nil {
							return nil, err
						}
						object = strings.Replace(string(body), `"resourceVersion":"1"`, `"resourceVersion":"2"`, 1)
						live = changed
						if timeout {
							return nil, fmt.Errorf("timeout after apply")
						}
						return phase2Response(200, object), nil
					default:
						t.Fatalf("unexpected method %s", r.Method)
						return nil, nil
					}
				})
				_, err := ClaimV1Alpha2Recovery(context.Background(), store, phase2Ref)
				var blocked *V1Alpha2QueueError
				if !errors.As(err, &blocked) {
					t.Fatalf("expected typed blocked outcome, got %v", err)
				}
				if puts != 1 || headGets < 2 {
					t.Fatalf("puts=%d head reads=%d", puts, headGets)
				}
				var saved, original map[string]json.RawMessage
				if err := json.Unmarshal([]byte(object), &saved); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(phase2IdleObject()), &original); err != nil {
					t.Fatal(err)
				}
				var savedStatus, originalStatus map[string]json.RawMessage
				_ = json.Unmarshal(saved["status"], &savedStatus)
				_ = json.Unmarshal(original["status"], &originalStatus)
				if !sameJSON(savedStatus["authority"], originalStatus["authority"]) || !sameJSON(savedStatus["unrelated"], originalStatus["unrelated"]) {
					t.Fatal("authority or unrelated status changed")
				}
				var claim V1Alpha2QueueClaim
				if err := json.Unmarshal(savedStatus["recoveryQueue"], &claim); err != nil || claim.RequestUID != "old" {
					t.Fatalf("claim was not retained: %+v %v", claim, err)
				}
			})
		}
	}
}

func TestPhase2QueueExistingClaimRechecksLiveAuthority(t *testing.T) {
	head := phase2Recovery("old", "2026-01-01T00:00:00Z", 2)
	mutations := map[string]func(string) string{
		"generation":    func(s string) string { return strings.Replace(s, `"generation":2`, `"generation":3`, 1) },
		"pending write": func(s string) string { return strings.Replace(s, `"pendingWrite":false`, `"pendingWrite":true`, 1) },
		"binding":       func(s string) string { return strings.Replace(s, `"storageID":"storage"`, `"storageID":"other"`, 1) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			object := phase2IdleObject()
			puts, clusterGets := 0, 0
			changeAfterRead := false
			store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				switch {
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/rhizarecoveries"):
					return phase2Response(200, phase2List(`{"resourceVersion":"7"}`, head)), nil
				case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/rhizarecoveries/"):
					if changeAfterRead {
						object = mutate(object)
						changeAfterRead = false
					}
					return phase2Response(200, head), nil
				case r.Method == http.MethodGet:
					clusterGets++
					return phase2Response(200, object), nil
				case r.Method == http.MethodPut:
					puts++
					body, err := io.ReadAll(r.Body)
					if err != nil {
						return nil, err
					}
					object = strings.Replace(string(body), `"resourceVersion":"1"`, `"resourceVersion":"2"`, 1)
					return phase2Response(200, object), nil
				default:
					t.Fatalf("unexpected method %s", r.Method)
					return nil, nil
				}
			})
			if _, err := ClaimV1Alpha2Recovery(context.Background(), store, phase2Ref); err != nil {
				t.Fatal(err)
			}
			var before map[string]json.RawMessage
			_ = json.Unmarshal([]byte(object), &before)
			var beforeStatus map[string]json.RawMessage
			_ = json.Unmarshal(before["status"], &beforeStatus)
			changeAfterRead = true
			clusterGets = 0
			_, err := ClaimV1Alpha2Recovery(context.Background(), store, phase2Ref)
			var blocked *V1Alpha2QueueError
			if !errors.As(err, &blocked) {
				t.Fatalf("expected typed blocked outcome, got %v", err)
			}
			if puts != 1 || clusterGets < 2 {
				t.Fatalf("puts=%d live cluster reads=%d", puts, clusterGets)
			}
			var after map[string]json.RawMessage
			_ = json.Unmarshal([]byte(object), &after)
			var afterStatus map[string]json.RawMessage
			_ = json.Unmarshal(after["status"], &afterStatus)
			if !sameJSON(beforeStatus["recoveryQueue"], afterStatus["recoveryQueue"]) || !sameJSON(beforeStatus["unrelated"], afterStatus["unrelated"]) {
				t.Fatal("claim or unrelated status changed")
			}
		})
	}
}

func TestPhase2QueueConcurrentSameRevision(t *testing.T) {
	head := phase2Recovery("old", "2026-01-01T00:00:00Z", 2)
	var mu sync.Mutex
	object := phase2IdleObject()
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	puts := 0
	store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/rhizarecoveries"):
			return phase2Response(200, phase2List(`{"resourceVersion":"7"}`, head)), nil
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/rhizarecoveries/"):
			arrived <- struct{}{}
			<-release
			return phase2Response(200, head), nil
		case r.Method == http.MethodGet:
			mu.Lock()
			body := object
			mu.Unlock()
			return phase2Response(200, body), nil
		case r.Method == http.MethodPut:
			mu.Lock()
			defer mu.Unlock()
			puts++
			if puts > 1 {
				return phase2Response(409, ""), nil
			}
			b, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, err
			}
			object = strings.Replace(string(b), `"resourceVersion":"1"`, `"resourceVersion":"2"`, 1)
			return phase2Response(200, object), nil
		default:
			return phase2Response(500, ""), nil
		}
	})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, err := ClaimV1Alpha2Recovery(context.Background(), store, phase2Ref); results <- err }()
	}
	<-arrived
	<-arrived
	close(release)
	a, b := <-results, <-results
	if !((a == nil && errors.Is(b, ErrV1Alpha2StatusConflict)) || (b == nil && errors.Is(a, ErrV1Alpha2StatusConflict))) {
		t.Fatalf("results: %v, %v", a, b)
	}
	if puts != 2 {
		t.Fatalf("puts = %d", puts)
	}
}

func TestPhase2QueueDistinctClustersIndependent(t *testing.T) {
	objects := map[string]string{"cluster": phase2IdleObject(), "cluster2": strings.ReplaceAll(strings.ReplaceAll(phase2IdleObject(), `"cluster"`, `"cluster2"`), `:"uid"`, `:"uid2"`)}
	first := phase2Recovery("first", "2026-01-01T00:00:00Z", 2)
	second := strings.ReplaceAll(strings.ReplaceAll(phase2Recovery("second", "2026-01-01T00:00:00Z", 2), `"cluster"`, `"cluster2"`), `:"uid"`, `:"uid2"`)
	store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/rhizarecoveries"):
			return phase2Response(200, phase2List(`{"resourceVersion":"7"}`, first, second)), nil
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/rhizarecoveries/first"):
			return phase2Response(200, first), nil
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/rhizarecoveries/second"):
			return phase2Response(200, second), nil
		case r.Method == http.MethodGet:
			for name, object := range objects {
				if strings.HasSuffix(r.URL.Path, "/"+name) {
					return phase2Response(200, object), nil
				}
			}
		case r.Method == http.MethodPut:
			for name := range objects {
				if strings.HasSuffix(r.URL.Path, "/"+name+"/status") {
					b, err := io.ReadAll(r.Body)
					if err != nil {
						return nil, err
					}
					objects[name] = strings.Replace(string(b), `"resourceVersion":"1"`, `"resourceVersion":"2"`, 1)
					return phase2Response(200, objects[name]), nil
				}
			}
		}
		return phase2Response(404, ""), nil
	})
	a, err := ClaimV1Alpha2Recovery(context.Background(), store, phase2Ref)
	if err != nil || a.RequestUID != "first" {
		t.Fatalf("first: %+v %v", a, err)
	}
	b, err := ClaimV1Alpha2Recovery(context.Background(), store, V1Alpha2ClusterRef{Name: "cluster2", Namespace: "ns", UID: "uid2"})
	if err != nil || b.RequestUID != "second" {
		t.Fatalf("second: %+v %v", b, err)
	}
}

func phase2List(meta string, items ...string) string {
	return `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaRecoveryList","metadata":` + meta + `,"items":[` + strings.Join(items, ",") + `]}`
}

func phase2IdleObject() string {
	s := strings.Replace(phase2Object, `"pendingWrite":{"keep":1},"transition":{"keep":2}`, `"pendingWrite":false`, 1)
	return strings.Replace(s, `,"recoveryQueue":{"version":1}`, "", 1)
}

func TestPhase2QueueOrderingAndIncompleteLists(t *testing.T) {
	old := phase2Recovery("old", "2026-01-01T00:00:00Z", 2)
	newer := phase2Recovery("new", "2026-01-02T00:00:00Z", 2)
	tieA := phase2Recovery("a", "2026-01-01T00:00:00Z", 2)
	tests := []struct {
		name, list, want string
		writes           int
	}{
		{"reverse observation", phase2List(`{"resourceVersion":"7"}`, newer, old), "old", 1},
		{"timestamp tie uses uid", phase2List(`{"resourceVersion":"7"}`, old, tieA), "a", 1},
		{"incomplete page", phase2List(`{"resourceVersion":"7","continue":"next"}`, old), "", 0},
		{"missing list version", phase2List(`{}`, old), "", 0},
		{"duplicate uid", phase2List(`{"resourceVersion":"7"}`, old, old), "", 0},
		{"bad timestamp", phase2List(`{"resourceVersion":"7"}`, phase2Recovery("old", "bad", 2)), "", 0},
		{"wrong namespace", phase2List(`{"resourceVersion":"7"}`, strings.Replace(old, `"namespace":"ns"`, `"namespace":"other"`, 1)), "", 0},
		{"generation drift", phase2List(`{"resourceVersion":"7"}`, phase2Recovery("old", "2026-01-01T00:00:00Z", 1)), "", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writes := 0
			object := phase2IdleObject()
			store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				switch {
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/rhizarecoveries"):
					return phase2Response(200, tt.list), nil
				case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/rhizarecoveries/"):
					for _, candidate := range []string{old, newer, tieA} {
						var x struct {
							Metadata struct {
								Name string `json:"name"`
							} `json:"metadata"`
						}
						if json.Unmarshal([]byte(candidate), &x) == nil && strings.HasSuffix(r.URL.Path, "/"+x.Metadata.Name) {
							return phase2Response(200, candidate), nil
						}
					}
					return phase2Response(404, ""), nil
				case r.Method == http.MethodGet:
					return phase2Response(200, object), nil
				case r.Method == http.MethodPut:
					writes++
					b, err := io.ReadAll(r.Body)
					if err != nil {
						t.Fatal(err)
					}
					object = strings.Replace(string(b), `"resourceVersion":"1"`, `"resourceVersion":"2"`, 1)
					return phase2Response(200, object), nil
				default:
					t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
					return nil, nil
				}
			})
			claim, err := ClaimV1Alpha2Recovery(context.Background(), store, phase2Ref)
			if tt.want == "" {
				if err == nil {
					t.Fatal("unsafe claim accepted")
				}
			} else if err != nil || claim.RequestUID != tt.want {
				t.Fatalf("claim = %+v, error = %v", claim, err)
			}
			if writes != tt.writes {
				t.Fatalf("writes = %d, want %d", writes, tt.writes)
			}
		})
	}
}

func TestPhase2QueueExistingClaimCannotDisappearOrChange(t *testing.T) {
	old := phase2Recovery("old", "2026-01-01T00:00:00Z", 2)
	list := phase2List(`{"resourceVersion":"7"}`, old)
	object := phase2IdleObject()
	writes := 0
	store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/rhizarecoveries"):
			return phase2Response(200, list), nil
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/rhizarecoveries/"):
			return phase2Response(200, old), nil
		case r.Method == http.MethodGet:
			return phase2Response(200, object), nil
		case r.Method == http.MethodPut:
			writes++
			b, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			object = strings.Replace(string(b), `"resourceVersion":"1"`, `"resourceVersion":"2"`, 1)
			return phase2Response(200, object), nil
		default:
			t.Fatalf("unexpected method %s", r.Method)
			return nil, nil
		}
	})
	claim, err := ClaimV1Alpha2Recovery(context.Background(), store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	list = phase2List(`{"resourceVersion":"8"}`, phase2Recovery("new", "2025-01-01T00:00:00Z", 2), old)
	again, err := ClaimV1Alpha2Recovery(context.Background(), store, phase2Ref)
	if err != nil || again != claim {
		t.Fatalf("active claim changed: %+v %v", again, err)
	}
	list = phase2List(`{"resourceVersion":"9"}`, phase2Recovery("new", "2025-01-01T00:00:00Z", 2))
	if _, err := ClaimV1Alpha2Recovery(context.Background(), store, phase2Ref); err == nil {
		t.Fatal("missing active request accepted")
	}
	list = phase2List(`{"resourceVersion":"10"}`, phase2Recovery("old", "2026-01-01T00:00:00Z", 3))
	if _, err := ClaimV1Alpha2Recovery(context.Background(), store, phase2Ref); err == nil {
		t.Fatal("changed intent accepted")
	}
	for name, changed := range map[string]string{
		"terminal": strings.TrimSuffix(old, "}") + `,"status":{"phase":"Complete"}}`,
		"deleting": strings.Replace(old, `"resourceVersion":"7"`, `"deletionTimestamp":"2026-01-02T00:00:00Z","resourceVersion":"7"`, 1),
	} {
		list = phase2List(`{"resourceVersion":"11"}`, changed)
		if _, err := ClaimV1Alpha2Recovery(context.Background(), store, phase2Ref); err == nil {
			t.Errorf("%s active request accepted", name)
		}
	}
	list = phase2List(`{"resourceVersion":"12"}`, old)
	object = strings.Replace(object, `"generation":2`, `"generation":3`, 1)
	if _, err := ClaimV1Alpha2Recovery(context.Background(), store, phase2Ref); err == nil {
		t.Error("active generation drift accepted")
	}
	if writes != 1 {
		t.Fatalf("got %d writes", writes)
	}
	var saved map[string]json.RawMessage
	if err := json.Unmarshal([]byte(object), &saved); err != nil {
		t.Fatal(err)
	}
}
