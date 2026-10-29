package operator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

const phase3Recovery = `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaRecovery","metadata":{"name":"req","namespace":"ns","uid":"uid","resourceVersion":"1","labels":{"keep":"yes"},"finalizers":["other"]},"spec":{"keep":true},"status":{"keep":true}}`

func TestPhase3FinalizerInstall(t *testing.T) {
	ref := V1Alpha2RecoveryRef{Namespace: "ns", Name: "req", UID: "uid", ResourceVersion: "1"}
	for _, tc := range []struct {
		name, initial, readback string
		putErr                  bool
		want                    error
		writes                  int
		reject                  bool
	}{
		{"direct success", phase3Recovery, "", false, nil, 1, false},
		{"conflict", phase3Recovery, "conflict", false, ErrV1Alpha2FinalizerConflict, 1, false},
		{"applied unknown", phase3Recovery, "applied", true, nil, 1, false},
		{"managed fields response", phase3Recovery, "managed response", false, nil, 1, false},
		{"managed fields readback", phase3Recovery, "managed readback", true, nil, 1, false},
		{"not applied", phase3Recovery, phase3Recovery, true, ErrV1Alpha2FinalizerOutcomeUnknown, 1, false},
		{"replacement", phase3Recovery, "replacement", true, ErrV1Alpha2FinalizerOutcomeUnknown, 1, false},
		{"unrelated mutation", phase3Recovery, "mutation", true, ErrV1Alpha2FinalizerOutcomeUnknown, 1, false},
		{"deleting protected", strings.Replace(strings.Replace(phase3Recovery, `"resourceVersion":"1"`, `"resourceVersion":"1","deletionTimestamp":"now"`, 1), `"finalizers":["other"]`, `"finalizers":["other","rhiza.mrchypark.dev/recovery-record"]`, 1), "", false, nil, 0, false},
		{"deleting", strings.Replace(phase3Recovery, `"resourceVersion":"1"`, `"resourceVersion":"1","deletionTimestamp":"now"`, 1), "", false, nil, 0, true},
		{"wrong uid", strings.Replace(phase3Recovery, `"uid":"uid"`, `"uid":"other"`, 1), "", false, nil, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gets, puts := 0, 0
			var applied string
			store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/ns/rhizarecoveries/req" {
					t.Fatalf("path %s", r.URL.Path)
				}
				if r.Method == http.MethodGet {
					gets++
					if gets == 1 {
						return phase2Response(200, tc.initial), nil
					}
					if tc.readback == "applied" {
						return phase2Response(200, applied), nil
					}
					if tc.readback == "replacement" {
						return phase2Response(200, strings.Replace(applied, `"uid":"uid"`, `"uid":"other"`, 1)), nil
					}
					if tc.readback == "mutation" {
						return phase2Response(200, strings.Replace(applied, `"keep":"yes"`, `"keep":"changed"`, 1)), nil
					}
					if tc.readback == "managed readback" {
						return phase2Response(200, strings.Replace(applied, `"namespace":"ns"`, `"managedFields":[{"manager":"server"}],"namespace":"ns"`, 1)), nil
					}
					return phase2Response(200, tc.readback), nil
				}
				puts++
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(body), `"finalizers":["other","rhiza.mrchypark.dev/recovery-record"]`) || !strings.Contains(string(body), `"keep":true`) {
					t.Fatalf("unexpected body: %s", body)
				}
				applied = strings.Replace(string(body), `"resourceVersion":"1"`, `"resourceVersion":"2"`, 1)
				if tc.putErr {
					return nil, fmt.Errorf("timeout")
				}
				if tc.readback == "conflict" {
					return phase2Response(http.StatusConflict, "conflict"), nil
				}
				if tc.readback == "managed response" {
					return phase2Response(200, strings.Replace(applied, `"namespace":"ns"`, `"managedFields":[{"manager":"server"}],"namespace":"ns"`, 1)), nil
				}
				return phase2Response(200, applied), nil
			})
			err := (&V1Alpha2FinalizerStore{Kube: store.Kube}).Install(context.Background(), ref)
			if tc.reject {
				if err == nil {
					t.Fatal("expected rejection")
				}
			} else if !errors.Is(err, tc.want) {
				t.Fatalf("error %v, want %v", err, tc.want)
			}
			if puts != tc.writes {
				t.Fatalf("writes %d", puts)
			}
			if tc.readback == "conflict" && gets != 1 {
				t.Fatalf("conflict performed %d reads", gets)
			}
		})
	}
}
