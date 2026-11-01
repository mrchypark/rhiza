package operator

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"testing"
)

func TestRepairV1Alpha2ActualWireIntegerTokens(t *testing.T) {
	for _, large := range []bool{false, true} {
		t.Run(strconv.FormatBool(large), func(t *testing.T) {
			f, old := repairTestSetup(t)
			pending, observation, err := ReserveV1Alpha2Write(context.Background(), f.store, old, old.Authority(), old.Evidence(), V1Alpha2WriteRequest{Kind: "fixture", ID: "repair-pending", Fingerprint: "repair-pending-fingerprint"})
			if err != nil || observation != V1Alpha2CASApplied {
				t.Fatalf("reserve before wire Begin: %s %v", observation, err)
			}
			old = pending
			if large {
				f.plan.ExpectedConfigID = 9007199254740993
				f.plan.ExpectedAbortSlot = 9007199254740993
			}
			var received bool
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				received = true
				data, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					w.WriteHeader(500)
					return
				}
				var object, status, repair, plan map[string]json.RawMessage
				if json.Unmarshal(data, &object) != nil || json.Unmarshal(object["status"], &status) != nil || json.Unmarshal(status["repair"], &repair) != nil || json.Unmarshal(repair["plan"], &plan) != nil {
					t.Error("invalid wire structure")
					w.WriteHeader(500)
					return
				}
				for _, field := range []string{"generation", "expectedConfigID", "expectedAbortSlot"} {
					if !regexp.MustCompile(`^(0|[1-9][0-9]*)$`).Match(plan[field]) {
						t.Errorf("%s wire token is not an unsigned integer", field)
					}
					var n uint64
					if json.Unmarshal(plan[field], &n) != nil {
						t.Errorf("%s uint64 decoding failed", field)
					}
					if field == "expectedConfigID" && n != f.plan.ExpectedConfigID || field == "expectedAbortSlot" && n != f.plan.ExpectedAbortSlot || field == "generation" && n != uint64(f.plan.Generation) {
						t.Errorf("%s integer lost precision", field)
					}
				}
				var oldStatus map[string]json.RawMessage
				_ = json.Unmarshal(f.cluster["status"], &oldStatus)
				for _, field := range []string{"authority", "applicationInit", "bootstrapPlan", "genesis"} {
					if !sameJSON(status[field], oldStatus[field]) {
						t.Errorf("%s changed on wire", field)
					}
				}
				if !sameJSON(object["metadata"], f.cluster["metadata"]) {
					t.Error("UID/RV metadata changed on wire")
				}
				var meta map[string]json.RawMessage
				_ = json.Unmarshal(object["metadata"], &meta)
				var rv string
				_ = json.Unmarshal(meta["resourceVersion"], &rv)
				n, _ := strconv.Atoi(rv)
				meta["resourceVersion"], _ = json.Marshal(strconv.Itoa(n + 1))
				object["metadata"], _ = json.Marshal(meta)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(object)
			}))
			t.Cleanup(server.Close)
			original := f.store.Kube.Client.Transport
			endpoint, _ := url.Parse(server.URL)
			f.store.Kube.Client.Transport = phase2Transport(func(r *http.Request) (*http.Response, error) {
				if r.Method != http.MethodPut {
					return original.RoundTrip(r)
				}
				copy := r.Clone(r.Context())
				u := *r.URL
				u.Scheme, u.Host = endpoint.Scheme, endpoint.Host
				copy.URL = &u
				return server.Client().Transport.RoundTrip(copy)
			})
			_, observation, err = BeginV1Alpha2Repair(context.Background(), f.store, old, old.Authority(), f.plan)
			if err != nil || observation != V1Alpha2CASApplied || !received {
				t.Fatalf("actual wire Begin: %s %v received=%t", observation, err, received)
			}
		})
	}
}
