package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestMembershipMutationErrorDetails(t *testing.T) {
	const admin = "admin-secret"
	const memberToken = "member-secret"
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"native error", 500, string(jsonBytes(network.ErrorResponse{Code: "internal_error", Error: "remove failed", RequestID: "private-request"})), "membership change returned HTTP 500: internal_error: remove failed"},
		{"malformed", 500, `{"code":"internal_error","error":`, "membership change returned HTTP 500"},
		{"oversized", 500, `{"code":"internal_error","error":"` + strings.Repeat("x", 16<<10) + `"}`, "membership change returned HTTP 500"},
		{"trailing data", 500, `{"code":"internal_error","error":"failed"} garbage`, "membership change returned HTTP 500"},
		{"unauthorized", 401, `{"code":"unauthorized","error":"access denied"}`, "membership change returned HTTP 401: unauthorized: access denied"},
		{"redacted", 500, `{"code":"internal_error","error":"admin-secret member-secret"}`, "membership change returned HTTP 500: internal_error: [redacted] [redacted]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/membership/change" || r.Header.Get("Authorization") != "Bearer "+admin {
					t.Error("unexpected mutation request or authorization")
				}
				var change network.MembershipChange
				if err := json.NewDecoder(r.Body).Decode(&change); err != nil || change.Add == nil || change.Add.Token != memberToken {
					t.Error("mutation request was not preserved")
				}
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			host, portText, err := net.SplitHostPort(server.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			port, err := strconv.Atoi(portText)
			if err != nil {
				t.Fatal(err)
			}
			pod := object{"status": object{"podIP": host}, "spec": object{"containers": []any{object{"name": "rhiza", "ports": []any{object{"name": "http", "containerPort": float64(port)}}}}}}
			c := &Controller{HTTP: server.Client()}
			err = c.membershipMutation(context.Background(), pod, "rhiza", admin, "/membership/change", network.MembershipChange{Add: &quepaxa.Member{Token: memberToken}})
			if err == nil || err.Error() != tc.want {
				t.Fatalf("error=%v, want %q", err, tc.want)
			}
		})
	}
}
