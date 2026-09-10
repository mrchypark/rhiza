package network

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestMembershipStatusAuthenticationAndRedaction(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1", Token: "peer-secret"}}, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	t.Cleanup(server.Close)
	server.SetMembershipToken("admin-secret")
	for _, token := range []string{"", "Bearer wrong", "Bearer admin-secret"} {
		request := httptest.NewRequest(http.MethodGet, "/membership/status", nil)
		request.Header.Set("Authorization", token)
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		if token != "Bearer admin-secret" {
			if response.Code != http.StatusForbidden {
				t.Fatalf("unauthorized status=%d", response.Code)
			}
			continue
		}
		var status MembershipStatus
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &status) != nil || len(status.Voters) != 1 || status.Voters[0] != "n1" || !status.Voting || status.Pending {
			t.Fatalf("invalid status: %d %s", response.Code, response.Body.String())
		}
		if strings.Contains(response.Body.String(), "secret") || strings.Contains(response.Body.String(), "token") {
			t.Fatal("status leaked credentials")
		}
	}
}
