package fencing

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuthorityClientInitialRegistrationRequiresExplicitEvidence(t *testing.T) {
	id := testIdentity()
	id.NodeID, id.PodName = "rhiza-0", "rhiza-0"
	for _, evidence := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy ordinary grant", true: "explicit initial grant"}[evidence], func(t *testing.T) {
			grant := StartupGrant{Identity: id, Role: StartupRoleVoterActive}
			if evidence {
				grant.InitialRegistration = &InitialRegistrationProvenance{
					Version: InitialRegistrationProvenanceVersion, IdentityDigest: InitialRegistrationIdentityDigest(id),
					MembershipHash: strings.Repeat("a", 64), StoreScopeHash: strings.Repeat("b", 64), RunIdentity: id,
				}
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request AuthorityMessage
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Action != "register" || !request.InitialRegistration || request.Identity != id || r.Header.Get("Authorization") != "Bearer test-token" {
					t.Error("initial registration request lost its authenticated classification")
					http.Error(w, "invalid request", http.StatusBadRequest)
					return
				}
				_ = json.NewEncoder(w).Encode(grant)
			}))
			defer server.Close()
			token := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(token, []byte("test-token"), 0600); err != nil {
				t.Fatal(err)
			}
			client := AuthorityClient{URL: server.URL, TokenFile: token, Client: server.Client()}
			got, err := client.RegisterInitialStartup(t.Context(), id)
			if !evidence {
				if err == nil || client.role != "" {
					t.Fatal("unmarked grant enabled initial-registration authority")
				}
				return
			}
			if err != nil || got.InitialRegistration == nil || *got.InitialRegistration != *grant.InitialRegistration {
				t.Fatalf("explicit initial evidence did not survive HTTP response: %v", err)
			}
		})
	}
}
