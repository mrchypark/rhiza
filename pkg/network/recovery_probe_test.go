package network

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRecoveryProbeAuthenticatesFreshStatus(t *testing.T) {
	s := NewServer(nil, nil, "cluster", false, nil)
	defer s.Close()
	s.SetRecoveryArchive("secret", func(context.Context) error { return nil })
	s.SetVoterRecoveryStatus(func(context.Context) VoterRecoveryStatus {
		return VoterRecoveryStatus{NodeID: "n1", ClusterID: "cluster", Quorum: true}
	})
	nonce := strings.Repeat("ab", 32)
	call := func(token, challenge string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/recovery/probe?nonce="+challenge, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		result := httptest.NewRecorder()
		s.ServeHTTP(result, req)
		return result
	}
	if got := call("wrong", nonce); got.Code != http.StatusForbidden {
		t.Fatal(got.Code)
	}
	if got := call("secret", "short"); got.Code != http.StatusBadRequest {
		t.Fatal(got.Code)
	}
	got := call("secret", nonce)
	if got.Code != http.StatusOK {
		t.Fatal(got.Code, got.Body.String())
	}
	var proof RecoveryProbe
	if err := json.Unmarshal(got.Body.Bytes(), &proof); err != nil {
		t.Fatal(err)
	}
	if !VerifyRecoveryProbe(proof, nonce, "secret") || !proof.Status.Quorum {
		t.Fatal("valid fresh probe rejected")
	}
	if VerifyRecoveryProbe(proof, strings.Repeat("cd", 32), "secret") || VerifyRecoveryProbe(proof, nonce, "wrong") {
		t.Fatal("replay/wrong key accepted")
	}
	proof.Status.ClusterID = "other"
	if VerifyRecoveryProbe(proof, nonce, "secret") {
		t.Fatal("tampered identity accepted")
	}
}
