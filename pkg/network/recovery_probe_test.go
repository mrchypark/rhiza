package network

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestMembershipRepairProbeAuthenticatesExactCandidateObservation(t *testing.T) {
	s := NewServer(nil, nil, "cluster", false, nil)
	t.Cleanup(s.Close)
	s.SetRecoveryArchive("secret", func(context.Context) error { return nil })
	request := validMembershipRepairProbeRequest()
	want := validMembershipRepairProbeResponse(request)
	s.SetMembershipRepairProbe(func(_ context.Context, got MembershipRepairProbeRequest) (MembershipRepairProbeResponse, error) {
		if got.Version != request.Version || got.Challenge != request.Challenge || got.ScopeDigest != request.ScopeDigest ||
			got.SourceConfigID != request.SourceConfigID || got.AbortSlot == nil || *got.AbortSlot != *request.AbortSlot ||
			got.ExcludedOldMemberID != request.ExcludedOldMemberID || got.CandidateMember != request.CandidateMember || got.CandidateBindRef != request.CandidateBindRef {
			t.Fatalf("callback request=%+v", got)
		}
		return want, nil
	})
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/recovery/probe", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	s.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	got, err := StrictDecodeMembershipRepairProbeResponse(response.Body.Bytes(), request, "secret")
	if err != nil {
		t.Fatal(err)
	}
	if got.BarrierNonce != membershipRepairBarrierNonceHex(request.Challenge) || got.CandidatePrefixHash != got.PrefixHash {
		t.Fatalf("observation=%+v", got)
	}

	if _, err := StrictDecodeMembershipRepairProbeResponse(response.Body.Bytes(), request, "wrong-secret"); !errors.Is(err, ErrRecoveryProbeMAC) {
		t.Fatalf("wrong key accepted: %v", err)
	}
	request.CandidateBindRef = "different-bind"
	if _, err := StrictDecodeMembershipRepairProbeResponse(response.Body.Bytes(), request, "secret"); err == nil {
		t.Fatal("response for another candidate bind was accepted")
	}
}

func TestMembershipRepairProbeRejectsUnauthenticatedAndUnboundRuntimeResult(t *testing.T) {
	s := NewServer(nil, nil, "cluster", false, nil)
	t.Cleanup(s.Close)
	s.SetRecoveryArchive("secret", func(context.Context) error { return nil })
	called := false
	s.SetMembershipRepairProbe(func(_ context.Context, request MembershipRepairProbeRequest) (MembershipRepairProbeResponse, error) {
		called = true
		response := validMembershipRepairProbeResponse(request)
		response.CandidateMember.WALIdentity = strings.Repeat("ee", 32)
		return response, nil
	})
	body, err := json.Marshal(validMembershipRepairProbeRequest())
	if err != nil {
		t.Fatal(err)
	}
	call := func(token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/recovery/probe", strings.NewReader(string(body)))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		s.ServeHTTP(response, req)
		return response
	}
	if got := call("wrong"); got.Code != http.StatusForbidden || called {
		t.Fatalf("unauthorized probe status=%d called=%v", got.Code, called)
	}
	if got := call("secret"); got.Code != http.StatusServiceUnavailable || !called {
		t.Fatalf("unbound result status=%d called=%v body=%s", got.Code, called, got.Body.String())
	}
}

func validMembershipRepairProbeRequest() MembershipRepairProbeRequest {
	abort := quepaxa.Slot(0)
	return MembershipRepairProbeRequest{
		Version: 1, Challenge: strings.Repeat("ab", 32), ScopeDigest: strings.Repeat("cd", 32),
		SourceConfigID: 3, AbortSlot: &abort, ExcludedOldMemberID: "old",
		CandidateMember:  quepaxa.Member{ID: "candidate", URL: "https://candidate", PeerURL: "quic://candidate", LogURL: "https://candidate/log", PublicKey: quepaxa.PublicKey{1}, WALIdentity: strings.Repeat("ef", 32)},
		CandidateBindRef: "bind-ref",
	}
}

func validMembershipRepairProbeResponse(request MembershipRepairProbeRequest) MembershipRepairProbeResponse {
	return MembershipRepairProbeResponse{
		Version: 1, Challenge: request.Challenge, ScopeDigest: request.ScopeDigest,
		VerifierNodeID: "voter", VerifierWALIdentity: strings.Repeat("12", 32), Ready: true, Voting: true,
		SourceConfigID: request.SourceConfigID, SourceMembers: []quepaxa.Member{
			{ID: "voter", URL: "https://voter", PeerURL: "quic://voter", LogURL: "https://voter/log", PublicKey: quepaxa.PublicKey{2}, WALIdentity: strings.Repeat("12", 32)},
			{ID: "old", URL: "https://old", PeerURL: "quic://old", LogURL: "https://old/log", PublicKey: quepaxa.PublicKey{3}, WALIdentity: strings.Repeat("34", 32)},
		}, AbortSlot: request.AbortSlot, ExcludedOldMemberID: request.ExcludedOldMemberID,
		ReadIndexSlot: 10, BarrierSlot: 11, BarrierNonce: membershipRepairBarrierNonceHex(request.Challenge),
		PrefixHash: strings.Repeat("56", 32), CandidateMember: request.CandidateMember,
		CandidateBindRef: request.CandidateBindRef, CandidatePrefixThrough: 11, CandidatePrefixHash: strings.Repeat("56", 32),
	}
}

func TestRecoveryProbeAuthenticatesFreshStatus(t *testing.T) {
	s := NewServer(nil, nil, "cluster", false, nil)
	defer s.Close()
	s.SetRecoveryArchive("secret", func(context.Context) error { return nil })
	s.SetVoterRecoveryStatus(func(context.Context) VoterRecoveryStatus {
		return VoterRecoveryStatus{
			NodeID: "n1", ClusterID: "cluster", Durability: "before-ack",
			Ready: false, Quorum: true, CertifiedTip: 19, AppliedTip: 17, ArchiveTip: 13,
		}
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
	if !VerifyRecoveryProbe(proof, nonce, "secret") || !proof.Status.Quorum || proof.Status.Ready {
		t.Fatal("valid fresh probe rejected")
	}
	var legacy map[string]json.RawMessage
	if err := json.Unmarshal(got.Body.Bytes(), &legacy); err != nil {
		t.Fatal(err)
	}
	if len(legacy) != 3 || legacy["version"] != nil {
		t.Fatalf("legacy top-level shape changed: %s", got.Body.String())
	}
	if gotMAC := strings.Trim(string(legacy["mac"]), `"`); gotMAC != "b99dbe6a0fe6178f5a0ca7ae8766d427b9c24741d29884f7614c64a630f5d785" {
		t.Fatalf("legacy MAC changed: %s", gotMAC)
	}
	var legacyStatus map[string]json.RawMessage
	if err := json.Unmarshal(legacy["status"], &legacyStatus); err != nil || len(legacyStatus) != 8 {
		t.Fatalf("legacy status shape changed: fields=%d err=%v body=%s", len(legacyStatus), err, got.Body.String())
	}
	if _, err := StrictDecodeRecoveryProbe(got.Body.Bytes(), nonce, "", "secret"); !errors.Is(err, ErrRecoveryProbeSchema) {
		t.Fatalf("strict v1 client accepted legacy response: %v", err)
	}
	if VerifyRecoveryProbe(proof, strings.Repeat("cd", 32), "secret") || VerifyRecoveryProbe(proof, nonce, "wrong") {
		t.Fatal("replay/wrong key accepted")
	}
	proof.Status.ClusterID = "other"
	if VerifyRecoveryProbe(proof, nonce, "secret") {
		t.Fatal("tampered identity accepted")
	}
	proof.Status.ClusterID = "cluster"
}

func TestVersionedRecoveryProbeUsesStrictSignedContract(t *testing.T) {
	s := NewServer(nil, nil, "cluster", false, nil)
	t.Cleanup(s.Close)
	s.SetRecoveryArchive("secret", func(context.Context) error { return nil })
	s.SetVoterRecoveryObservation(func(context.Context, string) VoterRecoveryStatus {
		return VoterRecoveryStatus{
			NodeID: "n1", ClusterID: "cluster", Durability: "before-ack", Ready: true,
			ConfigID: 4, Voters: []string{"n1"}, WALIdentity: "wal-1", Voting: true, AbortSlot: 0,
			Quorum: true, QuorumKnown: true, QuorumConfigID: 4, ReadIndexSlot: 0,
		}
	})
	nonce := strings.Repeat("12", 32)
	req := httptest.NewRequest(http.MethodGet, "/recovery/probe?version=1&nonce="+nonce, nil)
	req.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()
	s.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	probe, err := StrictDecodeRecoveryProbe(response.Body.Bytes(), nonce, "", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if probe.Version != RecoveryProbeProtocolVersion || !probe.Status.QuorumKnown || probe.Status.ReadIndexSlot != 0 || probe.Status.AbortSlot != 0 {
		t.Fatalf("probe=%+v", probe)
	}
	unsupported := httptest.NewRequest(http.MethodGet, "/recovery/probe?version=2&nonce="+nonce, nil)
	unsupported.Header.Set("Authorization", "Bearer secret")
	response = httptest.NewRecorder()
	s.ServeHTTP(response, unsupported)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unsupported version status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestStrictDecodeRecoveryProbeRejectsIncompleteOrTamperedPayload(t *testing.T) {
	nonce := strings.Repeat("56", 32)
	probe := RecoveryProbe{
		Version: RecoveryProbeProtocolVersion, Excluded: "", Nonce: nonce,
		Status: VoterRecoveryStatus{
			NodeID: "n1", ClusterID: "cluster", ConfigID: 4, Voters: []string{"n1"}, WALIdentity: "wal-1",
			Voting: true, Quorum: true, QuorumKnown: true, QuorumConfigID: 4, ReadIndexSlot: 0,
		},
	}
	probe.MAC = recoveryProbeMAC(probe, "secret")
	valid, err := json.Marshal(probe)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := StrictDecodeRecoveryProbe(valid, nonce, "", "secret"); err != nil {
		t.Fatalf("valid zero-valued fields rejected: %v", err)
	}
	legacy := strings.Replace(string(valid), `"version":1,`, "", 1)
	if _, err := StrictDecodeRecoveryProbe([]byte(legacy), nonce, "", "secret"); !errors.Is(err, ErrRecoveryProbeSchema) {
		t.Fatalf("missing version error=%v", err)
	}
	cases := []struct {
		name string
		edit func(string) string
		want error
	}{
		{"duplicate top-level signed version", func(s string) string { return strings.Replace(s, `"version":1,`, `"version":1,"version":1,`, 1) }, ErrRecoveryProbeSchema},
		{"duplicate nested signed quorum", func(s string) string { return strings.Replace(s, `"quorum":true,`, `"quorum":false,"quorum":true,`, 1) }, ErrRecoveryProbeSchema},
		{"missing zero abort slot", func(s string) string { return strings.Replace(s, `"abort_slot":0,`, "", 1) }, ErrRecoveryProbeSchema},
		{"null abort slot", func(s string) string { return strings.Replace(s, `"abort_slot":0,`, `"abort_slot":null,`, 1) }, ErrRecoveryProbeSchema},
		{"wrong voting type", func(s string) string { return strings.Replace(s, `"voting":true,`, `"voting":"true",`, 1) }, ErrRecoveryProbeSchema},
		{"null known flag", func(s string) string { return strings.Replace(s, `"quorum_known":true,`, `"quorum_known":null,`, 1) }, ErrRecoveryProbeSchema},
		{"extra field", func(s string) string { return strings.Replace(s, `"version":1,`, `"version":1,"extra":true,`, 1) }, ErrRecoveryProbeSchema},
		{"unsupported version", func(s string) string { return strings.Replace(s, `"version":1,`, `"version":2,`, 1) }, ErrRecoveryProbeVersion},
		{"tampered signed status", func(s string) string { return strings.Replace(s, `"ready":false,`, `"ready":true,`, 1) }, ErrRecoveryProbeMAC},
		{"tampered signed state tuple", func(s string) string {
			return strings.Replace(s, `"quorum":true,"quorum_known":true,"quorum_unavailable":false,"quorum_config_id":4,`, `"quorum":false,"quorum_known":false,"quorum_unavailable":true,"quorum_config_id":0,`, 1)
		}, ErrRecoveryProbeMAC},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mutated := tc.edit(string(valid))
			if mutated == string(valid) {
				t.Fatal("test mutation did not match wire fixture")
			}
			if _, err := StrictDecodeRecoveryProbe([]byte(mutated), nonce, "", "secret"); !errors.Is(err, tc.want) {
				t.Fatalf("error=%v, want %v", err, tc.want)
			}
		})
	}
}

func TestStrictDecodeRecoveryProbeAcceptsOnlyThreeQuorumStates(t *testing.T) {
	nonce := strings.Repeat("78", 32)
	base := VoterRecoveryStatus{
		NodeID: "n1", ClusterID: "cluster", ConfigID: 4, Voters: []string{"n1"}, WALIdentity: "wal-1", Voting: true,
		Quorum: true, QuorumKnown: true, QuorumConfigID: 4, ReadIndexSlot: 0,
	}
	cases := []struct {
		name string
		edit func(*VoterRecoveryStatus)
		ok   bool
	}{
		{name: "success slot zero is valid", edit: func(*VoterRecoveryStatus) {}, ok: true},
		{name: "stable unavailable", edit: func(s *VoterRecoveryStatus) {
			s.Quorum = false
			s.QuorumKnown = false
			s.QuorumUnavailable = true
			s.QuorumConfigID = 0
		}, ok: true},
		{name: "unknown", edit: func(s *VoterRecoveryStatus) { s.Quorum = false; s.QuorumKnown = false; s.QuorumConfigID = 0 }, ok: true},
		{name: "both flags", edit: func(s *VoterRecoveryStatus) { s.QuorumUnavailable = true }, ok: false},
		{name: "unavailable with config claim", edit: func(s *VoterRecoveryStatus) { s.Quorum = false; s.QuorumKnown = false; s.QuorumUnavailable = true }, ok: false},
		{name: "unknown with slot claim", edit: func(s *VoterRecoveryStatus) {
			s.Quorum = false
			s.QuorumKnown = false
			s.QuorumConfigID = 0
			s.ReadIndexSlot = 2
		}, ok: false},
		{name: "success config mismatch", edit: func(s *VoterRecoveryStatus) { s.QuorumConfigID++ }, ok: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status := base
			status.Voters = append([]string(nil), base.Voters...)
			tc.edit(&status)
			probe := RecoveryProbe{Version: RecoveryProbeProtocolVersion, Nonce: nonce, Status: status}
			probe.MAC = recoveryProbeMAC(probe, "secret")
			payload, err := json.Marshal(probe)
			if err != nil {
				t.Fatal(err)
			}
			_, err = StrictDecodeRecoveryProbe(payload, nonce, "", "secret")
			if tc.ok && err != nil {
				t.Fatalf("valid state rejected: %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrRecoveryProbeSchema) {
				t.Fatalf("invalid state error=%v", err)
			}
		})
	}
}

func TestRecoveryProbePassesExclusionToCoherentStatusSource(t *testing.T) {
	s := NewServer(nil, nil, "cluster", false, nil)
	t.Cleanup(s.Close)
	s.SetRecoveryArchive("secret", func(context.Context) error { return nil })
	var gotExcluded string
	s.SetVoterRecoveryObservation(func(_ context.Context, excluded string) VoterRecoveryStatus {
		gotExcluded = excluded
		return VoterRecoveryStatus{NodeID: "n1", ClusterID: "cluster", ConfigID: 4, Voters: []string{"n1"}, Voting: true, Quorum: true, QuorumKnown: true, QuorumConfigID: 4}
	})
	nonce := strings.Repeat("cd", 32)
	req := httptest.NewRequest(http.MethodGet, "/recovery/probe?version=1&nonce="+nonce+"&exclude=n3", nil)
	req.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()
	s.ServeHTTP(response, req)
	if response.Code != http.StatusOK || gotExcluded != "n3" {
		t.Fatalf("status=%d excluded=%q body=%s", response.Code, gotExcluded, response.Body.String())
	}
	probe, err := StrictDecodeRecoveryProbe(response.Body.Bytes(), nonce, "n3", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if probe.Excluded != "n3" || !probe.Status.QuorumKnown || probe.Status.QuorumConfigID != probe.Status.ConfigID {
		t.Fatalf("probe=%+v", probe)
	}
}

func TestRecoveryProbeRejectsLegacyUnboundStatusSource(t *testing.T) {
	s := NewServer(nil, nil, "cluster", false, nil)
	t.Cleanup(s.Close)
	s.SetRecoveryArchive("secret", func(context.Context) error { return nil })
	s.SetVoterRecoveryStatus(func(context.Context) VoterRecoveryStatus {
		return VoterRecoveryStatus{NodeID: "n1", ClusterID: "cluster", Quorum: true}
	})
	req := httptest.NewRequest(http.MethodGet, "/recovery/probe?version=1&nonce="+strings.Repeat("ef", 32), nil)
	req.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()
	s.ServeHTTP(response, req)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestRecoveryProbeRejectsContradictoryQuorumFlags(t *testing.T) {
	s := NewServer(nil, nil, "cluster", false, nil)
	t.Cleanup(s.Close)
	s.SetRecoveryArchive("secret", func(context.Context) error { return nil })
	s.SetVoterRecoveryObservation(func(context.Context, string) VoterRecoveryStatus {
		return VoterRecoveryStatus{
			NodeID: "n1", ClusterID: "cluster", ConfigID: 4, Voters: []string{"n1"}, Voting: true,
			Quorum: true, QuorumKnown: true, QuorumUnavailable: true, QuorumConfigID: 4,
		}
	})
	req := httptest.NewRequest(http.MethodGet, "/recovery/probe?version=1&nonce="+strings.Repeat("34", 32), nil)
	req.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()
	s.ServeHTTP(response, req)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestVersionedRecoveryProbeRejectsSelfExclusion(t *testing.T) {
	s := NewServer(nil, nil, "cluster", false, nil)
	t.Cleanup(s.Close)
	s.SetRecoveryArchive("secret", func(context.Context) error { return nil })
	s.SetVoterRecoveryObservation(func(context.Context, string) VoterRecoveryStatus {
		return VoterRecoveryStatus{
			NodeID: "n1", ClusterID: "cluster", ConfigID: 4, Voters: []string{"n1"}, Voting: true,
			Quorum: false, QuorumUnavailable: true,
		}
	})
	nonce := strings.Repeat("90", 32)
	req := httptest.NewRequest(http.MethodGet, "/recovery/probe?version=1&nonce="+nonce+"&exclude=n1", nil)
	req.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()
	s.ServeHTTP(response, req)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}
