package network

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedAppendOnlyPeerProfileBindsCommonPlanAndGeneration(t *testing.T) {
	digest := strings.Repeat("a", 64)
	profile := ManagedPeerProfile{ApplicationProtocol: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, BootstrapPlanDigest: digest, Generation: 0}
	transport, err := NewTransportForManagedProfile("cluster", "n1", &quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, "", profile)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	got := transport.tls.NextProtos
	want, err := managedProfileALPN(profile, sqlpolicy.ManagedRequestMaterializationPolicy)
	if err != nil || len(got) != 1 || got[0] != want {
		t.Fatalf("append-only transport ALPN=%v want=%q err=%v", got, want, err)
	}
	if strings.Contains(want, "n1") {
		t.Fatal("local voter identity leaked into common profile ALPN")
	}
	member := testMember("cluster", "peer", "peer-token")
	member.PeerURL = "quic://127.0.0.1:9090"
	peer, err := NewPeerIdentity("cluster", member)
	if err != nil {
		t.Fatal(err)
	}
	learner, err := NewLearnerTransportForManagedProfile("cluster", "learner", 1, []PeerIdentity{peer}, "admin-token", profile)
	if err != nil {
		t.Fatal(err)
	}
	defer learner.Close()
	if len(learner.tls.NextProtos) != 1 || learner.tls.NextProtos[0] != want {
		t.Fatalf("learner transport ALPN=%v want %q", learner.tls.NextProtos, want)
	}

	legacy, err := NewTransportForPolicy("cluster", "n1", &quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, "", sqlpolicy.ManagedRequestMaterializationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	if legacy.tls.NextProtos[0] != managedRequestPeerALPN || legacy.tls.NextProtos[0] == want {
		t.Fatalf("legacy request ALPN changed or aliases append-only: %v", legacy.tls.NextProtos)
	}

	otherGeneration := profile
	otherGeneration.Generation = 1
	otherALPN, err := managedProfileALPN(otherGeneration, sqlpolicy.ManagedRequestMaterializationPolicy)
	if err != nil || otherALPN == want {
		t.Fatalf("generation was not bound into ALPN: %q %v", otherALPN, err)
	}
	otherDigest := profile
	otherDigest.BootstrapPlanDigest = strings.Repeat("b", 64)
	otherALPN, err = managedProfileALPN(otherDigest, sqlpolicy.ManagedRequestMaterializationPolicy)
	if err != nil || otherALPN == want {
		t.Fatalf("plan digest was not bound into ALPN: %q %v", otherALPN, err)
	}
}

func TestManagedAppendOnlyProfileSurvivesAllTransportPaths(t *testing.T) {
	for _, protocol := range []string{sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol} {
		t.Run(protocol, func(t *testing.T) {
			profile := ManagedPeerProfile{ApplicationProtocol: protocol, BootstrapPlanDigest: strings.Repeat("e", 64), Generation: 0}
			policy, _, err := types.ManagedApplicationPolicy(protocol)
			if err != nil {
				t.Fatal(err)
			}
			alpn, err := managedProfileALPN(profile, policy)
			if err != nil {
				t.Fatal(err)
			}
			member, ops := managedKVTransportEndpoint(t, alpn)
			a := testMember("cluster", "a", "a-token")
			config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{a, member}}
			tr, err := NewTransportForManagedProfile("cluster", "a", &config, "a-token", profile)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tr.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if _, err := tr.ReadTip(ctx, "b"); err != nil {
				cancel()
				t.Fatal(err)
			}
			cancel()
			if <-ops != peerfb.OperationReadIndex {
				t.Fatal("managed profile read path used another operation")
			}
			mismatchProfile := profile
			mismatchProfile.Generation++
			mismatch, err := NewTransportForManagedProfile("cluster", "a", &config, "a-token", mismatchProfile)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel = context.WithTimeout(context.Background(), time.Second)
			_, mismatchErr := mismatch.ReadTip(ctx, "b")
			cancel()
			_ = mismatch.Close()
			if mismatchErr == nil {
				t.Fatal("transport with a different generation negotiated the peer ALPN")
			}
			ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
			if err := tr.sendTerminalDecision(ctx, quepaxa.Decision{Slot: 1, ConfigID: 1}, config, config); err != nil {
				cancel()
				t.Fatal(err)
			}
			cancel()
			if <-ops != peerfb.OperationLearned {
				t.Fatal("terminal temporary transport lost managed profile binding")
			}
			origin := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{a}}
			probe, err := NewTransportForManagedProfile("cluster", "a", &origin, "a-token", profile)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
			err = probe.VerifyLearner(ctx, member, 3, quepaxa.ValueHash{1})
			cancel()
			_ = probe.Close()
			if err != nil || <-ops != peerfb.OperationVerifyPrefix {
				t.Fatalf("learner probe lost managed profile binding: %v", err)
			}
		})
	}
}

func TestManagedAppendOnlyPeerProfileConstructorsRejectMixedBindings(t *testing.T) {
	profile := ManagedPeerProfile{ApplicationProtocol: sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol, BootstrapPlanDigest: strings.Repeat("c", 64), Generation: 0}
	transport, err := NewTransportForManagedProfile("cluster", "n1", &quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, "", profile)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	if len(transport.tls.NextProtos) != 1 || !strings.HasPrefix(transport.tls.NextProtos[0], managedAppendOnlyPeerALPNPrefix+"/") {
		t.Fatalf("Graph append-only ALPN missing: %v", transport.tls.NextProtos)
	}
	server, err := NewServerForManagedProfile(nil, nil, "cluster", true, transport, profile)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	serverALPN, err := peerALPNForProfile(server.expectedPolicy, server.managedProfile)
	if err != nil || serverALPN != transport.tls.NextProtos[0] {
		t.Fatalf("listener ALPN=%q transport ALPN=%v err=%v", serverALPN, transport.tls.NextProtos, err)
	}
	if _, err := NewServerForManagedProfile(nil, nil, "cluster", true, transport, ManagedPeerProfile{ApplicationProtocol: sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol, BootstrapPlanDigest: strings.Repeat("d", 64), Generation: 0}); err == nil {
		t.Fatal("server accepted transport bound to another bootstrap plan")
	}
	for _, bad := range []ManagedPeerProfile{
		{ApplicationProtocol: sqlpolicy.ManagedGraphRequestApplicationProtocol, BootstrapPlanDigest: strings.Repeat("c", 64), Generation: 0},
		{ApplicationProtocol: sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol, BootstrapPlanDigest: "not-a-digest", Generation: 0},
		{ApplicationProtocol: sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol, BootstrapPlanDigest: strings.Repeat("c", 64), Generation: -1},
	} {
		if _, err := NewTransportForManagedProfile("cluster", "n1", &quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, "", bad); err == nil {
			t.Fatalf("invalid profile accepted: %+v", bad)
		}
	}
}
