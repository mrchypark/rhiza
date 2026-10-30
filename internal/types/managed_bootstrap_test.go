package types

import (
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedIdentityDerivation(t *testing.T) {
	c, err := ManagedClusterID("uid", 0)
	if err != nil || !strings.HasPrefix(string(c), "rhiza-c-") || len(c) != 72 {
		t.Fatalf("cluster: %q %v", c, err)
	}
	c2, _ := ManagedClusterID("uid", 1)
	n, err := ManagedNodeID("uid")
	if err != nil || !strings.HasPrefix(string(n), "rhiza-n-") || len(n) != 72 || string(c) == string(n) || c == c2 {
		t.Fatal("identity separation failed")
	}
	for _, uid := range []string{"", " uid", "uid\x00x", strings.Repeat("x", 257)} {
		if _, err := ManagedNodeID(uid); err == nil {
			t.Fatalf("accepted %q", uid)
		}
	}
	if _, err := ManagedClusterID("uid", -1); err == nil {
		t.Fatal("accepted negative generation")
	}
}

func TestManagedBootstrapPlanDigestCanonicalMembers(t *testing.T) {
	a := []quepaxa.Member{{ID: "b", PublicKey: quepaxa.PublicKey{2}, PeerURL: "quic://b:1", WALIdentity: "wal-b"}, {ID: "a", PublicKey: quepaxa.PublicKey{1}, PeerURL: "quic://a:1", WALIdentity: "wal-a"}}
	b := []quepaxa.Member{{ID: "a", PublicKey: quepaxa.PublicKey{1}, PeerURL: "quic://a:1"}, {ID: "b", PublicKey: quepaxa.PublicKey{2}, PeerURL: "quic://b:1"}}
	x, err := ManagedBootstrapPlanDigest("cluster", a, "source", "sql-v1", "", true)
	if err != nil {
		t.Fatal(err)
	}
	y, err := ManagedBootstrapPlanDigest("cluster", b, "source", "sql-v1", ObjectStoreDurabilityBeforeAck, true)
	if err != nil || x != y || a[0].WALIdentity != "wal-b" {
		t.Fatalf("canonical digest mismatch: %s %s %v", x, y, err)
	}
	z, err := ManagedBootstrapPlanDigest("cluster", b, "other", "sql-v1", ObjectStoreDurabilityBeforeAck, true)
	if err != nil || x == z {
		t.Fatalf("physical binding not included: %v", err)
	}
}

func TestManagedBootstrapPlanDigestRejectsDuplicateMembers(t *testing.T) {
	base := []quepaxa.Member{{ID: "a", PublicKey: quepaxa.PublicKey{1}, PeerURL: "quic://a:1"}, {ID: "b", PublicKey: quepaxa.PublicKey{2}, PeerURL: "quic://b:1"}}
	for _, change := range []func([]quepaxa.Member){func(m []quepaxa.Member) { m[1].ID = m[0].ID }, func(m []quepaxa.Member) { m[1].PublicKey = m[0].PublicKey }, func(m []quepaxa.Member) { m[1].PeerURL = m[0].PeerURL }} {
		m := append([]quepaxa.Member(nil), base...)
		change(m)
		if _, err := ManagedBootstrapPlanDigest("cluster", m, "source", "sql-v1", "", true); err == nil {
			t.Fatal("duplicate member accepted")
		}
	}
}
