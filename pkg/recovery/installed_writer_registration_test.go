package recovery

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestInstalledWriterRegistrationFinalized(t *testing.T) {
	r := InstalledWriterRegistration{ClusterID: "cluster", InstalledPolicyHash: [32]byte{1}, NodeID: "node", PublicKey: quepaxa.PublicKey{2}, Nonce: strings.Repeat("ab", 32), BootstrapMembership: strings.Repeat("cd", 32), StorageBindingID: "store", Registered: true}
	data, err := r.FinalizedCanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseFinalizedInstalledWriterRegistration(data)
	if err != nil || parsed != r {
		t.Fatalf("round trip: %+v %v", parsed, err)
	}
	hash, err := InstalledWriterIdentityHash(data)
	if err != nil || hash != sha256.Sum256(data) {
		t.Fatalf("hash: %x %v", hash, err)
	}
	invalid := []struct {
		name   string
		mutate func(*InstalledWriterRegistration)
	}{
		{"cluster", func(r *InstalledWriterRegistration) { r.ClusterID = "" }},
		{"policy", func(r *InstalledWriterRegistration) { r.InstalledPolicyHash = [32]byte{} }},
		{"node", func(r *InstalledWriterRegistration) { r.NodeID = "" }},
		{"key", func(r *InstalledWriterRegistration) { r.PublicKey = quepaxa.PublicKey{} }},
		{"nonce", func(r *InstalledWriterRegistration) { r.Nonce = strings.Repeat("AB", 32) }},
		{"membership", func(r *InstalledWriterRegistration) { r.BootstrapMembership = "bad" }},
		{"store", func(r *InstalledWriterRegistration) { r.StorageBindingID = "" }},
		{"pending", func(r *InstalledWriterRegistration) { r.Registered = false }},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			bad := r
			tc.mutate(&bad)
			if _, err := bad.FinalizedCanonicalBytes(); err == nil {
				t.Fatal("accepted incomplete registration")
			}
		})
	}
	for _, bad := range [][]byte{append(append([]byte{}, data...), ' '), append(append([]byte{}, data...), []byte("{}")...), bytes.Replace(data, []byte("{"), []byte("{\"unknown\":1,"), 1), bytes.Repeat([]byte("x"), installedWriterRegistrationMaxSize+1)} {
		if _, err := ParseFinalizedInstalledWriterRegistration(bad); err == nil {
			t.Fatal("accepted noncanonical registration")
		}
		if _, err := InstalledWriterIdentityHash(bad); err == nil {
			t.Fatal("hashed invalid registration")
		}
	}
}

func TestInstalledWriterRegistrationRejectsZeroBindings(t *testing.T) {
	base := InstalledWriterRegistration{ClusterID: "cluster", InstalledPolicyHash: [32]byte{1}, NodeID: "node", PublicKey: quepaxa.PublicKey{2}, Nonce: strings.Repeat("ab", 32), BootstrapMembership: strings.Repeat("cd", 32), StorageBindingID: "store"}
	for _, field := range []string{"nonce", "membership"} {
		for _, finalized := range []bool{false, true} {
			t.Run(field+map[bool]string{false: "/pending", true: "/finalized"}[finalized], func(t *testing.T) {
				r := base
				r.Registered = finalized
				if field == "nonce" {
					r.Nonce = strings.Repeat("0", 64)
				} else {
					r.BootstrapMembership = strings.Repeat("0", 64)
				}
				if _, err := r.FinalizedCanonicalBytes(); err == nil {
					t.Error("encoded zero binding")
				}
				data, err := json.Marshal(r)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := ParseFinalizedInstalledWriterRegistration(data); err == nil {
					t.Error("parsed zero binding")
				}
				if _, err := InstalledWriterIdentityHash(data); err == nil {
					t.Error("hashed zero binding")
				}
			})
		}
	}
}

func TestInstalledWriterRegistrationSizeBoundary(t *testing.T) {
	r := InstalledWriterRegistration{ClusterID: "cluster", InstalledPolicyHash: [32]byte{1}, NodeID: "node", PublicKey: quepaxa.PublicKey{2}, Nonce: strings.Repeat("ab", 32), BootstrapMembership: strings.Repeat("cd", 32), StorageBindingID: "x", Registered: true}
	base, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	r.StorageBindingID = strings.Repeat("x", 1+installedWriterRegistrationMaxSize-1-len(base))
	below, err := r.FinalizedCanonicalBytes()
	if err != nil || len(below) != installedWriterRegistrationMaxSize-1 {
		t.Fatalf("below boundary: len=%d err=%v", len(below), err)
	}
	if _, err := ParseFinalizedInstalledWriterRegistration(below); err != nil {
		t.Fatal(err)
	}
	if _, err := InstalledWriterIdentityHash(below); err != nil {
		t.Fatal(err)
	}
	r.StorageBindingID += "x"
	exact, err := json.Marshal(r)
	if err != nil || len(exact) != installedWriterRegistrationMaxSize {
		t.Fatalf("boundary setup: len=%d err=%v", len(exact), err)
	}
	if _, err := r.FinalizedCanonicalBytes(); err == nil {
		t.Error("encoded exact 64 KiB")
	}
	if _, err := ParseFinalizedInstalledWriterRegistration(exact); err == nil {
		t.Error("parsed exact 64 KiB")
	}
	if _, err := InstalledWriterIdentityHash(exact); err == nil {
		t.Error("hashed exact 64 KiB")
	}
}
