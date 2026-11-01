package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type voterLookupAuthority struct{}

func (voterLookupAuthority) ValidateManagedStartup(context.Context, ManagedStartupIdentity) error {
	return nil
}
func (voterLookupAuthority) ValidateManagedAdmission(context.Context, ManagedStartupIdentity, ManagedRuntimeState) error {
	return nil
}
func (voterLookupAuthority) ValidateManagedMembership(context.Context, ManagedStartupIdentity, ManagedMembershipRequest, quepaxa.Cluster, quepaxa.Slot) error {
	return nil
}

func TestManagedVoterWALIdentityActualRegistrations(t *testing.T) {
	ctx := context.Background()
	n, bucket, _, identities := terminalRetryFixture(t, nil)
	// The fixture really registers a/b as bootstrap voters and c with a
	// learner key, using ensureVoterIdentity and actual bound WALs.
	n.config.Members = append([]quepaxa.Member(nil), n.config.Members[:2]...)
	for i := range n.config.Members {
		n.config.Members[i].WALIdentity = ""
	}
	source, _, err := n.preparedBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	n.managedStartup = &ManagedStartupConfig{Checker: voterLookupAuthority{}, Identity: ManagedStartupIdentity{PhysicalBinding: source}}
	n.opened.Store(true)
	for _, member := range n.core.CurrentCluster().Members {
		if member.ID == "a" {
			continue
		}
		got, err := n.ManagedVoterWALIdentity(ctx, 1, 0, member)
		if err != nil || got != identities[member.ID] {
			t.Fatalf("actual registered voter %s: %v", member.ID, err)
		}
	}
	member := n.core.CurrentCluster().Members[1]
	key := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID), "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(member.ID))))
	original, err := readVoterRegistration(ctx, bucket, key)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"store", "membership", "nonce", "cluster", "node", "learner_public_key", "registered", "version"} {
		t.Run(field, func(t *testing.T) {
			var record map[string]any
			if err := json.Unmarshal(original, &record); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "registered":
				record[field] = false
			case "version":
				record[field] = 1
			case "nonce":
				record[field] = strings.Repeat("f", 64)
			default:
				record[field] = "foreign"
			}
			data, _ := json.Marshal(record)
			if err := bucket.Upload(ctx, key, bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			if _, err := n.ManagedVoterWALIdentity(ctx, 1, 0, member); err == nil {
				t.Fatal("accepted foreign registration")
			}
		})
	}
	if err := bucket.Upload(ctx, key, bytes.NewReader(original)); err != nil {
		t.Fatal(err)
	}
	if _, err := n.ManagedVoterWALIdentity(ctx, 2, 0, member); err == nil {
		t.Fatal("accepted stale config")
	}
	if _, err := n.ManagedVoterWALIdentity(ctx, 1, 1, member); err == nil {
		t.Fatal("accepted stale abort revision")
	}
	wrongKey := member
	wrongKey.PublicKey[0] ^= 1
	if _, err := n.ManagedVoterWALIdentity(ctx, 1, 0, wrongKey); err == nil {
		t.Fatal("accepted a key not in current certified membership")
	}
	if err := bucket.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := n.ManagedVoterWALIdentity(ctx, 1, 0, member); err == nil {
		t.Fatal("minted nonce for absent registration")
	}
}
