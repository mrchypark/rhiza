//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path"
	"testing"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestCaptureManagedPreparedRepairSourceAcceptsVerifiedTipAdvance(t *testing.T) {
	ctx := context.Background()
	n, identities, bucket, _ := newMembershipOperationNode(t)
	source, _, err := n.preparedBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	n.managedStartup = &ManagedStartupConfig{Checker: preparedRepairCaptureChecker{}, Identity: ManagedStartupIdentity{PhysicalBinding: source}}
	n.opened.Store(true)
	cfg := *n.config
	cfg.NodeID, cfg.Learner = "c", nil
	registered := voterIdentityFor(&cfg)
	registered.Nonce, registered.Registered = identities["c"], true
	data, err := json.Marshal(registered)
	if err != nil {
		t.Fatal(err)
	}
	key := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID), "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte("c"))))
	if err := bucket.Upload(ctx, key, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if slot, _, err := n.core.Propose(ctx, []byte("fixed-source")); err != nil || slot != 1 {
		t.Fatalf("establish fixed source: slot=%d err=%v", slot, err)
	}
	if err := n.archive.SyncThrough(ctx, n.core, 1); err != nil {
		t.Fatal(err)
	}
	var advanced bool
	restore := localtesthooks.Set(func(event string) {
		if event != "node:managed-repair:after-fixed-source-sync" || advanced {
			return
		}
		advanced = true
		slot, _, err := n.core.Propose(ctx, []byte("concurrent-write"))
		if err != nil || slot != 2 {
			t.Fatalf("advance live source: slot=%d err=%v", slot, err)
		}
		if err := n.archive.SyncThrough(ctx, n.core, slot); err != nil {
			t.Fatal(err)
		}
	})
	defer restore()
	request := ManagedMembershipRequest{RepairOperationID: "live-write-capture", Action: ManagedMembershipAdd, ExpectedConfigID: 1, OldNodeID: "c", OldWALIdentity: identities["c"]}
	captured, err := n.CaptureManagedPreparedRepairSource(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if captured.PreparedSlot != 2 || captured.PreparedHash != n.corePrefixForTest(2) {
		t.Fatalf("capture did not pin verified advanced view: slot=%d hash=%x", captured.PreparedSlot, captured.PreparedHash)
	}
}

func (n *Node) corePrefixForTest(slot quepaxa.Slot) [32]byte {
	prefix, _ := n.core.PrefixHash(slot)
	return prefix
}
