package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
	thanosobjstore "github.com/thanos-io/objstore"
)

// TestReplicatedObjectsNeverCarryPeerSecrets scans every object a
// reconfiguration-enabled voter writes to its shared object store. A principal
// with read-only access to the bucket must not recover a voter's private peer
// token or the retired version-1 token-hash fingerprint.
func TestReplicatedObjectsNeverCarryPeerSecrets(t *testing.T) {
	ctx := context.Background()
	n, identities, bucket, transport := newMembershipOperationNode(t)
	transport.disable("c")

	remove := network.MembershipChange{
		OperationID: "scan-remove-c", ClusterID: "cluster", ExpectedConfigID: 1, Remove: "c",
		Fence: &network.MembershipFence{NodeID: "c", WALIdentity: identities["c"], WorkloadUID: "workload-c", Confirmed: true, Evidence: "external-fence"},
	}
	if err := n.changeMembership(ctx, remove); err != nil {
		t.Fatalf("remove: %v", err)
	}
	assertDurableArchivedMembership(t, ctx, n, bucket, n.core.Tip())

	tokens := []string{"a-token", "b-token", "c-token", "admin-token", "fresh-token"}
	forbidden := make([][]byte, 0, len(tokens)*2)
	for _, token := range tokens {
		forbidden = append(forbidden, []byte(token))
		sum := sha256.Sum256([]byte(token))
		forbidden = append(forbidden, []byte(hex.EncodeToString(sum[:])))
	}

	scanned := 0
	if err := bucket.Iter(ctx, n.config.ObjStorePrefix, func(name string) error {
		reader, err := bucket.Get(ctx, name)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil {
			return fmt.Errorf("read %s: %v %v", name, readErr, closeErr)
		}
		scanned++
		for _, secret := range forbidden {
			if bytes.Contains(body, secret) {
				return fmt.Errorf("object %s leaks %q", name, secret)
			}
		}
		return nil
	}, thanosobjstore.WithRecursiveIter()); err != nil {
		t.Fatal(err)
	}
	if scanned == 0 {
		t.Fatal("scan read no objects; the assertion would be vacuous")
	}
}
