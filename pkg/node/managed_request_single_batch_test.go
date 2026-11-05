//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
)

func TestManagedRequestResolveCertifiedKVSingleAndBatch(t *testing.T) {
	for _, layout := range []string{"single", "batch-second-item"} {
		t.Run(layout, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			n, _, _ := newKVWrapperFixture(t)
			command := types.KVCommand{RequestID: "layout-original", Operation: "put", Key: "layout-key", Value: []byte("original")}
			value, err := types.EncodeManagedKVCommand(command)
			if err != nil {
				t.Fatal(err)
			}
			ordinal := uint32(0)
			if layout == "batch-second-item" {
				first, err := types.EncodeManagedKVBatchItem(types.KVCommand{RequestID: "layout-neighbor", Operation: "put", Key: "neighbor", Value: []byte("neighbor")})
				if err != nil {
					t.Fatal(err)
				}
				item, err := types.EncodeManagedKVBatchItem(command)
				if err != nil {
					t.Fatal(err)
				}
				value, err = types.AssembleManagedKVBatch([][]byte{first, item})
				if err != nil {
					t.Fatal(err)
				}
				ordinal = 1
			}
			slot, _, err := n.core.Propose(ctx, value)
			if err != nil {
				t.Fatal("actual quorum certification", err)
			}
			if err := n.replayLocalDecisions(ctx); err != nil {
				t.Fatal("actual certified apply", err)
			}
			ack, err := n.ExecuteManagedKV(ctx, "put", network.KVMutationRequest{RequestID: command.RequestID, Key: command.Key, Value: command.Value})
			if err != nil || ack.Slot != uint64(slot) || ack.Status != types.MutationCommitted {
				t.Fatal("actual original ACK", ack, err)
			}
			original, found, err := n.RetainedRequest(ctx, "kv", command.RequestID)
			if err != nil || !found || original.BatchIndex != ordinal || original.Receipt != ack.MutationReceipt {
				t.Fatal("exact original", err)
			}
			decision, found := n.core.CertifiedValue(slot)
			if !found || n.core.ValidateCertifiedValue(ctx, decision) != nil || !bytes.Equal(decision.Value, value) {
				t.Fatal("actual certificate unavailable")
			}
			if _, err := n.captureManagedRequestResolutionCut(ctx, original); err != nil {
				t.Fatal("valid layout failed exact certificate matching", err)
			}
			for _, badField := range []string{"ordinal", "item", "canonical", "fingerprint"} {
				bad := original
				switch badField {
				case "ordinal":
					bad.BatchIndex++
				case "item":
					bad.ExecutionItem = append(bytes.Clone(bad.ExecutionItem), 0)
				case "canonical":
					bad.CanonicalOriginal = append(bytes.Clone(bad.CanonicalOriginal), 0)
				case "fingerprint":
					bad.Fingerprint[0] ^= 1
				}
				if _, err := n.captureManagedRequestResolutionCut(ctx, bad); err == nil {
					t.Fatal("changed original accepted", badField)
				}
			}
			tip := n.core.Tip()
			resolved, err := n.ResolveRequest(ctx, "kv", command.RequestID)
			if err != nil || resolved.Receipt != ack.MutationReceipt || !reflect.DeepEqual(resolved.Original, original) || n.core.Tip() != tip {
				t.Fatal("certified original resolution changed/rejected exact ACK", err)
			}
		})
	}
}
