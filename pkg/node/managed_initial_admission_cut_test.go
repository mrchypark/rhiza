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

func TestManagedInitialAdmissionCutAllowsOnlyLaterRecorderProgress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	n, _, fixture := newKVWrapperFixture(t)
	initial, err := n.ManagedState()
	if err != nil || initial.CertifiedTip != 0 || initial.AppliedTip != 0 || initial.RecorderTip != 0 {
		t.Fatal("fixture does not begin at its actual empty quorum prefix", initial, err)
	}
	if err := n.ValidateManagedInitialAdmission(ctx, n.managedStartup.Identity, initial); err != nil {
		t.Fatal("empty resolved initial cut rejected", err)
	}
	ack, err := n.ExecuteManagedKV(ctx, "put", network.KVMutationRequest{RequestID: "initial-cut-original", Key: "initial-cut-key", Value: []byte("original")})
	if err != nil {
		t.Fatal("actual committed original", err)
	}
	before, err := n.ManagedState()
	if err != nil || before.CertifiedTip != before.AppliedTip || before.RecorderTip > before.CertifiedTip {
		t.Fatal("initial cut is not resolved", err)
	}
	original, found, err := n.RetainedRequest(ctx, "kv", "initial-cut-original")
	if err != nil || !found || original.Receipt != ack.MutationReceipt {
		t.Fatal("actual retained ACK", err)
	}
	values, tip, err := n.core.DecisionsFrom(1, 256)
	if err != nil || uint64(tip) != before.CertifiedTip {
		t.Fatal("actual certified prefix", err)
	}
	prefix, ok := n.core.PrefixHash(tip)
	if !ok {
		t.Fatal("actual prefix unavailable")
	}
	var nonce [types.ReadBarrierNonceSize]byte
	nonce[0] = 197
	control := types.EncodeReadBarrier(nonce)
	recordFreshAdmissionControl(t, ctx, fixture, tip+1, control, false)
	identity := n.managedStartup.Identity
	if err := n.ValidateManagedInitialAdmission(ctx, identity, before); err != nil {
		t.Fatal("later recorder invalidated the exact resolved initial cut", err)
	}
	after, err := n.ManagedState()
	if err != nil || after.RecorderTip != uint64(tip+1) || after.CertifiedTip != before.CertifiedTip || after.AppliedTip != before.AppliedTip {
		t.Fatal("initial-cut validation changed pending progress", after, err)
	}
	if _, found := n.core.CertifiedValue(tip + 1); found {
		t.Fatal("initial-cut validation fabricated a certificate")
	}
	// Existing public evidence/freshness inspectors must stay strict.
	if _, err := n.InspectManagedKV(ctx, false); err == nil {
		t.Fatal("general evidence inspection accepted unresolved history")
	}
	if _, err := n.InspectManagedKV(ctx, true); err == nil {
		t.Fatal("fresh source inspection accepted unresolved history")
	}
	if err := n.ValidateManagedInitialAdmission(ctx, identity, after); err == nil {
		t.Fatal("preexisting unresolved recorder established an initial cut")
	}
	for _, field := range []string{"wal", "certified", "applied", "registered"} {
		bad := before
		switch field {
		case "wal":
			bad.WALIdentity = "different"
		case "certified":
			bad.CertifiedTip++
			bad.AppliedTip++
		case "applied":
			bad.AppliedTip--
		case "registered":
			bad.ExistingRegisteredWAL = !bad.ExistingRegisteredWAL
		}
		if err := n.ValidateManagedInitialAdmission(ctx, identity, bad); err == nil {
			t.Fatal("changed runtime cut accepted", field)
		}
	}
	badIdentity := identity
	badIdentity.Member.Generation++
	if err := n.ValidateManagedInitialAdmission(ctx, badIdentity, before); err == nil {
		t.Fatal("changed generation accepted")
	}
	cut := managedInitialAdmissionCut{state: before, prefix: prefix}
	cut.floor, cut.root, cut.hasRoot = n.core.RecoveryRoot()
	for _, field := range []string{"prefix", "root", "floor"} {
		bad := cut
		switch field {
		case "prefix":
			bad.prefix[0] ^= 1
		case "root":
			bad.root[0] ^= 1
		case "floor":
			bad.floor++
		}
		if n.initialAdmissionCutCurrent(bad, after) {
			t.Fatal("changed certified anchor accepted", field)
		}
	}
	afterValues, afterTip, err := n.core.DecisionsFrom(1, 256)
	afterPrefix, ok := n.core.PrefixHash(tip)
	afterOriginal, found, originalErr := n.RetainedRequest(ctx, "kv", "initial-cut-original")
	if err != nil || originalErr != nil || !found || !reflect.DeepEqual(original, afterOriginal) || !reflect.DeepEqual(values, afterValues) || afterTip != tip || !ok || afterPrefix != prefix || !bytes.Equal(afterOriginal.ExecutionItem, original.ExecutionItem) {
		t.Fatal("validation changed original, receipt or certified prefix", err, originalErr)
	}
}
