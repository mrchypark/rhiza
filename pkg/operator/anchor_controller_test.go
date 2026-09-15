package operator

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

// fakeAnchorClient implements AnchorClient for controller tests.
type fakeAnchorClient struct {
	activate  func(context.Context, recoveryanchor.Request) (recoveryanchor.Receipt, error)
	verify    func(context.Context, recoveryanchor.Request, recoveryanchor.Receipt) error
	activateN int32
	verifyN   int32
}

func (f *fakeAnchorClient) Activate(ctx context.Context, req recoveryanchor.Request) (recoveryanchor.Receipt, error) {
	atomic.AddInt32(&f.activateN, 1)
	if f.activate != nil {
		return f.activate(ctx, req)
	}
	return recoveryanchor.Receipt{
		AnchorID:    req.AnchorID,
		OperationID: req.OperationID,
		RequestHash: "hash",
		Target:      recoveryanchor.Binding{ClusterID: req.TargetClusterID},
	}, nil
}

func (f *fakeAnchorClient) Verify(ctx context.Context, req recoveryanchor.Request, receipt recoveryanchor.Receipt) error {
	atomic.AddInt32(&f.verifyN, 1)
	if f.verify != nil {
		return f.verify(ctx, req, receipt)
	}
	return nil
}

func setAnchorEnv(r *Resource, api *operatorAPI) {
	ctr, _ := container(api.sts, "rhiza")
	ctr["env"] = append(list(ctr["env"]), object{"name": "RHIZA_RECOVERY_ANCHOR_ID", "value": r.Spec.AnchorID})
	ctr["env"] = append(list(ctr["env"]), object{"name": "RHIZA_ENABLE_RECONFIGURATION", "value": "true"})
}

func TestAnchorBlockedWhenBackendNil(t *testing.T) {
	ctx, _, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	r.Spec.AnchorID = "my-anchor"
	setAnchorEnv(r, api)
	c.Anchor = nil
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" || r.Status.Message != "anchor backend is not configured for anchorID my-anchor" {
		t.Fatalf("status=%+v", r.Status)
	}
}

func TestAnchorEnvMismatchBlocks(t *testing.T) {
	ctx, _, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	r.Spec.AnchorID = "my-anchor"
	c.Anchor = &fakeAnchorClient{}
	ctr, err := container(api.sts, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	ctr["env"] = append(list(ctr["env"]), object{"name": "RHIZA_RECOVERY_ANCHOR_ID", "value": "other-anchor"})
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" {
		t.Fatalf("phase=%s want Blocked", r.Status.Phase)
	}
}

func TestAnchorEnvMatchProceeds(t *testing.T) {
	ctx, _, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	r.Spec.AnchorID = "my-anchor"
	c.Anchor = &fakeAnchorClient{}
	setAnchorEnv(r, api)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase == "Blocked" && r.Status.Message == "RHIZA_RECOVERY_ANCHOR_ID does not match spec anchorID" {
		t.Fatal("should not block on env mismatch when IDs match")
	}
}

func TestAnchorActivateFailsClosed(t *testing.T) {
	ctx, bucket, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	r.Spec.AnchorID = "my-anchor"
	setAnchorEnv(r, api)

	anchor := &fakeAnchorClient{}
	c.Anchor = anchor

	// empty → Reserved
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Stage != "Reserved" {
		t.Fatalf("stage=%q want Reserved", r.Status.Stage)
	}

	// Reserved → Sealed
	c = &Controller{Kube: c.Kube, Bucket: bucket, Prefix: c.Prefix, StoreIdentity: c.StoreIdentity, Anchor: anchor}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Stage != "Sealed" {
		t.Fatalf("stage=%q want Sealed", r.Status.Stage)
	}

	// Sealed → blocked: failing anchor client.
	failAnchor := &fakeAnchorClient{activate: func(_ context.Context, _ recoveryanchor.Request) (recoveryanchor.Receipt, error) {
		return recoveryanchor.Receipt{}, fmt.Errorf("anchor service unavailable")
	}}
	c = &Controller{Kube: c.Kube, Bucket: bucket, Prefix: c.Prefix, StoreIdentity: c.StoreIdentity, Anchor: failAnchor}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" || r.Status.Message != "anchor activation failed: anchor service unavailable" {
		t.Fatalf("status=%+v", r.Status)
	}
	// Original anchor client was never called; only failAnchor was.
	if atomic.LoadInt32(&anchor.activateN) != 0 {
		t.Fatalf("original anchor should not be called, activateN=%d", anchor.activateN)
	}
	// Source sealed but no fork result published.
	_, err := bucket.Get(ctx, path.Join("root", r.Spec.SourceClusterID, "fork/result.json"))
	if !bucket.IsObjNotFoundErr(err) {
		t.Fatalf("source should not have fork result after blocked activation: %v", err)
	}
}

func TestAnchorSuccessFullFlow(t *testing.T) {
	ctx, bucket, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	r.Spec.AnchorID = "my-anchor"
	setAnchorEnv(r, api)

	anchor := &fakeAnchorClient{}
	c.Anchor = anchor

	advance := func(stage string) {
		t.Helper()
		c = &Controller{Kube: c.Kube, Bucket: bucket, Prefix: c.Prefix, StoreIdentity: c.StoreIdentity, Anchor: anchor}
		if err := c.reconcileAll(ctx); err != nil {
			t.Fatal(err)
		}
		if r.Status.Stage != stage {
			t.Fatalf("stage=%q want=%q phase=%q msg=%q", r.Status.Stage, stage, r.Status.Phase, r.Status.Message)
		}
	}

	advance("Reserved")
	advance("Sealed")
	advance("Stopping")
	advance("Stopping")
	advance("Starting")
	advance("Restarted")
	advance("Complete")

	if r.Status.Phase != "Complete" {
		t.Fatalf("phase=%q want Complete", r.Status.Phase)
	}
	if atomic.LoadInt32(&anchor.activateN) != 1 {
		t.Fatalf("activateN=%d want 1", anchor.activateN)
	}
	if atomic.LoadInt32(&anchor.verifyN) < 1 {
		t.Fatalf("verifyN=%d want >= 1", anchor.verifyN)
	}
	if r.Status.AnchorRequest == nil || r.Status.AnchorReceipt == nil {
		t.Fatal("anchor request/receipt not persisted")
	}
}

func TestAnchorMissingReceiptBlocks(t *testing.T) {
	ctx, _, r, _, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	r.Spec.AnchorID = "my-anchor"
	r.Status.Stage = "Starting"
	r.Status.RecoveryID = "recovery-1"
	r.Status.SpecHash = immutableSpecHash(r.Spec)
	r.Status.Target = "rhiza-r-" + shortID(c.operationID(r))
	r.Status.SecretName = "rhiza-recovery-" + shortID(c.operationID(r))
	r.Status.RecoveredTip = 1
	r.Status.Source = "source"
	r.Status.SourceDurability = "before-ack"
	r.Status.SourceMembership = sourceFingerprint("source", "before-ack", members(), "root/source")
	r.Status.StatefulSetUID = "sts-uid"

	req := recoveryanchor.Request{AnchorID: "my-anchor", OperationID: c.operationID(r)}
	r.Status.AnchorRequest = &req
	r.Status.AnchorReceipt = nil
	c.Anchor = &fakeAnchorClient{}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" {
		t.Fatalf("phase=%s want Blocked when receipt missing", r.Status.Phase)
	}
}

func TestAnchorTamperedRequestBlocks(t *testing.T) {
	ctx, bucket, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	r.Spec.AnchorID = "my-anchor"
	setAnchorEnv(r, api)

	anchor := &fakeAnchorClient{}
	c.Anchor = anchor

	advance := func(stage string) {
		t.Helper()
		c = &Controller{Kube: c.Kube, Bucket: bucket, Prefix: c.Prefix, StoreIdentity: c.StoreIdentity, Anchor: anchor}
		if err := c.reconcileAll(ctx); err != nil {
			t.Fatal(err)
		}
		if r.Status.Stage != stage {
			t.Fatalf("stage=%q want=%q", r.Status.Stage, stage)
		}
	}

	advance("Reserved")
	advance("Sealed")
	advance("Stopping")
	advance("Stopping")
	advance("Starting")

	// Tamper with the persisted anchor request.
	r.Status.AnchorRequest.Fork.Tip = 999

	c = &Controller{Kube: c.Kube, Bucket: bucket, Prefix: c.Prefix, StoreIdentity: c.StoreIdentity, Anchor: anchor}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" {
		t.Fatalf("phase=%q want Blocked for tampered request", r.Status.Phase)
	}
}

func TestAnchorLostStatusWriteRetryIdempotent(t *testing.T) {
	ctx, bucket, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	r.Spec.AnchorID = "my-anchor"
	setAnchorEnv(r, api)

	anchor := &fakeAnchorClient{}
	c.Anchor = anchor

	// empty → Reserved
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Stage != "Reserved" {
		t.Fatalf("stage=%q want Reserved", r.Status.Stage)
	}

	// Reserved → Sealed
	c = &Controller{Kube: c.Kube, Bucket: bucket, Prefix: c.Prefix, StoreIdentity: c.StoreIdentity, Anchor: anchor}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Stage != "Sealed" {
		t.Fatalf("stage=%q want Sealed", r.Status.Stage)
	}

	// Simulate lost status write.
	api.failStatus = 1
	c = &Controller{Kube: c.Kube, Bucket: bucket, Prefix: c.Prefix, StoreIdentity: c.StoreIdentity, Anchor: anchor}
	err := c.reconcileAll(ctx)
	if err == nil {
		t.Fatal("expected status write failure")
	}
	if r.Status.Stage != "Sealed" {
		t.Fatalf("stage=%q want Sealed after failed status write", r.Status.Stage)
	}

	// Retry: idempotent.
	c = &Controller{Kube: c.Kube, Bucket: bucket, Prefix: c.Prefix, StoreIdentity: c.StoreIdentity, Anchor: anchor}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Stage != "Stopping" {
		t.Fatalf("stage=%q want Stopping after retry", r.Status.Stage)
	}
	if atomic.LoadInt32(&anchor.activateN) != 2 {
		t.Fatalf("activateN=%d want 2", anchor.activateN)
	}
}

func TestAnchorStartingAlreadyTargetStaleVerify(t *testing.T) {
	ctx, bucket, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	r.Spec.AnchorID = "my-anchor"
	setAnchorEnv(r, api)
	anchor := &fakeAnchorClient{}
	c.Anchor = anchor
	advance := func(stage string) {
		t.Helper()
		c = &Controller{Kube: c.Kube, Bucket: bucket, Prefix: c.Prefix, StoreIdentity: c.StoreIdentity, Anchor: anchor}
		if err := c.reconcileAll(ctx); err != nil {
			t.Fatal(err)
		}
		if r.Status.Stage != stage {
			t.Fatalf("stage=%q want=%q phase=%q msg=%q", r.Status.Stage, stage, r.Status.Phase, r.Status.Message)
		}
	}
	advance("Reserved")
	advance("Sealed")
	advance("Stopping")
	advance("Stopping")
	advance("Starting")
	advance("Restarted")
	// Switch to failing verify client.
	verifyErr := errors.New("stale receipt")
	failAnchor := &fakeAnchorClient{verify: func(_ context.Context, _ recoveryanchor.Request, _ recoveryanchor.Receipt) error {
		return verifyErr
	}}
	// Simulate lost status: sts already has target env, reset only Stage.
	r.Status.Stage = "Starting"
	nBefore := atomic.LoadInt32(&anchor.verifyN)
	c = &Controller{Kube: c.Kube, Bucket: bucket, Prefix: c.Prefix, StoreIdentity: c.StoreIdentity, Anchor: failAnchor}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, verifyErr.Error()) {
		t.Fatalf("phase=%q msg=%q want Blocked with %q", r.Status.Phase, r.Status.Message, verifyErr.Error())
	}
	nAfter := atomic.LoadInt32(&anchor.verifyN)
	if nAfter != nBefore {
		t.Fatalf("verifyN went %d->%d, original anchor should not be called", nBefore, nAfter)
	}
	if atomic.LoadInt32(&failAnchor.verifyN) != 1 {
		t.Fatalf("failAnchor.verifyN=%d want 1", failAnchor.verifyN)
	}
}

func TestNonAnchoredResourceSkipsGate(t *testing.T) {
	ctx, _, r, _, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	c.Anchor = nil
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase == "Blocked" && r.Status.Message == "anchor backend is not configured for anchorID " {
		t.Fatal("non-anchored resource should not require anchor backend")
	}
}

func TestAnchorStartingVerifyFailsWhenBackendUnavailable(t *testing.T) {
	ctx, bucket, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	r.Spec.AnchorID = "my-anchor"
	setAnchorEnv(r, api)
	anchor := &fakeAnchorClient{}
	c.Anchor = anchor
	advance := func(stage string) {
		t.Helper()
		c = &Controller{Kube: c.Kube, Bucket: bucket, Prefix: c.Prefix, StoreIdentity: c.StoreIdentity, Anchor: anchor}
		if err := c.reconcileAll(ctx); err != nil {
			t.Fatal(err)
		}
		if r.Status.Stage != stage {
			t.Fatalf("stage=%q want=%q phase=%q msg=%q", r.Status.Stage, stage, r.Status.Phase, r.Status.Message)
		}
	}
	advance("Reserved")
	advance("Sealed")
	advance("Stopping")
	advance("Stopping")
	advance("Starting")
	// Nil the anchor backend. anchorEnvGate fires before receipt check.
	c.Anchor = nil
	c = &Controller{Kube: c.Kube, Bucket: bucket, Prefix: c.Prefix, StoreIdentity: c.StoreIdentity}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, "anchor backend") {
		t.Fatalf("phase=%q msg=%q want Blocked from anchor safety gate", r.Status.Phase, r.Status.Message)
	}
}
