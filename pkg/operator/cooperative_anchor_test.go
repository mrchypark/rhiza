package operator

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/thanos-io/objstore"
)

func TestAnchoredCooperativeRecoveryBlocksBeforeArchiveOrAnchorEffects(t *testing.T) {
	ctx, bucket, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "anchored-cooperative-recovery"
	r.Spec.Fence = fence(r)
	r.Spec.AnchorID = "required-anchor"
	setAnchorEnv(r, api)

	// Use the actual cooperative backend. It deliberately does not implement
	// canonical fork registration; an anchor must not imply that capability.
	c.Fencer = &KubernetesFencer{Kube: c.Kube}
	if _, ok := c.Fencer.(ArchiveForkGenesisRegistrar); ok {
		t.Fatal("Kubernetes cooperative fencer unexpectedly implements canonical fork registration")
	}
	anchor := &fakeAnchorClient{}
	c.Anchor = anchor
	ctr, err := container(api.sts, r.Spec.Container)
	if err != nil {
		t.Fatal(err)
	}
	env, err := c.environment(ctx, ctr)
	if err != nil {
		t.Fatal(err)
	}
	if env["RHIZA_ENABLE_RECONFIGURATION"] != "true" || env["RHIZA_RECOVERY_ANCHOR_ID"] != r.Spec.AnchorID {
		t.Fatalf("fixture does not exercise anchored reconfiguration: env=%v anchorID=%q", env, r.Spec.AnchorID)
	}

	before, err := archiveObjectVersions(ctx, bucket)
	if err != nil {
		t.Fatalf("snapshot source archive before reconcile: %v", err)
	}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}

	if r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, "canonical archive fork registration") {
		t.Fatalf("anchored cooperative recovery was not blocked with an actionable capability error: status=%+v", r.Status)
	}
	if r.Status.Target != "" || r.Status.Stage != "" {
		t.Fatalf("blocked anchored recovery reserved a target or advanced stages: status=%+v", r.Status)
	}
	if got := atomic.LoadInt32(&anchor.activateN); got != 0 {
		t.Fatalf("anchor activation ran before canonical fork registration was available: calls=%d", got)
	}
	if api.stsPuts != 0 {
		t.Fatalf("blocked anchored recovery mutated the source StatefulSet: puts=%d", api.stsPuts)
	}
	after, err := archiveObjectVersions(ctx, bucket)
	if err != nil {
		t.Fatalf("snapshot source archive after reconcile: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("blocked anchored recovery changed source/target archive objects: before=%v after=%v", before, after)
	}
}

func archiveObjectVersions(ctx context.Context, bucket objstore.Bucket) (map[string]string, error) {
	objects := make(map[string]string)
	err := bucket.Iter(ctx, "root/", func(name string) error {
		attrs, err := bucket.Attributes(ctx, name)
		if err != nil {
			return err
		}
		version := ""
		if attrs.Version != nil {
			version = fmt.Sprintf("%d:%s", attrs.Version.Type, attrs.Version.Value)
		}
		objects[name] = fmt.Sprintf("%d:%s", attrs.Size, version)
		return nil
	}, objstore.WithRecursiveIter())
	return objects, err
}
