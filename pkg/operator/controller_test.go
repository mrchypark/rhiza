package operator

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

const controllerFixtureAdminToken = "controller-fixture-admin"

func TestControllerObservationAndFenceDoNotMutateStatefulSet(t *testing.T) {
	ctx, bucket, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Observed" || api.stsPuts != 0 {
		t.Fatalf("observation=%+v puts=%d", r.Status, api.stsPuts)
	}
	r.Spec.RecoveryID = "recovery-1"
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "AwaitingFence" || api.stsPuts != 0 {
		t.Fatalf("fence=%+v puts=%d", r.Status, api.stsPuts)
	}
	if _, err := bucket.Get(ctx, "root/source/recovery/successor.json"); !bucket.IsObjNotFoundErr(err) {
		t.Fatalf("unexpected successor: %v", err)
	}
}

func TestControllerAsyncSourceRequiresLossPolicyForBeforeAckTarget(t *testing.T) {
	ctx, _, r, _, c := controllerFixture(t, "async", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" || r.Status.Message != "async source requires explicit allowDataLoss, regardless of target mode" {
		t.Fatalf("status=%+v", r.Status)
	}
}

func TestControllerEmptySourceRequiresCanonicalCommitAndTargetReadBarrier(t *testing.T) {
	ctx, bucket, r, api, c := controllerEmptyFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "empty-recovery-1"
	r.Spec.Fence = fence(r)
	registrar := &archiveForkRegistrarFixture{emptyState: "authorized"}
	c.Fencer = registrar

	for _, stage := range []string{"Reserved", "Sealed", "Stopping", "Stopping", "Starting", "Restarted", "Restarted"} {
		if err := c.reconcileAll(ctx); err != nil {
			t.Fatal(err)
		}
		if r.Status.Stage != stage {
			t.Fatalf("stage=%q want=%q status=%+v", r.Status.Stage, stage, r.Status)
		}
	}
	if r.Status.TargetBaseKind != "empty" || r.Status.RecoveredTip != 0 || r.Status.ManifestHash != "" {
		t.Fatalf("empty source was represented as a fork: %+v", r.Status)
	}
	if r.Status.Phase != "WaitingReady" {
		t.Fatalf("target did not wait for runtime quorum after activation: %+v", r.Status)
	}
	// Model a target process publishing its first archive HEAD. Subsequent
	// reconciliation must consult the canonical receipt, not rerun the
	// pre-activation pristine-scope check against a live archive.
	if err := bucket.Upload(ctx, "root/"+r.Status.Target+"/archive/head.bin", bytes.NewReader(emptyArchiveHeadFixture())); err != nil {
		t.Fatal(err)
	}
	if registrar.emptyCalls < 2 || registrar.emptyRequest.RecoveryName != resourceName(r) ||
		registrar.emptyRequest.PolicyDigest != hashJSON(r.Spec) || registrar.emptyRequest.SourceFenceOperationID != r.Spec.RecoveryID {
		t.Fatalf("empty recovery was not bound to its persisted request: calls=%d request=%+v", registrar.emptyCalls, registrar.emptyRequest)
	}
	if _, err := bucket.Get(ctx, "root/"+r.Status.Target+"/recovery/generation.json"); !bucket.IsObjNotFoundErr(err) {
		t.Fatalf("empty recovery fabricated fork lineage: %v", err)
	}

	api.noQuorum = true
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Stage == "Complete" {
		t.Fatal("empty recovery completed without a target service ReadIndex barrier")
	}
	api.noQuorum = false
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Stage == "Complete" {
		t.Fatal("authorized empty recovery completed before canonical genesis commit")
	}
	registrar.emptyState = "committed"
	api.readBarrierUnavailable = true
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Stage == "Complete" || api.readBarrierCalls == 0 {
		t.Fatalf("empty recovery completed without the target application read-durability barrier: status=%+v calls=%d", r.Status, api.readBarrierCalls)
	}
	api.readBarrierUnavailable = false
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Stage != "Complete" || r.Status.RecoveredTip != 0 {
		t.Fatalf("committed empty recovery did not complete after the real target read-durability barrier: %+v", r.Status)
	}
}

func TestTargetReadBarrierRejectsReplacementAfterFinalProbe(t *testing.T) {
	ctx, _, r, api, c := controllerEmptyFixture(t, "before-ack", "before-ack")
	api.replacePodAfterProbe = true
	ready := map[string]bool{}
	for _, member := range api.voters {
		ready[string(member.ID)] = true
	}
	err := c.targetApplicationReadBarrier(ctx, r, api.sts, ready, controllerFixtureAdminToken)
	if err == nil {
		t.Fatal("read barrier accepted a Pod replacement after its final authenticated probe")
	}
	if api.finalProbeCalls == 0 || api.podUIDOverride == "" {
		t.Fatalf("fixture did not replace the probed Pod incarnation: probes=%d override=%q", api.finalProbeCalls, api.podUIDOverride)
	}
}

func TestControllerEmptyAsyncSourceRequiresExplicitLossPolicy(t *testing.T) {
	ctx, bucket, r, api, c := controllerEmptyFixture(t, "async", "before-ack")
	r.Spec.RecoveryID = "empty-async-recovery"
	r.Spec.Fence = fence(r)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" || r.Status.Stage != "" || api.zeroPuts != 0 {
		t.Fatalf("async empty source without loss policy advanced: status=%+v zeroPuts=%d", r.Status, api.zeroPuts)
	}
	if _, err := bucket.Get(ctx, "root/source/recovery/successor.json"); !bucket.IsObjNotFoundErr(err) {
		t.Fatalf("async no-policy request reserved source: %v", err)
	}
}

func TestControllerEmptyRecoveryFailsClosedWithoutInitializedHeadOrRegistrar(t *testing.T) {
	t.Run("missing source HEAD is not fresh empty", func(t *testing.T) {
		ctx, bucket, r, api, c := controllerEmptyFixture(t, "before-ack", "before-ack")
		if err := bucket.Delete(ctx, "root/source/archive/head.bin"); err != nil {
			t.Fatal(err)
		}
		r.Spec.RecoveryID = "missing-empty-head"
		r.Spec.Fence = fence(r)
		if err := c.reconcileAll(ctx); err != nil {
			t.Fatal(err)
		}
		if r.Status.Phase != "Blocked" || r.Status.Stage != "" || api.zeroPuts != 0 {
			t.Fatalf("missing source HEAD advanced recovery: status=%+v zeroPuts=%d", r.Status, api.zeroPuts)
		}
		if _, err := bucket.Get(ctx, "root/source/recovery/successor.json"); !bucket.IsObjNotFoundErr(err) {
			t.Fatalf("missing source HEAD reserved a successor: %v", err)
		}
	})

	t.Run("independent backend without empty registrar blocks before shutdown", func(t *testing.T) {
		ctx, _, r, api, c := controllerEmptyFixture(t, "before-ack", "before-ack")
		r.Spec.RecoveryID = "empty-without-registrar"
		r.Spec.Fence = fence(r)
		c.Fencer = archiveForkRegistrarHidingFencer{&FencingClient{}}
		for range 3 {
			if err := c.reconcileAll(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if r.Status.Phase != "Blocked" || r.Status.Stage != "Sealed" || api.zeroPuts != 0 {
			t.Fatalf("missing empty registrar advanced recovery: status=%+v zeroPuts=%d", r.Status, api.zeroPuts)
		}
	})
}

func controllerEmptyFixture(t *testing.T, sourceMode, targetMode string) (context.Context, objstore.Bucket, *Resource, *operatorAPI, *Controller) {
	t.Helper()
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	voters := members()
	if err := bucket.Upload(ctx, "root/source/archive/head.bin", bytes.NewReader(emptyArchiveHeadFixture())); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, "root/source/voters/membership.json", bytes.NewReader(jsonBytes(recovery.NewMembershipRecord("source", voters, sourceMode)))); err != nil {
		t.Fatal(err)
	}
	r := &Resource{Metadata: map[string]any{"name": "recover", "uid": "resource-uid", "generation": float64(1), "resourceVersion": "1"}, Spec: Spec{StatefulSet: "rhiza", SourceClusterID: "source", Durability: targetMode}}
	api := newOperatorAPIVoters(t, r, sourceMode, voters)
	ctr, err := container(api.sts, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	env := list(ctr["env"])
	env = replaceEnv(env, "RHIZA_ENABLE_RECONFIGURATION", object{"value": "true"})
	env = replaceEnv(env, "RHIZA_ADMIN_TOKEN", object{"value": controllerFixtureAdminToken})
	ctr["env"] = env
	t.Cleanup(api.Close)
	return ctx, bucket, r, api, &Controller{Kube: api.kube(t), Bucket: bucket, Prefix: "root", StoreIdentity: storeIdentity(), Fencer: &archiveForkRegistrarFixture{emptyState: "authorized"}}
}

func emptyArchiveHeadFixture() []byte {
	// Minimal valid config-1 generation-1 HEAD with an empty decision history.
	data := make([]byte, 132)
	copy(data[:8], []byte{'R', 'H', 'Z', 'A', 'H', 'D', '2', '!'})
	binary.BigEndian.PutUint32(data[8:12], uint32(len(data)))
	binary.BigEndian.PutUint64(data[16:24], uint64(recovery.ArchiveConfigID))
	binary.BigEndian.PutUint64(data[24:32], 1)
	binary.BigEndian.PutUint32(data[124:128], 0)
	binary.BigEndian.PutUint32(data[128:132], crc32.Checksum(data[:128], crc32.MakeTable(crc32.Castagnoli)))
	return data
}

func TestControllerReconfigurationForkPersistsAnchoredGenerationAcrossRestarts(t *testing.T) {
	ctx, bucket, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	ctr, err := container(api.sts, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_ENABLE_RECONFIGURATION", object{"value": "true"})

	restart := func() {
		c = &Controller{Kube: c.Kube, Bucket: bucket, Prefix: c.Prefix, StoreIdentity: c.StoreIdentity, Fencer: c.Fencer}
	}
	advance := func(stage string) {
		t.Helper()
		restart()
		if err := c.reconcileAll(ctx); err != nil {
			t.Fatal(err)
		}
		if r.Status.Stage != stage {
			t.Fatalf("stage=%q want=%q status=%+v", r.Status.Stage, stage, r.Status)
		}
	}
	advance("Reserved")
	advance("Sealed")
	advance("Stopping")
	if api.secret == nil || api.secret["immutable"] != true {
		t.Fatalf("anchored fork did not create immutable target credentials: %v", api.secret)
	}
	credentials := append([]byte(nil), jsonBytes(api.secret)...)
	encoded := str(nested(api.secret, "data", "members"))
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var targetMembers []quepaxa.Member
	if err := json.Unmarshal(data, &targetMembers); err != nil {
		t.Fatal(err)
	}
	wantMembership := recovery.NewMembershipRecord(r.Status.Target, targetMembers, r.Spec.Durability)
	anchor, anchorHash, err := recovery.ReadGenerationAnchor(ctx, bucket, "root/"+r.Status.Target)
	if err != nil || anchor.TargetMembership != wantMembership || anchor.SourcePrefix != "root/source" || anchor.TargetPrefix != "root/"+r.Status.Target {
		t.Fatalf("anchored target=%+v err=%v", anchor, err)
	}
	if _, err := recovery.VerifyGenerationAnchor(ctx, bucket, "root/"+r.Status.Target, wantMembership, anchorHash); err != nil {
		t.Fatalf("anchored generation is not verifiable: %v", err)
	}

	for _, stage := range []string{"Stopping", "Starting", "Restarted", "Complete"} {
		advance(stage)
		if !bytes.Equal(credentials, jsonBytes(api.secret)) {
			t.Fatal("target credentials changed after controller restart")
		}
	}
	if r.Status.Phase != "Complete" {
		t.Fatalf("completion=%+v", r.Status)
	}
	registrar := c.Fencer.(*archiveForkRegistrarFixture)
	wantRegistration := ArchiveForkGenesisRequest{
		StatefulSetUID: "sts-uid", TargetClusterID: r.Status.Target,
		OperationID: c.operationID(r), SourceFenceOperationID: r.Spec.RecoveryID,
	}
	if registrar.calls != 1 || registrar.request != wantRegistration {
		t.Fatalf("canonical fork registration calls=%d request=%+v want=%+v", registrar.calls, registrar.request, wantRegistration)
	}
}

type archiveForkRegistrarFixture struct {
	request      ArchiveForkGenesisRequest
	calls        int
	err          error
	emptyRequest ArchiveEmptyRecoveryRequest
	emptyCalls   int
	emptyState   string
	emptyErr     error
}

type archiveForkRegistrarHidingFencer struct{ Fencer }

func (*archiveForkRegistrarFixture) Fence(context.Context, FenceRequest) (*FenceProof, error) {
	return nil, fmt.Errorf("unexpected fencing request")
}

func (f *archiveForkRegistrarFixture) RegisterArchiveForkGenesis(_ context.Context, request ArchiveForkGenesisRequest) error {
	f.calls++
	f.request = request
	if request.StatefulSetUID == "" || request.TargetClusterID == "" || request.OperationID == "" || request.SourceFenceOperationID == "" {
		return fmt.Errorf("incomplete archive fork registration request: %+v", request)
	}
	return f.err
}

func (f *archiveForkRegistrarFixture) RegisterArchiveEmptyRecovery(_ context.Context, request ArchiveEmptyRecoveryRequest) (ArchiveEmptyRecoveryResponse, error) {
	f.emptyCalls++
	f.emptyRequest = request
	if f.emptyErr != nil {
		return ArchiveEmptyRecoveryResponse{}, f.emptyErr
	}
	digest := hashJSON("empty-recovery-fixture")
	return ArchiveEmptyRecoveryResponse{State: f.emptyState, Authorization: fencing.ArchiveEmptyRecoveryAuthorization{
		Version: fencing.ArchiveGenesisVersion, StatefulSetUID: request.StatefulSetUID,
		TargetClusterID: request.TargetClusterID, OperationID: request.OperationID,
		SourceFenceOperationID: request.SourceFenceOperationID, ConfigID: recovery.ArchiveConfigID,
		SourceScopeHash: digest, TargetScopeHash: hashJSON("target-scope"),
		SourceGenesisRefDigest: digest, SourceHeadDigest: digest,
		SourceHeadVersion: objstore.ObjectVersion{Type: objstore.ETag, Value: "source-head"},
		SourceRootDigest:  digest, SourceFenceDigest: digest, SourceMembershipHash: digest,
		SourceDurability: "before-ack", TargetMembershipHash: request.TargetMembershipHash,
		TargetDurability: request.TargetDurability, AllowDataLoss: request.AllowDataLoss,
		PolicyDigest: emptyRecoveryPolicyDigest(request.PolicyDigest, "before-ack", request.TargetDurability, request.AllowDataLoss),
	}}, nil
}

func TestControllerRegistersCanonicalForkBeforeAdvancing(t *testing.T) {
	for _, failRegistration := range []bool{false, true} {
		name := "success"
		if failRegistration {
			name = "registration error"
		}
		t.Run(name, func(t *testing.T) {
			ctx, _, r, api, c := controllerFixture(t, "before-ack", "before-ack")
			r.Spec.RecoveryID = "source-fence-op"
			r.Spec.Fence = fence(r)
			ctr, err := container(api.sts, "rhiza")
			if err != nil {
				t.Fatal(err)
			}
			ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_ENABLE_RECONFIGURATION", object{"value": "true"})
			registrar := &archiveForkRegistrarFixture{}
			if failRegistration {
				registrar.err = fmt.Errorf("canonical registration unavailable")
			}
			c.Fencer = registrar
			for range 3 {
				if err := c.reconcileAll(ctx); err != nil {
					t.Fatal(err)
				}
				if r.Status.Stage == "Stopping" || r.Status.Phase == "Blocked" {
					break
				}
			}
			if registrar.calls != 1 {
				t.Fatalf("registration calls=%d, want 1", registrar.calls)
			}
			want := ArchiveForkGenesisRequest{
				StatefulSetUID: "sts-uid", TargetClusterID: r.Status.Target,
				OperationID: c.operationID(r), SourceFenceOperationID: r.Spec.RecoveryID,
			}
			if registrar.request != want {
				t.Fatalf("registered request=%+v want=%+v", registrar.request, want)
			}
			if failRegistration {
				if r.Status.Phase != "Blocked" || r.Status.Stage != "Sealed" || api.zeroPuts != 0 {
					t.Fatalf("failed registration advanced recovery: status=%+v zeroPuts=%d", r.Status, api.zeroPuts)
				}
			} else if r.Status.Stage != "Stopping" {
				t.Fatalf("successful registration did not advance verified fork: %+v", r.Status)
			}
		})
	}
}

func TestControllerDoesNotTreatMissingRegistrarAsCooperativeProfile(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fencer    Fencer
		wantStage string
		wantPhase string
	}{
		{name: "wrapped independent backend without registrar", fencer: archiveForkRegistrarHidingFencer{&FencingClient{}}, wantStage: "Sealed", wantPhase: "Blocked"},
		{name: "explicit Kubernetes cooperative backend", fencer: &KubernetesFencer{}, wantStage: "Stopping", wantPhase: "Recovering"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, _, r, api, c := controllerFixture(t, "before-ack", "before-ack")
			r.Spec.RecoveryID = "source-fence-op"
			r.Spec.Fence = fence(r)
			ctr, err := container(api.sts, "rhiza")
			if err != nil {
				t.Fatal(err)
			}
			ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_ENABLE_RECONFIGURATION", object{"value": "true"})
			c.Fencer = tc.fencer
			for range 3 {
				if err := c.reconcileAll(ctx); err != nil {
					t.Fatal(err)
				}
				if r.Status.Stage == "Stopping" || r.Status.Phase == "Blocked" {
					break
				}
			}
			if r.Status.Stage != tc.wantStage || r.Status.Phase != tc.wantPhase {
				t.Fatalf("unexpected recovery progress: got stage=%q phase=%q, want stage=%q phase=%q", r.Status.Stage, r.Status.Phase, tc.wantStage, tc.wantPhase)
			}
			if tc.wantPhase == "Blocked" && api.zeroPuts != 0 {
				t.Fatalf("missing canonical registrar advanced activation: zero replica writes=%d", api.zeroPuts)
			}
		})
	}
}

func TestControllerRejectsInvalidReconfigurationFlag(t *testing.T) {
	ctx, _, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	ctr, err := container(api.sts, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_ENABLE_RECONFIGURATION", object{"value": "sometimes"})
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" || api.stsPuts != 0 {
		t.Fatalf("status=%+v writes=%d", r.Status, api.stsPuts)
	}
}

// A recovered StatefulSet can inherit a scalar RHIZA_PEER_TOKEN from its source
// template. The activation write injects the voter token map, and
// configEnvPeerToken rejects that combination, so the scalar must be overridden
// in the same write. EnvVar.Value carries omitempty, so the clearing override is
// stored name-only; the kubelet still applies it after envFrom, and the
// operator's own environment reader must read it back as an empty value.
func TestControllerActivationOverridesInheritedScalarPeerToken(t *testing.T) {
	ctx, _, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	ctr, err := container(api.sts, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	ctr["env"] = append(list(ctr["env"]), object{"name": "RHIZA_PEER_TOKEN", "value": "inherited-secret"})

	// Stopping needs one reconcile to scale the source down and one more to
	// observe the terminated Pods; the activation write happens on the next
	// reconcile, when the recorded stage is already Starting.
	for _, stage := range []string{"Reserved", "Sealed", "Stopping", "Stopping", "Starting", "Restarted"} {
		if err := c.reconcileAll(ctx); err != nil {
			t.Fatal(err)
		}
		if r.Status.Stage != stage {
			t.Fatalf("stage=%q want=%q status=%+v", r.Status.Stage, stage, r.Status)
		}
	}
	activated, err := container(api.sts, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	var overrides int
	var mapInjected bool
	for _, item := range list(activated["env"]) {
		entry := asObject(item)
		switch str(entry["name"]) {
		case "RHIZA_PEER_TOKEN":
			overrides++
			if entry["value"] != nil || entry["valueFrom"] != nil {
				t.Fatalf("clearing override must be a name-only entry: %+v", entry)
			}
		case "RHIZA_PEER_TOKENS":
			mapInjected = nested(entry, "valueFrom", "secretKeyRef", "key") == "peer_tokens"
		}
	}
	if overrides != 1 {
		t.Fatalf("activation wrote %d RHIZA_PEER_TOKEN entries, want one override: %+v", overrides, activated["env"])
	}
	if !mapInjected {
		t.Fatalf("voter token map was not injected: %+v", activated["env"])
	}
	// The operator reads the same StatefulSet back during activation; a name-only
	// entry the reader could not resolve would block recovery there instead of at
	// node startup, and a non-empty value would reintroduce the conflict.
	env, err := c.environment(ctx, activated)
	if err != nil {
		t.Fatalf("activated StatefulSet environment is unreadable: %v", err)
	}
	if env["RHIZA_PEER_TOKEN"] != "" {
		t.Fatalf("clearing override read back as %q", env["RHIZA_PEER_TOKEN"])
	}
}

func TestControllerMembershipRetryUsesExactJournaledPayloads(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	api.membership["n1"] = network.MembershipStatus{NodeID: "n1", ClusterID: "source", ConfigID: 1, Voters: []quepaxa.NodeID{"n1", "n2", "n3"}, Voting: true, AbortSlot: 7}
	api.membership["n2"] = network.MembershipStatus{NodeID: "n2", ClusterID: "source", ConfigID: 1, Voters: []quepaxa.NodeID{"n1", "n2", "n3"}, Voting: true, AbortSlot: 7}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Membership == nil || r.Status.Membership.RemoveRequest == nil || r.Status.Membership.RemoveRequest.ExpectedAbortSlot != 7 || len(api.changes) != 0 {
		t.Fatalf("remove journal=%+v changes=%d", r.Status.Membership, len(api.changes))
	}
	api.changeStatus = http.StatusServiceUnavailable
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.changes) != 2 || !bytes.Equal(jsonBytes(api.changes[0]), jsonBytes(api.changes[1])) {
		t.Fatalf("remove retries differ: %+v", api.changes)
	}
	api.changeStatus = 0
	api.membership["n1"] = network.MembershipStatus{NodeID: "n1", ClusterID: "source", ConfigID: 2, Voters: []quepaxa.NodeID{"n1", "n2"}, Voting: true, AbortSlot: 7}
	api.membership["n2"] = network.MembershipStatus{NodeID: "n2", ClusterID: "source", ConfigID: 2, Voters: []quepaxa.NodeID{"n1", "n2"}, Voting: true, AbortSlot: 7}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	added := voterMember("source", "n4", "new-token")
	if r.Status.Membership.Add == nil || r.Status.Membership.Add.ExpectedAbortSlot != 7 || bytes.Contains(jsonBytes(r.Status), []byte(hex.EncodeToString(added.PublicKey[:]))) {
		t.Fatalf("add journal leaks credentials: status=%+v journal=%+v", r.Status, r.Status.Membership)
	}
	api.membershipSecret["metadata"].(object)["resourceVersion"] = "2"
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" || len(api.changes) != 2 {
		t.Fatalf("credential drift=%+v changes=%d", r.Status, len(api.changes))
	}
	api.membershipSecret["metadata"].(object)["resourceVersion"] = "1"
	api.changeStatus = http.StatusServiceUnavailable
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if len(api.changes) != 4 || !bytes.Equal(jsonBytes(api.changes[2]), jsonBytes(api.changes[3])) {
		t.Fatalf("add retries differ: %+v", api.changes)
	}
	api.membership["n1"] = network.MembershipStatus{NodeID: "n1", ClusterID: "source", ConfigID: 3, Voters: []quepaxa.NodeID{"n1", "n2", "n4"}, Voting: true}
	api.membership["n2"] = network.MembershipStatus{NodeID: "n2", ClusterID: "source", ConfigID: 3, Voters: []quepaxa.NodeID{"n1", "n2", "n4"}, Voting: true}
	api.membership["n4"] = network.MembershipStatus{NodeID: "n4", ClusterID: "source", ConfigID: 3, Voters: []quepaxa.NodeID{"n1", "n2", "n4"}, WALIdentity: strings.Repeat("a", 64), Voting: true}
	originalCredential := api.membershipSecret["data"].(object)["member"]
	api.membershipSecret["data"].(object)["member"] = base64.StdEncoding.EncodeToString(jsonBytes(voterMember("source", "n4", "changed-token")))
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase == "Complete" {
		t.Fatal("add request hash drift bypassed completion validation")
	}
	api.membershipSecret["data"].(object)["member"] = originalCredential
	api.membershipSecret["metadata"].(object)["resourceVersion"] = "2"
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase == "Complete" {
		t.Fatal("credential drift bypassed completion validation")
	}
	api.membershipSecret["metadata"].(object)["resourceVersion"] = "1"
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Complete" || api.stsPuts != 0 {
		t.Fatalf("completion=%+v StatefulSet writes=%d", r.Status, api.stsPuts)
	}
}

func TestControllerMembershipRejectsBadFenceAndWorkload(t *testing.T) {
	for name, change := range map[string]func(*MembershipFence){
		"bad fence":      func(f *MembershipFence) { f.Confirmed = false },
		"wrong workload": func(f *MembershipFence) { f.WorkloadUID = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			ctx, r, api, c := membershipFixture(t)
			change(&r.Spec.Membership.Fence)
			if err := c.reconcileAll(ctx); err != nil {
				t.Fatal(err)
			}
			if r.Status.Phase != "Blocked" || len(api.changes) != 0 || api.stsPuts != 0 {
				t.Fatalf("status=%+v changes=%d writes=%d", r.Status, len(api.changes), api.stsPuts)
			}
		})
	}
}

func TestControllerMembershipFailsClosedWithoutQuorum(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	delete(api.membership, "n2")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" || len(api.changes) != 0 || api.stsPuts != 0 {
		t.Fatalf("status=%+v changes=%d writes=%d", r.Status, len(api.changes), api.stsPuts)
	}
}

func TestControllerMembershipAbortThenResumeWithNewLearner(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	} // journal remove
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	} // journal add
	if r.Status.Membership.Add == nil {
		t.Fatal("add was not journaled")
	}
	r.Spec.Membership.AbortAddition = true
	api.replacement, api.membershipSecret, api.membershipSecrets = nil, nil, map[string]object{}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Membership.Phase != "Aborted" || len(api.aborts) != 1 || api.aborts[0].Add != nil || api.aborts[0].ExpectedConfigID != 2 || api.aborts[0].ExpectedAbortSlot != 0 {
		t.Fatalf("abort=%+v journal=%+v", api.aborts, r.Status.Membership)
	}
	api.membership["n1"] = network.MembershipStatus{NodeID: "n1", ClusterID: "source", ConfigID: 2, Voters: []quepaxa.NodeID{"n1", "n2"}, Voting: true, AbortSlot: 1}
	api.membership["n2"] = network.MembershipStatus{NodeID: "n2", ClusterID: "source", ConfigID: 2, Voters: []quepaxa.NodeID{"n1", "n2"}, Voting: true, AbortSlot: 1}
	r.Spec.Membership.AbortAddition = false
	r.Spec.Membership.ReplacementPod, r.Spec.Membership.ReplacementSecret = "learner2", "learner2-credentials"
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Membership.Add != nil || r.Status.Membership.Phase != "Adding" {
		t.Fatalf("abort reset=%+v", r.Status.Membership)
	}
	_, port, err := net.SplitHostPort(api.learner.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	api.replacement2 = object{"metadata": object{"name": "learner2", "uid": "learner2-uid"}, "status": object{"podIP": "127.0.0.1"}, "spec": object{"containers": []any{object{"name": "rhiza", "ports": []any{object{"name": "http", "containerPort": float64(p)}}}}}}
	member := voterMember("source", "n5", "newer-token")
	api.membershipSecrets["learner2-credentials"] = object{"immutable": true, "metadata": object{"name": "learner2-credentials", "uid": "secret2-uid", "resourceVersion": "1"}, "data": object{"member": base64.StdEncoding.EncodeToString(jsonBytes(member))}}
	api.learnerID = "n5"
	api.membership["n5"] = network.MembershipStatus{NodeID: "n5", ClusterID: "source", ConfigID: 2, Voters: []quepaxa.NodeID{"n1", "n2"}, WALIdentity: strings.Repeat("d", 64)}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Membership.Add == nil || r.Status.Membership.Add.ExpectedAbortSlot != 1 || r.Status.Membership.Add.MemberID != "n5" {
		t.Fatalf("resumed add=%+v", r.Status.Membership)
	}
	api.membership["n1"] = network.MembershipStatus{NodeID: "n1", ClusterID: "source", ConfigID: 3, Voters: []quepaxa.NodeID{"n1", "n2", "n5"}, Voting: true}
	api.membership["n2"] = network.MembershipStatus{NodeID: "n2", ClusterID: "source", ConfigID: 3, Voters: []quepaxa.NodeID{"n1", "n2", "n5"}, Voting: true}
	api.membership["n5"] = network.MembershipStatus{NodeID: "n5", ClusterID: "source", ConfigID: 3, Voters: []quepaxa.NodeID{"n1", "n2", "n5"}, WALIdentity: strings.Repeat("d", 64), Voting: true}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Membership.Phase != "Complete" {
		t.Fatalf("replacement completed after abort-slot reset=%+v", r.Status.Membership)
	}
}

func TestControllerMembershipSecondReplacementUsesStandaloneVoter(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	r.Spec.Membership = &MembershipSpec{OperationID: "replace-n1", Remove: "n1", Fence: MembershipFence{NodeID: "n1", WALIdentity: strings.Repeat("c", 64), WorkloadUID: "pod-n1", Confirmed: true, Evidence: "external-fencer: incident-43"}, VoterPods: []VoterPod{{NodeID: "n2", Pod: "n2"}, {NodeID: "n4", Pod: "learner"}}, ReplacementPod: "learner2", ReplacementSecret: "learner-credentials"}
	api.replacement2 = object{"metadata": object{"name": "learner2", "uid": "learner2-uid"}, "status": object{"podIP": "127.0.0.1"}, "spec": object{"containers": []any{object{"name": "rhiza", "ports": []any{object{"name": "http", "containerPort": float64(8080)}}}}}}
	api.membership["n1"] = membershipStatus("n1", 3, "n1", "n2", "n4")
	api.membership["n2"] = membershipStatus("n2", 3, "n1", "n2", "n4")
	api.membership["n4"] = network.MembershipStatus{NodeID: "n4", ClusterID: "source", ConfigID: 3, Voters: []quepaxa.NodeID{"n1", "n2", "n4"}, WALIdentity: strings.Repeat("a", 64), Voting: true}
	delete(api.membership, "n4")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" || r.Status.Membership != nil {
		t.Fatalf("standalone voter was not required: %+v", r.Status)
	}
	api.membership["n4"] = network.MembershipStatus{NodeID: "n4", ClusterID: "source", ConfigID: 3, Voters: []quepaxa.NodeID{"n1", "n2", "n4"}, WALIdentity: strings.Repeat("a", 64), Voting: true}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Membership == nil || r.Status.Membership.RemoveRequest == nil || r.Status.Membership.RemoveRequest.ExpectedConfigID != 3 {
		t.Fatalf("second replacement was not journaled from standalone-voter quorum: %+v", r.Status)
	}
}

func membershipFixture(t *testing.T) (context.Context, *Resource, *operatorAPI, *Controller) {
	t.Helper()
	ctx, _, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.Membership = &MembershipSpec{OperationID: "replace-n3", Remove: "n3", Fence: MembershipFence{NodeID: "n3", WALIdentity: strings.Repeat("b", 64), WorkloadUID: "pod-n3", Confirmed: true, Evidence: "external-fencer: incident-42"}, VoterPods: []VoterPod{{NodeID: "n1", Pod: "n1"}, {NodeID: "n2", Pod: "n2"}}, ReplacementPod: "learner", ReplacementSecret: "learner-credentials"}
	ctr, err := container(api.sts, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_ADMIN_TOKEN", object{"value": controllerFixtureAdminToken})
	_, port, err := net.SplitHostPort(api.learner.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	api.replacement = object{"metadata": object{"name": "learner", "uid": "learner-uid"}, "status": object{"podIP": "127.0.0.1"}, "spec": object{"containers": []any{object{"name": "rhiza", "ports": []any{object{"name": "http", "containerPort": float64(p)}}}}}}
	member := voterMember("source", "n4", "new-token")
	api.membershipSecret = object{"immutable": true, "metadata": object{"name": "learner-credentials", "uid": "secret-uid", "resourceVersion": "1"}, "data": object{"member": base64.StdEncoding.EncodeToString(jsonBytes(member))}}
	api.learnerID = "n4"
	api.membershipSecrets = map[string]object{"learner-credentials": api.membershipSecret}
	api.membership = map[string]network.MembershipStatus{"n1": membershipStatus("n1", 1, "n1", "n2", "n3"), "n2": membershipStatus("n2", 1, "n1", "n2", "n3"), "n4": {NodeID: "n4", ClusterID: "source", ConfigID: 1, Voters: []quepaxa.NodeID{"n1", "n2", "n3"}, WALIdentity: strings.Repeat("a", 64)}}
	return ctx, r, api, c
}

func membershipStatus(node string, config uint, voters ...string) network.MembershipStatus {
	status := network.MembershipStatus{NodeID: quepaxa.NodeID(node), ClusterID: "source", ConfigID: config, Voting: true}
	for _, voter := range voters {
		status.Voters = append(status.Voters, quepaxa.NodeID(voter))
	}
	return status
}

func TestControllerStagesResumeAfterStatusFailure(t *testing.T) {
	for _, sourceMode := range []string{"before-ack", "async"} {
		t.Run(sourceMode, func(t *testing.T) {
			ctx, _, r, api, c := controllerFixture(t, sourceMode, "before-ack")
			r.Spec.RecoveryID = "recovery-1"
			r.Spec.Fence = fence(r)
			r.Spec.AllowDataLoss = sourceMode == "async"
			advance := func(stage string) {
				t.Helper()
				if err := c.reconcileAll(ctx); err != nil {
					t.Fatal(err)
				}
				if r.Status.Stage != stage {
					t.Fatalf("stage=%q want=%q status=%+v", r.Status.Stage, stage, r.Status)
				}
			}
			advance("Reserved")
			advance("Sealed")
			api.failStatus = 1
			if err := c.reconcileAll(ctx); err == nil {
				t.Fatal("status failure unexpectedly succeeded")
			}
			if api.secret == nil || r.Status.Stage != "Sealed" || api.zeroPuts != 0 {
				t.Fatalf("fork must be resumable before source deletion: secret=%v stage=%q zero=%d", api.secret != nil, r.Status.Stage, api.zeroPuts)
			}
			secret := str(nested(api.secret, "data", "members"))
			advance("Stopping")
			if api.zeroPuts != 0 || str(nested(api.secret, "data", "members")) != secret {
				t.Fatalf("resume changed preserved target: zero=%d", api.zeroPuts)
			}
			advance("Stopping")
			if api.zeroPuts != 1 || number(nested(api.sts, "spec", "replicas")) != 0 {
				t.Fatalf("source not scaled once: zero=%d sts=%v", api.zeroPuts, api.sts)
			}
			advance("Starting")
			advance("Restarted")
			advance("Complete")
			if r.Status.Phase != "Complete" || number(nested(api.sts, "spec", "replicas")) != 3 {
				t.Fatalf("completion=%+v", r.Status)
			}
		})
	}
}

func TestControllerBlocksSuccessorAndDrift(t *testing.T) {
	ctx, bucket, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	if err := bucket.Upload(ctx, "root/source/recovery/successor.json", bytes.NewReader([]byte(`{"operation_id":"other"}`)), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" || api.stsPuts != 0 {
		t.Fatalf("successor=%+v puts=%d", r.Status, api.stsPuts)
	}

	ctx, _, r, api, c = controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	api.sts["metadata"].(object)["uid"] = "new-uid"
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Message != "StatefulSet identity changed during recovery" {
		t.Fatalf("uid drift=%+v", r.Status)
	}

	ctx, _, r, _, c = controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	r.Spec.Container = "other"
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Message != "reserved recovery request is immutable; create a new resource" {
		t.Fatalf("spec drift=%+v", r.Status)
	}
}

func TestControllerRejectsUnknownTargetDurability(t *testing.T) {
	ctx, _, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.Durability = "unknown"
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Message != "target durability must be async or before-ack" || api.stsPuts != 0 {
		t.Fatalf("status=%+v", r.Status)
	}
}

// The voter count, the target replica count, and the frozen credential set must
// stay one number, and completion must require a strict majority of it.
func TestControllerVoterCountContract(t *testing.T) {
	for _, tc := range []struct{ voters, ready int }{
		{voters: 3, ready: 1},
		{voters: 3, ready: 2},
		{voters: 4, ready: 2},
		{voters: 4, ready: 3},
		{voters: 5, ready: 2},
		{voters: 5, ready: 3},
	} {
		t.Run(fmt.Sprintf("voters=%d ready=%d", tc.voters, tc.ready), func(t *testing.T) {
			voters := voterMembers(tc.voters)
			ctx, bucket, r, api, c := controllerFixtureVoters(t, "before-ack", "before-ack", voters)
			r.Spec.RecoveryID = "recovery-1"
			r.Spec.Fence = fence(r)
			api.unready = map[string]bool{}
			for i := tc.ready; i < tc.voters; i++ {
				api.unready[string(voters[i].ID)] = true
			}
			for range 8 {
				c = &Controller{Kube: c.Kube, Bucket: bucket, Prefix: c.Prefix, StoreIdentity: c.StoreIdentity, Fencer: c.Fencer}
				if err := c.reconcileAll(ctx); err != nil {
					t.Fatal(err)
				}
				if r.Status.Stage == "Restarted" {
					break
				}
			}
			if r.Status.Stage != "Restarted" {
				t.Fatalf("target did not start: %+v", r.Status)
			}
			if replicas := int(number(nested(api.sts, "spec", "replicas"))); replicas != tc.voters {
				t.Fatalf("target replicas=%d want=%d", replicas, tc.voters)
			}
			members, _, tokens := decodedCredentials(t, api.secret)
			if len(members) != tc.voters || len(tokens) != tc.voters {
				t.Fatalf("target credentials: members=%d tokens=%d want=%d", len(members), len(tokens), tc.voters)
			}
			for _, member := range members {
				if tokens[string(member.ID)] == "" {
					t.Fatalf("target peer token missing for %s", member.ID)
				}
			}
			if err := c.reconcileAll(ctx); err != nil {
				t.Fatal(err)
			}
			if len(r.Status.Peers) != tc.voters {
				t.Fatalf("probed peers=%d want=%d", len(r.Status.Peers), tc.voters)
			}
			if tc.ready < tc.voters/2+1 {
				if r.Status.Phase != "WaitingReady" || r.Status.Stage != "Restarted" {
					t.Fatalf("below-majority target was not held: %+v", r.Status)
				}
				return
			}
			if r.Status.Phase != "Complete" {
				t.Fatalf("majority target did not complete: %+v", r.Status)
			}
		})
	}
}

func decodedCredentials(t *testing.T, secret object) ([]quepaxa.Member, string, map[string]string) {
	t.Helper()
	memberData, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "members")))
	if err != nil {
		t.Fatal(err)
	}
	var members []quepaxa.Member
	if err := json.Unmarshal(memberData, &members); err != nil {
		t.Fatal(err)
	}
	tokenData, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "peer_tokens")))
	if err != nil {
		t.Fatal(err)
	}
	var tokens map[string]string
	if err := json.Unmarshal(tokenData, &tokens); err != nil {
		t.Fatal(err)
	}
	admin, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "admin")))
	if err != nil {
		t.Fatal(err)
	}
	return members, string(admin), tokens
}

func TestControllerRejectsVoterCountMismatch(t *testing.T) {
	ctx, _, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	asObject(api.sts["spec"])["replicas"] = float64(5)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if want := "source StatefulSet has 5 desired replicas but RHIZA_CLUSTER_MEMBERS lists 3 voters"; r.Status.Phase != "Blocked" || r.Status.Message != want {
		t.Fatalf("status=%+v want message=%q", r.Status, want)
	}
	if api.stsPuts != 0 {
		t.Fatalf("mismatched recovery mutated the StatefulSet: %d puts", api.stsPuts)
	}
}

func TestControllerRejectsFewerThanThreeVoters(t *testing.T) {
	ctx, _, r, _, c := controllerFixtureVoters(t, "before-ack", "before-ack", voterMembers(2))
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" || r.Status.Message != "recovery requires at least three voters" {
		t.Fatalf("status=%+v", r.Status)
	}
}

// Three ready peers of a five-voter target are its quorum, but a scaled-down
// workload must not become the next generation.
func TestControllerRejectsTargetReplicaShortfall(t *testing.T) {
	ctx, _, r, api, c := controllerFixtureVoters(t, "before-ack", "before-ack", voterMembers(5))
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	for range 8 {
		if err := c.reconcileAll(ctx); err != nil {
			t.Fatal(err)
		}
		if r.Status.Stage == "Restarted" {
			break
		}
	}
	if r.Status.Stage != "Restarted" {
		t.Fatalf("target did not start: %+v", r.Status)
	}
	asObject(api.sts["spec"])["replicas"] = float64(3)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if want := "target StatefulSet has 3 desired replicas but 5 target voters are frozen"; r.Status.Phase != "Blocked" || r.Status.Message != want {
		t.Fatalf("status=%+v want=%q", r.Status, want)
	}
}

// A reused immutable operation-owned Secret may not shrink the generation: it
// must name every source voter before the recovery forks or scales anything.
func TestControllerRejectsSubsetTargetCredentials(t *testing.T) {
	ctx, bucket, r, api, c := controllerFixtureVoters(t, "before-ack", "before-ack", voterMembers(5))
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	ctr, err := container(api.sts, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_ENABLE_RECONFIGURATION", object{"value": "true"})
	subset := []quepaxa.Member{voterMember("source", "n1", "new1"), voterMember("source", "n2", "new2"), voterMember("source", "n3", "new3")}
	tokens := map[string]string{}
	for _, member := range subset {
		tokens[string(member.ID)] = "token-" + string(member.ID)
	}
	api.secret = object{"immutable": true, "metadata": object{"name": "rhiza-recovery-" + shortID(c.operationID(r)), "annotations": object{ownerAnnotation: c.operationID(r)}}, "data": object{
		"members":     base64.StdEncoding.EncodeToString(jsonBytes(subset)),
		"admin":       base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("a"), 32)),
		"peer_tokens": base64.StdEncoding.EncodeToString(jsonBytes(tokens)),
	}}
	for range 4 {
		if err := c.reconcileAll(ctx); err != nil {
			t.Fatal(err)
		}
		if r.Status.Phase == "Blocked" {
			break
		}
	}
	if want := "target credentials preserve 3 of 5 source voters"; r.Status.Phase != "Blocked" || r.Status.Message != want {
		t.Fatalf("status=%+v want=%q", r.Status, want)
	}
	if replicas := int(number(nested(api.sts, "spec", "replicas"))); replicas != 5 || api.zeroPuts != 0 {
		t.Fatalf("subset target credentials scaled the workload: replicas=%d zeroPuts=%d", replicas, api.zeroPuts)
	}
	if r.Status.Target == "" {
		t.Fatal("recovery reserved no target generation")
	}
	if _, err := bucket.Get(ctx, "root/"+r.Status.Target+"/voters/membership.json"); !bucket.IsObjNotFoundErr(err) {
		t.Fatalf("subset target credentials forked a generation: %v", err)
	}
}

func fence(r *Resource) Fence {
	return Fence{RecoveryID: r.Spec.RecoveryID, ClusterID: r.Spec.SourceClusterID, StatefulSetUID: "sts-uid", Confirmed: true, Evidence: "incident-42"}
}

// voterMember derives the public identity a peer token grants one voter, so
// fixtures carry no secret in the replicated member record.
func voterMember(clusterID, name, token string) quepaxa.Member {
	return quepaxa.Member{ID: quepaxa.NodeID(name), URL: "http://" + name, PeerURL: "quic://" + name, PublicKey: quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(clusterID), quepaxa.NodeID(name), token))}
}

func controllerFixture(t *testing.T, sourceMode, targetMode string) (context.Context, objstore.Bucket, *Resource, *operatorAPI, *Controller) {
	t.Helper()
	return controllerFixtureVoters(t, sourceMode, targetMode, members())
}

func controllerFixtureRecoveryProbeMAC(probe network.RecoveryProbe, token string) string {
	probe.MAC = ""
	payload, _ := json.Marshal(probe)
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// controllerFixtureVoters builds the same recovery fixture for any supported
// voter count.
func controllerFixtureVoters(t *testing.T, sourceMode, targetMode string, voters []quepaxa.Member) (context.Context, objstore.Bucket, *Resource, *operatorAPI, *Controller) {
	t.Helper()
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	seedArchive(t, ctx, bucket, "root/source", voters, sourceMode)
	r := &Resource{Metadata: map[string]any{"name": "recover", "uid": "resource-uid", "generation": float64(1), "resourceVersion": "1"}, Spec: Spec{StatefulSet: "rhiza", SourceClusterID: "source", Durability: targetMode}}
	api := newOperatorAPIVoters(t, r, sourceMode, voters)
	ctr, err := container(api.sts, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	env := list(ctr["env"])
	env = replaceEnv(env, "RHIZA_ENABLE_RECONFIGURATION", object{"value": "true"})
	env = replaceEnv(env, "RHIZA_ADMIN_TOKEN", object{"value": controllerFixtureAdminToken})
	ctr["env"] = env
	t.Cleanup(api.Close)
	return ctx, bucket, r, api, &Controller{Kube: api.kube(t), Bucket: bucket, Prefix: "root", StoreIdentity: storeIdentity(), Fencer: &archiveForkRegistrarFixture{}}
}

type operatorAPI struct {
	mu                            sync.Mutex
	r                             *Resource
	sts                           object
	secret                        object
	membershipSecret              object
	membershipSecrets             map[string]object
	replacement                   object
	replacement2                  object
	failStatus, stsPuts, zeroPuts int
	server                        *httptest.Server
	peers                         []*httptest.Server
	learner                       *httptest.Server
	membership                    map[string]network.MembershipStatus
	changes                       []network.MembershipChange
	aborts                        []network.MembershipChange
	changeStatus                  int
	noQuorum                      bool
	readBarrierUnavailable        bool
	readBarrierCalls              int
	finalProbeCalls               int
	replacePodAfterProbe          bool
	podUIDOverride                string
	learnerID                     string
	voters                        []quepaxa.Member
	unready                       map[string]bool
}

func newOperatorAPIVoters(t *testing.T, r *Resource, mode string, voters []quepaxa.Member) *operatorAPI {
	a := &operatorAPI{r: r, sts: fakeSTS(mode, voters), voters: voters}
	for _, member := range voters {
		member := member
		a.peers = append(a.peers, httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
			a.mu.Lock()
			defer a.mu.Unlock()
			if q.URL.Path == "/recovery/archive" && q.Method == http.MethodPost {
				if q.Header.Get("Authorization") != "Bearer "+controllerFixtureAdminToken {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				// seedArchive already materializes the current-policy certified
				// source history; this endpoint models its authenticated capture ack.
				w.WriteHeader(http.StatusOK)
				return
			}
			if q.URL.Path == "/recovery/probe" && q.Method == http.MethodGet {
				if q.Header.Get("Authorization") != "Bearer "+controllerFixtureAdminToken {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				a.finalProbeCalls++
				tip := uint64(1)
				if a.r.Status.TargetBaseKind == "empty" {
					tip = 0
				}
				probe := network.RecoveryProbe{Nonce: q.URL.Query().Get("nonce"), Status: network.VoterRecoveryStatus{
					NodeID: string(member.ID), ClusterID: "rhiza-r-" + shortID(str(a.r.Metadata["uid"])+":"+a.r.Spec.RecoveryID), Durability: a.r.Spec.Durability,
					Ready: !a.unready[string(member.ID)], Quorum: !a.noQuorum, CertifiedTip: tip, AppliedTip: tip,
				}}
				probe.MAC = controllerFixtureRecoveryProbeMAC(probe, controllerFixtureAdminToken)
				_ = json.NewEncoder(w).Encode(probe)
				if a.replacePodAfterProbe && a.finalProbeCalls >= 2 {
					a.podUIDOverride = "replacement-after-probe"
				}
				return
			}
			if q.URL.Path == "/sql/query" && q.Method == http.MethodPost {
				a.readBarrierCalls++
				var request network.QueryRequest
				if json.NewDecoder(q.Body).Decode(&request) != nil || request.SQL != "SELECT 1" || request.Consistency != "linearizable" {
					http.Error(w, "invalid application read barrier", http.StatusBadRequest)
					return
				}
				if a.readBarrierUnavailable {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				_ = json.NewEncoder(w).Encode(network.QueryResponse{Columns: []string{"1"}, Rows: [][]interface{}{{float64(1)}}, AppliedSlot: 0, ConsensusTip: 0})
				return
			}
			if q.URL.Path == "/membership/status" {
				status, ok := a.membership[string(member.ID)]
				if !ok {
					http.NotFound(w, q)
					return
				}
				_ = json.NewEncoder(w).Encode(status)
				return
			}
			if q.URL.Path == "/membership/change" {
				var change network.MembershipChange
				if json.NewDecoder(q.Body).Decode(&change) != nil {
					http.Error(w, "bad request", http.StatusBadRequest)
					return
				}
				a.changes = append(a.changes, change)
				if a.changeStatus != 0 {
					w.WriteHeader(a.changeStatus)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]string{"status": "completed"})
				return
			}
			if q.URL.Path == "/membership/abort" {
				var change network.MembershipChange
				if json.NewDecoder(q.Body).Decode(&change) != nil {
					http.Error(w, "bad request", http.StatusBadRequest)
					return
				}
				a.aborts = append(a.aborts, change)
				_ = json.NewEncoder(w).Encode(map[string]string{"status": "completed"})
				return
			}
			if q.URL.Path != "/recovery/status" {
				http.NotFound(w, q)
				return
			}
			tip, durability := uint64(1), "before-ack"
			clusterID := "rhiza-r-" + shortID("resource-uid:recovery-1")
			if a.r.Status.TargetBaseKind == "empty" {
				tip = 0
				durability = a.r.Spec.Durability
				clusterID = "rhiza-r-" + shortID(str(a.r.Metadata["uid"])+":"+a.r.Spec.RecoveryID)
			}
			_ = json.NewEncoder(w).Encode(network.VoterRecoveryStatus{NodeID: string(member.ID), ClusterID: clusterID, Durability: durability, Ready: !a.unready[string(member.ID)], Quorum: !a.noQuorum, CertifiedTip: tip, AppliedTip: tip})
		})))
	}
	a.learner = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		a.mu.Lock()
		defer a.mu.Unlock()
		if q.URL.Path != "/membership/status" {
			http.NotFound(w, q)
			return
		}
		_ = json.NewEncoder(w).Encode(a.membership[a.learnerID])
	}))
	a.server = httptest.NewTLSServer(http.HandlerFunc(a.serve))
	return a
}
func (a *operatorAPI) Close() {
	a.server.Close()
	for _, peer := range a.peers {
		peer.Close()
	}
	a.learner.Close()
}
func (a *operatorAPI) kube(t *testing.T) *Kubernetes {
	t.Helper()
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("token"), 0600); err != nil {
		t.Fatal(err)
	}
	return &Kubernetes{BaseURL: a.server.URL, Namespace: "default", TokenFile: token, Client: a.server.Client()}
}
func (a *operatorAPI) serve(w http.ResponseWriter, q *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch q.URL.Path {
	case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizarecoveries":
		if q.Method == "GET" {
			json.NewEncoder(w).Encode(recoveryList{Items: []Resource{*a.r}})
			return
		}
	case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizarecoveries/recover/status":
		if q.Method == "PUT" {
			if a.failStatus > 0 {
				a.failStatus--
				w.WriteHeader(500)
				return
			}
			var next Resource
			if json.NewDecoder(q.Body).Decode(&next) == nil {
				*a.r = next
				json.NewEncoder(w).Encode(a.r)
				return
			}
		}
	case "/apis/apps/v1/namespaces/default/statefulsets/rhiza":
		if q.Method == "GET" {
			json.NewEncoder(w).Encode(a.sts)
			return
		}
		if q.Method == "PUT" {
			var next object
			if json.NewDecoder(q.Body).Decode(&next) == nil {
				a.sts = next
				apiserverNormalize(a.sts)
				a.stsPuts++
				if number(nested(next, "spec", "replicas")) == 0 {
					a.zeroPuts++
				}
				json.NewEncoder(w).Encode(a.sts)
				return
			}
		}
	case "/api/v1/namespaces/default/secrets":
		if q.Method == "POST" {
			var next object
			if json.NewDecoder(q.Body).Decode(&next) == nil {
				if a.secret == nil {
					a.secret = next
				}
				json.NewEncoder(w).Encode(a.secret)
				return
			}
		}
	case "/api/v1/namespaces/default/pods":
		if q.Method == "GET" {
			json.NewEncoder(w).Encode(podList{Items: a.pods()})
			return
		}
	case "/api/v1/namespaces/default/services/rhiza-headless":
		if q.Method == "GET" {
			json.NewEncoder(w).Encode(object{"spec": object{"clusterIP": "None", "publishNotReadyAddresses": true, "selector": object{"app.kubernetes.io/name": "rhiza"}}})
			return
		}
	}
	if q.Method == "GET" && strings.HasPrefix(q.URL.Path, "/api/v1/namespaces/default/pods/") {
		name := strings.TrimPrefix(q.URL.Path, "/api/v1/namespaces/default/pods/")
		if name == "learner" && a.replacement != nil {
			json.NewEncoder(w).Encode(a.replacement)
			return
		}
		if name == "learner2" && a.replacement2 != nil {
			json.NewEncoder(w).Encode(a.replacement2)
			return
		}
		for _, pod := range a.pods() {
			if str(nested(pod, "metadata", "name")) == name {
				json.NewEncoder(w).Encode(pod)
				return
			}
		}
	}
	if q.Method == "GET" && strings.HasPrefix(q.URL.Path, "/api/v1/namespaces/default/secrets/") {
		name := strings.TrimPrefix(q.URL.Path, "/api/v1/namespaces/default/secrets/")
		if secret := a.membershipSecrets[name]; secret != nil {
			json.NewEncoder(w).Encode(secret)
			return
		}
		if name == "learner-credentials" && a.membershipSecret != nil {
			json.NewEncoder(w).Encode(a.membershipSecret)
			return
		}
	}
	if q.Method == "GET" && a.secret != nil && strings.HasPrefix(q.URL.Path, "/api/v1/namespaces/default/secrets/rhiza-recovery-") {
		json.NewEncoder(w).Encode(a.secret)
		return
	}
	http.NotFound(w, q)
}

func (a *operatorAPI) pods() []object {
	// A scale-down removes Pods; the fixture must not keep reporting them.
	replicas := int(number(nested(a.sts, "spec", "replicas")))
	if replicas <= 0 {
		return nil
	}
	result := make([]object, 0, replicas)
	for i, peer := range a.peers {
		if i >= replicas {
			break
		}
		_, port, err := net.SplitHostPort(peer.Listener.Addr().String())
		if err != nil {
			continue
		}
		p, err := strconv.Atoi(port)
		if err != nil {
			continue
		}
		podUID := "pod-" + string(a.voters[i].ID)
		if a.podUIDOverride != "" {
			podUID = a.podUIDOverride
		}
		result = append(result, object{
			"metadata": object{"name": string(a.voters[i].ID), "uid": podUID, "ownerReferences": []any{object{"uid": "sts-uid", "kind": "StatefulSet"}}},
			"status":   object{"podIP": "127.0.0.1"},
			"spec":     object{"containers": []any{object{"name": "rhiza", "ports": []any{object{"name": "http", "containerPort": float64(p)}}}}},
		})
	}
	return result
}

func fakeSTS(mode string, voters []quepaxa.Member) object {
	raw, _ := json.Marshal(voters)
	return object{
		"metadata": object{"name": "rhiza", "uid": "sts-uid", "generation": float64(1), "annotations": object{}},
		"spec": object{
			"serviceName": "rhiza-headless", "replicas": float64(len(voters)), "volumeClaimTemplates": []any{},
			"template": object{"metadata": object{"labels": object{"app.kubernetes.io/name": "rhiza"}}, "spec": object{
				"containers": []any{object{
					"name":         "rhiza",
					"env":          []any{object{"name": "RHIZA_CLUSTER_ID", "value": "source"}, object{"name": "RHIZA_CLUSTER_MEMBERS", "value": string(raw)}, object{"name": "RHIZA_OBJSTORE_DURABILITY", "value": mode}, object{"name": "RHIZA_OBJSTORE_PREFIX", "value": "root"}, object{"name": "RHIZA_OBJSTORE_PROVIDER", "value": "s3"}, object{"name": "RHIZA_OBJSTORE_ENDPOINT", "value": "https://store.example"}, object{"name": "RHIZA_OBJSTORE_BUCKET", "value": "rhiza"}},
					"volumeMounts": []any{object{"name": "data", "mountPath": "/data"}},
				}},
				"volumes": []any{object{"name": "data", "emptyDir": object{}}},
			}},
		},
	}
}

// apiserverNormalize applies the serialization the real API server performs on
// a StatefulSet write: EnvVar.Value carries omitempty, so an empty string is
// dropped and only the name survives. A fake that keeps `"value":""` would
// hide exactly the defect the operator hit when it cleared an env entry.
func apiserverNormalize(sts object) {
	for _, cRef := range list(nested(sts, "spec", "template", "spec", "containers")) {
		for _, eRef := range list(asObject(cRef)["env"]) {
			entry := asObject(eRef)
			if str(entry["value"]) == "" {
				delete(entry, "value")
			}
		}
	}
}

func members() []quepaxa.Member {
	return voterMembers(3)
}

func voterMembers(count int) []quepaxa.Member {
	voters := make([]quepaxa.Member, 0, count)
	for i := 1; i <= count; i++ {
		voters = append(voters, voterMember("source", fmt.Sprintf("n%d", i), fmt.Sprintf("old%d", i)))
	}
	return voters
}
func storeIdentity() map[string]string {
	return map[string]string{"RHIZA_OBJSTORE_PROVIDER": "s3", "RHIZA_OBJSTORE_ENDPOINT": "https://store.example", "RHIZA_OBJSTORE_BUCKET": "rhiza", "RHIZA_OBJSTORE_PREFIX": "root", "RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT": ""}
}

func seedArchive(t *testing.T, ctx context.Context, bucket objstore.Bucket, prefix string, ms []quepaxa.Member, mode string) {
	t.Helper()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: ms[0].ID, Cluster: quepaxa.Cluster{ConfigID: recovery.ArchiveConfigID, Members: ms}, WAL: wal, Transport: testTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "recovery-fixture", SQL: "CREATE TABLE recovery_fixture (id INTEGER PRIMARY KEY)"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := types.ValidateExecutionPolicy(value); err != nil {
		t.Fatalf("seed archive must use the current SQL execution policy: %v", err)
	}
	if _, _, err = core.Propose(ctx, value); err != nil {
		t.Fatal(err)
	}
	archive := recovery.NewManager(bucket, prefix, recovery.ArchiveConfigID)
	defer archive.Close()
	if err = archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(recovery.NewMembershipRecord("source", ms, mode))
	if err = bucket.Upload(ctx, prefix+"/voters/membership.json", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}

type testTransport struct{}

func (testTransport) SendRecord(_ context.Context, to quepaxa.NodeID, r quepaxa.RecordRequest) (quepaxa.Summary, error) {
	p := r.Proposal
	return quepaxa.Summary{RecorderID: to, Step: r.Step, FirstCurrent: &p}, nil
}
func (testTransport) SendDecision(context.Context, quepaxa.Decision) error          { return nil }
func (testTransport) ReadTip(context.Context, quepaxa.NodeID) (quepaxa.Slot, error) { return 0, nil }
func (testTransport) StageValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash, []byte) error {
	return nil
}
func (testTransport) FetchValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash) ([]byte, error) {
	return nil, nil
}
