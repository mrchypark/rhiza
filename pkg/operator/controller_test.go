package operator

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
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
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

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

func TestControllerReconfigurationForkPersistsAnchoredGenerationAcrossRestarts(t *testing.T) {
	ctx, bucket, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	ctr, err := container(api.sts, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	ctr["env"] = append(list(ctr["env"]), object{"name": "RHIZA_ENABLE_RECONFIGURATION", "value": "true"})

	restart := func() {
		c = &Controller{Kube: c.Kube, Bucket: bucket, Prefix: c.Prefix, StoreIdentity: c.StoreIdentity}
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
}

func TestControllerRejectsInvalidReconfigurationFlag(t *testing.T) {
	ctx, _, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	ctr, err := container(api.sts, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	ctr["env"] = append(list(ctr["env"]), object{"name": "RHIZA_ENABLE_RECONFIGURATION", "value": "sometimes"})
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" || api.stsPuts != 0 {
		t.Fatalf("status=%+v writes=%d", r.Status, api.stsPuts)
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
	if r.Status.Membership.Add == nil || r.Status.Membership.Add.ExpectedAbortSlot != 7 || bytes.Contains(jsonBytes(r.Status), []byte("new-token")) {
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
	api.membershipSecret["data"].(object)["member"] = base64.StdEncoding.EncodeToString(jsonBytes(quepaxa.Member{ID: "n4", URL: "http://n4", PeerURL: "quic://n4", Token: "changed-token"}))
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
	member := quepaxa.Member{ID: "n5", URL: "http://n5", PeerURL: "quic://n5", Token: "newer-token"}
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
	ctr["env"] = append(list(ctr["env"]), object{"name": "RHIZA_ADMIN_TOKEN", "value": "admin"})
	_, port, err := net.SplitHostPort(api.learner.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	api.replacement = object{"metadata": object{"name": "learner", "uid": "learner-uid"}, "status": object{"podIP": "127.0.0.1"}, "spec": object{"containers": []any{object{"name": "rhiza", "ports": []any{object{"name": "http", "containerPort": float64(p)}}}}}}
	member := quepaxa.Member{ID: "n4", URL: "http://n4", PeerURL: "quic://n4", Token: "new-token"}
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

func fence(r *Resource) Fence {
	return Fence{RecoveryID: r.Spec.RecoveryID, ClusterID: r.Spec.SourceClusterID, StatefulSetUID: "sts-uid", Confirmed: true, Evidence: "incident-42"}
}

func controllerFixture(t *testing.T, sourceMode, targetMode string) (context.Context, objstore.Bucket, *Resource, *operatorAPI, *Controller) {
	t.Helper()
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	members := members()
	seedArchive(t, ctx, bucket, "root/source", members, sourceMode)
	r := &Resource{Metadata: map[string]any{"name": "recover", "uid": "resource-uid", "generation": float64(1), "resourceVersion": "1"}, Spec: Spec{StatefulSet: "rhiza", SourceClusterID: "source", Durability: targetMode}}
	api := newOperatorAPI(t, r, sourceMode)
	t.Cleanup(api.Close)
	return ctx, bucket, r, api, &Controller{Kube: api.kube(t), Bucket: bucket, Prefix: "root", StoreIdentity: storeIdentity()}
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
	learnerID                     string
}

func newOperatorAPI(t *testing.T, r *Resource, mode string) *operatorAPI {
	a := &operatorAPI{r: r, sts: fakeSTS(mode)}
	for _, member := range members() {
		member := member
		a.peers = append(a.peers, httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
			a.mu.Lock()
			defer a.mu.Unlock()
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
			_ = json.NewEncoder(w).Encode(network.VoterRecoveryStatus{NodeID: string(member.ID), ClusterID: "rhiza-r-" + shortID("resource-uid:recovery-1"), Durability: "before-ack", Ready: true, Quorum: true, CertifiedTip: 1, AppliedTip: 1})
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
	if number(nested(a.sts, "spec", "replicas")) == 0 {
		return nil
	}
	result := make([]object, 0, len(a.peers))
	for i, peer := range a.peers {
		_, port, err := net.SplitHostPort(peer.Listener.Addr().String())
		if err != nil {
			continue
		}
		p, err := strconv.Atoi(port)
		if err != nil {
			continue
		}
		result = append(result, object{
			"metadata": object{"name": string(members()[i].ID), "uid": "pod-" + string(members()[i].ID), "ownerReferences": []any{object{"uid": "sts-uid", "kind": "StatefulSet"}}},
			"status":   object{"podIP": "127.0.0.1"},
			"spec":     object{"containers": []any{object{"name": "rhiza", "ports": []any{object{"name": "http", "containerPort": float64(p)}}}}},
		})
	}
	return result
}

func fakeSTS(mode string) object {
	raw, _ := json.Marshal(members())
	return object{
		"metadata": object{"name": "rhiza", "uid": "sts-uid", "generation": float64(1), "annotations": object{}},
		"spec": object{
			"replicas": float64(3), "volumeClaimTemplates": []any{},
			"template": object{"spec": object{
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
func members() []quepaxa.Member {
	return []quepaxa.Member{{ID: "n1", URL: "http://n1", PeerURL: "quic://n1", Token: "old1"}, {ID: "n2", URL: "http://n2", PeerURL: "quic://n2", Token: "old2"}, {ID: "n3", URL: "http://n3", PeerURL: "quic://n3", Token: "old3"}}
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
	core, err := quepaxa.New(quepaxa.Config{NodeID: ms[0].ID, Cluster: quepaxa.Cluster{ConfigID: 1, Members: ms}, WAL: wal, Transport: testTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	value, err := types.EncodeSQLBatch([]types.SQLCommand{{RequestID: "recovery-fixture", SQL: "CREATE TABLE recovery_fixture (id INTEGER PRIMARY KEY)"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = core.Propose(ctx, value); err != nil {
		t.Fatal(err)
	}
	archive := recovery.NewManager(bucket, prefix, 1)
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
