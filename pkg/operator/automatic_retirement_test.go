package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type retirementTransport func(*http.Request) (*http.Response, error)

func (f retirementTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type retirementFencer func(context.Context, FenceRequest) (*FenceProof, error)

func (f retirementFencer) Fence(ctx context.Context, r FenceRequest) (*FenceProof, error) {
	return f(ctx, r)
}

func TestLearnerRetirementReservesHandshakeWithoutSnapshotJournal(t *testing.T) {
	r := autoClusterResource("cluster", "uid", "logical")
	state := validAutoControl()
	state.Intent = &autoIntent{RecoveryName: "replace", LearnerName: "replace-learner-0", LearnerUID: "learner-uid", LearnerWAL: strings.Repeat("a", 64), LearnerMissingSince: time.Now().Add(-time.Hour), Request: FenceRequest{LogicalID: "logical", BindingUID: "binding", Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: "sts", SourceClusterID: "source", OperationID: "replace", Scope: ScopeVoter}}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPut {
			_ = json.NewEncoder(w).Encode(r)
			return
		}
		http.NotFound(w, req)
	}))
	defer api.Close()
	count := 0
	c := autoController(autoBucket(t))
	c.Kube.BaseURL, c.Kube.Client = api.URL, api.Client()
	c.Kube.TokenFile = filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(c.Kube.TokenFile, []byte("token"), 0600); err != nil {
		t.Fatal(err)
	}
	child := &Resource{} // Snapshot read before a delayed executor journals its add.
	liveChild := &Resource{}
	if _, err := c.handleAutomaticLearner(context.Background(), r, state, nil, nil, child); err != nil {
		t.Fatal(err)
	}
	liveChild.Status.Membership = &MembershipStatus{Phase: "Adding", Add: &MembershipAddJournal{MemberID: "replace-learner-0"}}
	if !state.Intent.Aborting || state.Intent.LearnerAttempt != 0 || state.Intent.LearnerName != "replace-learner-0" {
		t.Fatalf("candidate changed before executor retirement acknowledgment: %+v", state.Intent)
	}
	if liveChild.Status.Membership.Add == nil {
		t.Fatal("delayed journal was lost")
	}
	if count != 0 {
		t.Fatal("physical fencing started before retirement was acknowledged")
	}
	liveChild.Status.Membership.Phase = "Aborted"
	if handled, err := c.handleAutomaticLearner(context.Background(), r, state, nil, nil, liveChild); handled || err != nil || state.Intent.LearnerAttempt != 0 {
		t.Fatalf("stale acknowledgment advanced the candidate: handled=%t err=%v", handled, err)
	}
	liveChild.Spec.Membership = &MembershipSpec{AbortAddition: true, ReplacementPod: state.Intent.LearnerName}
	state, version, err := c.loadAuto(context.Background(), c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	state.Intent.SpecHash = hashJSON(liveChild.Spec)
	if err := c.saveAuto(context.Background(), r, state, version); err != nil {
		t.Fatal(err)
	}
	state, version, err = c.loadAuto(context.Background(), c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var fencedRequest FenceRequest
	c.Fencer = retirementFencer(func(ctx context.Context, request FenceRequest) (*FenceProof, error) {
		fencedRequest = request
		close(entered)
		<-release
		return (&countingFencer{count: &count}).Fence(ctx, request)
	})
	done := make(chan error, 1)
	go func() {
		_, err := c.handleAutomaticLearner(context.Background(), r, state, version, nil, liveChild)
		done <- err
	}()
	<-entered
	// Another reconciler can finish this exact retirement while the old call is
	// paused. Its CAS must prevent the delayed proof from rotating a second time.
	current, currentVersion, err := c.loadAuto(context.Background(), c.autoKey(r))
	if err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	current.Intent.Aborting = false
	current.Intent.LearnerAttempt = 1
	current.Intent.LearnerName = "replace-learner-1"
	current.Intent.LearnerUID, current.Intent.LearnerWAL = "", ""
	if err := c.saveAuto(context.Background(), r, current, currentVersion); err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	close(release)
	if err := <-done; !c.Bucket.IsConditionNotMetErr(err) {
		t.Fatalf("stale post-fence CAS was accepted: %v", err)
	}
	if count != 1 || len(fencedRequest.Targets) != 1 || fencedRequest.Targets[0].PodUID != "learner-uid" || fencedRequest.Targets[0].NodeID != "replace-learner-0" {
		t.Fatalf("delayed fence escaped its retired incarnation: %+v", fencedRequest)
	}
	current, _, err = c.loadAuto(context.Background(), c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	if current.Intent.LearnerAttempt != 1 || current.Intent.LearnerName != "replace-learner-1" {
		t.Fatalf("stale fence changed the next candidate: %+v", current.Intent)
	}
	if current.Intent.RetirementSince.IsZero() {
		t.Fatal("incident retirement budget was lost")
	}
}

func TestMembershipRetirementAcknowledgesNeverJournaledAddition(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	r.Spec.Membership.AbortAddition = true
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Membership == nil || r.Status.Membership.Phase != "Aborted" || len(api.changes) != 0 || len(api.aborts) != 0 {
		t.Fatalf("never dispatched candidate did not retire: %+v", r.Status)
	}
}

func TestMembershipRetirementRecognizesCommittedAdditionAfterLearnerLoss(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	add := r.Status.Membership.Add
	if add == nil {
		t.Fatal("missing addition journal")
	}
	if err := writeRetirementOperation(ctx, c, r); err != nil {
		t.Fatal(err)
	}
	api.membership["n1"] = membershipStatus("n1", 3, "n1", "n2", "n4")
	api.membership["n2"] = membershipStatus("n2", 3, "n1", "n2", "n4")
	api.replacement = nil
	delete(api.membership, "n4")
	r.Spec.Membership.AbortAddition = true
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Complete" || r.Status.Membership.Add == nil || len(api.aborts) != 0 {
		t.Fatalf("committed promotion blocked on unavailable learner: %+v aborts=%d", r.Status, len(api.aborts))
	}
}

func TestMembershipRetirementRejectsDelayedJournalAfterAcknowledgment(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	r.Metadata["resourceVersion"] = "1"
	var stale Resource
	if err := json.Unmarshal(jsonBytes(r), &stale); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	c.HTTP = &http.Client{Transport: retirementTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == strings.TrimPrefix(api.learner.URL, "http://") {
			close(entered)
			select {
			case <-release:
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}
		return http.DefaultTransport.RoundTrip(req)
	})}
	var mu sync.Mutex
	current := "2" // The automatic parent has submitted its retirement revision.
	casAPI := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/recover/status") && req.Method == http.MethodPut {
			mu.Lock()
			defer mu.Unlock()
			var next Resource
			if json.NewDecoder(req.Body).Decode(&next) != nil {
				http.Error(w, "bad request", 400)
				return
			}
			if str(next.Metadata["resourceVersion"]) != current {
				w.WriteHeader(http.StatusConflict)
				return
			}
			current = "3"
			next.Metadata["resourceVersion"] = current
			*r = next
			_ = json.NewEncoder(w).Encode(next)
			return
		}
		api.serve(w, req)
	}))
	defer casAPI.Close()
	c.Kube.BaseURL, c.Kube.Client = casAPI.URL, casAPI.Client()
	done := make(chan error, 1)
	go func() { done <- c.reconcileMembership(ctx, &stale) }()
	<-entered // Old executor has observed a live learner but has not journaled Add.
	r.Metadata["resourceVersion"] = "2"
	r.Spec.Membership.AbortAddition = true
	if err := c.reconcileMembership(ctx, r); err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	close(release)
	err := <-done
	var conflict *APIError
	if !errors.As(err, &conflict) || conflict.StatusCode != http.StatusConflict {
		t.Fatalf("stale journal write error=%v", err)
	}
	if r.Status.Membership.Phase != "Aborted" || r.Status.Membership.Add != nil || len(api.changes) != 0 {
		t.Fatalf("delayed executor reopened retired candidate: %+v changes=%d", r.Status, len(api.changes))
	}
}

func TestMembershipRetirementReplaysJournalBeforeFirstDispatch(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	r.Spec.Membership.AbortAddition = true
	requestHash := r.Status.Membership.Add.RequestHash
	c.HTTP = &http.Client{Transport: retirementTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/membership/abort" {
			return &http.Response{StatusCode: http.StatusConflict, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
		}
		return http.DefaultTransport.RoundTrip(req)
	})}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Aborting" || len(api.changes) != 1 || hashJSON(api.changes[0]) != requestHash {
		t.Fatalf("journaled request was not replayed verbatim: %+v changes=%+v", r.Status, api.changes)
	}
	c.HTTP = nil
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Aborted" || len(api.aborts) != 1 {
		t.Fatalf("replayed round did not retire: %+v", r.Status)
	}
}

func TestMembershipRetirementDelayedDispatchCannotEscapeAbortedRound(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	frozen, err := c.retiringMembershipRequest(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	pods, err := c.membershipPods(ctx, r.Spec.Membership.VoterPods)
	if err != nil {
		t.Fatal(err)
	}
	// Model the node's existing abort-revision contract; its real Core/WAL
	// implementation is covered by TestMembershipAdditionAbortAllowsNextRevision.
	var mu sync.Mutex
	var revision quepaxa.Slot
	pending := false
	model := retirementTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/membership/change" && req.URL.Path != "/membership/abort" {
			return http.DefaultTransport.RoundTrip(req)
		}
		var request network.MembershipChange
		if err := json.NewDecoder(req.Body).Decode(&request); err != nil {
			return nil, err
		}
		mu.Lock()
		defer mu.Unlock()
		code := http.StatusConflict
		if request.ExpectedConfigID == 2 && request.ExpectedAbortSlot == revision {
			if req.URL.Path == "/membership/change" {
				pending = true
				code = http.StatusServiceUnavailable
			} else if pending {
				pending = false
				revision++
				code = http.StatusOK
			}
		}
		api.mu.Lock()
		for _, id := range []string{"n1", "n2"} {
			status := api.membership[id]
			status.Pending, status.AbortSlot = pending, revision
			api.membership[id] = status
		}
		api.mu.Unlock()
		return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})
	c.HTTP = &http.Client{Transport: model}
	entered, release := make(chan struct{}), make(chan struct{})
	delayed := *c
	delayed.HTTP = &http.Client{Transport: retirementTransport(func(req *http.Request) (*http.Response, error) {
		close(entered)
		<-release
		return model.RoundTrip(req)
	})}
	done := make(chan error, 1)
	go func() { done <- delayed.changeMembership(ctx, pods["n1"], "rhiza", "admin", frozen) }()
	<-entered
	r.Spec.Membership.AbortAddition = true
	if err := c.reconcileMembership(ctx, r); err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	if r.Status.Phase != "Aborting" {
		close(release)
		<-done
		t.Fatalf("unstarted delayed round not reserved: %+v", r.Status)
	}
	if err := c.reconcileMembership(ctx, r); err != nil {
		close(release)
		<-done
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("delayed HTTP request escaped its aborted round")
	}
	if r.Status.Phase != "Aborted" || r.Status.Membership.Add == nil || revision == 0 || pending {
		t.Fatalf("round remained executable: %+v revision=%d pending=%t", r.Status, revision, pending)
	}
}

func TestMembershipRetirementCommittedEvidenceFailsClosed(t *testing.T) {
	for _, scenario := range []string{"missing-operation", "changed-operation-target", "changed-request", "changed-secret", "wrong-wal", "pending-survivor", "abort-revision", "unexpected-voter"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, r, api, c := membershipFixture(t)
			if err := c.reconcileAll(ctx); err != nil {
				t.Fatal(err)
			}
			api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
			api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
			if err := c.reconcileAll(ctx); err != nil {
				t.Fatal(err)
			}
			if scenario != "missing-operation" {
				if err := writeRetirementOperation(ctx, c, r); err != nil {
					t.Fatal(err)
				}
			}
			api.membership["n1"] = membershipStatus("n1", 3, "n1", "n2", "n4")
			api.membership["n2"] = membershipStatus("n2", 3, "n1", "n2", "n4")
			api.replacement = nil
			r.Spec.Membership.AbortAddition = true
			switch scenario {
			case "changed-operation-target":
				key := path.Join(c.Prefix, r.Spec.SourceClusterID, "membership", "2.json")
				reader, err := c.Bucket.Get(ctx, key)
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(reader)
				if err != nil {
					t.Fatal(err)
				}
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
				var operation struct {
					Request network.MembershipChange `json:"request"`
					Target  quepaxa.Cluster          `json:"target"`
				}
				if err := json.Unmarshal(data, &operation); err != nil {
					t.Fatal(err)
				}
				operation.Target.Members[2].WALIdentity = strings.Repeat("e", 64)
				if err := c.Bucket.Upload(ctx, key, bytes.NewReader(jsonBytes(operation))); err != nil {
					t.Fatal(err)
				}
			case "changed-request":
				r.Status.Membership.Add.RequestHash = "changed"
			case "changed-secret":
				api.membershipSecret["metadata"].(object)["uid"] = "new-secret"
			case "wrong-wal":
				r.Status.Membership.Add.WALIdentity = strings.Repeat("d", 64)
			case "pending-survivor":
				s := api.membership["n2"]
				s.Pending = true
				api.membership["n2"] = s
			case "abort-revision":
				s := api.membership["n2"]
				s.AbortSlot = 7
				api.membership["n2"] = s
			case "unexpected-voter":
				api.membership["n1"] = membershipStatus("n1", 3, "n1", "n2", "other")
				api.membership["n2"] = membershipStatus("n2", 3, "n1", "n2", "other")
			}
			if err := c.reconcileAll(ctx); err != nil {
				t.Fatal(err)
			}
			if r.Status.Phase != "Blocked" || r.Status.Membership.Add == nil || len(api.changes) != 0 || len(api.aborts) != 0 {
				t.Fatalf("unproven commit accepted: %+v", r.Status)
			}
		})
	}
}

func TestMembershipRetirementRestartKeepsRoundUntilAbortRevisionObserved(t *testing.T) {
	ctx, r, api, c := membershipFixture(t)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	r.Spec.Membership.AbortAddition = true
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	var restarted Resource
	if err := json.Unmarshal(jsonBytes(r), &restarted); err != nil {
		t.Fatal(err)
	}
	restarted.Spec.Membership.AbortAddition = false
	restarted.Spec.Membership.ReplacementPod = "learner2"
	restarted.Spec.Membership.ReplacementSecret = "learner2-credentials"
	if err := c.reconcileMembership(ctx, &restarted); err != nil {
		t.Fatal(err)
	}
	if restarted.Status.Phase != "Aborted" || restarted.Status.Membership.Add == nil {
		t.Fatalf("unconfirmed abort discarded journal after restart: %+v", restarted.Status)
	}
	api.membership["n1"] = network.MembershipStatus{NodeID: "n1", ClusterID: "source", ConfigID: 2, Voters: []quepaxa.NodeID{"n1", "n2"}, Voting: true, AbortSlot: 1}
	api.membership["n2"] = network.MembershipStatus{NodeID: "n2", ClusterID: "source", ConfigID: 2, Voters: []quepaxa.NodeID{"n1", "n2"}, Voting: true, AbortSlot: 1}
	if err := c.reconcileMembership(ctx, &restarted); err != nil {
		t.Fatal(err)
	}
	if restarted.Status.Phase != "Adding" || restarted.Status.Membership.Add != nil {
		t.Fatalf("confirmed abort did not release round after restart: %+v", restarted.Status)
	}
}

func writeRetirementOperation(ctx context.Context, c *Controller, r *Resource) error {
	add := r.Status.Membership.Add
	member, _, _, err := c.replacementMember(ctx, r.Spec.Membership.ReplacementSecret)
	if err != nil {
		return err
	}
	member.WALIdentity = add.WALIdentity
	request := network.MembershipChange{OperationID: add.OperationID, ClusterID: r.Spec.SourceClusterID, ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot, Add: &member}
	data := jsonBytes(struct {
		Request network.MembershipChange `json:"request"`
		Target  quepaxa.Cluster          `json:"target"`
	}{request, quepaxa.Cluster{ConfigID: add.ExpectedConfigID + 1, Members: []quepaxa.Member{{ID: "n1"}, {ID: "n2"}, member}}})
	return c.Bucket.Upload(ctx, path.Join(c.Prefix, r.Spec.SourceClusterID, "membership", fmt.Sprintf("%d.json", add.ExpectedConfigID)), bytes.NewReader(data))
}
